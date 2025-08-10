# Enhanced agent.py with Memory Support
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, AIMessage
from langchain.callbacks.base import BaseCallbackHandler
from sqlalchemy import create_engine, text
from utils import get_live_schema
import os, time, threading, re
from dotenv import load_dotenv; load_dotenv()
from sqlHandler import SQLHandler
from typing import Callable, Dict, Any, List
import logging
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def build_manual_context(original_q: str,
                        base_sql: str | None,
                        modifications: list[str],
                        new_request: str) -> str:
    """Build the prompt context that will be sent to the LLM."""
    
    # If this is a modification request, treat the new_request as the primary query
    if modifications and new_request:
        parts = [
            f"ORIGINAL QUESTION: {original_q}",
            f"USER'S UPDATED REQUEST: {new_request}",
            "",
            "TASK: Generate a completely new SQL query based on the USER'S UPDATED REQUEST above.",
            "Ignore the original question if the updated request is more specific.",
            "Focus on what the user is asking for now, not what they asked before.",
            "Do not try to modify existing SQL - create a fresh query."
        ]
        
        if base_sql:
            parts.insert(-4, f"PREVIOUS SQL (for reference only): {base_sql}")
            
    else:
        # Original flow for first-time questions
        parts = [f"ORIGINAL: {original_q}"]
        if base_sql:
            parts.append(f"BASE_SQL: {base_sql}")
        if modifications:
            parts.append("PREVIOUS_CHANGES:\n" + "\n".join(f"• {m}" for m in modifications))
        parts.append(f"NEW_CHANGE: {new_request}")
        parts.append(
            "TASK: Apply ALL changes to the BASE_SQL and output one valid, \
            read-only SQL query. Do not invent new tables."
        )
    
    final_context = "\n\n".join(parts)
    # Add this debug line
    logger.info(f"CONTEXT BEING SENT TO AI: {final_context[:500]}...")
    
    return final_context

def format_results_consistently(rows, columns=None):
    """
    Format query results as a markdown table with real column names.
    Accepts:
      rows: list of tuples or dicts.
      columns: list of column names, if available.
    """
    if not rows:
        return "```\nNo results found.\n```"
    
    if isinstance(rows, str):
        return f"```\n{rows}\n```"
    
    # Use dict keys if present
    if isinstance(rows[0], dict):
        keys = list(rows[0].keys())
        header = " | ".join(keys)
        separator = "-" * len(header)
        data_lines = [" | ".join(str(row.get(k, "")) for k in keys) for row in rows]
    elif columns:
        header = " | ".join(columns)
        separator = "-" * len(header)
        data_lines = [" | ".join(str(x) for x in row) for row in rows]
    else:
        num_cols = len(rows[0])
        header = " | ".join([f"Column{i+1}" for i in range(num_cols)])
        separator = "-" * len(header)
        data_lines = [" | ".join(str(x) for x in row) for row in rows]
    result = f"{header}\n{separator}\n" + "\n".join(data_lines)
    return f"```{result}```"


def format_explanation_for_slack(explanation: str) -> str:
    """Format explanation text for Slack (remove markdown)"""
    # Remove ALL markdown formatting
    explanation = explanation.replace("**", "")
    explanation = explanation.replace("*", "")
    explanation = explanation.replace("__", "")
    explanation = explanation.replace("_", "")
    
    # Convert numbered lists to bullet points
    import re
    explanation = re.sub(r'(\d+)\.\s*', '• ', explanation)
    
    # Remove any remaining markdown-style formatting
    explanation = re.sub(r'\*\*(.*?)\*\*', r'\1', explanation)  # Bold
    explanation = re.sub(r'\*(.*?)\*', r'\1', explanation)      # Italic
    explanation = re.sub(r'__(.*?)__', r'\1', explanation)      # Bold
    explanation = re.sub(r'_(.*?)_', r'\1', explanation)        # Italic
    
    return explanation

class ModificationChain:
    def __init__(self):
        self.items: list[str] = []

    def add(self, request: str):
        self.items.append(request)

    def summary(self) -> list[str]:
        return self.items


class UserCancelledError(Exception):
    pass

class HumanApprovalCallbackHandler(BaseCallbackHandler):
    """Enhanced callback handler"""
    
    def __init__(self, 
                 thinking_callback: Callable[[str], None],
                 approval_callback: Callable[[str, str], bool],
                 session_id: str = None, cancel_event: threading.Event = None):
        logger.debug("CALLBACK HANDLER INIT CALLED")
        self.thinking_callback = thinking_callback
        self.approval_callback = approval_callback
        self.session_id = session_id
        self.sql_queries = []
        self.thinking_steps = []
        self.approval_pending = False
        self.query_executed = False
        self.cancelled = False
        self.cancel_event = cancel_event or threading.Event()

    def _check_cancelled(self):
        """Check if cancelled and raise exception immediately"""
        if self.cancel_event.is_set():
            raise UserCancelledError()
        
    def _think(self, step: str):
        self._check_cancelled()
        
        # Skip temporary schema error steps
        if "❌" in step and ("schema" in step.lower() or "examining" in step.lower()):
            return
        self.thinking_steps.append(step)
        self.thinking_callback(step)

    def on_tool_start(self, serialized: Dict[str, Any], input_str: str, **kwargs) -> None:
        self._check_cancelled()
        
        if self.cancelled:
            raise Exception("Analysis cancelled by user")
        
        #print(f"on_tool_start CALLED!")
        tool_name = serialized.get("name", "Unknown Tool")
        #print(f"TOOL NAME: {tool_name}")
        # #print(f"TOOL INPUT: {str(input_str)[:200]}")
        
        if self.query_executed:
            return
            
        logger.info(f"=== TOOL START DEBUG ===")
        logger.info(f"Tool name: {tool_name}")
        # logger.info(f"Input: {repr(str(input_str)[:200])}")
        logger.info(f"========================")
        
        if "list_tables" in tool_name.lower():
            step = "Discovering available database tables"
            self._think(step)

        elif "schema" in tool_name.lower():
            if input_str and isinstance(input_str, dict) and "table_names" in input_str:
                table_name = input_str["table_names"]
                step = f"Examining schema for table: {table_name}"
            else:
                step = "Examining table structure and columns"
            self._think(step)

        elif "query_checker" in tool_name.lower():
            step = "Validating SQL query syntax"
            self._think(step)

    
    def on_tool_end(self, output: str, **kwargs) -> None:
        self._check_cancelled()
        
        if self.cancelled:
            raise Exception("Analysis cancelled by user")
        
        logger.debug(f"Tool end called, output length: {len(output)}")

        if not self.query_executed:
            try:
                parsed = None
                if output.strip().startswith("[") and output.strip().endswith("]"):
                    parsed = ast.literal_eval(output)
                if isinstance(parsed, list) and all(isinstance(t, str) for t in parsed):
                    clean_tables = parsed
                    # show up to 6
                    display = ", ".join(clean_tables[:6])
                    if len(clean_tables) > 6:
                        display += f" and {len(clean_tables)-6} more"
                    step = f"📋 Found tables: {display}"
                    return self._think(step)
            except Exception:
                pass

            # Detect table list
            try:
                if ("," in output and 
                    len(output.split(",")) > 3 and
                    "CREATE TABLE" not in output.upper() and
                    len(output) < 1000):
                    
                    tables = [t.strip() for t in output.split(",")]
                    clean_tables = []

                    for table in tables:
                        if (len(table) > 50 or 
                        "INFO" in table.upper() or 
                        "\n" in table or
                        "ERROR" in table.upper() or
                        table.count(" ") > 2):
                            break

                        if table and len(table.split()) <= 2:  # Allow up to 2 words
                            clean_tables.append(table)

                    if len(clean_tables) >= 3:  # Only show if we have meaningful tables
                        if len(clean_tables) <= 6:
                            table_names = ', '.join(clean_tables)
                        else:
                            table_names = ', '.join(clean_tables[:5])
                            table_names += f" and {len(clean_tables)-5} more"
                        
                        step = f"📋 Found tables: {table_names}"
                        self._think(step)
                        logger.info(f"TABLE LIST DETECTED: {step}")
                        return
            except Exception as e:
                # Silently continue if table detection fails
                logger.debug(f"Table detection failed gracefully: {e}")
                pass
            
            # Detect schema output with better error handling
            try:
                if "CREATE TABLE" in output.upper():
                    match = re.search(r'CREATE TABLE\s+(\w+)', output, re.IGNORECASE)
                    if match:
                        table_name = match.group(1)
                        # Validate table name looks reasonable
                        if len(table_name) < 50 and table_name.isalnum():
                            step = f"Analyzed schema for table: {table_name}"
                            self._think(step)
                            return
                    
                    # Fallback if we can't extract specific table name
                    step = f"Examined database schema structure"
                    self._think(step)
                    return
            except Exception as e:
                logger.debug(f"Schema detection failed gracefully: {e}")
                pass
            
        # ONLY count results ONCE when query is actually executed
        if (self.query_executed and output and "No results" not in output and
        not hasattr(self, '_result_counted')):
            try:
                # Try to parse the actual query result format
                if output.startswith('[') and output.endswith(']'):
                    # This looks like actual query results
                    import ast
                    try:
                        parsed_results = ast.literal_eval(output)
                        if isinstance(parsed_results, list):
                            row_count = len(parsed_results)
                            if row_count > 0:  # Only show if there are actual rows
                                step = f"📊 Analysis completed - results generated"
                                self._think(step)
                                self._result_counted = True
                    except:
                        pass
                else:
                    # Fallback to line counting method
                    lines = output.strip().split('\n')
                    data_lines = [line for line in lines if line.strip() and not line.startswith('-') and '|' in line]
                    if len(data_lines) > 1:  # Exclude header
                        row_count = len(data_lines) - 1
                        step = f"📊 Analysis completed - results generated"
                        self._think(step)
                        self._result_counted = True
            except Exception as e:
                logger.debug(f"Could not parse result count: {e}")

    def on_agent_action(self, action, **kwargs) -> None:
        self._check_cancelled()
        
        if self.cancelled:
            raise Exception("Analysis cancelled by user")
            
        logger.debug(f"Agent action called: {action.tool}")
    
        tool_name = action.tool
        tool_input = action.tool_input

        if "list_tables" in tool_name.lower():
            step = "Discovering available database tables"
            self._think(step)
        elif "schema" in tool_name.lower():
            if isinstance(tool_input, dict) and "table_names" in tool_input:
                table_names = tool_input["table_names"]
                # Handle multiple tables more gracefully
                if "," in table_names:
                    tables_list = [t.strip() for t in table_names.split(",")]
                    if len(tables_list) <= 2:
                        step = f"Examining schema for: {', '.join(tables_list)}"
                    else:
                        step = f"Examining schema for {len(tables_list)} tables"
                else:
                    step = f"Examining schema for table: {table_names}"
            else:
                step = "Examining table structure and columns"
            self._think(step)
        elif "query_checker" in tool_name.lower():
            step = "Validating SQL query syntax"
            self._think(step)
        elif "sql_db_query" == tool_name.lower():
            self._check_cancelled()

            sql_query = tool_input.get("query", str(tool_input))
            is_valid, validation_message = self._validate_sql_query(sql_query)

            if not is_valid:
                error_step = f"❌ Query validation failed: {validation_message}"
                self._think(error_step)
                raise Exception(f"Query rejected: {validation_message}")
        
            step = "Generated SQL query - requesting human approval"
            self._think(step)
            
            explanation = self._explain_sql_query(sql_query)

            self.approval_pending = True

            #check if cancelled again
            self._check_cancelled()
            
            approved = self.approval_callback(sql_query, explanation)
            self.approval_pending = False
            
            if not approved:
                raise Exception("Query execution cancelled by user")
                
            step = "Human approved - executing query"
            self._think(step)
            
            self.sql_queries.append({
                "query": sql_query,
                "explanation": explanation,
                "approved": approved,
                "timestamp": time.time()
            })

            self.query_executed = True

    def _validate_sql_query(self, sql_query: str) -> tuple[bool, str]:
        """Validate that the query is a safe SELECT statement"""
        clean_query = sql_query.strip().upper()
        
        clean_query = re.sub(r'--.*$', '', clean_query, flags=re.MULTILINE)
        clean_query = re.sub(r'/\*.*?\*/', '', clean_query, flags=re.DOTALL)
        clean_query = ' '.join(clean_query.split())
        
        if len(sql_query) > 10000:
            return False, "Query is too long (max 10,000 characters). Please ask for more specific data."
        
        dangerous_patterns = {
            r'\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER)\b': 
                "Data modification queries are not allowed. I can only read data using SELECT statements.",
            r'\b(CREATE|GRANT|REVOKE|EXEC|EXECUTE)\b': 
                "Administrative operations are not permitted. Please ask for data analysis only.",
            r'\b(DECLARE|SET)\b': 
                "Variable declarations are not allowed in queries.",
            r'\b(CALL|PROCEDURE)\b': 
                "Stored procedure calls are not permitted."
        }
        
        for pattern, message in dangerous_patterns.items():
            if re.search(pattern, clean_query):
                return False, message
        
        injection_patterns = [
            r"['\"];.*--",
            r"UNION\s+ALL\s+SELECT.*(?:DROP|DELETE|INSERT|UPDATE)",  # Only block dangerous UNION ALL
            r"OR\s+1\s*=\s*1",
            r"AND\s+1\s*=\s*1"
        ]
        
        for pattern in injection_patterns:
            if re.search(pattern, clean_query, re.IGNORECASE):
                return False, "Query contains potentially unsafe patterns. Please rephrase your question."
        
        return True, "Query validation passed"

    def _explain_sql_query(self, sql_query: str) -> str:
        """Generate a human-readable explanation of the SQL query"""
        try:
            llm = ChatOpenAI(
                model="gpt-4o-mini",
                temperature=0,
                openai_api_key=os.getenv("OPENAI_API_KEY")
            )
            
            
            explanation_prompt = f"""
            Please explain this SQL query in simple, human-readable terms:
            
            {sql_query}
            
            Focus on:
            1. What data it's looking for
            2. Which tables it's querying
            3. Any filters or conditions
            4. How results will be ordered/grouped
            
            Keep it concise and non-technical. Be precise about the actual values in the query.
            """
            
            response = llm.invoke([HumanMessage(content=explanation_prompt)])
            explanation = response.content.strip()
        
            formatted_explanation = format_explanation_for_slack(explanation)
            return formatted_explanation
            
        except Exception as e:
            logger.error(f"Error generating SQL explanation: {e}")
            return f"This query will search the database for information related to your question. Query: {sql_query[:100]}..."

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.database_url = os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        self.engine = create_engine(self.database_url)
        self._validate_connection()
    
    def _validate_connection(self):
        """Validate database connection"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection validated successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def get_schema_info(self) -> Dict:
        """Get database schema information"""
        try:
            return get_live_schema(self.engine)
        except Exception as e:
            logger.error(f"Error getting schema: {e}")
            return {}
    
    def execute_query(self, query: str) -> List:
        """Execute a SQL query safely"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
                columns = result.keys()
                return [tuple(row) for row in rows], list(columns)

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

class SafeSQLQueryTool(QuerySQLDataBaseTool):
    """Custom SQL tool that only allows SELECT queries"""
    
    def _run(self, query: str) -> str:
        clean_query = query.strip().upper()
        if not clean_query.startswith('SELECT'):
            raise ValueError("Only SELECT queries are allowed")
        
        dangerous_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']
        for keyword in dangerous_keywords:
            if keyword in clean_query:
                raise ValueError(f"Query contains forbidden keyword: {keyword}")
        
        return super()._run(query)

class IntelligentSQLAgent:
    """Enhanced SQL agent"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.llm = self._create_llm()
        self.db = self._create_langchain_db()
        
    def _create_llm(self) -> ChatOpenAI:
        """Create optimized LLM instance"""
        return ChatOpenAI(
            model=os.getenv("OPENAI_MODEL", "gpt-4o"),
            temperature=float(os.getenv("OPENAI_TEMPERATURE", "0.1")),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            max_tokens=int(os.getenv("OPENAI_MAX_TOKENS", "2000")),
            request_timeout=int(os.getenv("OPENAI_TIMEOUT", "60"))
        )
    
    # In the IntelligentSQLAgent class, add this method:
    def _create_langchain_db(self) -> SQLDatabase:
        """Create LangChain SQL database instance with better error handling"""
        
        try:
            return SQLDatabase.from_uri(
                self.db_manager.database_url,
                include_tables= None,
                sample_rows_in_table_info=int(os.getenv("SAMPLE_ROWS", "2")),
                max_string_length=int(os.getenv("MAX_STRING_LENGTH", "200")),
            )
        except Exception as e:
            logger.warning(f"Schema initialization had issues: {e}, continuing with basic setup")
            return SQLDatabase.from_uri(
                self.db_manager.database_url,
                sample_rows_in_table_info=1,  # Minimal sample
                max_string_length=100,        # Short strings
            )

    def _extract_tables_from_analysis(self, analysis: str) -> List[str]:
        """Extract likely table names from analysis"""
        schema = self.db_manager.get_schema_info()
        mentioned_tables = []
        
        for table_name in schema.keys():
            if table_name.lower() in analysis.lower():
                mentioned_tables.append(table_name)
        
        return mentioned_tables
    
    def _assess_complexity(self, question: str) -> str:
        """Assess query complexity"""
        complex_keywords = ["join", "group by", "aggregate", "average", "sum", "count", "compare", "trend", "over time"]
        medium_keywords = ["filter", "where", "specific", "between", "range"]
        
        question_lower = question.lower()
        
        if any(keyword in question_lower for keyword in complex_keywords):
            return "high"
        elif any(keyword in question_lower for keyword in medium_keywords):
            return "medium"
        else:
            return "low"
    
    def _should_visualize(self, question: str, analysis: str) -> bool:
        """Determine if visualization would be helpful"""
        viz_keywords = ["chart", "graph", "plot", "visualize", "trend", "compare", "distribution", "over time"]
        combined_text = (question + " " + analysis).lower()
        
        return any(keyword in combined_text for keyword in viz_keywords)
    
    def _check_relation_to_previous(self, question: str, sql_history: List[Dict]) -> bool:
        """Check if current question relates to previous queries"""
        if not sql_history:
            return False
        
        # Simple keyword matching - could be enhanced with semantic similarity
        question_lower = question.lower()
        relation_keywords = ["same", "similar", "also", "again", "like before", "previous", "earlier"]
        
        if any(keyword in question_lower for keyword in relation_keywords):
            return True
        
        # Check for common table/column references
        recent_query = sql_history[-1]["query"].lower() if sql_history else ""
        
        # Extract potential table names from recent query
        table_matches = re.findall(r'from\s+(\w+)', recent_query)
        for table in table_matches:
            if table in question_lower:
                return True
        
        return False

def get_agent_with_human_approval(
    question: str,
    context,
    thinking_callback: Callable[[str], None],
    approval_callback: Callable[[str, str], bool],
    session_id: str = None
) -> Dict[str, Any]:
    """
    Enhanced main function
    
    Args:
        question: User's question
        context: Conversation context
        thinking_callback: Function to call with thinking updates
        approval_callback: Function to call for SQL approval (returns bool)
        session_id: Session ID
    
    Returns:
        Dict with answer, sql_query, db_rows, and metadata
    """
    try:
        # Initialize intelligent agent
        agent = IntelligentSQLAgent()
        
        # Use session_id from context if not provided
        if not session_id and hasattr(context, 'session_id'):
            session_id = context.session_id
        
        # Phase 1: Analyze the question
        thinking_callback("Starting intelligent analysis")
        
        analysis_result = {
            "analysis": "Analyzing question",
            "tables_needed": [],
            "complexity": "medium",
            "visualization_recommended": False,
            "has_context": False,
            "related_to_previous": False
        }
        
        # Phase 2: Create synchronous approval mechanism
        approval_result = {"approved": False, "sql_query": None, "explanation": None}
        approval_event = threading.Event()
        
        def sync_approval_callback(sql_query: str, explanation: str) -> bool:
            """Synchronous wrapper for approval callback"""
            approval_result["sql_query"] = sql_query
            approval_result["explanation"] = explanation
            
            try:
                result = approval_callback(sql_query, explanation)
                approval_result["approved"] = result
                approval_event.set()
                return result
            except Exception as e:
                logger.error(f"Error in approval callback: {e}")
                approval_result["approved"] = False
                approval_event.set()
                return False
        
        # Phase 3: Create callback handler with session ID
        callback_handler = HumanApprovalCallbackHandler(
            thinking_callback, 
            sync_approval_callback,
            session_id,
            context.cancel_event
        )
        
        # Phase 4: Create SQL agent
        thinking_callback("Setting up database query agent")
        
        toolkit = SQLDatabaseToolkit(db=agent.db, llm=agent.llm)
        
        manual_prompt = build_manual_context(
            context.original_question,
            context.base_sql,
            context.mod_chain.summary(),
            question
        )

        # Create the agent
        agent_executor = create_sql_agent(
            llm=agent.llm,
            toolkit=toolkit,
            agent_type="openai-functions",
            verbose=bool(os.getenv("AGENT_VERBOSE", "false").lower() == "true"),
            agent_executor_kwargs={
                "callbacks": [callback_handler],
                "handle_parsing_errors": True
            },
            system_message=manual_prompt,
            max_iterations=int(os.getenv("AGENT_MAX_ITERATIONS", "5")),
        )
        
        #print(f"AGENT EXECUTOR CREATED: {agent_executor}")
        #print(f"AGENT CALLBACKS: {agent_executor.callbacks}")

        # Phase 5: Execute the agent
        thinking_callback("Executing analysis with human oversight")
        
        try:
            result = agent_executor.invoke({"input": question})
        except UserCancelledError:
            thinking_callback("Analysis cancelled by user request")
            return {
            "answer": "Analysis was cancelled",
            "sql_query": None,
            "db_rows": None,
            "db_columns": [],
            "cancelled": True
            }

        except TypeError as e:
            if "multiple values for keyword argument" in str(e):
                logger.error(f"Parameter conflict in agent creation: {e}")
                thinking_callback("Fixing agent configuration...")
                
                agent_executor = create_sql_agent(
                    llm=agent.llm,
                    toolkit=toolkit,
                    agent_type="openai-functions",
                    verbose=False,
                    agent_executor_kwargs={"callbacks": [callback_handler]},
                    max_iterations=3
                )
                result = agent_executor.invoke({"input": question})
            else:
                raise e
        except Exception as e:
            error_msg = str(e).lower()

            if "cancelled by user" in str(e):
                thinking_callback("Analysis cancelled by user request")
                return {
                    "answer": "Analysis was cancelled",
                    "sql_query": approval_result.get("sql_query"),
                    "db_rows": None,
                    "db_columns": [],  # ADD THIS
                    "cancelled": True,
                    "error": None
                }

            elif any(keyword in error_msg for keyword in ["forbidden", "security", "not allowed"]):
                thinking_callback(f"❌ Security validation failed: {str(e)}")
                return {
                    "answer": f"Query blocked for security reasons: {str(e)}",
                    "sql_query": None,
                    "db_rows": None,
                    "db_columns": [],  # ADD THIS
                    "cancelled": False,
                    "error": str(e),
                    "error_type": "security"
                }

            elif "timeout" in error_msg:
                thinking_callback("❌ Analysis timed out - please try a simpler question")
                return {
                    "answer": "Analysis timed out. Please try asking a more specific question.",
                    "sql_query": None,
                    "db_rows": None,
                    "db_columns": [],  # ADD THIS
                    "cancelled": False,
                    "error": str(e),
                    "error_type": "timeout"
                }

            else:
                thinking_callback(f"❌ Unexpected error: {str(e)}")
                return {
                    "answer": f"I encountered an unexpected error: {str(e)}",
                    "sql_query": None,
                    "db_rows": None,
                    "db_columns": [],  # ADD THIS
                    "cancelled": False,
                    "error": str(e),
                    "error_type": "unexpected"
                }

        
        # Phase 6: Process results
        sql_query = None
        db_rows = None
        db_columns = None  # ADD THIS LINE - Initialize db_columns

        if callback_handler.sql_queries and callback_handler.query_executed:
            latest_query_info = callback_handler.sql_queries[-1]
            sql_query = latest_query_info["query"]
            thinking_callback("Processing query results")
            try:
                db_rows, db_columns = agent.db_manager.execute_query(sql_query)
                if len(db_rows) > 0:
                    thinking_callback(f"Successfully retrieved {len(db_rows)} rows of data")
                else:
                    thinking_callback("Query executed successfully - processing results")
                
            except Exception as e:
                thinking_callback(f"Error retrieving results: {str(e)}")
                logger.error(f"Error executing final query: {e}")
                # Ensure db_columns is still set even on error
                if db_columns is None:
                    db_columns = []

        elif callback_handler.sql_queries and not callback_handler.query_executed:
            thinking_callback("Warning: Query was generated but not executed")
            return {
                "answer": "Query was generated but execution was interrupted",
                "sql_query": callback_handler.sql_queries[-1]["query"],
                "db_rows": None,
                "db_columns": [],  # ADD THIS
                "cancelled": True,
                "error": "Execution interrupted"
            }

        if sql_query:                         
            if context.base_sql is None:
                context.base_sql = sql_query
            context.mod_chain.add(question)

        return {
            "answer": result.get("output", "No answer generated"),
            "sql_query": sql_query,
            "db_rows": db_rows,
            "db_columns": db_columns or [],  # ADD fallback empty list
            "analysis": analysis_result,
            "thinking_steps": callback_handler.thinking_steps,
            "cancelled": False,
            "metadata": {
                "complexity": analysis_result["complexity"],
                "visualization_recommended": analysis_result["visualization_recommended"],
                "tables_used": analysis_result["tables_needed"],
                "query_count": len(callback_handler.sql_queries),
                "query_approved": len(callback_handler.sql_queries) > 0 and callback_handler.sql_queries[-1]["approved"],
                "has_conversation_context": analysis_result.get("has_context", False),
                "related_to_previous": analysis_result.get("related_to_previous", False),
                "session_id": session_id
            }
        }

        
    except Exception as e:
        if "cancelled by user" in str(e):
            thinking_callback("Analysis cancelled by user request")
            return {
                "answer": "Analysis was cancelled",
                "sql_query": None,
                "db_rows": None,
                "db_columns": [],  # ADD THIS
                "cancelled": True,
                "error": str(e)
            }
        else:
            thinking_callback(f"Error during analysis: {str(e)}")
            logger.error(f"Error in get_agent_with_human_approval: {e}")
            return {
                "answer": f"Sorry, I encountered an error: {str(e)}",
                "sql_query": None,
                "db_rows": None,
                "db_columns": [],  # ADD THIS
                "cancelled": False,
                "error": str(e)
            }


def execute_approved_sql(sql_query: str) -> List:
    """Execute a pre-approved SQL query"""
    try:
        db_manager = DatabaseManager()
        return db_manager.execute_query(sql_query)
    except Exception as e:
        logger.error(f"Error executing approved SQL: {e}")
        raise

# Legacy function for backward compatibility
def get_agent(question: str):
    """
    Legacy function - use get_agent_with_human_approval for new implementations
    """
    logger.warning("Using legacy get_agent function - consider upgrading to get_agent_with_human_approval")
    
    try:
        agent = IntelligentSQLAgent()
        
        toolkit = SQLDatabaseToolkit(db=agent.db, llm=agent.llm)
        handler = SQLHandler()
        
        agent_executor = create_sql_agent(
            llm=agent.llm,
            toolkit=toolkit,
            agent_type="openai-functions",
            verbose=True,
            agent_executor_kwargs={"callbacks": [handler]}
        )
        
        result = agent_executor.invoke({"input": question})
        sql_query = handler.sql_queries[-1]["query"] if handler.sql_queries else None
        db_rows = None

        if sql_query:
            try:
                db_rows, db_columns = agent.db_manager.execute_query(sql_query)
            except Exception as e:
                logger.error(f"SQL execution failed: {e}")
        
        return result["output"], sql_query, db_rows, db_columns
        
    except Exception as e:
        logger.error(f"Error in legacy get_agent: {e}")
        return f"Error: {str(e)}", None, None