from langchain.callbacks.base import BaseCallbackHandler
import re
import ast

class SQLHandler(BaseCallbackHandler):
    def __init__(self):
        self.sql_queries = []
        self.db_rows = None

    def on_agent_action(self, action, **kwargs):
        if action.tool in ("sql_db_query", "QuerySQLDatabaseTool"):
            # Store the actual SQL query
            if isinstance(action.tool_input, dict):
                query = action.tool_input.get("query", "")
            else:
                query = str(action.tool_input)
            
            self.sql_queries.append({"query": query})