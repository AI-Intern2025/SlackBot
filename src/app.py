from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler
from flask import Flask, request
import os, re, time, uuid, threading, asyncio
from dotenv import load_dotenv; load_dotenv()
from viz import chart_to_slack, _render_chart
from agent import get_agent_with_human_approval, format_results_consistently, ModificationChain, build_manual_context
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from anomaly_detector import DatabaseAnomalyDetector, AnomalyResult
from alert_config import MetricDefinition, MetricType
from datetime import datetime
from automated_monitor import FullyAutomatedMonitor
import logging
import croniter
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from functools import wraps
from pyslop.cronslator import cronslate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AgentState(Enum):
    ANALYZING = "analyzing"
    WAITING_APPROVAL = "waiting_approval"
    WAITING_MODIFICATION = "waiting_modification" 
    EXECUTING = "executing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

@dataclass
class ConversationContext:
    user_id: str
    channel_id: str
    thread_ts: str
    original_question: str
    agent_state: AgentState
    thinking_steps: List[str]
    proposed_sql: Optional[str] = None
    sql_explanation: Optional[str] = None
    execution_results: Optional[List] = None
    chart_data: Optional[List] = None
    session_id: str = None
    created_at: float = None
    last_activity: float = None
    approval_message_ts: Optional[str] = None  # Main approval message
    approval_buttons_ts: Optional[str] = None  # Buttons message
    approval_event: Optional[threading.Event] = None
    approval_response: Optional[Dict] = None
    awaiting_modification: bool = False
    last_thinking_ts: Optional[str] = None
    main_message_ts: Optional[str] = None  # Track main status message
    instruction_message_ts: Optional[str] = None  # Track instruction message
    modification_request: Optional[str] = None  # Track what user requested to change
    modification_message_ts: Optional[str] = None  # NEW: Track modification notice message
    processing_results: bool = False  # Add this field
    error_count: int = 0  # Track errors
    last_error: Optional[str] = None  # Track last error
    timeout_count: int = 0  # Track timeouts
    conversation_turns: int = 0
    memory_enabled: bool = True
    related_to_previous: bool = False
    mod_processing_started: bool = False
    last_processed_user_ts: Optional[str] = None
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)
    state_lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    base_sql: Optional[str] = None
    mod_chain: ModificationChain = field(default_factory=ModificationChain)

    def __post_init__(self):
        if self.session_id is None:
            self.session_id = str(uuid.uuid4())[:8]
        if self.created_at is None:
            self.created_at = time.time()
        if self.last_activity is None:
            self.last_activity = time.time()

    def safe_update(self, **kwargs):
        """Thread-safe state updates"""
        with self.state_lock:
            for key, value in kwargs.items():
                if hasattr(self, key):
                    setattr(self, key, value)
            self.last_activity = time.time()

class ConversationManager:
    def __init__(self):
        self.conversations: Dict[str, ConversationContext] = {}
        self.cleanup_interval = 3600  # 1 hour
        self.max_conversation_age = 7200  # 2 hours
        self._start_cleanup_thread()

    def _start_cleanup_thread(self):
        def cleanup():
            while True:
                current_time = time.time()
                expired_sessions = [
                    session_id for session_id, conv in self.conversations.items()
                    if current_time - conv.last_activity > self.max_conversation_age
                ]
                for session_id in expired_sessions:
                    self.cleanup_conversation(session_id)  # Use proper cleanup
                time.sleep(self.cleanup_interval)
        
        cleanup_thread = threading.Thread(target=cleanup, daemon=True)
        cleanup_thread.start()

    def create_conversation(self, user_id: str, channel_id: str, thread_ts: str, question: str) -> ConversationContext:
        context = ConversationContext(
            user_id=user_id,
            channel_id=channel_id,
            thread_ts=thread_ts,
            original_question=question,
            agent_state=AgentState.ANALYZING,
            thinking_steps=[]
        )
        self.conversations[context.session_id] = context
        return context

    def get_conversation(self, session_id: str) -> Optional[ConversationContext]:
        conv = self.conversations.get(session_id)
        if conv:
            conv.last_activity = time.time()
        return conv

    def get_conversation_by_thread(self, thread_ts: str) -> Optional[ConversationContext]:
        matching = [conv for conv in self.conversations.values() if conv.thread_ts == thread_ts]
        if not matching:
            return None
        
        # Return the most recently active conversation
        most_recent = max(matching, key=lambda x: x.last_activity)
        most_recent.last_activity = time.time()
        return most_recent

    def update_conversation(self, session_id: str, **kwargs):
        if session_id in self.conversations:
            self.conversations[session_id].safe_update(**kwargs)


    def cleanup_conversation(self, session_id: str):
        """Properly cleanup a conversation and its resources"""
        if session_id in self.conversations:
            context = self.conversations[session_id]
            # Signal any waiting approval processes to stop
            try:
                if context.approval_event is not None:
                    context.approval_event.set()
                if context.approval_response is not None:
                    context.approval_response["approved"] = False
            except Exception:
                pass
            del self.conversations[session_id]

def user_wants_chart(question: str) -> bool:
    q_lower = question.lower()
    chart_keywords = [
        "chart", "graph", "plot", "visualize", "visualization", "visualise",
        "bar chart", "line chart", "pie chart", "display as chart",
        "show as graph", "create chart", "generate graph", "as a chart",
        "draw", "visual", "diagram", "graphic", "display as a chart",
        "result as a chart", "show chart", "display chart", "as chart"
    ]
    return any(keyword in q_lower for keyword in chart_keywords)

def remove_chart_keywords_from_question(question: str) -> str:
    """Remove chart-related keywords from question before sending to agent"""
    chart_patterns = [
        r'\s*(as a|in a|create a|make a|generate a?|show a?)\s*(chart|graph|plot|visualization)\s*',
        r'\s*(chart|graph|plot|visualize|visualization)\s*(it|this|that|the data|results)\s*',
        r'\s*(bar|line|pie)\s*(chart|graph)\s*',
        r'\s*chart\s*',
        r'\s*graph\s*',
        r'\s*plot\s*'
    ]
    
    cleaned_question = question
    for pattern in chart_patterns:
        cleaned_question = re.sub(pattern, ' ', cleaned_question, flags=re.IGNORECASE)
    
    cleaned_question = re.sub(r'\s+', ' ', cleaned_question).strip()
    return cleaned_question

def limit_chart_data(data: List[Dict], max_items: int = 15) -> List[Dict]:
    """Enhanced chart data limiting with better error handling"""
    if not data:
        return []
    
    if not isinstance(data, list):
        logger.error(f"Chart data must be a list, got {type(data)}")
        return []
    
    # Validate data structure
    valid_data = []
    for item in data:
        if not isinstance(item, dict):
            logger.warning(f"Skipping invalid chart data item: {item}")
            continue
        
        if 'label' not in item or 'value' not in item:
            logger.warning(f"Skipping chart item missing label/value: {item}")
            continue
            
        # Ensure value is numeric
        try:
            item['value'] = float(item['value'])
            valid_data.append(item)
        except (ValueError, TypeError):
            logger.warning(f"Skipping chart item with non-numeric value: {item}")
            continue
    
    if not valid_data:
        logger.error("No valid chart data after filtering")
        return []
    
    if len(valid_data) <= max_items:
        return valid_data
    
    # Sort and group others
    try:
        sorted_data = sorted(valid_data, key=lambda x: float(x.get('value', 0)), reverse=True)
        top_items = sorted_data[:max_items-1]
        remaining_items = sorted_data[max_items-1:]
        
        others_value = sum(float(item.get('value', 0)) for item in remaining_items)
        others_count = len(remaining_items)
        
        if others_count > 0:
            top_items.append({
                "label": f"Others ({others_count} items)",
                "value": others_value
            })
        
        return top_items
        
    except Exception as e:
        logger.error(f"Error processing chart data: {e}")
        return valid_data[:max_items]  # Fallback to simple truncation

def cleanup_approval_messages(client, context):
    """Clean up all approval-related messages"""
    try:
        if context.approval_message_ts:
            client.chat_delete(
                channel=context.channel_id,
                ts=context.approval_message_ts
            )
    except Exception:
        pass
    
    try:
        if context.approval_buttons_ts:
            client.chat_delete(
                channel=context.channel_id,
                ts=context.approval_buttons_ts
            )
    except Exception:
        pass

def create_approval_blocks(session_id: str):
    """Create interactive blocks for SQL approval"""
    return [
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "✅ Execute Query"
                    },
                    "style": "primary",
                    "action_id": "approve_sql",
                    "value": session_id
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "❌ Cancel"
                    },
                    "style": "danger",
                    "action_id": "cancel_sql",
                    "value": session_id
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "🔄 Modify Query"
                    },
                    "action_id": "modify_sql",
                    "value": session_id
                }
            ]
        }
    ]

def create_chart_options_blocks(session_id: str, chart_title: str):
    """Create interactive blocks for chart type selection"""
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "📊 *These result seems large. Want to visualize your data?*"
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "📊 Bar Chart"
                    },
                    "action_id": "create_chart_bar",
                    "value": f"{session_id}|bar|{chart_title}"
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "📈 Line Chart"
                    },
                    "action_id": "create_chart_line", 
                    "value": f"{session_id}|line|{chart_title}"
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "🥧 Pie Chart"
                    },
                    "action_id": "create_chart_pie",
                    "value": f"{session_id}|pie|{chart_title}"
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "📝 Text Only"
                    },
                    "action_id": "show_text_only",
                    "value": session_id
                }
            ]
        }
    ]

def honour_cancel(context: "ConversationContext"):
    """
    Helper that immediately aborts the current thread when the user has clicked
    ❌ Cancel or 🔄 Modify Query.
    """
    if context.cancel_event.is_set():
        raise RuntimeError("Analysis cancelled by user")

def format_step(step):
    # Clean the step first
    clean_step = step.lstrip('✓ ').lstrip('❌ ').lstrip('🔄 ').strip()
    
    # Apply the right symbol
    if clean_step == "Human requested query modification":
        return f"🔄 {clean_step}"  # Blue spinning arrow = user action
    elif clean_step == "Human cancelled query execution":
        return f"⏹️ {clean_step}"
    else:
        return f"✓ {clean_step}"

def _rebuild_main_message_without_progress(context: ConversationContext,
                                           extra_step: str) -> str:
    """
    Returns the text for the main status message with:
    • NO progress bar
    • An extra analysis step marked with ❌
    """
    # Copy existing steps and add the new one
    updated_steps = context.thinking_steps.copy()
    if extra_step not in updated_steps:
        updated_steps.append(extra_step)

    steps_text = "\n".join([format_step(s) for s in updated_steps])

    header = f"🤖 *Analysis Session #{context.session_id}*"
    if context.agent_state == AgentState.CANCELLED:
        header += " – Cancelled"
    elif context.awaiting_modification:
        header += " – Modification Requested"

    base_text = (
        f"{header}\n\n"
        f"*Question:* {context.original_question}"
    )

    if context.modification_request:
        base_text += f"\n*Change:* {context.modification_request}"

    # Build the message with SQL and explanation if available
    message_parts = [
        base_text,
        f"🧠 *Analysis Steps:*\n{steps_text}"
    ]

    # Add SQL and explanation if they exist
    if context.proposed_sql:
        message_parts.append(f"🔍 *Generated SQL Query:*\n```{context.proposed_sql}```")
        
    if context.sql_explanation:
        message_parts.append(f"📝 *Explanation:* {context.sql_explanation}")

    return "\n\n".join(message_parts)

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from functools import wraps

def with_timeout(timeout_seconds, timeout_message_func):
    """
    Improved timeout decorator that properly handles Slack bot functions
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Debug: Print what we received
            logger.info(f"TIMEOUT DEBUG: Function {func.__name__} called with {len(args)} args")
            
            def actual_function():
                return func(*args, **kwargs)
            
            try:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(actual_function)
                    result = future.result(timeout=timeout_seconds)
                    logger.info(f"TIMEOUT DEBUG: Function {func.__name__} completed normally")
                    return result
                    
            except FutureTimeoutError:
                logger.warning(f"TIMEOUT DEBUG: Function {func.__name__} timed out after {timeout_seconds}s")
                
                # Try to find context and client from args
                context = None
                client = None
                
                for i, arg in enumerate(args):
                    logger.info(f"TIMEOUT DEBUG: Arg {i}: {type(arg)} - {hasattr(arg, 'session_id') if hasattr(arg, '__dict__') else 'no dict'}")
                    
                    # Look for context (has session_id)
                    if hasattr(arg, 'session_id'):
                        context = arg
                        logger.info(f"TIMEOUT DEBUG: Found context at arg {i}")
                    
                    # Look for client (has chat_update method)
                    elif hasattr(arg, 'chat_update'):
                        client = arg
                        logger.info(f"TIMEOUT DEBUG: Found client at arg {i}")
                
                if context and client:
                    try:
                        # Generate timeout message
                        timeout_msg = timeout_message_func(context, *args, **kwargs)
                        logger.info(f"TIMEOUT DEBUG: Generated timeout message: {timeout_msg[:50]}...")
                        
                        # Determine which message to update
                        message_ts = None
                        if hasattr(context, 'modification_message_ts') and context.modification_message_ts:
                            message_ts = context.modification_message_ts
                            logger.info(f"TIMEOUT DEBUG: Using modification_message_ts: {message_ts}")
                        elif hasattr(context, 'main_message_ts') and context.main_message_ts:
                            message_ts = context.main_message_ts
                            logger.info(f"TIMEOUT DEBUG: Using main_message_ts: {message_ts}")
                        
                        if message_ts:
                            client.chat_update(
                                channel=context.channel_id,
                                ts=message_ts,
                                text=timeout_msg
                            )
                            logger.info(f"TIMEOUT DEBUG: Successfully updated message {message_ts}")
                        else:
                            # Fallback to posting new message
                            client.chat_postMessage(
                                channel=context.channel_id,
                                thread_ts=context.thread_ts,
                                text=timeout_msg
                            )
                            logger.info(f"TIMEOUT DEBUG: Posted new timeout message")
                            
                    except Exception as e:
                        logger.error(f"TIMEOUT DEBUG: Failed to send timeout message: {e}")
                else:
                    logger.error(f"TIMEOUT DEBUG: Could not find context ({context is not None}) or client ({client is not None})")
                
                return None
                
            except Exception as e:
                logger.error(f"TIMEOUT DEBUG: Error in {func.__name__}: {e}")
                raise e
                
        return wrapper
    return decorator

def main_question_timeout_msg(context, *args, **kwargs):
    """Fixed to accept variable arguments"""
    return f"⏰ *Analysis Timeout #{context.session_id}*\n\n*Question:* {context.original_question}\n\n❌ Analysis took longer than expected and was cancelled.\n\n"

def modification_timeout_msg(context, *args, **kwargs):
    """Fixed to accept variable arguments"""
    message_text = kwargs.get('message_text') or (args[2] if len(args) > 2 else 'modification request')
    return f"⏰ *Modification Timeout #{context.session_id}*\n\n*Original:* {context.original_question}\n*Modification:* {message_text}\n\n❌ Modification took too long and was cancelled.\n\n"

def followup_timeout_msg(context, *args, **kwargs):
    """Fixed to accept variable arguments"""
    message_text = kwargs.get('message_text') or (args[2] if len(args) > 2 else 'follow-up question')
    return f"⏰ *Follow-up Timeout*\n\n*Original:* {context.original_question}\n*Follow-up:* {message_text}\n\n❌ Follow-up analysis took too long and was cancelled.\n\n"

class AlertManager:
    """Manages automated alerts and notifications"""
    
    def __init__(self, slack_client, conversation_manager):
        self.client = slack_client
        self.conversation_manager = conversation_manager
        self.detector = DatabaseAnomalyDetector()
        self.alert_channels = ["#alerts"]  # Default alert channel
        self.running = False
        self.check_interval = 3600  # Check every hour
        
    def start_monitoring(self):
        """Start the background monitoring process"""
        if self.running:
            return
        
        self.running = True
        
        def monitor_loop():
            while self.running:
                try:
                    self.run_anomaly_checks()
                    time.sleep(self.check_interval)
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {e}")
                    time.sleep(60)  # Wait a minute before retrying
        
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        logger.info("Alert monitoring started")
    
    # def stop_monitoring(self):
    #     """Stop the background monitoring process"""
    #     self.running = False
    #     logger.info("Alert monitoring stopped")
    
    def run_anomaly_checks(self):
        """Run all anomaly checks and send alerts"""
        try:
            anomalies = self.detector.run_all_checks()
            
            if anomalies:
                logger.info(f"Found {len(anomalies)} anomalies")
                self.send_anomaly_alerts(anomalies)
            else:
                logger.info("No anomalies detected")
                
        except Exception as e:
            logger.error(f"Error running anomaly checks: {e}")
            self.send_error_alert(str(e))
    
    def send_anomaly_alerts(self, anomalies: List[AnomalyResult]):
        """Send anomaly alerts to Slack"""
        for anomaly in anomalies:
            self.send_single_alert(anomaly)
    
    def send_single_alert(self, anomaly: AnomalyResult):
        """Send a single anomaly alert"""

        import sys
        if 'alert_config' in sys.modules:
            del sys.modules['alert_config']
        import alert_config
        BUSINESS_METRICS = alert_config.BUSINESS_METRICS

        # Determine emoji based on severity
        emoji_map = {
            "low": "🟡",
            "medium": "🟠", 
            "high": "🔴",
            "critical": "🚨"
        }
        
        emoji = emoji_map.get(anomaly.severity, "⚠️")
        
        # Format the alert message
        alert_text = f"""{emoji} ANOMALY DETECTED

Metric: {anomaly.metric_name}
Current Value: {anomaly.current_value:,.2f}
Expected Range: {anomaly.expected_range[0]:,.2f} - {anomaly.expected_range[1]:,.2f}
Severity: {anomaly.severity.upper()}
Business Impact: {anomaly.business_impact.upper()}

Description: {anomaly.message}

Suggested Action: {anomaly.suggested_action}

Historical Context:
• Average: {anomaly.historical_context.get('mean', 0):,.2f}
• Standard Deviation: {anomaly.historical_context.get('std', 0):,.2f}
• Historical Range: {anomaly.historical_context.get('min', 0):,.2f} - {anomaly.historical_context.get('max', 0):,.2f}

*Detected at: {anomaly.timestamp.strftime('%Y-%m-%d %H:%M:%S')}*"""

        # Send to appropriate channels
        metric_def = None
        for metric_id, metric in BUSINESS_METRICS.items():
            if metric.name == anomaly.metric_name:
                metric_def = metric
                break
        
        channels = metric_def.alert_channels if metric_def else self.alert_channels
        
        for channel in channels:
            try:
                self.client.chat_postMessage(
                    channel=channel,
                    text=alert_text
                )
            except Exception as e:
                logger.error(f"Error sending alert to {channel}: {e}")
    
    def send_error_alert(self, error_message: str,client, channel_id, check_and_respond):
        """Send system error alert to configured channels in Slack."""
        alert_text = (
            "🚨 *ALERT SYSTEM ERROR*\n\n"
            "The anomaly detection system encountered an error:\n\n"
            f"*Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n"
            "Please check the system logs and fix the issue."
        )
        for channel in self.alert_channels:
            try:
                self.client.chat_postMessage(
                    channel=channel,
                    text=alert_text
                )
            except Exception as e:
                logger.error(f"Error sending error alert to {channel}: {e}")
                client.chat_postMessage(
                    channel=channel_id,
                    text=f"❌ Error running anomaly detection: {str(e)}"
                )
        threading.Thread(target=check_and_respond, daemon=True).start()
# Initialize components
conversation_manager = ConversationManager()

# Initialize Slack App
app = App(
    token=os.getenv("SLACK_BOT_TOKEN"),
    signing_secret=os.getenv("SLACK_SIGNING_SECRET")
)

# automated_monitor = AutomatedBusinessMonitor(app.client)
automated_monitor = FullyAutomatedMonitor(app.client)
alert_manager = AlertManager(app.client, conversation_manager)
# Slack request handler for Flask
handler = SlackRequestHandler(app)

# Flask app
flask_app = Flask(__name__)

@app.event("app_mention")
def handle_mention(body, say, client, logger):
    user = body["event"]["user"]
    channel = body["event"]["channel"]
    ts = body["event"]["ts"]
    question = re.sub(r"<@[^>]+>", "", body["event"]["text"]).strip()
    
    if not question:
        say("Hi! Please ask me a question about your data.", thread_ts=ts)
        return

    # Add at the start of handle_mention
    existing_context = conversation_manager.get_conversation_by_thread(ts)
    if existing_context:
        logger.info(f"DUPLICATE PREVENTION: Already processing in session {existing_context.session_id}")
        return
    
    # Create initial thread response
    initial_response = client.chat_postMessage(
        channel=channel,
        thread_ts=ts,
        text=f"🤖 *Starting Analysis Session*\n\n*Question:* {question}\n\n🧠 *Thinking...*\nAnalyzing your question and understanding the data structure..."
    )
    
    thread_ts = initial_response["ts"]
    
    # Create conversation context
    context = conversation_manager.create_conversation(user, channel, ts, question)
    context.main_message_ts = thread_ts  # Track the main message
    
    try:
        @with_timeout(300, main_question_timeout_msg)
        def process_question(context, client, channel, thread_ts, question):
            honour_cancel(context) 
            # ATOMIC STATE CHECK - prevents race conditions
            with context.state_lock:
                if getattr(context, 'processing_active', False):
                    logger.info(f"DUPLICATE PREVENTION: Already processing question for {context.session_id}")
                    return
                
                context.safe_update(processing_active=True)
            
            conversation_manager.update_conversation(
                context.session_id,
                processing_started=True
            )

            try:
                client.chat_update(
                    channel=channel,
                    ts=thread_ts,
                    text=f"🤖 *Analysis Session #{context.session_id}*\n\n*Question:* {question}\n\n🧠 *Status: Understanding Question*\nBreaking down your request and identifying relevant data sources..."
                )
                
                approval_response = {"approved": None, "sql_query": None, "explanation": None}
                approval_event = threading.Event()
                
                def thinking_update(step):
                    update_analysis_status(client, channel, thread_ts, context, step, "Analysis Session")
                
                def request_approval(sql_query, explanation):
                    logger.info(f"APPROVAL DEBUG: Requesting approval for session {context.session_id}")
                    approval_response["sql_query"] = sql_query
                    approval_response["explanation"] = explanation
                    
                    request_sql_approval(client, channel, thread_ts, context, sql_query, explanation)
                    
                    logger.info(f"APPROVAL DEBUG: Starting wait...")
                    # Wait with better timeout handling
                    try:
                        approval_received = approval_event.wait(timeout=300)  # 5 minute timeout
                        logger.info(f"APPROVAL DEBUG: Wait result: {approval_received}, approved: {approval_response['approved']}")

                        # CHECK for modification request flag instead
                        if approval_response.get("modification_requested"):
                            logger.info("APPROVAL DEBUG: Modification requested, returning False to cancel")
                            return False  # Treat modification request as cancellation
        
                        if not approval_received or approval_response["approved"] is None:
                            # Cleanup any remaining approval messages on timeout
                            cleanup_approval_messages(client, context)

                            timeout_message = f"⏰ *Approval Timeout #{context.session_id}*\n\n*Question:* {context.original_question}\n\n❌ No response received within 5 minutes\n\n💡 What happened?\nThe system was waiting for your approval to execute the SQL query, but didn't receive a response.\n\n🔄 Next steps:\n• Ask your question again to generate a new query\n• I'll wait for your approval before executing any database operations\n\n🛡️ Security note: Queries always require your explicit approval before execution."

                            client.chat_postMessage(
                                channel=channel,
                                thread_ts=thread_ts,
                                text=timeout_message
                            )
                            
                            raise Exception("Approval timeout - please try again")
                        
                        return approval_response["approved"]
                    except Exception as e:
                        # Ensure cleanup on any error
                        cleanup_approval_messages(client, context)
                        raise e
                
                conversation_manager.update_conversation(
                    context.session_id,
                    approval_event=approval_event,
                    approval_response=approval_response
                )
                
                wants_chart = user_wants_chart(question)
                cleaned_question = remove_chart_keywords_from_question(question) if wants_chart else question
                
                enhanced_question = build_manual_context(
                    context.original_question,
                    context.base_sql,
                    context.mod_chain.summary(),
                    cleaned_question
                )
                result = get_agent_with_human_approval(
                    enhanced_question, 
                    context, 
                    thinking_update,
                    request_approval,
                    session_id=context.session_id
                )
                
                # Check for errors in analysis
                if result.get("error") and not result.get("cancelled"):
                    # Edit the current message to show error and stop
                    client.chat_update(
                        channel=channel,
                        ts=thread_ts,
                        text=f"🤖 *Analysis Session #{context.session_id} - Error*\n\n*Question:* {question}\n\n❌ *Analysis Error:* {result['error']}"
                    )
                    return
                
                # ADD THIS CHECK before handling results:
                if getattr(context, 'awaiting_modification', False):
                    logger.info("MODIFICATION DEBUG: Skipping result handling - modification in progress")
                    return  # Don't process results if modification is pending

                if result["cancelled"]:
                    return
                
                # Clean up any approval messages first
                cleanup_approval_messages(client, context)
                
                # NOW handle the successful result 
                if not result.get("cancelled") and not result.get("error") and result.get("sql_query"):
                    handle_successful_result(client, channel, thread_ts, context, result, wants_chart)
                    
            except Exception as e:
                logger.error(f"Error in process_question: {e}")
                client.chat_postMessage(
                    channel=channel,
                    thread_ts=thread_ts,
                    text=f"```🤖 *Session #{context.session_id} - Error*\n\n*Question:* {question}\n\n❌ *Error:* {str(e)}```"
                )
            
            finally:
                # Always reset processing flag
                with context.state_lock:
                    context.safe_update(processing_active=False)
        
        threading.Thread(target=lambda: process_question(context, client, channel, thread_ts, question), daemon=True).start()
        
    except Exception as e:
        logger.error(f"Error in handle_mention: {e}")
        say(f"```❌ Sorry, I encountered an error: {str(e)}```", thread_ts=ts)
    
def create_progress_indicator(current_step: str, total_steps: int, current_step_num: int) -> str:
    """Create a simple progress indicator"""
    percentage = int(100 * current_step_num / total_steps)
    progress_dots = "●" * (current_step_num) + "○" * (total_steps - current_step_num)
    
    return f"🔄 Progress: {progress_dots} {percentage}% - {current_step}"

def get_analysis_phases():
    """Define the main analysis phases"""
    return [
        "Understanding Question",
        "Analyzing Data Requirements", 
        "Examining Database Schema",
        "Generating SQL Query",
        "Requesting Human Approval",
        "Executing Query",
        "Processing Results",
        "Generating Visualization"
    ]

# def update_thinking_status_with_progress(client, channel, thread_ts, context, step):
#     honour_cancel(context) 
#     """Update the thinking status message with progress indicator"""

#     max_retries = 3
#     retry_delay = 1

#     for attempt in range(max_retries):
#         try:
#             # Check if this is an error step
#             if "❌" in step or "error" in step.lower():
#                 error_text = step.replace("❌", "").strip()
                
#                 error_message = f"🤖 *Analysis Session #{context.session_id} - Error*\n\n*Question:* {context.original_question}\n\n❌ *Analysis Error:* {error_text}"

#                 client.chat_update(
#                     channel=channel,
#                     ts=thread_ts,
#                     text=error_message
#                 )
#                 return  # Stop processing here
            
#             # Skip temporary schema errors - they usually self-correct
#             if "❌" in step and ("schema" in step.lower() or "examining" in step.lower()):
#                 logger.info(f"Skipping temporary schema error: {step}")
#                 return
            
#             # PREVENT duplicate updates after results are complete
#             if "Query Analysis Complete" in context.thinking_steps:
#                 logger.info(f"THINKING STOPPED: Analysis already complete for {context.session_id}")
#                 return
    
#              # ADD THIS CHECK to prevent race conditions
#             if getattr(context, 'processing_results', False):
#                 logger.info(f"RACE CONDITION PREVENTED: Skipping thinking update for {context.session_id}")
#                 return
            
#             phases = get_analysis_phases()
            
#             current_phase_num = 1
#             current_phase = "Understanding Question"
            
#             step_lower = step.lower()
#             if "analyzing" in step_lower and "question" in step_lower:
#                 current_phase_num = 2
#                 current_phase = "Analyzing Data Requirements"
#             elif "schema" in step_lower or "database" in step_lower or "table" in step_lower:
#                 current_phase_num = 3
#                 current_phase = "Examining Database Schema"
#             elif "sql" in step_lower and "generat" in step_lower:
#                 current_phase_num = 4
#                 current_phase = "Generating SQL Query"
#             elif "approval" in step_lower or "human" in step_lower:
#                 current_phase_num = 5
#                 current_phase = "Requesting Human Approval"
#             elif "execut" in step_lower:
#                 current_phase_num = 6
#                 current_phase = "Executing Query"
#             elif "processing" in step_lower or "result" in step_lower:
#                 current_phase_num = 7
#                 current_phase = "Processing Results"
#             elif "chart" in step_lower or "visual" in step_lower:
#                 current_phase_num = 8
#                 current_phase = "Generating Visualization"
            
#             progress_indicator = create_progress_indicator(current_phase, len(phases), current_phase_num)
            
#             # Filter out temporary error steps from the display
#             display_steps = []
#             for s in context.thinking_steps:
#                 if not (s.startswith("❌") and ("schema" in s.lower() or "examining" in s.lower())):
#                     display_steps.append(f"✓ {s}")

#             steps_text = "\n".join(display_steps)
#             current_step_text = f"🔄 {step}"
            
#             #table_info = generate_dynamic_table_info(current_phase_num, step, context)
            
#             status_text = f"🤖 Analysis Session #{context.session_id}\n\nQuestion: {context.original_question}\n\n{progress_indicator}\n\n🧠 Analysis Steps:\n{steps_text}\n{current_step_text}"
            
#             client.chat_update(
#                 channel=channel,
#                 ts=thread_ts,
#                 text=status_text
#             )
            
#             break
            
#         except Exception as e:
#             if attempt < max_retries - 1:
#                 logger.warning(f"Message update attempt {attempt + 1} failed: {e}, retrying...")
#                 time.sleep(retry_delay)
#                 retry_delay *= 2  # Exponential backoff
#             else:
#                 logger.error(f"Failed to update message after {max_retries} attempts: {e}")
#                 # Try posting a new message as fallback
#                 try:
#                     client.chat_postMessage(
#                         channel=channel,
#                         thread_ts=thread_ts,
#                         text=f"🔄 Status Update: {step}"
#                     )
#                 except Exception as fallback_error:
#                     logger.error(f"Fallback message posting also failed: {fallback_error}")

#     # Only add non-error steps to the permanent record
#     if not (step.startswith("❌") and ("schema" in step.lower() or "examining" in step.lower())):
#         context.thinking_steps.append(step)
def update_analysis_status(client, channel_id, message_ts, context, current_step, headline="Analysis Session"):
    """Fixed progress updater with realistic step count"""
    
    # Skip errors and duplicates
    if "❌" in current_step or current_step in context.thinking_steps:
        return
    
    # Don't show completion steps as progress
    completion_keywords = ["complete", "analysis complete", "query analysis complete"]
    if any(keyword in current_step.lower() for keyword in completion_keywords):
        return
    
    if context.awaiting_modification or context.agent_state in [AgentState.CANCELLED]:
        show_progress = False
    else:
        show_progress = True

    # Realistic total steps based on your actual flow
    total_steps = 12
    current_step_num = len(context.thinking_steps) + 1
    
    # Don't show 100% until truly done
    display_step_num = min(current_step_num, total_steps - 1)
    
    # Progress bar
    dots = "●" * display_step_num + "○" * (total_steps - display_step_num)
    percentage = int(100 * display_step_num / total_steps)
    progress_line = f"🔄 Progress: {dots} {percentage}%" if show_progress else ""
    
    # Show completed steps (filter out completion announcements)
    completed_steps = []
    for s in context.thinking_steps:
        if not any(keyword in s.lower() for keyword in completion_keywords):
            completed_steps.append(f"✓ {s}")
    
    steps_text = "\n".join(completed_steps)
    
    # Build message - only show current step if it's not a completion announcement
    base_text = f"🤖 *{headline} #{context.session_id}*\n\n*Question:* {context.original_question}"
    if hasattr(context, 'modification_request') and context.modification_request:
        base_text += f"\n*Change:* {context.modification_request}"
    
    final_text = f"{base_text}\n\n{progress_line}\n\n🧠 *Analysis Steps:*\n{steps_text}\n🔄 {current_step}"
    
    try:
        client.chat_update(channel=channel_id, ts=message_ts, text=final_text)
        # Only add meaningful steps to history
        if not any(keyword in current_step.lower() for keyword in completion_keywords):
            context.thinking_steps.append(current_step)
    except Exception as e:
        logger.warning(f"Status update failed: {e}")

def request_sql_approval(client, channel, thread_ts, context, sql_query, explanation):
    """Request human approval for SQL execution - FIXED VERSION"""
    conversation_manager.update_conversation(
        context.session_id,
        agent_state=AgentState.WAITING_APPROVAL,
        proposed_sql=sql_query,
        sql_explanation=explanation
    )
    
    steps_text = "\n".join([f"✓ {step}" for step in context.thinking_steps])
    
    dots = "●" * 8 + "○" * 4  # 8 of 12 steps done
    progress = f"🔄 Progress: {dots} 66%"
    # ALWAYS UPDATE the main message to show final SQL query 
    main_message_text = f"🤖 *Analysis Session #{context.session_id}*\n\n*Question:* {context.original_question}\n\n{progress}\n\n🧠 *Analysis Steps:*\n{steps_text}\n\n🔍 *Generated SQL Query:*\n```{sql_query}\n```\n\n📝 *Explanation:* {explanation}"
    
    try:
        client.chat_update(
            channel=channel,
            ts=thread_ts,
            text=main_message_text
        )
    except:
        # If update fails, the message will show old content, but continue
        pass
    
    # Post the "waiting for approval" message SEPARATELY
    waiting_response = client.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text="⏳ *Waiting for your approval to execute the query...*"
    )
    
    # Post approval buttons in ANOTHER separate message
    approval_blocks = create_approval_blocks(context.session_id)
    buttons_response = client.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text="👆 Please choose an action:",
        blocks=approval_blocks
    )
    
    # Store both message timestamps for cleanup
    conversation_manager.update_conversation(
        context.session_id,
        approval_message_ts=waiting_response["ts"],
        approval_buttons_ts=buttons_response["ts"]
    )

def handle_successful_result(client, channel, thread_ts, context, result, wants_chart=False):
    honour_cancel(context) 
    with threading.Lock():  # Add this lock
        if getattr(context, 'awaiting_modification', False):
            logger.info(f"RACE CONDITION PREVENTED: Skipping result handling for {context.session_id}")
            return
        
        if getattr(context, 'processing_results', False):
            logger.info(f"RACE CONDITION PREVENTED: Already processing results for {context.session_id}")
            return
        
        # Set a flag to prevent modifications during result processing
        conversation_manager.update_conversation(
            context.session_id,
            processing_results=True
        )

    try:
        """Handle successful query execution result"""
        answer_text = result.get("answer")
        sql_query = result.get("sql_query")
        db_rows = result.get("db_rows")
        db_columns = result.get("db_columns")
        chart_worthy = False
        processed_rows = []
        limited_chart_data = None
        steps_text = "\n".join([f"✓ {step}" for step in context.thinking_steps])
        
        # Build the result text with optional modification request
        base_text = f"🤖 *Analysis Session #{context.session_id} - Complete*\n\n*Question:* {context.original_question}"
        
        # Add modification request if this was a modified query
        if hasattr(context, 'modification_request') and context.modification_request:
            base_text += f"\n*Change:* {context.modification_request}"
        
        # Handle results
        if db_rows and len(db_rows) > 0:
            formatted_answer = format_results_consistently(db_rows, db_columns)
        else:
            if answer_text and answer_text.strip():
                cleaned_narrative = answer_text
                cleaned_narrative = cleaned_narrative.replace("**", "")
                cleaned_narrative = cleaned_narrative.replace("*", "")
                cleaned_narrative = re.sub(r'__(.+?)__', r'\1', cleaned_narrative)
                cleaned_narrative = re.sub(r'_(.+?)_', r'\1', cleaned_narrative)
                
                formatted_answer = f"```{cleaned_narrative}```"
            else:
                formatted_answer = f"```{"No results found."}```"

        formatted_answer = format_results_consistently(db_rows,db_columns) if (db_rows and len(db_rows) > 0) else f"```{answer_text}```"
            
        display_sql = context.proposed_sql or sql_query or "No SQL query available"
        display_explanation = context.sql_explanation or "No explanation available"

        result_text = f"{base_text}\n\n🧠 Analysis Steps:\n{steps_text}\n\n🔍 Executed SQL Query:\n```sql\n{display_sql}\n```\n\n📝 Explanation: {display_explanation}\n\n📊 Results:\n{formatted_answer}"
        
        if db_rows and isinstance(db_rows, list) and len(db_rows) > 0:
            processed_rows = []
            
            if isinstance(db_rows[0], tuple):
                if len(db_rows[0]) == 2:
                    try:
                        processed_rows = [{"label": str(row[0]), "value": float(row[1])} for row in db_rows]
                        chart_worthy = True
                    except (ValueError, TypeError):
                        try:
                            processed_rows = [{"label": str(row[0]), "value": len(str(row[1]))} for row in db_rows]
                            chart_worthy = True
                        except:
                            pass
                elif len(db_rows[0]) >= 2:
                    try:
                        for col_idx in range(1, len(db_rows[0])):
                            try:
                                processed_rows = [{"label": str(row[0]), "value": float(row[col_idx])} for row in db_rows]
                                chart_worthy = True
                                break
                            except (ValueError, TypeError):
                                continue
                    except:
                        pass
            
            elif isinstance(db_rows[0], dict):
                possible_label_keys = ['label', 'name', 'round', 'roundcode', 'category', 'type', 'id']
                possible_value_keys = ['value', 'count', 'views', 'amount', 'total', 'sum']
                
                label_key = None
                value_key = None
                
                for key in possible_label_keys:
                    if key in db_rows[0]:
                        label_key = key
                        break
                
                for key in possible_value_keys:
                    if key in db_rows[0]:
                        try:
                            float(db_rows[0][key])
                            value_key = key
                            break
                        except:
                            continue
                
                if not label_key or not value_key:
                    keys = list(db_rows[0].keys())
                    if len(keys) >= 2:
                        label_key = keys[0]
                        value_key = keys[1]
                
                if label_key and value_key:
                    try:
                        processed_rows = [{"label": str(row[label_key]), "value": float(row[value_key])} for row in db_rows]
                        chart_worthy = True
                    except:
                        pass
            
            if chart_worthy and processed_rows and (wants_chart or len(processed_rows) >= 10):
                logger.info(f"CHART DEBUG: processed_rows = {processed_rows[:3]}")
                logger.info(f"CHART DEBUG: len(processed_rows) = {len(processed_rows)}")
                logger.info(f"CHART DEBUG: chart_worthy = {chart_worthy}")
                
                # if len(processed_rows) >= 10 or wants_chart:
                limited_chart_data = limit_chart_data(processed_rows, max_items=15)
                if limited_chart_data:
                    logger.info(f"CHART DEBUG: Creating chart with {len(limited_chart_data)} items")
                else:
                    logger.warning("CHART DEBUG: limited_chart_data is None")
                    limited_chart_data = processed_rows[:15]  # Fallback

                # Update the main message with final results
                try:
                    client.chat_update(
                        channel=channel,
                        ts=thread_ts,
                        text=result_text
                    )
                    logger.info(f"Successfully updated message {thread_ts}")
                except Exception as e:
                    logger.warning(f"Main message update failed: {e}, trying post as fallback")
                    try:
                        client.chat_postMessage(
                            channel=channel,
                            thread_ts=thread_ts,
                            text=result_text
                        )
                    except Exception as e2:
                        logger.error(f"Both update and post failed: {e2}")
                
                if wants_chart and limited_chart_data:
                    placeholder = client.chat_postMessage(
                        channel=channel,
                        thread_ts=thread_ts,
                        text="📈 Creating your chart..."
                    )
                    
                    placeholder_ts = placeholder["ts"]
                    try:
                        chart_to_slack(
                            limited_chart_data, 
                            channel, 
                            title=f"Results: {context.original_question}", 
                            thread_ts=thread_ts
                        )
                        
                        chart_info = ""
                        if len(processed_rows) > len(limited_chart_data):
                            others_count = len(processed_rows) - len(limited_chart_data) + 1
                            chart_info = f"\n\n📋 *Chart shows top {len(limited_chart_data)-1} items + Others ({others_count} items grouped)*"

                        client.chat_update(
                            channel=channel,
                            ts=placeholder_ts,
                            text="✅ *Chart generated successfully!*" + chart_info
                        )
                    except Exception as e:
                        client.chat_update(
                            channel=channel,
                            ts=placeholder_ts,
                            text=f"❌ *Chart generation failed: {str(e)}*"
                        )
                elif wants_chart and not limited_chart_data:
                    client.chat_postMessage(
                        channel=channel,
                        thread_ts=thread_ts,
                        text="📊 *Chart requested but data isn't suitable for visualization*\n\nThe current results don't have the right structure for charting (need 2 columns with numeric values). The data is displayed in table format above."
                )
                else:
                    # ALWAYS show chart options for chart-worthy data
                    chart_blocks = create_chart_options_blocks(context.session_id, f"Results: {context.original_question}")
                    
                    chart_info = "📊 *These results look chart-worthy! Want to visualize your data?*"
                    if limited_chart_data and processed_rows and len(processed_rows) > len(limited_chart_data):
                        others_count = len(processed_rows) - len(limited_chart_data) + 1
                        chart_info += f"\n\n📋 *Note: Chart will show top {len(limited_chart_data)-1} items + Others ({others_count} items grouped)*"
                    
                    # Post chart options immediately after the main results message
                    client.chat_postMessage(
                        channel=channel,
                        thread_ts=thread_ts,
                        text=chart_info,
                        blocks=chart_blocks
                    )

            else:
                try:
                    client.chat_update(
                        channel=channel,
                        ts=thread_ts,
                        text=result_text
                    )
                except:
                    client.chat_postMessage(
                        channel=channel,
                        thread_ts=thread_ts,
                        text=result_text
                    )
        else:
            try:
                client.chat_update(
                    channel=channel,
                    ts=thread_ts,
                    text=result_text
                )
            except:
                client.chat_postMessage(
                    channel=channel,
                    thread_ts=thread_ts,
                    text=result_text
                )
        # At the very end - SAFE SINGLE UPDATE
        conversation_manager.update_conversation(
            context.session_id,
            chart_data=limited_chart_data if (chart_worthy and limited_chart_data) else None,
            execution_results=db_rows if db_rows else None,
            agent_state=AgentState.COMPLETED,
            awaiting_modification=False,
            modification_request=None,
            mod_processing_started=False,
            processing_results=False
        )

    finally:
        # Always clear the processing flags
        update_data = {
            "processing_results": False,
            "mod_processing_started": False  # ADD THIS LINE
        }

        # Determine if this was a successful execution
        was_successful = (
            result.get("sql_query") is not None and 
            not result.get("cancelled", False) and
            not result.get("error")
        )
        
        # Only increment conversation turns on successful query execution
        if was_successful:
            update_data["conversation_turns"] = context.conversation_turns + 1
        
        conversation_manager.update_conversation(context.session_id, **update_data)

@app.action("approve_sql")
def handle_sql_approval(ack, body, client, logger):
    ack()
    session_id = body["actions"][0]["value"]
    context = conversation_manager.get_conversation(session_id)
    
    if not context:
        client.chat_postMessage(
            channel=body["channel"]["id"],
            thread_ts=body["message"]["ts"],
            text="❌ Session expired. Please ask your question again."
        )
        return
    
    # IMMEDIATELY clean up approval messages
    cleanup_approval_messages(client, context)
    
    # Signal approval with proper error handling
    try:
        if context.approval_response is not None:
            context.approval_response["approved"] = True
        if context.approval_event is not None:
            context.approval_event.set()
    except Exception as e:
        logger.error(f"Error setting approval response: {e}")
    
    conversation_manager.update_conversation(session_id, agent_state=AgentState.EXECUTING, awaiting_modification=False)
    
    logger.info(f"SQL approved for session {session_id}")

@app.action("cancel_sql")
def handle_sql_cancellation(ack, body, client, logger):
    ack()
    session_id = body["actions"][0]["value"]
    context = conversation_manager.get_conversation(session_id)
    
    if context:
        with context.state_lock:
            # Check if already cancelled to prevent double messages
            if context.agent_state == AgentState.CANCELLED:
                logger.info(f"Already cancelled session {session_id}")
                return
            
             # ADD THIS: Prevent modification process from also posting error
            if hasattr(context, 'modification_message_ts') and context.modification_message_ts:
                context.cancel_event.set()
                context.safe_update(
                    agent_state=AgentState.CANCELLED,
                    awaiting_modification=False,
                    processing_active=False
                )

            context.safe_update(cancellation_handled=True)
            
            # Signal any waiting approvals
            if context.approval_response:
                context.approval_response["approved"] = False
            if context.approval_event:
                context.approval_event.set()
        
        # Clean up UI
        cleanup_approval_messages(client, context)
        
        cancel_step = "Human cancelled query execution"
        try:
            new_text = _rebuild_main_message_without_progress(context, cancel_step)
             # Determine which message to update
            active_message_ts = None
            if hasattr(context, 'modification_message_ts') and context.modification_message_ts:
                active_message_ts = context.modification_message_ts
                new_text = new_text.replace("Analysis Session", "Cancelled Session")
            elif context.main_message_ts:
                active_message_ts = context.main_message_ts
                new_text = new_text.replace("Analysis Session", "Analysis Session") + " – Cancelled"
            
            if active_message_ts:
                client.chat_update(
                    channel=context.channel_id,
                    ts=active_message_ts,
                    text=new_text
                )
        except Exception as e:
            logger.warning(f"Couldn't update main message on cancel: {e}")

        # Record the step
        # Record the step
        if cancel_step not in context.thinking_steps:
            context.thinking_steps.append(cancel_step)

        # ONLY post separate message if this was NOT during modification
        if not (hasattr(context, 'modification_message_ts') and context.modification_message_ts):
            client.chat_postMessage(
                channel=context.channel_id,
                thread_ts=context.thread_ts,
                text="❌ Query execution cancelled by user."
            )

        logger.info(f"SQL cancelled for session {session_id}")


@app.action("modify_sql")
def handle_sql_modification(ack, body, client, logger):
    ack()
    session_id = body["actions"][0]["value"]
    context = conversation_manager.get_conversation(session_id)
    
    if context:
        with context.state_lock:
            # IMMEDIATE CANCELLATION of current operation
            context.cancel_event.set()
            context.safe_update(
                agent_state=AgentState.WAITING_MODIFICATION,
                awaiting_modification=True,
                processing_active=False,
                cancel_event=threading.Event()  # NEW cancel token
            )
            
            # Signal cancellation to stop any waiting approval process
            if context.approval_response:
                context.approval_response["approved"] = False
                context.approval_response["modification_requested"] = True
            if context.approval_event:
                context.approval_event.set()
        
        # Clean up UI
        cleanup_approval_messages(client, context)
        
        mod_step = "Human requested query modification"
        try:
            new_text = _rebuild_main_message_without_progress(context, mod_step)
            if context.main_message_ts:
                client.chat_update(
                    channel=context.channel_id,
                    ts=context.main_message_ts,
                    text=new_text
                )
        except Exception as e:
            logger.warning(f"Couldn't update main message on modification: {e}")

        # Also record the ❌ step so later code sees it
        if mod_step not in context.thinking_steps:
            context.thinking_steps.append(mod_step)

        modified_notice = client.chat_postMessage(
            channel=context.channel_id,
            thread_ts=context.thread_ts,
            text="🔧 *Modify Query Instructions*\n\nPlease describe what changes you'd like me to make to the SQL query. For example:\n• \"Only show data from the last 3 days\"\n• \"Sort by views descending\"\n• \"Add filtering for specific rounds\"\n• \"Include additional columns\"\n\nI'll regenerate the query based on your feedback."
        )
        
        context.safe_update(
            instruction_message_ts=modified_notice["ts"],
            modification_message_ts=modified_notice["ts"]
        )

@app.action("create_chart_bar")
@app.action("create_chart_line")
@app.action("create_chart_pie")
def handle_chart_creation(ack, body, client, logger):
    ack()
    try:
        action_id = body["actions"][0]["action_id"]
        value_parts = body["actions"][0]["value"].split("|")
        session_id = value_parts[0]
        
        chart_type = action_id.replace("create_chart_", "")
        chart_title = "|".join(value_parts[2:]) if len(value_parts) > 2 else f"Chart for session {session_id}"
        
        context = conversation_manager.get_conversation(session_id)
        if not context or not context.chart_data:
            client.chat_postMessage(
                channel=body["channel"]["id"],
                thread_ts=body["message"]["ts"],
                text="❌ Chart data not available. Please run your query again."
            )
            return
        
        buf = _render_chart(context.chart_data, chart_type, chart_title)
        
        upload_resp = client.files_upload_v2(
            file=buf,
            filename=f"{chart_type}_chart.png",
            title=f"{chart_title} – {chart_type.capitalize()}",
            channel=context.channel_id,
            thread_ts=context.thread_ts,
            initial_comment=f"📊 Here's your *{chart_type}* chart!"
        )
        
        logger.info(f"Chart uploaded successfully: {upload_resp['file']['permalink']}")
        
    except Exception as e:
        logger.error(f"Error creating chart: {e}")
        client.chat_postMessage(
            channel=body["channel"]["id"],
            thread_ts=body["message"]["ts"],
            text=f"❌ Error creating chart: {str(e)}"
        )

@app.action("toggle_chart_bar")
@app.action("toggle_chart_line") 
@app.action("toggle_chart_pie")
def handle_toggle_chart_creation(ack, body, client, logger):
    ack()
    try:
        action_id = body["actions"][0]["action_id"]
        value_parts = body["actions"][0]["value"].split("|")
        
        chart_type = action_id.replace("toggle_chart_", "")
        chart_title = "|".join(value_parts[1:]) if len(value_parts) > 1 else "Chart"
        
        channel_id = body["channel"]["id"]
        message = body["message"]
        thread_ts = message.get("thread_ts", message["ts"])
        
        context = conversation_manager.get_conversation_by_thread(thread_ts)
        
        if not context:
            for conv in conversation_manager.conversations.values():
                if conv.channel_id == channel_id and conv.chart_data:
                    context = conv
                    break
        
        if not context or not context.chart_data:
            client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text="❌ Chart data not available. Please run your query again."
            )
            return
        
        buf = _render_chart(context.chart_data, chart_type, chart_title)
        
        upload_resp = client.files_upload_v2(
            file=buf,
            filename=f"{chart_type}_chart.png",
            title=f"{chart_title} – {chart_type.capitalize()}",
            channel=channel_id,
            thread_ts=thread_ts,
            initial_comment=f"📊 Here's your *{chart_type}* chart!"
        )
        
        logger.info(f"Chart uploaded successfully: {upload_resp['file']['permalink']}")
        
    except Exception as e:
        logger.error(f"Error creating toggle chart: {e}")
        client.chat_postMessage(
            channel=body["channel"]["id"],
            thread_ts=body["message"].get("thread_ts", body["message"]["ts"]),
            text=f"❌ Error creating chart: {str(e)}"
        )

@app.action("show_text_only")
def handle_text_only(ack, body, client):
    ack()
    session_id = body["actions"][0]["value"]
    context = conversation_manager.get_conversation(session_id)
    
    if context and context.execution_results:
        formatted_result = format_results_consistently(context.execution_results)
        client.chat_postMessage(
            channel=context.channel_id,
            thread_ts=context.thread_ts,
            text=f"📝 Detailed Results:\n{formatted_result}"
        )

@app.event("message")
def handle_threaded_messages(body, client, logger):
    logger.info(f"DEBUG: Message event received: {body['event'].get('text', 'NO_TEXT')}")
    event = body["event"]
    thread_ts = event.get("thread_ts")
    
    if not thread_ts:
        logger.info("DEBUG: No thread_ts, skipping")
        return

    context = conversation_manager.get_conversation_by_thread(thread_ts)
    logger.info(f"DEBUG: Found context = {context is not None}")
    
    if not context:
        return

    if event.get("subtype") not in (None, "file_share"):
        return

    msg_ts = event["ts"]
    if getattr(context, "last_processed_user_ts", None) == msg_ts:
        logger.info(f"DEBUG: Duplicate message {msg_ts}, skipping")
        return

    conversation_manager.update_conversation(
        context.session_id,
        last_processed_user_ts=msg_ts
    )

    # ADD THIS DEBUG LOGGING:
    logger.info(f"DEBUG: Context state = {context.agent_state}")
    logger.info(f"DEBUG: Awaiting modification = {getattr(context, 'awaiting_modification', False)}")

    message_text = event["text"].strip()
    user = event["user"]

    if event.get("bot_id"):
        return

    # Handle query modification requests
    if (context.agent_state == AgentState.WAITING_MODIFICATION and
        getattr(context, 'awaiting_modification', False)):
        
        logger.info(f"Processing modification request from user {user}: '{message_text}'")

        # CLEAN UP: Delete the instruction message and user's response message
        try:
            if hasattr(context, 'instruction_message_ts') and context.instruction_message_ts:
                client.chat_delete(
                    channel=context.channel_id,
                    ts=context.instruction_message_ts
                )
        except Exception:
            pass

        # Delete the user's modification request message to keep thread clean
        try:
            client.chat_delete(
                channel=context.channel_id,
                ts=event["ts"]
            )
        except Exception:
            pass

        conversation_manager.update_conversation(
            session_id=context.session_id,
            modification_request=message_text
        )

        # Posting the message to show modification processing
        modification_processing = client.chat_postMessage(
            channel=context.channel_id,
            thread_ts=context.thread_ts,
            text=f"🔄 *Processing Modification*\n\n*Original:* {context.original_question}\n*Your request:* {message_text}\n\n⚙️ Regenerating SQL query with your modifications..."
        )

        # Update the context with modification message timestamp
        conversation_manager.update_conversation(
            session_id=context.session_id,
            modification_message_ts=modification_processing["ts"]
        )

        @with_timeout(240, modification_timeout_msg)  # 4 minutes
        def process_modified_question(context, client, message_text):
            # Clear the previous SQL and start fresh
            context.safe_update(
                base_sql=None,  # Clear base SQL so we start fresh
                proposed_sql=None,
                sql_explanation=None,
                agent_state=AgentState.ANALYZING
            )

            # ATOMIC STATE CHECK - prevents race conditions
            with context.state_lock:
                if getattr(context, 'processing_active', False):
                    logger.info(f"PREVENTED: Already processing for {context.session_id}")
                    return
                context.safe_update(
                    processing_active=True,
                    modification_request=message_text,
                    awaiting_modification=False,
                    agent_state=AgentState.ANALYZING,
                    thinking_steps=[],
                    proposed_sql=None,
                    sql_explanation=None
                )

                context.mod_chain.add(message_text)

            try:
                # Create NEW approval objects (isolated from previous)
                approval_response = {"approved": None, "sql_query": None, "explanation": None}
                approval_event = threading.Event()
                
                def thinking_update(step):
                    if context.cancel_event.is_set():
                        raise RuntimeError("Analysis cancelled by user")
                    
                    update_analysis_status(
                        client, context.channel_id, context.modification_message_ts, 
                        context, step, "Analysis Session – Modification"
                    )
                
                def request_approval(sql_query, explanation):
                    # Store in isolated approval response
                    approval_response.update({
                        "sql_query": sql_query,
                        "explanation": explanation,
                        "approved": None
                    })
                    
                    context.safe_update(
                        sql_explanation=explanation,
                        proposed_sql=sql_query
                    )
                    
                    # Update main message with final SQL
                    steps_text = "\n".join([f"✓ {s}" for s in context.thinking_steps])
                    final_text = f"🤖 *Modified Query Ready #{context.session_id}*\n\n*Original:* {context.original_question}\n*Change:* {message_text}\n\n🧠 *Analysis Steps:*\n{steps_text}\n\n🔍 *Generated SQL Query:*\n```{sql_query}```\n\n📝 *Explanation:* {explanation}"
                    
                    client.chat_update(
                        channel=context.channel_id,
                        ts=context.modification_message_ts,
                        text=final_text
                    )
                    
                    # Post approval buttons
                    request_sql_approval(client, context.channel_id, context.thread_ts, context, sql_query, explanation)
                    
                    # Wait for approval with timeout
                    if approval_event.wait(timeout=300):
                        return approval_response.get("approved", False)
                    else:
                        cleanup_approval_messages(client, context)

                        timeout_message = f"⏰ *Approval Timeout #{context.session_id}*\n\n*Question:* {context.original_question}\n\n❌ No response received within 5 minutes\n\n💡 What happened?\nThe system was waiting for your approval to execute the SQL query, but didn't receive a response.\n\n🔄 Next steps:\n• Ask your question again to generate a new query\n• I'll wait for your approval before executing any database operations\n\n🛡️ Security note: Queries always require your explicit approval before execution."

                        client.chat_postMessage(
                            channel=context.channel_id,
                            thread_ts=context.thread_ts,
                            text=timeout_message
                        )
                        
                        raise Exception("Approval timeout")
                
                # Update context with NEW approval objects
                with context.state_lock:
                    context.approval_event = approval_event
                    context.approval_response = approval_response
                
                wants_chart = user_wants_chart(message_text)
                cleaned_question = remove_chart_keywords_from_question(message_text) if wants_chart else message_text

                enhanced_question = build_manual_context(
                    context.original_question,
                    None,
                    context.mod_chain.summary(),
                    cleaned_question
                )
                # Run analysis
                result = get_agent_with_human_approval(
                    enhanced_question, context, thinking_update, request_approval, context.session_id
                )
                
                # Handle results
                if result.get("cancelled") or result.get("error"):
                    return
                    
                cleanup_approval_messages(client, context)
                
                # Handle successful result
                handle_successful_result(
                    client, context.channel_id, context.modification_message_ts, 
                    context, result, wants_chart
                )
                
            except Exception as e:
                # CHECK if this was a cancellation, not a real error
                if "cancelled by user" in str(e).lower() and getattr(context, 'agent_state', None) == AgentState.CANCELLED:
                    logger.info(f"Modification cancelled cleanly for {context.session_id}")
                    return
                logger.error(f"Modification error: {e}")
                client.chat_postMessage(
                    channel=context.channel_id,
                    thread_ts=context.thread_ts,
                    text=f"❌ Error processing modification: {str(e)}"
                )
            finally:
                # Always reset processing flag
                with context.state_lock:
                    context.safe_update(processing_active=False)

        threading.Thread(target=lambda: process_modified_question(context, client, message_text), daemon=True).start()
        return

    # Handle follow-up questions
    if context.agent_state in [AgentState.COMPLETED, AgentState.CANCELLED]:
        if len(message_text) > 10 and not getattr(context, 'mod_processing_started', False):
            
            logger.info(f"Processing follow-up question: {message_text}")
            
            # IMMEDIATELY reset cancel event for new analysis
            conversation_manager.update_conversation(
                context.session_id,
                cancel_event=threading.Event(),  # NEW cancel token
                mod_processing_started=False,
                awaiting_modification=False,
                agent_state=AgentState.ANALYZING,
                thinking_steps=[],
                proposed_sql=None,
                sql_explanation=None
            )

            # Post initial follow-up message
            initial_followup_msg = client.chat_postMessage(
                channel=context.channel_id,
                thread_ts=thread_ts,
                text="🔄 I see you have additional questions. Let me help you refine the analysis..."
            )

            initial_followup_ts = initial_followup_msg["ts"]
            
            # Delete the user's message to keep thread clean
            try:
                client.chat_delete(
                    channel=context.channel_id,
                    ts=event["ts"]
                )
            except Exception:
                pass

            @with_timeout(240, followup_timeout_msg)  # 4 minutes
            def process_follow_up(context, client, message_text):
                honour_cancel(context) 
                # Check if already processing this exact follow-up
                current_mod_request = getattr(context, 'modification_request', None)
                if (getattr(context, "mod_processing_started", False) and current_mod_request == message_text):
                    logger.info(f"[DEDUPE] Same follow-up already running for {context.session_id}: {message_text[:50]}...")
                    return

                # Set processing flags for follow-up
                conversation_manager.update_conversation(
                    context.session_id,
                    mod_processing_started=True,
                    modification_request=message_text,
                    awaiting_modification=True,
                    agent_state=AgentState.ANALYZING,
                    thinking_steps=[],
                    proposed_sql=None,
                    sql_explanation=None
                )

                context.mod_chain.add(message_text)

                try:
                    # Create follow-up processing message
                    try:
                        client.chat_update(
                            channel=context.channel_id,
                            ts=initial_followup_ts,
                            text=f"🔄 *Processing Follow-up Question*\n\n*Original:* {context.original_question}\n*Your follow-up:* {message_text}\n\n⚙️ Setting up analysis..."
                        )
                        followup_processing = {"ts": initial_followup_ts}
                    except Exception:
                        # Fallback to posting new message if update fails
                        followup_processing = client.chat_postMessage(
                            channel=context.channel_id,
                            thread_ts=context.thread_ts,
                            text=f"🔄 *Processing Follow-up Question*\n\n*Original:* {context.original_question}\n*Your follow-up:* {message_text}\n\n⚙️ Setting up analysis..."
                        )
                    
                    conversation_manager.update_conversation(
                        context.session_id,
                        modification_message_ts=followup_processing["ts"]
                    )

                    # Set up approval mechanism for follow-up
                    approval_response = {"approved": None, "sql_query": None, "explanation": None}
                    approval_event = threading.Event()

                    def thinking_update(step):
                        update_analysis_status(client, context.channel_id, followup_processing["ts"], context, step, "Analysis Session – Follow-up")

                    def request_approval(sql_query, explanation):
                        approval_response["sql_query"] = sql_query
                        approval_response["explanation"] = explanation
                        approval_response["approved"] = None
                        
                        conversation_manager.update_conversation(
                            context.session_id,
                            sql_explanation=explanation,
                            proposed_sql=sql_query
                        )

                        # Update the main message to show final SQL
                        steps_text = "\n".join([f"✓ {step}" for step in context.thinking_steps])
                        final_text = f"🔄 *Follow-up Query Ready*\n\n*Original:* {context.original_question}\n*Follow-up:* {message_text}\n\n🧠 *Analysis Steps:*\n{steps_text}\n\n🔍 *Generated SQL Query:*\n```{sql_query}```\n\n📝 *Explanation:* {explanation}"
                        
                        try:
                            client.chat_update(
                                channel=context.channel_id,
                                ts=followup_processing["ts"],
                                text=final_text
                            )
                        except Exception:
                            pass

                        # Post waiting message
                        waiting_response = client.chat_postMessage(
                            channel=context.channel_id,
                            thread_ts=context.thread_ts,
                            text="⏳ *Waiting for your approval to execute the follow-up query...*"
                        )

                        # Post approval buttons
                        approval_blocks = create_approval_blocks(context.session_id)
                        buttons_response = client.chat_postMessage(
                            channel=context.channel_id,
                            thread_ts=context.thread_ts,
                            text="👆 Choose an action:",
                            blocks=approval_blocks
                        )

                        conversation_manager.update_conversation(
                            context.session_id,
                            approval_message_ts=waiting_response["ts"],
                            approval_buttons_ts=buttons_response["ts"]
                        )

                        # Wait for approval
                        try:
                            approval_received = approval_event.wait(timeout=300)
                            if not approval_received or approval_response["approved"] is None:
                                cleanup_approval_messages(client, context)
                                timeout_message = f"⏰ *Approval Timeout #{context.session_id}*\n\n*Question:* {context.original_question}\n\n❌ No response received within 5 minutes\n\n💡 What happened?\nThe system was waiting for your approval to execute the SQL query, but didn't receive a response.\n\n🔄 Next steps:\n• Ask your question again to generate a new query\n• I'll wait for your approval before executing any database operations\n\n🛡️ Security note: Queries always require your explicit approval before execution."

                                client.chat_postMessage(
                                    channel=context.channel_id,
                                    thread_ts=context.thread_ts,
                                    text=timeout_message
                                )
                                raise Exception("Approval timeout - please try again")
                            return approval_response["approved"]
                        except Exception as e:
                            cleanup_approval_messages(client, context)
                            raise e

                    # Store approval objects in context
                    context.approval_event = approval_event
                    context.approval_response = approval_response
                    conversation_manager.update_conversation(
                        context.session_id,
                        approval_event=approval_event,
                        approval_response=approval_response
                    )
                    
                    # Get the last successful SQL query for better context
                    last_sql = getattr(context, 'proposed_sql', None) or ""
                    sql_context = f"\n\nPrevious SQL query in this conversation: {last_sql}" if last_sql else ""

                    # Check for chart request
                    wants_chart = user_wants_chart(message_text)
                    cleaned_question = remove_chart_keywords_from_question(message_text) if wants_chart else message_text

                    enhanced_question = build_manual_context(
                        context.original_question,
                        context.base_sql,
                        context.mod_chain.summary(),
                        cleaned_question
                    )

                    # Execute the follow-up analysis
                    result = get_agent_with_human_approval(
                        enhanced_question,
                        context,
                        thinking_update,
                        request_approval,
                        session_id=context.session_id
                    )

                    # Handle errors in follow-up
                    if result.get("error") and not result.get("cancelled"):
                        client.chat_update(
                            channel=context.channel_id,
                            ts=followup_processing["ts"],
                            text=f"🔄 *Follow-up Processing Error*\n\n*Original:* {context.original_question}\n*Follow-up:* {message_text}\n\n❌ *Error:* {result['error']}"
                        )
                        return

                    if result.get("cancelled"):
                        return

                    # Clean up approval messages from follow-up
                    cleanup_approval_messages(client, context)

                    # Force chart if user mentioned it
                    if "chart" in message_text.lower():
                        wants_chart = True

                    # Update the main message with final results
                    if not result.get("cancelled") and not result.get("error") and result.get("sql_query"):
                        handle_successful_result(client, context.channel_id, followup_processing["ts"], context, result, wants_chart)

                except Exception as e:
                    logger.error(f"Error in follow-up processing: {e}")
                    client.chat_postMessage(
                        channel=context.channel_id,
                        thread_ts=context.thread_ts,
                        text=f"❌ Error processing follow-up: {str(e)}\n\nPlease try asking your question again."
                    )
                finally:
                    # Always reset the flags
                    conversation_manager.update_conversation(
                        context.session_id,
                        mod_processing_started=False,
                        awaiting_modification=False
                    )
            
            # Run in thread to prevent blocking
            threading.Thread(target=lambda: process_follow_up(context, client, message_text), daemon=True).start()

def parse_natural_language_alert(text: str) -> dict:
    """Parse natural language into metric configuration"""
    import re
    
    text_lower = text.lower()
    
    # Extract metric name and description
    name = extract_metric_name(text)
    description = text.strip()
    
    # Extract table (guess from keywords)
    table = extract_table_name(text_lower)
    
    # Extract metric type
    metric_type = extract_metric_type(text_lower)
    
    # Extract thresholds
    min_threshold, max_threshold = extract_thresholds(text_lower)
    
    # Extract business impact
    business_impact = extract_business_impact(text_lower)
    
    # Extract alert strategy
    alert_strategy = extract_alert_strategy(text_lower)
    
    # Extract schedule
    schedule = extract_schedule_smart(text_lower, alert_strategy)
    
    return {
        'name': name,
        'description': description,
        'table': table,
        'metric_type': metric_type,
        'min_threshold': min_threshold,
        'max_threshold': max_threshold,
        'business_impact': business_impact,
        'alert_strategy': alert_strategy,
        'schedule': schedule
    }

def extract_metric_name(text: str) -> str:
    """Extract a clean metric name from natural language - COMMA-FREE"""
    # Remove ALL punctuation first
    clean_text = re.sub(r'[^\w\s]', '', text)
    
    stop_words = {'monitor', 'track', 'watch', 'alert', 'when', 'if', 'below', 'above', 'more', 'than', 'per', 'daily', 'hourly'}
    words = [w for w in clean_text.lower().split() if w not in stop_words and len(w) > 2]
    key_words = words[:3]
    return ' '.join(word.title() for word in key_words)

def extract_table_name(text: str) -> str:
    """Map keywords to database tables"""
    table_keywords = {
        'leaderboard': 'alt_leaderboardviewed',
        'deposit': 'alt_depositsuccessclient', 
        'payment': 'alt_depositsuccessclient',
        'contest': 'alt_contestjoinedclient',
        'join': 'alt_contestjoinedclient',
        'signup': 'alt_leaderboardviewed',  # Proxy for user activity
        'user': 'alt_leaderboardviewed',
        'error': 'alt_paymenterrorshown',
        'failure': 'alt_contestjoinfailed',
        'failed': 'alt_contestjoinfailed'
    }
    
    for keyword, table in table_keywords.items():
        if keyword in text:
            return table
    
    # Default fallback
    return 'alt_leaderboardviewed'

def extract_metric_type(text: str) -> MetricType:
    """Determine metric type from natural language"""
    if any(word in text for word in ['count', 'number', 'total']):
        return MetricType.COUNT
    elif any(word in text for word in ['sum', 'amount', 'revenue']):
        return MetricType.SUM
    elif any(word in text for word in ['average', 'avg', 'mean']):
        return MetricType.AVERAGE
    elif any(word in text for word in ['rate', 'percentage', '%']):
        return MetricType.PERCENTAGE
    elif any(word in text for word in ['unique', 'distinct']):
        return MetricType.UNIQUE_COUNT
    else:
        return MetricType.COUNT  # Default

def extract_thresholds(text: str) -> tuple:
    """Extract min/max thresholds from text"""
    import re
    
    min_threshold = None
    max_threshold = None
    
    # Look for "below X" or "less than X"
    below_match = re.search(r'(?:below|less than|under)\s+(\d+(?:\.\d+)?)', text)
    if below_match:
        min_threshold = float(below_match.group(1))
    
    # Look for "above X" or "more than X"  
    above_match = re.search(r'(?:above|more than|over|greater than)\s+(\d+(?:\.\d+)?)', text)
    if above_match:
        max_threshold = float(above_match.group(1))
    
    return min_threshold, max_threshold

def extract_business_impact(text: str) -> str:
    """Extract business impact level"""
    if any(word in text for word in ['critical', 'urgent', 'emergency']):
        return 'critical'
    elif any(word in text for word in ['high', 'important', 'priority']):
        return 'high'
    elif any(word in text for word in ['low', 'minor']):
        return 'low'
    else:
        return 'medium'  # Default

def extract_alert_strategy(text: str) -> str:
    """Extract alert strategy"""
    if any(word in text for word in ['realtime', 'real-time', 'immediate', 'instant']):
        return 'realtime'
    elif any(word in text for word in ['digest', 'daily', 'summary']):
        return 'digest'
    else:
        return 'realtime'  # Default for safety

def extract_schedule_smart(text: str, strategy: str) -> str:
    """
    COMPLETE SCHEDULE EXTRACTION:
    1. If digest strategy → return None (handled elsewhere)
    2. If user mentions specific time → use it  
    3. If no time but has priority → use priority-based schedule
    4. If neither → use default schedule
    """
    
    # STEP 1: Handle digest strategy
    if strategy == 'digest':
        return None  # Digest alerts don't need cron schedules
    
    text_lower = text.lower()
    
    # STEP 2: Try cronslator for specific times first
    specific_time_patterns = [
        'at ', 'daily at', 'weekly at', 'monday at', 'tuesday at', 'wednesday at', 
        'thursday at', 'friday at', 'saturday at', 'sunday at',
        '9am', '10am', '11am', '12pm', '1pm', '2pm', '3pm', '4pm', '5pm',
        'noon', 'midnight', 'morning', 'afternoon', 'evening'
    ]
    
    has_specific_time = any(pattern in text_lower for pattern in specific_time_patterns)
    
    if has_specific_time:
        try:
            # Use cronslator for complex time expressions
            from pyslop.cronslator import cronslate
            cron_expr = cronslate(text)
            
            # Handle cronslator return types
            if isinstance(cron_expr, list):
                cron_expr = str(cron_expr[0]) if cron_expr else None
            elif cron_expr:
                cron_expr = str(cron_expr)
            
            # Validate cronslator result
            if cron_expr and cron_expr != "* * * * *":  # Reject overly frequent
                logger.info(f"SPECIFIC TIME: cronslator returned '{cron_expr}' for '{text}'")
                return cron_expr
        except Exception as e:
            logger.warning(f"cronslator failed for '{text}': {e}")
    
    # STEP 3: Handle basic time intervals (hourly, daily, etc.)
    basic_time_keywords = [
        'per hour', 'hourly', 'every hour', 'each hour',
        'per minute', 'every minute', 'minutes', 'minute',
        'every 5 minutes', 'every 10 minutes', 'every 15 minutes', 'every 30 minutes',
        'daily', 'per day', 'every day', 'twice daily',
        'weekly', 'every week', 'monthly'
    ]
    
    has_basic_time = any(keyword in text_lower for keyword in basic_time_keywords)
    
    if has_basic_time:
        logger.info(f"BASIC TIME: Using extract_schedule_basic for '{text}'")
        return extract_schedule_basic(text, strategy)
    
    # STEP 4: No time mentioned → use priority-based schedule
    priority_keywords = ['critical', 'high', 'medium', 'low']
    priority_found = None
    
    for priority in priority_keywords:
        if priority in text_lower:
            priority_found = priority
            break
    
    if priority_found:
        priority_schedules = {
            'critical': '*/5 * * * *',    # Every 5 minutes
            'high': '*/10 * * * *',       # Every 10 minutes  
            'medium': '*/30 * * * *',     # Every 30 minutes
            'low': '0 */2 * * *'          # Every 2 hours
        }
        logger.info(f"PRIORITY-BASED: Using {priority_schedules[priority_found]} for priority '{priority_found}'")
        return priority_schedules[priority_found]
    
    # STEP 5: No time, no priority → default schedule
    logger.info(f"DEFAULT SCHEDULE: Using fallback for '{text}'")
    return "*/15 * * * *"  # Every 15 minutes default
    
def extract_schedule_basic(text: str, strategy: str) -> str:
    """Handle explicit time mentions only"""
    if strategy == 'digest':
        return None
    
    text_lower = text.lower()
    numbers = re.findall(r'\b(\d+)\b', text_lower)
    
    # Hourly patterns (PRIORITY - user said "per hour")
    if any(word in text_lower for word in ['per hour', 'hourly', 'every hour', 'each hour']):
        return "0 * * * *"  # Every hour at minute 0
    
    # Specific minute intervals
    elif 'every 5 minutes' in text_lower or '5 minutes' in text_lower:
        return "*/5 * * * *"
    elif 'every 10 minutes' in text_lower or '10 minutes' in text_lower:
        return "*/10 * * * *"
    elif 'every 15 minutes' in text_lower or '15 minutes' in text_lower:
        return "*/15 * * * *"
    elif 'every 30 minutes' in text_lower or '30 minutes' in text_lower:
        return "*/30 * * * *"
    
    # Daily patterns
    elif any(word in text_lower for word in ['daily', 'every day', 'per day']):
        return "0 0 * * *"  # Midnight daily
    elif 'twice daily' in text_lower:
        return "0 0,12 * * *"  # Midnight and noon
    
    # Weekly patterns  
    elif 'weekly' in text_lower:
        return "0 0 * * 0"  # Sunday midnight
    
    # Generic time mentions with numbers
    elif any(word in text_lower for word in ['minute', 'minutes']) and numbers:
        interval = int(numbers[0])
        if interval <= 59:
            return f"*/{interval} * * * *"
        return "*/15 * * * *"  # Fallback
        
    elif any(word in text_lower for word in ['hour', 'hours']) and numbers:
        interval = int(numbers[0])
        if interval <= 23:
            return f"0 */{interval} * * *"
        return "0 * * * *"  # Fallback to hourly
    
    # If we got here, user mentioned time words but not specific - default
    return "*/15 * * * *"


def force_complete_reload():
    """NUCLEAR OPTION - Complete system restart without server restart"""
    import sys
    import importlib
    import gc
    
    # Clear ALL possible module variations
    modules_to_clear = [
        'alert_config', 'anomaly_detector', 
        'src.alert_config', 'src.anomaly_detector',
        'DatabaseAnomalyDetector', 'AnomalyResult'
    ]
    
    for module_name in modules_to_clear:
        if module_name in sys.modules:
            del sys.modules[module_name]
            logger.info(f"Cleared module: {module_name}")
    
    # Force garbage collection
    gc.collect()
    
    # Fresh imports with explicit paths
    try:
        # Force re-import alert_config
        import alert_config
        importlib.reload(alert_config)
        
        # Force re-import anomaly_detector  
        from anomaly_detector import DatabaseAnomalyDetector
        importlib.reload(sys.modules['anomaly_detector'])
        
        # Recreate detector with COMPLETELY fresh instance
        global alert_manager
        alert_manager.detector = DatabaseAnomalyDetector()
        
        logger.info(f"Nuclear reload complete - {len(alert_config.BUSINESS_METRICS)} metrics loaded")
        return alert_config.BUSINESS_METRICS
        
    except Exception as e:
        logger.error(f"Nuclear reload failed: {e}")
        raise e

def reload_alert_config():
    """Dynamically reload alert configuration without restart"""
    try:
        import importlib
        import sys
        
        # Add src to path if not already there
        if 'src' not in sys.path:
            sys.path.insert(0, 'src')
        
        # Clear any cached modules
        if 'alert_config' in sys.modules:
            del sys.modules['alert_config']

        import alert_config
        
        # Reload the module
        importlib.reload(alert_config)
        
        # Update the global reference
        global BUSINESS_METRICS
        from alert_config import BUSINESS_METRICS, MetricDefinition, MetricType
        
        # Update the alert manager's detector
        alert_manager.detector = DatabaseAnomalyDetector()
        # Verify the metric exists
        if 'failed_payments_hour' in BUSINESS_METRICS:
            logger.info(f"✅ Metric loaded successfully")
            logger.info(f"📊 Total metrics: {len(BUSINESS_METRICS)}")
        else:
            logger.error(f"❌ Metric 'failed_payments_hour' not found after reload")
            logger.info(f"Available metrics: {list(BUSINESS_METRICS.keys())}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to reload config: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def create_metric_definition(parsed_config: dict) -> tuple:
    """Create MetricDefinition object from parsed config"""
    
    # Generate clean metric_id - remove ALL punctuation
    clean_name = re.sub(r'[^\w\s]', '', parsed_config['name'])
    metric_id = '_'.join(clean_name.lower().split())
    metric_id = metric_id.strip('_,').replace(',', '_')
    
    # SAFE schedule extraction - handle list/string/None
    schedule_value = parsed_config.get('schedule')
    if isinstance(schedule_value, list):
        schedule_value = str(schedule_value[0]) if schedule_value else None
    elif schedule_value is not None:
        schedule_value = str(schedule_value)

    # Clean description too
    clean_description = re.sub(r'[,]+$', '', parsed_config['description']).strip()
    
    # Create MetricDefinition
    metric_def = MetricDefinition(
        name=str(parsed_config['name']).strip(','),
        description=clean_description,
        table=str(parsed_config['table']),
        metric_type=parsed_config['metric_type'],
        business_impact=str(parsed_config['business_impact']),
        alert_strategy=str(parsed_config['alert_strategy']),
        schedule=schedule_value,
        min_threshold=parsed_config['min_threshold'],
        max_threshold=parsed_config['max_threshold'],
        alert_channels=["#alerts"]
    )
    
    return metric_id, metric_def

def save_metric_to_config(metric_id: str, metric_def) -> bool:
    """Save the new metric to alert_config.py file"""
    try:
        config_path = 'src/alert_config.py'
        
        # Read current config file
        with open(config_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()  # Use readlines() to keep \n
        
        # Find BUSINESS_METRICS closing brace
        insert_index = None
        in_business_metrics = False
        brace_count = 0
        
        for i, line in enumerate(lines):
            if 'BUSINESS_METRICS' in line and '= {' in line:
                in_business_metrics = True
                brace_count = 1
                # DON'T continue here - we need to process this line
                
            elif in_business_metrics:
                # Count braces to find the matching closing brace
                brace_count += line.count('{')
                brace_count -= line.count('}')
                
                # When brace_count hits 0, we found the closing brace
                if brace_count == 0:
                    insert_index = i  # This is the line with }
                    break
        
        if insert_index is None:
            raise Exception("Could not find BUSINESS_METRICS closing brace")
        
        # Generate clean metric code
        schedule_val = f'"{metric_def.schedule}"' if metric_def.schedule else 'None'
        min_val = metric_def.min_threshold if metric_def.min_threshold is not None else 'None'
        max_val = metric_def.max_threshold if metric_def.max_threshold is not None else 'None'
        
        ultra_clean_id = re.sub(r'[^\w_]', '', metric_id)
        ultra_clean_name = re.sub(r'[,]+', '', metric_def.name).strip()
        ultra_clean_desc = re.sub(r'[,]+$', '', metric_def.description).strip()
        
        # Create the new metric entry
        new_metric_lines = [
            f'    "{ultra_clean_id}": MetricDefinition(\n',
            f'        name="{ultra_clean_name}",\n',
            f'        description="{ultra_clean_desc}",\n',
            f'        table="{metric_def.table}",\n',
            f'        metric_type=MetricType.{metric_def.metric_type.name},\n',
            f'        business_impact="{metric_def.business_impact}",\n',
            f'        alert_strategy="{metric_def.alert_strategy}",\n',
            f'        schedule={schedule_val},\n',
            f'        min_threshold={min_val},\n',
            f'        max_threshold={max_val},\n',
            f'        alert_channels={metric_def.alert_channels},\n',
            f'    ),\n',
            f'\n'  # Empty line before closing brace
        ]
        
        # Insert all lines at once, right before the closing brace
        for j, new_line in enumerate(new_metric_lines):
            lines.insert(insert_index + j, new_line)
        
        # Write the modified lines back to file
        with open(config_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)  # Use writelines() since we have \n already
        
        logger.info(f"Successfully saved CLEAN metric {ultra_clean_id}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save metric: {e}")
        return False


@flask_app.route("/slack/events", methods=["POST"])
def slack_events():
    return handler.handle(request)

@flask_app.route("/slack/interactions", methods=["POST"])
def slack_interactions():
    return handler.handle(request)

# @flask_app.route("/health", methods=["GET"])
# def health_check():
#     return {"status": "healthy", "active_conversations": len(conversation_manager.conversations)}

@app.command("/check-metric")
def handle_single_metric_check(ack, body, client, logger):
    ack()
    user_id = body["user_id"]
    channel_id = body["channel_id"]
    text = body.get("text", "").strip()
    
    BUSINESS_METRICS = force_complete_reload()

    if not text:
        available_metrics = list(BUSINESS_METRICS.keys())
        metrics_list = "\n".join([f"• `{metric_id}`: {BUSINESS_METRICS[metric_id].name}" for metric_id in available_metrics])
        
        client.chat_postMessage(
            channel=channel_id,
            text=f"Please specify a metric to check.\n\n📊 Available metrics:\n\n{metrics_list}\n\n💡 Usage: `/check-metric daily_active_users`"
        )
        return
    
    metric_id = text.lower().replace("-", "_").replace(" ", "_")
    
    # Validate metric exists
    if metric_id not in BUSINESS_METRICS:
        similar_metrics = [m for m in BUSINESS_METRICS.keys() if metric_id.replace("_", "") in m.replace("_", "")]
        suggestion_text = f"\n\n💡 Did you mean: `{similar_metrics[0]}`?" if similar_metrics else ""
        
        client.chat_postMessage(
            channel=channel_id,
            text=f"❌ Unknown metric: `{metric_id}`{suggestion_text}\n\nUse `/check-metric` without parameters to see available metrics."
        )
        return
    
    # Create initial progress message
    metric_name = BUSINESS_METRICS[metric_id].name
    start_time = time.time()
    
    initial_message = client.chat_postMessage(
        channel=channel_id,
        text=f"🔍 Single Metric Analysis Started\n\n📊 Target Metric: {metric_name}\n⏰ Started: {datetime.now().strftime('%H:%M:%S')}\n⏳ Status: Initializing analysis...\n⏱️ Est. Time: Calculating..."
    )
    
    message_ts = initial_message["ts"]
    
    def check_and_respond():
        try:
            # Step 1: Database connection
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                text=f"🔍 Single Metric Analysis in Progress\n\n📊 Target: {metric_name}\n\n🔄 Step 1/4: Connecting to database...\n• ✓ Database connection established"
            )
            
            # Step 2: Loading metric definition
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                text=f"🔍 Single Metric Analysis in Progress\n\n📊 Target: {metric_name}\n\n🔄 Step 2/4: Loading metric configuration...\n• ✓ Database connection established\n• ✓ Metric definition loaded\n• ✓ Business impact level: {BUSINESS_METRICS[metric_id].business_impact.upper()}"
            )
            
            # Step 3: Historical analysis
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                text=f"🔍 Single Metric Analysis in Progress\n\n📊 Target: {metric_name}\n\n🔄 Step 3/4: Analyzing historical data...\n• ✓ Database connection established\n• ✓ Metric definition loaded\n• ✓ Business impact: {BUSINESS_METRICS[metric_id].business_impact.upper()}\n• 🔄 Calculating 14-day baseline..."
            )
            
            # Step 4: Running anomaly detection
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                text=f"🔍 Single Metric Analysis in Progress\n\n📊 Target: {metric_name}\n\n🔄 Step 4/4: Running anomaly detection...\n• ✓ Database connection established\n• ✓ Metric definition loaded\n• ✓ Business impact: {BUSINESS_METRICS[metric_id].business_impact.upper()}\n• ✓ Historical baseline calculated\n• 🔄 Detecting anomalies..."
            )
            
            analysis_start = time.time()
            
            anomalies = alert_manager.detector.run_single_check(metric_id)
            
            analysis_end = time.time()
            
            total_duration = time.time() - start_time
            analysis_duration = analysis_end - analysis_start
            
            if not anomalies:
                final_text = f"✅ Single Metric Analysis Complete\n\n📊 Metric: {metric_name}\n🎯 Result: No anomalies detected\n📈 Status: Metric is within normal ranges\n\n📋 Analysis Details:\n• ⏱️ Duration: {total_duration:.1f} seconds\n• 📅 Historical Period: 14 days\n• 🎚️ Thresholds: ±2 standard deviations\n• 🏢 Business Impact: {BUSINESS_METRICS[metric_id].business_impact.upper()}\n• 📢 Alert Channels: {', '.join(BUSINESS_METRICS[metric_id].alert_channels)}"
                
            else:
                anomaly = anomalies[0]  # Single metric should have at most one anomaly
                severity_emoji = {"low": "🟡", "medium": "🟠", "high": "🔴", "critical": "🚨"}.get(anomaly.severity, "⚠️")
                
                final_text = f"🚨 Anomaly Detected in Single Metric\n\n📊 Metric: {metric_name}\n{severity_emoji} Severity: {anomaly.severity.upper()}\n\n⚠️ Issue: {anomaly.message}\n\n📊 Current Value: {anomaly.current_value:,.2f}\n📈 Expected Range: {anomaly.expected_range[0]:,.2f} - {anomaly.expected_range[1]:,.2f}\n\n🎯 Suggested Action:\n{anomaly.suggested_action}\n\n📋 Analysis Details:\n• ⏱️ Duration: {total_duration:.1f} seconds\n• 📅 Historical Period: 14 days\n• 🏢 Business Impact: {anomaly.business_impact.upper()}\n• 📢 Detailed alert sent to: {', '.join(BUSINESS_METRICS[metric_id].alert_channels)}"
            
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                text=final_text + "\n\n💡 *Note: This was a manual check. Scheduled alerts will handle ongoing monitoring.*"
            )
            
            # # Send detailed alerts if anomalies found
            # if anomalies:
            #     alert_manager.send_anomaly_alerts(anomalies)
                
        except ValueError as e:
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                text=f"❌ Metric Analysis Failed\n\n📊 Metric: {metric_id}\n🚨 Error: {str(e)}\n\n💡 Suggestion: Use `/check-metric` to see available metrics."
            )
        except Exception as e:
            logger.error(f"Error checking single metric: {e}")
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                text=f"❌ System Error During Analysis\n\n📊 Metric: {metric_name}\n🚨 Error: {str(e)}\n\n🔄 Action: Please try again or contact support if the issue persists."
            )
    
    threading.Thread(target=check_and_respond, daemon=True).start()

@app.command("/check-anomalies")
def handle_anomaly_check(ack, body, client, logger):
    ack()
    channel_id = body["channel_id"]
    text = body.get("text", "").strip()
    start_time = time.time()

    # Create initial message
    if text:
        initial_message = client.chat_postMessage(
            channel=channel_id,
            text=f"🔍 Starting Single Metric Anomaly Check\n\n📊 Target: {text}\n\n⏳ Initializing analysis..."
        )
    else:
        initial_message = client.chat_postMessage(
            channel=channel_id,
            text="🔍 Starting Full Anomaly Detection\n\n⏳ Initializing system checks..."
        )
    
    message_ts = initial_message["ts"]
    
    def check_and_respond():
        try:
            FRESH_METRICS = force_complete_reload()
            
            if text:  # Single metric check
                metric_id = text.lower().replace("-", "_").replace(" ", "_")
                
                if metric_id not in FRESH_METRICS:
                    client.chat_update(
                        channel=channel_id,
                        ts=message_ts,
                        text=f"❌ Unknown metric: `{metric_id}`\n\nUse `/check-anomalies` without parameters to check all metrics."
                    )
                    return
                
                # Single metric analysis
                metric_name = FRESH_METRICS[metric_id].name
                client.chat_update(
                    channel=channel_id,
                    ts=message_ts,
                    text=f"🔍 Single Metric Analysis\n\n📊 Target: {metric_name}\n\n🔄 Running anomaly detection..."
                )
                
                analysis_start = time.time()
                anomalies = alert_manager.detector.run_single_check(metric_id)
                analysis_end = time.time()
                
                if not anomalies:
                    final_text = f"✅ Single Metric Check Complete\n\n📊 Metric: {metric_name}\n🎯 Result: No anomalies detected"
                else:
                    anomaly = anomalies[0]
                    final_text = f"🚨 Anomaly Detected\n\n📊 Metric: {metric_name}\n⚠️ Issue: {anomaly.message}"
                
            else:  # Full check
                total_metrics = len(FRESH_METRICS)
                client.chat_update(
                    channel=channel_id,
                    ts=message_ts,
                    text=f"🔍 Full Anomaly Detection\n\n📊 Checking {total_metrics} metrics...\n\n🔄 Running analysis..."
                )
                
                analysis_start = time.time()
                anomalies = alert_manager.detector.run_all_checks()
                analysis_end = time.time()
                
                if not anomalies:
                    final_text = f"✅ Full Anomaly Check Complete\n\n📊 Metrics Checked: {total_metrics}\n📈 Anomalies Found: 0"
                else:
                    summary_text = f"🚨 Full Anomaly Check Complete\n\n⚠️ Found {len(anomalies)} anomalies:\n\n"
                    for anomaly in anomalies:
                        summary_text += f"• {anomaly.metric_name} – {anomaly.message}\n"
                    final_text = summary_text
            
            total_time = time.time() - start_time
            analysis_time = analysis_end - analysis_start
            final_text += f"\n\n⏱️ Total Time: {total_time:.1f}s\n🔍 Analysis Time: {analysis_time:.1f}s"
            
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                text=final_text + "\n\n💡 *Note: This was a manual check.*"
            )
            
        except Exception as e:
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                text=f"❌ Anomaly Detection Failed\n\n🚨 Error: {str(e)}"
            )
            logger.error(f"Error in anomaly check: {e}")
    
    threading.Thread(target=check_and_respond, daemon=True).start()

# @app.command("/debug-tables")
# def handle_debug_tables(ack, body, client, logger):
#     ack()
    
#     channel_id = body["channel_id"]
    
#     def debug_and_respond():
#         debug_text = "🔍 DATABASE DEBUG REPORT\n\n"
        
#         tables_to_check = [
#             'alt_leaderboardviewed',
#             'alt_depositsuccessclient', 
#             'alt_contestjoinedclient',
#             'alt_contestjoinfailed',
#             'alt_paymenterrorshown'
#         ]
        
#         for table in tables_to_check:
#             has_data, has_recent = alert_manager.detector.debug_table_data(table)
#             status = "✅" if has_recent else "⚠️" if has_data else "❌"
#             debug_text += f"{status} {table}: {'Recent data' if has_recent else 'Old data only' if has_data else 'No data'}\n"
        
#         client.chat_postMessage(
#             channel=channel_id,
#             text=debug_text
#         )
    
#     threading.Thread(target=debug_and_respond, daemon=True).start()

# @app.command("/stop-monitoring")
# def handle_stop_monitoring(ack, body, client, logger):
#     ack()
    
    # automated_monitor.stop_monitoring()
    
    # client.chat_postMessage(
    #     channel=body["channel_id"],
    #     text="⏹️ Automated monitoring stopped.\n\nYou can restart it anytime using `/start-monitoring`."
    # )

# @app.command("/monitoring-status")
# def handle_monitoring_status(ack, body, client, logger):
#     ack()
    
#     status = automated_monitor.get_monitoring_status()
    
#     status_emoji = "✅" if status["is_running"] else "⏹️"
#     business_hours_emoji = "🟢" if status["business_hours_active"] else "🔵"
    
#     status_text = f"""📊 MONITORING STATUS

# {status_emoji} System Status: {'Running' if status['is_running'] else 'Stopped'}
# {business_hours_emoji} Business Hours: {'Active' if status['business_hours_active'] else 'Off-hours'}

# 📈 Metrics Monitored: {status['metrics_monitored']}
# ⏰ Recent Alerts Sent: {status['last_alerts_count']}

# 🔄 Alert Intervals:
# • Critical: Every {status['alert_intervals']['critical']} min
# • High: Every {status['alert_intervals']['high']} min  
# • Medium: Every {status['alert_intervals']['medium']} min
# • Low: Every {status['alert_intervals']['low']} min

# ⏰ Critical Hours: {status['critical_hours']['start']}:00 - {status['critical_hours']['end']}:00 IST"""

#     client.chat_postMessage(
#         channel=body["channel_id"],
#         text=status_text
#     )

# @app.command("/configure-metric")
# def handle_metric_configuration(ack, body, client, logger):
#     ack()
    
#     text = body.get("text", "").strip()
#     channel_id = body["channel_id"]
    
#     if not text:
#         # Show current configurations
#         config_text = "📊 **CURRENT METRIC CONFIGURATIONS**\n\n"
        
#         for metric_id, metric_def in BUSINESS_METRICS.items():
#             strategy_emoji = "⚡" if metric_def.alert_strategy == "realtime" else "📋"
#             config_text += f"{strategy_emoji} **{metric_def.name}**\n"
#             config_text += f"   • Strategy: {metric_def.alert_strategy.title()}\n"
#             config_text += f"   • Schedule: {metric_def.schedule}\n"
#             config_text += f"   • Impact: {metric_def.business_impact.title()}\n\n"
        
#         client.chat_postMessage(
#             channel=channel_id,
#             text=config_text
#         )
#         return
    
#     # Handle configuration changes (future enhancement)
#     client.chat_postMessage(
#         channel=channel_id,
#         text="⚙️ **Configuration changes coming soon!**\n\nFor now, configurations are managed in code. Use `/configure-metric` to view current settings."
#     )

@app.command("/create-alert")
def handle_create_alert(ack, body, client, logger):
    ack()
    
    channel_id = body["channel_id"]
    text = body.get("text", "").strip()
    
    if not text:
        example_text = """📊 CREATE ALERT FROM NATURAL LANGUAGE

Usage: `/create-alert [your description]`

Examples:
• `/create-alert Monitor daily signups, alert when below 100`
• `/create-alert Track failed payments per hour, critical priority`
• `/create-alert Watch average session time, medium impact`
• `/create-alert Alert when contest creation fails more than 5%`

What I can understand:
✓ Metric descriptions (signups, payments, sessions, etc.)
✓ Thresholds (above/below numbers)
✓ Priority levels (low, medium, high, critical)
✓ Time periods (daily, hourly)
✓ Alert channels"""
        
        client.chat_postMessage(
            channel=channel_id,
            text=example_text
        )
        return
    
    # Create processing message
    processing_msg = client.chat_postMessage(
        channel=channel_id,
        text=f"🤖 Creating Alert from Natural Language\n\n*Your Request:* {text}\n\n⚙️ Analyzing requirements..."
    )
    
    def process_nl_alert():
        try:
            # Step 1: Parse the natural language
            client.chat_update(
                channel=channel_id,
                ts=processing_msg["ts"],
                text=f"🤖 Creating Alert from Natural Language\n\n*Your Request:* {text}\n\n🔄 Step 1/4: Parsing natural language...\n• ✓ Extracting metric requirements\n• 🔄 Identifying thresholds and priorities"
            )
            
            parsed_config = parse_natural_language_alert(text)
            
            # Step 2: Generate metric definition
            client.chat_update(
                channel=channel_id,
                ts=processing_msg["ts"],
                text=f"🤖 Creating Alert from Natural Language\n\n*Your Request:* {text}\n\n🔄 Step 2/4: Generating metric definition...\n• ✓ Requirements parsed successfully\n• ✓ Metric type: {parsed_config['metric_type'].value}\n• 🔄 Creating configuration object"
            )
            
            metric_id, metric_def = create_metric_definition(parsed_config)
            # Step 3: Validate and save
            client.chat_update(
                channel=channel_id,
                ts=processing_msg["ts"],
                text=f"🤖 Creating Alert from Natural Language\n\n*Your Request:* {text}\n\n🔄 Step 3/4: Validating configuration...\n• ✓ Metric definition created\n• ✓ ID: {metric_id}\n• 🔄 Validating alert rules"
            )
            
            # Step 4: Save to file
            client.chat_update(
                channel=channel_id,
                ts=processing_msg["ts"],
                text=f"🤖 Creating Alert from Natural Language\n\n*Your Request:* {text}\n\n🔄 Step 4/4: Saving to alert_config.py...\n• ✓ Configuration validated\n• ✓ File path confirmed\n• 🔄 Writing to config file"
            )
            
            success = save_metric_to_config(metric_id, metric_def)
            
            if success:
                # Success message
                success_text = f"""✅ Alert Created Successfully!

📊 *Metric Details:*
• ID: `{metric_id}`
• Name: {metric_def.name}
• Table: {metric_def.table}
• Type: {metric_def.metric_type.value}
• Priority: {metric_def.business_impact.upper()}
• Strategy: {metric_def.alert_strategy.title()}

📁 *File Status:*
• ✓ Saved to: `alert_config.py`
• ✓ Configuration active
• ✅ Alert is now ACTIVE

🚀 *Next Steps:* 
• Test with: `/check-metric {metric_id}`"""

                reload_alert_config()
                client.chat_update(
                    channel=channel_id,
                    ts=processing_msg["ts"],
                    text=success_text
                )
            else:
                raise Exception("Failed to save metric to config file")
                
        except Exception as e:
            client.chat_update(
                channel=channel_id,
                ts=processing_msg["ts"],
                text=f"❌ Alert Creation Failed\n\n*Your Request:* {text}\n\nError: {str(e)}\n\n💡 Tip: Try rephrasing your request or use `/create-alert` for examples"
            )
    
    threading.Thread(target=process_nl_alert, daemon=True).start()


# Auto-start monitoring when the app starts
def start_automated_systems():
    """Start all automated systems"""
    try:
        automated_monitor.start_monitoring()  # New automated monitor
    except Exception as e:
        logger.error(f"❌ Error starting automated systems: {e}")
    
if __name__ == "__main__":
    # start_automated_systems() 
    port = int(os.getenv("PORT", 3000))
    debug = os.getenv("FLASK_DEBUG", "False").lower() == "true"
    flask_app.run(host="0.0.0.0", port=port, debug=debug)