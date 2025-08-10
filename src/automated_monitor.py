import time, threading, schedule, os
from datetime import datetime, timedelta
from anomaly_detector import DatabaseAnomalyDetector, AnomalyResult
from alert_config import BUSINESS_METRICS, MetricDefinition, MetricType, AnomalyType, get_metrics_by_strategy
import logging
import croniter 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FullyAutomatedMonitor:
    def __init__(self, slack_client):
        self.client = slack_client
        self.detector = DatabaseAnomalyDetector()
        self.is_running = False
        self.digest_alerts = []  # For daily digest
        self.testing_mode = os.getenv("TESTING_MODE", "true").lower() == "true" 

    def start_monitoring(self):
        """Start fully automated monitoring - no user intervention needed"""
        if self.is_running:
            return
            
        self.is_running = True
        
        # Setup automatic schedules based on your hybrid system
        self._setup_automatic_scheduling()
        
        # Start background thread
        self.monitor_thread = threading.Thread(target=self._run_continuous_monitoring, daemon=True)
        self.monitor_thread.start()

    def _should_run_now(self, cron):
        """Check if metric should run based on cron timing"""
        try:
            # Get the last run time and next run time
            prev_run = cron.get_prev(datetime)
            current_time = datetime.now()
            
            # Check if we're within 1 minute of the scheduled time
            time_since_last = (current_time - prev_run).total_seconds()
            
            # Return True if we're within 60 seconds of scheduled time
            return time_since_last <= 60
            
        except Exception as e:
            logger.error(f"Error checking cron timing: {e}")
            return False

    def _check_metric_by_cron(self, metric_id, config):
        """Check if metric should run based on cron schedule"""
        try:
            cron = croniter.croniter(config.schedule, datetime.now())
            if self._should_run_now(cron):
                anomalies = self.detector.run_single_check(metric_id)
                if anomalies:
                    self._send_realtime_alerts(anomalies)
        except Exception as e:
            logger.error(f"Error in cron check: {e}")

    def _setup_automatic_scheduling(self):
        # Individual schedules for real-time metrics only
        for metric_id, config in BUSINESS_METRICS.items():
            if config.alert_strategy == "realtime" and config.schedule:
                schedule.every().minute.do(
                    self._check_metric_by_cron, 
                    metric_id, 
                    config
                )
        
        # Single schedule for daily digest
        schedule.every().day.at("13:30").do(self._send_daily_digest)

    def _send_daily_digest(self):
        """Collect ALL digest metrics and send ONE consolidated message"""
        digest_metrics = get_metrics_by_strategy("digest")
        
        all_digest_issues = []
        for metric_id in digest_metrics:
            anomalies = self.detector.run_single_check(metric_id)
            all_digest_issues.extend(anomalies)
        
        # Send ONE consolidated digest with all issues
        self._format_and_send_digest(all_digest_issues)

    def _run_continuous_monitoring(self):
        """Run monitoring continuously in background"""
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                time.sleep(60)

    # def _check_critical_metrics(self):
    #     """Check critical metrics and send real-time alerts"""
    #     critical_metrics = [
    #         metric_id for metric_id, config in BUSINESS_METRICS.items() 
    #         if config.business_impact == "critical" and config.alert_strategy == "realtime"
    #     ]
        
    #     for metric_id in critical_metrics:
    #         try:
    #             anomalies = self.detector.run_single_check(metric_id)
    #             if anomalies:
    #                 self._send_realtime_alerts(anomalies)
    #         except Exception as e:
    #             logger.error(f"Error checking critical metric {metric_id}: {e}")

    # def _check_high_metrics(self):
    #     """Check high priority metrics and send real-time alerts"""
    #     high_metrics = [
    #         metric_id for metric_id, config in BUSINESS_METRICS.items() 
    #         if config.business_impact == "high" and config.alert_strategy == "realtime"
    #     ]
        
    #     for metric_id in high_metrics:
    #         try:
    #             anomalies = self.detector.run_single_check(metric_id)
    #             if anomalies:
    #                 self._send_realtime_alerts(anomalies)
    #         except Exception as e:
    #             logger.error(f"Error checking high metric {metric_id}: {e}")

    def _send_realtime_alerts(self, anomalies):
        """Send immediate real-time alerts"""
        for anomaly in anomalies:
            severity_emojis = {
                "low": "🟡", "medium": "🟠", 
                "high": "🔴", "critical": "🚨"
            }
            
            emoji = severity_emojis.get(anomaly.severity, "⚠️")
            
            alert_text = f"""{emoji} AUTOMATED BUSINESS ALERT

{anomaly.metric_name} - {anomaly.severity.upper()} ANOMALY

📊 Current Situation:
• Current Value: {anomaly.current_value:,.2f}
• Expected Range: {anomaly.expected_range[0]:,.2f} - {anomaly.expected_range[1]:,.2f}
• Business Impact: {anomaly.business_impact.upper()}

📝 Analysis:
{anomaly.message}

🎯 Immediate Action Required:
{anomaly.suggested_action}

📈 Historical Context:
• Average (14 days): {anomaly.historical_context.get('mean', 0):,.2f}
• Standard Deviation: {anomaly.historical_context.get('std', 0):,.2f}
• Min-Max Range: {anomaly.historical_context.get('min', 0):,.2f} - {anomaly.historical_context.get('max', 0):,.2f}

🕐 Alert Details:
• Detected: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}
• Auto-Monitor: Active

---
*This is an automated alert. Use `/check-metric {anomaly.metric_name.lower().replace(' ', '_')}` for detailed investigation.*"""

            try:
                self.client.chat_postMessage(
                    channel="#alerts",
                    text=alert_text
                )
                logger.info(f"🚀 Automated alert sent: {anomaly.metric_name}")
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")

    def _send_daily_digest(self):
        """Send daily digest for medium/low priority metrics"""
        digest_metrics = [
            metric_id for metric_id, config in BUSINESS_METRICS.items() 
            if config.alert_strategy == "digest"
        ]
        
        all_digest_anomalies = []
        
        for metric_id in digest_metrics:
            try:
                anomalies = self.detector.run_single_check(metric_id)
                all_digest_anomalies.extend(anomalies)
            except Exception as e:
                logger.error(f"Error checking digest metric {metric_id}: {e}")

        if not all_digest_anomalies:
            digest_text = f"""📊 DAILY BUSINESS DIGEST
*{datetime.now().strftime('%Y-%m-%d')}*

✅ All medium/low priority metrics are normal

📈 Metrics Monitored: {len(digest_metrics)}
📱 Issues Found: 0

*Next digest: Tomorrow at same time*"""
        else:
            digest_text = f"""📊 DAILY BUSINESS DIGEST
*{datetime.now().strftime('%Y-%m-%d')}*

⚠️ Issues Found ({len(all_digest_anomalies)})

"""
            for anomaly in all_digest_anomalies:
                digest_text += f"• {anomaly.metric_name}: {anomaly.current_value:,.0f} (Expected: {anomaly.expected_range[0]:,.0f}+)\n"

            digest_text += f"""
📈 Total Metrics: {len(digest_metrics)}
📱 Action Required: Review issues above

*Next digest: Tomorrow at same time*"""

        try:
            self.client.chat_postMessage(
                channel="#alerts",
                text=digest_text
            )
            logger.info("📋 Daily digest sent")
        except Exception as e:
            logger.error(f"Error sending daily digest: {e}")

    def stop_monitoring(self):
        """Stop automated monitoring"""
        self.is_running = False
        logger.info("⏹️ Automated monitoring stopped")

# class AutomatedBusinessMonitor:
#     """Automated monitoring system for key business metrics"""
    
#     def __init__(self, slack_client):
#         self.client = slack_client
#         self.detector = DatabaseAnomalyDetector()
#         self.is_running = False
#         self.monitor_thread = None
#         self.startup_channel = None
        
#         # Critical business hours (IST)
#         self.critical_hours = {
#             'start': 9,   # 9 AM IST
#             'end': 23     # 11 PM IST
#         }
        
#         # Alert frequency based on business impact
#         self.alert_intervals = {
#             'critical': 5,    # Every 5 minutes during business hours
#             'high': 15,       # Every 15 minutes
#             'medium': 60,     # Every hour
#             'low': 240        # Every 4 hours
#         }
        
#         self.last_alerts = {}  # Track when alerts were last sent
        
#     def start_monitoring(self, startup_channel=None):
#         """Start automated monitoring with intelligent scheduling"""
#         if self.is_running:
#             logger.info("Monitoring already running")
#             return
        
#         self.startup_channel = startup_channel
#         self.is_running = True
        
#         # Schedule different checks based on priority
#         schedule.every(5).minutes.do(self._check_critical_metrics)
#         schedule.every(15).minutes.do(self._check_high_priority_metrics)
#         schedule.every().hour.do(self._check_medium_priority_metrics)
#         schedule.every(4).hours.do(self._check_low_priority_metrics)
        
#         # Daily health check at 9 AM IST
#         schedule.every().day.at("09:00").do(self._daily_health_check)
        
#         # Start monitoring thread
#         self.monitor_thread = threading.Thread(target=self._run_scheduler, daemon=True)
#         self.monitor_thread.start()
        
#         logger.info("🚀 Automated business monitoring started")
#         # Only send startup notification if channel is provided
#         if self.startup_channel:
#             self._send_startup_notification()
        
#     def stop_monitoring(self):
#         """Stop automated monitoring"""
#         self.is_running = False
#         schedule.clear()
#         logger.info("⏹️ Automated monitoring stopped")
        
#     def _run_scheduler(self):
#         """Main scheduler loop"""
#         while self.is_running:
#             try:
#                 schedule.run_pending()
#                 time.sleep(60)  # Check every minute
#             except Exception as e:
#                 logger.error(f"Scheduler error: {e}")
#                 time.sleep(60)
                
#     def _is_business_hours(self):
#         """Check if current time is within critical business hours"""
#         current_hour = datetime.now().hour
#         return self.critical_hours['start'] <= current_hour <= self.critical_hours['end']
        
#     def _check_critical_metrics(self):
#         """Check critical business metrics (revenue, user engagement)"""
#         if not self._is_business_hours():
#             return
            
#         critical_metrics = [
#             'daily_active_users',
#             'daily_deposit_amount', 
#             'contest_joins_daily'
#         ]
        
#         self._run_targeted_checks(critical_metrics, 'critical')
        
#     def _check_high_priority_metrics(self):
#         """Check high priority metrics"""
#         high_priority_metrics = [
#             'daily_deposit_count',
#             'payment_errors_daily',
#             'contest_failure_rate'
#         ]
        
#         self._run_targeted_checks(high_priority_metrics, 'high')
        
#     def _check_medium_priority_metrics(self):
#         """Check medium priority metrics"""
#         medium_priority_metrics = [
#             'average_loading_time',
#             'otp_failures_daily',
#             'kyc_verifications_daily'
#         ]
        
#         self._run_targeted_checks(medium_priority_metrics, 'medium')
        
#     def _check_low_priority_metrics(self):
#         """Check low priority metrics"""
#         # Check all remaining metrics
#         all_metrics = set(BUSINESS_METRICS.keys())
#         priority_metrics = set([
#             'daily_active_users', 'daily_deposits', 'deposit_success_rate',
#             'contest_joins', 'payment_errors', 'contest_join_failure_rate',
#             'average_loading_time', 'otp_failure_rate', 'kyc_verifications'
#         ])
        
#         low_priority_metrics = list(all_metrics - priority_metrics)
#         self._run_targeted_checks(low_priority_metrics, 'low')
        
#     def _run_targeted_checks(self, metric_ids, priority_level):
#         """Run anomaly checks for specific metrics"""
#         try:
#             anomalies = []
            
#             for metric_id in metric_ids:
#                 if metric_id not in BUSINESS_METRICS:
#                     continue
                    
#                 # Check if we should send alert based on timing
#                 if not self._should_send_alert(metric_id, priority_level):
#                     continue
                    
#                 try:
#                     metric_anomalies = self.detector.run_single_check(metric_id)
#                     anomalies.extend(metric_anomalies)
                    
#                     # Update last alert time if anomaly found
#                     if metric_anomalies:
#                         self.last_alerts[metric_id] = datetime.now()
                        
#                 except Exception as e:
#                     logger.error(f"Error checking metric {metric_id}: {e}")
                    
#             # Send alerts if any anomalies found
#             if anomalies:
#                 self._send_automated_alerts(anomalies, priority_level)
                
#         except Exception as e:
#             logger.error(f"Error in targeted checks for {priority_level}: {e}")
            
#     def _should_send_alert(self, metric_id, priority_level):
#         """Determine if alert should be sent based on timing and priority"""
#         if metric_id not in self.last_alerts:
#             return True
            
#         last_alert_time = self.last_alerts[metric_id]
#         time_since_last = (datetime.now() - last_alert_time).total_seconds() / 60
        
#         min_interval = self.alert_intervals.get(priority_level, 60)
        
#         return time_since_last >= min_interval
        
#     def _send_automated_alerts(self, anomalies, priority_level):
#         """Send automated alerts with context about monitoring"""
#         timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
        
#         for anomaly in anomalies:
#             # Get severity emoji
#             severity_emojis = {
#                 "low": "🟡", "medium": "🟠", 
#                 "high": "🔴", "critical": "🚨"
#             }
            
#             priority_emojis = {
#                 "low": "📊", "medium": "⚠️", 
#                 "high": "🔥", "critical": "🚨"
#             }
            
#             severity_emoji = severity_emojis.get(anomaly.severity, "⚠️")
#             priority_emoji = priority_emojis.get(priority_level, "📊")
            
#             alert_text = f"""{priority_emoji} AUTOMATED BUSINESS ALERT

# {severity_emoji} {anomaly.metric_name} - {anomaly.severity.upper()} ANOMALY

# 📊 Current Situation:
# • Current Value: {anomaly.current_value:,.2f}
# • Expected Range: {anomaly.expected_range[0]:,.2f} - {anomaly.expected_range[1]:,.2f}
# • Business Impact: {anomaly.business_impact.upper()}

# 📝 Analysis:
# {anomaly.message}

# 🎯 Immediate Action Required:
# {anomaly.suggested_action}

# 📈 Historical Context:
# • Average (14 days): {anomaly.historical_context.get('mean', 0):,.2f}
# • Standard Deviation: {anomaly.historical_context.get('std', 0):,.2f}
# • Min-Max Range: {anomaly.historical_context.get('min', 0):,.2f} - {anomaly.historical_context.get('max', 0):,.2f}

# 🕐 Alert Details:
# • Detected: {timestamp}
# • Priority Level: {priority_level.upper()}
# • Auto-Monitor: Active
# • Next Check: {self.alert_intervals.get(priority_level, 60)} minutes

# ---
# *This is an automated alert. Reply to this thread for manual analysis or use `/check-metric {anomaly.metric_name.lower().replace(' ', '_')}` for detailed investigation.*"""

#             # Send to appropriate channels
#             channels = self._get_channels_for_metric(anomaly.metric_name)
            
#             for channel in channels:
#                 try:
#                     self.client.chat_postMessage(
#                         channel=channel,
#                         text=alert_text
#                     )
#                     logger.info(f"🚀 Automated alert sent to {channel}: {anomaly.metric_name}")
                    
#                 except Exception as e:
#                     logger.error(f"Failed to send alert to {channel}: {e}")
                    
#     def _get_channels_for_metric(self, metric_name):
#         """Get alert channels for a specific metric"""
#         # Find the metric configuration
#         for metric_id, metric_def in BUSINESS_METRICS.items():
#             if metric_def.name == metric_name:
#                 return metric_def.alert_channels
                
#         # Default fallback
#         return ["#alerts"]
        
#     def _daily_health_check(self):
#         """Send daily health check summary"""
#         try:
#             summary_text = f"""🌅 DAILY BUSINESS HEALTH CHECK

# 📅 Date: {datetime.now().strftime('%Y-%m-%d')}
# 🕘 Time: 09:00 IST

# 🔍 System Status:
# • Automated Monitoring: ✅ Active
# • Database Connection: ✅ Healthy
# • Alert Channels: ✅ Validated

# 📊 Monitoring Schedule:
# • Critical Metrics: Every 5 minutes (business hours)
# • High Priority: Every 15 minutes
# • Medium Priority: Every hour
# • Low Priority: Every 4 hours

# 📈 Metrics Being Monitored: {len(BUSINESS_METRICS)} business metrics

# *Starting today's automated monitoring. Have a productive day! 🚀*

# ---
# *Use `/check-anomalies` for manual checks or `/check-metric [metric_name]` for specific analysis.*"""

#             # Use startup channel if available, fallback to #general
#             channel = self.startup_channel if self.startup_channel else "#general"
#             self.client.chat_postMessage(
#                 channel=channel,  # Send to main channel
#                 text=summary_text
#             )
            
#         except Exception as e:
#             logger.error(f"Error in daily health check: {e}")
            
#     def _send_startup_notification(self):
#         """Notify about monitoring startup"""
#         startup_text = f"""🚀 AUTOMATED MONITORING ACTIVATED

# 📊 Business Intelligence Bot is now actively monitoring your key business metrics.

# 🔄 What's Happening:
# • Real-time anomaly detection across {len(BUSINESS_METRICS)} business metrics
# • Intelligent alerting based on business impact and priority
# • Proactive notifications to relevant teams
# • 24/7 monitoring with business hours optimization

# ⚡ Alert Schedule:
# • Critical: Every 5 min (business hours)
# • High: Every 15 min
# • Medium: Every hour
# • Low: Every 4 hours

# 📱 You'll receive alerts for:
# • Sudden spikes or drops in key metrics
# • Threshold violations
# • System performance issues
# • Revenue and user engagement anomalies

# *No more waiting for data - insights come to you automatically! 📈*

# Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}"""

#         try:
#             self.client.chat_postMessage(
#                 channel=self.startup_channel,
#                 text=startup_text
#             )
#         except Exception as e:
#             logger.error(f"Error sending startup notification: {e}")

#     def get_monitoring_status(self):
#         """Get current monitoring status"""
#         return {
#             "is_running": self.is_running,
#             "metrics_monitored": len(BUSINESS_METRICS),
#             "critical_hours": self.critical_hours,
#             "alert_intervals": self.alert_intervals,
#             "last_alerts_count": len(self.last_alerts),
#             "business_hours_active": self._is_business_hours()
#         }
