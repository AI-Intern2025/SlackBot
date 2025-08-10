# alert_config.py
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable
from enum import Enum
import json

class MetricType(Enum):
    COUNT = "count"
    SUM = "sum"
    AVERAGE = "average"
    RATIO = "ratio"
    UNIQUE_COUNT = "unique_count"
    PERCENTAGE = "percentage"

class AnomalyType(Enum):
    SPIKE = "spike"           # Sudden increase
    DROP = "drop"             # Sudden decrease
    THRESHOLD = "threshold"   # Above/below threshold
    TREND = "trend"          # Unusual trend
    ZERO = "zero"            # Unexpected zero values

@dataclass
class MetricDefinition:
    name: str
    description: str
    table: str
    metric_type: MetricType
    value_column: Optional[str] = None
    filter_conditions: Optional[str] = None
    group_by: Optional[str] = None
    time_column: str = "alt_event_date"
    
    # Anomaly detection settings
    enabled_anomalies: List[AnomalyType] = None
    spike_threshold: float = 2.0        # Standard deviations
    drop_threshold: float = -2.0        # Standard deviations
    min_threshold: Optional[float] = None
    max_threshold: Optional[float] = None
    
    # Alert settings
    alert_strategy: str = "realtime"  # "realtime" or "digest"
    schedule: str = "*/5 * * * *"     # Cron expression
    alert_channels: List[str] = None
    alert_frequency: str = "daily"      # daily, hourly, real-time
    business_impact: str = "medium"     # low, medium, high, critical
    
    def __post_init__(self):
        if self.enabled_anomalies is None:
            self.enabled_anomalies = [AnomalyType.SPIKE, AnomalyType.DROP]
        if self.alert_channels is None:
            self.alert_channels = ["#alerts"]

# ========================================
# BUSINESS METRICS - ORGANIZED BY PRIORITY
# ========================================

BUSINESS_METRICS = {
    
    # ==========================================
    # 🚨 CRITICAL PRIORITY - REAL-TIME ALERTS
    # ==========================================
    
    "daily_active_users": MetricDefinition(
        name="Daily Active Users",
        description="Number of unique users per day across all activities",
        table="alt_leaderboardviewed",
        metric_type=MetricType.UNIQUE_COUNT,
        value_column="member_code",
        business_impact="critical",
        alert_strategy="realtime",
        schedule="*/1 * * * *",       # Every 15 minutes
        alert_channels=["#alerts"],
        min_threshold=10,
    ),
    
    "daily_deposits": MetricDefinition(
        name="Daily Deposit Amount",
        description="Total deposit amount per day",
        table="alt_depositsuccessclient",
        metric_type=MetricType.SUM,
        value_column="alt_depositamount",
        business_impact="critical",
        alert_strategy="realtime",
        schedule="*/20 * * * *",       # Every 20 minutes
        alert_channels=["#alerts"],
        min_threshold=1000,
    ),
    
    # ==========================================
    # 🔥 HIGH PRIORITY - REAL-TIME ALERTS
    # ==========================================
    
    "deposit_success_rate": MetricDefinition(
        name="Deposit Success Rate",
        description="Percentage of successful deposits vs payment errors",
        table="alt_depositsuccessclient",
        metric_type=MetricType.PERCENTAGE,
        business_impact="high",
        alert_strategy="realtime",
        schedule="*/2 * * * *",       # Every 25 minutes
        alert_channels=["#alerts"],
        min_threshold=0.95,
    ),
    
    "contest_joins": MetricDefinition(
        name="Daily Contest Joins",
        description="Number of successful contest joins per day",
        table="alt_contestjoinedclient",
        metric_type=MetricType.COUNT,
        filter_conditions="alt_contestjoinstatus = 'success'",
        business_impact="high",
        alert_strategy="realtime",
        schedule="*/30 * * * *",       # Every 30 minutes
        alert_channels=["#alerts"],
    ),
    
    "contest_join_failure_rate": MetricDefinition(
        name="Contest Join Failure Rate",
        description="Percentage of failed contest joins",
        table="alt_contestjoinfailed",
        metric_type=MetricType.COUNT,
        business_impact="high",
        alert_strategy="realtime",
        schedule="*/30 * * * *",       # Every 30 minutes
        alert_channels=["#alerts"],
        max_threshold=0.1,
    ),
    
    "payment_errors": MetricDefinition(
        name="Daily Payment Errors",
        description="Number of payment errors per day",
        table="alt_paymenterrorshown",
        metric_type=MetricType.COUNT,
        business_impact="high",
        alert_strategy="realtime",
        schedule="*/45 * * * *",       # Every 45 minutes
        alert_channels=["#alerts"],
        max_threshold=50,
    ),
    
    # ==========================================
    # ⚠️ MEDIUM PRIORITY - DAILY DIGEST
    # ==========================================
    # No individual schedule needed

    "average_loading_time": MetricDefinition(
        name="Average Page Loading Time",
        description="Average loading time for contest interfaces",
        table="alt_contestinterfaceloaded",
        metric_type=MetricType.AVERAGE,
        value_column="alt_loadingtime",
        business_impact="medium",
        alert_strategy="digest",
        schedule=None,
        alert_channels=["#alerts"],
        max_threshold=5000,
    ),

    "otp_failure_rate": MetricDefinition(
        name="OTP Failure Rate",
        description="Percentage of OTP submission failures",
        table="alt_otpsubmissionfailed",
        metric_type=MetricType.COUNT,
        business_impact="medium",
        alert_strategy="digest",
        schedule=None,
        alert_channels=["#alerts"],
        max_threshold=0.15,
    ),

    "kyc_verifications": MetricDefinition(
        name="Daily KYC Verifications", 
        description="Number of KYC verification attempts per day",
        table="alt_kycverificationtapped",
        metric_type=MetricType.COUNT,
        business_impact="medium",
        alert_strategy="digest",
        schedule=None,
        alert_channels=["#alerts"],
    ),
    
    "failed_payments_hour": MetricDefinition(
        name="Failed Payments Hour",
        description="Track failed payments per hour, critical priority",
        table="alt_depositsuccessclient",
        metric_type=MetricType.COUNT,
        business_impact="critical",
        alert_strategy="realtime",
        schedule="0 * * * *",
        min_threshold=None,
        max_threshold=None,
        alert_channels=['#alerts'],
    ),

}

# ========================================
# HELPER FUNCTIONS FOR YOUR TEAM
# ========================================

def get_metrics_by_strategy(strategy: str) -> List[str]:
    """Get metrics filtered by alert strategy"""
    return [
        metric_id for metric_id, config in BUSINESS_METRICS.items()
        if config.alert_strategy == strategy
    ]

def get_metrics_by_impact(impact: str) -> List[str]:
    """Get metrics filtered by business impact"""
    return [
        metric_id for metric_id, config in BUSINESS_METRICS.items()
        if config.business_impact == impact
    ]
