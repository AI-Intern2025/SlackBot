# anomaly_detector.py
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from sqlalchemy import create_engine, text
import logging
from alert_config import MetricDefinition, AnomalyType, BUSINESS_METRICS
import json
from alert_config import MetricType
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class AnomalyResult:
    metric_name: str
    metric_description: str
    anomaly_type: AnomalyType
    current_value: float
    expected_range: Tuple[float, float]
    severity: str
    message: str
    timestamp: datetime
    business_impact: str
    suggested_action: str
    historical_context: Dict

class DatabaseAnomalyDetector:
    """Database-agnostic anomaly detection system"""
    
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL must be provided")
        
        self.engine = create_engine(self.database_url)
        self.metrics = BUSINESS_METRICS
        self.historical_days = 14  # Days to look back for baseline
        
    def add_custom_metric(self, metric_id: str, metric_def: MetricDefinition):
        """Add custom metric definition"""
        self.metrics[metric_id] = metric_def
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the current database"""
        try:
            query = text(f"SELECT 1 FROM {table_name} LIMIT 1")
            with self.engine.connect() as conn:
                conn.execute(query)
            return True
        except Exception as e:
            logger.warning(f"Table {table_name} does not exist or is not accessible: {e}")
            return False
    
    def get_available_metrics(self) -> List[str]:
        """Get list of metrics that can be checked based on available tables"""
        available_metrics = []
        for metric_id, metric_def in self.metrics.items():
            if self.check_table_exists(metric_def.table):
                available_metrics.append(metric_id)
            else:
                logger.info(f"Skipping metric '{metric_id}' - table '{metric_def.table}' not available")
        return available_metrics
    
    def build_metric_query(self, metric_def: MetricDefinition, start_date: str, end_date: str) -> str:
        """Build SQL query for a specific metric"""
        base_table = metric_def.table
        time_col = metric_def.time_column
        
        # Base WHERE clause
        where_conditions = [f"{time_col} BETWEEN '{start_date}' AND '{end_date}'"]
        if metric_def.filter_conditions:
            where_conditions.append(metric_def.filter_conditions)
        
        where_clause = " AND ".join(where_conditions)
        
        # Build SELECT based on metric type
        if metric_def.metric_type == MetricType.COUNT:
            select_clause = "COUNT(*)"
        elif metric_def.metric_type == MetricType.UNIQUE_COUNT:
            select_clause = f"COUNT(DISTINCT {metric_def.value_column})"
        elif metric_def.metric_type == MetricType.SUM:
            select_clause = f"SUM({metric_def.value_column})"
        elif metric_def.metric_type == MetricType.AVERAGE:
            select_clause = f"AVG({metric_def.value_column})"
        else:
            select_clause = f"COUNT(*)"  # Default fallback
        
        # Group by date for time series
        if metric_def.group_by:
            group_clause = f"GROUP BY {time_col}, {metric_def.group_by}"
            select_clause = f"{time_col}, {metric_def.group_by}, {select_clause}"
        else:
            group_clause = f"GROUP BY {time_col}"
            select_clause = f"{time_col}, {select_clause}"
        
        query = f"""
        SELECT {time_col}::date AS {time_col}, {select_clause}
        FROM {base_table}
        WHERE {where_clause}
        GROUP BY {time_col}::date
        ORDER BY {time_col}::date
        """
        
        return query
    
    def execute_metric_query(self, metric_def: MetricDefinition, days_back: int = 14) -> pd.DataFrame:
        """Execute metric query and return results as DataFrame"""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        query = self.build_metric_query(metric_def, str(start_date), str(end_date))
        
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            
            if df.empty:
                logger.warning(f"No data found for metric: {metric_def.name}")
                return df
            
            # Ensure we have a proper time column
            df[metric_def.time_column] = pd.to_datetime(df[metric_def.time_column])
            return df
            
        except Exception as e:
            logger.error(f"Error executing metric query for {metric_def.name}: {e}")
            logger.error(f"Query: {query}")
            return pd.DataFrame()
    
    def calculate_baseline_stats(self, data: pd.DataFrame) -> Dict:
        """Calculate baseline statistics for anomaly detection"""
        if data.empty or len(data) < 1:
            return {"mean": 0, "std": 1, "min": 0, "max": 0, "count": 0}
        
        values = data['metric_value'].dropna()
        
        return {
            "mean": float(values.mean()),
            "std": max(float(values.std()) if len(values) > 1 else 1.0, 0.1),
            "min": float(values.min()),
            "max": float(values.max()),
            "count": len(values),
            "median": float(values.median()),
            "q1": float(values.quantile(0.25)),
            "q3": float(values.quantile(0.75))
        }
    
    def detect_anomalies(self, metric_id: str, metric_def: MetricDefinition, current_data: pd.DataFrame, baseline_stats: Dict) -> List[AnomalyResult]:
        """Detect anomalies in current data"""
        anomalies = []
        
        if current_data.empty:
            # No data is itself an anomaly
            anomalies.append(AnomalyResult(
                metric_name=metric_def.name,
                metric_description=metric_def.description,
                anomaly_type=AnomalyType.ZERO,
                current_value=0,
                expected_range=(baseline_stats["min"], baseline_stats["max"]),
                severity="high",
                message=f"No data found for {metric_def.name} - this could indicate a system issue",
                timestamp=datetime.now(),
                business_impact=metric_def.business_impact,
                suggested_action="Check data pipeline and system health",
                historical_context=baseline_stats
            ))
            return anomalies
        
        latest_value = float(current_data['metric_value'].iloc[-1])
        mean = baseline_stats["mean"]
        std = baseline_stats["std"]
        
        # PRIORITY: Check threshold violations first (these are business rules)
        threshold_anomaly = None

        if metric_def.min_threshold and latest_value < metric_def.min_threshold:
            threshold_anomaly = AnomalyResult(
            metric_name=metric_def.name,
            metric_description=metric_def.description,
            anomaly_type=AnomalyType.THRESHOLD,
            current_value=latest_value,
            expected_range=(metric_def.min_threshold, float('inf')),
            severity="high",
            message=f"{metric_def.name} ({latest_value}) is below minimum threshold ({metric_def.min_threshold})",
            timestamp=datetime.now(),
            business_impact=metric_def.business_impact,
            suggested_action=f"Investigate why {metric_def.name} is below acceptable levels",
            historical_context=baseline_stats
        )
        elif metric_def.max_threshold and latest_value > metric_def.max_threshold:
            threshold_anomaly = AnomalyResult(
                metric_name=metric_def.name,
                metric_description=metric_def.description,
                anomaly_type=AnomalyType.THRESHOLD,
                current_value=latest_value,
                expected_range=(0, metric_def.max_threshold),
                severity="high",
                message=f"{metric_def.name} ({latest_value}) is above maximum threshold ({metric_def.max_threshold})",
                timestamp=datetime.now(),
                business_impact=metric_def.business_impact,
                suggested_action=f"Investigate why {metric_def.name} is exceeding acceptable levels",
                historical_context=baseline_stats
            )
        
        # If we have a threshold anomaly, use it (business rules take priority)
        if threshold_anomaly:
            anomalies.append(threshold_anomaly)
            return anomalies  # ← EARLY RETURN - prevents statistical check
    
        # Only check statistical anomalies if no threshold violations
        if AnomalyType.SPIKE in metric_def.enabled_anomalies:
            if latest_value > mean + (metric_def.spike_threshold * std) and std > 0:
                severity = "critical" if latest_value > mean + (3 * std) else "high"
                anomalies.append(AnomalyResult(
                    metric_name=metric_def.name,
                    metric_description=metric_def.description,
                    anomaly_type=AnomalyType.SPIKE,
                    current_value=latest_value,
                    expected_range=(mean - std, mean + std),
                    severity=severity,
                    message=f"{metric_def.name} is {((latest_value - mean) / mean * 100):.1f}% above normal",
                    timestamp=datetime.now(),
                    business_impact=metric_def.business_impact,
                    suggested_action=self._get_spike_action(metric_id),
                    historical_context=baseline_stats
                ))
        
        # Check for drops
        if AnomalyType.DROP in metric_def.enabled_anomalies:
            if latest_value < mean + (metric_def.drop_threshold * std) and std > 0:
                severity = "critical" if latest_value < mean - (3 * std) else "high"
                anomalies.append(AnomalyResult(
                    metric_name=metric_def.name,
                    metric_description=metric_def.description,
                    anomaly_type=AnomalyType.DROP,
                    current_value=latest_value,
                    expected_range=(mean - std, mean + std),
                    severity=severity,
                    message=f"{metric_def.name} is {((mean - latest_value) / mean * 100):.1f}% below normal",
                    timestamp=datetime.now(),
                    business_impact=metric_def.business_impact,
                    suggested_action=self._get_drop_action(metric_id),
                    historical_context=baseline_stats
                ))
        
        return anomalies
    
    def _get_spike_action(self, metric_id: str) -> str:
        """Get suggested action for spike anomalies"""
        actions = {
            "daily_active_users": "Check for marketing campaigns or viral content",
            "daily_deposits": "Verify payment processing systems and check for promotional campaigns",
            "contest_joins": "Check contest configurations and promotional activities",
            "payment_errors": "Investigate payment gateway issues immediately",
            "average_loading_time": "Check server performance and database queries"
        }
        return actions.get(metric_id, "Investigate the cause of this unusual increase")
    
    def _get_drop_action(self, metric_id: str) -> str:
        """Get suggested action for drop anomalies"""
        actions = {
            "daily_active_users": "Check app functionality and user acquisition channels",
            "daily_deposits": "Investigate payment systems and user experience",
            "contest_joins": "Check contest availability and user engagement",
            "deposit_success_rate": "Immediate investigation of payment processing required",
        }
        return actions.get(metric_id, "Investigate the cause of this unusual decrease")
    
    def run_all_checks(self) -> List[AnomalyResult]:
        """Run anomaly detection on all available metrics"""
        all_anomalies = []
        available_metrics = self.get_available_metrics()
        
        if not available_metrics:
            logger.warning("No available metrics found for anomaly detection")
            return all_anomalies
    
        logger.info(f"Checking {len(available_metrics)} available metrics for anomalies...")
        
        failed_metrics = []

        for metric_id in available_metrics:
            metric_def = self.metrics[metric_id]
            
            try:
                # Get historical data for baseline
                historical_data = self.execute_metric_query(metric_def, self.historical_days)
                
                if historical_data.empty:
                    logger.warning(f"No historical data found for metric: {metric_def.name}")
                    continue

                # Get current day data
                current_data = self.execute_metric_query(metric_def, 1)
                
                # Calculate baseline statistics
                baseline_stats = self.calculate_baseline_stats(historical_data)
                
                if baseline_stats["count"] == 0:
                    logger.warning(f"Insufficient baseline data for metric: {metric_def.name}")
                    continue

                # Detect anomalies
                anomalies = self.detect_anomalies(metric_id, metric_def, current_data, baseline_stats)
                all_anomalies.extend(anomalies)
                
                if anomalies:
                    logger.info(f"Found {len(anomalies)} anomalies for metric: {metric_def.name}")
                
            except Exception as e:
                failed_metrics.append((metric_id, str(e)))
                logger.error(f"Error checking metric {metric_id}: {e}")
        
        # Report failed metrics
        if failed_metrics:
            logger.warning(f"Failed to check {len(failed_metrics)} metrics:")
            for metric_id, error in failed_metrics:
                logger.warning(f"  - {metric_id}: {error}")

        return all_anomalies
    
    def run_single_check(self, metric_id: str) -> List[AnomalyResult]:
        """Run anomaly detection on a single metric"""
        if metric_id not in self.metrics:
            raise ValueError(f"Unknown metric: {metric_id}")
        
        if not self.check_table_exists(self.metrics[metric_id].table):
            raise ValueError(f"Table {self.metrics[metric_id].table} not available")
        
        metric_def = self.metrics[metric_id]
        
        # Get historical data for baseline
        historical_data = self.execute_metric_query(metric_def, self.historical_days)
        
        # Get current day data
        current_data = self.execute_metric_query(metric_def, 1)
        
        # Calculate baseline statistics
        baseline_stats = self.calculate_baseline_stats(historical_data)
        
        # Detect anomalies
        return self.detect_anomalies(metric_id, metric_def, current_data, baseline_stats)
    
        # Add this method to DatabaseAnomalyDetector class
    def build_metric_query(self, metric_def: MetricDefinition, start_date: str, end_date: str) -> str:
        """Build optimized SQL query for Dream11 metrics"""
        base_table = metric_def.table
        time_col = metric_def.time_column
        
        # Base WHERE clause with proper date filtering
        where_conditions = [f"{time_col} BETWEEN '{start_date}' AND '{end_date}'"]
        
        if metric_def.filter_conditions:
            where_conditions.append(metric_def.filter_conditions)
        
        where_clause = " AND ".join(where_conditions)
        
        # Build SELECT based on metric type
        if metric_def.metric_type == MetricType.COUNT:
            select_clause = "COUNT(*) as metric_value"
        elif metric_def.metric_type == MetricType.UNIQUE_COUNT:
            select_clause = f"COUNT(DISTINCT {metric_def.value_column}) as metric_value"
        elif metric_def.metric_type == MetricType.SUM:
            select_clause = f"SUM(COALESCE({metric_def.value_column}, 0)) as metric_value"
        elif metric_def.metric_type == MetricType.AVERAGE:
            select_clause = f"AVG(COALESCE({metric_def.value_column}, 0)) as metric_value"
        else:
            select_clause = "COUNT(*) as metric_value"  # Default fallback
        
        # Group by date for time series analysis
        query = f"""
        SELECT {time_col}::date as {time_col}, {select_clause}
        FROM {base_table}
        WHERE {where_clause}
        GROUP BY {time_col}::date
        ORDER BY {time_col}::date
        """

        return query

    def build_composite_metric_query(self, composite_metric: dict, start_date: str, end_date: str) -> str:
        """Build queries for composite metrics like conversion rates"""
        numerator_table = composite_metric["numerator_table"]
        denominator_table = composite_metric["denominator_table"]
        
        query = f"""
        WITH numerator AS (
            SELECT alt_event_date::date as alt_event_date, COUNT(*) as num_count
            FROM {numerator_table}
            WHERE alt_event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY alt_event_date::date
        ),
        denominator AS (
            SELECT alt_event_date::date as alt_event_date, COUNT(*) as den_count  
            FROM {denominator_table}
            WHERE alt_event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY alt_event_date::date
        )
        SELECT 
            COALESCE(n.alt_event_date, d.alt_event_date) as alt_event_date,
            CASE 
                WHEN COALESCE(d.den_count, 0) = 0 THEN 0
                ELSE COALESCE(n.num_count, 0)::float / d.den_count 
            END as metric_value
        FROM numerator n
        FULL OUTER JOIN denominator d ON n.alt_event_date = d.alt_event_date
        ORDER BY alt_event_date
        """
        
        return query
    def debug_table_data(self, table_name):
        """Debug method to check table data"""
        try:
            # Check if table has any data
            query = f"SELECT COUNT(*), MAX(alt_event_date), MIN(alt_event_date) FROM {table_name}"
            with self.engine.connect() as conn:
                result = conn.execute(text(query)).fetchone()
                
            total_rows, max_date, min_date = result
            logger.info(f"Table {table_name}: {total_rows} rows, Date range: {min_date} to {max_date}")
            
            # Check recent data (last 7 days)
            recent_query = f"SELECT COUNT(*) FROM {table_name} WHERE alt_event_date >= CURRENT_DATE - INTERVAL '7 days'"
            with self.engine.connect() as conn:
                recent_count = conn.execute(text(recent_query)).fetchone()[0]
                
            logger.info(f"Table {table_name}: {recent_count} rows in last 7 days")
            return total_rows > 0, recent_count > 0
            
        except Exception as e:
            logger.error(f"Error checking table {table_name}: {e}")
            return False, False

