"""
======================================================================================
🔧 PHASE 4.1 — PIPELINE CONFIGURATION & UTILITIES
======================================================================================
Centralized configuration for production orchestration.
Includes: environment setup, parameter management, audit logging, error handling.
======================================================================================
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum
import logging

# ============================================================================
# ENUMERATIONS
# ============================================================================

class RunMode(Enum):
    """Pipeline execution modes"""
    FULL = "full"                  # Complete reload
    INCREMENTAL = "incremental"    # Only process new data
    REPAIR = "repair"              # Rebuild specific failed layers
    
class TaskStatus(Enum):
    """Task execution states"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    RETRY = "RETRY"
    
class Environment(Enum):
    """Deployment environments"""
    DEV = "dev"
    STAGING = "staging"
    PRODUCTION = "production"


# ============================================================================
# DATACLASSES FOR TYPE SAFETY
# ============================================================================

@dataclass
class PipelineConfig:
    """Global pipeline configuration"""
    catalog_name: str = "fintech_analytics"
    environment: str = "production"
    run_mode: str = "incremental"
    lookback_days: int = 7
    max_retries: int = 3
    retry_delay_seconds: int = 30
    enable_data_quality: bool = True
    enable_audit_logging: bool = True
    timeout_minutes: int = 60
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class TaskConfig:
    """Individual task configuration"""
    task_name: str
    task_type: str  # "notebook" | "sql" | "python"
    depends_on: List[str]
    timeout_minutes: int = 30
    max_retries: int = 2
    critical: bool = True  # If fails, stop pipeline
    skip_on_condition: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class PipelineRun:
    """Pipeline execution tracking"""
    run_id: str
    execution_date: str
    environment: str
    run_mode: str
    status: str = "RUNNING"
    start_time: str = None
    end_time: str = None
    duration_seconds: int = None
    total_tasks: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0
    skipped_tasks: int = 0
    error_message: str = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure structured logging for pipeline"""
    logger = logging.getLogger("fintech_pipeline")
    logger.setLevel(getattr(logging, log_level))
    
    # Console handler
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

LOGGER = setup_logging()


# ============================================================================
# PIPELINE METADATA & CONTRACTS
# ============================================================================

PIPELINE_TASKS = {
    "silver_transformation": TaskConfig(
        task_name="silver_transformation",
        task_type="notebook",
        depends_on=[],
        timeout_minutes=30,
        max_retries=2,
        critical=True
    ),
    "gold_analytics": TaskConfig(
        task_name="gold_analytics",
        task_type="notebook",
        depends_on=["silver_transformation"],
        timeout_minutes=20,
        max_retries=2,
        critical=True
    ),
    "table_optimization": TaskConfig(
        task_name="table_optimization",
        task_type="sql",
        depends_on=["gold_analytics"],
        timeout_minutes=15,
        max_retries=1,
        critical=False  # Non-critical optimization
    ),
    "data_quality_checks": TaskConfig(
        task_name="data_quality_checks",
        task_type="python",
        depends_on=["gold_analytics"],
        timeout_minutes=10,
        max_retries=1,
        critical=True
    ),
}

DATA_QUALITY_CONTRACTS = {
    "silver_fact_insider_transactions": {
        "required_columns": [
            "transaction_id", "company_name", "filing_date",
            "insider_name", "transaction_amount", "shares"
        ],
        "null_checks": [
            "transaction_id", "company_name", "filing_date"
        ],
        "min_rows": 100,
        "max_null_percentage": 5.0
    },
    "silver_fact_institutional_holdings": {
        "required_columns": [
            "holding_id", "company_name", "quarter_date",
            "institution_name", "shares_held", "conviction_score"
        ],
        "null_checks": [
            "holding_id", "company_name", "quarter_date"
        ],
        "min_rows": 50,
        "max_null_percentage": 5.0
    },
    "gold_risk_indicators": {
        "required_columns": [
            "transaction_id", "risk_score", "risk_category",
            "insider_type", "filing_date"
        ],
        "null_checks": ["risk_score", "risk_category"],
        "min_rows": 100,
        "numeric_ranges": {
            "risk_score": (0.0, 100.0)
        }
    }
}

PARTITION_STRATEGY = {
    "silver.silver_fact_insider_transactions": [
        "year(filing_date)",
        "month(filing_date)"
    ],
    "silver.silver_fact_institutional_holdings": [
        "quarter(quarter_date)",
        "year(quarter_date)"
    ],
    "gold.gold_risk_indicators": [
        "year(filing_date)",
        "month(filing_date)"
    ]
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_execution_context() -> Dict[str, Any]:
    """Extract Databricks execution context"""
    try:
        dbutils = globals().get("dbutils")
        if dbutils:
            return {
                "notebook_path": dbutils.notebook.entry_point.getDbfsPath(),
                "user": dbutils.notebook.run.__self__.entry_point_info.user,
                "job_id": dbutils.jobs.taskRuns.getCurrentRunId() if hasattr(dbutils.jobs.taskRuns, "getCurrentRunId") else None
            }
    except:
        pass
    
    return {
        "notebook_path": os.getenv("DATABRICKS_NOTEBOOK_PATH", "unknown"),
        "user": os.getenv("DATABRICKS_USER", "unknown"),
        "job_id": os.getenv("DATABRICKS_JOB_ID", None)
    }

def generate_run_id() -> str:
    """Generate unique run identifier"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def get_execution_date_range(run_mode: str, lookback_days: int = 7) -> tuple:
    """Calculate date range for data processing"""
    end_date = datetime.now()
    
    if run_mode == "full":
        start_date = datetime(2020, 1, 1)  # Arbitrary start
    elif run_mode == "incremental":
        start_date = end_date - timedelta(days=lookback_days)
    else:
        start_date = end_date - timedelta(days=1)
    
    return (start_date.date(), end_date.date())

def flatten_dict(d: Dict, parent_key: str = '', sep: str = '.') -> Dict:
    """Flatten nested dictionary for logging"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


# ============================================================================
# INITIALIZATION
# ============================================================================

if __name__ == "__main__":
    LOGGER.info("Pipeline configuration loaded successfully")
    config = PipelineConfig()
    LOGGER.info(f"Config: {config.to_dict()}")
