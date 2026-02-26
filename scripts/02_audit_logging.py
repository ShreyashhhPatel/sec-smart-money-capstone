"""
======================================================================================
🔍 PHASE 4.2 — AUDIT LOGGING & ERROR HANDLING FRAMEWORK
======================================================================================
Production-grade audit tables, error tracking, and recovery mechanisms.
======================================================================================
"""

from datetime import datetime
from typing import Dict, Any, List, Optional
import json
import traceback

LOGGER = None  # Will be injected from config

# ============================================================================
# AUDIT SCHEMA INITIALIZATION
# ============================================================================

def initialize_audit_tables(spark, catalog: str = "fintech_analytics", 
                           schema: str = "audit"):
    """
    Create audit infrastructure tables.
    These tables track pipeline execution and enable debugging/recovery.
    """
    
    # Create audit schema if not exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    # ========================================================================
    # TABLE 1: Pipeline Runs
    # ========================================================================
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pipeline_runs (
        run_id STRING NOT NULL,
        execution_date DATE NOT NULL,
        environment STRING,
        run_mode STRING,
        total_tasks INT,
        successful_tasks INT,
        failed_tasks INT,
        skipped_tasks INT,
        status STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        duration_seconds LONG,
        error_summary STRING,
        created_at TIMESTAMP DEFAULT current_timestamp(),
        updated_at TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    PARTITIONED BY (execution_date)
    """)
    
    LOGGER.info(f"✅ Created: {catalog}.{schema}.pipeline_runs")
    
    # ========================================================================
    # TABLE 2: Task Execution Details
    # ========================================================================
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.task_runs (
        run_id STRING NOT NULL,
        task_name STRING NOT NULL,
        task_type STRING,
        execution_order INT,
        attempt INT,
        status STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        duration_seconds LONG,
        rows_processed LONG,
        rows_inserted LONG,
        rows_updated LONG,
        error_message STRING,
        error_type STRING,
        stack_trace STRING,
        created_at TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    PARTITIONED BY (run_id)
    """)
    
    LOGGER.info(f"✅ Created: {catalog}.{schema}.task_runs")
    
    # ========================================================================
    # TABLE 3: Data Quality Checks
    # ========================================================================
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.data_quality_checks (
        run_id STRING NOT NULL,
        table_name STRING NOT NULL,
        check_name STRING NOT NULL,
        check_type STRING,
        expected_value ANY,
        actual_value ANY,
        status STRING,
        message STRING,
        execution_date DATE,
        created_at TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    PARTITIONED BY (execution_date)
    """)
    
    LOGGER.info(f"✅ Created: {catalog}.{schema}.data_quality_checks")
    
    # ========================================================================
    # TABLE 4: Incremental Watermarks (for state tracking)
    # ========================================================================
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.watermarks (
        source_table STRING NOT NULL,
        target_table STRING NOT NULL,
        last_processed_date DATE,
        last_processed_timestamp TIMESTAMP,
        row_count LONG,
        updated_at TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    """)
    
    LOGGER.info(f"✅ Created: {catalog}.{schema}.watermarks")
    
    # ========================================================================
    # TABLE 5: Error Log (rapid lookup)
    # ========================================================================
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.error_log (
        error_id STRING NOT NULL,
        run_id STRING,
        task_name STRING,
        error_type STRING,
        error_message STRING,
        full_traceback STRING,
        context_data STRING,
        severity STRING,
        is_resolved BOOLEAN DEFAULT false,
        resolution_notes STRING,
        created_at TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    """)
    
    LOGGER.info(f"✅ Created: {catalog}.{schema}.error_log")
    
    # ========================================================================
    # TABLE 6: Repair History (tracks recovery attempts)
    # ========================================================================
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.repair_history (
        repair_id STRING NOT NULL,
        original_run_id STRING NOT NULL,
        original_task_name STRING NOT NULL,
        repair_type STRING,
        repair_status STRING,
        rows_reprocessed LONG,
        attempted_at TIMESTAMP,
        completed_at TIMESTAMP,
        notes STRING,
        created_at TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
    """)
    
    LOGGER.info(f"✅ Created: {catalog}.{schema}.repair_history")
    
    print("\n" + "="*80)
    print("✅ AUDIT INFRASTRUCTURE INITIALIZED")
    print("="*80)


# ============================================================================
# AUDIT LOGGER CLASS
# ============================================================================

class AuditLogger:
    """Centralized audit logging for pipeline execution"""
    
    def __init__(self, spark, catalog: str = "fintech_analytics", 
                 schema: str = "audit"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.run_id = None
        self.execution_date = None
        self.current_task = None
        
    def start_run(self, run_id: str, execution_date: str, 
                  environment: str, run_mode: str, total_tasks: int) -> None:
        """Record pipeline run start"""
        self.run_id = run_id
        self.execution_date = execution_date
        
        self.spark.sql(f"""
        INSERT INTO {self.catalog}.{self.schema}.pipeline_runs 
        (run_id, execution_date, environment, run_mode, total_tasks, 
         status, start_time)
        VALUES (
            '{run_id}',
            '{execution_date}',
            '{environment}',
            '{run_mode}',
            {total_tasks},
            'RUNNING',
            current_timestamp()
        )
        """)
        
        print(f"🚀 Pipeline started: {run_id}")
    
    def log_task_start(self, task_name: str, task_type: str, 
                      execution_order: int, attempt: int = 1) -> None:
        """Record task execution start"""
        self.current_task = task_name
        
        self.spark.sql(f"""
        INSERT INTO {self.catalog}.{self.schema}.task_runs
        (run_id, task_name, task_type, execution_order, attempt, 
         status, start_time)
        VALUES (
            '{self.run_id}',
            '{task_name}',
            '{task_type}',
            {execution_order},
            {attempt},
            'RUNNING',
            current_timestamp()
        )
        """)
        
        print(f"  ⚙️  Task: {task_name} (attempt {attempt})")
    
    def log_task_success(self, task_name: str, rows_processed: int = 0,
                        rows_inserted: int = 0, rows_updated: int = 0) -> None:
        """Record task completion"""
        self.spark.sql(f"""
        UPDATE {self.catalog}.{self.schema}.task_runs
        SET 
            status = 'SUCCESS',
            end_time = current_timestamp(),
            duration_seconds = CAST(UNIX_TIMESTAMP(current_timestamp()) - 
                                   UNIX_TIMESTAMP(start_time) AS LONG),
            rows_processed = {rows_processed},
            rows_inserted = {rows_inserted},
            rows_updated = {rows_updated}
        WHERE run_id = '{self.run_id}' 
          AND task_name = '{task_name}'
          AND status = 'RUNNING'
        """)
        
        print(f"    ✅ {task_name} succeeded (rows: {rows_processed})")
    
    def log_task_failure(self, task_name: str, error: Exception, 
                        stack_trace: str = None) -> None:
        """Record task failure and error details"""
        error_type = type(error).__name__
        error_message = str(error).replace("'", "''")  # Escape quotes
        stack_trace = stack_trace or traceback.format_exc()
        
        # Update task status
        self.spark.sql(f"""
        UPDATE {self.catalog}.{self.schema}.task_runs
        SET 
            status = 'FAILED',
            end_time = current_timestamp(),
            duration_seconds = CAST(UNIX_TIMESTAMP(current_timestamp()) - 
                                   UNIX_TIMESTAMP(start_time) AS LONG),
            error_type = '{error_type}',
            error_message = '{error_message}'
        WHERE run_id = '{self.run_id}' 
          AND task_name = '{task_name}'
          AND status = 'RUNNING'
        """)
        
        # Log error details
        error_id = f"{self.run_id}_{task_name}_{int(datetime.now().timestamp())}"
        
        self.spark.sql(f"""
        INSERT INTO {self.catalog}.{self.schema}.error_log
        (error_id, run_id, task_name, error_type, error_message, 
         full_traceback, severity)
        VALUES (
            '{error_id}',
            '{self.run_id}',
            '{task_name}',
            '{error_type}',
            '{error_message}',
            '{stack_trace.replace("'", "''")}',
            'ERROR'
        )
        """)
        
        print(f"    ❌ {task_name} failed: {error_type}")
    
    def log_task_skip(self, task_name: str, reason: str) -> None:
        """Record task skip"""
        self.spark.sql(f"""
        INSERT INTO {self.catalog}.{self.schema}.task_runs
        (run_id, task_name, status, start_time, end_time)
        VALUES (
            '{self.run_id}',
            '{task_name}',
            'SKIPPED',
            current_timestamp(),
            current_timestamp()
        )
        """)
        
        print(f"    ⏭️  {task_name} skipped ({reason})")
    
    def log_data_quality_check(self, table_name: str, check_name: str,
                              check_type: str, status: str, 
                              expected: Any = None, actual: Any = None,
                              message: str = "") -> bool:
        """Log data quality validation result"""
        expected_str = json.dumps(expected) if expected else "NULL"
        actual_str = json.dumps(actual) if actual else "NULL"
        
        self.spark.sql(f"""
        INSERT INTO {self.catalog}.{self.schema}.data_quality_checks
        (run_id, table_name, check_name, check_type, expected_value, 
         actual_value, status, message, execution_date)
        VALUES (
            '{self.run_id}',
            '{table_name}',
            '{check_name}',
            '{check_type}',
            '{expected_str.replace("'", "''")}',
            '{actual_str.replace("'", "''")}',
            '{status}',
            '{message.replace("'", "''")}',
            '{self.execution_date}'
        )
        """)
        
        icon = "✅" if status == "PASS" else "⚠️"
        print(f"    {icon} {table_name}.{check_name}: {status}")
        
        return status == "PASS"
    
    def end_run(self, overall_status: str, successful_tasks: int,
               failed_tasks: int, skipped_tasks: int, 
               error_summary: str = "") -> None:
        """Record pipeline run completion"""
        self.spark.sql(f"""
        UPDATE {self.catalog}.{self.schema}.pipeline_runs
        SET 
            status = '{overall_status}',
            end_time = current_timestamp(),
            duration_seconds = CAST(UNIX_TIMESTAMP(current_timestamp()) - 
                                   UNIX_TIMESTAMP(start_time) AS LONG),
            successful_tasks = {successful_tasks},
            failed_tasks = {failed_tasks},
            skipped_tasks = {skipped_tasks},
            error_summary = '{error_summary.replace("'", "''")}'
        WHERE run_id = '{self.run_id}'
        """)
        
        icon = "✅" if overall_status == "SUCCESS" else "❌"
        print(f"\n{icon} Pipeline {overall_status}: {self.run_id}")
        print(f"   Successful: {successful_tasks} | Failed: {failed_tasks} | Skipped: {skipped_tasks}")


# ============================================================================
# ERROR RECOVERY UTILITIES
# ============================================================================

class ErrorRecovery:
    """Handles error recovery and repair workflows"""
    
    def __init__(self, spark, catalog: str, schema: str = "audit"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
    
    def record_repair_attempt(self, original_run_id: str, task_name: str,
                            repair_type: str, rows_reprocessed: int = 0) -> str:
        """Record repair attempt in history"""
        repair_id = f"{original_run_id}_repair_{int(datetime.now().timestamp())}"
        
        self.spark.sql(f"""
        INSERT INTO {self.catalog}.{self.schema}.repair_history
        (repair_id, original_run_id, original_task_name, repair_type, 
         repair_status, rows_reprocessed, attempted_at)
        VALUES (
            '{repair_id}',
            '{original_run_id}',
            '{task_name}',
            '{repair_type}',
            'COMPLETED',
            {rows_reprocessed},
            current_timestamp()
        )
        """)
        
        return repair_id
    
    def get_failed_tasks(self, run_id: str) -> List[Dict[str, Any]]:
        """Retrieve failed tasks from a run"""
        return self.spark.sql(f"""
        SELECT task_name, error_type, error_message
        FROM {self.catalog}.{self.schema}.task_runs
        WHERE run_id = '{run_id}' AND status = 'FAILED'
        """).collect()
    
    def can_repair(self, run_id: str, task_name: str) -> bool:
        """Determine if a task can be repaired"""
        result = self.spark.sql(f"""
        SELECT COUNT(*) as attempt_count
        FROM {self.catalog}.{self.schema}.task_runs
        WHERE run_id = '{run_id}' 
          AND task_name = '{task_name}'
          AND status = 'FAILED'
        """).collect()[0]['attempt_count']
        
        return result > 0


if __name__ == "__main__":
    print("Audit logging framework loaded")
