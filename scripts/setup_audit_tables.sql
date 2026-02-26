-- ============================================================================
-- SEC Smart Money - Audit Tables Setup
-- Creates core audit tables referenced by notebooks/orchestration.
-- ============================================================================

USE CATALOG fintech_analytics;
CREATE SCHEMA IF NOT EXISTS audit;
USE SCHEMA audit;

CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id STRING,
  execution_date DATE,
  environment STRING,
  run_mode STRING,
  status STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds DOUBLE,
  total_tasks INT,
  successful_tasks INT,
  failed_tasks INT
)
USING DELTA;

CREATE TABLE IF NOT EXISTS task_runs (
  run_id STRING,
  task_name STRING,
  status STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds DOUBLE,
  rows_processed BIGINT,
  error_message STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS data_quality_checks (
  run_id STRING,
  table_name STRING,
  check_name STRING,
  status STRING,
  message STRING,
  execution_date DATE,
  created_at TIMESTAMP
)
USING DELTA;

-- Optional tables (documented in config/schemas.yml)
CREATE TABLE IF NOT EXISTS error_log (
  run_id STRING,
  task_name STRING,
  error_message STRING,
  created_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS watermarks (
  pipeline_name STRING,
  last_processed_date DATE,
  updated_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS repair_history (
  run_id STRING,
  action STRING,
  details STRING,
  created_at TIMESTAMP
)
USING DELTA;

