# Data flow

## Bronze → Silver

- **Inputs**: raw SEC data (files or ingested tables)
- **Actions**:
  - type normalization
  - deduplication (optional/parameterized)
  - basic data quality checks
- **Outputs**:
  - `silver_fact_insider_transactions`
  - `silver_fact_institutional_holdings`
  - `silver_dim_companies`
  - `silver_dim_insiders`
  - `silver_dim_institutions`

## Silver → Gold

Gold is built as **views** on top of Silver to support BI and analysis:

- company-level aggregations
- insider activity rollups
- sentiment classification (bullish/bearish/neutral)
- KPI summary view(s)

## Audit + Quality

- Orchestration and quality checks can write to `audit.*` tables:
  - `pipeline_runs`
  - `task_runs`
  - `data_quality_checks`
  - (optional) `error_log`, `watermarks`, `repair_history`

