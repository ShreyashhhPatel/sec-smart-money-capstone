# Architecture

## Overview

SEC Smart Money is an **enterprise-style Databricks** pipeline that implements a **medallion architecture**:

- **Bronze**: raw SEC inputs preserved as-is
- **Silver**: cleaned/normalized fact + dimension tables
- **Gold**: analytical views for BI and signal detection

Dashboards query the Gold/Silver layer to visualize KPIs and smart-money signals.

## Components

### Notebooks (`notebooks/`)

- `01_sec_smart_money_silver.ipynb`: builds/updates Silver tables (facts + dimensions)
- `02_sec_smart_money_gold.ipynb`: creates Gold analytical views
- `03_data_quality_checks.ipynb`: validates data quality and optionally logs to audit tables
- `04_table_optimization.ipynb`: `OPTIMIZE` + `ZORDER` + `ANALYZE` for key tables
- `05_master_orchestration.ipynb`: orchestrates the end-to-end run

### Scripts (`scripts/`)

- `setup_schemas.sql`: creates schemas (bronze/silver/gold/audit)
- `setup_audit_tables.sql`: creates audit tables for pipeline/task/quality logging
- `generate_sample_data.sql`: optional bootstrap helpers
- Python helpers used by the notebooks live here as well.

### Config (`config/`)

- `environment.yml`: workspace/catalog defaults and cluster/warehouse metadata
- `parameters.yml`: environment-specific execution parameters
- `schemas.yml`: documented schema/table/view definitions
- `dashboard_config.yml`: dashboard layout/config (mirrors `dashboards/dashboard_config.yml`)

### SQL (`sql/`)

Reference SQL grouped by layer:

- `sql/silver_layer/`: transformations + table definitions
- `sql/gold_layer/`: view/KPI definitions + sentiment logic
- `sql/quality_checks/`: validation queries

## Data model (high level)

### Silver

- **Facts**
  - `silver_fact_insider_transactions`
  - `silver_fact_institutional_holdings`
- **Dimensions**
  - `silver_dim_companies`
  - `silver_dim_insiders`
  - `silver_dim_institutions`

### Gold (views)

- `gold_insider_summary_by_company`
- `gold_high_activity_insiders`
- `gold_combined_smart_money`
- `gold_insider_sentiment`
- `gold_institutional_conviction`
- `gold_kpi_summary`
- `gold_smart_money_signals`
- `gold_top_holdings_by_institution`

## Operational considerations

- **Parameterization**: notebooks accept widgets for environment/catalog and thresholds.
- **Audit logging**: orchestration/quality checks can write execution metadata into `audit.*` tables.
- **Performance**: use `OPTIMIZE` + `ZORDER` on high-cardinality filters (e.g. `company_name`, `filing_date`) and compute table statistics.

