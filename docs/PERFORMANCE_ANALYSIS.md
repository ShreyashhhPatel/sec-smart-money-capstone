# Performance analysis

## What we optimize

The pipeline is designed for Databricks/Delta Lake workloads where query patterns typically filter by:

- `company_name`
- `filing_date`
- entity keys (e.g. `cik`)

## Techniques used

- **Delta OPTIMIZE + ZORDER** on heavily queried tables (see `notebooks/04_table_optimization.ipynb`)
- **ANALYZE TABLE … COMPUTE STATISTICS** to improve query planning
- Parameterized lookback windows to limit scan ranges

## Expected outcomes

- faster dashboard queries (lower scan + fewer files)
- more stable query latencies for common filters

## Where to look

- Optimization notebook: `notebooks/04_table_optimization.ipynb`
- Dashboard SQL: `dashboards/dashboard_queries.sql`

