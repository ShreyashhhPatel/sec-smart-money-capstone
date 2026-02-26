# Setup

This project is designed to run in **Databricks** (Unity Catalog + Delta Lake) and to be executed end-to-end via the master orchestration notebook.

## Prerequisites

- Databricks workspace access with:
  - Unity Catalog enabled (recommended)
  - Permissions to create catalogs/schemas/tables/views (or use an existing catalog)
- A SQL Warehouse (for dashboards) or ability to create one
- Optional: `DATABRICKS_TOKEN` environment variable for automation

## Repository layout

- **Notebooks**: `notebooks/` (numbered in execution order)
- **Config**: `config/`
- **Dashboards**: `dashboards/`
- **SQL**: `sql/` (layered SQL reference)
- **Bootstrap scripts**: `scripts/`

## Configure

1. Review `config/environment.yml` for workspace/catalog defaults.
2. Review `config/parameters.yml` for per-environment parameters (dev/staging/prod).

## Create schemas (one-time)

Run `scripts/setup_schemas.sql` in Databricks SQL (or in a SQL notebook). This creates the expected schemas:

- `bronze`
- `silver`
- `gold`
- `audit`

## Create audit tables (recommended)

Run `scripts/setup_audit_tables.sql` to create the audit tables used by orchestration and quality checks.

## Run the pipeline

Run the notebooks in order (or just run the master notebook):

1. `notebooks/01_sec_smart_money_silver.ipynb`
2. `notebooks/02_sec_smart_money_gold.ipynb`
3. `notebooks/03_data_quality_checks.ipynb`
4. `notebooks/04_table_optimization.ipynb`
5. `notebooks/05_master_orchestration.ipynb` (**recommended entrypoint**)

## Dashboards

- Dashboard configuration: `dashboards/dashboard_config.yml` (also mirrored in `config/dashboard_config.yml`)
- Dashboard SQL: `dashboards/dashboard_queries.sql`
- Documentation: `dashboards/README.md` and `docs/DASHBOARD_DOCUMENTATION.md`

## Notes

- The included SQL files reference example catalog/schema names (e.g. `fintech_analytics`). Update them to match your environment if needed.
- For production runs, prefer using Databricks Jobs/Workflows to schedule `05_master_orchestration.ipynb`.

