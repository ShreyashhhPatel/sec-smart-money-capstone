-- Silver layer transformations (reference)
-- Implemented primarily in `notebooks/01_sec_smart_money_silver.ipynb`.

-- Example: create dimension tables from a cleaned fact.
-- CREATE OR REPLACE TABLE fintech_analytics.silver.silver_dim_companies AS
-- SELECT DISTINCT cik, company_name
-- FROM fintech_analytics.silver.silver_fact_insider_transactions
-- WHERE cik IS NOT NULL;

