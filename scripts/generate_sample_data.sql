-- ============================================================================
-- SEC Smart Money - Sample Data Generator (optional)
-- This file is intentionally minimal; prefer real SEC/EDGAR inputs.
-- ============================================================================

USE CATALOG fintech_analytics;
CREATE SCHEMA IF NOT EXISTS bronze;
USE SCHEMA bronze;

-- Example placeholder tables (adjust columns/types to match your source).
CREATE TABLE IF NOT EXISTS bronze_fact_insider_transactions (
  cik STRING,
  company_name STRING,
  filing_date STRING,
  insider_name STRING,
  security_title STRING,
  transaction_date STRING,
  shares DOUBLE,
  price_per_share DOUBLE,
  transaction_code STRING,
  acquired_disposed STRING,
  shares_after_transaction DOUBLE,
  confidence_score INT,
  ingest_time TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS bronze_fact_institutional_holdings (
  cik STRING,
  company_name STRING,
  issuer_name STRING,
  market_value DOUBLE,
  conviction_score INT,
  filing_date STRING,
  ingest_time TIMESTAMP
)
USING DELTA;

-- Insert a few rows to validate the pipeline wiring (optional).
-- INSERT INTO bronze_fact_insider_transactions VALUES (...);
-- INSERT INTO bronze_fact_institutional_holdings VALUES (...);

