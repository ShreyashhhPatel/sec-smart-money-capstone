-- ============================================================================
-- SEC SMART MONEY - DASHBOARD SQL QUERIES
-- All Dashboard Visualizations & KPI Calculations
-- ============================================================================

-- ============================================================================
-- SECTION 1: KPI TILES
-- ============================================================================

-- KPI 1: Total Institutions/Companies
-- Query: kpi_total_institutions
SELECT COUNT(DISTINCT cik) as value
FROM fintech_analytics.silver.silver_fact_institutional_holdings;

-- KPI 2: Total Assets Under Management (AUM)
-- Query: kpi_total_aum
SELECT CONCAT('$', ROUND(SUM(market_value) / 1000000000, 2), 'B') as value
FROM fintech_analytics.silver.silver_fact_institutional_holdings;

-- KPI 3: Average Conviction Score
-- Query: kpi_avg_conviction
SELECT ROUND(AVG(conviction_score), 1) as value
FROM fintech_analytics.silver.silver_fact_institutional_holdings
WHERE conviction_score IS NOT NULL;

-- KPI 4: Total Insider Transactions
-- Query: kpi_total_transactions
SELECT COUNT(*) as value
FROM fintech_analytics.silver.silver_fact_insider_transactions;

-- KPI 5: Bullish Signals Count
-- Query: kpi_bullish_signals
SELECT COUNT(*) as value
FROM fintech_analytics.gold.gold_insider_sentiment
WHERE sentiment = 'BULLISH';

-- KPI 6: Neutral Signals Percentage
-- Query: kpi_neutral_signals
SELECT
    COUNT(*) AS total_signals,
    SUM(CASE WHEN sentiment = 'NEUTRAL' THEN 1 ELSE 0 END) AS neutral_signals,
    ROUND(100 * SUM(CASE WHEN sentiment = 'NEUTRAL' THEN 1 ELSE 0 END) / COUNT(*), 2) AS neutral_percentage
FROM fintech_analytics.gold.gold_insider_sentiment;

-- ============================================================================
-- SECTION 2: BAR CHARTS
-- ============================================================================

-- Chart: Top Institutions by Portfolio Value
-- Query: chart_top_institutions
SELECT 
    company_name as Institution,
    ROUND(SUM(market_value) / 1000000, 1) as Portfolio_Value_M
FROM fintech_analytics.silver.silver_fact_institutional_holdings
WHERE market_value IS NOT NULL
GROUP BY company_name
ORDER BY Portfolio_Value_M DESC
LIMIT 15;

-- Chart: Insider Buy/Sell Activity
-- Query: chart_insider_activity
SELECT 
    company_name as Company,
    SUM(CASE WHEN acquired_disposed = 'A' THEN 1 ELSE 0 END) as Buys,
    SUM(CASE WHEN acquired_disposed = 'D' THEN 1 ELSE 0 END) as Sells
FROM fintech_analytics.silver.silver_fact_insider_transactions
GROUP BY company_name
ORDER BY (Buys + Sells) DESC
LIMIT 10;

-- ============================================================================
-- SECTION 3: LINE CHARTS & TIME SERIES
-- ============================================================================

-- Chart: Transaction Timeline (Buys vs Sells Over Time)
-- Query: chart_transaction_timeline
SELECT 
    filing_date,
    SUM(CASE WHEN acquired_disposed = 'A' THEN 1 ELSE 0 END) as Buys,
    SUM(CASE WHEN acquired_disposed = 'D' THEN 1 ELSE 0 END) as Sells,
    COUNT(*) as Total_Transactions
FROM fintech_analytics.silver.silver_fact_insider_transactions
WHERE filing_date IS NOT NULL
GROUP BY filing_date
ORDER BY filing_date DESC;

-- ============================================================================
-- SECTION 4: PIE CHARTS & DISTRIBUTIONS
-- ============================================================================

-- Chart: Security Type Distribution
-- Query: chart_security_distribution
SELECT 
    security_title,
    COUNT(*) as count
FROM fintech_analytics.silver.silver_fact_insider_transactions
WHERE security_title IS NOT NULL
GROUP BY security_title
ORDER BY count DESC
LIMIT 8;

-- Chart: Sentiment Distribution
-- Query: chart_sentiment_distribution
SELECT 
    sentiment,
    COUNT(*) as count,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM fintech_analytics.gold.gold_insider_sentiment
WHERE sentiment IS NOT NULL
GROUP BY sentiment
ORDER BY count DESC;

-- ============================================================================
-- SECTION 5: DETAILED TABLES
-- ============================================================================

-- Table: Top Holdings by Position Size & Conviction
-- Query: table_top_positions
SELECT 
    issuer_name as Stock,
    ROUND(market_value / 1000000, 2) as Position_M,
    conviction_score as Conviction,
    company_name as Institution,
    filing_date as Latest_Update
FROM fintech_analytics.silver.silver_fact_institutional_holdings
WHERE market_value IS NOT NULL 
  AND conviction_score IS NOT NULL
ORDER BY market_value DESC
LIMIT 50;

-- Table: Insider Transaction Activity
-- Query: table_insider_transactions
SELECT 
    filing_date as Date,
    company_name as Company,
    insider_name as Insider,
    CASE WHEN acquired_disposed = 'A' THEN 'BUY' ELSE 'SELL' END as Signal,
    shares as Shares,
    price_per_share as Price,
    ROUND(shares * price_per_share, 2) as Value,
    confidence_score as Confidence
FROM fintech_analytics.silver.silver_fact_insider_transactions
WHERE confidence_score >= 50
ORDER BY filing_date DESC, confidence_score DESC
LIMIT 100;

-- Table: Company Sentiment Summary
-- Query: table_company_sentiment
SELECT 
    company_name as Company,
    COUNT(*) as Total_Signals,
    SUM(CASE WHEN sentiment = 'BULLISH' THEN 1 ELSE 0 END) as Bullish_Count,
    SUM(CASE WHEN sentiment = 'NEUTRAL' THEN 1 ELSE 0 END) as Neutral_Count,
    SUM(CASE WHEN sentiment = 'BEARISH' THEN 1 ELSE 0 END) as Bearish_Count,
    sentiment as Current_Sentiment,
    ROUND(AVG(CAST(conviction_score AS DOUBLE)), 2) as Avg_Conviction
FROM fintech_analytics.gold.gold_insider_sentiment
WHERE conviction_score IS NOT NULL
GROUP BY company_name, sentiment
ORDER BY Total_Signals DESC
LIMIT 50;

-- ============================================================================
-- SECTION 6: FILTER OPTIONS
-- ============================================================================

-- Filter: Available Security Types
-- Query: filter_security_types
SELECT DISTINCT security_title as option
FROM fintech_analytics.silver.silver_fact_insider_transactions
WHERE security_title IS NOT NULL
ORDER BY security_title;

-- Filter: Available Companies
-- Query: filter_companies
SELECT DISTINCT company_name as option
FROM fintech_analytics.silver.silver_fact_insider_transactions
ORDER BY company_name;

-- Filter: Available Sentiments
-- Query: filter_sentiments
SELECT DISTINCT sentiment as option
FROM fintech_analytics.gold.gold_insider_sentiment
WHERE sentiment IS NOT NULL
ORDER BY sentiment;

-- ============================================================================
-- SECTION 7: ADVANCED ANALYTICS
-- ============================================================================

-- Advanced: Conviction Score Distribution
-- Query: advanced_conviction_distribution
SELECT 
    CASE 
        WHEN conviction_score >= 80 THEN '80-100 (Strong)'
        WHEN conviction_score >= 60 THEN '60-79 (Moderate)'
        WHEN conviction_score >= 40 THEN '40-59 (Weak)'
        ELSE '0-39 (Very Weak)'
    END as Confidence_Range,
    COUNT(*) as Count,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as Percentage
FROM fintech_analytics.silver.silver_fact_insider_transactions
WHERE conviction_score IS NOT NULL
GROUP BY Confidence_Range
ORDER BY COUNT(*) DESC;

-- Advanced: Buy/Sell Ratio by Company
-- Query: advanced_buy_sell_ratio
SELECT 
    company_name as Company,
    SUM(CASE WHEN acquired_disposed = 'A' THEN 1 ELSE 0 END) as Total_Buys,
    SUM(CASE WHEN acquired_disposed = 'D' THEN 1 ELSE 0 END) as Total_Sells,
    ROUND(
        100 * SUM(CASE WHEN acquired_disposed = 'A' THEN 1 ELSE 0 END) / 
        (SUM(CASE WHEN acquired_disposed = 'A' THEN 1 ELSE 0 END) + 
         SUM(CASE WHEN acquired_disposed = 'D' THEN 1 ELSE 0 END)), 
        2
    ) as Buy_Percentage
FROM fintech_analytics.silver.silver_fact_insider_transactions
GROUP BY company_name
HAVING (Total_Buys + Total_Sells) > 5
ORDER BY Buy_Percentage DESC;

-- Advanced: Monthly Transaction Trend
-- Query: advanced_monthly_trend
SELECT 
    DATE_TRUNC('MONTH', filing_date) as Month,
    COUNT(*) as Total_Transactions,
    SUM(CASE WHEN acquired_disposed = 'A' THEN 1 ELSE 0 END) as Buys,
    SUM(CASE WHEN acquired_disposed = 'D' THEN 1 ELSE 0 END) as Sells,
    ROUND(AVG(conviction_score), 2) as Avg_Conviction
FROM fintech_analytics.silver.silver_fact_insider_transactions
WHERE filing_date IS NOT NULL
GROUP BY DATE_TRUNC('MONTH', filing_date)
ORDER BY Month DESC;

-- ============================================================================
-- SECTION 8: PERFORMANCE MONITORING
-- ============================================================================

-- Monitor: Last Update Time
-- Query: monitor_last_update
SELECT 
    'Last Dashboard Refresh' as metric,
    MAX(filing_date) as value
FROM fintech_analytics.silver.silver_fact_insider_transactions;

-- Monitor: Data Freshness
-- Query: monitor_data_freshness
SELECT 
    'Data Age (Days)' as metric,
    DATEDIFF(CURRENT_DATE, MAX(filing_date)) as value
FROM fintech_analytics.silver.silver_fact_insider_transactions;

-- Monitor: Record Counts
-- Query: monitor_record_counts
SELECT 
    'Total Holdings' as metric,
    COUNT(*) as value
FROM fintech_analytics.silver.silver_fact_institutional_holdings
UNION ALL
SELECT 
    'Total Transactions' as metric,
    COUNT(*) as value
FROM fintech_analytics.silver.silver_fact_insider_transactions
UNION ALL
SELECT 
    'Total Sentiments' as metric,
    COUNT(*) as value
FROM fintech_analytics.gold.gold_insider_sentiment;

-- ============================================================================
-- END OF DASHBOARD QUERIES
-- ============================================================================

