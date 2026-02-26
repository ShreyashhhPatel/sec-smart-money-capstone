# SEC Smart Money - Dashboard Documentation

## Dashboard Overview

The SEC Smart Money dashboard provides real-time business intelligence for insider trading signal detection and institutional investment tracking.

### Key Features

- **Real-time KPI monitoring** of institutional holdings and insider activity
- **Interactive visualizations** of buy/sell sentiment and portfolio allocations
- **Detailed transaction tables** with conviction scoring
- **Time-series analysis** of market trends
- **Customizable filters** for date ranges and conviction thresholds

## Dashboard Components

### Section 1: Key Performance Indicators (KPIs)

**Top Row - Core Metrics:**

1. **Total Institutions** (Blue Tile)
   - Metric: Count of unique company CIKs
   - Query: `kpi_total_institutions`
   - Refresh: Real-time
   - Range: 0 to N institutions

2. **Total AUM** (Green Tile)
   - Metric: Sum of market values in billions
   - Query: `kpi_total_aum`
   - Format: $X.XXB
   - Calculation: SUM(market_value) / 1,000,000,000

3. **Avg Conviction Score** (Orange Tile)
   - Metric: Average confidence score (0-100)
   - Query: `kpi_avg_conviction`
   - Format: #0.0
   - Calculation: ROUND(AVG(conviction_score), 1)

**Middle Row - Activity Metrics:**

4. **Total Insider Transactions** (Purple Tile)
   - Metric: Count of all insider transactions
   - Query: `kpi_total_transactions`
   - Refresh: Daily
   - Data Source: silver_fact_insider_transactions

5. **Bullish Signals** (Green Tile)
   - Metric: Count of bullish sentiment signals
   - Query: `kpi_bullish_signals`
   - Definition: More buys than sells
   - Data Source: gold_insider_sentiment

6. **Neutral Signals** (Gray Tile)
   - Metric: Percentage of neutral sentiment
   - Query: `kpi_neutral_signals`
   - Format: #0.00%
   - Definition: Equal or nearly equal buys/sells

### Section 2: Visualizations

#### Chart 1: Top Institutions by AUM
- **Type:** Horizontal bar chart
- **Data:** Top 15 institutions ranked by portfolio value
- **X-Axis:** Institution name
- **Y-Axis:** Portfolio value (millions)
- **Query:** `chart_top_institutions`
- **Interactivity:** Sortable, clickable for details

#### Chart 2: Insider Buy/Sell Activity
- **Type:** Grouped bar chart
- **Data:** Top 10 companies by transaction volume
- **Bars:** Buys vs Sells
- **Query:** `chart_insider_activity`
- **Insights:** Identifies bullish/bearish sentiment by company

#### Chart 3: Transaction Timeline
- **Type:** Multi-line chart
- **Data:** Daily transaction counts over 365 days
- **Lines:** Buys, Sells, Total
- **X-Axis:** Filing date
- **Y-Axis:** Transaction count
- **Query:** `chart_transaction_timeline`
- **Use Case:** Identifies trend changes

#### Chart 4: Security Type Distribution
- **Type:** Pie chart
- **Data:** Transaction distribution by security type
- **Query:** `chart_security_distribution`
- **Insights:** Shows most active security types

### Section 3: Detailed Tables

#### Table 1: Top Holdings
- **Columns:** Stock, Position (M), Conviction, Institution, Latest Update
- **Query:** `table_top_positions`
- **Rows:** Top 50 positions by value
- **Sorting:** Position value (descending)
- **Filtering:** Conviction score slider
- **Use Case:** Due diligence on large positions

#### Table 2: Insider Transaction Activity
- **Columns:** Date, Company, Insider, Signal, Shares, Price, Value, Confidence
- **Query:** `table_insider_transactions`
- **Rows:** Last 100 transactions, filtered to confidence >= 50
- **Sorting:** Latest first, confidence descending
- **Use Case:** Real-time transaction monitoring

#### Table 3: Company Sentiment Summary
- **Columns:** Company, Total Signals, Bullish, Neutral, Bearish, Current Sentiment, Avg Conviction
- **Query:** `table_company_sentiment`
- **Rows:** All companies with sentiment data (top 50)
- **Use Case:** Institutional positioning analysis

## Filters & Parameters

### Date Range Filter
- **Type:** Date picker with preset options
- **Default:** Last 365 days
- **Applies to:** Transaction timeline, insider activity
- **Range:** Any date up to current date

### Conviction Threshold Slider
- **Type:** Numeric slider
- **Range:** 0-100
- **Default:** 50
- **Step:** 5
- **Applies to:** Top positions, institutions

### Security Type Filter
- **Type:** Multi-select dropdown
- **Options:** Dynamic from database
- **Default:** All types
- **Applies to:** Security distribution chart

## Performance & Refresh

- **Dashboard Refresh:** Daily at 6:00 AM UTC
- **Cache TTL:** 60 minutes
- **Query Timeout:** 5 minutes (300 seconds)
- **Max Rows:** 100,000 per query

## Data Freshness

- **Latest Update:** See monitor_last_update query
- **Data Age:** Calculated daily in monitor_data_freshness
- **Latency:** 24 hours from filing to dashboard

## Troubleshooting

### Query Timeout
- Reduce date range
- Increase conviction threshold
- Use specific security type filter

### Missing Data
- Check data freshness (monitor_data_freshness)
- Verify conviction score filter
- Check date range selection

### Slow Dashboard Load
- Clear browser cache
- Reduce number of open tabs
- Contact data team for infrastructure scaling

## Maintenance & Updates

- **Backup Frequency:** Daily
- **Schema Changes:** Backwards compatible
- **Query Updates:** Monthly optimization pass
- **Documentation:** Updated with each release

