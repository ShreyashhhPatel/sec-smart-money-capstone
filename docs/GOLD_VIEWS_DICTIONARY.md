# Gold views dictionary

This page documents the analytical views created in `gold` (see `notebooks/02_sec_smart_money_gold.ipynb`).

## Views

### `gold_insider_summary_by_company`

- **Purpose**: company-level rollup of insider activity and confidence
- **Keys**: `company_name`
- **Metrics**: number of insiders, total transactions, buys/sells, avg confidence, latest filing date

### `gold_high_activity_insiders`

- **Purpose**: identify insiders with high transaction activity (e.g. 5+ transactions)
- **Keys**: `insider_name`
- **Metrics**: total transactions, number of companies, avg confidence, latest transaction date

### `gold_combined_smart_money`

- **Purpose**: transaction-level smart money view with signal strength classification
- **Keys**: `company_name`, `insider_name`, `filing_date`
- **Metrics**: derived `transaction_value`, `signal_strength`

### `gold_insider_sentiment`

- **Purpose**: buy/sell sentiment by company and date
- **Keys**: `company_name`, `filing_date`
- **Metrics**: buy_count, sell_count, buy_percentage, avg confidence, `sentiment`

### `gold_institutional_conviction`

- **Purpose**: conviction statistics grouped by company + security
- **Keys**: `company_name`, `security_title`
- **Metrics**: avg/max/min conviction, volatility, latest date

### `gold_kpi_summary`

- **Purpose**: small KPI table for high-level reporting
- **Keys**: `metric_date`, `metric_name`
- **Metrics**: `metric_value` (string), environment

### `gold_smart_money_signals`

- **Purpose**: normalized BUY/SELL signals with strength and transaction value
- **Keys**: `company_name`, `insider_name`, `signal_date`
- **Metrics**: signal_type, signal_strength, transaction_value, confidence_score

### `gold_top_holdings_by_institution`

- **Purpose**: holdings rollups grouped by company/insider/security (as implemented in the notebook)
- **Keys**: `company_name`, `insider_name`, `security_title`
- **Metrics**: total_shares, avg_price, avg_confidence, latest_update

