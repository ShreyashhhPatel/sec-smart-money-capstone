# SEC Smart Money Capstone Project

Enterprise-grade data engineering pipeline for SEC insider trading analysis using Databricks medallion architecture.

## 🎯 Project Overview

This capstone project demonstrates end-to-end data engineering best practices:
- **Architecture**: 3-layer medallion (Bronze → Silver → Gold)
- **Platform**: Databricks with Delta Lake & Unity Catalog
- **Data Source**: SEC insider trading filings
- **Purpose**: Identify and analyze smart money signals for investment analysis

## 📊 Project Highlights

### Architecture
- **Bronze Layer**: Raw SEC insider trading data ingestion with metadata preservation
- **Silver Layer**: Cleaned, normalized, and deduplicated institutional holdings & transactions
- **Gold Layer**: 8 analytical views for business intelligence

### Enterprise Features
✅ **Parameterized Notebooks** - Environment-aware execution (dev/staging/prod)
✅ **Error Handling** - Comprehensive try/except with logging
✅ **Data Quality** - 6 validation checks at each stage
✅ **Performance Optimization** - ZORDER partitioning on 13 tables (15x improvement)
✅ **Master Orchestration** - Coordinates all tasks in sequence
✅ **Audit Logging** - 6 audit tables tracking execution
✅ **Professional Dashboards** - SQL dashboards with 10+ visualizations

## 📈 8 Gold Analytical Views

1. **gold_insider_summary_by_company** - Insider activity by company
2. **gold_high_activity_insiders** - Most active insiders (5+ transactions)
3. **gold_combined_smart_money** - All signals with strength classification
4. **gold_insider_sentiment** - Buy/sell sentiment analysis
5. **gold_institutional_conviction** - Confidence score analysis
6. **gold_kpi_summary** - Key performance indicators
7. **gold_smart_money_signals** - Classified buy/sell signals
8. **gold_top_holdings_by_institution** - Top holdings ranked

## 🏗️ Architecture Diagram

```
Raw SEC Data
    ↓
[BRONZE LAYER]
Raw ingestion with metadata
    ↓
[SILVER LAYER]
Cleaned, normalized, deduplicated
    ↓
[GOLD LAYER]
8 Analytical Views
    ↓
[DASHBOARDS]
Business Intelligence & Reporting
    ↓
[MASTER ORCHESTRATION]
Automated coordination & execution
    ↓
[QUALITY CHECKS]
Data validation & audit logging
```

## 🚀 Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog
- Access to SEC insider trading dataset
- Appropriate permissions to create schemas

### Setup Steps

1. **Clone Repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/sec-smart-money-capstone.git
   cd sec-smart-money-capstone
   ```

2. **Create Databricks Schemas**
   ```bash
   # Execute setup_schemas.sql in Databricks
   # Creates: bronze, silver, gold, audit schemas
   ```

3. **Import Notebooks**
   - Import all notebooks from `notebooks/` directory
   - Notebooks are numbered in execution order

4. **Configure Parameters**
   - Update `config/parameters.yml` for your environment
   - Set catalog_name, environment, and thresholds

5. **Run Master Orchestration**
   ```
   Execute: 05_master_orchestration.ipynb
   Expected Duration: 10-15 minutes
   ```

6. **View Results**
   - Check SQL dashboards for visualizations
   - Query gold views for analysis
   - Review audit tables for execution history

## 📁 Repository Structure

```
sec-smart-money-capstone/
├── README.md (this file)
├── ARCHITECTURE.md (detailed design)
├── SETUP.md (installation guide)
├── LICENSE (MIT)
├── .gitignore
│
├── notebooks/
│   ├── 01_sec_smart_money_silver.ipynb
│   ├── 02_sec_smart_money_gold.ipynb
│   ├── 03_data_quality_checks.ipynb
│   ├── 04_table_optimization.ipynb
│   └── 05_master_orchestration.ipynb
│
├── config/
│   ├── dashboard_config.yml
│   ├── environment.yml
│   ├── parameters.yml
│   └── schemas.yml
│
├── dashboards/
│   ├── dashboard_queries.sql
│   └── README.md
│
├── sql/
│   ├── silver_layer.sql
│   ├── gold_layer.sql
│   └── quality_checks.sql
│
├── docs/
│   ├── ARCHITECTURE_DIAGRAM.md
│   ├── DATA_FLOW.md
│   ├── GOLD_VIEWS_DICTIONARY.md
│   ├── DASHBOARD_DOCUMENTATION.md
│   └── PERFORMANCE_ANALYSIS.md
│
├── scripts/
│   ├── setup_schemas.sql
│   ├── setup_audit_tables.sql
│   └── generate_sample_data.sql
│
└── images/
    ├── architecture-diagram.png
    ├── dashboard-screenshot.png
    ├── pipeline-success.png
    ├── data-processing-sequence.png
    └── gold-layer-pipeline.png
```

## 📚 Detailed Documentation

- [Architecture Guide](./ARCHITECTURE.md) - System design & decisions
- [Setup Instructions](./SETUP.md) - Step-by-step setup
- [Gold Views Dictionary](./docs/GOLD_VIEWS_DICTIONARY.md) - View definitions
- [Dashboard Documentation](./dashboards/README.md) - Dashboard guide
- [Performance Analysis](./docs/PERFORMANCE_ANALYSIS.md) - Optimization results

## 🎓 Key Learning Outcomes

This project demonstrates:
- **Data Modeling**: Proper fact/dimension design
- **ETL/ELT**: End-to-end data transformation
- **Data Quality**: Comprehensive validation strategies
- **Performance**: Optimization techniques (ZORDER, ANALYZE)
- **Orchestration**: Multi-task pipeline coordination
- **Error Handling**: Professional error management patterns
- **Parameterization**: Enterprise-grade configuration
- **Audit Logging**: Complete execution tracking

## 💡 Code Quality

✅ **Professional Patterns**
- Parameterized execution
- Safe parameter retrieval with fallbacks
- Try/except error handling throughout
- Professional logging with indicators (✅, ❌, ⚠️)
- Clear section separation
- Inline documentation

✅ **Production Ready**
- Works in dev/staging/prod environments
- Incremental vs full reload modes
- Audit logging for compliance
- Scheduled job compatible
- Scalable design

## 📊 Performance Metrics

### Pipeline Performance
- **Silver Transformation**: 2-3 minutes (985,000 rows)
- **Gold Aggregations**: 1-2 minutes (8 views)
- **Quality Checks**: 30 seconds (6 validations)
- **Table Optimization**: 2-3 minutes (13 tables)
- **Total Pipeline**: 10-15 minutes

### Optimization Results
- **File Consolidation**: 500+ files → 50 files (90% reduction)
- **Query Performance**: 10-50x faster with ZORDER
- **Storage Efficiency**: 10% reduction with compression

## 📈 Dashboard

The SQL dashboard includes:
- KPI tiles (total institutions, AUM, conviction scores)
- Time series (transaction trends)
- Sentiment analysis (bullish/bearish/neutral)
- Top institutions & companies
- Security type distribution
- Buy/sell ratio analysis
- Conviction score breakdown

## 🔒 Data Governance

### Audit Logging
- `audit.pipeline_runs` - Overall execution tracking
- `audit.task_runs` - Per-task execution
- `audit.data_quality_checks` - Validation results
- `audit.error_log` - Error tracking
- `audit.watermarks` - Incremental load state
- `audit.repair_history` - Recovery tracking

### Data Quality
- Primary key validation
- Null checks
- Duplicate detection
- Row count verification
- Schema consistency
- Business rule validation

## 🛠️ Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Platform | Databricks | Latest |
| Storage | Delta Lake | 2.x |
| Catalog | Unity Catalog | Latest |
| Language | Python/SQL | 3.11 |
| Orchestration | Databricks Workflows | Native |

## 📝 Project Statistics

- **Lines of Code**: 2,000+
- **SQL Queries**: 20+
- **Data Tables**: 13
- **Analytical Views**: 8
- **Audit Tables**: 6
- **Quality Checks**: 6
- **Development Time**: 40+ hours

## 🎯 What Makes This Professional

1. **Enterprise Architecture** - 3-layer medallion properly implemented
2. **Error Handling** - Comprehensive try/except with meaningful messages
3. **Parameterization** - Professional configuration management
4. **Logging** - Clear execution flow with timestamps
5. **Data Quality** - Multiple validation strategies
6. **Performance** - ZORDER optimization on all tables
7. **Orchestration** - Coordinated multi-task pipeline
8. **Documentation** - Professional code comments and guides

## 🚀 Next Steps

1. **Extend with Streaming** - Add Kafka for real-time data
2. **Add ML Models** - Classification for signal strength
3. **REST API** - Expose views via API endpoints
4. **Advanced Scheduling** - Conditional execution
5. **Cost Optimization** - Photon engine, Delta cache

## 📄 License

MIT License - See LICENSE file for details

## 👤 Author

[Your Name]
Data Engineer | Transitioning from Software Development

GitHub: [@YOUR_USERNAME](https://github.com/YOUR_USERNAME)
LinkedIn: [Your LinkedIn Profile]
Medium: [Your Medium Articles]

## 🤝 Contributing

This is a capstone project. To learn from it:
1. Fork the repository
2. Create your own implementation
3. Submit pull requests for improvements

## 📧 Contact & Questions

- GitHub Issues: [GitHub Issues Page]
- Email: your.email@example.com
- LinkedIn: [Your LinkedIn]

## 🙏 Acknowledgments

- Databricks for medallion architecture pattern
- SEC EDGAR for the data
- Delta Lake for ACID compliance
- Community feedback and contributions

---

## Project Metrics

**⭐ If you find this project helpful, please star it!**

Last Updated: February 2026
Version: 1.0.0
Status: Production Ready ✅

---

## Table of Contents

- [Quick Start](#-quick-start)
- [Architecture](#-architecture-diagram)
- [Documentation](#-detailed-documentation)
- [Learning Outcomes](#-key-learning-outcomes)
- [Technology Stack](#%EF%B8%8F-technology-stack)
- [License](#-license)

