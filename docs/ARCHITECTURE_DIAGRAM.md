# Architecture diagram (text)

```text
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

If you have an exported PNG/SVG architecture diagram, place it in `images/` and link it from `README.md`.

