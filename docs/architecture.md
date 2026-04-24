# RetailStream Architecture

RetailStream combines batch ingestion, streaming ingestion, dbt-based transformation, and KPI analysis in a single end-to-end retail data pipeline.

## Pipeline Diagram
```mermaid
flowchart LR
    A[Olist CSV Files] --> B[Python Loader]
    B --> C[Bronze Layer (DuckDB)]

    A --> D[Kafka Producer]
    D --> E[Kafka Topic: retail_orders]
    E --> F[Kafka Consumer]
    F --> C

    C --> G[dbt Staging Layer (Silver)]
    G --> H[dbt Gold Layer<br/>(Dim + Fact)]
    H --> I[dbt Reporting Marts (Business Layer)]
    I --> J[KPI Analysis (Python)]
```

## Layer Overview
### Bronze
Bronze contains raw ingested data loaded into DuckDB with minimal transformation. It preserves source-level structure from CSV ingestion and Kafka-based event ingestion.

### Staging (Silver)
The staging layer represents cleaned and standardized data. This layer applies filtering, deduplication, type casting, and text normalization to make the data ready for dimensional modeling.

### Gold
Gold contains the star schema used for analytics. It includes dimension tables and a central fact table designed for consistent joins and business reporting.

### Reporting Marts
Reporting marts are business-level aggregated tables built from the Gold layer. These models simplify downstream KPI analysis and dashboard development.

## Transformation and Orchestration
- `dbt` handles transformations from Bronze to staging, Gold, and reporting marts.
- `pipeline/run_pipeline.py` orchestrates the main execution flow across Python loading, dbt runs, dbt tests, quality checks, and KPI analysis.
