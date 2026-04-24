# RetailStream: End-to-End Data Engineering Pipeline

## Overview
RetailStream is an end-to-end data engineering project that demonstrates both batch and streaming data pipelines on retail marketplace data. The project uses the Olist e-commerce dataset and models operational data such as orders, customers, products, sellers, payments, and reviews.

The platform combines:
- DuckDB as the analytical warehouse
- Kafka for streaming event ingestion
- Python for ETL and orchestration
- SQL-based transformations across a Medallion architecture

The pipeline is designed to move data from raw source files into Bronze, Silver, and Gold layers, then surface business-ready KPI outputs for analysis.

## Architecture
### Batch Pipeline
Raw -> Bronze -> Silver -> Gold -> KPIs

- Raw: source CSV files stored in `data/raw/`
- Bronze: raw ingestion tables in DuckDB
- Silver: cleaned and standardized tables for analytics
- Gold: star-schema style dimensions and facts
- KPIs: business queries for revenue, orders, products, and geography

### Streaming Pipeline
Producer -> Kafka -> Consumer -> Bronze

- A Python producer reads source order records and publishes them to Kafka
- Kafka buffers and distributes streaming order events
- A Python consumer reads those events and inserts them into the Bronze layer

## Tech Stack
- Python
- DuckDB
- Apache Kafka
- Docker
- SQL

## Data Pipeline Steps
1. Data ingestion
   Raw retail CSV files are ingested directly into DuckDB, and Kafka-based streaming ingestion is supported for order events.
2. Bronze layer
   Raw source records are loaded into Bronze tables with minimal processing to preserve source structure.
3. Silver layer
   Bronze tables are cleaned, deduplicated, standardized, and typed for downstream use.
4. Gold layer
   Silver tables are modeled into a star schema with dimension and fact tables.
5. KPI analysis
   Business queries are run on the Gold layer to measure revenue, order activity, product performance, and regional demand.

## Tables Created
### Bronze
- `bronze_orders`
- `bronze_order_items`
- `bronze_customers`
- `bronze_products`
- `bronze_sellers`
- `bronze_payments`
- `bronze_reviews`
- `bronze_metadata`

### Silver
- `silver_orders`
- `silver_order_items`
- `silver_customers`
- `silver_products`
- `silver_sellers`
- `silver_payments`
- `silver_reviews`

### Gold
- `dim_customers`
- `dim_products`
- `dim_sellers`
- `dim_date`
- `fact_orders`

## KPI Results
Current KPI results from the project DuckDB warehouse:

- Total Revenue: approximately `16.8M`
- Average Order Value: approximately `158.04`
- Top State by Orders: `SP`
- Top Products by Revenue include:
  - `bb50f2e236e5eea0100680137654686c`
  - `6cdd53843498f92890544667809f1595`
  - `d6160fb7873f184099d9bc95e30376af`

## How to Run
Run all commands from the project root:

```powershell
cd retailstream
pip install -r requirements.txt
```

1. Create Bronze tables
```powershell
python bronze\create_bronze_tables.py
```

2. Load raw CSV data into Bronze
```powershell
python ingestion\load_to_bronze.py
```

3. Build Silver tables
```powershell
python pipeline\silver_transform.py
```

4. Build Gold tables
```powershell
python pipeline\gold_transform.py
```

5. Run KPI analysis
```powershell
python pipeline\kpi_analysis.py
```

6. Optional Kafka setup
```powershell
docker compose up -d
python ingestion\producer.py
python ingestion\consumer.py
```

## Key Learnings
- Building both batch and streaming pipelines highlights the tradeoffs between scheduled warehouse loading and event-driven ingestion.
- Real retail datasets require careful data cleaning for null values, duplicates, inconsistent text fields, and timestamp handling.
- Star schema design improves analytical usability by separating descriptive dimensions from transactional facts.

## Folder Structure
```text
retailstream/
├── .gitignore
├── data_inspection.py
├── docker-compose.yml
├── README.md
├── requirements.txt
├── bronze/
│   └── create_bronze_tables.py
├── ingestion/
│   ├── consumer.py
│   ├── load_to_bronze.py
│   └── producer.py
├── pipeline/
│   ├── db_setup.py
│   ├── gold_transform.py
│   ├── kpi_analysis.py
│   └── silver_transform.py
├── data/
│   ├── raw/
│   └── sample/
│   
├── dashboard/
├── docs/
├── dbt_retailstream/
└── quality/
```
