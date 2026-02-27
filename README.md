## 📖 Project Overview

This project involves:
1. **Data Architecture**: Designing a modern data warehouse using Medallion Architecture (Bronze, Silver, and Gold layers)
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries using star schema
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights

## 🛠️ Technology Stack

* **BigQuery**: Data warehouse and SQL processing engine
* **Google Cloud Storage**: Cloud storage for raw data files and staging
* **SQL**: Primary language for data transformation and analytics
* **Stored Procedures**: Automated ETL orchestration

## 🏗️ Data Architecture

The data architecture follows the **Medallion Architecture** pattern with three distinct layers:

1. **Bronze Layer**: Raw ingestion from CSV files into BigQuery tables, no transformations.
2. **Silver Layer**: Cleansed and standardized data — deduplication, normalization, and business rules applied via stored procedures.
3. **Gold Layer**: Business-ready star schema views optimized for reporting:
   - `dim_customers` — Customer dimension with surrogate key
   - `dim_products` — Active products with category hierarchy
   - `fact_sales` — Sales transactions linked to both dimensions

### Architecture Diagram
```
Source Systems (CRM, ERP)
         ↓
    [CSV Files in GCS]
         ↓
   📊 Bronze Layer (Raw Data)
         ↓
   🔄 Silver Layer (Cleaned & Validated)
         ↓
   ⭐ Gold Layer (Star Schema)
         ↓
    Analytics & Reports
```

## 🔄 ETL Pipeline

The ETL process is automated using BigQuery stored procedures:

1. **Bronze Layer**: Raw data loaded from GCS CSV files into BigQuery tables
2. **Silver Layer**: `load_silver` stored procedure performs transformations and data quality checks
3. **Gold Layer**: Views created on top of Silver layer tables to form the star schema

### Running the ETL Pipeline
```sql
-- Load Silver layer from Bronze
CALL `project-id.silver.load_silver`();

-- Gold layer views are automatically available once Silver layer is populated
SELECT * FROM `project-id.gold.dim_customers`;
SELECT * FROM `project-id.gold.dim_products`;
SELECT * FROM `project-id.gold.fact_sales`;
```

## 📈 Key Features

- **Medallion Architecture**: Structured three-layer approach for data quality and governance
- **Data Quality**: Comprehensive validation, deduplication, and normalization rules
- **Star Schema**: Optimized dimensional model for analytical queries
- **Automated ETL**: Stored procedures for repeatable and maintainable data pipelines
- **Surrogate Keys**: Dimension tables use surrogate keys for better performance and flexibility
- **Data Lineage**: Clear transformation logic from Bronze → Silver → Gold
`
