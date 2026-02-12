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

1. **Bronze Layer**: Stores raw data as-is from source systems. Data is ingested from CSV files in Google Cloud Storage into BigQuery tables with minimal transformation.

2. **Silver Layer**: Contains cleansed, validated, and standardized data. Business rules and data quality checks are applied through stored procedures to prepare data for consumption.

3. **Gold Layer**: Houses business-ready data modeled as a star schema (dimension and fact tables) optimized for reporting and analytics.

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

## 📊 Data Layer Details

### Bronze Layer Tables
Raw data ingested from source systems:

| Table name | Source | Main purpose | Key columns |
|------------|--------|--------------|-------------|
| `bronze.crm_cust_info` | CRM | Customer master | cst_id, cst_key, firstname, lastname, marital_status, gender, create_date |
| `bronze.crm_prd_info` | CRM | Product master | prd_id, prd_key, name, cost, product_line, start_dt, end_dt |
| `bronze.crm_sales_details` | CRM | Sales transactions | order_num, prd_key, cust_id, dates, sales, quantity, price |
| `bronze.erp_loc_a101` | ERP | Customer location | cid, country |
| `bronze.erp_cust_az12` | ERP | Customer demographics | cid, birthdate, gender |
| `bronze.erp_px_cat_g1v2` | ERP | Product categories | id, category, subcategory, maintenance |

### Silver Layer Tables
Cleaned and transformed data with quality rules applied:

| Table name | Source | Key transformations |
|------------|--------|---------------------|
| `silver.crm_cust_info` | Bronze CRM | Deduplicated by cst_id, trimmed text, normalized marital status & gender |
| `silver.crm_prd_info` | Bronze CRM | Extracted cat_id from prd_key, normalized product lines, calculated end dates |
| `silver.crm_sales_details` | Bronze CRM | Parsed dates (YYYYMMDD→DATE), recalculated sales = qty × price |
| `silver.erp_cust_az12` | Bronze ERP | Removed "NAS" prefix, validated birthdates, normalized gender |
| `silver.erp_loc_a101` | Bronze ERP | Removed dashes from cid, standardized country codes |
| `silver.erp_px_cat_g1v2` | Bronze ERP | Pass-through (no transformations) |

### Gold Layer Views (Star Schema)
Analytics-ready dimension and fact tables:

| View name | Type | Purpose & Key features |
|-----------|------|------------------------|
| `gold.dim_customers` | Dimension | Enriched customer data: merges CRM + ERP sources, surrogate key, fallback logic for gender |
| `gold.dim_products` | Dimension | Current products only (prd_end_dt IS NULL), includes category hierarchy, surrogate key |
| `gold.fact_sales` | Fact | Sales transactions with surrogate keys to dimensions, measures: sales, quantity, price |

**Relationships**: `fact_sales` → `dim_customers` (customer_key), `dim_products` (product_key)  
**Grain**: One row per order line item

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
