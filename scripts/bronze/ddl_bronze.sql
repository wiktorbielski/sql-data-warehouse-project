/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' dataset, dropping existing tables 
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables

INSTRUCTIONS:
    Replace 'your-project-id' with your actual GCP project ID before running.
===============================================================================
*/

-- Set your project ID
DECLARE project_id STRING DEFAULT 'your-project-id';

-- Drop and create bronze.crm_cust_info
BEGIN
  EXECUTE IMMEDIATE FORMAT('DROP TABLE IF EXISTS `%s.bronze.crm_cust_info`', project_id);
END;

EXECUTE IMMEDIATE FORMAT('''
CREATE TABLE `%s.bronze.crm_cust_info` (
    cst_id              INT64,
    cst_key             STRING,
    cst_firstname       STRING,
    cst_lastname        STRING,
    cst_marital_status  STRING,
    cst_gndr            STRING,
    cst_create_date     DATE
)''', project_id);

-- Drop and create bronze.crm_prd_info
BEGIN
  EXECUTE IMMEDIATE FORMAT('DROP TABLE IF EXISTS `%s.bronze.crm_prd_info`', project_id);
END;

EXECUTE IMMEDIATE FORMAT('''
CREATE TABLE `%s.bronze.crm_prd_info` (
    prd_id       INT64,
    prd_key      STRING,
    prd_nm       STRING,
    prd_cost     INT64,
    prd_line     STRING,
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
)''', project_id);

-- Drop and create bronze.crm_sales_details
BEGIN
  EXECUTE IMMEDIATE FORMAT('DROP TABLE IF EXISTS `%s.bronze.crm_sales_details`', project_id);
END;

EXECUTE IMMEDIATE FORMAT('''
CREATE TABLE `%s.bronze.crm_sales_details` (
    sls_ord_num  STRING,
    sls_prd_key  STRING,
    sls_cust_id  INT64,
    sls_order_dt INT64,
    sls_ship_dt  INT64,
    sls_due_dt   INT64,
    sls_sales    INT64,
    sls_quantity INT64,
    sls_price    INT64
)''', project_id);

-- Drop and create bronze.erp_loc_a101
BEGIN
  EXECUTE IMMEDIATE FORMAT('DROP TABLE IF EXISTS `%s.bronze.erp_loc_a101`', project_id);
END;

EXECUTE IMMEDIATE FORMAT('''
CREATE TABLE `%s.bronze.erp_loc_a101` (
    cid    STRING,
    cntry  STRING
)''', project_id);

-- Drop and create bronze.erp_cust_az12
BEGIN
  EXECUTE IMMEDIATE FORMAT('DROP TABLE IF EXISTS `%s.bronze.erp_cust_az12`', project_id);
END;

EXECUTE IMMEDIATE FORMAT('''
CREATE TABLE `%s.bronze.erp_cust_az12` (
    cid    STRING,
    bdate  DATE,
    gen    STRING
)''', project_id);

-- Drop and create bronze.erp_px_cat_g1v2
BEGIN
  EXECUTE IMMEDIATE FORMAT('DROP TABLE IF EXISTS `%s.bronze.erp_px_cat_g1v2`', project_id);
END;

EXECUTE IMMEDIATE FORMAT('''
CREATE TABLE `%s.bronze.erp_px_cat_g1v2` (
    id           STRING,
    cat          STRING,
    subcat       STRING,
    maintenance  STRING
)''', project_id);
