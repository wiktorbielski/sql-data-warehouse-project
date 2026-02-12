/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' dataset. It includes checks for:
    - Null or duplicate primary keys
    - Unwanted spaces in string fields
    - Data standardization and consistency
    - Invalid date ranges and orders
    - Data consistency between related fields

Usage Notes:
    - Run these checks after loading Silver Layer using load_silver procedure
    - Investigate and resolve any discrepancies found during the checks
    - Each check indicates expected result (typically "No Results")
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) AS count
FROM `project-id.silver.crm_cust_info`
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    cst_key 
FROM `project-id.silver.crm_cust_info`
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
-- Expectation: Only valid values (Single, Married, n/a)
SELECT DISTINCT 
    cst_marital_status 
FROM `project-id.silver.crm_cust_info`
ORDER BY cst_marital_status;

-- Check Gender Standardization
-- Expectation: Only valid values (Female, Male, n/a)
SELECT DISTINCT 
    cst_gndr 
FROM `project-id.silver.crm_cust_info`
ORDER BY cst_gndr;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prd_id,
    COUNT(*) AS count
FROM `project-id.silver.crm_prd_info`
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    prd_nm 
FROM `project-id.silver.crm_prd_info`
WHERE prd_nm != TRIM(prd_nm);

-- Check for Negative Values in Cost
-- Expectation: No Results (Cost should be >= 0)
SELECT 
    prd_id,
    prd_cost 
FROM `project-id.silver.crm_prd_info`
WHERE prd_cost < 0;

-- Data Standardization & Consistency
-- Expectation: Only valid values (Mountain, Road, Touring, Other Sales, n/a)
SELECT DISTINCT 
    prd_line 
FROM `project-id.silver.crm_prd_info`
ORDER BY prd_line;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT 
    prd_id,
    prd_key,
    prd_start_dt,
    prd_end_dt
FROM `project-id.silver.crm_prd_info`
WHERE prd_end_dt IS NOT NULL 
  AND prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

-- Check for NULL values in critical date fields
-- Expectation: Review results for data quality issues
SELECT 
    'NULL Order Dates' AS check_type,
    COUNT(*) AS count
FROM `project-id.silver.crm_sales_details`
WHERE sls_order_dt IS NULL
UNION ALL
SELECT 
    'NULL Ship Dates' AS check_type,
    COUNT(*) AS count
FROM `project-id.silver.crm_sales_details`
WHERE sls_ship_dt IS NULL
UNION ALL
SELECT 
    'NULL Due Dates' AS check_type,
    COUNT(*) AS count
FROM `project-id.silver.crm_sales_details`
WHERE sls_due_dt IS NULL;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM `project-id.silver.crm_sales_details`
WHERE (sls_ship_dt IS NOT NULL AND sls_order_dt > sls_ship_dt)
   OR (sls_due_dt IS NOT NULL AND sls_order_dt > sls_due_dt);

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results (should be consistent after transformation)
SELECT 
    sls_ord_num,
    sls_sales,
    sls_quantity,
    sls_price,
    sls_quantity * sls_price AS calculated_sales,
    ABS(sls_sales - (sls_quantity * sls_price)) AS difference
FROM `project-id.silver.crm_sales_details`
WHERE ABS(sls_sales - (sls_quantity * sls_price)) > 0.01  -- Allow for rounding
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY difference DESC
LIMIT 100;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT 
    cid,
    bdate 
FROM `project-id.silver.erp_cust_az12`
WHERE bdate < DATE('1924-01-01') 
   OR bdate > CURRENT_DATE();

-- Check for Future Birthdates (should be NULL after transformation)
-- Expectation: No Results
SELECT 
    cid,
    bdate
FROM `project-id.silver.erp_cust_az12`
WHERE bdate > CURRENT_DATE();

-- Data Standardization & Consistency
-- Expectation: Only valid values (Female, Male, n/a)
SELECT DISTINCT 
    gen 
FROM `project-id.silver.erp_cust_az12`
ORDER BY gen;

-- Check for NAS prefix removal
-- Expectation: No Results (NAS prefix should be removed)
SELECT 
    cid
FROM `project-id.silver.erp_cust_az12`
WHERE cid LIKE 'NAS%';

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

-- Data Standardization & Consistency
-- Expectation: Standardized country names (Germany, United States, n/a, or other full names)
SELECT 
    cntry,
    COUNT(*) AS count
FROM `project-id.silver.erp_loc_a101`
GROUP BY cntry
ORDER BY cntry;

-- Check for country code remnants (should be standardized)
-- Expectation: No Results with codes like DE, US, USA
SELECT 
    cntry
FROM `project-id.silver.erp_loc_a101`
WHERE cntry IN ('DE', 'US', 'USA')
   OR LENGTH(cntry) = 2;

-- Check for dashes in customer IDs (should be removed)
-- Expectation: No Results
SELECT 
    cid
FROM `project-id.silver.erp_loc_a101`
WHERE cid LIKE '%-%';

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM `project-id.silver.erp_px_cat_g1v2`
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
-- Expectation: Review distinct values
SELECT DISTINCT 
    maintenance 
FROM `project-id.silver.erp_px_cat_g1v2`
ORDER BY maintenance;

-- Check for NULL values in key fields
-- Expectation: Review results
SELECT 
    'NULL IDs' AS check_type,
    COUNT(*) AS count
FROM `project-id.silver.erp_px_cat_g1v2`
WHERE id IS NULL
UNION ALL
SELECT 
    'NULL Categories' AS check_type,
    COUNT(*) AS count
FROM `project-id.silver.erp_px_cat_g1v2`
WHERE cat IS NULL;

-- ====================================================================
-- Summary Quality Report
-- ====================================================================

-- Combined quality metrics across all tables
SELECT 
    'crm_cust_info' AS table_name,
    COUNT(*) AS total_records,
    COUNTIF(cst_id IS NULL) AS null_primary_keys,
    COUNTIF(cst_marital_status NOT IN ('Single', 'Married', 'n/a')) AS invalid_marital_status,
    COUNTIF(cst_gndr NOT IN ('Female', 'Male', 'n/a')) AS invalid_gender
FROM `project-id.silver.crm_cust_info`

UNION ALL

SELECT 
    'crm_prd_info' AS table_name,
    COUNT(*) AS total_records,
    COUNTIF(prd_id IS NULL) AS null_primary_keys,
    COUNTIF(prd_cost < 0) AS negative_costs,
    COUNTIF(prd_line NOT IN ('Mountain', 'Road', 'Touring', 'Other Sales', 'n/a')) AS invalid_product_lines
FROM `project-id.silver.crm_prd_info`

UNION ALL

SELECT 
    'crm_sales_details' AS table_name,
    COUNT(*) AS total_records,
    COUNTIF(sls_ord_num IS NULL) AS null_order_numbers,
    COUNTIF(sls_order_dt IS NULL) AS null_order_dates,
    COUNTIF(ABS(sls_sales - (sls_quantity * sls_price)) > 0.01) AS inconsistent_sales
FROM `project-id.silver.crm_sales_details`

UNION ALL

SELECT 
    'erp_cust_az12' AS table_name,
    COUNT(*) AS total_records,
    COUNTIF(cid IS NULL) AS null_customer_ids,
    COUNTIF(bdate > CURRENT_DATE()) AS future_birthdates,
    COUNTIF(gen NOT IN ('Female', 'Male', 'n/a')) AS invalid_gender
FROM `project-id.silver.erp_cust_az12`

UNION ALL

SELECT 
    'erp_loc_a101' AS table_name,
    COUNT(*) AS total_records,
    COUNTIF(cid IS NULL) AS null_customer_ids,
    COUNTIF(cid LIKE '%-%') AS ids_with_dashes,
    COUNTIF(LENGTH(cntry) = 2) AS country_codes_remaining
FROM `project-id.silver.erp_loc_a101`

UNION ALL

SELECT 
    'erp_px_cat_g1v2' AS table_name,
    COUNT(*) AS total_records,
    COUNTIF(id IS NULL) AS null_ids,
    COUNTIF(cat IS NULL) AS null_categories,
    0 AS placeholder
FROM `project-id.silver.erp_px_cat_g1v2`

ORDER BY table_name;
