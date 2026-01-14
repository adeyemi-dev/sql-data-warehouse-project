%sql
-- =====================================================================================
-- NOTEBOOK: Silver Layer - Data Quality Checks
-- PURPOSE:
--   Validate Silver tables after running:
--     CALL DataWarehouse.silver.sp_load_silver();
--
-- OUTPUT:
--   A set of check queries + a final summary (PASS/FAIL style).
-- =====================================================================================

-- COMMAND ----------
-- =====================================================================================
-- 0) OPTIONAL: set context
-- =====================================================================================
-- USE CATALOG DataWarehouse;
-- USE SCHEMA silver;

-- COMMAND ----------
-- =====================================================================================
-- 1) ROW COUNT CHECKS (Silver should not exceed Bronze unexpectedly)
--    - These checks are "sanity checks", not strict rules.
-- =====================================================================================

SELECT 'ROWCOUNT crm_cust_info' AS check_name,
       (SELECT COUNT(*) FROM DataWarehouse.bronze.crm_cust_info) AS bronze_rows,
       (SELECT COUNT(*) FROM DataWarehouse.silver.crm_cust_info) AS silver_rows;

SELECT 'ROWCOUNT crm_prd_info' AS check_name,
       (SELECT COUNT(*) FROM DataWarehouse.bronze.crm_prd_info) AS bronze_rows,
       (SELECT COUNT(*) FROM DataWarehouse.silver.crm_prd_info) AS silver_rows;

SELECT 'ROWCOUNT crm_sales_details' AS check_name,
       (SELECT COUNT(*) FROM DataWarehouse.bronze.crm_sales_details) AS bronze_rows,
       (SELECT COUNT(*) FROM DataWarehouse.silver.crm_sales_details) AS silver_rows;

SELECT 'ROWCOUNT erp_cust_az12' AS check_name,
       (SELECT COUNT(*) FROM DataWarehouse.bronze.erp_cust_az12) AS bronze_rows,
       (SELECT COUNT(*) FROM DataWarehouse.silver.erp_cust_az12) AS silver_rows;

SELECT 'ROWCOUNT erp_loc_a101' AS check_name,
       (SELECT COUNT(*) FROM DataWarehouse.bronze.erp_loc_a101) AS bronze_rows,
       (SELECT COUNT(*) FROM DataWarehouse.silver.erp_loc_a101) AS silver_rows;

SELECT 'ROWCOUNT erp_px_cat_g1v2' AS check_name,
       (SELECT COUNT(*) FROM DataWarehouse.bronze.erp_px_cat_g1v2) AS bronze_rows,
       (SELECT COUNT(*) FROM DataWarehouse.silver.erp_px_cat_g1v2) AS silver_rows;


-- COMMAND ----------
-- =====================================================================================
-- 2) PRIMARY KEY UNIQUENESS CHECKS
-- =====================================================================================

-- CRM customers: expect 1 row per cst_id after dedupe
SELECT 'PK_UNIQUENESS crm_cust_info.cst_id' AS check_name,
       COUNT(*) AS duplicate_rows
FROM (
  SELECT cst_id
  FROM DataWarehouse.silver.crm_cust_info
  GROUP BY cst_id
  HAVING COUNT(*) > 1
) d;

-- CRM products: (prd_key, prd_start_dt) should be unique in your model
SELECT 'PK_UNIQUENESS crm_prd_info(prd_key, prd_start_dt)' AS check_name,
       COUNT(*) AS duplicate_rows
FROM (
  SELECT prd_key, prd_start_dt
  FROM DataWarehouse.silver.crm_prd_info
  GROUP BY prd_key, prd_start_dt
  HAVING COUNT(*) > 1
) d;

-- CRM sales: order number should be unique if that's the business key
SELECT 'PK_UNIQUENESS crm_sales_details.sls_ord_num' AS check_name,
       COUNT(*) AS duplicate_rows
FROM (
  SELECT sls_ord_num
  FROM DataWarehouse.silver.crm_sales_details
  GROUP BY sls_ord_num
  HAVING COUNT(*) > 1
) d;


-- COMMAND ----------
-- =====================================================================================
-- 3) NULL CHECKS (critical fields should not be null)
-- =====================================================================================

SELECT 'NULLS crm_cust_info.cst_id' AS check_name,
       COUNT(*) AS null_count
FROM DataWarehouse.silver.crm_cust_info
WHERE cst_id IS NULL;

SELECT 'NULLS crm_prd_info.prd_key' AS check_name,
       COUNT(*) AS null_count
FROM DataWarehouse.silver.crm_prd_info
WHERE prd_key IS NULL OR TRIM(prd_key) = '';

SELECT 'NULLS crm_sales_details.sls_ord_num' AS check_name,
       COUNT(*) AS null_count
FROM DataWarehouse.silver.crm_sales_details
WHERE sls_ord_num IS NULL OR TRIM(sls_ord_num) = '';

SELECT 'NULLS erp_loc_a101.CID' AS check_name,
       COUNT(*) AS null_count
FROM DataWarehouse.silver.erp_loc_a101
WHERE CID IS NULL OR TRIM(CID) = '';

SELECT 'NULLS erp_px_cat_g1v2.ID' AS check_name,
       COUNT(*) AS null_count
FROM DataWarehouse.silver.erp_px_cat_g1v2
WHERE ID IS NULL OR TRIM(ID) = '';


-- COMMAND ----------
-- =====================================================================================
-- 4) DOMAIN / VALUE CHECKS (only allowed standardised values)
-- =====================================================================================

-- Gender should be standardised
SELECT 'DOMAIN crm_cust_info.cst_gndr' AS check_name,
       COUNT(*) AS invalid_count
FROM DataWarehouse.silver.crm_cust_info
WHERE cst_gndr NOT IN ('Male','Female','N/A');

-- Marital status should be standardised
SELECT 'DOMAIN crm_cust_info.cst_marital_status' AS check_name,
       COUNT(*) AS invalid_count
FROM DataWarehouse.silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Single','Married','N/A');

-- ERP gender standard
SELECT 'DOMAIN erp_cust_az12.GEN' AS check_name,
       COUNT(*) AS invalid_count
FROM DataWarehouse.silver.erp_cust_az12
WHERE GEN NOT IN ('Male','Female','N/A');

-- Country should not be empty
SELECT 'DOMAIN erp_loc_a101.CNTRY not empty' AS check_name,
       COUNT(*) AS invalid_count
FROM DataWarehouse.silver.erp_loc_a101
WHERE CNTRY IS NULL OR TRIM(CNTRY) = '';


-- COMMAND ----------
-- =====================================================================================
-- 5) DATE VALIDITY CHECKS
-- =====================================================================================

-- No future birthdates in ERP demographics
SELECT 'DATE erp_cust_az12.BDATE not in future' AS check_name,
       COUNT(*) AS invalid_count
FROM DataWarehouse.silver.erp_cust_az12
WHERE BDATE IS NOT NULL AND BDATE > CURRENT_DATE();

-- Sales dates should not be in the far future (sanity check)
SELECT 'DATE crm_sales_details.order_dt not far future' AS check_name,
       COUNT(*) AS suspicious_count
FROM DataWarehouse.silver.crm_sales_details
WHERE sls_order_dt IS NOT NULL AND sls_order_dt > DATE_ADD(CURRENT_DATE(), 30);

-- Product: end date should be >= start date when end date exists
SELECT 'DATE crm_prd_info.end_dt >= start_dt' AS check_name,
       COUNT(*) AS invalid_count
FROM DataWarehouse.silver.crm_prd_info
WHERE prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt;


-- COMMAND ----------
-- =====================================================================================
-- 6) REFERENTIAL INTEGRITY CHECKS
--    Sales should map to valid customers and products
-- =====================================================================================

-- Sales -> Customers (missing customers)
SELECT 'RI sales -> customers (missing cst_id)' AS check_name,
       COUNT(*) AS orphan_rows
FROM DataWarehouse.silver.crm_sales_details s
LEFT JOIN DataWarehouse.silver.crm_cust_info c
  ON s.sls_cust_id = c.cst_id
WHERE c.cst_id IS NULL;

-- Sales -> Products (missing product key)
SELECT 'RI sales -> products (missing prd_key)' AS check_name,
       COUNT(*) AS orphan_rows
FROM DataWarehouse.silver.crm_sales_details s
LEFT JOIN DataWarehouse.silver.crm_prd_info p
  ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL;


-- COMMAND ----------
-- =====================================================================================
-- 7) NUMERIC SANITY CHECKS (sales / price / quantity)
-- =====================================================================================

SELECT 'NUMERIC crm_sales_details.quantity > 0' AS check_name,
       COUNT(*) AS invalid_count
FROM DataWarehouse.silver.crm_sales_details
WHERE sls_quantity IS NULL OR sls_quantity <= 0;

SELECT 'NUMERIC crm_sales_details.sales >= 0' AS check_name,
       COUNT(*) AS invalid_count
FROM DataWarehouse.silver.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales < 0;

SELECT 'NUMERIC crm_sales_details.price >= 0' AS check_name,
       COUNT(*) AS invalid_count
FROM DataWarehouse.silver.crm_sales_details
WHERE sls_price IS NULL OR sls_price < 0;


-- COMMAND ----------
-- =====================================================================================
-- 8) FINAL SUMMARY TABLE (PASS/FAIL COUNTS)
--    If failed_checks > 0, the Silver layer needs investigation.
-- =====================================================================================

WITH checks AS (
  SELECT 'DUP crm_cust_info.cst_id' AS check_name,
         (SELECT COUNT(*) FROM (
            SELECT cst_id FROM DataWarehouse.silver.crm_cust_info
            GROUP BY cst_id HAVING COUNT(*) > 1
          )) AS failed_count

  UNION ALL
  SELECT 'DUP crm_prd_info(prd_key, prd_start_dt)' AS check_name,
         (SELECT COUNT(*) FROM (
            SELECT prd_key, prd_start_dt FROM DataWarehouse.silver.crm_prd_info
            GROUP BY prd_key, prd_start_dt HAVING COUNT(*) > 1
          )) AS failed_count

  UNION ALL
  SELECT 'NULL crm_prd_info.prd_key' AS check_name,
         (SELECT COUNT(*) FROM DataWarehouse.silver.crm_prd_info
          WHERE prd_key IS NULL OR TRIM(prd_key) = '') AS failed_count

  UNION ALL
  SELECT 'DOMAIN crm_cust_info.cst_gndr' AS check_name,
         (SELECT COUNT(*) FROM DataWarehouse.silver.crm_cust_info
          WHERE cst_gndr NOT IN ('Male','Female','N/A')) AS failed_count

  UNION ALL
  SELECT 'RI sales->customers' AS check_name,
         (SELECT COUNT(*) FROM DataWarehouse.silver.crm_sales_details s
          LEFT JOIN DataWarehouse.silver.crm_cust_info c ON s.sls_cust_id = c.cst_id
          WHERE c.cst_id IS NULL) AS failed_count

  UNION ALL
  SELECT 'RI sales->products' AS check_name,
         (SELECT COUNT(*) FROM DataWarehouse.silver.crm_sales_details s
          LEFT JOIN DataWarehouse.silver.crm_prd_info p ON s.sls_prd_key = p.prd_key
          WHERE p.prd_key IS NULL) AS failed_count
)

SELECT
  check_name,
  failed_count,
  CASE WHEN failed_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM checks
ORDER BY status DESC, failed_count DESC;
