%sql
-- =====================================================================================
-- NOTEBOOK: Silver Layer - Setup + Loads (CRM + ERP)
-- PURPOSE:
--   1) Create/replace Silver Delta tables
--   2) Load Silver from Bronze with cleaning/standardisation
--
-- PLATFORM:
--   Databricks (Spark SQL / Delta Lake) + Unity Catalog
--
-- LAYERS:
--   Bronze = raw ingestion
--   Silver = cleaned / conformed (business-friendly, analytics-ready)
-- =====================================================================================

-- COMMAND ----------
-- =====================================================================================
-- 0) USE CATALOG / SCHEMA (optional - adjust if needed)
-- =====================================================================================
-- USE CATALOG datawarehouse;
-- USE SCHEMA silver;

-- COMMAND ----------
-- =====================================================================================
-- 1) CREATE / REPLACE SILVER TABLES (CRM)
-- =====================================================================================

CREATE OR REPLACE TABLE datawarehouse.silver.crm_cust_info (
  cst_id           INT       COMMENT 'Customer identifier',
  cst_key          STRING    COMMENT 'Customer business key',
  cst_firstname    STRING    COMMENT 'Customer first name (trimmed)',
  cst_lastname     STRING    COMMENT 'Customer last name (trimmed)',
  cst_marital_status STRING  COMMENT 'Customer marital status (standardised)',
  cst_gndr         STRING    COMMENT 'Customer gender (standardised)',
  cst_create_date  DATE      COMMENT 'Customer record creation date',
  dwh_load_ts      TIMESTAMP COMMENT 'Load timestamp into Silver'
)
USING DELTA
COMMENT 'Silver: CRM customer information (cleaned & deduplicated)';


CREATE OR REPLACE TABLE datawarehouse.silver.crm_prd_info (
  prd_id        INT       COMMENT 'Product identifier',
  cat_id        STRING    COMMENT 'Derived product category identifier',
  prd_key       STRING    COMMENT 'Business product key (cleaned)',
  prd_nm        STRING    COMMENT 'Product name',
  prd_cost      INT       COMMENT 'Product cost (raw integer; cast later if needed)',
  prd_line      STRING    COMMENT 'Product line (standardised)',
  prd_start_dt  DATE      COMMENT 'Product start date',
  prd_end_dt    DATE      COMMENT 'Product end date (derived via SCD window)',
  dwh_load_ts   TIMESTAMP COMMENT 'Load timestamp into Silver'
)
USING DELTA
COMMENT 'Silver: CRM product information (cleaned + derived end dates)';


CREATE OR REPLACE TABLE datawarehouse.silver.crm_sales_details (
  sls_ord_num   STRING    COMMENT 'Sales order number (business key)',
  sls_prd_key   STRING    COMMENT 'Product business key',
  sls_cust_id   INT       COMMENT 'Customer identifier',
  sls_order_dt  DATE      COMMENT 'Order date (converted from YYYYMMDD)',
  sls_ship_dt   DATE      COMMENT 'Shipment date (converted from YYYYMMDD)',
  sls_due_dt    DATE      COMMENT 'Due date (converted from YYYYMMDD)',
  sls_sales     INT       COMMENT 'Total sales amount (cleaned)',
  sls_quantity  INT       COMMENT 'Quantity sold',
  sls_price     INT       COMMENT 'Unit price (cleaned)',
  dwh_load_ts   TIMESTAMP COMMENT 'Load timestamp into Silver'
)
USING DELTA
COMMENT 'Silver: CRM sales details (date parsing + sales/price fixes)';


-- COMMAND ----------
-- =====================================================================================
-- 2) CREATE / REPLACE SILVER TABLES (ERP)
-- =====================================================================================

CREATE OR REPLACE TABLE datawarehouse.silver.erp_loc_a101 (
  CID         STRING    COMMENT 'Customer identifier (cleaned)',
  CNTRY       STRING    COMMENT 'Customer country (standardised)',
  dwh_load_ts TIMESTAMP COMMENT 'Load timestamp into Silver'
)
USING DELTA
COMMENT 'Silver: ERP customer location (cleaned country + cleaned CID)';


CREATE OR REPLACE TABLE datawarehouse.silver.erp_cust_az12 (
  CID         STRING    COMMENT 'Customer identifier (cleaned)',
  BDATE       DATE      COMMENT 'Customer birth date (future dates null)',
  GEN         STRING    COMMENT 'Customer gender (standardised)',
  dwh_load_ts TIMESTAMP COMMENT 'Load timestamp into Silver'
)
USING DELTA
COMMENT 'Silver: ERP customer demographics (CID cleanup + gender standardisation)';


CREATE OR REPLACE TABLE datawarehouse.silver.erp_px_cat_g1v2 (
  ID          STRING    COMMENT 'Product identifier',
  CAT         STRING    COMMENT 'Product category',
  SUBCAT      STRING    COMMENT 'Product sub-category',
  MAINTENANCE STRING    COMMENT 'Maintenance / service classification',
  dwh_load_ts TIMESTAMP COMMENT 'Load timestamp into Silver'
)
USING DELTA
COMMENT 'Silver: ERP product category & maintenance mapping';


-- COMMAND ----------
-- =====================================================================================
-- 3) LOAD SILVER: CRM CUSTOMERS
--    - Trim names
--    - Standardise marital status and gender
--    - Deduplicate by keeping latest record per cst_id (by create_date desc)
-- =====================================================================================

INSERT OVERWRITE datawarehouse.silver.crm_cust_info
SELECT
  cst_id,
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname)  AS cst_lastname,

  CASE
    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    ELSE 'N/A'
  END AS cst_marital_status,

  CASE
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'N/A'
  END AS cst_gndr,

  cst_create_date,
  CURRENT_TIMESTAMP() AS dwh_load_ts
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
  FROM datawarehouse.bronze.crm_cust_info
) x
WHERE rn = 1;


-- COMMAND ----------
-- =====================================================================================
-- 4) LOAD SILVER: CRM PRODUCTS
--    - Derive cat_id and prd_key from source key
--    - Standardise prd_line
--    - Compute prd_end_dt using LEAD(prd_start_dt) per prd_key (SCD-style window)
-- =====================================================================================

INSERT OVERWRITE datawarehouse.silver.crm_prd_info
WITH base AS (
  SELECT
    prd_id,
    prd_key AS src_prd_key,

    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key))      AS prd_key,

    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,

    CASE
      WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
      WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
      WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
      WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
      ELSE 'N/A'
    END AS prd_line,

    CAST(prd_start_dt AS DATE) AS prd_start_dt
  FROM datawarehouse.bronze.crm_prd_info
),
scd AS (
  SELECT
    *,
    LEAD(prd_start_dt) OVER (
      PARTITION BY prd_key
      ORDER BY prd_start_dt
    ) AS next_start_dt
  FROM base
)
SELECT
  prd_id,
  cat_id,
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  CASE
    WHEN next_start_dt IS NULL THEN NULL
    ELSE DATE_SUB(next_start_dt, 1)
  END AS prd_end_dt,
  CURRENT_TIMESTAMP() AS dwh_load_ts
FROM scd;


-- COMMAND ----------
-- =====================================================================================
-- 5) LOAD SILVER: CRM SALES DETAILS
--    - Convert YYYYMMDD integer to DATE safely
--    - Fix sales if missing/invalid (derive from qty * abs(price))
--    - Fix price if missing/invalid (derive from sales/qty)
-- =====================================================================================

INSERT OVERWRITE datawarehouse.silver.crm_sales_details
SELECT
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,

  CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS STRING)) <> 8 THEN NULL
       ELSE TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd') END AS sls_order_dt,

  CASE WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) <> 8 THEN NULL
       ELSE TO_DATE(CAST(sls_ship_dt AS STRING), 'yyyyMMdd') END AS sls_ship_dt,

  CASE WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS STRING)) <> 8 THEN NULL
       ELSE TO_DATE(CAST(sls_due_dt AS STRING), 'yyyyMMdd') END AS sls_due_dt,

  CASE
    WHEN sls_sales IS NULL OR sls_sales <= 0 THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END AS sls_sales,

  sls_quantity,

  CASE
    WHEN sls_price IS NULL OR sls_price <= 0 THEN
      CAST(
        (
          CASE
            WHEN sls_sales IS NULL OR sls_sales <= 0 THEN (sls_quantity * ABS(sls_price)) / NULLIF(sls_quantity, 0)
            ELSE sls_sales / NULLIF(sls_quantity, 0)
          END
        ) AS INT
      )
    ELSE sls_price
  END AS sls_price,

  CURRENT_TIMESTAMP() AS dwh_load_ts
FROM datawarehouse.bronze.crm_sales_details;


-- COMMAND ----------
-- =====================================================================================
-- 6) LOAD SILVER: ERP CUSTOMER DEMOGRAPHICS
--    - Remove NAS prefix from CID
--    - Null future birthdates
--    - Standardise gender
-- =====================================================================================

INSERT OVERWRITE datawarehouse.silver.erp_cust_az12
SELECT
  CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LENGTH(CID)) ELSE CID END AS CID,

  CASE WHEN BDATE > CURRENT_DATE() THEN NULL ELSE BDATE END AS BDATE,

  CASE
    WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE')   THEN 'Male'
    ELSE 'N/A'
  END AS GEN,

  CURRENT_TIMESTAMP() AS dwh_load_ts
FROM datawarehouse.bronze.erp_cust_az12;


-- COMMAND ----------
-- =====================================================================================
-- 7) LOAD SILVER: ERP LOCATION
--    - Remove dashes from CID
--    - Standardise country values
--    - Fix 'DE%' logic (must use LIKE)
-- =====================================================================================

INSERT OVERWRITE datawarehouse.silver.erp_loc_a101
SELECT
  REPLACE(CID, '-', '') AS CID,

  CASE
    WHEN TRIM(CNTRY) LIKE 'DE%' THEN 'Germany'
    WHEN UPPER(TRIM(CNTRY)) IN ('USA', 'US') THEN 'United States'
    WHEN CNTRY IS NULL OR TRIM(CNTRY) = '' THEN 'N/A'
    ELSE TRIM(CNTRY)
  END AS CNTRY,

  CURRENT_TIMESTAMP() AS dwh_load_ts
FROM datawarehouse.bronze.erp_loc_a101;


-- COMMAND ----------
-- =====================================================================================
-- 8) LOAD SILVER: ERP PRODUCT CATEGORY
-- =====================================================================================

INSERT OVERWRITE datawarehouse.silver.erp_px_cat_g1v2
SELECT
  ID,
  CAT,
  SUBCAT,
  MAINTENANCE,
  CURRENT_TIMESTAMP() AS dwh_load_ts
FROM datawarehouse.bronze.erp_px_cat_g1v2;

-- COMMAND ----------
-- =====================================================================================
-- 9) QUICK VALIDATION CHECKS (optional)
-- =====================================================================================
-- SELECT COUNT(*) AS crm_cust_rows FROM datawarehouse.silver.crm_cust_info;
-- SELECT COUNT(*) AS crm_prd_rows  FROM datawarehouse.silver.crm_prd_info;
-- SELECT COUNT(*) AS crm_sls_rows  FROM datawarehouse.silver.crm_sales_details;
-- SELECT COUNT(*) AS erp_az12_rows FROM datawarehouse.silver.erp_cust_az12;
-- SELECT COUNT(*) AS erp_loc_rows  FROM datawarehouse.silver.erp_loc_a101;
-- SELECT COUNT(*) AS erp_cat_rows  FROM datawarehouse.silver.erp_px_cat_g1v2;


