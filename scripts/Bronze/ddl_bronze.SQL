-- =====================================================================================
-- FILE: bronze_load_procedure.sql
-- DDL Script
-- PROJECT: Databricks Medallion Architecture (Bronze Layer)
--
-- DESCRIPTION:
--   This script creates:
--     1) A Bronze load log table for observability
--     2) A stored procedure (sp_load_bronze) that:
--        - Creates/Replaces Bronze tables (CRM + ERP)
--        - Truncates tables for clean reload
--        - Loads CSV data from Unity Catalog Volumes using COPY INTO
--        - Logs step-by-step progress, row counts, and durations (seconds)
--
-- PLATFORM:
--   Databricks Free Edition
--   Unity Catalog enabled
--   Delta Lake tables
--   SQL Scripting stored procedure
--
-- IMPORTANT:
--   1) Ensure the CSV files exist in these paths:
--      - /Volumes/datawarehouse/bronze/source_crm/cust_info.csv
--      - /Volumes/datawarehouse/bronze/source_crm/prd_info.csv
--      - /Volumes/datawarehouse/bronze/source_crm/sales_details.csv
--      - /Volumes/datawarehouse/bronze/source_erp/CUST_AZ12.csv
--      - /Volumes/datawarehouse/bronze/source_erp/LOC_A101.csv
--      - /Volumes/datawarehouse/bronze/source_erp/PX_CAT_G1V2.csv
--
--   2) Bronze tables intentionally match raw source formats:
--      - CRM sales dates remain INT (YYYYMMDD)
--      - ERP IDs remain STRING (alphanumeric)
--      - prd_cost remains INT as in source
--
-- HOW TO USE:
--   1) Run this file once to create the procedure.
--   2) Then execute:
--        CALL DataWarehouse.bronze.sp_load_bronze();
--   3) View logs:
--        SELECT * FROM DataWarehouse.bronze.bronze_load_log ORDER BY start_ts DESC;
-- =====================================================================================


-- =====================================================================================
-- 1) LOG TABLE (Observability)
--    Stores: run_id, step status, timestamps, and durations
-- =====================================================================================
CREATE TABLE IF NOT EXISTS DataWarehouse.bronze.bronze_load_log (
  run_id STRING,
  step STRING,
  status STRING,
  message STRING,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  duration_seconds BIGINT
)
USING DELTA
COMMENT 'Detailed execution log for Bronze load procedure';


-- =====================================================================================
-- 2) STORED PROCEDURE
-- =====================================================================================
CREATE OR REPLACE PROCEDURE DataWarehouse.bronze.sp_load_bronze()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN

  -- -------------------------
  -- Variables
  -- -------------------------
  DECLARE v_run_id STRING;
  SET v_run_id = uuid();

  -- -------------------------
  -- TOTAL START (overall timer)
  -- -------------------------
  INSERT INTO DataWarehouse.bronze.bronze_load_log
  VALUES (
    v_run_id, 'TOTAL', 'RUNNING',
    'Bronze load started',
    current_timestamp(), NULL, NULL
  );


  -- ===================================================================================
  -- STEP 1: CREATE / REPLACE TABLES (CRM + ERP)
  -- ===================================================================================
  INSERT INTO DataWarehouse.bronze.bronze_load_log
  VALUES (
    v_run_id, 'CREATE_TABLES', 'RUNNING',
    'Creating/Replacing Bronze tables (CRM + ERP)',
    current_timestamp(), NULL, NULL
  );

  -- -------------------------
  -- CRM tables (match CSV)
  -- -------------------------
  CREATE OR REPLACE TABLE DataWarehouse.bronze.crm_cust_info (
    cst_id INT,
    cst_key STRING,
    cst_firstname STRING,
    cst_lastname STRING,
    cst_marital_status STRING,
    cst_gndr STRING,
    cst_create_date DATE
  )
  USING DELTA
  COMMENT 'Bronze: CRM customer raw';

  CREATE OR REPLACE TABLE DataWarehouse.bronze.crm_prd_info (
    prd_id INT,
    prd_key STRING,
    prd_nm STRING,
    prd_cost INT,
    prd_line STRING,
    prd_start_dt DATE,
    prd_end_dt DATE
  )
  USING DELTA
  COMMENT 'Bronze: CRM product raw';

  CREATE OR REPLACE TABLE DataWarehouse.bronze.crm_sales_details (
    sls_ord_num STRING,
    sls_prd_key STRING,
    sls_cust_id INT,
    sls_order_dt INT,   -- raw YYYYMMDD
    sls_ship_dt INT,    -- raw YYYYMMDD
    sls_due_dt INT,     -- raw YYYYMMDD
    sls_sales INT,      -- raw integer
    sls_quantity INT,
    sls_price INT       -- raw integer
  )
  USING DELTA
  COMMENT 'Bronze: CRM sales raw (dates as YYYYMMDD int)';

  -- -------------------------
  -- ERP tables (match CSV exactly; uppercase + raw types)
  -- -------------------------
  CREATE OR REPLACE TABLE DataWarehouse.bronze.erp_cust_az12 (
    CID STRING,   -- alphanumeric
    BDATE DATE,
    GEN STRING
  )
  USING DELTA
  COMMENT 'Bronze: ERP customer demographics raw';

  CREATE OR REPLACE TABLE DataWarehouse.bronze.erp_loc_a101 (
    CID STRING,   -- alphanumeric
    CNTRY STRING
  )
  USING DELTA
  COMMENT 'Bronze: ERP customer location raw';

  CREATE OR REPLACE TABLE DataWarehouse.bronze.erp_px_cat_g1v2 (
    ID STRING,    -- alphanumeric
    CAT STRING,
    SUBCAT STRING,
    MAINTENANCE STRING
  )
  USING DELTA
  COMMENT 'Bronze: ERP product category raw';

  -- End CREATE_TABLES step (duration fixed using current_timestamp())
  UPDATE DataWarehouse.bronze.bronze_load_log
  SET
    status = 'OK',
    end_ts = current_timestamp(),
    duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(start_ts),
    message = 'Tables created/replaced'
  WHERE run_id = v_run_id AND step = 'CREATE_TABLES';



  -- ===================================================================================
  -- STEP 2: TRUNCATE TABLES (Clean Reload)
  -- ===================================================================================
  INSERT INTO DataWarehouse.bronze.bronze_load_log
  VALUES (
    v_run_id, 'TRUNCATE', 'RUNNING',
    'Truncating Bronze tables for clean reload',
    current_timestamp(), NULL, NULL
  );

  TRUNCATE TABLE DataWarehouse.bronze.crm_cust_info;
  TRUNCATE TABLE DataWarehouse.bronze.crm_prd_info;
  TRUNCATE TABLE DataWarehouse.bronze.crm_sales_details;
  TRUNCATE TABLE DataWarehouse.bronze.erp_cust_az12;
  TRUNCATE TABLE DataWarehouse.bronze.erp_loc_a101;
  TRUNCATE TABLE DataWarehouse.bronze.erp_px_cat_g1v2;

  UPDATE DataWarehouse.bronze.bronze_load_log
  SET
    status = 'OK',
    end_ts = current_timestamp(),
    duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(start_ts),
    message = 'Tables truncated'
  WHERE run_id = v_run_id AND step = 'TRUNCATE';



  -- ===================================================================================
  -- STEP 3: LOAD DATA USING COPY INTO
  -- ===================================================================================
  INSERT INTO DataWarehouse.bronze.bronze_load_log
  VALUES (
    v_run_id, 'LOAD', 'RUNNING',
    'Loading CSV files into Bronze via COPY INTO',
    current_timestamp(), NULL, NULL
  );

  -- -------------------------
  -- CRM loads
  -- -------------------------
  COPY INTO DataWarehouse.bronze.crm_cust_info
  FROM '/Volumes/datawarehouse/bronze/source_crm/cust_info.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

  COPY INTO DataWarehouse.bronze.crm_prd_info
  FROM '/Volumes/datawarehouse/bronze/source_crm/prd_info.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

  COPY INTO DataWarehouse.bronze.crm_sales_details
  FROM '/Volumes/datawarehouse/bronze/source_crm/sales_details.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

  -- -------------------------
  -- ERP loads
  -- -------------------------
  COPY INTO DataWarehouse.bronze.erp_cust_az12
  FROM '/Volumes/datawarehouse/bronze/source_erp/CUST_AZ12.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

  COPY INTO DataWarehouse.bronze.erp_loc_a101
  FROM '/Volumes/datawarehouse/bronze/source_erp/LOC_A101.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

  COPY INTO DataWarehouse.bronze.erp_px_cat_g1v2
  FROM '/Volumes/datawarehouse/bronze/source_erp/PX_CAT_G1V2.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

  UPDATE DataWarehouse.bronze.bronze_load_log
  SET
    status = 'OK',
    end_ts = current_timestamp(),
    duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(start_ts),
    message = 'All COPY INTO loads completed'
  WHERE run_id = v_run_id AND step = 'LOAD';



  -- ===================================================================================
  -- STEP 4: ROW COUNTS (Audit)
  -- ===================================================================================
  INSERT INTO DataWarehouse.bronze.bronze_load_log
  SELECT v_run_id, 'ROWCOUNT', 'OK',
         concat('crm_cust_info rows=', COUNT(*)),
         current_timestamp(), current_timestamp(), 0
  FROM DataWarehouse.bronze.crm_cust_info;

  INSERT INTO DataWarehouse.bronze.bronze_load_log
  SELECT v_run_id, 'ROWCOUNT', 'OK',
         concat('crm_prd_info rows=', COUNT(*)),
         current_timestamp(), current_timestamp(), 0
  FROM DataWarehouse.bronze.crm_prd_info;

  INSERT INTO DataWarehouse.bronze.bronze_load_log
  SELECT v_run_id, 'ROWCOUNT', 'OK',
         concat('crm_sales_details rows=', COUNT(*)),
         current_timestamp(), current_timestamp(), 0
  FROM DataWarehouse.bronze.crm_sales_details;

  INSERT INTO DataWarehouse.bronze.bronze_load_log
  SELECT v_run_id, 'ROWCOUNT', 'OK',
         concat('erp_cust_az12 rows=', COUNT(*)),
         current_timestamp(), current_timestamp(), 0
  FROM DataWarehouse.bronze.erp_cust_az12;

  INSERT INTO DataWarehouse.bronze.bronze_load_log
  SELECT v_run_id, 'ROWCOUNT', 'OK',
         concat('erp_loc_a101 rows=', COUNT(*)),
         current_timestamp(), current_timestamp(), 0
  FROM DataWarehouse.bronze.erp_loc_a101;

  INSERT INTO DataWarehouse.bronze.bronze_load_log
  SELECT v_run_id, 'ROWCOUNT', 'OK',
         concat('erp_px_cat_g1v2 rows=', COUNT(*)),
         current_timestamp(), current_timestamp(), 0
  FROM DataWarehouse.bronze.erp_px_cat_g1v2;


  -- ===================================================================================
  -- TOTAL END (overall duration)
  -- ===================================================================================
  UPDATE DataWarehouse.bronze.bronze_load_log
  SET
    status = 'OK',
    end_ts = current_timestamp(),
    duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(start_ts),
    message = 'Bronze load completed successfully'
  WHERE run_id = v_run_id AND step = 'TOTAL';

END;


-- =====================================================================================
-- 3) RUN COMMAND (use in another cell)
-- =====================================================================================
-- CALL DataWarehouse.bronze.sp_load_bronze();


-- =====================================================================================
-- 4) VIEW LATEST RUN LOGS
-- =====================================================================================
-- Latest run_id (most recent start)
-- WITH latest_run AS (
--   SELECT run_id
--   FROM DataWarehouse.bronze.bronze_load_log
--   ORDER BY start_ts DESC
--   LIMIT 1
-- )
-- SELECT *
-- FROM DataWarehouse.bronze.bronze_load_log
-- WHERE run_id = (SELECT run_id FROM latest_run)
-- ORDER BY start_ts;
