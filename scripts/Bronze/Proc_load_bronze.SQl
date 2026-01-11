-- =====================================================================================
-- FILE: bronze_load_procedure.sql
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
%sql
CREATE OR REPLACE TABLE DataWarehouse.bronze.bronze_load_log (
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

--New Cell
-- =====================================================================================
-- 2) STORED PROCEDURE
-- =====================================================================================
%sql
CREATE OR REPLACE PROCEDURE DataWarehouse.bronze.sp_load_bronze()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN

  DECLARE v_run_id STRING;
  DECLARE v_step_start TIMESTAMP;

  SET v_run_id = uuid();

  -- =========================
  -- START (overall)
  -- =========================
  SET v_step_start = current_timestamp();

  INSERT INTO DataWarehouse.bronze.bronze_load_log
  VALUES (
    v_run_id, 'TOTAL', 'RUNNING',
    'Bronze load started',
    v_step_start, NULL, NULL
  );

  -- =========================
  -- CREATE TABLES
  -- =========================
  SET v_step_start = current_timestamp();

  INSERT INTO DataWarehouse.bronze.bronze_load_log
  VALUES (
    v_run_id, 'CREATE_TABLES', 'RUNNING',
    'Creating Bronze tables',
    v_step_start, NULL, NULL
  );
  -- ===================================================================================
  -- STEP 1: CREATE / REPLACE TABLES (CRM + ERP)
  -- ===================================================================================
  CREATE OR REPLACE TABLE DataWarehouse.bronze.crm_cust_info (
    cst_id INT, cst_key STRING, cst_firstname STRING,
    cst_lastname STRING, cst_marital_status STRING,
    cst_gndr STRING, cst_create_date DATE
  ) USING DELTA;

  CREATE OR REPLACE TABLE DataWarehouse.bronze.crm_prd_info (
    prd_id INT, prd_key STRING, prd_nm STRING,
    prd_cost INT, prd_line STRING,
    prd_start_dt DATE, prd_end_dt DATE
  ) USING DELTA;

  CREATE OR REPLACE TABLE DataWarehouse.bronze.crm_sales_details (
    sls_ord_num STRING, sls_prd_key STRING, sls_cust_id INT,
    sls_order_dt INT, sls_ship_dt INT, sls_due_dt INT,
    sls_sales INT, sls_quantity INT, sls_price INT
  ) USING DELTA;

  CREATE OR REPLACE TABLE DataWarehouse.bronze.erp_cust_az12 (
    CID STRING, BDATE DATE, GEN STRING
  ) USING DELTA;

  CREATE OR REPLACE TABLE DataWarehouse.bronze.erp_loc_a101 (
    CID STRING, CNTRY STRING
  ) USING DELTA;

  CREATE OR REPLACE TABLE DataWarehouse.bronze.erp_px_cat_g1v2 (
    ID STRING, CAT STRING, SUBCAT STRING, MAINTENANCE STRING
  ) USING DELTA;

  UPDATE DataWarehouse.bronze.bronze_load_log
  SET status = 'OK',
      end_ts = current_timestamp(),
      duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(start_ts)
  WHERE run_id = v_run_id AND step = 'CREATE_TABLES';

  -- ===================================================================================
  -- STEP 2: TRUNCATE TABLES (Clean Reload)
  -- ===================================================================================
  SET v_step_start = current_timestamp();

  INSERT INTO DataWarehouse.bronze.bronze_load_log
  VALUES (
    v_run_id, 'TRUNCATE', 'RUNNING',
    'Truncating Bronze tables',
    v_step_start, NULL, NULL
  );

  TRUNCATE TABLE DataWarehouse.bronze.crm_cust_info;
  TRUNCATE TABLE DataWarehouse.bronze.crm_prd_info;
  TRUNCATE TABLE DataWarehouse.bronze.crm_sales_details;
  TRUNCATE TABLE DataWarehouse.bronze.erp_cust_az12;
  TRUNCATE TABLE DataWarehouse.bronze.erp_loc_a101;
  TRUNCATE TABLE DataWarehouse.bronze.erp_px_cat_g1v2;

  UPDATE DataWarehouse.bronze.bronze_load_log
  SET status = 'OK',
      end_ts = current_timestamp(),
      duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(start_ts)
  WHERE run_id = v_run_id AND step = 'TRUNCATE';

  -- =========================
  -- LOAD (COPY INTO)
  -- =========================
  SET v_step_start = current_timestamp();

  INSERT INTO DataWarehouse.bronze.bronze_load_log
  VALUES (
    v_run_id, 'LOAD', 'RUNNING',
    'Loading CSV files into Bronze',
    v_step_start, NULL, NULL
  );

  COPY INTO DataWarehouse.bronze.crm_cust_info
  FROM '/Volumes/datawarehouse/bronze/source_crm/cust_info.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true','inferSchema'='true');

  COPY INTO DataWarehouse.bronze.crm_prd_info
  FROM '/Volumes/datawarehouse/bronze/source_crm/prd_info.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true','inferSchema'='true');

  COPY INTO DataWarehouse.bronze.crm_sales_details
  FROM '/Volumes/datawarehouse/bronze/source_crm/sales_details.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true','inferSchema'='true');

  COPY INTO DataWarehouse.bronze.erp_cust_az12
  FROM '/Volumes/datawarehouse/bronze/source_erp/CUST_AZ12.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true','inferSchema'='true');

  COPY INTO DataWarehouse.bronze.erp_loc_a101
  FROM '/Volumes/datawarehouse/bronze/source_erp/LOC_A101.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true','inferSchema'='true');

  COPY INTO DataWarehouse.bronze.erp_px_cat_g1v2
  FROM '/Volumes/datawarehouse/bronze/source_erp/PX_CAT_G1V2.csv'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header'='true','inferSchema'='true');

  UPDATE DataWarehouse.bronze.bronze_load_log
  SET status = 'OK',
      end_ts = current_timestamp(),
      duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(start_ts)
  WHERE run_id = v_run_id AND step = 'LOAD';

  -- =========================
  -- END (total)
  -- =========================
  UPDATE DataWarehouse.bronze.bronze_load_log
  SET status = 'OK',
      end_ts = current_timestamp(),
      duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(start_ts),
      message = 'Bronze load completed successfully'
  WHERE run_id = v_run_id AND step = 'TOTAL';

END;

-- New Cell
CALL DataWarehouse.bronze.sp_load_bronze();

--New Cell

%sql
SELECT
  run_id,
  step,
  start_ts,
  end_ts,
  unix_timestamp(end_ts) - unix_timestamp(start_ts) AS duration_seconds
FROM DataWarehouse.bronze.bronze_load_log
WHERE step = 'TOTAL'
ORDER BY start_ts DESC;

-- New Cell

%sql
SELECT
  step,
  duration_seconds
FROM DataWarehouse.bronze.bronze_load_log
WHERE run_id = (
  SELECT run_id
  FROM DataWarehouse.bronze.bronze_load_log
  ORDER BY start_ts DESC
  LIMIT 1
)
ORDER BY start_ts;

