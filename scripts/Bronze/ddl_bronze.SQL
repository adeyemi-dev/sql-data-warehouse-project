%sql
-- =====================================================================================
-- NOTEBOOK: Bronze Layer Setup + Loads
-- PURPOSE:
--   1) Create/replace Bronze Delta tables for CRM and ERP sources
--   2) Bulk load CSV files from a Unity Catalog Volume into the Bronze tables
--
-- PLATFORM:
--   Databricks Free Edition
--   Unity Catalog enabled
--   SQL Dialect: Spark SQL (Delta Lake)
--
-- VOLUME USED (from your setup):
--   /Volumes/datawarehouse/bronze/source_crm/
-- =====================================================================================

-- =========================
-- CRM: Customer Information (matches cust_info.csv)
-- =========================
CREATE OR REPLACE TABLE DataWarehouse.bronze.crm_cust_info (
    cst_id INT COMMENT 'Customer identifier',
    cst_key STRING COMMENT 'Customer business key',
    cst_firstname STRING COMMENT 'Customer first name',
    cst_lastname STRING COMMENT 'Customer last name',
    cst_marital_status STRING COMMENT 'Customer marital status (raw)',
    cst_gndr STRING COMMENT 'Customer gender',
    cst_create_date DATE COMMENT 'Customer record creation date'
)
USING DELTA
COMMENT 'Bronze layer: raw customer information from CRM source system (no transformations)';

-- =========================
-- CRM: Product Information (matches prd_info.csv)
--   NOTE: prd_cost in the CSV is inferred as INT, so we store it as INT in Bronze.
--         Convert to DECIMAL in Silver/Gold if needed.
-- =========================
CREATE OR REPLACE TABLE DataWarehouse.bronze.crm_prd_info (
    prd_id INT COMMENT 'Product identifier',
    prd_key STRING COMMENT 'Business product key',
    prd_nm STRING COMMENT 'Product name',
    prd_cost INT COMMENT 'Product cost (raw integer)',
    prd_line STRING COMMENT 'Product line or category',
    prd_start_dt DATE COMMENT 'Product start date',
    prd_end_dt DATE COMMENT 'Product end date'
)
USING DELTA
COMMENT 'Bronze layer: raw product information from CRM source system (no transformations)';

-- =========================
-- CRM: Sales Details (matches sales_details.csv)
--   NOTE: dates are in YYYYMMDD format in the CSV, so we store them as INT in Bronze.
--         Convert to DATE in Silver.
-- =========================
CREATE OR REPLACE TABLE DataWarehouse.bronze.crm_sales_details (
    sls_ord_num STRING COMMENT 'Sales order number (business key)',
    sls_prd_key STRING COMMENT 'Product business key',
    sls_cust_id INT COMMENT 'Customer identifier',
    sls_order_dt INT COMMENT 'Order date in YYYYMMDD format (raw)',
    sls_ship_dt INT COMMENT 'Shipment date in YYYYMMDD format (raw)',
    sls_due_dt INT COMMENT 'Due date in YYYYMMDD format (raw)',
    sls_sales INT COMMENT 'Total sales amount (raw integer)',
    sls_quantity INT COMMENT 'Quantity sold',
    sls_price INT COMMENT 'Unit price (raw integer)'
)
USING DELTA
COMMENT 'Bronze layer: raw sales order details from CRM source system (no transformations)';

---------new cell
%sql
-- Optional: clear tables if you want a clean reload (uncomment if needed)
TRUNCATE TABLE DataWarehouse.bronze.crm_cust_info;
TRUNCATE TABLE DataWarehouse.bronze.crm_prd_info;
TRUNCATE TABLE DataWarehouse.bronze.crm_sales_details;

-- Customer bulk load
COPY INTO DataWarehouse.bronze.crm_cust_info
FROM '/Volumes/datawarehouse/bronze/source_crm/cust_info.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

-- Product bulk load
COPY INTO DataWarehouse.bronze.crm_prd_info
FROM '/Volumes/datawarehouse/bronze/source_crm/prd_info.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

-- Sales bulk load
COPY INTO DataWarehouse.bronze.crm_sales_details
FROM '/Volumes/datawarehouse/bronze/source_crm/sales_details.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

--New Cell
dbutils.fs.ls("dbfs:/Volumes/datawarehouse/bronze/source_erp/")
dbutils.fs.ls("/Volumes/datawarehouse/bronze/source_crm/")

%sql
SELECT 'crm_cust_info'      AS table_name, COUNT(*) AS row_count FROM DataWarehouse.bronze.crm_cust_info
UNION ALL
SELECT 'crm_prd_info'       AS table_name, COUNT(*) AS row_count FROM DataWarehouse.bronze.crm_prd_info
UNION ALL
SELECT 'crm_sales_details'  AS table_name, COUNT(*) AS row_count FROM DataWarehouse.bronze.crm_sales_details;


--New Cell
%sql
-- =====================================================================================
-- SCRIPT: ERP Bronze Tables + Bulk Loads (CSV)
-- PLATFORM: Databricks Free Edition | Unity Catalog | Delta Lake
-- SOURCE VOLUME: /Volumes/datawarehouse/bronze/source_erp/
-- =====================================================================================

-- =========================
-- Create/Replace ERP Bronze tables
-- =========================
-- =========================
-- ERP: Customer Location
-- =========================
CREATE OR REPLACE TABLE DataWarehouse.bronze.erp_loc_a101 (
  CID STRING COMMENT 'Customer identifier (raw, alphanumeric)',
  CNTRY STRING COMMENT 'Customer country'
)
USING DELTA
COMMENT 'Bronze layer: raw customer location data from ERP A101 source system';

-- =========================
-- ERP: Customer Demographics
-- =========================
CREATE OR REPLACE TABLE DataWarehouse.bronze.erp_cust_az12 (
  CID STRING COMMENT 'Customer identifier (raw, alphanumeric)',
  BDATE DATE COMMENT 'Customer birth date',
  GEN STRING COMMENT 'Customer gender'
)
USING DELTA
COMMENT 'Bronze layer: raw customer demographic data from ERP AZ12 source system';

-- =========================
-- ERP: Product Category
-- =========================
CREATE OR REPLACE TABLE DataWarehouse.bronze.erp_px_cat_g1v2 (
  ID STRING COMMENT 'Product identifier (raw, alphanumeric)',
  CAT STRING COMMENT 'Product category',
  SUBCAT STRING COMMENT 'Product sub-category',
  MAINTENANCE STRING COMMENT 'Maintenance or service classification'
)
USING DELTA
COMMENT 'Bronze layer: raw product category and maintenance classification data from ERP system';

-- =========================
-- Optional: TRUNCATE for clean reload (uncomment if needed)
-- =========================
TRUNCATE TABLE DataWarehouse.bronze.erp_cust_az12;
TRUNCATE TABLE DataWarehouse.bronze.erp_loc_a101;
TRUNCATE TABLE DataWarehouse.bronze.erp_px_cat_g1v2;

-- =========================
-- Bulk load from CSV files using COPY INTO
-- =========================

-- ERP CUST_AZ12
COPY INTO DataWarehouse.bronze.erp_cust_az12
FROM '/Volumes/datawarehouse/bronze/source_erp/CUST_AZ12.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

-- ERP LOC_A101
COPY INTO DataWarehouse.bronze.erp_loc_a101
FROM '/Volumes/datawarehouse/bronze/source_erp/LOC_A101.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

-- ERP PX_CAT_G1V2
COPY INTO DataWarehouse.bronze.erp_px_cat_g1v2
FROM '/Volumes/datawarehouse/bronze/source_erp/PX_CAT_G1V2.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

---new Cell
%sql
SELECT 'erp_cust_az12' AS table_name, COUNT(*) AS row_count FROM DataWarehouse.bronze.erp_cust_az12
UNION ALL
SELECT 'erp_loc_a101'  AS table_name, COUNT(*) AS row_count FROM DataWarehouse.bronze.erp_loc_a101
UNION ALL
SELECT 'erp_px_cat_g1v2' AS table_name, COUNT(*) AS row_count FROM DataWarehouse.bronze.erp_px_cat_g1v2;






