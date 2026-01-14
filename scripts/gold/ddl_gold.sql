-- =====================================================================
-- PROJECT  : SQL DATA ENGINEERING PROJECT (Medallion Architecture)
-- LAYER    : GOLD (Business-ready Views)
-- AUTHOR   : Afeez Laguda
-- PURPOSE  :
--   Create Gold layer star-schema views for analytics and BI:
--     1) dim_customers  - Customer master dimension (enriched)
--     2) dim_products   - Product master dimension (active only)
--     3) fact_sales     - Sales fact (order-line grain)
--
-- NOTES:
--   - Surrogate keys are deterministic using xxhash64() to avoid key drift.
--   - Gold views are built from Silver curated tables.
-- =====================================================================


-- =====================================================================
-- 1) GOLD DIMENSION: Customers
-- =====================================================================
-- PURPOSE:
--   Business-ready customer dimension.
--   Enriches CRM customers with ERP attributes (gender, birthdate, country).
--
-- GRAIN:
--   1 row per customer_id (ci.cst_id)
-- =====================================================================

CREATE OR REPLACE VIEW datawarehouse.gold.dim_customers AS
SELECT
    -- Deterministic surrogate key (stable across runs)
    xxhash64(ci.cst_id) AS customer_key,

    -- Business identifiers
    ci.cst_id  AS customer_id,
    ci.cst_key AS customer_number,

    -- Descriptive attributes
    ci.cst_firstname      AS first_name,
    ci.cst_lastname       AS last_name,
    la.CNTRY              AS country,
    ci.cst_marital_status AS marital_status,

    -- Gender resolution:
    --   Use CRM gender unless it is 'N/A', otherwise fall back to ERP gender.
    CASE
        WHEN ci.cst_gndr <> 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(ca.GEN, 'N/A')
    END AS gender,

    -- Dates
    ci.cst_create_date AS create_date,
    ca.BDATE           AS birth_date

FROM datawarehouse.silver.crm_cust_info ci
LEFT JOIN datawarehouse.silver.erp_cust_az12 ca
    ON ci.cst_key = ca.CID
LEFT JOIN datawarehouse.silver.erp_loc_a101 la
    ON ci.cst_key = la.CID
;


-- =====================================================================
-- 2) GOLD DIMENSION: Products
-- =====================================================================
-- PURPOSE:
--   Business-ready product dimension (active products only).
--   Enriches CRM products with ERP category hierarchy.
--
-- GRAIN:
--   1 row per active product_number (pn.prd_key) where prd_end_dt is NULL
-- =====================================================================

CREATE OR REPLACE VIEW datawarehouse.gold.dim_products AS
SELECT
    -- Deterministic surrogate key (stable across runs)
    xxhash64(pn.prd_key) AS product_key,

    -- Business identifiers
    pn.prd_id  AS product_id,
    pn.prd_key AS product_number,

    -- Descriptive attributes
    pn.prd_nm     AS product_name,
    pn.cat_id     AS category_id,
    pc.CAT        AS category,
    pc.SUBCAT     AS sub_category,
    pc.MAINTENANCE,
    pn.prd_cost   AS cost,
    pn.prd_line   AS product_line,

    -- Dates
    pn.prd_start_dt AS start_date

FROM datawarehouse.silver.crm_prd_info pn
LEFT JOIN datawarehouse.silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.ID

-- Keep only active products (filter out history)
WHERE pn.prd_end_dt IS NULL
;


-- =====================================================================
-- 3) GOLD FACT: Sales
-- =====================================================================
-- PURPOSE:
--   Business-ready sales fact for BI reporting.
--   Connects sales detail to customer/product dimensions.
--
-- GRAIN:
--   1 row per order line (order_number + product_number + customer_id)
-- =====================================================================

CREATE OR REPLACE VIEW datawarehouse.gold.fact_sales AS
SELECT
    -- Order identifiers
    sd.sls_ord_num AS order_number,

    -- Dimension keys
    prd.product_key,
    cu.customer_key,

    -- Dates
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS ship_date,
    sd.sls_due_dt   AS due_date,

    -- Measures
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price

FROM datawarehouse.silver.crm_sales_details sd
LEFT JOIN datawarehouse.gold.dim_products prd
    ON sd.sls_prd_key = prd.product_number
LEFT JOIN datawarehouse.gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id
;
