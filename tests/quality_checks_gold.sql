-- =====================================================================
-- PROJECT  : SQL DATA ENGINEERING PROJECT (Medallion Architecture)
-- LAYER    : GOLD - DATA QUALITY CHECKS
-- AUTHOR   : Afeez Laguda
-- PURPOSE  :
--   Validate Gold views (dim_customers, dim_products, fact_sales) for:
--     - Row counts + basic health
--     - Null / key integrity
--     - Uniqueness of surrogate + business keys
--     - Referential integrity (fact -> dims)
--     - Duplicate detection
--     - Basic measure sanity checks
--
-- HOW TO USE:
--   Run this notebook/script after your Gold views are created/refreshed.
--   Any query returning rows (or failing thresholds) indicates an issue.
-- =====================================================================


-- =====================================================================
-- A) QUICK HEALTH SUMMARY (ROW COUNTS)
-- =====================================================================

SELECT 'gold.dim_customers' AS object_name, COUNT(*) AS row_count
FROM datawarehouse.gold.dim_customers
UNION ALL
SELECT 'gold.dim_products'  AS object_name, COUNT(*) AS row_count
FROM datawarehouse.gold.dim_products
UNION ALL
SELECT 'gold.fact_sales'    AS object_name, COUNT(*) AS row_count
FROM datawarehouse.gold.fact_sales
;


-- =====================================================================
-- B) DIM_CUSTOMERS CHECKS
-- =====================================================================

-- B1) NULL checks for required keys
SELECT *
FROM datawarehouse.gold.dim_customers
WHERE customer_key IS NULL
   OR customer_id IS NULL
   OR customer_number IS NULL
LIMIT 50
;

-- B2) Uniqueness: customer_key must be unique
SELECT
  customer_key,
  COUNT(*) AS cnt
FROM datawarehouse.gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50
;

-- B3) Uniqueness: customer_id must be unique (expected 1 row per customer)
SELECT
  customer_id,
  COUNT(*) AS cnt
FROM datawarehouse.gold.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50
;

-- B4) Basic domain checks (optional sanity checks)
-- Gender should not be null (allowed 'N/A')
SELECT *
FROM datawarehouse.gold.dim_customers
WHERE gender IS NULL
LIMIT 50
;

-- Birth date should not be in the future (if present)
SELECT *
FROM datawarehouse.gold.dim_customers
WHERE birth_date IS NOT NULL
  AND birth_date > current_date()
LIMIT 50
;


-- =====================================================================
-- C) DIM_PRODUCTS CHECKS
-- =====================================================================

-- C1) NULL checks for required keys
SELECT *
FROM datawarehouse.gold.dim_products
WHERE product_key IS NULL
   OR product_id IS NULL
   OR product_number IS NULL
LIMIT 50
;

-- C2) Uniqueness: product_key must be unique
SELECT
  product_key,
  COUNT(*) AS cnt
FROM datawarehouse.gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50
;

-- C3) Uniqueness: product_number must be unique (active product list)
SELECT
  product_number,
  COUNT(*) AS cnt
FROM datawarehouse.gold.dim_products
GROUP BY product_number
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50
;

-- C4) Active product filter validation (should be zero rows)
-- Ensures your dim_products view is not leaking historical products.
SELECT *
FROM datawarehouse.silver.crm_prd_info
WHERE prd_end_dt IS NOT NULL
  AND prd_key IN (SELECT product_number FROM datawarehouse.gold.dim_products)
LIMIT 50
;

-- C5) Cost sanity: should not be negative
SELECT *
FROM datawarehouse.gold.dim_products
WHERE cost < 0
LIMIT 50
;


-- =====================================================================
-- D) FACT_SALES CHECKS
-- =====================================================================

-- D1) NULL checks for required columns
SELECT *
FROM datawarehouse.gold.fact_sales
WHERE order_number IS NULL
   OR product_key IS NULL
   OR customer_key IS NULL
   OR order_date IS NULL
LIMIT 50
;

-- D2) Duplicate detection (grain check)
-- If your intended grain is order_number + product_key + customer_key,
-- duplicates indicate double loading or join explosion.
SELECT
  order_number,
  product_key,
  customer_key,
  COUNT(*) AS cnt
FROM datawarehouse.gold.fact_sales
GROUP BY order_number, product_key, customer_key
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50
;

-- D3) Referential integrity: fact -> dim_products (orphans)
SELECT
  'MISSING_DIM_PRODUCT' AS issue_type,
  COUNT(*) AS missing_rows
FROM datawarehouse.gold.fact_sales fs
LEFT JOIN datawarehouse.gold.dim_products dp
  ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL
;

-- Show sample orphan product keys
SELECT
  fs.product_key,
  COUNT(*) AS cnt
FROM datawarehouse.gold.fact_sales fs
LEFT JOIN datawarehouse.gold.dim_products dp
  ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL
GROUP BY fs.product_key
ORDER BY cnt DESC
LIMIT 50
;

-- D4) Referential integrity: fact -> dim_customers (orphans)
SELECT
  'MISSING_DIM_CUSTOMER' AS issue_type,
  COUNT(*) AS missing_rows
FROM datawarehouse.gold.fact_sales fs
LEFT JOIN datawarehouse.gold.dim_customers dc
  ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL
;

-- Show sample orphan customer keys
SELECT
  fs.customer_key,
  COUNT(*) AS cnt
FROM datawarehouse.gold.fact_sales fs
LEFT JOIN datawarehouse.gold.dim_customers dc
  ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL
GROUP BY fs.customer_key
ORDER BY cnt DESC
LIMIT 50
;

-- D5) Measure sanity checks
-- Sales amount should not be negative (unless returns exist; adjust if needed)
SELECT *
FROM datawarehouse.gold.fact_sales
WHERE sales_amount < 0
LIMIT 50
;

-- Quantity should be > 0 (adjust if returns allowed)
SELECT *
FROM datawarehouse.gold.fact_sales
WHERE quantity <= 0
LIMIT 50
;

-- Price should be >= 0
SELECT *
FROM datawarehouse.gold.fact_sales
WHERE price < 0
LIMIT 50
;

-- D6) Date logic checks
-- ship_date and due_date should not be earlier than order_date (if present)
SELECT *
FROM datawarehouse.gold.fact_sales
WHERE (ship_date IS NOT NULL AND ship_date < order_date)
   OR (due_date  IS NOT NULL AND due_date  < order_date)
LIMIT 50
;


-- =====================================================================
-- E) RECONCILIATION CHECKS (OPTIONAL BUT STRONG)
-- =====================================================================

-- E1) Fact vs Silver rowcount reconciliation
-- Helps detect join explosion or heavy filtering.
SELECT
  (SELECT COUNT(*) FROM datawarehouse.silver.crm_sales_details) AS silver_sales_details_rows,
  (SELECT COUNT(*) FROM datawarehouse.gold.fact_sales)         AS gold_fact_sales_rows
;

-- E2) Join coverage diagnostics (how many rows got matched to dims)
SELECT
  COUNT(*) AS total_fact_rows,
  SUM(CASE WHEN product_key  IS NULL THEN 1 ELSE 0 END) AS null_product_key_rows,
  SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_key_rows
FROM datawarehouse.gold.fact_sales
;


-- =====================================================================
-- F) OPTIONAL: SINGLE "PASS/FAIL" SCOREBOARD (SUMMARY TABLE)
-- =====================================================================
-- Returns PASS/FAIL flags for key checks.
-- You can extend thresholds as needed.

WITH
dc AS (SELECT COUNT(*) AS n FROM datawarehouse.gold.dim_customers),
dp AS (SELECT COUNT(*) AS n FROM datawarehouse.gold.dim_products),
fs AS (SELECT COUNT(*) AS n FROM datawarehouse.gold.fact_sales),

dc_null AS (
  SELECT COUNT(*) AS n
  FROM datawarehouse.gold.dim_customers
  WHERE customer_key IS NULL OR customer_id IS NULL OR customer_number IS NULL
),
dp_null AS (
  SELECT COUNT(*) AS n
  FROM datawarehouse.gold.dim_products
  WHERE product_key IS NULL OR product_id IS NULL OR product_number IS NULL
),
fs_null AS (
  SELECT COUNT(*) AS n
  FROM datawarehouse.gold.fact_sales
  WHERE order_number IS NULL OR product_key IS NULL OR customer_key IS NULL OR order_date IS NULL
),

fs_orphan_prod AS (
  SELECT COUNT(*) AS n
  FROM datawarehouse.gold.fact_sales fs
  LEFT JOIN datawarehouse.gold.dim_products dp
    ON fs.product_key = dp.product_key
  WHERE dp.product_key IS NULL
),
fs_orphan_cust AS (
  SELECT COUNT(*) AS n
  FROM datawarehouse.gold.fact_sales fs
  LEFT JOIN datawarehouse.gold.dim_customers dc
    ON fs.customer_key = dc.customer_key
  WHERE dc.customer_key IS NULL
)

SELECT
  'ROWCOUNT_DIM_CUSTOMERS' AS check_name,
  CASE WHEN (SELECT n FROM dc) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  (SELECT n FROM dc) AS metric
UNION ALL
SELECT
  'ROWCOUNT_DIM_PRODUCTS',
  CASE WHEN (SELECT n FROM dp) > 0 THEN 'PASS' ELSE 'FAIL' END,
  (SELECT n FROM dp)
UNION ALL
SELECT
  'ROWCOUNT_FACT_SALES',
  CASE WHEN (SELECT n FROM fs) > 0 THEN 'PASS' ELSE 'FAIL' END,
  (SELECT n FROM fs)
UNION ALL
SELECT
  'NULLS_DIM_CUSTOMERS_KEYS',
  CASE WHEN (SELECT n FROM dc_null) = 0 THEN 'PASS' ELSE 'FAIL' END,
  (SELECT n FROM dc_null)
UNION ALL
SELECT
  'NULLS_DIM_PRODUCTS_KEYS',
  CASE WHEN (SELECT n FROM dp_null) = 0 THEN 'PASS' ELSE 'FAIL' END,
  (SELECT n FROM dp_null)
UNION ALL
SELECT
  'NULLS_FACT_SALES_REQUIRED',
  CASE WHEN (SELECT n FROM fs_null) = 0 THEN 'PASS' ELSE 'FAIL' END,
  (SELECT n FROM fs_null)
UNION ALL
SELECT
  'ORPHANS_FACT_TO_DIM_PRODUCTS',
  CASE WHEN (SELECT n FROM fs_orphan_prod) = 0 THEN 'PASS' ELSE 'FAIL' END,
  (SELECT n FROM fs_orphan_prod)
UNION ALL
SELECT
  'ORPHANS_FACT_TO_DIM_CUSTOMERS',
  CASE WHEN (SELECT n FROM fs_orphan_cust) = 0 THEN 'PASS' ELSE 'FAIL' END,
  (SELECT n FROM fs_orphan_cust)
;
