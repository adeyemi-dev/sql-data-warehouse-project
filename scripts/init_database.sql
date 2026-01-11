%sql
-- =====================================================================================
-- SCRIPT NAME: 00_setup_medallion_catalog.sql
-- PURPOSE:
--   Create a simple Medallion Architecture structure (Bronze/Silver/Gold) in Databricks.
--
-- WHAT THIS SCRIPT DOES:
--   1) Creates a Unity Catalog catalog called: DataWarehouse
--   2) Creates three schemas inside it: bronze, silver, gold
--   3) Verifies creation by listing catalogs and schemas
--   4) (Optional) Creates example tables to confirm everything works end-to-end
--
-- PLATFORM / CONTEXT:
--   - Databricks FREE EDITION
--   - Unity Catalog enabled (catalog -> schema -> table)
--
-- HOW TO RUN:
--   - Paste into a Databricks SQL notebook cell (SQL Editor / Databricks SQL)
--   - Run top to bottom
--
-- NOTES:
--   - If you don't have permission to create catalogs, you'll get a permission error.
--   - In Free Edition, permissions/features can be limited depending on workspace settings.
-- =====================================================================================

-- -------------------------
-- 0) (Optional) Check what catalogs you already have
-- -------------------------
SHOW CATALOGS;

-- -------------------------
-- 1) Create the catalog
-- -------------------------
-- A "catalog" is the top-level container in Unity Catalog.
CREATE CATALOG IF NOT EXISTS DataWarehouse;

-- -------------------------
-- 2) Use the catalog
-- -------------------------
USE CATALOG DataWarehouse;

-- -------------------------
-- 3) Create the schemas (Bronze / Silver / Gold)
-- -------------------------
-- Schemas are where your tables and views will live.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- -------------------------
-- 4) Verify schemas were created
-- -------------------------
SHOW SCHEMAS IN DataWarehouse;


