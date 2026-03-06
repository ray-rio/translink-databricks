-- Databricks notebook source
-- Initialise Unity Catalog -- Permissions
--
-- Grants the service principal access to the catalog, schema, tables, and volume
-- created by the init_tables notebook.
-- All GRANT statements are idempotent -- safe to re-run.
--
-- Parameters (passed via base_parameters from the DABs job):
--   catalog            -- Unity Catalog name
--   schema             -- Schema name
--   service_principal  -- Service principal application ID
--
-- Run via DABs:
--   databricks bundle run init_catalog -t dev

-- COMMAND ----------

-- Catalog & Schema Access

EXECUTE IMMEDIATE
  'GRANT USE_CATALOG ON CATALOG ' || :catalog || ' TO `' || :service_principal || '`';

-- COMMAND ----------

EXECUTE IMMEDIATE
  'GRANT USE_SCHEMA ON SCHEMA ' || :catalog || '.' || :schema || ' TO `' || :service_principal || '`';

-- COMMAND ----------

-- Table Permissions (Zerobus ingestion requires MODIFY and SELECT)

EXECUTE IMMEDIATE
  'GRANT MODIFY, SELECT ON TABLE ' || :catalog || '.' || :schema || '.raw_vehicle_positions TO `' || :service_principal || '`';

-- COMMAND ----------

EXECUTE IMMEDIATE
  'GRANT MODIFY, SELECT ON TABLE ' || :catalog || '.' || :schema || '.raw_trip_updates TO `' || :service_principal || '`';

-- COMMAND ----------

-- Volume Permissions (gtfs-static Lambda needs read/write access)

EXECUTE IMMEDIATE
  'GRANT READ VOLUME, WRITE VOLUME ON VOLUME ' || :catalog || '.' || :schema || '.gtfs_static TO `' || :service_principal || '`';

-- COMMAND ----------

SELECT 'Permissions granted' AS status, :service_principal AS granted_to;
