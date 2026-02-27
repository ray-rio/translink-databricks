-- =============================================================================
-- Grant Zerobus service principal access to the vehicle_positions table.
-- Required permissions: USE_CATALOG, USE_SCHEMA, MODIFY, SELECT
-- Ref: https://github.com/databricks/zerobus-sdk-py
-- =============================================================================

-- Set these before running
DECLARE OR REPLACE catalog_name      STRING DEFAULT 'your_catalog';
DECLARE OR REPLACE schema_name       STRING DEFAULT 'your_schema';
DECLARE OR REPLACE service_principal STRING DEFAULT 'your-service-principal-app-id';

EXECUTE IMMEDIATE
  'GRANT USE_CATALOG ON CATALOG ' || catalog_name || ' TO `' || service_principal || '`';

EXECUTE IMMEDIATE
  'GRANT USE_SCHEMA ON SCHEMA ' || catalog_name || '.' || schema_name || ' TO `' || service_principal || '`';

EXECUTE IMMEDIATE
  'GRANT MODIFY, SELECT ON TABLE ' || catalog_name || '.' || schema_name || '.vehicle_positions TO `' || service_principal || '`';

EXECUTE IMMEDIATE
  'GRANT MODIFY, SELECT ON TABLE ' || catalog_name || '.' || schema_name || '.trip_updates TO `' || service_principal || '`';
