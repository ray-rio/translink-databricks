# /// script
# requires-python = ">=3.9"
# dependencies = ["pyyaml"]
# ///
"""
Generate downstream config files from the central config.yml.

Usage:
    uv run scripts/sync_config.py

Generates:
    - connectors/.env.example
    - connectors/samconfig.toml.example
    - analytics/databricks.yml
"""

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config.yml"


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def derived(cfg):
    """Compute derived values from the base config."""
    cat = cfg["databricks"]["catalog"]
    sch = cfg["databricks"]["schema"]
    return {
        "volume_path": f"/Volumes/{cat}/{sch}/gtfs_static",
        "vehicle_positions_table": f"{cat}.{sch}.raw_vehicle_positions",
        "trip_updates_table": f"{cat}.{sch}.raw_trip_updates",
    }


# ── Generators ───────────────────────────────────────────────────────────


def generate_env_example(cfg, d):
    """Generate connectors/.env.example."""
    db = cfg["databricks"]
    tl = cfg["translink"]

    content = f"""\
# Generated from config.yml — do not edit directly.
# Run: uv run scripts/sync_config.py

# Databricks (Zerobus endpoint is derived: https://<workspace-id>.zerobus.<region>.cloud.databricks.com)
DATABRICKS_WORKSPACE_URL={db["workspace_url"]}
DATABRICKS_WORKSPACE_ID={db["workspace_id"]}
DATABRICKS_REGION={cfg["aws"]["region"]}
DATABRICKS_CLIENT_ID=<service-principal-app-id>
DATABRICKS_CLIENT_SECRET=<service-principal-secret>
DATABRICKS_TABLE_NAME={d["vehicle_positions_table"]}
DATABRICKS_TRIP_UPDATES_TABLE_NAME={d["trip_updates_table"]}

# Translink
TRANSLINK_API_URL={tl["vehicle_positions_url"]}
TRANSLINK_TRIP_UPDATES_URL={tl["trip_updates_url"]}

# GTFS Static (gtfs-static connector)
DATABRICKS_VOLUME_PATH={d["volume_path"]}
TRANSLINK_GTFS_STATIC_URL={tl["gtfs_static_url"]}

# Logging
LOG_LEVEL={cfg["log_level"]}
"""
    out = ROOT / "connectors" / ".env.example"
    out.write_text(content)
    print(f"  wrote {out.relative_to(ROOT)}")


def generate_samconfig_example(cfg, d):
    """Generate connectors/samconfig.toml.example."""
    db = cfg["databricks"]
    aws = cfg["aws"]

    content = f"""\
# Generated from config.yml — do not edit directly.
# Run: uv run scripts/sync_config.py
#
# Copy to samconfig.toml and fill in secrets:
#   cp samconfig.toml.example samconfig.toml
#
# Then deploy with:
#   sam build --use-container && sam deploy

version = 0.1

[default.build.parameters]
cached = true

[default.deploy.parameters]
stack_name = "{aws["stack_name"]}"
resolve_s3 = true
s3_prefix = "{aws["stack_name"]}"
region = "{aws["region"]}"
capabilities = "CAPABILITY_IAM"
confirm_changeset = true

parameter_overrides = [
    "DatabricksWorkspaceUrl={db["workspace_url"]}",
    "DatabricksWorkspaceId={db["workspace_id"]}",
    "DatabricksRegion={aws["region"]}",
    "DatabricksClientId=<service-principal-app-id>",
    "DatabricksClientSecret=<service-principal-secret>",
    "DatabricksVehiclePositionsTableName={d["vehicle_positions_table"]}",
    "DatabricksTripUpdatesTableName={d["trip_updates_table"]}",
    "DatabricksVolumePath={d["volume_path"]}",
    "TranslinkApiUrl={cfg["translink"]["vehicle_positions_url"]}",
    "TranslinkTripUpdatesUrl={cfg["translink"]["trip_updates_url"]}",
    "TranslinkGtfsStaticUrl={cfg["translink"]["gtfs_static_url"]}",
]
"""
    out = ROOT / "connectors" / "samconfig.toml.example"
    out.write_text(content)
    print(f"  wrote {out.relative_to(ROOT)}")


def generate_databricks_yml(cfg, d):
    """Generate analytics/databricks.yml."""
    db = cfg["databricks"]

    content = f"""\
# Generated from config.yml — do not edit directly.
# Run: uv run scripts/sync_config.py

bundle:
  name: translink_analytics

variables:
  catalog:
    default: {db["catalog"]}
  schema:
    default: {db["schema"]}
  volume_path:
    default: {d["volume_path"]}
  service_principal:
    default: {db["service_principal"]}
  warehouse_id:
    lookup:
      warehouse: "Serverless Starter Warehouse"

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: {db["workspace_url"]}

  prod:
    mode: production
    workspace:
      host: {db["workspace_url"]}
"""
    out = ROOT / "analytics" / "databricks.yml"
    out.write_text(content)
    print(f"  wrote {out.relative_to(ROOT)}")


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    if not CONFIG_PATH.exists():
        print(f"ERROR: {CONFIG_PATH} not found", file=sys.stderr)
        sys.exit(1)

    cfg = load_config()
    d = derived(cfg)

    print(f"Syncing config from {CONFIG_PATH.relative_to(ROOT)} …")
    generate_env_example(cfg, d)
    generate_samconfig_example(cfg, d)
    generate_databricks_yml(cfg, d)
    print("Done.")


if __name__ == "__main__":
    main()
