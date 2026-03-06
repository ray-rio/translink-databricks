# /// script
# requires-python = ">=3.9"
# dependencies = ["pyyaml"]
# ///
"""
Generate downstream config files from the central config.yml.

Usage:
    uv run scripts/sync_config.py

Generates (committed to git — masked secrets):
    - connectors/.env.example
    - connectors/samconfig.toml.example

Generates (gitignored — real values):
    - connectors/.env
    - connectors/samconfig.toml
    - analytics/databricks.yml
"""

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config.yml"

# Keys to mask in .example files (value replaced with <placeholder-description>)
MASKED_KEYS = {
    "client_id": "<service-principal-app-id>",
    "client_secret": "<service-principal-secret>",
    "workspace_url": "https://<workspace>.cloud.databricks.com",
    "workspace_id": "<workspace-id>",
    "catalog": "<catalog-name>",
    "schema": "<schema-name>",
}


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


def masked(cfg):
    """Compute derived values using masked placeholders (for .example files)."""
    cat = MASKED_KEYS["catalog"]
    sch = MASKED_KEYS["schema"]
    return {
        "volume_path": f"/Volumes/{cat}/{sch}/gtfs_static",
        "vehicle_positions_table": f"{cat}.{sch}.raw_vehicle_positions",
        "trip_updates_table": f"{cat}.{sch}.raw_trip_updates",
    }


# ── Content builders ─────────────────────────────────────────────────────


def _env_content(cfg, d, db_overrides=None):
    db = {**cfg["databricks"], **(db_overrides or {})}
    tl = cfg["translink"]
    region = cfg["aws"]["region"]
    return f"""\
# Generated from config.yml — do not edit directly.
# Run: uv run scripts/sync_config.py

# Databricks (Zerobus endpoint is derived: https://<workspace-id>.zerobus.<region>.cloud.databricks.com)
DATABRICKS_WORKSPACE_URL={db["workspace_url"]}
DATABRICKS_WORKSPACE_ID={db["workspace_id"]}
DATABRICKS_REGION={region}
DATABRICKS_CLIENT_ID={db["client_id"]}
DATABRICKS_CLIENT_SECRET={db["client_secret"]}
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


def _samconfig_content(cfg, d, db_overrides=None):
    db = {**cfg["databricks"], **(db_overrides or {})}
    aws = cfg["aws"]
    return f"""\
# Generated from config.yml — do not edit directly.
# Run: uv run scripts/sync_config.py

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
    "DatabricksClientId={db["client_id"]}",
    "DatabricksClientSecret={db["client_secret"]}",
    "DatabricksVehiclePositionsTableName={d["vehicle_positions_table"]}",
    "DatabricksTripUpdatesTableName={d["trip_updates_table"]}",
    "DatabricksVolumePath={d["volume_path"]}",
    "TranslinkApiUrl={cfg["translink"]["vehicle_positions_url"]}",
    "TranslinkTripUpdatesUrl={cfg["translink"]["trip_updates_url"]}",
    "TranslinkGtfsStaticUrl={cfg["translink"]["gtfs_static_url"]}",
]
"""


# ── Generators ───────────────────────────────────────────────────────────


def generate_env(cfg, d):
    """Generate .env (real values) and .env.example (masked)."""
    env_path = ROOT / "connectors" / ".env"
    example_path = ROOT / "connectors" / ".env.example"

    example_path.write_text(_env_content(cfg, masked(cfg), db_overrides=MASKED_KEYS))
    print(f"  wrote {example_path.relative_to(ROOT)}")

    env_path.write_text(_env_content(cfg, d))
    print(f"  wrote {env_path.relative_to(ROOT)}")


def generate_samconfig(cfg, d):
    """Generate samconfig.toml (real values) and .example (masked)."""
    sam_path = ROOT / "connectors" / "samconfig.toml"
    example_path = ROOT / "connectors" / "samconfig.toml.example"

    example_path.write_text(_samconfig_content(cfg, masked(cfg), db_overrides=MASKED_KEYS))
    print(f"  wrote {example_path.relative_to(ROOT)}")

    sam_path.write_text(_samconfig_content(cfg, d))
    print(f"  wrote {sam_path.relative_to(ROOT)}")


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
    default: {db["client_id"]}
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
        print("  Copy config.yml.example to config.yml and fill in your values.", file=sys.stderr)
        sys.exit(1)

    cfg = load_config()
    d = derived(cfg)

    print(f"Syncing config from {CONFIG_PATH.relative_to(ROOT)} …")
    generate_env(cfg, d)
    generate_samconfig(cfg, d)
    generate_databricks_yml(cfg, d)
    print("Done.")


if __name__ == "__main__":
    main()
