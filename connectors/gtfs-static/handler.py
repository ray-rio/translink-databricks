"""
AWS Lambda handler — Translink GTFS Static Feed → Databricks Volume.

Downloads the SEQ GTFS static ZIP from Translink, extracts key CSV files,
and uploads them to a Unity Catalog Volume via the Databricks Files API.

Runs daily (not every minute like the real-time connectors).  No Zerobus SDK
or protobuf — just HTTP requests and stdlib zipfile.
"""

import io
import logging
import os
import zipfile

import requests as http_requests

# ── Logging ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Only extract these files (skip shapes.txt, stop_times.txt — too large for Phase 2)
TARGET_FILES = {
    "agency.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "calendar.txt",
    "calendar_dates.txt",
}


# ── Databricks OAuth ───────────────────────────────────────────────────

def get_oauth_token(workspace_url, client_id, client_secret):
    """Acquire an OAuth2 M2M access token from the Databricks workspace."""
    token_url = f"{workspace_url}/oidc/v1/token"
    resp = http_requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "all-apis",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# ── File operations ────────────────────────────────────────────────────

def download_gtfs_zip(url):
    """Download the GTFS static ZIP file and return as bytes."""
    resp = http_requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def upload_to_volume(workspace_url, token, volume_path, filename, content):
    """Upload a single file to a Databricks Unity Catalog Volume."""
    # Files API: PUT /api/2.0/fs/files/<volume-path>/<filename>
    api_url = f"{workspace_url}/api/2.0/fs/files{volume_path}/{filename}"
    resp = http_requests.put(
        api_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        },
        params={"overwrite": "true"},
        data=content,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.status_code


# ── Lambda entry point ─────────────────────────────────────────────────

def lambda_handler(event, context):
    """AWS Lambda handler."""
    gtfs_url = os.environ["TRANSLINK_GTFS_STATIC_URL"]
    workspace_url = os.environ["DATABRICKS_WORKSPACE_URL"]
    client_id = os.environ["DATABRICKS_CLIENT_ID"]
    client_secret = os.environ["DATABRICKS_CLIENT_SECRET"]
    volume_path = os.environ["DATABRICKS_VOLUME_PATH"]
    # e.g. /Volumes/tc/translink/gtfs_static

    # 1. Acquire OAuth token
    logger.info("Acquiring Databricks OAuth token …")
    token = get_oauth_token(workspace_url, client_id, client_secret)

    # 2. Download GTFS ZIP
    logger.info("Downloading GTFS static feed from %s", gtfs_url)
    zip_bytes = download_gtfs_zip(gtfs_url)
    logger.info("Downloaded %d bytes", len(zip_bytes))

    # 3. Extract and upload target CSVs
    uploaded = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if name in TARGET_FILES:
                content = zf.read(name)
                logger.info("Uploading %s (%d bytes) …", name, len(content))
                upload_to_volume(workspace_url, token, volume_path, name, content)
                uploaded.append(name)

    logger.info("Uploaded %d files: %s", len(uploaded), uploaded)

    return {
        "statusCode": 200,
        "body": f"Uploaded {len(uploaded)} GTFS static files: {', '.join(sorted(uploaded))}",
    }


# ── Local testing ───────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = lambda_handler({}, None)
    print(result)
