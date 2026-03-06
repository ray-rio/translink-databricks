#!/usr/bin/env bash
# =============================================================================
# Build a Lambda-ready zip for a specific connector.
#
# Usage:  ./build.sh [connector]
#   connector: vehicle-positions (default) | trip-updates
# Output: dist/<connector>.zip
#
# The Zerobus SDK ships a native Rust wheel (.so), so the pip install step
# must target the Lambda runtime platform (linux, x86_64 or aarch64).
# Set LAMBDA_ARCH to match your Lambda function's architecture.
# =============================================================================
set -euo pipefail

CONNECTOR="${1:-vehicle-positions}"
LAMBDA_ARCH="${LAMBDA_ARCH:-x86_64}"          # or aarch64
LAMBDA_PYTHON="${LAMBDA_PYTHON:-3.13}"        # match your Lambda runtime
DIST_DIR="dist"
PKG_DIR="${DIST_DIR}/package"

# Validate connector name
case "${CONNECTOR}" in
  vehicle-positions)
    HANDLER="${CONNECTOR}/handler.py"
    PROTO="${CONNECTOR}/vehicle_position_pb2.py"
    RT_SHARED=true
    ;;
  trip-updates)
    HANDLER="${CONNECTOR}/handler.py"
    PROTO="${CONNECTOR}/trip_update_pb2.py"
    RT_SHARED=true
    ;;
  gtfs-static)
    HANDLER="${CONNECTOR}/handler.py"
    PROTO=""
    RT_SHARED=false
    ;;
  *)
    echo "ERROR: Unknown connector '${CONNECTOR}'. Use: vehicle-positions | trip-updates | gtfs-static"
    exit 1
    ;;
esac

echo "Building Lambda zip for ${CONNECTOR} (arch=${LAMBDA_ARCH}, python=${LAMBDA_PYTHON})"

# ── Clean ────────────────────────────────────────────────────────────────
rm -rf "${DIST_DIR}"
mkdir -p "${PKG_DIR}"

# ── Export pinned requirements from lockfile ─────────────────────────────
uv export --no-dev --no-hashes -o "${DIST_DIR}/requirements.txt"

# ── Install deps targeting Lambda's Linux platform ───────────────────────
uv pip install \
  --target "${PKG_DIR}" \
  --python-platform linux \
  --python-version "${LAMBDA_PYTHON}" \
  -r "${DIST_DIR}/requirements.txt"

# ── Copy handler + compiled protos ───────────────────────────────────────
cp "${HANDLER}" "${PKG_DIR}/"
if [ -n "${PROTO}" ]; then
  cp shared/gtfs_realtime_pb2.py "${PKG_DIR}/"
  cp "${PROTO}" "${PKG_DIR}/"
fi
if [ "${RT_SHARED}" = true ]; then
  cp shared/rt_feed.py "${PKG_DIR}/"
  cp shared/rt_ingest.py "${PKG_DIR}/"
fi

# ── Zip ──────────────────────────────────────────────────────────────────
(cd "${PKG_DIR}" && zip -qr "../${CONNECTOR}.zip" .)

SIZE=$(du -sh "${DIST_DIR}/${CONNECTOR}.zip" | cut -f1)
echo "${DIST_DIR}/${CONNECTOR}.zip (${SIZE})"
