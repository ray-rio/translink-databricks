#!/usr/bin/env bash
# =============================================================================
# Build a Lambda-ready zip from the UV-managed project.
#
# Usage:  ./build.sh
# Output: dist/lambda.zip
#
# The Zerobus SDK ships a native Rust wheel (.so), so the pip install step
# must target the Lambda runtime platform (linux, x86_64 or aarch64).
# Set LAMBDA_ARCH to match your Lambda function's architecture.
# =============================================================================
set -euo pipefail

LAMBDA_ARCH="${LAMBDA_ARCH:-x86_64}"          # or aarch64
LAMBDA_PYTHON="${LAMBDA_PYTHON:-3.13}"        # match your Lambda runtime
DIST_DIR="dist"
PKG_DIR="${DIST_DIR}/package"

echo "🔧 Building Lambda zip (arch=${LAMBDA_ARCH}, python=${LAMBDA_PYTHON})"

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
cp handler.py "${PKG_DIR}/"
cp gtfs_realtime_pb2.py "${PKG_DIR}/"
cp vehicle_position_pb2.py "${PKG_DIR}/"

# ── Zip ──────────────────────────────────────────────────────────────────
(cd "${PKG_DIR}" && zip -qr ../lambda.zip .)

SIZE=$(du -sh "${DIST_DIR}/lambda.zip" | cut -f1)
echo "✅ ${DIST_DIR}/lambda.zip (${SIZE})"
