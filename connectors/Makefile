# =============================================================================
# SAM custom build for Lambda connectors.
#
# The Zerobus SDK contains native Rust code and has no pre-built Linux wheels,
# so `sam build --use-container` is REQUIRED. The container (Amazon Linux)
# compiles the Rust extension natively for the Lambda runtime.
#
# Before building, generate requirements.txt from the UV lockfile:
#   uv export --frozen --no-dev --no-hashes -o requirements.txt
#
# SAM invokes:  make build-<FunctionLogicalId>
# and sets ARTIFACTS_DIR to the target output directory.
#
# Each target installs shared deps, then copies the connector-specific
# handler + proto from its subdirectory, plus the shared GTFS-RT proto.
# =============================================================================

.PHONY: build-VehiclePositionsFunction build-TripUpdatesFunction build-GtfsStaticFunction

build-VehiclePositionsFunction:
	@test -f requirements.txt || { echo "ERROR: requirements.txt not found. Run: uv export --frozen --no-dev --no-hashes -o requirements.txt"; exit 1; }
	pip install --target "$(ARTIFACTS_DIR)" -r requirements.txt
	cp vehicle-positions/handler.py "$(ARTIFACTS_DIR)/"
	cp shared/gtfs_realtime_pb2.py "$(ARTIFACTS_DIR)/"
	cp shared/rt_feed.py "$(ARTIFACTS_DIR)/"
	cp shared/rt_ingest.py "$(ARTIFACTS_DIR)/"
	cp vehicle-positions/vehicle_position_pb2.py "$(ARTIFACTS_DIR)/"

build-TripUpdatesFunction:
	@test -f requirements.txt || { echo "ERROR: requirements.txt not found. Run: uv export --frozen --no-dev --no-hashes -o requirements.txt"; exit 1; }
	pip install --target "$(ARTIFACTS_DIR)" -r requirements.txt
	cp trip-updates/handler.py "$(ARTIFACTS_DIR)/"
	cp shared/gtfs_realtime_pb2.py "$(ARTIFACTS_DIR)/"
	cp shared/rt_feed.py "$(ARTIFACTS_DIR)/"
	cp shared/rt_ingest.py "$(ARTIFACTS_DIR)/"
	cp trip-updates/trip_update_pb2.py "$(ARTIFACTS_DIR)/"

build-GtfsStaticFunction:
	@test -f requirements.txt || { echo "ERROR: requirements.txt not found. Run: uv export --frozen --no-dev --no-hashes -o requirements.txt"; exit 1; }
	pip install --target "$(ARTIFACTS_DIR)" -r requirements.txt
	cp gtfs-static/handler.py "$(ARTIFACTS_DIR)/"
