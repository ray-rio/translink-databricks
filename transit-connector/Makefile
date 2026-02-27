# =============================================================================
# SAM custom build for Lambda.
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
# =============================================================================

.PHONY: build-TransitConnectorFunction build-TripUpdatesConnectorFunction

build-TransitConnectorFunction:
	@test -f requirements.txt || { echo "ERROR: requirements.txt not found. Run: uv export --frozen --no-dev --no-hashes -o requirements.txt"; exit 1; }
	pip install --target "$(ARTIFACTS_DIR)" -r requirements.txt
	# Copy handler + compiled protobuf modules
	cp handler.py "$(ARTIFACTS_DIR)/"
	cp gtfs_realtime_pb2.py "$(ARTIFACTS_DIR)/"
	cp vehicle_position_pb2.py "$(ARTIFACTS_DIR)/"

build-TripUpdatesConnectorFunction:
	@test -f requirements.txt || { echo "ERROR: requirements.txt not found. Run: uv export --frozen --no-dev --no-hashes -o requirements.txt"; exit 1; }
	pip install --target "$(ARTIFACTS_DIR)" -r requirements.txt
	# Copy handler + compiled protobuf modules
	cp handler_trip_updates.py "$(ARTIFACTS_DIR)/"
	cp gtfs_realtime_pb2.py "$(ARTIFACTS_DIR)/"
	cp trip_update_pb2.py "$(ARTIFACTS_DIR)/"
