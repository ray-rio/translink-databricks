"""
Zerobus stream lifecycle helper for real-time Lambda connectors.

Encapsulates SDK initialisation, stream creation, batch ingestion with
ack collection, and cleanup. Used by both VehiclePositions and TripUpdates.
"""

import logging
import os

from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

logger = logging.getLogger(__name__)


def ingest_records(records, proto_descriptor, table_name):
    """Create a Zerobus stream, ingest all records, wait for acks, and close.

    Args:
        records: Iterable of protobuf message instances to ingest.
        proto_descriptor: The protobuf DESCRIPTOR for the target table schema.
        table_name: Fully qualified Delta table name.

    Returns:
        int: Number of records successfully ingested.
    """
    workspace_url = os.environ["DATABRICKS_WORKSPACE_URL"]
    workspace_id = os.environ["DATABRICKS_WORKSPACE_ID"]
    region = os.environ["DATABRICKS_REGION"]
    client_id = os.environ["DATABRICKS_CLIENT_ID"]
    client_secret = os.environ["DATABRICKS_CLIENT_SECRET"]

    server_endpoint = f"https://{workspace_id}.zerobus.{region}.cloud.databricks.com"

    sdk = ZerobusSdk(server_endpoint, workspace_url)
    table_props = TableProperties(table_name, proto_descriptor)
    options = StreamConfigurationOptions(record_type=RecordType.PROTO)
    stream = sdk.create_stream(client_id, client_secret, table_props, options)

    try:
        acks = []
        for record in records:
            ack = stream.ingest_record(record)
            acks.append(ack)

        for ack in acks:
            ack.wait_for_ack()

        return len(acks)
    finally:
        stream.close()
