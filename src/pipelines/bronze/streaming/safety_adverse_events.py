from src.Utility.bronze_common import ingest_feed


ingest_feed(
    table="safety_adverse_events",
    folder="Safety/adverse_events",
    comment="Raw safety adverse event records incrementally ingested from S3 Landing."
)