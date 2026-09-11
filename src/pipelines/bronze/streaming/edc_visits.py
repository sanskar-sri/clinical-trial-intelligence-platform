from src.Utility.bronze_common import ingest_feed

ingest_feed(
    table="edc_visits",
    folder="EDC/visits",
    comment="Raw EDC visit records incrementally ingested from S3 Landing."
)