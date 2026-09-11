from src.Utility.bronze_common import ingest_feed


ingest_feed(
    table="lab_results",
    folder="Lab/lab_results",
    comment="Raw laboratory result records incrementally ingested from S3 Landing."
)