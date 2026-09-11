from src.Utility.bronze_materialized_common import ingest_materialized


ingest_materialized(
    table="ctms_studies",
    folder="CTMS/studies",
    comment="Bronze CTMS study master data loaded from S3 Landing."
)