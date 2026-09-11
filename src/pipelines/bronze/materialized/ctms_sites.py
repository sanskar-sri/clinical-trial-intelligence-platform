from src.Utility.bronze_materialized_common import ingest_materialized


ingest_materialized(
    table="ctms_sites",
    folder="CTMS/sites",
    comment="Bronze CTMS site master data loaded from S3 Landing."
)