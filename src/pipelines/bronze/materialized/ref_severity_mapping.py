from src.Utility.bronze_materialized_common import ingest_materialized

ingest_materialized(
    table="ref_severity_mapping",
    folder="reference/severity_mapping.csv",
    comment="Bronze severity mapping reference data loaded from S3 Landing."
)