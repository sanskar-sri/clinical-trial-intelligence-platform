from src.Utility.bronze_materialized_common import ingest_materialized

ingest_materialized(
    table="ref_unit_mapping",
    folder="reference/unit_mapping.csv",
    comment="Bronze unit mapping reference data loaded from S3 Landing."
)