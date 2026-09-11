from src.Utility.bronze_materialized_common import ingest_materialized

ingest_materialized(
    table="ref_sex_mapping",
    folder="reference/sex_mapping.csv",
    comment="Bronze sex mapping reference data loaded from S3 Landing."
)