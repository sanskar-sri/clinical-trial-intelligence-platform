from src.Utility.bronze_materialized_common import ingest_materialized

ingest_materialized(
    table="ref_diagnosis_mapping",
    folder="reference/diagnosis_mapping.csv",
    comment="Bronze diagnosis mapping reference data loaded from S3 Landing."
)