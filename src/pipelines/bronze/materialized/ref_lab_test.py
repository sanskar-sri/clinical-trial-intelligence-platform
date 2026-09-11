from src.Utility.bronze_materialized_common import ingest_materialized


ingest_materialized(
    table="ref_lab_test",
    folder="reference/lab_test_reference.csv",
    comment="Bronze lab test reference data loaded from S3 Landing."
)