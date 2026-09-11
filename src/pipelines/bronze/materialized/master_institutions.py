# master_institutions.py

from src.Utility.bronze_materialized_common import ingest_materialized


ingest_materialized(
    table="master_institutions",
    folder="master/institutions",
    comment="Bronze institution master data loaded from S3 Landing."
)