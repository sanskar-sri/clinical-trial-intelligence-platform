# master_sponsors.py

from src.Utility.bronze_materialized_common import ingest_materialized


ingest_materialized(
    table="master_sponsors",
    folder="master/sponsors",
    comment="Bronze sponsor master data loaded from S3 Landing."
)