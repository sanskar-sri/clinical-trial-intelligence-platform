# master_products.py

from src.Utility.bronze_materialized_common import ingest_materialized


ingest_materialized(
    table="master_products",
    folder="master/products",
    comment="Bronze product master data loaded from S3 Landing."
)