from src.Utility.bronze_materialized_common import ingest_materialized


ingest_materialized(
    table="ref_country_region",
    folder="reference/country_region.csv",
    comment="Bronze country-to-region reference data loaded from S3 Landing."
)