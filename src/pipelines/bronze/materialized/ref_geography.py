from src.Utility.bronze_materialized_common import ingest_materialized

ingest_materialized(

    table="ref_geography",

    folder="reference/geography.csv",

    comment="Bronze geography reference data loaded from S3 Landing."

)