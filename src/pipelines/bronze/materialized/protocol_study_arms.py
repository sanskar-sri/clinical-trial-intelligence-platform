from src.Utility.bronze_materialized_common import ingest_materialized

ingest_materialized(

    table="protocol_study_arms",

    folder="protocol/study_arms",

    comment="Bronze study-arm data loaded from S3 Landing."

)