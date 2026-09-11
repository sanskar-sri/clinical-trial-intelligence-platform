from src.Utility.bronze_common import ingest_feed

ingest_feed(

    table="edc_subjects",

    folder="EDC/subjects",

    comment="Raw EDC subject records incrementally ingested from S3 Landing."

)