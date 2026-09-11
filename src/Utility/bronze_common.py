from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date


# ---------------------------------------------------------
# Common Bronze ingestion function
# ---------------------------------------------------------

def ingest_feed(table, folder, comment):

    source_path = (
        "s3://clinical-trial-intelligence-platform-sk/"
        f"Landing/{folder}/"
    )

    @dp.table(
        name=table,
        comment=comment
    )
    def bronze_table():
        spark = SparkSession.getActiveSession()

        return (
            spark.readStream
            .format("cloudFiles")

            # Source files are CSV
            .option("cloudFiles.format", "csv")
            .option("header", "true")

            # Infer source column types
            .option("cloudFiles.inferColumnTypes", "true")

            # Use managed file events
            .option("cloudFiles.useManagedFileEvents", "true")

            # Read incrementally from S3
            .load(source_path)

            # Source file lineage
            .selectExpr(
                "*",
                "_metadata.file_path AS _source_file",
                "_metadata.file_name AS _source_file_name",
                "_metadata.file_modification_time "
                "AS _source_file_modification_ts"
            )

            # Ingestion metadata
            .withColumn(
                "_ingestion_ts",
                current_timestamp()
            )
            .withColumn(
                "_ingestion_date",
                current_date()
            )
        )