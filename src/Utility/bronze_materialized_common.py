from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date


def ingest_materialized(table, folder, comment):

    source_path = (
        "s3://clinical-trial-intelligence-platform-sk/"
        f"Landing/{folder}/"
    )

    @dp.materialized_view(
        name=table,
        comment=comment
    )
    def bronze_materialized_view():

        spark = SparkSession.getActiveSession()

        return (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(source_path)

            .selectExpr(
                "*",
                "_metadata.file_path AS _source_file",
                "_metadata.file_name AS _source_file_name",
                "_metadata.file_modification_time "
                "AS _source_file_modification_ts"
            )

            .withColumn(
                "_ingestion_ts",
                current_timestamp()
            )
            .withColumn(
                "_ingestion_date",
                current_date()
            )
        )