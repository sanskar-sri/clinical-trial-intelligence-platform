from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "clinical_trial_intelligence"


@dp.materialized_view(
    name="gold_subject_summary",
    comment="One row per subject combining Gold subject spine with visit, AE and laboratory rollups."
)
def gold_subject_summary():

    spine = spark.table("gold_subject_spine")

    visits = (
        spark.table(f"{CATALOG}.silver.visits")
        .groupBy("subject_id")
        .agg(
            F.count("*").alias("total_visits"),

            F.sum(
                F.when(F.col("visit_status") == "COMPLETED", 1).otherwise(0)
            ).alias("completed_visits"),

            F.sum(
                F.when(F.col("visit_status") == "MISSED", 1).otherwise(0)
            ).alias("missed_visits"),

            F.sum(
                F.when(F.col("visit_status") == "RESCHEDULED", 1).otherwise(0)
            ).alias("rescheduled_visits"),

            F.min("visit_date").alias("first_visit_date"),
            F.max("visit_date").alias("last_visit_date")
        )
        .withColumn(
            "visit_completion_rate",
            F.when(
                F.col("total_visits") > 0,
                F.col("completed_visits") / F.col("total_visits")
            )
        )
    )

    ae = (
        spark.table(f"{CATALOG}.silver.adverse_events")
        .groupBy("subject_id")
        .agg(
            F.count("*").alias("ae_count"),

            F.sum(
                F.when(F.col("serious_flag") == "YES", 1).otherwise(0)
            ).alias("serious_ae_count"),

            F.sum(
                F.when(F.upper(F.col("outcome")) == "FATAL", 1).otherwise(0)
            ).alias("fatal_ae_count"),

            F.sum(
                F.when(
                    F.col("resolution_date").isNull(), 1
                ).otherwise(0)
            ).alias("ongoing_ae_count"),

            F.max("severity_rank").alias("max_ae_severity")
        )
    )

    labs = (
        spark.table(f"{CATALOG}.silver.lab_results")
        .groupBy("subject_id")
        .agg(
            F.count("*").alias("total_lab_results"),

            F.sum(
                F.when(
                    F.upper(F.col("derived_abnormal_flag")).isin("HIGH", "LOW"),
                    1
                ).otherwise(0)
            ).alias("abnormal_lab_count"),

            F.sum(
                F.when(
                    F.col("abnormal_flag_discrepancy") == True,
                    1
                ).otherwise(0)
            ).alias("lab_flag_discrepancy_count")
        )
        .withColumn(
            "abnormal_lab_rate",
            F.when(
                F.col("total_lab_results") > 0,
                F.col("abnormal_lab_count") / F.col("total_lab_results")
            )
        )
    )

    return (
        spine
        .join(visits, "subject_id", "left")
        .join(ae, "subject_id", "left")
        .join(labs, "subject_id", "left")

        .withColumn(
            "days_since_last_visit",
            F.when(
                F.col("last_visit_date").isNotNull(),
                F.datediff(F.current_date(), F.col("last_visit_date"))
            )
        )

        .withColumn("_gold_generated_at", F.current_timestamp())
    )