from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "clinical_trial_intelligence"


@dp.materialized_view(
    name="gold_visit_compliance",
    comment="Visit compliance by study, site and protocol visit. eligible_visits = COMPLETED + MISSED + RESCHEDULED; UNKNOWN is excluded."
)
def gold_visit_compliance():

    v = spark.table(f"{CATALOG}.silver.visits")

    x = (
        v.groupBy(
            "study_id",
            "site_id",
            "visit_name",
            "visit_number"
        )
        .agg(
            F.countDistinct("subject_id").alias("subjects_with_visit"),

            F.sum(
                F.when(
                    F.col("visit_status").isin(
                        "COMPLETED",
                        "MISSED",
                        "RESCHEDULED"
                    ),
                    1
                ).otherwise(0)
            ).alias("eligible_visits"),

            F.sum(
                F.when(F.col("visit_status") == "COMPLETED", 1).otherwise(0)
            ).alias("completed_visits"),

            F.sum(
                F.when(F.col("visit_status") == "MISSED", 1).otherwise(0)
            ).alias("missed_visits"),

            F.sum(
                F.when(F.col("visit_status") == "RESCHEDULED", 1).otherwise(0)
            ).alias("rescheduled_visits"),

            F.sum(
                F.when(
                    ~F.col("visit_status").isin(
                        "COMPLETED",
                        "MISSED",
                        "RESCHEDULED"
                    ),
                    1
                ).otherwise(0)
            ).alias("unknown_status_visits"),

            F.expr(
                "percentile_approx(days_from_baseline, 0.5)"
            ).alias("median_days_from_baseline")
        )
    )

    return (
        x
        .withColumn(
            "visit_completion_rate",
            F.when(
                F.col("eligible_visits") > 0,
                F.col("completed_visits") / F.col("eligible_visits")
            )
        )
        .withColumn(
            "missed_visit_rate",
            F.when(
                F.col("eligible_visits") > 0,
                F.col("missed_visits") / F.col("eligible_visits")
            )
        )
        .withColumn("_gold_generated_at", F.current_timestamp())
    )