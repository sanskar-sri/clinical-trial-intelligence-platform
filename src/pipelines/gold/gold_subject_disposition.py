from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_subject_disposition",
    comment="Study-site subject disposition funnel. Rates retain their numerator and denominator components."
)
def gold_subject_disposition():

    s = spark.table("gold_subject_spine")

    x = (
        s.groupBy("study_id", "site_id")
        .agg(
            F.sum("ever_screened_flag").alias("screened_subjects"),
            F.sum("ever_screen_failed_flag").alias("screen_failed_subjects"),
            F.sum("ever_enrolled_flag").alias("enrolled_subjects"),
            F.sum("ever_randomized_flag").alias("randomized_subjects"),
            F.sum("ever_completed_flag").alias("completed_subjects"),
            F.sum("ever_discontinued_flag").alias("discontinued_subjects"),

            F.sum(
                F.when(
                    (F.col("ever_enrolled_flag") == 1)
                    & (F.col("ever_completed_flag") == 0)
                    & (F.col("ever_discontinued_flag") == 0),
                    1
                ).otherwise(0)
            ).alias("ongoing_subjects")
        )
    )

    return (
        x
        .withColumn(
            "screen_failure_rate",
            F.when(
                F.col("screened_subjects") > 0,
                F.col("screen_failed_subjects") / F.col("screened_subjects")
            )
        )
        .withColumn(
            "randomization_rate",
            F.when(
                F.col("enrolled_subjects") > 0,
                F.col("randomized_subjects") / F.col("enrolled_subjects")
            )
        )
        .withColumn(
            "completion_rate",
            F.when(
                F.col("enrolled_subjects") > 0,
                F.col("completed_subjects") / F.col("enrolled_subjects")
            )
        )
        .withColumn(
            "discontinuation_rate",
            F.when(
                F.col("enrolled_subjects") > 0,
                F.col("discontinued_subjects") / F.col("enrolled_subjects")
            )
        )
        .withColumn("_gold_generated_at", F.current_timestamp())
    )


@dp.materialized_view(
    name="gold_discontinuation_reasons",
    comment="Subject discontinuation counts by study, site and discontinuation reason."
)
def gold_discontinuation_reasons():

    return (
        spark.table("gold_subject_spine")
        .where(F.col("ever_discontinued_flag") == 1)
        .withColumn(
            "discontinuation_reason",
            F.coalesce(
                F.col("discontinuation_reason"),
                F.lit("UNKNOWN")
            )
        )
        .groupBy(
            "study_id",
            "site_id",
            "discontinuation_reason"
        )
        .agg(
            F.countDistinct("subject_id")
            .alias("discontinued_subjects")
        )
        .withColumn("_gold_generated_at", F.current_timestamp())
    )