from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "clinical_trial_intelligence"


@dp.materialized_view(
    name="gold_subject_spine",
    comment="One row per trusted subject. Canonical Gold subject population and disposition flags."
)
def gold_subject_spine():

    s = spark.table(f"{CATALOG}.silver.subjects")

    # Full SCD2 history -> ever-state flags and version metadata
    history = (
        s.groupBy("subject_id")
        .agg(
            F.max(F.when(F.col("screening_date").isNotNull(), 1).otherwise(0))
                .alias("ever_screened_flag"),

            F.max(F.when(F.col("enrollment_date").isNotNull(), 1).otherwise(0))
                .alias("ever_enrolled_flag"),

            F.max(F.when(F.col("randomization_date").isNotNull(), 1).otherwise(0))
                .alias("ever_randomized_flag"),

            F.max(
                F.when(
                    F.upper(F.col("subject_status")).isin("COMPLETED", "COMPLETE"),
                    1
                ).otherwise(0)
            ).alias("ever_completed_flag"),

            F.max(
                F.when(
                    F.col("discontinuation_date").isNotNull()
                    | F.upper(F.col("subject_status")).isin(
                        "DISCONTINUED", "WITHDRAWN"
                    ),
                    1
                ).otherwise(0)
            ).alias("ever_discontinued_flag"),

            F.max(
                F.when(
                    F.upper(F.col("subject_status")).isin(
                        "SCREEN FAILED", "SCREEN_FAILED", "SCREEN FAILURE"
                    ),
                    1
                ).otherwise(0)
            ).alias("ever_screen_failed_flag"),

            F.count("*").alias("subject_version_count"),

            F.min(F.col("__START_AT.source_snapshot_date"))
                .alias("first_version_at"),

            F.max(F.col("__START_AT.source_snapshot_date"))
                .alias("last_version_at")
        )
    )

    current = (
        s.where(F.col("__END_AT").isNull())
        .select(
            "subject_id",
            "study_id",
            "site_id",
            "arm_code",
            "age",
            "sex",
            "baseline_condition_code",
            "baseline_condition",
            "informed_consent_date",
            "screening_date",
            "enrollment_date",
            "randomization_date",
            "discontinuation_date",
            "discontinuation_reason",
            F.col("subject_status").alias("current_subject_status")
        )
    )

    return (
        current
        .join(history, "subject_id", "left")
        .withColumn(
            "age_group",
            F.when(F.col("age") < 65, "<65")
             .when(F.col("age") >= 65, ">=65")
             .otherwise("UNKNOWN")
        )
        .withColumn(
            "days_on_study",
            F.when(
                F.col("screening_date").isNotNull(),
                F.datediff(
                    F.coalesce(
                        F.col("discontinuation_date"),
                        F.current_date()
                    ),
                    F.col("screening_date")
                )
            )
        )
        .withColumn("_gold_generated_at", F.current_timestamp())
    )