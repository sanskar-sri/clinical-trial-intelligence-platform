from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "clinical_trial_intelligence"


@dp.materialized_view(
    name="gold_site_performance",
    comment="Study-site operational performance and RBQM-style monitoring metrics."
)
def gold_site_performance():

    # ============================================================
    # 1. SUBJECT METRICS
    # ============================================================

    s = spark.table("gold_subject_spine")

    subjects = (
        s.groupBy(
            "study_id",
            "site_id"
        )
        .agg(
            F.min("screening_date")
                .alias("first_screening_date"),

            F.min("enrollment_date")
                .alias("first_enrollment_date"),

            F.max("enrollment_date")
                .alias("last_enrollment_date"),

            F.sum("ever_screened_flag")
                .alias("subjects_screened"),

            F.sum("ever_enrolled_flag")
                .alias("subjects_enrolled"),

            F.sum("ever_randomized_flag")
                .alias("subjects_randomized"),

            F.sum("ever_completed_flag")
                .alias("subjects_completed"),

            F.sum("ever_discontinued_flag")
                .alias("subjects_discontinued")
        )
        .withColumn(
            "active_months",
            F.when(
                F.col("first_screening_date").isNotNull(),
                F.greatest(
                    F.lit(1.0),
                    F.months_between(
                        F.current_date(),
                        F.col("first_screening_date")
                    )
                )
            )
        )
    )

    # ============================================================
    # 2. VISIT METRICS
    # silver.visits already contains site_id
    # ============================================================

    visits = (
        spark.table(
            f"{CATALOG}.silver.visits"
        )
        .groupBy(
            "study_id",
            "site_id"
        )
        .agg(
            F.count("*")
                .alias("scheduled_visits"),

            F.sum(
                F.when(
                    F.upper(F.col("visit_status")) == "COMPLETED",
                    1
                ).otherwise(0)
            )
            .alias("completed_visits"),

            F.sum(
                F.when(
                    F.upper(F.col("visit_status")) == "MISSED",
                    1
                ).otherwise(0)
            )
            .alias("missed_visits")
        )
    )

    # ============================================================
    # 3. ADVERSE EVENT METRICS
    # silver.adverse_events already contains site_id
    # ============================================================

    ae = (
        spark.table(
            f"{CATALOG}.silver.adverse_events"
        )
        .groupBy(
            "study_id",
            "site_id"
        )
        .agg(
            F.count("*")
                .alias("ae_events"),

            F.countDistinct("subject_id")
                .alias("subjects_with_ae"),

            F.countDistinct(
                F.when(
                    F.upper(F.col("serious_flag")) == "YES",
                    F.col("subject_id")
                )
            )
            .alias("subjects_with_serious_ae")
        )
    )

    # ============================================================
    # 4. LAB METRICS
    #
    # IMPORTANT:
    # silver.lab_results does NOT contain site_id.
    # Derive site_id using the current trusted Silver subject.
    # ============================================================

    labs_raw = (
        spark.table(
            f"{CATALOG}.silver.lab_results"
        )
        .alias("l")
    )

    lab_subject_sites = (
        spark.table(
            f"{CATALOG}.silver.subjects"
        )
        .where(
            F.col("__END_AT").isNull()
        )
        .select(
            "subject_id",
            "site_id"
        )
        .dropDuplicates(
            ["subject_id"]
        )
        .alias("s")
    )

    labs_enriched = (
        labs_raw
        .join(
            lab_subject_sites,
            F.col("l.subject_id")
            == F.col("s.subject_id"),
            "left"
        )
        .select(
            "l.*",
            F.col("s.site_id").alias("site_id")
        )
    )

    labs = (
        labs_enriched
        .groupBy(
            "study_id",
            "site_id"
        )
        .agg(
            F.count("*")
                .alias("lab_results"),

            F.sum(
                F.when(
                    F.upper(
                        F.col("derived_abnormal_flag")
                    ).isin(
                        "HIGH",
                        "LOW"
                    ),
                    1
                ).otherwise(0)
            )
            .alias("abnormal_lab_results")
        )
    )

    # ============================================================
    # 5. COMBINE SITE-LEVEL METRICS
    # ============================================================

    x = (
        subjects
        .join(
            visits,
            ["study_id", "site_id"],
            "left"
        )
        .join(
            ae,
            ["study_id", "site_id"],
            "left"
        )
        .join(
            labs,
            ["study_id", "site_id"],
            "left"
        )
    )

    # ============================================================
    # 6. DERIVED SITE PERFORMANCE METRICS
    #
    # Numerators and denominators are retained so downstream
    # dashboards can correctly recalculate aggregate rates.
    # ============================================================

    return (
        x

        # --------------------------------------------------------
        # Screen failure
        # --------------------------------------------------------
        .withColumn(
            "screen_failure_count",
            F.col("subjects_screened")
            - F.col("subjects_enrolled")
        )

        .withColumn(
            "screen_failure_rate",
            F.when(
                F.col("subjects_screened") > 0,
                F.col("screen_failure_count")
                / F.col("subjects_screened")
            )
        )

        # --------------------------------------------------------
        # Discontinuation
        # --------------------------------------------------------
        .withColumn(
            "discontinuation_rate",
            F.when(
                F.col("subjects_enrolled") > 0,
                F.col("subjects_discontinued")
                / F.col("subjects_enrolled")
            )
        )

        # --------------------------------------------------------
        # Enrollment velocity
        # --------------------------------------------------------
        .withColumn(
            "enrollment_rate_per_month",
            F.when(
                F.col("active_months") > 0,
                F.col("subjects_enrolled")
                / F.col("active_months")
            )
        )

        # --------------------------------------------------------
        # Visit compliance
        # --------------------------------------------------------
        .withColumn(
            "visit_completion_rate",
            F.when(
                F.col("scheduled_visits") > 0,
                F.col("completed_visits")
                / F.col("scheduled_visits")
            )
        )

        # --------------------------------------------------------
        # AE burden
        # --------------------------------------------------------
        .withColumn(
            "ae_events_per_subject",
            F.when(
                F.col("subjects_enrolled") > 0,
                F.col("ae_events")
                / F.col("subjects_enrolled")
            )
        )

        # --------------------------------------------------------
        # Laboratory abnormality
        # --------------------------------------------------------
        .withColumn(
            "abnormal_lab_rate",
            F.when(
                F.col("lab_results") > 0,
                F.col("abnormal_lab_results")
                / F.col("lab_results")
            )
        )

        # --------------------------------------------------------
        # Gold audit timestamp
        # --------------------------------------------------------
        .withColumn(
            "_gold_generated_at",
            F.current_timestamp()
        )
    )