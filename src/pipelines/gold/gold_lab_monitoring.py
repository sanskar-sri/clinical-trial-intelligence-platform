from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "clinical_trial_intelligence"


@dp.materialized_view(
    name="gold_lab_monitoring",
    comment=(
        "Study-site-test laboratory monitoring using standardized "
        "values and derived abnormality."
    )
)
def gold_lab_monitoring():

    # ----------------------------------------------------
    # Silver laboratory results
    # ----------------------------------------------------
    labs = (
        spark.table(
            f"{CATALOG}.silver.lab_results"
        )
        .alias("l")
    )

    # ----------------------------------------------------
    # Current trusted subject -> site mapping
    #
    # silver.lab_results does not physically contain site_id.
    # Therefore site_id is derived from the current Silver
    # subject record using subject_id.
    # ----------------------------------------------------
    subjects = (
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

    # ----------------------------------------------------
    # Enrich labs with site_id
    # ----------------------------------------------------
    l = (
        labs
        .join(
            subjects,
            F.col("l.subject_id")
            == F.col("s.subject_id"),
            "left"
        )
        .select(
            "l.*",
            F.col("s.site_id").alias("site_id")
        )
    )

    # ----------------------------------------------------
    # Study + site + laboratory test aggregation
    # ----------------------------------------------------
    x = (
        l.groupBy(
            "study_id",
            "site_id",
            "lab_test_code",
            "lab_test_name",
            "standard_unit"
        )
        .agg(
            F.count("*")
            .alias("total_results"),

            F.countDistinct("subject_id")
            .alias("subjects_tested"),

            F.sum(
                F.when(
                    F.upper(
                        F.col("derived_abnormal_flag")
                    ) == "NORMAL",
                    1
                ).otherwise(0)
            )
            .alias("normal_count"),

            F.sum(
                F.when(
                    F.upper(
                        F.col("derived_abnormal_flag")
                    ) == "LOW",
                    1
                ).otherwise(0)
            )
            .alias("low_count"),

            F.sum(
                F.when(
                    F.upper(
                        F.col("derived_abnormal_flag")
                    ) == "HIGH",
                    1
                ).otherwise(0)
            )
            .alias("high_count"),

            F.sum(
                F.when(
                    F.upper(
                        F.col("derived_abnormal_flag")
                    ).isin(
                        "LOW",
                        "HIGH"
                    ),
                    1
                ).otherwise(0)
            )
            .alias("abnormal_count"),

            F.countDistinct(
                F.when(
                    F.upper(
                        F.col("derived_abnormal_flag")
                    ).isin(
                        "LOW",
                        "HIGH"
                    ),
                    F.col("subject_id")
                )
            )
            .alias("subjects_with_abnormal"),

            F.sum(
                F.when(
                    F.col(
                        "abnormal_flag_discrepancy"
                    ) == True,
                    1
                ).otherwise(0)
            )
            .alias(
                "source_derived_discrepancy_count"
            ),

            F.min(
                "standardized_result_value"
            )
            .alias(
                "min_standardized_value"
            ),

            F.expr(
                """
                percentile_approx(
                    standardized_result_value,
                    0.5
                )
                """
            )
            .alias(
                "median_standardized_value"
            ),

            F.max(
                "standardized_result_value"
            )
            .alias(
                "max_standardized_value"
            )
        )
    )

    # ----------------------------------------------------
    # Final Gold metrics
    #
    # Rates retain their numerator and denominator columns
    # so downstream aggregation can be recalculated safely.
    # ----------------------------------------------------
    return (
        x.withColumn(
            "abnormal_rate",
            F.when(
                F.col("total_results") > 0,
                F.col("abnormal_count")
                / F.col("total_results")
            )
        )
        .withColumn(
            "discrepancy_rate",
            F.when(
                F.col("total_results") > 0,
                F.col(
                    "source_derived_discrepancy_count"
                )
                / F.col("total_results")
            )
        )
        .withColumn(
            "_gold_generated_at",
            F.current_timestamp()
        )
    )