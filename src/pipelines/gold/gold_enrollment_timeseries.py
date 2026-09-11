from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="gold_enrollment_timeseries",
    comment="Dense monthly study-site enrollment and disposition timeseries with cumulative enrollment metrics."
)
def gold_enrollment_timeseries():

    s = spark.table("gold_subject_spine")

    boundaries = (
        s.groupBy("study_id", "site_id")
        .agg(
            F.trunc(
                F.min(
                    F.least(
                        F.coalesce("screening_date", F.lit("9999-12-31").cast("date")),
                        F.coalesce("enrollment_date", F.lit("9999-12-31").cast("date")),
                        F.coalesce("randomization_date", F.lit("9999-12-31").cast("date"))
                    )
                ),
                "month"
            ).alias("first_month"),

            F.trunc(
                F.greatest(
                    F.max("screening_date"),
                    F.max("enrollment_date"),
                    F.max("randomization_date"),
                    F.max("discontinuation_date"),
                    F.current_date()
                ),
                "month"
            ).alias("last_month")
        )
        .where(F.col("first_month") < F.lit("9999-12-01").cast("date"))
    )

    months = (
        boundaries
        .select(
            "study_id",
            "site_id",
            F.explode(
                F.sequence(
                    "first_month",
                    "last_month",
                    F.expr("INTERVAL 1 MONTH")
                )
            ).alias("month_start")
        )
    )

    def monthly(date_col, alias_name):
        return (
            s.where(F.col(date_col).isNotNull())
            .groupBy(
                "study_id",
                "site_id",
                F.trunc(date_col, "month").alias("month_start")
            )
            .agg(
                F.countDistinct("subject_id").alias(alias_name)
            )
        )

    screened = monthly("screening_date", "screened_subjects")
    enrolled = monthly("enrollment_date", "enrolled_subjects")
    randomized = monthly("randomization_date", "randomized_subjects")
    discontinued = monthly("discontinuation_date", "discontinued_subjects")

    x = (
        months
        .join(screened, ["study_id", "site_id", "month_start"], "left")
        .join(enrolled, ["study_id", "site_id", "month_start"], "left")
        .join(randomized, ["study_id", "site_id", "month_start"], "left")
        .join(discontinued, ["study_id", "site_id", "month_start"], "left")
        .fillna(
            0,
            subset=[
                "screened_subjects",
                "enrolled_subjects",
                "randomized_subjects",
                "discontinued_subjects"
            ]
        )
    )

    w = (
        Window
        .partitionBy("study_id", "site_id")
        .orderBy("month_start")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        x
        .withColumn(
            "cum_screened_subjects",
            F.sum("screened_subjects").over(w)
        )
        .withColumn(
            "cum_enrolled_subjects",
            F.sum("enrolled_subjects").over(w)
        )
        .withColumn(
            "cum_randomized_subjects",
            F.sum("randomized_subjects").over(w)
        )
        .withColumn("_gold_generated_at", F.current_timestamp())
    )