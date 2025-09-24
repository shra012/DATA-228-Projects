"""Metric computations for the Airbnb EMR analytics job."""

from __future__ import annotations

import datetime as dt
from typing import Dict, List

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T

from .utils import ratio


AMENITIES_OF_INTEREST: List[str] = [
    "Air conditioning",
    "Wifi",
    "Dedicated workspace",
    "Self check-in",
    "Free parking on premises",
    "Pool",
    "Hot tub",
    "Washer",
]


def neighborhood_investment_focus(calendar_enriched: DataFrame) -> DataFrame:
    """Evaluate revenue and demand trends by neighbourhood and month."""
    valid = calendar_enriched.filter(F.col("neighbourhood_cleansed").isNotNull())

    grouped = (
        valid
        .groupBy("neighbourhood_group_cleansed", "neighbourhood_cleansed", "month_start")
        .agg(
            F.countDistinct("listing_id").alias("active_listings"),
            F.sum("booked_night").alias("booked_nights"),
            F.sum("available_night").alias("available_nights"),
            F.sum("base_revenue").alias("booked_revenue"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("fee_revenue").alias("fee_revenue"),
            F.avg("price_numeric").alias("avg_listed_price"),
            F.avg("accommodates").alias("avg_accommodates"),
        )
        .withColumn("total_nights", F.col("booked_nights") + F.col("available_nights"))
        .withColumn("revpar", ratio(F.col("booked_revenue"), F.col("total_nights")))
        .withColumn("booking_rate", ratio(F.col("booked_nights"), F.col("total_nights")))
        .withColumn("supply_per_listing", ratio(F.col("available_nights"), F.col("active_listings")))
        .withColumn("fee_share", ratio(F.col("fee_revenue"), F.col("booked_revenue")))
    )

    neighborhood_window = Window.partitionBy("neighbourhood_cleansed").orderBy("month_start")
    rolling_window = neighborhood_window.rowsBetween(-2, 0)
    long_window = neighborhood_window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    with_trends = (
        grouped
        .withColumn("revpar_change_pct", ratio(F.col("revpar") - F.lag("revpar").over(neighborhood_window), F.lag("revpar").over(neighborhood_window)))
        .withColumn("booking_rate_change_pct", ratio(F.col("booking_rate") - F.lag("booking_rate").over(neighborhood_window), F.lag("booking_rate").over(neighborhood_window)))
        .withColumn("rolling_booking_rate", F.avg("booking_rate").over(rolling_window))
        .withColumn("long_term_booking_rate", F.avg("booking_rate").over(long_window))
        .withColumn("demand_index", ratio(F.col("rolling_booking_rate"), F.col("long_term_booking_rate")))
    )

    return with_trends


def dynamic_pricing_strategy(calendar_enriched: DataFrame) -> DataFrame:
    """Quantify pricing distributions and booking response by segment."""
    conditioned = calendar_enriched.filter(F.col("price_numeric").isNotNull())

    aggregated = (
        conditioned
        .groupBy("neighbourhood_group_cleansed", "neighbourhood_cleansed", "room_type", "month_start")
        .agg(
            F.count("*").alias("night_count"),
            F.sum("booked_night").alias("booked_nights"),
            F.expr("percentile_approx(price_numeric, 0.25)").alias("p25_price"),
            F.expr("percentile_approx(price_numeric, 0.5)").alias("median_price"),
            F.expr("percentile_approx(price_numeric, 0.75)").alias("p75_price"),
            F.avg("price_numeric").alias("avg_price"),
            F.sum("base_revenue").alias("base_revenue"),
            F.sum("total_revenue").alias("realized_revenue"),
        )
        .withColumn("booking_rate", ratio(F.col("booked_nights"), F.col("night_count")))
        .withColumn("avg_realized_price", ratio(F.col("realized_revenue"), F.col("booked_nights")))
        .withColumn("avg_base_price", ratio(F.col("base_revenue"), F.col("booked_nights")))
    )

    regional_window = Window.partitionBy("neighbourhood_cleansed", "room_type")
    result = (
        aggregated
        .withColumn("median_price_region", F.avg("median_price").over(regional_window))
        .withColumn("price_index", ratio(F.col("median_price"), F.col("median_price_region")))
    )
    return result


def minimum_stay_policy(listings_enriched: DataFrame) -> DataFrame:
    """Assess occupancy response across minimum-stay policy bands."""
    def bucket_expr(column: Column) -> Column:
        return (
            F.when(column <= 1, F.lit("01_one-night"))
            .when((column >= 2) & (column <= 3), F.lit("02_two-to-three"))
            .when((column >= 4) & (column <= 6), F.lit("03_four-to-six"))
            .when((column >= 7) & (column <= 13), F.lit("04_one-to-two-weeks"))
            .when((column >= 14) & (column <= 29), F.lit("05_two-to-four-weeks"))
            .otherwise(F.lit("06_thirty-plus"))
        )

    scored = listings_enriched.filter(F.col("occupancy_rate").isNotNull())
    bucketed = scored.withColumn("minimum_stay_bucket", bucket_expr(F.col("minimum_nights")))

    aggregated = (
        bucketed
        .groupBy("minimum_stay_bucket", "room_type")
        .agg(
            F.count("*").alias("listing_count"),
            F.expr("percentile_approx(occupancy_rate, 0.5)").alias("median_occupancy"),
            F.avg("occupancy_rate").alias("avg_occupancy"),
            F.avg("booked_revenue").alias("avg_booked_revenue"),
            F.avg("price_clean").alias("avg_price"),
        )
        .orderBy("minimum_stay_bucket", "room_type")
    )
    return aggregated


def amenity_roi(listings_enriched: DataFrame) -> DataFrame:
    """Compare occupancy and revenue for high-impact amenities."""
    spark = listings_enriched.sparkSession
    amenity_df = listings_enriched.filter(F.col("amenity_array").isNotNull())

    schema = T.StructType([
        T.StructField("amenity", T.StringType(), False),
        T.StructField("listings_with", T.IntegerType(), False),
        T.StructField("listings_without", T.IntegerType(), False),
        T.StructField("occupancy_with", T.DoubleType(), False),
        T.StructField("occupancy_without", T.DoubleType(), False),
        T.StructField("occupancy_lift", T.DoubleType(), False),
        T.StructField("revenue_with", T.DoubleType(), False),
        T.StructField("revenue_without", T.DoubleType(), False),
        T.StructField("revenue_lift", T.DoubleType(), False),
    ])

    if amenity_df.rdd.isEmpty():
        return spark.createDataFrame([], schema=schema)

    rows: List[Row] = []
    for amenity in AMENITIES_OF_INTEREST:
        stats = amenity_df.agg(
            F.count(F.when(F.array_contains(F.col("amenity_array"), amenity), True)).alias("with_count"),
            F.count(F.when(~F.array_contains(F.col("amenity_array"), amenity), True)).alias("without_count"),
            F.avg(F.when(F.array_contains(F.col("amenity_array"), amenity), F.col("occupancy_rate"))).alias("avg_occ_with"),
            F.avg(F.when(~F.array_contains(F.col("amenity_array"), amenity), F.col("occupancy_rate"))).alias("avg_occ_without"),
            F.avg(F.when(F.array_contains(F.col("amenity_array"), amenity), F.col("booked_revenue"))).alias("avg_rev_with"),
            F.avg(F.when(~F.array_contains(F.col("amenity_array"), amenity), F.col("booked_revenue"))).alias("avg_rev_without"),
        ).collect()[0]

        avg_occ_with = stats["avg_occ_with"] if stats["avg_occ_with"] is not None else 0.0
        avg_occ_without = stats["avg_occ_without"] if stats["avg_occ_without"] is not None else 0.0
        avg_rev_with = stats["avg_rev_with"] if stats["avg_rev_with"] is not None else 0.0
        avg_rev_without = stats["avg_rev_without"] if stats["avg_rev_without"] is not None else 0.0

        rows.append(Row(
            amenity=amenity,
            listings_with=int(stats["with_count"] or 0),
            listings_without=int(stats["without_count"] or 0),
            occupancy_with=float(avg_occ_with),
            occupancy_without=float(avg_occ_without),
            occupancy_lift=float(avg_occ_with - avg_occ_without),
            revenue_with=float(avg_rev_with),
            revenue_without=float(avg_rev_without),
            revenue_lift=float(avg_rev_with - avg_rev_without),
        ))

    return spark.createDataFrame(rows, schema=schema)


def seasonality_event_planning(calendar_enriched: DataFrame) -> DataFrame:
    """Surface seasonal peaks at the market and neighbourhood-group levels."""
    overall = (
        calendar_enriched
        .groupBy("month_start")
        .agg(
            F.sum("booked_night").alias("booked_nights"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("base_revenue").alias("base_revenue"),
            F.sum("night_count").alias("total_nights"),
            F.countDistinct("listing_id").alias("active_listings"),
        )
        .withColumn("scope_level", F.lit("market"))
        .withColumn("scope_name", F.lit("Santa Clara County"))
    )

    by_group = (
        calendar_enriched
        .filter(F.col("neighbourhood_group_cleansed").isNotNull())
        .groupBy("neighbourhood_group_cleansed", "month_start")
        .agg(
            F.sum("booked_night").alias("booked_nights"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("base_revenue").alias("base_revenue"),
            F.sum("night_count").alias("total_nights"),
            F.countDistinct("listing_id").alias("active_listings"),
        )
        .withColumn("scope_level", F.lit("neighbourhood_group"))
        .withColumnRenamed("neighbourhood_group_cleansed", "scope_name")
    )

    unioned = overall.unionByName(by_group, allowMissingColumns=True)
    unioned = unioned.withColumn("revpar", ratio(F.col("base_revenue"), F.col("total_nights")))

    window = (
        Window.partitionBy("scope_level", "scope_name")
        .orderBy("month_start")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    stats = (
        unioned
        .withColumn("avg_booked_nights", F.avg("booked_nights").over(window))
        .withColumn("seasonal_index", ratio(F.col("booked_nights"), F.col("avg_booked_nights")))
        .withColumn("is_peak", F.col("seasonal_index") >= F.lit(1.15))
        .withColumn("is_low", F.col("seasonal_index") <= F.lit(0.85))
    )
    return stats


def host_performance_coaching(listings_enriched: DataFrame, reviews_clean: DataFrame) -> DataFrame:
    """Identify hosts needing quality or operational support."""
    snapshot_rows = listings_enriched.select(F.max("last_scraped_date").alias("snapshot")).collect()
    snapshot = snapshot_rows[0]["snapshot"] if snapshot_rows else None
    if snapshot is None:
        snapshot = dt.date.today()
    one_year_ago = snapshot - dt.timedelta(days=365)
    six_months_ago = snapshot - dt.timedelta(days=180)

    host_base = (
        listings_enriched
        .select(
            "host_id",
            "id",
            "neighbourhood_cleansed",
            "room_type",
            "occupancy_rate",
            "booked_revenue",
            "review_scores_rating",
            "host_response_rate_pct",
            "host_acceptance_rate_pct",
            "host_is_superhost",
        )
        .groupBy("host_id")
        .agg(
            F.countDistinct("id").alias("active_listings"),
            F.avg("occupancy_rate").alias("avg_occupancy"),
            F.avg("booked_revenue").alias("avg_annual_revenue"),
            F.avg("review_scores_rating").alias("avg_review_score"),
            F.avg("host_response_rate_pct").alias("avg_response_rate"),
            F.avg("host_acceptance_rate_pct").alias("avg_acceptance_rate"),
            F.max("host_is_superhost").alias("has_superhost_flag"),
        )
    )

    reviews_recent = reviews_clean.filter((F.col("review_date") >= F.lit(one_year_ago)) & (F.col("review_date") <= F.lit(snapshot)))

    host_reviews = (
        reviews_recent
        .join(listings_enriched.select(F.col("id").alias("listing_id"), "host_id"), "listing_id", "inner")
        .groupBy("host_id")
        .agg(
            F.countDistinct("review_id").alias("reviews_last_12m"),
            F.sum(F.when(F.col("review_date") >= F.lit(six_months_ago), 1).otherwise(0)).alias("reviews_last_6m"),
        )
    )

    combined = host_base.join(host_reviews, "host_id", "left")

    scored = (
        combined
        .withColumn("needs_quality_support", (F.col("avg_review_score") < F.lit(4.3)) | (F.col("avg_response_rate") < F.lit(0.85)))
        .withColumn("needs_demand_support", F.col("avg_occupancy") < F.lit(0.45))
        .withColumn("priority_score", F.col("needs_quality_support").cast("int") + F.col("needs_demand_support").cast("int"))
    )

    return scored


def portfolio_expansion_mix(listings_enriched: DataFrame) -> DataFrame:
    """Summarise performance by property and room type to guide expansion."""
    filtered = listings_enriched.filter(F.col("occupancy_rate").isNotNull())
    result = (
        filtered
        .groupBy("property_type", "room_type")
        .agg(
            F.count("*").alias("listing_count"),
            F.avg("occupancy_rate").alias("avg_occupancy"),
            F.expr("percentile_approx(occupancy_rate, 0.5)").alias("median_occupancy"),
            F.avg("revpar").alias("avg_revpar"),
            F.avg("booked_revenue").alias("avg_booked_revenue"),
            F.avg("price_clean").alias("avg_price"),
            F.avg("accommodates").alias("avg_accommodates"),
        )
        .orderBy(F.desc("avg_revpar"))
    )
    return result


def supply_saturation_risk(monthly_calendar: DataFrame) -> DataFrame:
    """Highlight neighbourhoods where supply is outpacing demand."""
    valid = monthly_calendar.filter(F.col("neighbourhood_cleansed").isNotNull())
    window = Window.partitionBy("neighbourhood_cleansed").orderBy("month_start")

    result = (
        valid
        .withColumn("active_listing_growth", F.col("active_listings") - F.lag("active_listings").over(window))
        .withColumn("occupancy_trend", F.col("booking_rate") - F.lag("booking_rate").over(window))
        .withColumn("revenue_trend", F.col("revpar") - F.lag("revpar").over(window))
        .withColumn("growth_flag", (F.col("active_listing_growth") > 0) & (F.col("occupancy_trend") < 0))
        .withColumn("risk_score", F.when(F.col("growth_flag"), F.abs(F.col("occupancy_trend")) + F.abs(F.col("revenue_trend"))).otherwise(F.lit(0.0)))
    )
    return result


def compliance_exposure(listings_enriched: DataFrame) -> DataFrame:
    """Flag listings with elevated regulatory or licensing risk."""
    license_missing = (F.col("license").isNull()) | (F.trim(F.col("license")) == "")
    short_term_flag = (F.col("room_type") == "Entire home/apt") & (F.col("minimum_nights") < 31) & (F.col("availability_365") > 90)

    flagged = (
        listings_enriched
        .select(
            F.col("id").alias("listing_id"),
            "host_id",
            "neighbourhood_cleansed",
            "room_type",
            "minimum_nights",
            "availability_365",
            "license",
            "booked_revenue",
            "occupancy_rate",
            "avg_fee_share",
        )
        .withColumn("license_missing", license_missing)
        .withColumn("short_term_risk", short_term_flag)
        .withColumn("high_fee_share", F.col("avg_fee_share") > F.lit(0.15))
        .withColumn(
            "risk_score",
            F.col("license_missing").cast("int") + F.col("short_term_risk").cast("int") + F.col("high_fee_share").cast("int")
        )
        .filter((F.col("license_missing") | F.col("short_term_risk") | F.col("high_fee_share")))
    )
    return flagged


def fee_optimization(calendar_enriched: DataFrame) -> DataFrame:
    """Analyse the contribution of fees versus base rates for booked nights."""
    booked = calendar_enriched.filter(F.col("booked_night") == 1)

    aggregated = (
        booked
        .groupBy("neighbourhood_cleansed", "room_type", "month_start")
        .agg(
            F.count("*").alias("booked_nights"),
            F.sum("base_revenue").alias("base_revenue"),
            F.sum("total_revenue").alias("total_revenue"),
            F.avg("price_numeric").alias("avg_price"),
            F.avg("adjusted_price_numeric").alias("avg_adjusted_price"),
            F.sum("fee_revenue").alias("fee_revenue"),
        )
        .withColumn("fee_share", ratio(F.col("fee_revenue"), F.col("base_revenue")))
        .withColumn("effective_rate", ratio(F.col("total_revenue"), F.col("booked_nights")))
        .withColumn("base_rate", ratio(F.col("base_revenue"), F.col("booked_nights")))
    )
    return aggregated


def build_metric_frames(
    listings_enriched: DataFrame,
    calendar_enriched: DataFrame,
    monthly_calendar: DataFrame,
    reviews_clean: DataFrame,
) -> Dict[str, DataFrame]:
    """Compute and return all metric DataFrames keyed by output folder name."""
    return {
        "neighbourhood_investment": neighborhood_investment_focus(calendar_enriched),
        "dynamic_pricing": dynamic_pricing_strategy(calendar_enriched),
        "minimum_stay": minimum_stay_policy(listings_enriched),
        "amenity_roi": amenity_roi(listings_enriched),
        "seasonality": seasonality_event_planning(calendar_enriched),
        "host_performance": host_performance_coaching(listings_enriched, reviews_clean),
        "portfolio_mix": portfolio_expansion_mix(listings_enriched),
        "supply_saturation": supply_saturation_risk(monthly_calendar),
        "compliance_exposure": compliance_exposure(listings_enriched),
        "fee_optimization": fee_optimization(calendar_enriched),
    }
