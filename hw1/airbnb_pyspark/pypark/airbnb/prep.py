"""Data preparation helpers for Airbnb EMR analytics."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .utils import parse_currency, parse_percentage, ratio, safe_cast


def prepare_listings(listings: DataFrame, neighbourhoods: Optional[DataFrame] = None) -> DataFrame:
    """Clean and enrich the raw listings DataFrame."""
    df = (
        listings
        .withColumn("id", safe_cast(F.col("id"), "long"))
        .withColumn("host_id", safe_cast(F.col("host_id"), "long"))
        .withColumn("accommodates", safe_cast(F.col("accommodates"), "int"))
        .withColumn("bathrooms", safe_cast(F.col("bathrooms"), "double"))
        .withColumn("bedrooms", safe_cast(F.col("bedrooms"), "double"))
        .withColumn("beds", safe_cast(F.col("beds"), "double"))
        .withColumn("price_clean", parse_currency(F.col("price")))
        .withColumn("minimum_nights", safe_cast(F.col("minimum_nights"), "int"))
        .withColumn("maximum_nights", safe_cast(F.col("maximum_nights"), "int"))
        .withColumn("availability_365", safe_cast(F.col("availability_365"), "int"))
        .withColumn("estimated_occupancy_l365d", safe_cast(F.col("estimated_occupancy_l365d"), "double"))
        .withColumn("estimated_revenue_l365d", parse_currency(F.col("estimated_revenue_l365d")))
        .withColumn("reviews_per_month", safe_cast(F.col("reviews_per_month"), "double"))
        .withColumn("host_is_superhost", F.col("host_is_superhost") == F.lit("t"))
        .withColumn("instant_bookable", F.col("instant_bookable") == F.lit("t"))
        .withColumn("host_response_rate_pct", parse_percentage(F.col("host_response_rate")))
        .withColumn("host_acceptance_rate_pct", parse_percentage(F.col("host_acceptance_rate")))
        .withColumn("last_scraped_date", F.to_date(F.col("last_scraped")))
        .withColumn("calendar_last_scraped_date", F.to_date(F.col("calendar_last_scraped")))
        .withColumn("first_review_date", F.to_date(F.col("first_review")))
        .withColumn("last_review_date", F.to_date(F.col("last_review")))
        .withColumn(
            "amenity_array",
            F.from_json(F.col("amenities"), T.ArrayType(T.StringType()))
        )
    )

    if neighbourhoods is not None:
        neighbourhoods_clean = neighbourhoods.select(
            F.col("neighbourhood").alias("nbhd_join_key"),
            F.col("neighbourhood_group").alias("neighbourhood_group_lookup"),
        )
        df = df.join(
            neighbourhoods_clean,
            F.lower(F.trim(F.col("neighbourhood"))) == F.lower(F.trim(F.col("nbhd_join_key"))),
            "left",
        ).drop("nbhd_join_key")
        df = df.withColumn(
            "neighbourhood_group_cleansed",
            F.coalesce(F.col("neighbourhood_group_cleansed"), F.col("neighbourhood_group_lookup"))
        ).drop("neighbourhood_group_lookup")

    return df


def prepare_calendar(calendar: DataFrame) -> DataFrame:
    """Clean the raw calendar feed."""
    return (
        calendar
        .withColumn("listing_id", safe_cast(F.col("listing_id"), "long"))
        .withColumn("date", F.to_date(F.col("date")))
        .withColumn("price_numeric", parse_currency(F.col("price")))
        .withColumn("adjusted_price_numeric", parse_currency(F.col("adjusted_price")))
        .withColumn("minimum_nights", safe_cast(F.col("minimum_nights"), "int"))
        .withColumn("maximum_nights", safe_cast(F.col("maximum_nights"), "int"))
        .withColumn("night_count", F.lit(1))
        .withColumn("booked_night", F.when(F.col("available") == "f", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("available_night", F.when(F.col("available") == "t", F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "base_revenue",
            F.when(F.col("available") == "f", F.col("price_numeric")).otherwise(F.lit(0.0))
        )
        .withColumn(
            "total_revenue",
            F.when(F.col("available") == "f", F.col("adjusted_price_numeric")).otherwise(F.lit(0.0))
        )
        .withColumn("fee_revenue", F.col("total_revenue") - F.col("base_revenue"))
        .withColumn("month_start", F.to_date(F.date_trunc("month", F.col("date"))))
    )


def prepare_reviews(reviews: DataFrame) -> DataFrame:
    """Standardise the reviews extract."""
    return (
        reviews
        .withColumn("listing_id", safe_cast(F.col("listing_id"), "long"))
        .withColumn("review_id", safe_cast(F.col("id"), "long"))
        .withColumn("review_date", F.to_date(F.col("date")))
    )


def build_feature_views(
    listings: DataFrame,
    calendar: DataFrame,
    reviews: DataFrame,
    neighbourhoods: Optional[DataFrame] = None,
) -> Dict[str, DataFrame]:
    """Produce enriched DataFrames reused across downstream metrics."""
    listings_clean = prepare_listings(listings, neighbourhoods).cache()
    calendar_clean = prepare_calendar(calendar).cache()
    reviews_clean = prepare_reviews(reviews).cache()

    listings_dim = listings_clean.select(
        F.col("id").alias("listing_id"),
        "host_id",
        "neighbourhood",
        "neighbourhood_cleansed",
        "neighbourhood_group_cleansed",
        "property_type",
        "room_type",
        "accommodates",
        "minimum_nights",
        "license",
        "price_clean",
        "host_is_superhost",
        "estimated_occupancy_l365d",
        "estimated_revenue_l365d",
        "availability_365",
        "amenity_array",
        "host_response_rate_pct",
        "host_acceptance_rate_pct",
        "reviews_per_month",
        "last_scraped_date",
    )

    calendar_enriched = (
        calendar_clean
        .join(listings_dim, "listing_id", "left")
        .withColumn(
            "occupancy_flag",
            F.when(F.col("booked_night") == 1, F.lit(1.0)).otherwise(F.lit(0.0))
        )
    ).cache()

    listing_performance = (
        calendar_enriched
        .groupBy("listing_id")
        .agg(
            F.sum("night_count").alias("total_nights"),
            F.sum("booked_night").alias("booked_nights"),
            F.sum("available_night").alias("available_nights"),
            F.sum("base_revenue").alias("booked_revenue"),
            F.sum("total_revenue").alias("booked_revenue_adjusted"),
            F.sum("fee_revenue").alias("fee_revenue"),
            F.avg("price_numeric").alias("avg_listed_price"),
            F.avg("adjusted_price_numeric").alias("avg_adjusted_price"),
        )
        .withColumn("occupancy_rate", ratio(F.col("booked_nights"), F.col("total_nights")))
        .withColumn("revpar", ratio(F.col("booked_revenue"), F.col("total_nights")))
        .withColumn("avg_fee_share", ratio(F.col("fee_revenue"), F.col("booked_revenue")))
    )

    listings_enriched = (
        listings_clean
        .join(listing_performance, listings_clean["id"] == listing_performance["listing_id"], "left")
        .drop(listing_performance["listing_id"])
    ).cache()

    monthly_calendar = (
        calendar_enriched
        .groupBy(
            "neighbourhood_cleansed",
            "neighbourhood_group_cleansed",
            "room_type",
            "property_type",
            "month_start",
        )
        .agg(
            F.countDistinct("listing_id").alias("active_listings"),
            F.sum("night_count").alias("total_nights"),
            F.sum("booked_night").alias("booked_nights"),
            F.sum("available_night").alias("available_nights"),
            F.sum("base_revenue").alias("booked_revenue"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("fee_revenue").alias("fee_revenue"),
            F.avg("price_numeric").alias("avg_listed_price"),
            F.avg("adjusted_price_numeric").alias("avg_adjusted_price"),
            F.avg("accommodates").alias("avg_accommodates"),
        )
        .withColumn("revpar", ratio(F.col("booked_revenue"), F.col("total_nights")))
        .withColumn("booking_rate", ratio(F.col("booked_nights"), F.col("total_nights")))
    ).cache()

    return {
        "listings_enriched": listings_enriched,
        "calendar_enriched": calendar_enriched,
        "monthly_calendar": monthly_calendar,
        "reviews_clean": reviews_clean,
    }
