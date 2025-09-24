#!/usr/bin/env python3
"""Airbnb analytics PySpark job orchestrated on EMR."""

from __future__ import annotations

import argparse
import sys
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from airbnb import io, metrics, prep


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Airbnb insights analytics on EMR")
    parser.add_argument("--listings", required=True, help="Path to listings.csv or listings.csv.gz")
    parser.add_argument("--calendar", required=True, help="Path to calendar.csv or calendar.csv.gz")
    parser.add_argument("--reviews", required=True, help="Path to reviews.csv or reviews.csv.gz")
    parser.add_argument("--neighbourhoods", required=False, help="Optional neighbourhoods.csv lookup path")
    parser.add_argument("--output", required=True, help="Base S3 or local path for metric outputs")
    parser.add_argument(
        "--output-format",
        default="parquet",
        choices=["parquet", "csv"],
        help="Output file format for metric datasets",
    )
    parser.add_argument(
        "--coalesce",
        type=int,
        default=1,
        help="Number of output partitions per metric (use 0 to keep Spark defaults)",
    )
    return parser.parse_args()


def create_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def log_info(message: str) -> None:
    print(f"[INFO] {message}")


def write_output(df: DataFrame, path: str, fmt: str, coalesce: int) -> None:
    output_df = df.coalesce(coalesce) if coalesce and coalesce > 0 else df
    writer = output_df.write.mode("overwrite")
    if fmt == "csv":
        writer = writer.option("header", True)
    writer.format(fmt).save(path)


def main() -> int:
    args = parse_args()
    spark = create_spark_session("Airbnb-Insights-EMR")

    try:
        log_info("Loading datasets")
        listings_df = io.read_listings(spark, args.listings)
        calendar_df = io.read_calendar(spark, args.calendar)
        reviews_df = io.read_reviews(spark, args.reviews)
        neighbourhoods_df = io.read_neighbourhoods(spark, args.neighbourhoods) if args.neighbourhoods else None

        log_info("Preparing feature views")
        feature_views = prep.build_feature_views(
            listings=listings_df,
            calendar=calendar_df,
            reviews=reviews_df,
            neighbourhoods=neighbourhoods_df,
        )

        log_info("Computing metric datasets")
        metric_frames: Dict[str, DataFrame] = metrics.build_metric_frames(
            listings_enriched=feature_views["listings_enriched"],
            calendar_enriched=feature_views["calendar_enriched"],
            monthly_calendar=feature_views["monthly_calendar"],
            reviews_clean=feature_views["reviews_clean"],
        )

        for name, df in metric_frames.items():
            target_path = f"{args.output.rstrip('/')}/{name}"
            log_info(f"Writing {name} -> {target_path}")
            write_output(df, target_path, args.output_format, args.coalesce)

        log_info("Airbnb insights job completed successfully")
        return 0
    except Exception as exc:
        log_info(f"Job failed with error: {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
