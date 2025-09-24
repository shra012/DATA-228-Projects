"""Data ingestion helpers for Airbnb EMR analytics."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_listings(spark: SparkSession, path: str) -> DataFrame:
    """Load the Inside Airbnb listings extract."""
    return (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", "\"")
        .option("quote", "\"")
        .csv(path)
    )


def read_calendar(spark: SparkSession, path: str) -> DataFrame:
    """Load the daily availability calendar extract."""
    return (
        spark.read
        .option("header", True)
        .csv(path)
    )


def read_reviews(spark: SparkSession, path: str) -> DataFrame:
    """Load the reviews extract."""
    return (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", "\"")
        .option("quote", "\"")
        .csv(path)
    )


def read_neighbourhoods(spark: SparkSession, path: str) -> DataFrame:
    """Load the neighbourhood lookup table."""
    return spark.read.option("header", True).csv(path)
