"""Utility functions for Airbnb EMR analytics."""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import Column
from pyspark.sql import functions as F


_NULL_STRINGS = {"", "na", "n/a", "null", "none", "nan"}


def safe_cast(column: Column, data_type: str) -> Column:
    """Cast a column, returning null for empty or invalid strings."""
    normalized = F.trim(column.cast("string"))
    return F.when(normalized.isNull() | normalized.eqNullSafe("") | normalized.rlike("(?i)^(na|n/a|null|none|nan)$"), None).otherwise(
        column.cast(data_type)
    )


def parse_currency(column: Column) -> Column:
    """Convert a currency-formatted string column into a double."""
    cleaned = F.regexp_replace(column, r"[^0-9\.-]", "")
    return safe_cast(cleaned, "double")


def parse_percentage(column: Column) -> Column:
    """Convert percentage strings like '85%' into a fractional double."""
    cleaned = F.regexp_replace(column, r"[^0-9\.-]", "")
    return safe_cast(cleaned, "double") / F.lit(100.0)


def ratio(numerator: Column, denominator: Column) -> Column:
    """Safely divide numerator by denominator."""
    return F.when((denominator.isNull()) | (denominator == 0), None).otherwise(numerator / denominator)


def coalesce_any(columns: Iterable[Column], fallback: Column | None = None) -> Column:
    """Coalesce the first non-null column from an iterable."""
    cols = list(columns)
    if not cols:
        if fallback is None:
            raise ValueError("At least one column must be provided or a fallback specified")
        cols = [fallback]
    return F.coalesce(*cols)
