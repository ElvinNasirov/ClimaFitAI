"""
Data quality checks for the Weather Intelligence Pipeline.

Each check returns a dictionary with:
- check: check name
- status: PASS, WARN, or FAIL
- details: diagnostic information

These checks are used in notebooks and inside the automated pipeline
as quality gates before feature engineering and model training.
"""

from datetime import date
from typing import Any

import pandas as pd


WEATHER_RANGES = {
    "temperature_2m_max": (-50, 60),
    "precipitation_sum": (0, 500),
    "wind_speed_10m_max": (0, 80),
    "relative_humidity_2m_mean": (0, 100),
    "cloud_cover_mean": (0, 100),
    "apparent_temperature_max": (-50, 70),
    "sunshine_duration": (0, 86400),
}


def make_check_result(
    check: str,
    status: str,
    details: Any,
) -> dict[str, Any]:
    """
    Create a standard quality check result.
    """
    return {
        "check": check,
        "status": status,
        "details": details,
    }


def ensure_datetime(
    df: pd.DataFrame,
    date_col: str = "time",
) -> pd.DataFrame:
    """
    Return a copy of the DataFrame with a parsed datetime column.
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    return df


def check_row_count(df: pd.DataFrame) -> dict[str, Any]:
    """
    Check that the DataFrame is not empty.
    """
    row_count = len(df)

    return make_check_result(
        check="row_count",
        status="PASS" if row_count > 0 else "FAIL",
        details=f"{row_count} rows",
    )


def check_missing_values(df: pd.DataFrame) -> dict[str, Any]:
    """
    Check whether any missing values are present.
    """
    missing_values = df.isna().sum()
    has_missing_values = missing_values.sum() > 0

    return make_check_result(
        check="missing_values",
        status="WARN" if has_missing_values else "PASS",
        details=missing_values.to_dict(),
    )


def check_duplicate_rows(df: pd.DataFrame) -> dict[str, Any]:
    """
    Check for fully duplicated rows.
    """
    duplicate_count = int(df.duplicated().sum())

    return make_check_result(
        check="duplicate_rows",
        status="WARN" if duplicate_count > 0 else "PASS",
        details=f"{duplicate_count} duplicate rows",
    )


def check_duplicate_city_dates(df: pd.DataFrame) -> dict[str, Any]:
    """
    Check for duplicate records with the same city and date.
    """
    df = ensure_datetime(df)
    duplicate_count = int(df.duplicated(subset=["city", "time"]).sum())

    return make_check_result(
        check="duplicate_city_dates",
        status="WARN" if duplicate_count > 0 else "PASS",
        details=f"{duplicate_count} duplicate city-date records",
    )


def check_date_coverage(df: pd.DataFrame) -> dict[str, Any]:
    """
    Summarize min date, max date, and row count by city.
    """
    df = ensure_datetime(df)

    coverage = (
        df.groupby("city")["time"]
        .agg(["min", "max", "count"])
        .reset_index()
    )

    return make_check_result(
        check="date_coverage",
        status="PASS",
        details=coverage.to_dict(orient="records"),
    )


def check_missing_dates(df: pd.DataFrame) -> dict[str, Any]:
    """
    Check whether each city has a continuous daily date range.
    """
    df = ensure_datetime(df)
    results = {}

    for city, group in df.groupby("city"):
        valid_dates = group["time"].dropna()

        if valid_dates.empty:
            results[city] = "no valid dates"
            continue

        full_range = pd.date_range(
            valid_dates.min(),
            valid_dates.max(),
            freq="D",
        )

        missing_dates = full_range.difference(valid_dates)
        results[city] = len(missing_dates)

    has_missing_dates = any(
        value != 0 for value in results.values()
    )

    return make_check_result(
        check="missing_dates",
        status="WARN" if has_missing_dates else "PASS",
        details=results,
    )


def check_column_consistency(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> dict[str, Any]:
    """
    Check whether two DataFrames contain the same columns.
    """
    result = {
        "same_columns": set(df1.columns) == set(df2.columns),
        "only_in_first": sorted(set(df1.columns) - set(df2.columns)),
        "only_in_second": sorted(set(df2.columns) - set(df1.columns)),
    }

    return make_check_result(
        check="column_consistency",
        status="PASS" if result["same_columns"] else "WARN",
        details=result,
    )


def check_weather_ranges(df: pd.DataFrame) -> dict[str, Any]:
    """
    Check whether weather values fall within realistic physical ranges.
    """
    violations = {}

    for column, (low, high) in WEATHER_RANGES.items():
        if column not in df.columns:
            continue

        invalid_rows = df[
            (df[column] < low) | (df[column] > high)
        ]

        violations[column] = len(invalid_rows)

    has_violations = any(count > 0 for count in violations.values())

    return make_check_result(
        check="weather_ranges",
        status="WARN" if has_violations else "PASS",
        details=violations,
    )


def check_freshness(
    df: pd.DataFrame,
    max_age_days: int = 2,
) -> dict[str, Any]:
    """
    Check whether the latest available date is recent enough.
    """
    df = ensure_datetime(df)

    latest_timestamp = df["time"].max()

    if pd.isna(latest_timestamp):
        return make_check_result(
            check="freshness",
            status="FAIL",
            details="no valid dates available",
        )

    latest_date = latest_timestamp.date()
    age_days = (date.today() - latest_date).days

    return make_check_result(
        check="freshness",
        status="WARN" if age_days > max_age_days else "PASS",
        details=f"{age_days} days old",
    )