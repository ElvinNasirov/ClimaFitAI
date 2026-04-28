import pandas as pd
from datetime import datetime


def ensure_datetime(df, date_col="time"):
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    return df


# -------------------------
# Row Count
# -------------------------
def check_row_count(df):
    return {
        "check": "row_count",
        "status": "PASS" if len(df) > 0 else "FAIL",
        "details": f"{len(df)} rows"
    }


# -------------------------
# Missing Values
# -------------------------
def check_missing_values(df):
    nulls = df.isna().sum()
    ratio = df.isna().mean()

    issues = ratio[ratio > 0.05]

    return {
        "check": "missing_values",
        "status": "WARN" if len(issues) > 0 else "PASS",
        "details": nulls.to_dict()
    }


# -------------------------
# Duplicate Rows
# -------------------------
def check_duplicate_rows(df):
    count = df.duplicated().sum()

    return {
        "check": "duplicate_rows",
        "status": "WARN" if count > 0 else "PASS",
        "details": f"{count} duplicates"
    }


# -------------------------
# Duplicate City-Date
# -------------------------
def check_duplicate_city_dates(df):
    df = ensure_datetime(df)
    count = df.duplicated(subset=["city", "time"]).sum()

    return {
        "check": "duplicate_city_dates",
        "status": "WARN" if count > 0 else "PASS",
        "details": f"{count} duplicates"
    }


# -------------------------
# Date Coverage
# -------------------------
def check_date_coverage(df):
    df = ensure_datetime(df)

    coverage = df.groupby("city")["time"].agg(["min", "max", "count"])

    return {
        "check": "date_coverage",
        "status": "PASS",
        "details": coverage.to_dict()
    }


# -------------------------
# Missing Dates
# -------------------------
def check_missing_dates(df):
    df = ensure_datetime(df)
    results = {}

    for city, group in df.groupby("city"):
        full_range = pd.date_range(group["time"].min(), group["time"].max(), freq="D")
        missing_dates = full_range.difference(group["time"])
        results[city] = len(missing_dates)

    return {
        "check": "missing_dates",
        "status": "WARN" if any(v > 0 for v in results.values()) else "PASS",
        "details": results
    }


# -------------------------
# Column Consistency
# -------------------------
def check_column_consistency(df1, df2):
    result = {
        "same_columns": set(df1.columns) == set(df2.columns),
        "only_in_first": sorted(list(set(df1.columns) - set(df2.columns))),
        "only_in_second": sorted(list(set(df2.columns) - set(df1.columns))),
    }

    return {
        "check": "column_consistency",
        "status": "PASS" if result["same_columns"] else "WARN",
        "details": result
    }


# -------------------------
# Weather Ranges
# -------------------------
def check_weather_ranges(df):
    ranges = {
        "temperature_2m_max": (-50, 60),
        "precipitation_sum": (0, 500),
        "windspeed_10m_max": (0, 80),  # FIXED name
        "relative_humidity_2m_mean": (0, 100),
        "apparent_temperature_max": (-50, 70),
    }

    results = {}

    for col, (low, high) in ranges.items():
        if col in df.columns:
            invalid_rows = df[(df[col] < low) | (df[col] > high)]
            results[col] = len(invalid_rows)

    return {
        "check": "weather_ranges",
        "status": "WARN" if any(v > 0 for v in results.values()) else "PASS",
        "details": results
    }


# -------------------------
# Freshness
# -------------------------
def check_freshness(df):
    df = ensure_datetime(df)

    latest = df["time"].max()
    diff = (datetime.today() - latest).days

    return {
        "check": "freshness",
        "status": "WARN" if diff > 2 else "PASS",
        "details": f"{diff} days old"
    }