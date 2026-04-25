import pandas as pd


def ensure_datetime(df, date_col="time"):
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    return df


def check_missing_values(df):
    return df.isna().sum()


def check_duplicate_rows(df):
    return df.duplicated().sum()


def check_duplicate_city_dates(df):
    df = ensure_datetime(df)
    return df.duplicated(subset=["city", "time"]).sum()


def check_date_coverage(df):
    df = ensure_datetime(df)
    return df.groupby("city")["time"].agg(["min", "max", "count"])


def check_missing_dates(df):
    df = ensure_datetime(df)
    results = {}

    for city, group in df.groupby("city"):
        full_range = pd.date_range(group["time"].min(), group["time"].max(), freq="D")
        missing_dates = full_range.difference(group["time"])
        results[city] = len(missing_dates)

    return results


def check_column_consistency(df1, df2):
    return {
        "same_columns": set(df1.columns) == set(df2.columns),
        "only_in_first": sorted(list(set(df1.columns) - set(df2.columns))),
        "only_in_second": sorted(list(set(df2.columns) - set(df1.columns))),
    }


def check_weather_ranges(df):
    ranges = {
        "temperature_2m_max": (-40, 60),
        "precipitation_sum": (0, 500),
        "wind_speed_10m_max": (0, 80),
        "relative_humidity_2m_mean": (0, 100),
        "cloud_cover_mean": (0, 100),
        "apparent_temperature_max": (-50, 70),
        "sunshine_duration": (0, 86400),
    }

    results = {}

    for col, (low, high) in ranges.items():
        if col in df.columns:
            invalid_rows = df[(df[col] < low) | (df[col] > high)]
            results[col] = len(invalid_rows)

    return results