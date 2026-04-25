import pandas as pd


def convert_datetime(df, date_col="time"):
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    return df


def fix_numeric_types(df):
    df = df.copy()

    numeric_cols = df.select_dtypes(include=["float64", "int64"]).columns

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def remove_duplicates(df):
    df = df.copy()
    df = df.drop_duplicates()
    return df


def remove_duplicate_city_dates(df):
    df = df.copy()
    df = df.drop_duplicates(subset=["city", "time"])
    return df

def handle_missing_values(df):
    df = df.copy()
    df = df.sort_values(["city", "time"])

    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df.groupby("city")[numeric_cols].ffill()

    return df


def ensure_continuous_dates(df):
    df = df.copy()
    df = convert_datetime(df)

    all_data = []

    for city, group in df.groupby("city"):
        full_range = pd.date_range(group["time"].min(), group["time"].max(), freq="D")

        group = group.set_index("time").reindex(full_range)

        group["city"] = city
        group = group.reset_index().rename(columns={"index": "time"})

        all_data.append(group)

    result = pd.concat(all_data).sort_values(["city", "time"]).reset_index(drop=True)

    return result


def clean_data(df):
    df = convert_datetime(df)
    df = fix_numeric_types(df)
    df = remove_duplicates(df)
    df = remove_duplicate_city_dates(df)
    df = ensure_continuous_dates(df)
    df = handle_missing_values(df)

    return df