import pandas as pd
from sklearn.preprocessing import LabelEncoder


def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add calendar-based features.
    """
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    df["month"] = df["time"].dt.month
    df["day_of_month"] = df["time"].dt.day
    return df


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add lag features by city.
    """
    df = df.copy()
    df = df.sort_values(["city", "time"])

    df["temperature_lag_1"] = df.groupby("city")["temperature_2m_max"].shift(1)
    df["precipitation_lag_1"] = df.groupby("city")["precipitation_sum"].shift(1)
    df["wind_lag_1"] = df.groupby("city")["wind_speed_10m_max"].shift(1)
    df["humidity_lag_1"] = df.groupby("city")["relative_humidity_2m_mean"].shift(1)

    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add rolling window features by city.
    """
    df = df.copy()
    df = df.sort_values(["city", "time"])

    df["temperature_3d_avg"] = (
        df.groupby("city")["temperature_2m_max"]
        .transform(lambda x: x.rolling(window=3, min_periods=1).mean())
    )

    df["precipitation_7d_sum"] = (
        df.groupby("city")["precipitation_sum"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).sum())
    )

    df["wind_3d_avg"] = (
        df.groupby("city")["wind_speed_10m_max"]
        .transform(lambda x: x.rolling(window=3, min_periods=1).mean())
    )

    df["humidity_7d_avg"] = (
        df.groupby("city")["relative_humidity_2m_mean"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    )

    return df


def add_city_encoding(df: pd.DataFrame):
    """
    Encode city column and return dataframe + encoder.
    """
    df = df.copy()
    le = LabelEncoder()
    df["city_encoded"] = le.fit_transform(df["city"])
    return df, le


def build_features(df: pd.DataFrame):
    """
    Full feature engineering pipeline.
    """
    df = df.copy()
    df = add_calendar_features(df)
    df = add_lag_features(df)
    df = add_rolling_features(df)
    df, le = add_city_encoding(df)

    # Drop rows with missing values created by lag features
    df = df.dropna().reset_index(drop=True)

    return df, le


def get_feature_columns():
    return [
        "apparent_temperature_max",
        "sunshine_duration",
        "city_encoded",
        "month",
        "day_of_month",
        "temperature_lag_1",
        "precipitation_lag_1",
        "wind_lag_1",
        "humidity_lag_1",
        "temperature_3d_avg",
        "precipitation_7d_sum",
        "wind_3d_avg",
        "humidity_7d_avg",
    ]


def get_target_columns():
    """
    Return target columns for multi-output regression.
    """
    return [
        "temperature_2m_max",
        "precipitation_sum",
        "wind_speed_10m_max",
        "relative_humidity_2m_mean",
        "cloud_cover_mean",
    ]