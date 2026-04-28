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
    # Improvement: cyclical date features help model learn seasonality better than raw month/day.
    df["day_of_year"] = df["time"].dt.dayofyear
    df["day_sin"] = np.sin(2 * np.pi * df["day_of_year"] / 365.25)
    df["day_cos"] = np.cos(2 * np.pi * df["day_of_year"] / 365.25)
    return df

def add_comfort_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add human comfort-related features.
    """
    df = df.copy()

    # Improvement: comfort gap shows how different the weather feels from actual temperature.
    df["comfort_gap"] = df["apparent_temperature_max"] - df["temperature_2m_max"]

    # Improvement: sunshine ratio makes sunshine duration easier for model to compare.
    df["sunshine_ratio"] = df["sunshine_duration"] / (24 * 3600)

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

     # Improvement: 3-day and 7-day lags capture slower weather patterns.
    df["temperature_lag_3"] = df.groupby("city")["temperature_2m_max"].shift(3)
    df["temperature_lag_7"] = df.groupby("city")["temperature_2m_max"].shift(7)

    df["precipitation_lag_3"] = df.groupby("city")["precipitation_sum"].shift(3)
    df["precipitation_lag_7"] = df.groupby("city")["precipitation_sum"].shift(7)

    df["wind_lag_3"] = df.groupby("city")["wind_speed_10m_max"].shift(3)
    df["humidity_lag_3"] = df.groupby("city")["relative_humidity_2m_mean"].shift(3)

    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add rolling window features by city.
    """
    df = df.copy()
    df = df.sort_values(["city", "time"])

      # Improvement: shift(1) avoids using today's value inside rolling averages.
    df["temperature_3d_avg"] = (
        df.groupby("city")["temperature_2m_max"]
        .transform(lambda x: x.shift(1).rolling(window=3, min_periods=1).mean())
    )

    df["temperature_7d_avg"] = (
        df.groupby("city")["temperature_2m_max"]
        .transform(lambda x: x.shift(1).rolling(window=7, min_periods=1).mean())
    )

    df["temperature_14d_avg"] = (
        df.groupby("city")["temperature_2m_max"]
        .transform(lambda x: x.shift(1).rolling(window=14, min_periods=1).mean())
    )

    df["precipitation_3d_sum"] = (
        df.groupby("city")["precipitation_sum"]
        .transform(lambda x: x.shift(1).rolling(window=3, min_periods=1).sum())
    )

    df["precipitation_7d_sum"] = (
        df.groupby("city")["precipitation_sum"]
        .transform(lambda x: x.shift(1).rolling(window=7, min_periods=1).sum())
    )

    df["precipitation_14d_sum"] = (
        df.groupby("city")["precipitation_sum"]
        .transform(lambda x: x.shift(1).rolling(window=14, min_periods=1).sum())
    )

    df["wind_3d_avg"] = (
        df.groupby("city")["wind_speed_10m_max"]
        .transform(lambda x: x.shift(1).rolling(window=3, min_periods=1).mean())
    )

    df["wind_7d_avg"] = (
        df.groupby("city")["wind_speed_10m_max"]
        .transform(lambda x: x.shift(1).rolling(window=7, min_periods=1).mean())
    )

    df["humidity_7d_avg"] = (
        df.groupby("city")["relative_humidity_2m_mean"]
        .transform(lambda x: x.shift(1).rolling(window=7, min_periods=1).mean())
    )

    df["humidity_14d_avg"] = (
        df.groupby("city")["relative_humidity_2m_mean"]
        .transform(lambda x: x.shift(1).rolling(window=14, min_periods=1).mean())
    )

    df["cloud_cover_7d_avg"] = (
        df.groupby("city")["cloud_cover_mean"]
        .transform(lambda x: x.shift(1).rolling(window=7, min_periods=1).mean())
    )

    # Improvement: counts rainy days in the last week, useful for precipitation pattern.
    df["rainy_days_7d"] = (
        df.groupby("city")["rain_flag"]
        .transform(lambda x: x.shift(1).rolling(window=7, min_periods=1).sum())
    )

    return df

def add_trend_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add short-term trend features.
    """
    df = df.copy()

    # Improvement: trend features show whether weather is increasing or decreasing.
    df["temperature_trend_1d"] = df["temperature_2m_max"] - df["temperature_lag_1"]
    df["humidity_trend_1d"] = df["relative_humidity_2m_mean"] - df["humidity_lag_1"]
    df["wind_trend_1d"] = df["wind_speed_10m_max"] - df["wind_lag_1"]
    df["precipitation_trend_1d"] = df["precipitation_sum"] - df["precipitation_lag_1"]

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
    df = add_comfort_features(df)
    df = add_lag_features(df)
    df = add_rolling_features(df)
    df = add_trend_features(df)
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
        "day_sin",
        "day_cos",
        "comfort_gap",
        "sunshine_ratio",
        "temperature_lag_1",
        "temperature_lag_3",
        "temperature_lag_7",
        "precipitation_lag_1",
        "precipitation_lag_3",
        "precipitation_lag_7",
        "wind_lag_1",
        "wind_lag_3",
        "humidity_lag_1",
        "humidity_lag_3",
        "temperature_3d_avg",
        "temperature_7d_avg",
        "temperature_14d_avg",
        "precipitation_3d_sum",
        "precipitation_7d_sum",
        "precipitation_14d_sum",
        "wind_3d_avg",
        "wind_7d_avg",
        "humidity_7d_avg",
        "humidity_14d_avg",
        "cloud_cover_7d_avg",
        "rainy_days_7d",
        "temperature_trend_1d",
        "humidity_trend_1d",
        "wind_trend_1d",
        "precipitation_trend_1d",
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