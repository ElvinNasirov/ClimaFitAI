"""
Feature engineering utilities for the Weather Intelligence Pipeline.

This module transforms cleaned daily weather data into a model-ready
feature table for multi-output weather forecasting.
"""

import numpy as np
import pandas as pd

from sklearn.preprocessing import LabelEncoder


TARGET_COLUMNS = [
    "temperature_2m_max",
    "precipitation_sum",
    "wind_speed_10m_max",
    "relative_humidity_2m_mean",
    "cloud_cover_mean",
    "sunshine_duration",
]


FEATURE_COLUMNS = [
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


def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add calendar and cyclical seasonality features.
    """
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])

    df["month"] = df["time"].dt.month
    df["day_of_month"] = df["time"].dt.day
    df["day_of_year"] = df["time"].dt.dayofyear

    df["day_sin"] = np.sin(2 * np.pi * df["day_of_year"] / 365.25)
    df["day_cos"] = np.cos(2 * np.pi * df["day_of_year"] / 365.25)

    return df


def add_comfort_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add human-comfort related weather features.
    """
    df = df.copy()

    df["comfort_gap"] = (
        df["apparent_temperature_max"] - df["temperature_2m_max"]
    )

    df["sunshine_ratio"] = df["sunshine_duration"] / (24 * 3600)

    return df


def add_risk_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add binary risk flags used for rolling risk features.
    """
    df = df.copy()

    df["rain_flag"] = (df["precipitation_sum"] > 1).astype(int)

    return df


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add previous-day and multi-day lag features by city.
    """
    df = df.copy()
    df = df.sort_values(["city", "time"])

    grouped = df.groupby("city")

    df["temperature_lag_1"] = grouped["temperature_2m_max"].shift(1)
    df["temperature_lag_3"] = grouped["temperature_2m_max"].shift(3)
    df["temperature_lag_7"] = grouped["temperature_2m_max"].shift(7)

    df["precipitation_lag_1"] = grouped["precipitation_sum"].shift(1)
    df["precipitation_lag_3"] = grouped["precipitation_sum"].shift(3)
    df["precipitation_lag_7"] = grouped["precipitation_sum"].shift(7)

    df["wind_lag_1"] = grouped["wind_speed_10m_max"].shift(1)
    df["wind_lag_3"] = grouped["wind_speed_10m_max"].shift(3)

    df["humidity_lag_1"] = grouped["relative_humidity_2m_mean"].shift(1)
    df["humidity_lag_3"] = grouped["relative_humidity_2m_mean"].shift(3)

    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add rolling weather trend features by city.

    Rolling windows use shift(1) to avoid data leakage from the current day.
    """
    df = df.copy()
    df = df.sort_values(["city", "time"])

    grouped = df.groupby("city")

    df["temperature_3d_avg"] = grouped["temperature_2m_max"].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )
    df["temperature_7d_avg"] = grouped["temperature_2m_max"].transform(
        lambda x: x.shift(1).rolling(window=7, min_periods=1).mean()
    )
    df["temperature_14d_avg"] = grouped["temperature_2m_max"].transform(
        lambda x: x.shift(1).rolling(window=14, min_periods=1).mean()
    )

    df["precipitation_3d_sum"] = grouped["precipitation_sum"].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).sum()
    )
    df["precipitation_7d_sum"] = grouped["precipitation_sum"].transform(
        lambda x: x.shift(1).rolling(window=7, min_periods=1).sum()
    )
    df["precipitation_14d_sum"] = grouped["precipitation_sum"].transform(
        lambda x: x.shift(1).rolling(window=14, min_periods=1).sum()
    )

    df["wind_3d_avg"] = grouped["wind_speed_10m_max"].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )
    df["wind_7d_avg"] = grouped["wind_speed_10m_max"].transform(
        lambda x: x.shift(1).rolling(window=7, min_periods=1).mean()
    )

    df["humidity_7d_avg"] = grouped["relative_humidity_2m_mean"].transform(
        lambda x: x.shift(1).rolling(window=7, min_periods=1).mean()
    )
    df["humidity_14d_avg"] = grouped["relative_humidity_2m_mean"].transform(
        lambda x: x.shift(1).rolling(window=14, min_periods=1).mean()
    )

    df["cloud_cover_7d_avg"] = grouped["cloud_cover_mean"].transform(
        lambda x: x.shift(1).rolling(window=7, min_periods=1).mean()
    )

    df["rainy_days_7d"] = grouped["rain_flag"].transform(
        lambda x: x.shift(1).rolling(window=7, min_periods=1).sum()
    )

    return df


def add_trend_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add short-term weather change features.
    """
    df = df.copy()

    df["temperature_trend_1d"] = (
        df["temperature_2m_max"] - df["temperature_lag_1"]
    )
    df["humidity_trend_1d"] = (
        df["relative_humidity_2m_mean"] - df["humidity_lag_1"]
    )
    df["wind_trend_1d"] = (
        df["wind_speed_10m_max"] - df["wind_lag_1"]
    )
    df["precipitation_trend_1d"] = (
        df["precipitation_sum"] - df["precipitation_lag_1"]
    )

    return df


def add_city_encoding(df: pd.DataFrame) -> tuple[pd.DataFrame, LabelEncoder]:
    """
    Encode city names as integer labels.
    """
    df = df.copy()

    encoder = LabelEncoder()
    df["city_encoded"] = encoder.fit_transform(df["city"])

    return df, encoder


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, LabelEncoder]:
    """
    Run the full feature engineering pipeline.
    """
    df = df.copy()

    df = add_calendar_features(df)
    df = add_comfort_features(df)
    df = add_risk_flags(df)
    df = add_lag_features(df)
    df = add_rolling_features(df)
    df = add_trend_features(df)
    df, encoder = add_city_encoding(df)

    df = df.dropna().reset_index(drop=True)

    return df, encoder


def get_feature_columns() -> list[str]:
    """
    Return model feature column names.
    """
    return FEATURE_COLUMNS


def get_target_columns() -> list[str]:
    """
    Return multi-output regression target column names.
    """
    return TARGET_COLUMNS