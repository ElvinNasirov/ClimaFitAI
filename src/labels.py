from __future__ import annotations

import pandas as pd


def assign_risk_level(row: pd.Series) -> str:
    """
    Assign a 3-class picnic risk label based on daily weather conditions.

    Classes:
    - High   -> picnic likely to be cancelled
    - Medium -> picnic possible, but conditions are not ideal
    - Low    -> good conditions for a picnic

    Expected columns:
    - precipitation_sum
    - temperature_2m_max
    - wind_speed_10m_max
    - apparent_temperature_max
    - relative_humidity_2m_mean
    - weather_code
    """

    precipitation = row.get("precipitation_sum", None)
    temp_max = row.get("temperature_2m_max", None)
    wind_max = row.get("wind_speed_10m_max", None)
    apparent_temp = row.get("apparent_temperature_max", None)
    humidity = row.get("relative_humidity_2m_mean", None)
    weather_code = row.get("weather_code", None)

    # High risk: rain, cold, or strong wind
    if (
        (precipitation is not None and precipitation > 2.0)
        or (temp_max is not None and temp_max < 12.0)
        or (wind_max is not None and wind_max > 30.0)
    ):
        return "High"

    # Medium risk: discomfort conditions
    # Use apparent temperature / humidity / cloudy or unsettled weather codes as softer warning signals
    cloudy_or_unsettled_codes = {
        1, 2, 3, 45, 48, 51, 53, 55, 61, 63, 65, 66, 67, 80, 81, 82
    }

    if (
        (apparent_temp is not None and apparent_temp > 30.0)
        or (humidity is not None and humidity > 80.0)
        or (weather_code is not None and int(weather_code) in cloudy_or_unsettled_codes)
    ):
        return "Medium"

    # Low risk: otherwise suitable for picnic
    return "Low"


def add_risk_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add risk_level and suitable_picnic columns to a dataframe.

    risk_level:
    - High / Medium / Low

    suitable_picnic:
    - 1 if Low risk
    - 0 otherwise
    """
    required_cols = [
        "precipitation_sum",
        "temperature_2m_max",
        "wind_speed_10m_max",
        "apparent_temperature_max",
        "relative_humidity_2m_mean",
        "weather_code",
    ]

    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for label generation: {missing}")

    out = df.copy()
    out["risk_level"] = out.apply(assign_risk_level, axis=1)
    out["suitable_picnic"] = (out["risk_level"] == "Low").astype(int)

    return out