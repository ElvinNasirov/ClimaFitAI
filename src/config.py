# src/config.py
import json
from datetime import date, timedelta
from pathlib import Path

from dateutil.relativedelta import relativedelta


# ── API endpoints ────────────────────────────────────────────────────────────
ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"


# ── Cities / locations ───────────────────────────────────────────────────────
CITIES = [
    {"name": "Baku",     "latitude": 40.41, "longitude": 49.87},
    {"name": "Lankaran", "latitude": 38.75, "longitude": 48.85},
    {"name": "Guba",     "latitude": 41.36, "longitude": 48.51},
    {"name": "Gabala",   "latitude": 40.58, "longitude": 47.50},
    {"name": "Shaki",    "latitude": 41.11, "longitude": 47.10},
]


# ── Historical date range ────────────────────────────────────────────────────
HISTORICAL_YEARS = 6

END_DATE_DT   = date.today() - timedelta(days=1)
START_DATE_DT = END_DATE_DT - relativedelta(years=HISTORICAL_YEARS)

START_DATE = START_DATE_DT.strftime("%Y-%m-%d")
END_DATE   = END_DATE_DT.strftime("%Y-%m-%d")


# ── Daily weather variables (MUST match Open-Meteo API names exactly) ────────
DAILY_VARIABLES = [
    # Target variables (predicted by ML model)
    "temperature_2m_max",
    "precipitation_sum",
    "wind_speed_10m_max",
    "relative_humidity_2m_mean",
    "cloud_cover_mean",

    # Extra features (used as model inputs)
    "apparent_temperature_max",
    "sunshine_duration",
]


# ── Output / storage settings ────────────────────────────────────────────────
DATA_DIR           = "data/raw"
HISTORICAL_SUBDIR  = "historical"
FORECAST_SUBDIR    = "forecast"
RAW_FILE_FORMAT    = "parquet"
FORECAST_DAYS      = 7       # Open-Meteo API limit
TIMEZONE           = "auto"


# ── City-specific activity suggestions ───────────────────────────────────────
# Data is stored in src/city_activities.json — edit that file to add/change
# activities without touching any Python code.
_ACTIVITIES_PATH = Path(__file__).with_name("city_activities.json")
with _ACTIVITIES_PATH.open(encoding="utf-8") as _f:
    CITY_ACTIVITIES: dict[str, dict[str, list[str]]] = json.load(_f)


# ── Activity recommendation logic ────────────────────────────────────────────
# weather_data must contain these keys (all produced by the ML pipeline):
#   temperature_2m_max, precipitation_sum, wind_speed_10m_max,
#   apparent_temperature_max, relative_humidity_2m_mean,
#   cloud_cover_mean, sunshine_duration

def get_city_suggestions(city: str, activity_type: str) -> list[str]:
    """
    Return a list of activity suggestions for a given city and activity type.

    Parameters
    ----------
    city : str
        City name (case-insensitive). Must match a key in city_activities.json.
    activity_type : str
        One of: 'perfect', 'indoor', 'hot', 'cool', 'mixed'.

    Returns
    -------
    list[str]
        Activity suggestions. Falls back to generic suggestions for unknown cities.
    """
    city_key = city.lower()
    if city_key not in CITY_ACTIVITIES:
        return ["General sightseeing", "Cafes"]
    return CITY_ACTIVITIES[city_key].get(activity_type, ["General activities"])


def get_activity_recommendation(weather_data: dict, city: str) -> dict:
    """
    Classify weather conditions and return city-specific activity suggestions.

    Parameters
    ----------
    weather_data : dict
        Must contain: temperature_2m_max, precipitation_sum, wind_speed_10m_max,
        apparent_temperature_max, relative_humidity_2m_mean, cloud_cover_mean,
        sunshine_duration.
    city : str
        City name (case-insensitive).

    Returns
    -------
    dict
        Keys: status, city, activity_type, reason, suggestions.
        status is 'success' or 'error'.
    """
    try:
        temp     = weather_data["temperature_2m_max"]
        feels    = weather_data["apparent_temperature_max"]
        rain     = weather_data["precipitation_sum"]
        wind     = weather_data["wind_speed_10m_max"]
        humidity = weather_data["relative_humidity_2m_mean"]
        cloud    = weather_data["cloud_cover_mean"]
        sun      = weather_data["sunshine_duration"]
    except KeyError as exc:
        return {"status": "error", "message": f"Missing required weather field: {exc}"}

    # Weather classification logic
    # 1. Harsh Conditions (Rain or Heavy Clouds)
    if rain > 10 and cloud > 80:
        activity_type = "indoor"
        reason = "High chance of rain or very overcast"

    # 2. Wind Safety (> 40 km/h = strong breeze, unsafe for outdoor activities)
    elif wind > 40:
        activity_type = "indoor"
        reason = "Strong winds; safer indoors"

    # 3. Heat Stress
    elif feels > 33 or (temp > 30 and humidity > 70):
        activity_type = "hot"
        reason = "High heat index; stay hydrated and seek shade"

    # 4. Optimal Conditions (Perfect)
    # sun > 21600 seconds is 6+ hours of sunshine; wind < 18 km/h = gentle breeze
    elif 20 <= feels <= 30 and rain < 1 and wind < 18 and sun > 21600:
        activity_type = "perfect"
        reason = "Sunny, calm, and pleasant temperature"

    # 5. Cool/Chilly
    elif feels < 20:
        activity_type = "cool"
        reason = "Cooler temperatures; dress in layers"

    # 6. Mixed/Cloudy
    elif cloud > 50:
        activity_type = "mixed"
        reason = "Partly cloudy; decent for most activities"

    # 7. Default
    else:
        activity_type = "mixed"
        reason = "Moderate and manageable conditions"

    return {
        "status": "success",
        "city": city,
        "activity_type": activity_type,
        "reason": reason,
        "suggestions": get_city_suggestions(city, activity_type),
    }


def format_recommendation(result: dict) -> str:
    """
    Convert get_activity_recommendation output into a human-friendly sentence.

    Parameters
    ----------
    result : dict
        Return value of get_activity_recommendation().

    Returns
    -------
    str
        A readable recommendation string.
    """
    if result["status"] != "success":
        return "Sorry, something went wrong while generating your recommendation."

    suggestions = result["suggestions"]
    if len(suggestions) == 1:
        suggestion_text = suggestions[0]
    else:
        suggestion_text = ", ".join(suggestions[:-1]) + " or " + suggestions[-1]

    return (
        f"In {result['city']}, the weather looks {result['reason'].lower()}.\n"
        f"We recommend activities like {suggestion_text}."
    )