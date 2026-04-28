# src/config.py
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta


# ------------------------
# API endpoints
# ------------------------
ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"


# ------------------------
# Cities / locations
# ------------------------
CITIES = [
    {"name": "Baku", "latitude": 40.41, "longitude": 49.87},
    {"name": "Lankaran", "latitude": 38.75, "longitude": 48.85},
    {"name": "Guba", "latitude": 41.36, "longitude": 48.51},
    {"name": "Gabala", "latitude": 40.58, "longitude": 47.50},
    {"name": "Shaki", "latitude": 41.11, "longitude": 47.10},
]


# ------------------------
# Historical date range
# ------------------------
HISTORICAL_YEARS = 6

END_DATE_DT = date.today() - timedelta(days=1)
START_DATE_DT = END_DATE_DT - relativedelta(years=HISTORICAL_YEARS)

START_DATE = START_DATE_DT.strftime("%Y-%m-%d")
END_DATE = END_DATE_DT.strftime("%Y-%m-%d")


# ------------------------
# Daily weather variables
# MUST match API names exactly
# ------------------------
DAILY_VARIABLES = [
    # 🎯 TARGET VARIABLES
    "temperature_2m_max",
    "precipitation_sum",
    "wind_speed_10m_max",
    "relative_humidity_2m_mean",
    "cloud_cover_mean",          

    # 🧠 EXTRA FEATURES
    "apparent_temperature_max",
    "sunshine_duration",         
]


# ------------------------
# Output / storage settings
# ------------------------
DATA_DIR = "data/raw"
HISTORICAL_SUBDIR = "historical"
FORECAST_SUBDIR = "forecast"

RAW_FILE_FORMAT = "parquet"

FORECAST_DAYS = 7   # keep (API limit)
TIMEZONE = "auto"