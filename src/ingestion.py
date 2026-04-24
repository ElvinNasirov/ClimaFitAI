import requests
import pandas as pd
import time
from datetime import datetime

from config import (
    ARCHIVE_API_URL,
    FORECAST_API_URL,
    TIMEZONE,
    FORECAST_DAYS,
)


# -------------------------------
# Request with retry + rate limit handling
# -------------------------------
def make_request(url, params, city_name, max_retries=5):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 429:
                wait_time = 2 ** attempt
                print(f"[{city_name}] Rate limit hit. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")

            return response.json()

        except Exception as e:
            print(f"[{city_name}] Attempt {attempt + 1} failed: {e}")
            time.sleep(2 ** attempt)

    raise Exception(f"[{city_name}] API request failed after {max_retries} retries")


# -------------------------------
# Fetch Historical Data
# -------------------------------
def fetch_historical(city_name, latitude, longitude, start_date, end_date, variables):
    if start_date >= end_date:
        raise ValueError("start_date must be before end_date")

    today = datetime.today().strftime("%Y-%m-%d")
    if end_date > today:
        raise ValueError("end_date cannot be in the future")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(variables),
        "timezone": TIMEZONE,
    }

    data = make_request(ARCHIVE_API_URL, params, city_name)

    if "daily" not in data:
        raise ValueError(f"[{city_name}] Malformed API response")

    df = pd.DataFrame(data["daily"])

    if df.empty:
        raise ValueError(f"[{city_name}] Empty historical dataset")

    df["time"] = pd.to_datetime(df["time"])
    df["city"] = city_name

    return df


# -------------------------------
# Fetch Forecast Data
# -------------------------------
def fetch_forecast(city_name, latitude, longitude, variables):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": ",".join(variables),
        "forecast_days": FORECAST_DAYS,
        "timezone": TIMEZONE,
    }

    data = make_request(FORECAST_API_URL, params, city_name)

    if "daily" not in data:
        raise ValueError(f"[{city_name}] Malformed forecast response")

    df = pd.DataFrame(data["daily"])

    if df.empty:
        raise ValueError(f"[{city_name}] Empty forecast dataset")

    df["time"] = pd.to_datetime(df["time"])
    df["city"] = city_name

    return df


# -------------------------------
# Fetch Historical Data for All Cities
# -------------------------------
def fetch_all_cities(cities_config, start_date, end_date, variables):
    all_data = {}

    for city in cities_config:
        name = city["name"]

        print(f"Fetching historical data for {name}...")

        df = fetch_historical(
            city_name=name,
            latitude=city["latitude"],
            longitude=city["longitude"],
            start_date=start_date,
            end_date=end_date,
            variables=variables,
        )

        all_data[name] = df

        print(f"{name}: {len(df)} historical rows")

        time.sleep(2)

    return all_data


# -------------------------------
# Fetch Forecast Data for All Cities
# -------------------------------
def fetch_forecast_all_cities(cities_config, variables):
    all_forecasts = {}

    for city in cities_config:
        name = city["name"]

        print(f"Fetching forecast data for {name}...")

        df = fetch_forecast(
            city_name=name,
            latitude=city["latitude"],
            longitude=city["longitude"],
            variables=variables,
        )

        all_forecasts[name] = df

        print(f"{name}: {len(df)} forecast rows")

        time.sleep(2)

    return all_forecasts
