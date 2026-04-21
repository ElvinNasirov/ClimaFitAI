import requests
import pandas as pd
import time
from datetime import datetime


def make_request(url, params, city_name):
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=10)

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")

            return response.json()

        except Exception as e:
            print(f"{city_name} attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)

    raise Exception("API request failed")


def fetch_historical(city_name, latitude, longitude, start_date, end_date, variables):

    if start_date >= end_date:
        raise ValueError("Invalid date range")

    today = datetime.today().strftime("%Y-%m-%d")
    if end_date > today:
        raise ValueError("End date cannot be in the future")

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(variables)
    }

    data = make_request(url, params, city_name)

    if "daily" not in data:
        raise ValueError("Malformed API response")

    df = pd.DataFrame(data["daily"])

    if df.empty:
        raise ValueError("Empty dataset")

    df["time"] = pd.to_datetime(df["time"])
    df["city"] = city_name

    return df


def fetch_forecast(city_name, latitude, longitude, variables):

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": ",".join(variables)
    }

    data = make_request(url, params, city_name)

    if "daily" not in data:
        raise ValueError("Malformed forecast response")

    df = pd.DataFrame(data["daily"])
    df["time"] = pd.to_datetime(df["time"])
    df["city"] = city_name

    return df


def fetch_all_cities(cities_config, start_date, end_date, variables):

    all_data = {}

    for city in cities_config:
        name = city["name"]

        df = fetch_historical(
            name,
            city["latitude"],
            city["longitude"],
            start_date,
            end_date,
            variables
        )

        all_data[name] = df

    return all_data