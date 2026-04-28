import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta

from ingestion import fetch_historical
from quality_checks import *

# -------------------------
# Logging setup
# -------------------------
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# -------------------------
# Config (temporary simple)
# -------------------------
CITIES = [
    {"name": "Baku", "latitude": 40.41, "longitude": 49.87},
    {"name": "Guba", "latitude": 41.36, "longitude": 48.51},
    {"name": "Lankaran", "latitude": 38.75, "longitude": 48.85},
    {"name": "Shaki", "latitude": 41.20, "longitude": 47.17},
    {"name": "Gabala", "latitude": 40.98, "longitude": 47.84}
]

VARIABLES = [
    "temperature_2m_max",
    "precipitation_sum",
    "windspeed_10m_max",
    "relative_humidity_2m_mean"
]


# -------------------------
# Helper: incremental date
# -------------------------
def get_start_date_incremental(city_name):
    try:
        df = pd.read_csv(f"../data/raw/{city_name}.csv")
        last_date = pd.to_datetime(df["time"]).max()
        return (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    except:
        return "2020-01-01"


# -------------------------
# Pipeline Runner
# -------------------------
def run_pipeline(mode):

    logging.info(f"Pipeline started in {mode} mode")

    all_results = []

    for city in CITIES:
        name = city["name"]

        if mode == "full":
            start_date = "2020-01-01"
        else:
            start_date = get_start_date_incremental(name)

        end_date = datetime.today().strftime("%Y-%m-%d")

        print(f"Processing {name} from {start_date} to {end_date}")

        df = fetch_historical(
            name,
            city["latitude"],
            city["longitude"],
            start_date,
            end_date,
            VARIABLES
        )

        # -------------------------
        # Save raw data
        # -------------------------
        file_path = f"data/raw/{name}.csv"

        try:
            old = pd.read_csv(file_path)
            df = pd.concat([old, df]).drop_duplicates(subset=["time"])
        except:
            pass

        df.to_csv(file_path, index=False)

        logging.info(f"{name}: {len(df)} rows saved")

        # -------------------------
        # Quality Checks
        # -------------------------
        checks = [
            check_row_count(df),
            check_missing_values(df),
            check_missing_dates(df),
            check_weather_ranges(df),
            check_freshness(df)
        ]

        for c in checks:
            logging.info(f"{name} - {c}")

        all_results.extend(checks)

    logging.info("Pipeline finished")

    return all_results


# -------------------------
# CLI
# -------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="full")

    args = parser.parse_args()

    results = run_pipeline(args.mode)

    print("\nQuality Check Summary:")
    for r in results:
        print(r)