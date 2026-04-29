"""
End-to-end Weather Intelligence Pipeline.

This pipeline:
1. Loads raw parquet files into DuckDB
2. Cleans historical weather data
3. Builds model-ready features
4. Trains the final multi-output regression model
5. Builds a hybrid 28-day forecast:
   - days 1-7 from Open-Meteo API forecast
   - days 8-28 from ML model
6. Saves outputs into DuckDB
"""

# ------------------------
# Imports
# ------------------------

from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.multioutput import MultiOutputRegressor

from src.config import (
    CITIES,
    START_DATE,
    END_DATE,
    DAILY_VARIABLES,
    DATA_DIR,
    HISTORICAL_SUBDIR,
    FORECAST_SUBDIR,
)

from src.ingestion import (
    fetch_all_cities,
    fetch_forecast_all_cities,
)

from src.db import (
    create_schemas,
    load_raw_data,
    run_query,
    get_connection,
)

from src.quality_checks import (
    check_missing_values,
    check_duplicate_rows,
    check_duplicate_city_dates,
    check_date_coverage,
    check_missing_dates,
    check_column_consistency,
    check_weather_ranges,
)

from src.cleaning import clean_data

from src.features import (
    build_features,
    get_feature_columns,
    get_target_columns,
)

# ------------------------
# Project paths
# ------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / DATA_DIR
RAW_HISTORICAL_DIR = RAW_DATA_DIR / HISTORICAL_SUBDIR
RAW_FORECAST_DIR = RAW_DATA_DIR / FORECAST_SUBDIR

# ------------------------
# General DuckDB helper
# ------------------------

def store_dataframe(
    df: pd.DataFrame,
    table_name: str,
    schema: str = "analytics",
) -> None:
    """
    Store a pandas DataFrame into DuckDB.
    Existing table is replaced.
    """
    with get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        conn.register("temp_df_view", df)

        conn.execute(f"""
            CREATE OR REPLACE TABLE {schema}.{table_name} AS
            SELECT * FROM temp_df_view;
        """)

def run_clean_data_quality_gate(clean_df: pd.DataFrame) -> None:
    """
    Run quality checks after cleaning and before feature engineering.

    The pipeline stops if critical checks fail.
    """
    print("Running quality gate on cleaned historical data...")

    failures = []

    clean_df = clean_df.copy()
    clean_df["time"] = pd.to_datetime(clean_df["time"])

    missing_values = check_missing_values(clean_df)
    duplicate_rows = check_duplicate_rows(clean_df)
    duplicate_city_dates = check_duplicate_city_dates(clean_df)
    missing_dates = check_missing_dates(clean_df)
    weather_range_violations = check_weather_ranges(clean_df)

    if missing_values.sum() > 0:
        failures.append(
            "Missing values found after cleaning:\n"
            f"{missing_values[missing_values > 0]}"
        )

    if duplicate_rows > 0:
        failures.append(f"Duplicate rows found after cleaning: {duplicate_rows}")

    if duplicate_city_dates > 0:
        failures.append(
            f"Duplicate city-date records found after cleaning: {duplicate_city_dates}"
        )

    cities_with_missing_dates = {
        city: count
        for city, count in missing_dates.items()
        if count > 0
    }

    if cities_with_missing_dates:
        failures.append(
            f"Missing dates found after cleaning: {cities_with_missing_dates}"
        )

    bad_ranges = {
        col: count
        for col, count in weather_range_violations.items()
        if count > 0
    }

    if bad_ranges:
        failures.append(
            f"Weather range violations found after cleaning: {bad_ranges}"
        )

    if failures:
        failure_message = "\n\n".join(failures)
        raise ValueError(f"Quality gate failed:\n\n{failure_message}")

    print("Quality gate passed.")

# ------------------------
# Feature preparation
# ------------------------

def clear_raw_parquet_files() -> None:
    """
    Remove old raw parquet files before writing fresh API outputs.

    This prevents stale files from being loaded into DuckDB.
    """
    RAW_HISTORICAL_DIR.mkdir(parents=True, exist_ok=True)
    RAW_FORECAST_DIR.mkdir(parents=True, exist_ok=True)

    for file in RAW_HISTORICAL_DIR.glob("*.parquet"):
        file.unlink()

    for file in RAW_FORECAST_DIR.glob("*.parquet"):
        file.unlink()

    print("Old raw parquet files removed.")


def save_city_frames_to_parquet(
    data_by_city: dict[str, pd.DataFrame],
    output_dir: Path,
    suffix: str,
) -> None:
    """
    Save city-level DataFrames into parquet files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    for city_name, df in data_by_city.items():
        safe_city = city_name.lower().replace(" ", "_")
        file_path = output_dir / f"{safe_city}_{suffix}.parquet"

        df.to_parquet(file_path, index=False)

        print(f"Saved: {file_path}")

def refresh_raw_data() -> None:
    """
    Fetch fresh historical and forecast data from Open-Meteo API,
    then save raw parquet files.

    Historical:
        START_DATE → END_DATE

    Forecast:
        next 7 days from Forecast API
    """
    print("Refreshing raw API data...")
    print(f"Historical range: {START_DATE} → {END_DATE}")
    print(f"Cities: {[city['name'] for city in CITIES]}")

    clear_raw_parquet_files()

    historical_data = fetch_all_cities(
        cities_config=CITIES,
        start_date=START_DATE,
        end_date=END_DATE,
        variables=DAILY_VARIABLES,
    )

    save_city_frames_to_parquet(
        data_by_city=historical_data,
        output_dir=RAW_HISTORICAL_DIR,
        suffix=f"historical_{START_DATE}_{END_DATE}",
    )

    forecast_data = fetch_forecast_all_cities(
        cities_config=CITIES,
        variables=DAILY_VARIABLES,
    )

    save_city_frames_to_parquet(
        data_by_city=forecast_data,
        output_dir=RAW_FORECAST_DIR,
        suffix="forecast",
    )

    print("Raw API data refreshed.")

def prepare_model_features() -> pd.DataFrame:
    """
    Load raw historical data from DuckDB, clean it,
    run quality checks, build ML features, and save analytics.model_features.
    """
    raw_df = run_query("SELECT * FROM raw.historical")

    clean_df = clean_data(raw_df)

    run_clean_data_quality_gate(clean_df)

    feature_df, _ = build_features(clean_df)

    store_dataframe(
        df=feature_df,
        table_name="model_features",
        schema="analytics",
    )

    return feature_df


def add_target_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add calendar features for the target date.
    These tell the model which future date it is predicting.
    """
    df = df.copy()

    df["target_time"] = pd.to_datetime(df["target_time"])

    df["target_month"] = df["target_time"].dt.month
    df["target_day_of_month"] = df["target_time"].dt.day
    df["target_day_of_year"] = df["target_time"].dt.dayofyear

    df["target_day_sin"] = np.sin(
        2 * np.pi * df["target_day_of_year"] / 365.25
    )
    df["target_day_cos"] = np.cos(
        2 * np.pi * df["target_day_of_year"] / 365.25
    )

    return df


def make_supervised(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """
    Create supervised dataset for a forecast horizon.

    time = origin date
    target_time = date being predicted
    target columns = weather values at target_time
    """
    out = df.copy().sort_values(["city", "time"]).reset_index(drop=True)

    out["target_time"] = out.groupby("city")["time"].shift(-horizon)

    for col in get_target_columns():
        out[f"{col}_target"] = out.groupby("city")[col].shift(-horizon)

    out = out.dropna().reset_index(drop=True)
    out = add_target_calendar_features(out)

    return out


def get_horizon_feature_columns() -> list[str]:
    """
    Base model features plus target-date calendar features.
    """
    return get_feature_columns() + [
        "target_month",
        "target_day_of_month",
        "target_day_of_year",
        "target_day_sin",
        "target_day_cos",
    ]


# ------------------------
# Model training
# ------------------------

def train_final_model(
    feature_df: pd.DataFrame,
    horizon: int = 28,
) -> MultiOutputRegressor:
    """
    Train final multi-output regression model.

    GradientBoosting was selected from the modeling notebook
    because it had the best average RMSE in backtesting.
    """
    supervised_df = make_supervised(feature_df, horizon=horizon)

    feature_cols = get_horizon_feature_columns()
    target_cols = [f"{col}_target" for col in get_target_columns()]

    X = supervised_df[feature_cols]
    y = supervised_df[target_cols]

    model = MultiOutputRegressor(
        GradientBoostingRegressor(random_state=42)
    )

    model.fit(X, y)

    return model


# ------------------------
# Forecast preparation
# ------------------------

def prepare_api_forecast_output() -> pd.DataFrame:
    """
    Prepare Open-Meteo forecast as days 1-7 of final 28-day forecast.
    """
    forecast_df = run_query("SELECT * FROM raw.forecast").copy()
    forecast_df["time"] = pd.to_datetime(forecast_df["time"])

    forecast_df = (
        forecast_df
        .sort_values(["city", "time"])
        .reset_index(drop=True)
    )

    forecast_df["origin_time"] = (
        forecast_df.groupby("city")["time"].transform("min")
        - pd.Timedelta(days=1)
    )

    forecast_df["forecast_horizon"] = forecast_df.groupby("city").cumcount() + 1
    forecast_df["target_time"] = forecast_df["time"]
    forecast_df["source"] = "api_forecast"

    output_cols = [
        "city",
        "origin_time",
        "forecast_horizon",
        "target_time",
        "source",
    ] + get_target_columns()

    return forecast_df[output_cols]


def prepare_latest_origin() -> pd.DataFrame:
    """
    Combine historical actuals + 7-day API forecast,
    rebuild features, and return the latest available feature row per city.

    This latest row becomes the starting point for ML days 8-28.
    """
    historical_raw = run_query("SELECT * FROM raw.historical").copy()
    historical_raw["time"] = pd.to_datetime(historical_raw["time"])

    forecast_raw = run_query("SELECT * FROM raw.forecast").copy()
    forecast_raw["time"] = pd.to_datetime(forecast_raw["time"])

    combined = pd.concat(
        [historical_raw, forecast_raw],
        ignore_index=True,
    )

    combined = (
        combined
        .sort_values(["city", "time"])
        .drop_duplicates(subset=["city", "time"], keep="last")
        .reset_index(drop=True)
    )

    future_feature_df, _ = build_features(combined)

    latest_origin = (
        future_feature_df
        .sort_values(["city", "time"])
        .groupby("city")
        .tail(1)
        .reset_index(drop=True)
    )

    return latest_origin


def predict_ml_days_8_to_28(
    model: MultiOutputRegressor,
    latest_origin_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Predict forecast horizons 8-28 using the trained ML model.

    Days 1-7 are already covered by API forecast.
    ML model predicts the remaining 21 days.
    """
    feature_cols = get_horizon_feature_columns()
    target_cols = get_target_columns()

    rows = []

    for _, origin_row in latest_origin_df.iterrows():
        city = origin_row["city"]
        origin_time = pd.to_datetime(origin_row["time"])

        for forecast_horizon in range(8, 29):
            days_after_api = forecast_horizon - 7
            target_time = origin_time + pd.Timedelta(days=days_after_api)

            row = origin_row.copy()
            row["target_time"] = target_time

            row_df = pd.DataFrame([row])
            row_df = add_target_calendar_features(row_df)

            X_future = row_df[feature_cols]
            pred = model.predict(X_future)[0]

            result = {
                "city": city,
                "origin_time": origin_time,
                "forecast_horizon": forecast_horizon,
                "target_time": target_time,
                "source": "ml_model",
            }

            for i, target in enumerate(target_cols):
                result[target] = pred[i]

            rows.append(result)

    return pd.DataFrame(rows)


def build_final_28d_forecast(
    feature_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build final hybrid 28-day forecast.

    Days 1-7:
        Open-Meteo API forecast

    Days 8-28:
        ML model forecast
    """
    model = train_final_model(feature_df, horizon=28)

    api_7d = prepare_api_forecast_output()
    latest_origin = prepare_latest_origin()
    ml_21d = predict_ml_days_8_to_28(model, latest_origin)

    final_forecast = pd.concat(
        [api_7d, ml_21d],
        ignore_index=True,
    )

    final_forecast = (
        final_forecast
        .sort_values(["city", "target_time"])
        .reset_index(drop=True)
    )

    store_dataframe(
        df=final_forecast,
        table_name="final_28d_forecast",
        schema="analytics",
    )

    return final_forecast


# ------------------------
# Main pipeline
# ------------------------

def run_pipeline(refresh_data: bool = True) -> dict[str, pd.DataFrame]:
    """
    Run the full weather intelligence pipeline.

    Parameters
    ----------
    refresh_data:
        If True, fetch fresh historical and forecast data from Open-Meteo API
        before rebuilding DuckDB and model outputs.

        If False, reuse existing raw parquet files.

    Returns
    -------
    dict
        Dictionary with model_features and final_28d_forecast DataFrames.
    """
    print("Step 1/7 — Creating schemas...")
    create_schemas()

    if refresh_data:
        print("Step 2/7 — Refreshing raw API data...")
        refresh_raw_data()
    else:
        print("Step 2/7 — Reusing existing raw parquet files...")

    print("Step 3/7 — Loading raw parquet files into DuckDB...")
    load_raw_data()

    print("Step 4/7 — Running data quality gate...")
    run_quality_gate()

    print("Step 5/7 — Cleaning data and building model features...")
    feature_df = prepare_model_features()

    print("Step 6/7 — Training model and building final 28-day forecast...")
    final_forecast = build_final_28d_forecast(feature_df)

    print("Step 7/7 — Pipeline completed.")
    print(f"Model feature rows: {len(feature_df)}")
    print(f"Final forecast rows: {len(final_forecast)}")

    return {
        "model_features": feature_df,
        "final_28d_forecast": final_forecast,
    }


if __name__ == "__main__":
    run_pipeline(refresh_data=True)