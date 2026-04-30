"""
End-to-end Weather Intelligence Pipeline.

Pipeline flow:
1. Fetch or reuse raw Open-Meteo data
2. Load raw parquet files into DuckDB
3. Validate raw data project scope
4. Clean historical weather data
5. Run quality gate
6. Build model-ready features
7. Train multi-output regression models
8. Build a hybrid 28-day forecast:
   - days 1-7 from Open-Meteo API forecast
   - days 8-28 from ML models
9. Save outputs into DuckDB
"""

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from sklearn.base import clone
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.multioutput import MultiOutputRegressor

from src.cleaning import clean_data
from src.config import (
    CITIES,
    DAILY_VARIABLES,
    DATA_DIR,
    END_DATE,
    FORECAST_DAYS,
    FORECAST_SUBDIR,
    HISTORICAL_SUBDIR,
    START_DATE,
)
from src.db import (
    create_schemas,
    get_connection,
    load_raw_data,
    run_query,
)
from src.features import (
    build_features,
    get_feature_columns,
    get_target_columns,
)
from src.ingestion import (
    fetch_all_cities,
    fetch_forecast_all_cities,
)
from src.quality_checks import (
    check_duplicate_city_dates,
    check_duplicate_rows,
    check_missing_dates,
    check_missing_values,
    check_weather_ranges,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / DATA_DIR
RAW_HISTORICAL_DIR = RAW_DATA_DIR / HISTORICAL_SUBDIR
RAW_FORECAST_DIR = RAW_DATA_DIR / FORECAST_SUBDIR


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
            SELECT *
            FROM temp_df_view;
        """)


def _print_check_results(checks: list[dict[str, Any]]) -> None:
    """
    Print quality check results in a compact CLI-friendly format.
    """
    for check in checks:
        print(f"- {check['check']}: {check['status']}")


def _raise_if_checks_failed(
    checks: list[dict[str, Any]],
    gate_name: str,
    fail_statuses: tuple[str, ...] = ("WARN", "FAIL"),
) -> None:
    """
    Raise an error if any check has a failing status.
    """
    failed_checks = [
        check for check in checks
        if check["status"] in fail_statuses
    ]

    if not failed_checks:
        return

    messages = []

    for check in failed_checks:
        messages.append(
            f"{check['check']} — {check['status']}\n"
            f"Details: {check['details']}"
        )

    raise ValueError(
        f"{gate_name} failed:\n\n" + "\n\n".join(messages)
    )


def run_raw_project_scope_gate() -> None:
    """
    Validate that raw data belongs to the project scope before cleaning.

    This protects the pipeline from unrelated, corrupted, or tampered data.
    """
    print("Running raw project-scope gate...")

    historical_df = run_query("SELECT * FROM raw.historical").copy()
    forecast_df = run_query("SELECT * FROM raw.forecast").copy()

    expected_cities = {city["name"] for city in CITIES}
    required_columns = set(["time", "city"] + DAILY_VARIABLES)

    historical_start = pd.to_datetime(START_DATE)
    historical_end = pd.to_datetime(END_DATE)

    expected_forecast_start = historical_end + pd.Timedelta(days=1)
    expected_forecast_end = expected_forecast_start + pd.Timedelta(
        days=FORECAST_DAYS - 1
    )

    checks = []

    def add_check(check: str, status: str, details: Any) -> None:
        checks.append({
            "check": check,
            "status": status,
            "details": details,
        })

    historical_missing_cols = sorted(
        required_columns - set(historical_df.columns)
    )
    forecast_missing_cols = sorted(
        required_columns - set(forecast_df.columns)
    )

    add_check(
        "historical_required_columns",
        "PASS" if not historical_missing_cols else "FAIL",
        historical_missing_cols or "all required columns present",
    )

    add_check(
        "forecast_required_columns",
        "PASS" if not forecast_missing_cols else "FAIL",
        forecast_missing_cols or "all required columns present",
    )

    if historical_missing_cols or forecast_missing_cols:
        print("Raw project-scope checks:")
        _print_check_results(checks)
        _raise_if_checks_failed(
            checks=checks,
            gate_name="Raw project-scope gate",
            fail_statuses=("FAIL",),
        )

    historical_df["time"] = pd.to_datetime(
        historical_df["time"],
        errors="coerce",
    )
    forecast_df["time"] = pd.to_datetime(
        forecast_df["time"],
        errors="coerce",
    )

    add_check(
        "historical_date_parse",
        "PASS" if historical_df["time"].notna().all() else "FAIL",
        "all dates parsed"
        if historical_df["time"].notna().all()
        else "invalid historical dates found",
    )

    add_check(
        "forecast_date_parse",
        "PASS" if forecast_df["time"].notna().all() else "FAIL",
        "all dates parsed"
        if forecast_df["time"].notna().all()
        else "invalid forecast dates found",
    )

    historical_cities = set(historical_df["city"].unique())
    forecast_cities = set(forecast_df["city"].unique())

    add_check(
        "historical_city_scope",
        "PASS" if historical_cities == expected_cities else "FAIL",
        {
            "expected": sorted(expected_cities),
            "actual": sorted(historical_cities),
        },
    )

    add_check(
        "forecast_city_scope",
        "PASS" if forecast_cities == expected_cities else "FAIL",
        {
            "expected": sorted(expected_cities),
            "actual": sorted(forecast_cities),
        },
    )

    actual_historical_start = historical_df["time"].min()
    actual_historical_end = historical_df["time"].max()

    add_check(
        "historical_date_range",
        "PASS"
        if actual_historical_start == historical_start
        and actual_historical_end == historical_end
        else "FAIL",
        {
            "expected": f"{historical_start.date()} → {historical_end.date()}",
            "actual": (
                f"{actual_historical_start.date()} "
                f"→ {actual_historical_end.date()}"
            ),
        },
    )

    actual_forecast_start = forecast_df["time"].min()
    actual_forecast_end = forecast_df["time"].max()

    add_check(
        "forecast_date_range",
        "PASS"
        if actual_forecast_start == expected_forecast_start
        and actual_forecast_end == expected_forecast_end
        else "FAIL",
        {
            "expected": (
                f"{expected_forecast_start.date()} "
                f"→ {expected_forecast_end.date()}"
            ),
            "actual": (
                f"{actual_forecast_start.date()} "
                f"→ {actual_forecast_end.date()}"
            ),
        },
    )

    expected_historical_rows = (
        historical_end - historical_start
    ).days + 1

    historical_counts = historical_df.groupby("city").size().to_dict()
    forecast_counts = forecast_df.groupby("city").size().to_dict()

    historical_count_ok = all(
        historical_counts.get(city, 0) == expected_historical_rows
        for city in expected_cities
    )

    forecast_count_ok = all(
        forecast_counts.get(city, 0) == FORECAST_DAYS
        for city in expected_cities
    )

    add_check(
        "historical_rows_per_city",
        "PASS" if historical_count_ok else "FAIL",
        {
            "expected_per_city": expected_historical_rows,
            "actual": historical_counts,
        },
    )

    add_check(
        "forecast_rows_per_city",
        "PASS" if forecast_count_ok else "FAIL",
        {
            "expected_per_city": FORECAST_DAYS,
            "actual": forecast_counts,
        },
    )

    print("Raw project-scope checks:")
    _print_check_results(checks)

    _raise_if_checks_failed(
        checks=checks,
        gate_name="Raw project-scope gate",
        fail_statuses=("FAIL",),
    )

    print("Raw project-scope gate passed.")


def run_clean_data_quality_gate(clean_df: pd.DataFrame) -> None:
    """
    Run quality checks after cleaning and before feature engineering.

    The pipeline stops if critical checks fail.
    """
    print("Running quality gate on cleaned historical data...")

    clean_df = clean_df.copy()
    clean_df["time"] = pd.to_datetime(clean_df["time"])

    checks = [
        check_missing_values(clean_df),
        check_duplicate_rows(clean_df),
        check_duplicate_city_dates(clean_df),
        check_missing_dates(clean_df),
        check_weather_ranges(clean_df),
    ]

    print("Quality checks:")
    _print_check_results(checks)

    _raise_if_checks_failed(
        checks=checks,
        gate_name="Quality gate",
        fail_statuses=("WARN", "FAIL"),
    )

    print("Quality gate passed.")


def clear_raw_parquet_files() -> None:
    """
    Remove old raw parquet files before writing fresh API outputs.
    """
    RAW_HISTORICAL_DIR.mkdir(parents=True, exist_ok=True)
    RAW_FORECAST_DIR.mkdir(parents=True, exist_ok=True)

    for file_path in RAW_HISTORICAL_DIR.glob("*.parquet"):
        file_path.unlink()

    for file_path in RAW_FORECAST_DIR.glob("*.parquet"):
        file_path.unlink()

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

    saved_count = 0

    for city_name, city_df in data_by_city.items():
        safe_city = city_name.lower().replace(" ", "_")
        file_path = output_dir / f"{safe_city}_{suffix}.parquet"

        city_df.to_parquet(file_path, index=False)
        saved_count += 1

    print(f"Saved {saved_count} parquet files to {output_dir}")


def refresh_raw_data() -> None:
    """
    Fetch fresh historical and forecast data from Open-Meteo API,
    then save raw parquet files.
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
        verbose=False,
    )

    save_city_frames_to_parquet(
        data_by_city=historical_data,
        output_dir=RAW_HISTORICAL_DIR,
        suffix=f"historical_{START_DATE}_{END_DATE}",
    )

    forecast_data = fetch_forecast_all_cities(
        cities_config=CITIES,
        variables=DAILY_VARIABLES,
        verbose=False,
    )

    save_city_frames_to_parquet(
        data_by_city=forecast_data,
        output_dir=RAW_FORECAST_DIR,
        suffix="forecast",
    )

    print("Raw API data refreshed.")


def prepare_model_features() -> pd.DataFrame:
    """
    Load raw historical data from DuckDB, clean it, run quality checks,
    build ML features, and save analytics.model_features.
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
    Add calendar features for the future target date.
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


def make_supervised(
    feature_df: pd.DataFrame,
    horizon: int,
) -> pd.DataFrame:
    """
    Create supervised training data for a specific forecast horizon.
    """
    supervised_df = (
        feature_df.copy()
        .sort_values(["city", "time"])
        .reset_index(drop=True)
    )

    supervised_df["target_time"] = (
        supervised_df.groupby("city")["time"].shift(-horizon)
    )

    for target in get_target_columns():
        supervised_df[f"{target}_target"] = (
            supervised_df.groupby("city")[target].shift(-horizon)
        )

    supervised_df = supervised_df.dropna().reset_index(drop=True)
    supervised_df = add_target_calendar_features(supervised_df)

    return supervised_df


def get_horizon_feature_columns() -> list[str]:
    """
    Return feature columns used for horizon-aware forecasting.
    """
    return get_feature_columns() + [
        "target_month",
        "target_day_of_month",
        "target_day_of_year",
        "target_day_sin",
        "target_day_cos",
    ]


def train_horizon_model(
    feature_df: pd.DataFrame,
    horizon: int,
) -> MultiOutputRegressor:
    """
    Train a multi-output regression model for one forecast horizon.
    """
    supervised_df = make_supervised(
        feature_df=feature_df,
        horizon=horizon,
    )

    feature_cols = get_horizon_feature_columns()
    target_cols = [f"{target}_target" for target in get_target_columns()]

    X_train = supervised_df[feature_cols]
    y_train = supervised_df[target_cols]

    model = MultiOutputRegressor(
        GradientBoostingRegressor(random_state=42)
    )

    model.fit(X_train, y_train)

    return model


def train_direct_horizon_models(
    feature_df: pd.DataFrame,
    horizons: list[int],
) -> dict[int, MultiOutputRegressor]:
    """
    Train one direct forecasting model per horizon.
    """
    trained_models = {}

    for horizon in horizons:
        print(f"Training GradientBoosting model for ML horizon={horizon}...")

        trained_models[horizon] = train_horizon_model(
            feature_df=feature_df,
            horizon=horizon,
        )

    return trained_models


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

    forecast_df["forecast_horizon"] = (
        forecast_df.groupby("city").cumcount() + 1
    )
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
    Build the latest feature row per city using historical data plus API forecast.

    The latest 7-day API forecast becomes the origin for ML days 8-28.
    """
    historical_raw = run_query("SELECT * FROM raw.historical").copy()
    forecast_raw = run_query("SELECT * FROM raw.forecast").copy()

    historical_raw["time"] = pd.to_datetime(historical_raw["time"])
    forecast_raw["time"] = pd.to_datetime(forecast_raw["time"])

    combined_df = pd.concat(
        [historical_raw, forecast_raw],
        ignore_index=True,
    )

    combined_df = (
        combined_df
        .sort_values(["city", "time"])
        .drop_duplicates(subset=["city", "time"], keep="last")
        .reset_index(drop=True)
    )

    future_feature_df, _ = build_features(combined_df)

    latest_origin = (
        future_feature_df
        .sort_values(["city", "time"])
        .groupby("city")
        .tail(1)
        .reset_index(drop=True)
    )

    return latest_origin


def predict_ml_days_8_to_28(
    horizon_models: dict[int, MultiOutputRegressor],
    latest_origin_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Predict forecast days 8-28 using direct horizon models.

    Horizon 1 after the API forecast corresponds to final forecast day 8.
    Horizon 21 after the API forecast corresponds to final forecast day 28.
    """
    feature_cols = get_horizon_feature_columns()
    target_cols = get_target_columns()

    rows = []

    for _, origin_row in latest_origin_df.iterrows():
        city = origin_row["city"]
        origin_time = pd.to_datetime(origin_row["time"])

        for ml_horizon, model in horizon_models.items():
            forecast_horizon = ml_horizon + FORECAST_DAYS
            target_time = origin_time + pd.Timedelta(days=ml_horizon)

            row = origin_row.copy()
            row["target_time"] = target_time

            row_df = pd.DataFrame([row])
            row_df = add_target_calendar_features(row_df)

            X_future = row_df[feature_cols]
            prediction = model.predict(X_future)[0]

            result = {
                "city": city,
                "origin_time": origin_time,
                "forecast_horizon": forecast_horizon,
                "target_time": target_time,
                "source": "ml_model",
            }

            for i, target in enumerate(target_cols):
                result[target] = prediction[i]

            rows.append(result)

    return pd.DataFrame(rows)


def build_final_28d_forecast(
    feature_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build the final hybrid 28-day forecast.

    Days 1-7 come from Open-Meteo API.
    Days 8-28 come from direct ML horizon models.
    """
    api_7d = prepare_api_forecast_output()
    latest_origin = prepare_latest_origin()

    ml_horizons = list(range(1, 22))

    horizon_models = train_direct_horizon_models(
        feature_df=feature_df,
        horizons=ml_horizons,
    )

    ml_21d = predict_ml_days_8_to_28(
        horizon_models=horizon_models,
        latest_origin_df=latest_origin,
    )

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


def run_pipeline(refresh_data: bool = True) -> dict[str, pd.DataFrame]:
    """
    Run the full weather intelligence pipeline.
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

    print("Step 4/7 — Validating raw data project scope...")
    run_raw_project_scope_gate()

    print("Step 5/7 — Cleaning data, running quality checks, and building model features...")
    feature_df = prepare_model_features()

    print("Step 6/7 — Training models and building final 28-day forecast...")
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