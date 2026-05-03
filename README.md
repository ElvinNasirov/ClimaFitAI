# ClimaFitAI

## Team: The Outliers

## Tourism Weather Risk & Activity Suitability

### Project Overview

ClimaFitAI is a weather intelligence pipeline that translates historical and forecast weather data into tourism-oriented weather risk signals and activity suitability decisions.

The business problem is practical: travel agencies and tour operators often sell activities in advance, but rain, wind, heat, humidity, poor sunshine, or poor visibility can force last-minute cancellations, refunds, itinerary changes, and lower customer satisfaction. ClimaFitAI helps planners evaluate weather-sensitive activities earlier and choose better dates, safer alternatives, or more suitable city-activity combinations.

---

## Project Objectives

- Build a reproducible end-to-end weather data pipeline.
- Collect historical and forecast weather data from Open-Meteo.
- Store raw and analytics-ready data in DuckDB.
- Validate data completeness, city coverage, date continuity, duplicates, and weather ranges.
- Engineer seasonal, lag, rolling, trend, city, and comfort-related features.
- Train and evaluate multi-output regression models for medium-range weather forecasting.
- Produce a final hybrid 28-day forecast:
  - days 1–7 from the Open-Meteo Forecast API,
  - days 8–28 from direct horizon ML models.
- Convert forecasted weather variables into activity suitability decisions for tourism use cases.

---

## Business Use Case

Tourism companies need to plan activities before the exact travel date. A mountain hike, cable car trip, boat tour, beach day, city walk, or museum visit can all depend on different weather conditions.

ClimaFitAI supports this planning process by answering questions such as:

- Is this city suitable for outdoor tourism on a selected date?
- Is rain, wind, heat, humidity, cloud cover, or low sunshine likely to increase operational risk?
- Should the agency keep the original activity or suggest a safer alternative?
- Which city and activity combination looks more suitable for the booking window?

The system is not intended to replace official weather forecasts. It is designed as a planning-support layer that converts weather data into tourism-oriented risk and suitability signals.

---

## Cities Covered

The current project scope includes five Azerbaijan tourism destinations:

| City     | Tourism relevance                                        |
| -------- | -------------------------------------------------------- |
| Baku     | city walks, seaside activities, museums, Old City        |
| Lankaran | coastal tourism, nature parks, tea plantations           |
| Guba     | mountain villages, waterfalls, hiking, forest activities |
| Gabala   | cable car, mountain resorts, Nohur Lake, hiking          |
| Shaki    | cultural tourism, historical sites, old town walks       |

---

## Data Sources

The project uses the Open-Meteo API.

### Historical weather data

```text
https://archive-api.open-meteo.com/v1/archive
```

### Forecast weather data

```text
https://api.open-meteo.com/v1/forecast
```

### API parameters

- latitude
- longitude
- start_date
- end_date
- daily weather variables
- timezone = `auto`
- wind_speed_unit = `kmh`

`timezone="auto"` allows Open-Meteo to resolve the local timezone from the city coordinates. Since all selected cities are in Azerbaijan, the daily records are aligned to Azerbaijan local time.

`wind_speed_unit="kmh"` is used to make wind speed units explicit. The project reports `wind_speed_10m_max` in kilometers per hour.

---

## Technical Specifications

### Historical window

The production pipeline is configured to use a **rolling 6-year daily historical window**, ending at the most recent complete day. This gives the model enough historical seasonality while keeping the data recent.

### Forecast window

The Open-Meteo Forecast API provides the first **7 forecast days**. These API forecast rows are used directly in the final output because short-range forecast data is more reliable than model-generated estimates.

### Final prediction horizon

The final system produces a **28-day forecast**:

| Forecast days | Source                   |
| ------------: | ------------------------ |
|           1–7 | Open-Meteo Forecast API  |
|          8–28 | ML direct horizon models |

### Granularity

The data is processed at **daily granularity**. Each row represents one city and one calendar day.

---

## Target Variables

The model predicts five weather variables required for activity suitability decisions.

| Target                      | Unit | Why it matters                                                                   |
| --------------------------- | ---: | -------------------------------------------------------------------------------- |
| `temperature_2m_max`        |   °C | Outdoor comfort, heat risk, seasonal suitability                                 |
| `precipitation_sum`         |   mm | Rain risk, cancellation risk, outdoor feasibility                                |
| `wind_speed_10m_max`        | km/h | Safety risk for boating, cable cars, mountain activities, exposed outdoor routes |
| `relative_humidity_2m_mean` |    % | Perceived comfort, heat stress, humid-weather discomfort                         |
| `cloud_cover_mean`          |    % | Visibility, atmosphere, sunshine quality, gloomy-weather risk                    |

---

## Feature Engineering

The model-ready table combines raw Open-Meteo variables with derived features that represent seasonality, recent weather memory, city differences, comfort, and short-term trends.

### Core weather and comfort features

| Feature                    |  Unit | Description                                                     |
| -------------------------- | ----: | --------------------------------------------------------------- |
| `apparent_temperature_max` |    °C | Perceived maximum temperature; useful for comfort and heat risk |
| `sunshine_duration`        |   sec | Daily sunshine duration                                         |
| `comfort_gap`              |    °C | Difference between apparent and actual maximum temperature      |
| `sunshine_ratio`           | ratio | Share of the day with sunshine                                  |

### City and calendar features

| Feature        | Unit | Description                                            |
| -------------- | ---: | ------------------------------------------------------ |
| `city_encoded` |    — | Encoded city identifier for location-specific patterns |
| `month`        |    — | Calendar month                                         |
| `day_of_month` |    — | Day within the month                                   |
| `day_sin`      |    — | Cyclical day-of-year sine feature                      |
| `day_cos`      |    — | Cyclical day-of-year cosine feature                    |

### Lag features

Lag features allow the model to use recent weather state when predicting future conditions.

| Feature group      | Examples                                                            | Description                         |
| ------------------ | ------------------------------------------------------------------- | ----------------------------------- |
| Temperature lags   | `temperature_lag_1`, `temperature_lag_3`, `temperature_lag_7`       | Previous temperature values by city |
| Precipitation lags | `precipitation_lag_1`, `precipitation_lag_3`, `precipitation_lag_7` | Previous rainfall values by city    |
| Wind lags          | `wind_lag_1`, `wind_lag_3`                                          | Previous wind speed values by city  |
| Humidity lags      | `humidity_lag_1`, `humidity_lag_3`                                  | Previous humidity values by city    |

### Rolling features

Rolling features smooth short-term fluctuations and capture recent weather trends without using the current target day.

| Feature                 | Unit | Description                                 |
| ----------------------- | ---: | ------------------------------------------- |
| `temperature_3d_avg`    |   °C | Previous 3-day average temperature          |
| `temperature_7d_avg`    |   °C | Previous 7-day average temperature          |
| `temperature_14d_avg`   |   °C | Previous 14-day average temperature         |
| `precipitation_3d_sum`  |   mm | Previous 3-day rainfall sum                 |
| `precipitation_7d_sum`  |   mm | Previous 7-day rainfall sum                 |
| `precipitation_14d_sum` |   mm | Previous 14-day rainfall sum                |
| `wind_3d_avg`           | km/h | Previous 3-day average wind speed           |
| `wind_7d_avg`           | km/h | Previous 7-day average wind speed           |
| `humidity_7d_avg`       |    % | Previous 7-day average humidity             |
| `humidity_14d_avg`      |    % | Previous 14-day average humidity            |
| `cloud_cover_7d_avg`    |    % | Previous 7-day average cloud cover          |
| `rainy_days_7d`         | days | Number of rainy days in the previous 7 days |

### Trend features

| Feature                  |              Unit | Description                  |
| ------------------------ | ----------------: | ---------------------------- |
| `temperature_trend_1d`   |                °C | One-day temperature change   |
| `humidity_trend_1d`      | percentage points | One-day humidity change      |
| `wind_trend_1d`          |              km/h | One-day wind speed change    |
| `precipitation_trend_1d` |                mm | One-day precipitation change |

---

## Modeling Approach

The project uses a **multi-output regression** setup because activity suitability depends on several weather variables at the same time. Predicting temperature, precipitation, wind, humidity, and cloud cover together creates one consistent weather scenario for downstream decision logic.

### Evaluation strategy

The modeling notebook uses a time-based backtesting strategy. This is important because weather forecasting is chronological: the model must learn from the past and predict future dates.

Candidate models include:

- Dummy baseline
- Ridge regression
- Random Forest
- Extra Trees
- Gradient Boosting
- XGBoost

The final selected model for the 28-day ML extension is:

```text
GradientBoostingRegressor wrapped in MultiOutputRegressor
```

### Why direct horizon models?

The pipeline trains separate direct models for future horizons used after the API forecast window. This avoids recursively feeding model predictions back into the model and keeps horizon-specific behavior explicit.

Final forecast strategy:

```text
Days 1–7  → Open-Meteo Forecast API
Days 8–28 → ML direct horizon models
```

---

## Model Evaluation Summary

The 28-day backtest shows that model performance differs by weather target.

| Target                      |     R² | Interpretation                                             |
| --------------------------- | -----: | ---------------------------------------------------------- |
| `temperature_2m_max`        |  0.768 | Strong; seasonal and persistent patterns are captured well |
| `precipitation_sum`         | -0.016 | Weak; rainfall is irregular and event-driven               |
| `wind_speed_10m_max`        |  0.112 | Limited but still useful as a broad wind-risk signal       |
| `relative_humidity_2m_mean` |  0.357 | Moderate; useful for comfort-risk estimation               |
| `cloud_cover_mean`          |  0.182 | Difficult but still useful as a broad sky-condition signal |

The model is strongest for temperature and weaker for precipitation and cloud cover. This is expected because rainfall and sky conditions are more event-driven and harder to predict at a 28-day horizon. For tourism planning, the output should be interpreted as a **medium-range risk signal**, not as an exact meteorological forecast.

---

## Data Quality Checks

The project includes automated quality checks before modeling.

Checked dimensions:

- missing values
- duplicate rows
- duplicate city-date records
- city coverage
- date coverage
- missing dates
- column consistency between historical and forecast data
- realistic weather value ranges

The pipeline stops if critical quality checks fail. This protects the model from training on incomplete, duplicated, out-of-scope, or physically unrealistic data.

---

## Statistical Analysis

The EDA and statistical analysis support the feature engineering and modeling decisions.

Key methods:

- distribution analysis by city
- correlation matrix
- Spearman correlation for non-linear or non-normal relationships
- normality checks
- variance checks
- Kruskal-Wallis tests for city-level differences
- Mann-Whitney post-hoc comparisons
- Benjamini-Hochberg correction for multiple testing
- effect-size interpretation

Key conclusion:

Weather conditions differ by city and by season. This supports the use of city encoding, calendar features, lag features, rolling features, and comfort-related features in the forecasting model.

---

## Activity Suitability Logic

The final forecast feeds a rule-based activity suitability layer. The logic evaluates forecasted weather variables and classifies conditions into tourism-relevant categories such as:

- indoor activity preference
- hot-weather activity preference
- perfect outdoor conditions
- cool-weather activity preference
- mixed / manageable conditions

The decision logic considers variables such as precipitation, wind, apparent temperature, humidity, cloud cover, and sunshine duration. City-specific suggestions are then selected based on the activity category.

---

## System Flow

```text
Open-Meteo API
→ raw parquet files
→ DuckDB raw tables
→ raw project-scope gate
→ cleaning
→ quality gate
→ feature engineering
→ analytics.model_features
→ direct horizon ML models
→ analytics.final_28d_forecast
→ activity suitability logic / website
```

Main DuckDB tables:

| Table                          | Purpose                                            |
| ------------------------------ | -------------------------------------------------- |
| `raw.historical`               | Raw historical daily weather data                  |
| `raw.forecast`                 | Raw 7-day forecast data                            |
| `analytics.model_features`     | Cleaned and feature-engineered model-ready dataset |
| `analytics.final_28d_forecast` | Final hybrid 28-day city-level forecast            |

---

## Repository Structure

```text
m5-project-weather-pipeline/
├── .github/
│   └── workflows/
│       └── weather_pipeline.yml
├── README.md
├── requirements.txt
├── .gitignore
├── assets/
│   └── logo.png
├── daily-briefs/
│   ├── day-01-project-kickoff.md
│   ├── day-02-data-ingestion.md
│   ├── day-03-database-design.md
│   ├── day-04-data-cleaning.md
│   ├── day-06-eda.md
│   ├── day-07-statistical-analysis.md
│   ├── day-08-predictive-modeling.md
│   └── day-09-final-presentation.md
├── data/
│   ├── raw/
│   │   └── .gitkeep
│   └── processed/
│       └── .gitkeep
├── notebooks/
│   ├── 01_data_ingestion.ipynb
│   ├── 02_data_quality_checks.ipynb
│   ├── 03_eda_and_trends.ipynb
│   ├── 04_modeling.ipynb
│   └── 05_pipeline.ipynb
├── reports/
│   ├── final_report.md
│   └── figures/
│       ├── feature_group_ablation_28d.png
│       ├── feature_importance_28d.png
│       ├── final_28d_forecast_baku_temperature_2m_max.png
│       ├── final_28d_forecast_guba_temperature_2m_max.png
│       ├── may_june_tourism_risk_by_city.png
│       ├── model_error_by_horizon.png
│       ├── model_rmse_by_target_28d.png
│       ├── monthly_temperature_by_city.png
│       ├── temperature_rmse_by_city_28d.png
│       ├── weather_correlation_matrix.png
│       └── weather_distribution_by_city.png
├── run_pipeline.bat
├── setup_and_run.bat
├── run_pipeline.sh
├── setup_and_run.sh
└── src/
    ├── __init__.py
    ├── config.py
    ├── ingestion.py
    ├── db.py
    ├── cleaning.py
    ├── quality_checks.py
    ├── features.py
    ├── pipeline.py
    └── weather_logic.py
```

Data files and DuckDB database artifacts are generated locally and are not committed to the repository.

---

## Team & Responsibilities

| Team member     | Main responsibility                                                                                                                  |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| **Elvin**       | ML & Pipeline Lead: horizon-aware modeling, model evaluation, pipeline orchestration, GitHub Actions validation, source-code cleanup |
| **Roya**        | Data quality and preprocessing, statistical analysis, hypothesis testing                                                             |
| **Sama**        | Data quality support, activity recommendation logic                                                                                  |
| **Jala**        | Web interface and presentation                                                                                                       |
| **Jala & Sama** | Project ideation and data definition, including initial concept and variable selection                                               |

---

## Project Timeline

| Date   | Work completed                                                                                        |
| ------ | ----------------------------------------------------------------------------------------------------- |
| 20 Apr | Project setup, repository creation, Open-Meteo API exploration, selected cities and weather variables |
| 21 Apr | Built ingestion module, fetched historical and forecast data, saved raw data as parquet               |
| 22 Apr | Set up DuckDB database, created schemas, loaded raw data into database                                |
| 23 Apr | Performed cleaning, missing-value checks, duplicate checks, and consistency checks                    |
| 24 Apr | Built data quality checks and initial feature engineering                                             |
| 25 Apr | Prepared model-ready dataset and stored it in DuckDB                                                  |
| 26 Apr | Completed EDA for weather distributions, trends, and city-level patterns                              |
| 27 Apr | Started modeling and activity suitability integration                                                 |
| 28 Apr | Developed pipeline orchestration and model evaluation workflow                                        |
| 29 Apr | Improved model evaluation, horizon experiments, feature importance, and ablation analysis             |
| 30 Apr | Final integration, GitHub Actions validation, figures, README, and presentation preparation           |

---

## How to Run the Project

The project is designed to run on Windows, macOS, and Linux.

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
```

Windows PowerShell / Command Prompt:

```bat
.venv\Scripts\activate
```

macOS / Linux:

```bash
source .venv/bin/activate
```

### 2. Install dependencies

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Run the full pipeline

Use module execution from the repository root:

```bash
python -m src.pipeline
```

This command is cross-platform and avoids import-path issues that can happen with direct script execution such as `python src/pipeline.py`.

The pipeline will:

1. create DuckDB schemas,
2. fetch fresh historical and forecast data,
3. save raw parquet files,
4. load raw data into DuckDB,
5. run project-scope validation,
6. clean and validate historical data,
7. build model features,
8. train direct horizon models,
9. create `analytics.final_28d_forecast`.

### 4. Optional Windows shortcuts

```bat
setup_and_run.bat
```

or

```bat
run_pipeline.bat
```

### 5. Optional macOS / Linux shortcuts

```bash
chmod +x setup_and_run.sh run_pipeline.sh
./setup_and_run.sh
```

or, after the environment is already set up:

```bash
./run_pipeline.sh
```

### 6. Run notebooks

Start Jupyter from the repository root:

```bash
jupyter notebook
```

Then open notebooks in this order:

```text
01_data_ingestion.ipynb
02_data_quality_checks.ipynb
03_eda_and_trends.ipynb
04_modeling.ipynb
05_pipeline.ipynb
```

---

## Automation

The repository includes a GitHub Actions workflow:

```text
.github/workflows/weather_pipeline.yml
```

The workflow:

- installs dependencies,
- runs the full pipeline on Ubuntu,
- validates that the final forecast table contains 140 rows:

```text
5 cities × 28 forecast days = 140 rows
```

It also uploads the generated DuckDB database as a workflow artifact.

---

## Limitations and Next Steps

- The model is stronger for temperature than for precipitation, wind, and cloud cover.
- Daily precipitation is irregular and difficult to predict at medium range.
- The system uses daily aggregates, not hourly forecasts.
- The current scope covers five cities in Azerbaijan.
- Activity suitability rules are designed for planning support and should be calibrated further with real tourism operator feedback.
- Future work can add formal unit tests, model persistence, more cities, and improved precipitation risk classification.

---

## Final Project Summary

ClimaFitAI demonstrates a complete weather intelligence workflow for tourism planning:

```text
weather data → validation → feature engineering → forecasting → activity suitability
```

The strongest parts of the project are the reproducible pipeline, DuckDB storage design, automated quality gates, rich feature engineering, time-based model evaluation, direct horizon forecasting, and hybrid 28-day forecast strategy. The system gives tourism planners an earlier view of weather risk so they can reduce last-minute cancellations and choose more suitable activities for each city and date.

The project should be evaluated as a practical planning-support system, not as a perfect meteorological forecast. Its strongest predictive target is temperature, while precipitation and cloud cover remain harder because they are more event-driven. This limitation is explicitly handled in the project interpretation and makes the final output more realistic and defensible.
