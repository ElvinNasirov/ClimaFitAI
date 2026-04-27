![logo_ironhack_blue 7](https://user-images.githubusercontent.com/23629340/40541063-a07a0a8a-601a-11e8-91b5-2f13e4e6b441.png)

# ClimaFit AI

## Team: The Outliers 

## Tourism Weather Risk & Activity Suitability

### Project Overview

This project provides data-driven weather insights to support tourism planning and decision-making. It helps travel agencies and tour operators evaluate the feasibility of outdoor activities based on both historical patterns and forecasted conditions.

The main objectives are:
- Minimize cancellation risks for tour operators  
- Enhance customer satisfaction by recommending optimal dates for specific activities  
- Provide AI-driven explanations for weather-based travel recommendations  

---
### Data Sources
We use the Open-Meteo API:

- Historical data:
https://archive-api.open-meteo.com/v1/archive

- Forecast data:
https://api.open-meteo.com/v1/forecast

Parameters:
- latitude, longitude
- start_date, end_date
- daily weather variables

---
### Team & Responsibilities

- **Jala (Data Engineer):** Ingestion pipeline, DuckDB management, and automation  
- **Roya (Team Manager & Data Quality):** Task coordination, data validation, and cleaning logic  
- **Sema (Data Analyst):** Exploratory Data Analysis (EDA), statistical testing, and tourism insights  
- **Elvin (ML Engineer):** Feature engineering, model training, and AI suitability logic  

---

### Technical Specifications

#### 1. Dataset History Length  
We use **5+ years of historical daily weather data (2021 up to the most recent available date)** to capture long-term seasonal patterns and climate variability across Azerbaijan.

#### 2. Dataset Granularity  
The data is processed at a **daily granularity**, where each record represents a 24-hour summary of weather conditions for a specific city.

#### 3. Prediction Horizon  
The project targets **medium-range forecasting (up to 28 days)**, aligning with tourism planning and booking cycles.

---

## Targets

We use a **multi-output regression model** to predict key weather variables required for activity suitability decisions.

- **temperature_2m_max**  
  Predicts peak daily temperature, which is critical for outdoor comfort and heat-related constraints.

- **precipitation_sum**  
  Measures total daily rainfall, the most important factor for determining whether outdoor activities are feasible.

- **wind_speed_10m_max**  
  Captures maximum wind conditions, essential for safety-sensitive activities such as cable car and boating.

- **relative_humidity_2m_mean**  
  Represents average humidity, which affects perceived temperature and overall comfort.

- **cloud_cover_mean**  
  Indicates sky conditions and visibility, useful for assessing atmosphere (e.g., sunny vs gloomy) and fog likelihood.

---

## Features

### Core & Derived Weather Features

- **apparent_temperature_max**  
  Represents perceived temperature (“feels like”), providing additional context for human comfort.

- **sunshine_duration**  
  Reflects how sunny a day is, providing additional context for outdoor activity quality and user experience.

- **city (encoded)**  
  Captures location-specific climate patterns across regions (Baku, Lankaran, Guba, Gabala, Shaki).

---

### Calendar Features

- **month**  
  Helps the model capture seasonal patterns within the May–June period.

- **day_of_month**  
  Provides finer time progression within each month, allowing the model to learn gradual weather changes.

---

### Lag Features (Previous Day Signals)

- **temperature_lag_1**  
  Yesterday’s temperature, used to model short-term continuity in temperature trends.

- **precipitation_lag_1**  
  Previous day’s rainfall, helping capture ongoing rain patterns.

- **wind_lag_1**  
  Yesterday’s wind speed, useful for modeling wind persistence.

- **humidity_lag_1**  
  Previous day’s humidity, supporting short-term atmospheric continuity.

---

### Rolling Features (Short-Term Trends)

- **temperature_3d_avg**  
  3-day average temperature, capturing recent warming or cooling trends.

- **precipitation_7d_sum**  
  Total rainfall over the past 7 days, indicating prolonged wet conditions.

- **wind_3d_avg**  
  3-day average wind speed, smoothing short-term fluctuations.

- **humidity_7d_avg**  
  7-day average humidity, reflecting persistent atmospheric conditions.

---
## 🧾 Feature Definitions

| Source      | Feature Name               | Unit | Description                  |
|------------|----------------------------|------|------------------------------|
| Open-Meteo | apparent_temperature_max   | °C   | perceived temperature        |
| Open-Meteo | sunshine_duration          | sec  | total daily sunlight         |
| Derived    | city (encoded)             | —    | location identifier          |
| Derived    | month                      | —    | month of observation         |
| Derived    | day_of_month               | —    | day within month             |
| Derived    | temperature_lag_1          | °C   | previous day temperature     |
| Derived    | precipitation_lag_1        | mm   | previous day rainfall        |
| Derived    | wind_lag_1                 | m/s  | previous day wind            |
| Derived    | humidity_lag_1             | %    | previous day humidity        |
| Derived    | temperature_3d_avg         | °C   | 3-day rolling avg temp       |
| Derived    | precipitation_7d_sum       | mm   | 7-day rainfall sum           |
| Derived    | wind_3d_avg                | m/s  | 3-day rolling avg wind       |
| Derived    | humidity_7d_avg            | %    | 7-day rolling avg humidity   |

---
## Model Approach

We use historical data for model training and evaluation, applying a time-based split to ensure realistic performance assessment.

Additionally, we integrate **7-day forecast data from the API** for short-term activity suitability decisions.

For medium-range predictions, we use a **multi-output regression model (Random Forest Regressor)** to predict all target variables simultaneously.

This approach allows us to:
- maintain a clean and unified forecasting pipeline  
- capture relationships between multiple weather variables  
- generate consistent inputs for downstream activity suitability decisions  

---

## System Flow

Historical Weather Data (Open-Meteo API)
+
7-Day Forecast Data (API)
↓
Data Cleaning & Validation
↓
Feature Engineering
(lag features, rolling averages, calendar features)
↓
Multi-output Regression Model
(Random Forest Regressor)
↓
Predicted Weather Variables
(temperature, precipitation, wind, humidity, cloud cover)
↓
AI Agent (Activity Suitability Layer)
↓
Final Output:

* Activity suitability (Suitable / Risky / Not Suitable)
* Natural language explanation

## Timeline

Each day has a brief in the [`daily-briefs/`](daily-briefs/) folder with detailed tasks, deliverables, and resources. Submit a Pull Request at the end of each day showing your incremental progress.

| Day | Date | Brief | Focus |
|-----|------|-------|-------|
| 1 | 20 Apr | [Project Kick-Off & API Exploration](daily-briefs/day-01-project-kickoff.md) | Repo setup, API exploration, city/variable selection, project plan |
| 2 | 21 Apr | [Data Ingestion Pipeline](daily-briefs/day-02-data-ingestion.md) | Ingestion module, config, full historical fetch, data audit |
| 3 | 22 Apr | [Database Design & Data Loading](daily-briefs/day-03-database-design.md) | DuckDB schema, loading functions, validation queries |
| 4 | 23 Apr | [Data Cleaning & Feature Engineering](daily-briefs/day-04-data-cleaning.md) | Quality assessment, cleaning pipeline, feature engineering, quality report |
| 5 | 24 Apr | [Pipeline Automation & Data Quality](daily-briefs/day-05-pipeline-automation.md) | Orchestrator, incremental loading, quality gates, logging |
| 6 | 27 Apr | [Exploratory Data Analysis](daily-briefs/day-06-eda.md) | Descriptive stats, distributions, time series, cross-city comparison |
| 7 | 28 Apr | [Statistical Analysis & Feature Selection](daily-briefs/day-07-statistical-analysis.md) | Hypothesis testing, correlation, feature selection |
| 8 | 29 Apr | [Predictive Modeling & Evaluation](daily-briefs/day-08-predictive-modeling.md) | 2+ models, train/test, confidence intervals, residual diagnostics |
| — | 30 Apr | [Final Presentation](daily-briefs/day-09-final-presentation.md) | 10 min presentation, live demo, project submission |

## Repository Structure

```
m5-project-weather-pipeline/
├── README.md               # This file — project overview
├── requirements.txt        # Python dependencies
├── .gitignore
├── daily-briefs/           # Day-by-day task briefs (read-only reference)
│   ├── day-01-project-kickoff.md
│   ├── ...
│   └── day-09-final-presentation.md
├── src/                    # Pipeline code
│   ├── __init__.py
│   ├── ingestion.py        # Day 2
│   ├── config.py           # Day 2
│   ├── database.py         # Day 3
│   ├── cleaning.py         # Day 4
│   ├── features.py         # Day 4
│   ├── quality_checks.py   # Day 5
│   └── pipeline.py         # Day 5
├── notebooks/              # Daily Jupyter notebooks
│   ├── day_01_exploration.ipynb
│   ├── day_02_ingestion.ipynb
│   ├── ...
│   └── day_08_modeling.ipynb
├── data/
│   └── raw/                # Raw API data (gitignored)
├── reports/
│   ├── figures/            # Saved visualisations
│   └── data_quality_report.md
└── logs/                   # Pipeline logs (gitignored)
```

## Open-Meteo API Reference

**Historical weather endpoint:**

```
https://archive-api.open-meteo.com/v1/archive?latitude=40.41&longitude=49.87&start_date=2020-01-01&end_date=2024-12-31&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max
```

**Forecast endpoint:**

```
https://api.open-meteo.com/v1/forecast?latitude=40.41&longitude=49.87&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max
```

Key daily variables: `temperature_2m_max`, `temperature_2m_min`, `temperature_2m_mean`, `precipitation_sum`, `rain_sum`, `snowfall_sum`, `windspeed_10m_max`, `relative_humidity_2m_mean`, `apparent_temperature_max`, `weathercode`.

## Evaluation Criteria

| Criterion | Weight | Description |
|-----------|--------|-------------|
| Pipeline completeness | 20% | End-to-end from API to database, automated, with quality checks |
| Data quality analysis | 15% | Thorough assessment, documented issues, justified decisions |
| Statistical rigour | 20% | Proper hypothesis testing, assumption checking, effect sizes |
| Prediction model | 20% | Appropriate model selection, evaluation, confidence intervals |
| Presentation quality | 15% | Clear narrative, good visuals, effective demo |
| Code quality | 10% | Clean, modular, documented, reproducible |

## Resources

- [Open-Meteo API Documentation](https://open-meteo.com/en/docs)
- [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
- [DuckDB Python Documentation](https://duckdb.org/docs/api/python/overview)
- [scipy.stats documentation](https://docs.scipy.org/doc/scipy/reference/stats.html)
- [statsmodels documentation](https://www.statsmodels.org/stable/index.html)
- [Seaborn gallery](https://seaborn.pydata.org/examples/index.html)
