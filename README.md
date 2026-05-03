<img src="assets/logo.png" width="300"/>

## ClimaFit AI

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

###  Team & Responsibilities

- **Elvin:** Machine Learning & Pipeline  
- **Roya:** Data Quality & Preprocessing, Statistical Analysis & Hypothesis Testing  
- **Sama:** Data Quality support, Activity Recommendation Logic 
- **Jala:** Web Interface & Presentation  
- **Jala & Sama:** Project Ideation & Data Definition (initial project concept and selection of relevant variables)

---

### Technical Specifications

#### 1. Dataset History Length  
We use **6 years of historical daily weather data** (from 6 years ago up to the most recent available date) to capture long-term seasonal patterns and climate variability across Azerbaijan.

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
  Captures location-specific climate patterns across regions (Baku, Lankaran, Guba, Gabala, Shaki). City names are normalised to title-case throughout the pipeline.

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
## Feature Table

| Source | Feature Name | Unit | Aggregation |
|--------|-------------|------|-------------|
| Open-Meteo | apparent_temperature_max | °C | daily max |
| Open-Meteo | sunshine_duration | sec | daily sum |
| Derived | city_encoded | — | categorical encoding |
| Derived | month | — | extracted from date |
| Derived | day_of_month | — | extracted from date |
| Derived | temperature_2m_max_lag_1 | °C | 1-day lag |
| Derived | precipitation_sum_lag_1 | mm | 1-day lag |
| Derived | wind_speed_10m_max_lag_1 | km/h | 1-day lag |
| Derived | relative_humidity_2m_mean_lag_1 | % | 1-day lag |
| Derived | temperature_2m_max_3d_avg | °C | 3-day rolling mean |
| Derived | precipitation_sum_7d_sum | mm | 7-day rolling sum |
| Derived | wind_speed_10m_max_3d_avg | m/s | 3-day rolling mean |
| Derived | relative_humidity_2m_mean_7d_avg | % | 7-day rolling mean |

---
## Model Approach

We use historical data for model training and evaluation, applying a time-based split to ensure realistic performance assessment.

Additionally, we integrate **7-day forecast data from the API** for short-term activity suitability decisions.

For medium-range predictions, we use a **multi-output regression model (Gradient Boosting Regressor)** to predict all target variables simultaneously.

This approach allows us to:
- maintain a clean and unified forecasting pipeline  
- capture relationships between multiple weather variables  
- generate consistent inputs for downstream activity suitability decisions  

---

## System Flow

**1. Data Sources**
- Historical Weather Data (Open-Meteo API)
- 7-Day Forecast Data (Open-Meteo API)

**2. Data Processing**
- Data Cleaning & Validation
- Feature Engineering  
  - Lag features  
  - Rolling averages  
  - Calendar features  

**3. Modeling**
- Multi-output Regression Model (Gradient Boosting Regressor)

**4. Predictions**
- Predicted Weather Variables:
  - Temperature  
  - Precipitation  
  - Wind Speed  
  - Humidity  
  - Cloud Cover  

**5. AI Agent Layer**
- Activity suitability decision system

**6. Final Output**
- Activity suitability (Suitable / Risky / Not Suitable)
- Natural language explanation


## Project Timeline

| Date | Daily Activities |
|------|------------------|
| 20 Apr | Project setup, repository creation, explored Open-Meteo API, selected cities and weather variables |
| 21 Apr | Built ingestion module, fetched 5+ years of historical data and 7-day forecast, saved raw data as parquet |
| 22 Apr | Set up DuckDB database, created schemas, loaded raw data into database |
| 23 Apr | Performed data cleaning, checked missing values and data consistency |
| 24 Apr | Worked on data quality checks and initial feature engineering (lags, basic flags, date features) |
| 25 Apr | Prepared model-ready dataset and stored it in DuckDB; initial website development started |
| 26 Apr | Started exploratory data analysis (EDA), analyzed distributions and trends for May–June |
| 27 Apr | Started model development and initial training; began website development (in progress) |
| 28 Apr | Continued model work and started pipeline development (pipeline orchestration in progress) |
| 29 Apr | Planned: model improvement and evaluation; continue website development |
| 30 Apr | Planned: final integration, presentation preparation, and project submission |

## Repository Structure

```
m5-project-weather-pipeline/
├── README.md
├── requirements.txt
├── .gitignore

├── src/
│   ├── init.py
│   ├── config.py
│   ├── ingestion.py
│   ├── db.py      ← DuckDB connection & queries
│   ├── cleaning.py
│   ├── features.py
│   ├── quality_checks.py
│   └── pipeline.py

├── notebooks/
│   ├── 01_data_ingestion.ipynb
│   ├── 02_data_quality_checks.ipynb
│   ├── 03_eda_and_trends.ipynb
│   └── 04_modeling.ipynb

├── data/
│   ├── weather.duckdb    ←  MAIN DATABASE
│   └── raw/
│       ├── historical/
│       └── forecast/

├── reports/
│   ├── figures/
│   └── final_report.md

└── logs/
```




