![logo_ironhack_blue 7](https://user-images.githubusercontent.com/23629340/40541063-a07a0a8a-601a-11e8-91b5-2f13e4e6b441.png)

# Project 1: Weather Intelligence Pipeline — Can We Trust This Data?

## Overview

Over the next two weeks you will build a complete, end-to-end **weather intelligence pipeline**. You will ingest historical and real-time weather data from the Open-Meteo API, store it in a local DuckDB analytical database, perform rigorous statistical analysis, and build a predictive model — all while critically evaluating whether the data can be trusted.

**Why weather?** Weather data is publicly available, refreshed daily, has genuine quality challenges (sensor gaps, interpolation artefacts, seasonal non-stationarity), and is the backbone of decisions in energy, agriculture, logistics, and insurance. It is the perfect playground for asking *"Can we trust this data?"*


## 🧪 Team Project Plan (Day 1)

### Problem Statement
Can we predict which cities in Azerbaijan will have good weather conditions for tourism during May–June?

The model will classify each day as suitable or not suitable for travel, allowing users to compare cities and choose the best destination without manually checking forecasts.

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

### Cities / Locations
- Baku, Azerbaijan (40.41, 49.87)
- Lankaran, Azerbaijan (38.75, 48.85)
- Gusar, Azerbaijan (41.42, 48.43)
- Guba, Azerbaijan (41.36, 48.51)
- Gabala, Azerbaijan (40.58, 47.50)
- Shaki, Azerbaijan (41.11, 47.10)
- Ganja, Azerbaijan (40.48, 46.21)

---

### Weather Variables

| Variable Name              | Unit  | Why it is relevant |
|---------------------------|------|-------------------|
| temperature_2m_max        | °C   | Determines if daytime conditions are suitable for activities like sightseeing, walking tours, and beach visits; very high temperatures can cause discomfort and limit outdoor time |
| temperature_2m_min        | °C   | Affects early morning and evening comfort, important for activities like dining outdoors, night walks, or early excursions |
| precipitation_sum         | mm   | Rain directly disrupts outdoor plans such as sightseeing, hiking, and city exploration; even light rain can reduce travel enjoyment |
| windspeed_10m_max         | m/s  | Strong winds can negatively affect activities like beach visits, boat trips, and outdoor dining; can also make walking uncomfortable |
| relative_humidity_2m_mean | %    | High humidity increases perceived temperature and discomfort, affecting physical activities like walking, hiking, and general outdoor exploration |
| shortwave_radiation_sum   | MJ/m²| Represents sunlight exposure; important for activities like sunbathing and photography, but excessive radiation can lead to overheating or sunburn |

---

### Methodology Outline

Week 1:
- Collect historical and forecast weather data
- Store data in DuckDB
- Clean and prepare dataset
- Perform feature engineering

Week 2:
- Conduct exploratory data analysis
- Perform statistical tests
- Build classification model
- Evaluate model performance

---

### Success Criteria

- Pipeline successfully collects and stores weather data
- Clean and structured dataset
- Meaningful visualizations and analysis
- Model correctly classifies good vs bad travel days
- Clear and interpretable results

## Project Requirements

| Week | Focus | Skills Applied |
|------|-------|----------------|
| **Week 1** (Days 1–5) | Data Engineering | Unit 2 — data sources, databases, ETL, pipelines |
| **Week 2** (Days 6–8 + Presentation) | Statistical Analysis & Prediction | Units 3 & 4.1 — descriptive stats, hypothesis testing, correlation, regression & classification modeling |

**You must:**

1. Choose **3 or more cities** (at least one should be Baku or another city relevant to you).
2. Ingest **at least 5 years** of daily historical weather data per city.
3. Ingest **real-time forecast data** (7-day forecast) for the same cities.
4. Store all data in a local **DuckDB** database with a well-designed schema.
5. Conduct **exploratory data analysis** with descriptive statistics and visualisations.
6. Formulate and execute **at least one formal hypothesis test** (more are encouraged if time permits).
7. Build **at least one statistical prediction model** (e.g., predict next-day temperature, rain probability, or seasonal anomaly) with confidence intervals.
8. Present findings on presentation day with a live pipeline demo.

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

## Getting Started

### 1. Fork & Clone

```bash
# Fork this repo on GitHub, then:
git clone https://github.com/<your-username>/m5-project-weather-pipeline.git
cd m5-project-weather-pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Verify the API

No API key needed. Test with:

```bash
curl "https://archive-api.open-meteo.com/v1/archive?latitude=40.41&longitude=49.87&start_date=2024-01-01&end_date=2024-01-07&daily=temperature_2m_max"
```

### 4. Start Day 1

Open [`daily-briefs/day-01-project-kickoff.md`](daily-briefs/day-01-project-kickoff.md) and follow the tasks.

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
├── src/                    # Your pipeline code
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

[Open-Meteo](https://open-meteo.com/) is a free, open-source weather API. **No API key required.**

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
