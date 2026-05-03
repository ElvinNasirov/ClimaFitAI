# Final Report — ClimaFitAI

## Tourism Weather Risk & Activity Suitability Platform

**Team:** The Outliers  
**Project:** Weather Intelligence Pipeline — Tourism Weather Risk & Activity Suitability  
**Main use case:** Medium-range weather planning for tourism activities in Azerbaijan

---

## 1. Executive Summary

ClimaFitAI is an end-to-end weather intelligence pipeline that helps tourism companies evaluate weather risk and activity suitability before the travel date. The project focuses on five Azerbaijan tourism cities: Baku, Lankaran, Guba, Gabala, and Shaki.

The system collects historical and forecast weather data from Open-Meteo, validates the data, stores it in DuckDB, engineers forecasting features, trains multi-output regression models, and produces a hybrid 28-day forecast. The final forecast supports downstream activity suitability decisions, such as whether outdoor activities are appropriate or whether indoor or lower-risk alternatives should be recommended.

The final forecast strategy is:

```text
Days 1–7  → Open-Meteo Forecast API
Days 8–28 → ML direct horizon models
```

This design fits the business problem: travel agencies often sell activities in advance, but weather can create cancellation, refund, safety, and customer satisfaction risks near the trip date.

---

## 2. Business Problem

Tourism agencies and tour operators need to plan activities ahead of time. However, many tourism activities are weather-sensitive:

- mountain hikes depend on rain, wind, visibility, and temperature;
- beach and seaside activities depend on temperature, sunshine, wind, and precipitation;
- cable car and boating activities are sensitive to wind;
- city walks depend on heat, rain, humidity, and general comfort;
- cultural and indoor activities can be used as alternatives during risky weather.

The business issue is that agencies may collect bookings in advance, but poor weather close to the trip date can force last-minute cancellations or itinerary changes. This can lead to refunds, operational problems, and lower customer satisfaction.

ClimaFitAI addresses this by converting weather forecasts and historical patterns into planning signals that help agencies choose better activity-date-city combinations earlier.

---

## 3. Project Scope

### Cities

The project covers five selected Azerbaijan cities:

| City     | Tourism relevance                                        |
| -------- | -------------------------------------------------------- |
| Baku     | city walks, seaside activities, museums, Old City        |
| Lankaran | coastal tourism, nature parks, tea plantations           |
| Guba     | mountain villages, waterfalls, hiking, forest activities |
| Gabala   | cable car, mountain resorts, Nohur Lake, hiking          |
| Shaki    | cultural tourism, historical sites, old town walks       |

### Forecast horizon

The project produces a 28-day city-level weather forecast.

|   Horizon | Source                   |
| --------: | ------------------------ |
|  Days 1–7 | Open-Meteo Forecast API  |
| Days 8–28 | ML direct horizon models |

### Data granularity

The system works with daily weather data. Each row represents one city and one date.

---

## 4. Data Source

The project uses the Open-Meteo API.

### Historical endpoint

```text
https://archive-api.open-meteo.com/v1/archive
```

### Forecast endpoint

```text
https://api.open-meteo.com/v1/forecast
```

### Daily weather variables

The pipeline collects the following daily variables:

| Variable                    |    Unit | Role    |
| --------------------------- | ------: | ------- |
| `temperature_2m_max`        |      °C | target  |
| `precipitation_sum`         |      mm | target  |
| `wind_speed_10m_max`        |    km/h | target  |
| `relative_humidity_2m_mean` |       % | target  |
| `cloud_cover_mean`          |       % | target  |
| `apparent_temperature_max`  |      °C | feature |
| `sunshine_duration`         | seconds | feature |

The pipeline uses `timezone="auto"`, so Open-Meteo resolves local daily records based on the city coordinates. For the current project cities, this corresponds to Azerbaijan local time.

---

## 5. Pipeline Architecture

The project is implemented as a reproducible data and ML pipeline.

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

### Main DuckDB tables

| Table                          | Description                                         |
| ------------------------------ | --------------------------------------------------- |
| `raw.historical`               | Raw historical daily weather data from Open-Meteo   |
| `raw.forecast`                 | Raw 7-day forecast data from Open-Meteo             |
| `analytics.model_features`     | Cleaned and feature-engineered dataset for modeling |
| `analytics.final_28d_forecast` | Final hybrid 28-day forecast                        |

### Pipeline reliability controls

The pipeline includes two important gates:

1. **Raw project-scope gate**
   - checks required columns,
   - validates selected cities,
   - validates expected date range,
   - validates expected rows per city.

2. **Clean data quality gate**
   - checks missing values,
   - duplicate rows,
   - duplicate city-date records,
   - missing dates,
   - realistic weather ranges.

If critical checks fail, the pipeline stops before feature engineering or modeling.

---

## 6. Data Quality Analysis

The data quality notebook validates both historical and forecast data before modeling.

### Checks performed

- missing values,
- duplicate rows,
- duplicate city-date combinations,
- city coverage,
- date coverage,
- missing dates,
- column consistency between historical and forecast datasets,
- realistic weather value ranges.

### Key findings

The checked data passed the main quality checks:

- no missing values were detected in the selected weather columns;
- no duplicate rows were found;
- no duplicate city-date records were found;
- each selected city had complete daily coverage in the checked historical window;
- forecast data contained 7 rows per city;
- historical and forecast datasets used consistent columns;
- all selected weather variables were within realistic physical ranges.

### Interpretation

This means the project can trust the dataset structurally: it is complete, consistent, non-duplicated, and within expected weather ranges. The project does not claim to validate sensor-level meteorological accuracy; instead, it validates whether the data is usable and reliable for the project pipeline and modeling workflow.

---

## 7. Exploratory Data Analysis

EDA was used to understand how weather conditions vary across cities and seasons.

### Main EDA findings

- Temperature patterns show strong seasonal behavior.
- Cities differ in temperature, precipitation, wind, humidity, and cloud cover.
- May–June is a useful window for tourism analysis because weather conditions become more relevant for outdoor activities.
- Sunshine and apparent temperature are important comfort-related signals.
- Rainfall and cloud cover are more irregular than temperature.

### Visual outputs

The project includes visualizations in `reports/figures/`, including:

- monthly temperature by city,
- weather distributions by city,
- weather correlation matrix,
- May–June tourism risk by city,
- model RMSE by target,
- model error by forecast horizon,
- temperature RMSE by city,
- feature importance,
- feature group ablation,
- final 28-day forecast examples for Baku and Guba.

---

## 8. Statistical Analysis

Statistical analysis was used to support the modeling and feature engineering decisions.

### Methods used

- normality checks,
- variance checks,
- Kruskal-Wallis tests for city-level differences,
- Mann-Whitney post-hoc comparisons,
- Spearman correlation analysis,
- Benjamini-Hochberg correction for multiple testing,
- effect-size interpretation.

### Why non-parametric tests were appropriate

Weather variables are often not normally distributed. Precipitation, for example, is highly skewed because many days have no rain and a smaller number of days have heavy rain. Because normality and equal-variance assumptions are not always satisfied, non-parametric tests are more appropriate for comparing weather distributions across cities.

### Main conclusion

The statistical analysis supports the idea that city-specific and season-specific effects matter. This justifies the use of:

- city encoding,
- calendar features,
- cyclical seasonality features,
- lag features,
- rolling features,
- comfort-related features.

The project does not rely only on statistical significance. Effect size and business relevance are also important because large datasets can produce very small p-values even when practical differences are modest.

---

## 9. Feature Engineering

The feature engineering layer converts cleaned daily weather data into a model-ready table.

### Feature groups

| Group                 | Examples                                        | Purpose                                                  |
| --------------------- | ----------------------------------------------- | -------------------------------------------------------- |
| Core weather features | `apparent_temperature_max`, `sunshine_duration` | Capture comfort and sunshine conditions                  |
| City feature          | `city_encoded`                                  | Capture location-specific climate patterns               |
| Calendar features     | `month`, `day_of_month`, `day_sin`, `day_cos`   | Capture seasonality                                      |
| Comfort features      | `comfort_gap`, `sunshine_ratio`                 | Represent perceived comfort and daylight quality         |
| Lag features          | temperature, precipitation, wind, humidity lags | Capture recent weather memory                            |
| Rolling features      | 3-day, 7-day, 14-day averages/sums              | Smooth short-term fluctuations and capture recent trends |
| Risk features         | `rainy_days_7d`                                 | Represent recent wet-period persistence                  |
| Trend features        | one-day changes in weather variables            | Capture short-term direction of change                   |

### Leakage control

Rolling features are shifted before rolling calculations. This helps avoid using the current target day as part of the input features.

---

## 10. Modeling

The modeling task is framed as multi-output regression.

### Targets

The model predicts:

- `temperature_2m_max`,
- `precipitation_sum`,
- `wind_speed_10m_max`,
- `relative_humidity_2m_mean`,
- `cloud_cover_mean`.

### Candidate models

The modeling notebook compares:

- Dummy baseline,
- Ridge regression,
- Random Forest,
- Extra Trees,
- Gradient Boosting,
- XGBoost.

### Final model

The selected final model for the ML forecast extension is:

```text
GradientBoostingRegressor wrapped in MultiOutputRegressor
```

The model is trained using direct horizon forecasting. Separate models are trained for future horizons after the API forecast window, and their predictions are used for days 8–28 of the final forecast.

### Why multi-output regression?

Tourism decisions depend on several weather variables together. For example, a day can be warm but unsuitable because of rain or wind. Multi-output regression creates a consistent prediction set for downstream suitability logic.

---

## 11. Model Evaluation

The model is evaluated using time-based backtesting, which better reflects a real forecasting workflow than a random split.

### 28-day overall performance

The final 28-day model performance summary:

| Model             | Horizon | MAE mean | RMSE mean | R² mean | NRMSE mean |
| ----------------- | ------: | -------: | --------: | ------: | ---------: |
| Gradient Boosting |      28 |     9.23 |     11.77 |    0.28 |       0.84 |

The average score combines targets with different units, so target-level metrics are more informative.

### 28-day target-level performance

| Target                      |   MAE |  RMSE |     R² | Interpretation                                             |
| --------------------------- | ----: | ----: | -----: | ---------------------------------------------------------- |
| `temperature_2m_max`        |  3.46 |  4.52 |  0.768 | Strong performance; temperature is seasonal and persistent |
| `precipitation_sum`         |  2.72 |  6.12 | -0.016 | Weak; daily rain is irregular and event-driven             |
| `wind_speed_10m_max`        |  4.54 |  5.81 |  0.112 | Limited but usable as broad wind-risk signal               |
| `relative_humidity_2m_mean` |  9.39 | 11.69 |  0.357 | Moderate performance; useful for comfort-risk estimation   |
| `cloud_cover_mean`          | 26.05 | 30.72 |  0.182 | Difficult but useful as broad sky-condition signal         |

### Interpretation

The model performs best for temperature. This is expected because temperature has strong seasonal structure and short-term persistence. The model performs weaker for precipitation and cloud cover because these variables are more irregular and event-driven.

For the business use case, this is still useful. The project does not require perfect daily rainfall prediction; it needs early risk signals to support tourism planning. The forecast should therefore be interpreted as a planning-support estimate, not a guarantee of exact weather conditions.

---

## 12. Final 28-Day Forecast

The final output table is:

```text
analytics.final_28d_forecast
```

Expected row count:

```text
5 cities × 28 forecast days = 140 rows
```

Each row contains:

- city,
- origin date,
- forecast horizon,
- target date,
- forecast source,
- predicted or API-provided weather variables.

Forecast source values:

| Source         | Meaning                                 |
| -------------- | --------------------------------------- |
| `api_forecast` | Days 1–7 from Open-Meteo Forecast API   |
| `ml_model`     | Days 8–28 from ML direct horizon models |

---

## 13. Activity Suitability Logic

The final forecast supports an activity suitability layer. The logic uses weather variables such as:

- precipitation,
- wind speed,
- apparent temperature,
- humidity,
- cloud cover,
- sunshine duration.

It classifies the day into categories such as:

- indoor activity preference,
- hot-weather activity preference,
- perfect outdoor conditions,
- cool-weather activity preference,
- mixed / manageable conditions.

Then city-specific activity suggestions are selected. This creates a bridge between numeric weather forecasts and practical tourism decisions.

---

## 14. Automation and Reproducibility

The repository includes a GitHub Actions workflow:

```text
.github/workflows/weather_pipeline.yml
```

The workflow:

1. checks out the repository,
2. installs Python dependencies,
3. runs the full pipeline,
4. verifies the final forecast output,
5. uploads the generated DuckDB database as an artifact.

This improves reproducibility because the pipeline can be rerun automatically instead of relying only on notebooks.

---

## 15. Limitations

The project has several limitations:

- The current scope includes only five cities.
- The model uses daily weather aggregates, not hourly data.
- Precipitation and cloud cover are difficult to predict at medium range.
- Activity suitability thresholds should be calibrated with real tourism operators.
- The current model is useful for planning support, not exact weather guarantees.
- Trained model artifacts are not yet persisted for reuse.
- Formal unit tests can be added to strengthen code quality.

---

## 16. Future Work

Potential improvements:

- Add more Azerbaijan cities and tourism regions.
- Add hourly forecast logic for short-term operational decisions.
- Store trained model artifacts using `joblib`.
- Add formal unit tests for ingestion, cleaning, feature engineering, and quality checks.
- Add confidence bands or risk levels to the final forecast.
- Improve precipitation modeling using classification-style rain/no-rain risk.
- Calibrate activity suitability thresholds with tourism professionals.
- Extend the website with city, date range, and activity-type filters.

---

## 17. Conclusion

ClimaFitAI demonstrates a complete data and machine learning workflow for tourism weather planning. The project connects a real business problem to a reproducible technical solution:

```text
weather data → quality checks → features → forecast model → tourism suitability decision
```

The strongest parts of the project are the end-to-end pipeline, DuckDB integration, quality gates, feature engineering, horizon-aware model evaluation, and hybrid forecast strategy. The main technical limitation is that some weather variables, especially precipitation and cloud cover, are difficult to predict accurately at a 28-day horizon. This limitation is handled by positioning the output as a medium-range planning signal rather than a precise deterministic forecast.

Overall, the project is suitable for demonstrating pipeline completeness, data quality analysis, statistical reasoning, predictive modeling, and practical business translation.

---

## 18. Final Rubric Readiness Summary

The project is ready to be presented as a complete data and machine learning pipeline rather than only a notebook analysis. It satisfies the main rubric areas in the following way:

| Rubric area           | Evidence in the project                                                                                                                                   |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pipeline completeness | API ingestion, parquet storage, DuckDB raw tables, validation gates, cleaning, feature engineering, modeling, final 28-day forecast table, GitHub Actions |
| Data quality analysis | missing-value checks, duplicate checks, city/date coverage checks, column consistency checks, weather range checks                                        |
| Statistical rigour    | EDA, distribution checks, non-parametric tests, correlation analysis, multiple-testing correction, effect-size interpretation                             |
| Prediction model      | time-based evaluation, baseline comparison, multi-output regression, horizon-aware modeling, target-level metrics, feature importance, ablation analysis  |
| Presentation quality  | clear business problem, architecture flow, visual outputs, model interpretation, limitations and future work                                              |
| Code quality          | modular `src/` package, reusable functions, DuckDB utilities, pipeline orchestration, GitHub Actions automation                                           |

The final defense should focus on one clear message: ClimaFitAI converts weather data into earlier tourism planning decisions. The system is strongest where weather has stable seasonal structure, especially temperature, and more cautious where weather is event-driven, especially precipitation. This makes the project technically honest and business-relevant.
