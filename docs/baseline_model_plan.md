# Baseline Model Plan

## Prediction Task

Predict the risk_level (Low / Medium / High) for picnic suitability.

## Features

We will use the following daily weather variables:

- temperature_2m_max
- temperature_2m_min
- apparent_temperature_max
- precipitation_sum
- wind_speed_10m_max
- relative_humidity_2m_mean
- weather_code

## Train/Test Split

We will use an 80/20 split based on time:

- First 80% of data → training
- Last 20% → testing

This avoids data leakage and simulates real-world prediction.

## Baseline Models

We will test simple models first:

- Logistic Regression (multiclass)
- Random Forest Classifier

These models are chosen because:

- Easy to interpret
- Fast to train
- Good baseline performance

## Evaluation Metrics

- Accuracy
- F1-score (important for class balance)
- Confusion Matrix

## Next Steps

- Improve feature engineering (rolling averages, lag features)
- Tune models
- Compare cities
