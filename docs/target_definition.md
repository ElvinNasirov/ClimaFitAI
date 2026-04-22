# Target Definition

## Problem Direction

We aim to predict the risk level of outdoor activity cancellation (specifically picnics) based on daily weather conditions.

This project focuses on activity suitability and risk prediction rather than predicting exact weather values.

## Target Variable

We define a categorical target variable:

risk_level:

- Low → Good conditions for picnic
- Medium → Some discomfort, but still possible
- High → High probability of cancellation

## Labeling Logic

Labels are generated using domain-based rules from daily weather variables:

### High Risk

- precipitation_sum > 2 mm (rain)
- OR temperature_2m_max < 12°C (too cold)
- OR wind_speed_10m_max > 30 km/h (strong wind)

### Medium Risk

- relative_humidity_2m_mean > 80%
- OR apparent_temperature_max > 30°C (heat discomfort)

### Low Risk

- All other conditions

## Why It Matters

This approach transforms raw weather data into actionable insights for users planning outdoor activities.

It helps answer:
"Is this a good day for a picnic?"
instead of
"What will the weather be?"
