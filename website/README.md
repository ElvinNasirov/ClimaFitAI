# ClimaFit AI

A full-stack web application for 1-month weather forecasts and curated activity recommendations, tailored for tourism agents in Azerbaijan.

## Cities Covered

| City     | Latitude | Longitude |
|----------|----------|-----------|
| Baku     | 40.41    | 49.87     |
| Lankaran | 38.75    | 48.85     |
| Guba     | 41.36    | 48.51     |
| Gabala   | 40.58    | 47.50     |
| Shaki    | 41.11    | 47.10     |

> **Single source of truth:** All city names, coordinates, and weather variables are defined in `src/config.py` and used by every layer of the stack.

## Features

- **FastAPI Backend** — REST API that serves the frontend and exposes `/forecast`, `/pipeline/status`, and `/pipeline/refresh` endpoints.
- **ML Pipeline Integration** — Runs `src/pipeline.py` on startup and every 24 hours; results are stored in `data/weather.duckdb → analytics.final_28d_forecast`.
- **DuckDB Database** — The shared `data/weather.duckdb` file is written by the pipeline and read by the backend (read-only during requests).
- **Activity Recommendations** — Weather-based activity suggestions driven by `src/weather_logic.py`, with city-specific landmark recommendations for all 5 cities.
- **Beautiful UI** — Glassmorphism aesthetic, modern typography, responsive layout, and smooth animations.

## Project Structure

```text
climafit-ai/
├── backend/
│   ├── main.py          # FastAPI app, pipeline scheduler, /forecast endpoint
│   └── requirements.txt # Python dependencies (fastapi, uvicorn, duckdb)
└── frontend/
    ├── index.html       # Main application interface
    ├── styles/
    │   └── index.css    # Modern glassmorphism UI
    └── src/
        ├── app.js       # Form submit → POST /forecast → render results
        └── components/
            ├── WeatherCard.js   # Renders temperature, condition, wind, rain, humidity
            └── ActivityList.js  # Renders city-specific activity suggestions
```

> The database lives at `data/weather.duckdb` (project root), **not** inside `climafit-ai/`. It is shared with the main pipeline.

## How to Run

### 1. Start the Backend (FastAPI)

```bash
cd climafit-ai/backend
pip install -r requirements.txt
python -m uvicorn main:app --reload
```

The server starts at **`http://127.0.0.1:8000`**.

- On startup the ML pipeline runs automatically in a background thread (~2 min first run).
- The frontend is served at `http://127.0.0.1:8000/` (no separate web server needed).
- Pipeline re-runs every **24 hours** automatically.

### 2. Open the App

Visit **`http://127.0.0.1:8000`** in your browser.

> While the pipeline is running its first boot, the `/forecast` endpoint will return HTTP 503 — just wait ~2 minutes and refresh.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/` | Serves `frontend/index.html` |
| `POST` | `/forecast` | Returns weather + activity recommendation |
| `GET`  | `/pipeline/status` | Reports pipeline run state |
| `POST` | `/pipeline/refresh` | Manually triggers pipeline re-run |

### POST /forecast — Request / Response

**Request body:**
```json
{ "location": "Baku", "date": "2026-05-10" }
```

**Response fields** (all match `src/config.py` variable names):
```json
{
  "temperature_2m_max": 24.5,
  "precipitation_sum": 0.0,
  "wind_speed_10m_max": 4.2,
  "relative_humidity_2m_mean": 61.0,
  "cloud_cover_mean": 35.0,
  "sunshine_duration": 23400.0,
  "apparent_temperature_max": 22.4,
  "condition": "sunny",
  "activity_type": "perfect",
  "reason": "Sunny and comfortable",
  "activities": ["A Walk along Baku Boulevard", "Exploring Icherisheher and Maiden Tower", "Visiting Flame Towers viewpoint"]
}
```
