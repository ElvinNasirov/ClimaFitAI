"""
ClimaFit AI — FastAPI Backend

Pipeline refresh strategy (NOT per-request — that would take 1–3 minutes):
  1. STARTUP  — runs the pipeline once when the server boots.
  2. SCHEDULER — re-runs the pipeline every 24 hours in a background thread,
                  so forecasts are always fresh without user waiting.
  3. POST /refresh — manual admin endpoint to trigger an immediate re-run
                     on demand (e.g. after you update the model).

Every forecast request reads pre-computed results from:
    data/weather.duckdb → analytics.final_28d_forecast
"""

import sys
import os
import math
import threading
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel
import duckdb

# ---------------------------------------------------------------------------
# Make the project root importable so we can use src.*
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # …/m5-project-weather-pipeline
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import CITIES, get_activity_recommendation
from src.pipeline import run_pipeline

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("climafit")

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="ClimaFit AI API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Serve the frontend from the same server
#   http://127.0.0.1:8000         → index.html
#   http://127.0.0.1:8000/styles/ → CSS
#   http://127.0.0.1:8000/src/    → JS
# ---------------------------------------------------------------------------
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

app.mount("/styles", StaticFiles(directory=FRONTEND_DIR / "styles"), name="styles")
app.mount("/src",    StaticFiles(directory=FRONTEND_DIR / "src"),    name="src")

@app.get("/", include_in_schema=False)
def serve_index() -> FileResponse:
    """Serve the frontend index.html at the root URL."""
    return FileResponse(FRONTEND_DIR / "index.html")

@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    """Return an empty response for favicon to prevent 404 logs."""
    return Response(content=b"", media_type="image/x-icon")

# ---------------------------------------------------------------------------
# Shared DuckDB — same file used by the main pipeline (src/db.py)
# ---------------------------------------------------------------------------
DB_PATH = PROJECT_ROOT / "data" / "weather.duckdb"

# ---------------------------------------------------------------------------
# City name normalisation
# ---------------------------------------------------------------------------
VALID_CITIES: dict[str, dict] = {city["name"]: city for city in CITIES}

# ---------------------------------------------------------------------------
# Pipeline state — shared across threads, protected by a lock
# ---------------------------------------------------------------------------
_pipeline_lock = threading.Lock()   # prevents two runs overlapping

pipeline_state = {
    "is_running":  False,           # True while pipeline is executing
    "last_run_at": None,            # datetime of last successful run
    "last_error":  None,            # last error message (str) or None
    "run_count":   0,               # total successful runs since boot
}

REFRESH_INTERVAL_HOURS = 24        # how often the scheduler re-runs

RUN_PIPELINE_ON_STARTUP = (
    os.getenv("RUN_PIPELINE_ON_STARTUP", "false").lower() == "true"
)

ENABLE_SCHEDULER = (
    os.getenv("ENABLE_SCHEDULER", "false").lower() == "true"
)


# ---------------------------------------------------------------------------
# Core pipeline runner  (called from startup + scheduler + /refresh)
# ---------------------------------------------------------------------------
def _run_pipeline_background(refresh_data: bool = True) -> None:
    """
    Run src/pipeline.py in a background thread.
    Uses a threading.Lock so only one run can happen at a time.
    Results are written to data/weather.duckdb automatically by the pipeline.
    """
    if not _pipeline_lock.acquire(blocking=False):
        log.warning("Pipeline already running — skipping duplicate trigger.")
        return

    try:
        pipeline_state["is_running"] = True
        pipeline_state["last_error"] = None
        log.info("▶  Pipeline started (refresh_data=%s)", refresh_data)

        run_pipeline(refresh_data=refresh_data)  # ← the real ML pipeline

        pipeline_state["last_run_at"] = datetime.utcnow().isoformat() + "Z"
        pipeline_state["run_count"]  += 1
        log.info("✅ Pipeline finished successfully (run #%d)", pipeline_state["run_count"])

    except Exception as exc:
        pipeline_state["last_error"] = str(exc)
        log.error("❌ Pipeline failed: %s", exc)

    finally:
        pipeline_state["is_running"] = False
        _pipeline_lock.release()


def _has_fresh_forecast() -> bool:
    """
    Returns True if analytics.final_28d_forecast already contains rows
    for today or later, meaning a previous pipeline run's data is still valid.
    Used on startup to avoid re-running the expensive pipeline unnecessarily.
    """
    if not DB_PATH.exists():
        return False
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        count = conn.execute("""
            SELECT COUNT(*)
            FROM analytics.final_28d_forecast
            WHERE CAST(target_time AS DATE) >= CURRENT_DATE
        """).fetchone()[0]
        conn.close()
        return count > 0
    except Exception as exc:
        log.warning("Could not check DuckDB for fresh data: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Background scheduler — re-runs pipeline every REFRESH_INTERVAL_HOURS
# ---------------------------------------------------------------------------
def _start_scheduler() -> None:
    """
    Daemon thread that wakes up every REFRESH_INTERVAL_HOURS and re-runs
    the pipeline so the 28-day forecast stays current.
    Runs as a daemon so it dies automatically when the server shuts down.
    """
    def _loop():
        interval_seconds = REFRESH_INTERVAL_HOURS * 3600
        while True:
            time.sleep(interval_seconds)
            log.info("⏰ Scheduler: triggering daily pipeline refresh...")
            _run_pipeline_background(refresh_data=True)

    t = threading.Thread(target=_loop, daemon=True, name="pipeline-scheduler")
    t.start()
    log.info(
        "📅 Scheduler started — pipeline will re-run every %d hours.",
        REFRESH_INTERVAL_HOURS,
    )


# ---------------------------------------------------------------------------
# FastAPI lifecycle — run pipeline ONCE on startup
# ---------------------------------------------------------------------------
@app.on_event("startup")
def on_startup() -> None:
    """
    Triggered once when the server starts.

    For Render demo deployment, the backend should use the existing
    DuckDB forecast snapshot instead of running the full ML pipeline
    on startup.
    """
    log.info("Server starting.")
    log.info("PROJECT_ROOT = %s", PROJECT_ROOT)
    log.info("DB_PATH = %s", DB_PATH)
    log.info("DB exists = %s", DB_PATH.exists())

    if RUN_PIPELINE_ON_STARTUP:
        log.info("RUN_PIPELINE_ON_STARTUP=true — launching pipeline in background thread.")
        t = threading.Thread(
            target=_run_pipeline_background,
            kwargs={"refresh_data": True},
            daemon=True,
            name="pipeline-startup",
        )
        t.start()
    else:
        log.info("RUN_PIPELINE_ON_STARTUP=false — using existing DuckDB snapshot.")

    if ENABLE_SCHEDULER:
        _start_scheduler()
    else:
        log.info("ENABLE_SCHEDULER=false — scheduler disabled.")


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------
class ForecastRequest(BaseModel):
    location: str
    date: str  # ISO-8601, e.g. "2026-05-10"


class ForecastResponse(BaseModel):
    # Raw weather values — names match src/config.py DAILY_VARIABLES
    temperature_2m_max: float
    precipitation_sum: float
    wind_speed_10m_max: float
    relative_humidity_2m_mean: float
    cloud_cover_mean: float
    sunshine_duration: float
    apparent_temperature_max: float

    # Derived fields for the frontend
    condition: str       # human-readable label
    activity_type: str   # matches weather_logic.py categories
    reason: str          # explanation from weather_logic.py
    activities: List[str]


class PipelineStatus(BaseModel):
    is_running: bool
    last_run_at: str | None
    last_error: str | None
    run_count: int
    next_auto_refresh_in_hours: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _derive_condition(data: dict) -> str:
    """
    Map raw weather variables → human-readable condition label.
    Thresholds match get_activity_recommendation() in src/config.py.
    Wind values are in km/h (Open-Meteo default).
    High cloud alone does NOT return 'rainy' — actual rain is required.
    """
    rain  = data["precipitation_sum"]
    cloud = data["cloud_cover_mean"]
    wind  = data["wind_speed_10m_max"]
    feels = data["apparent_temperature_max"]
    temp  = data["temperature_2m_max"]
    hum   = data["relative_humidity_2m_mean"]
    sun   = data["sunshine_duration"]

    if rain > 10:                                              # actual rain required
        return "rainy"
    elif wind > 40:                                            # km/h threshold
        return "windy"
    elif feels > 33 or (temp > 30 and hum > 70):
        return "hot"
    elif 20 <= feels <= 30 and rain < 1 and wind < 18 and sun > 21600:
        return "sunny"
    elif cloud > 60:                                           # cloudy but no rain
        return "cloudy"
    elif feels < 20:
        return "cool"
    return "moderate"


def load_from_duckdb(city_name: str, date_str: str) -> dict | None:
    """
    Read a pre-computed forecast row from analytics.final_28d_forecast.
    Returns None if the pipeline hasn't run yet or the date is out of range.
    """
    if not DB_PATH.exists():
        return None

    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        row = conn.execute(
            """
            SELECT
                temperature_2m_max,
                precipitation_sum,
                wind_speed_10m_max,
                relative_humidity_2m_mean,
                cloud_cover_mean,
                sunshine_duration
            FROM analytics.final_28d_forecast
            WHERE LOWER(city) = LOWER(?)
              AND CAST(target_time AS DATE) = CAST(? AS DATE)
            ORDER BY forecast_horizon
            LIMIT 1
            """,
            [city_name, date_str],
        ).fetchone()
        conn.close()

        if row is None:
            return None

        temp, rain, wind, hum, cloud, sun = row

        # apparent_temperature_max is not stored — derive using the
        # Australian BOM apparent temperature formula:
        #   AT = T + 0.33*e - 0.70*ws - 4.0
        # where e = vapour pressure (hPa), ws = wind speed (m/s)
        wind_ms = wind / 3.6                                      # km/h → m/s
        e_sat   = 6.1078 * math.exp(17.269 * temp / (temp + 237.3))  # saturation vapour pressure
        e       = e_sat * (hum / 100.0)                           # actual vapour pressure
        feels   = round(temp + 0.33 * e - 0.70 * wind_ms - 4.00, 1)

        return {
            "temperature_2m_max":        round(float(temp),  1),
            "precipitation_sum":         round(float(rain),  1),
            "wind_speed_10m_max":        round(float(wind),  1),
            "relative_humidity_2m_mean": round(float(hum),   1),
            "cloud_cover_mean":          round(float(cloud), 1),
            "sunshine_duration":         round(float(sun),   0),
            "apparent_temperature_max":  round(float(feels), 1),
        }
    except Exception as exc:
        log.warning("load_from_duckdb error for %s/%s: %s", city_name, date_str, exc)
        return None


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.get("/debug/db")
def debug_db() -> dict:
    """
    Debug endpoint for Render deployment.
    Shows whether the deployed backend can see DuckDB and the final forecast table.
    """
    info = {
        "project_root": str(PROJECT_ROOT),
        "db_path": str(DB_PATH),
        "db_exists": DB_PATH.exists(),
    }

    if not DB_PATH.exists():
        info["error"] = "DuckDB file does not exist at DB_PATH."
        return info

    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)

        table_exists = conn.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'analytics'
              AND table_name = 'final_28d_forecast'
        """).fetchone()[0]

        info["final_table_exists"] = bool(table_exists)

        if table_exists:
            summary = conn.execute("""
                SELECT
                    COUNT(*) AS row_count,
                    MIN(target_time) AS min_target_time,
                    MAX(target_time) AS max_target_time
                FROM analytics.final_28d_forecast
            """).fetchone()

            info["row_count"] = summary[0]
            info["min_target_time"] = str(summary[1])
            info["max_target_time"] = str(summary[2])

            cities = conn.execute("""
                SELECT city, COUNT(*) AS rows
                FROM analytics.final_28d_forecast
                GROUP BY city
                ORDER BY city
            """).fetchall()

            info["cities"] = [
                {"city": city, "rows": rows}
                for city, rows in cities
            ]

            baku_sample = conn.execute("""
                SELECT city, target_time, forecast_horizon
                FROM analytics.final_28d_forecast
                WHERE LOWER(city) = LOWER('Baku')
                ORDER BY target_time
                LIMIT 10
            """).fetchall()

            info["baku_sample"] = [
                {
                    "city": city,
                    "target_time": str(target_time),
                    "forecast_horizon": horizon,
                }
                for city, target_time, horizon in baku_sample
            ]

        conn.close()
        return info

    except Exception as exc:
        info["error"] = str(exc)
        return info


@app.post("/forecast", response_model=ForecastResponse)
def get_forecast(request: ForecastRequest) -> ForecastResponse:
    """
    Returns weather forecast + city-specific activity recommendations.
    Data comes from analytics.final_28d_forecast (written by the ML pipeline).
    If the pipeline is still running on first boot, returns 503 — retry in ~2 min.
    """
    # Validate location against canonical names from src/config.py
    location = request.location
    if location not in VALID_CITIES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"'{request.location}' is not a valid location. "
                f"Choose from: {', '.join(VALID_CITIES.keys())}."
            ),
        )

    # If pipeline crashed on startup, return the error
    if pipeline_state["last_error"] and pipeline_state["run_count"] == 0:
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline crashed during initialization: {pipeline_state['last_error']}"
        )

    # If pipeline is still running on startup, let the client know
    if pipeline_state["is_running"] and pipeline_state["run_count"] == 0:
        raise HTTPException(
            status_code=503,
            detail=(
                "The ML pipeline is still running its first boot. "
                "Please wait ~2 minutes and try again."
            ),
        )

    weather = load_from_duckdb(location, request.date)
    if weather is None:
        raise HTTPException(
            status_code=503,
            detail=(
                f"No forecast found for {location} on {request.date}. "
                "This date may be outside the 28-day forecast window. "
                "Check GET /pipeline/status for pipeline state."
            ),
        )

    recommendation = get_activity_recommendation(weather, location)
    if recommendation["status"] != "success":
        raise HTTPException(status_code=500, detail=recommendation["message"])

    return ForecastResponse(
        temperature_2m_max        = weather["temperature_2m_max"],
        precipitation_sum         = weather["precipitation_sum"],
        wind_speed_10m_max        = weather["wind_speed_10m_max"],
        relative_humidity_2m_mean = weather["relative_humidity_2m_mean"],
        cloud_cover_mean          = weather["cloud_cover_mean"],
        sunshine_duration         = weather["sunshine_duration"],
        apparent_temperature_max  = weather["apparent_temperature_max"],
        condition                 = _derive_condition(weather),
        activity_type             = recommendation["activity_type"],
        reason                    = recommendation["reason"],
        activities                = recommendation["suggestions"],
    )


@app.get("/pipeline/status", response_model=PipelineStatus)
def get_pipeline_status() -> PipelineStatus:
    """
    Returns the current state of the background pipeline.
    Useful for monitoring / debugging.
    """
    # Calculate approx hours until next scheduled run
    if pipeline_state["last_run_at"]:
        last_run = datetime.fromisoformat(pipeline_state["last_run_at"].rstrip("Z"))
        elapsed_hours = (datetime.utcnow() - last_run).total_seconds() / 3600
        next_in = max(0.0, round(REFRESH_INTERVAL_HOURS - elapsed_hours, 2))
    else:
        next_in = 0.0  # running now or never ran

    return PipelineStatus(
        is_running                = pipeline_state["is_running"],
        last_run_at               = pipeline_state["last_run_at"],
        last_error                = pipeline_state["last_error"],
        run_count                 = pipeline_state["run_count"],
        next_auto_refresh_in_hours = next_in,
    )


@app.post("/pipeline/refresh")
def trigger_pipeline_refresh(background_tasks: BackgroundTasks) -> dict:
    """
    Manually trigger an immediate pipeline re-run.
    The run happens in the background — this endpoint returns instantly.
    Poll GET /pipeline/status to track progress.
    """
    if pipeline_state["is_running"]:
        raise HTTPException(
            status_code=409,
            detail="Pipeline is already running. Check GET /pipeline/status.",
        )

    background_tasks.add_task(_run_pipeline_background, refresh_data=True)
    log.info("🔄 Manual pipeline refresh triggered via /pipeline/refresh")

    return {
        "message": "Pipeline refresh started in background.",
        "hint": "Poll GET /pipeline/status to track progress.",
    }
