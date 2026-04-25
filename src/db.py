# src/db.py
import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DB_PATH = PROJECT_ROOT / "data" / "weather.duckdb"

def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))

def create_schemas():
    with get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS analytics;")


def load_raw_data():
    historical_path = PROJECT_ROOT / "data" / "raw" / "historical" / "*.parquet"
    forecast_path = PROJECT_ROOT / "data" / "raw" / "forecast" / "*.parquet"

    with get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        conn.execute(f"""
            CREATE OR REPLACE TABLE raw.historical AS
            SELECT * FROM read_parquet('{historical_path.as_posix()}');
        """)

        conn.execute(f"""
            CREATE OR REPLACE TABLE raw.forecast AS
            SELECT * FROM read_parquet('{forecast_path.as_posix()}');
        """)


def run_query(query):
    with get_connection() as conn:
        return conn.execute(query).df()