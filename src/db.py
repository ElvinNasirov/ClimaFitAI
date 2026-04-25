# src/db.py
import duckdb
from pathlib import Path

DB_PATH = "data/weather.duckdb"

def get_connection():
    return duckdb.connect(DB_PATH)

def create_schemas():
    conn = get_connection()

    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

    conn.close()

def load_raw_data():
    conn = get_connection()

    # Example: load all historical parquet files
    conn.execute("""
        CREATE OR REPLACE TABLE raw.historical AS
        SELECT * FROM read_parquet('data/raw/historical/*.parquet');
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE raw.forecast AS
        SELECT * FROM read_parquet('data/raw/forecast/*.parquet');
    """)

    conn.close()

def run_query(query):
    conn = get_connection()
    result = conn.execute(query).df()
    conn.close()
    return result