"""Airflow DAG for the WeatherAPI.com ETL pipeline.

Schedule : hourly
Pipeline : extract → transform → load → data_quality_check

Data quality checks (run after every load):
  1. Row count    – at least 72 rows expected (3 full days × 24 h)
  2. Null check   – no NULLs in critical columns
  3. Temp range   – temp_c must be between -10 °C and 60 °C
  4. Humidity     – humidity must be 0–100
  5. Freshness    – most-recent local_time must be within the last 25 h
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.extract_weather import extract_weather_data
from jobs.load_weather import load_weather_data
from jobs.transform_weather import transform_weather

RAW_WEATHER_PATH = os.getenv("RAW_WEATHER_PATH", str(PROJECT_ROOT / "data" / "raw" / "weather.json"))
PROCESSED_WEATHER_PATH = os.getenv(
    "PROCESSED_WEATHER_PATH",
    str(PROJECT_ROOT / "data" / "processed" / "weather_cleaned_csv"),
)
WEATHER_TABLE_NAME = os.getenv("WEATHER_TABLE_NAME", "weather_current")

# ── Resolve database URL ──────────────────────────────────────────────────────
# Prefer the Airflow-managed connection "weather_mysql" if it exists,
# otherwise fall back to the DATABASE_URL environment variable.

AIRFLOW_CONN_ID = "weather_mysql"

def _get_database_url() -> str:
    try:
        conn = BaseHook.get_connection(AIRFLOW_CONN_ID)
        return f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 3306}/{conn.schema}"
    except Exception:
        return os.getenv("DATABASE_URL", "mysql+pymysql://weather:weather@mysql:3306/weather_etl")

DATABASE_URL = _get_database_url()


# ── Data quality check ────────────────────────────────────────────────────────

def run_data_quality_checks(database_url: str, table_name: str) -> None:
    """
    Run a suite of data quality checks against the loaded table.
    Raises ValueError on any failure so Airflow marks the task as failed.
    """
    import sqlalchemy as sa
    from sqlalchemy import text

    engine = sa.create_engine(database_url)
    failures: list[str] = []

    with engine.connect() as conn:

        # 1. Row count — expect at least 3 days × 24 h = 72 rows
        row_count = conn.execute(
            text(f"SELECT COUNT(*) FROM `{table_name}`")
        ).scalar()
        print(f"[QC] Row count: {row_count}")
        if row_count < 72:
            failures.append(f"Row count too low: {row_count} (expected >= 72)")

        # 2. Null check on critical columns
        for col in ("location_name", "local_time", "temp_c", "humidity"):
            null_count = conn.execute(
                text(f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col}` IS NULL")
            ).scalar()
            print(f"[QC] NULLs in {col}: {null_count}")
            if null_count > 0:
                failures.append(f"NULL values found in column '{col}': {null_count} rows")

        # 3. Temperature range: -10 °C to 60 °C
        bad_temp = conn.execute(
            text(f"SELECT COUNT(*) FROM `{table_name}` WHERE temp_c < -10 OR temp_c > 60")
        ).scalar()
        print(f"[QC] Out-of-range temp_c rows: {bad_temp}")
        if bad_temp > 0:
            failures.append(f"temp_c out of range [-10, 60]: {bad_temp} rows")

        # 4. Humidity range: 0–100
        bad_humidity = conn.execute(
            text(f"SELECT COUNT(*) FROM `{table_name}` WHERE humidity < 0 OR humidity > 100")
        ).scalar()
        print(f"[QC] Out-of-range humidity rows: {bad_humidity}")
        if bad_humidity > 0:
            failures.append(f"humidity out of range [0, 100]: {bad_humidity} rows")

        # 5. Freshness — extracted_at (UTC) must be within the last 25 hours
        latest = conn.execute(
            text(f"SELECT MAX(extracted_at) FROM `{table_name}`")
        ).scalar()
        print(f"[QC] Most recent extracted_at (UTC): {latest}")
        if latest is None:
            failures.append("Table appears empty — no extracted_at found")
        else:
            age_hours = (datetime.utcnow() - latest).total_seconds() / 3600
            print(f"[QC] Data age: {age_hours:.1f} h")
            if age_hours > 25:
                failures.append(f"Data is stale: most recent extraction is {age_hours:.1f} h old (threshold: 25 h)")

    if failures:
        msg = "\n".join(f"  ✗ {f}" for f in failures)
        raise ValueError(f"Data quality checks FAILED:\n{msg}")

    print(f"[QC] All checks passed ✓  ({row_count} rows)")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="weather_api_etl_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule="@hourly",
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["weather", "etl", "spark"],
    doc_md="""
## Weather API ETL Pipeline

Fetches hourly weather history for the last 3 days + today from WeatherAPI.com,
transforms it with PySpark, loads it into MySQL, then runs data quality checks.

| Task | Description |
|------|-------------|
| `extract_weather` | Calls `/history.json`, writes JSON-lines + CSV to `data/raw/` |
| `transform_weather` | PySpark cleans & casts, writes CSV to `data/processed/` |
| `load_weather` | Truncates `weather_current` and reloads the full window |
| `data_quality_check` | Validates row count, nulls, value ranges, and freshness |
""",
) as dag:

    extract_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather_data,
        op_kwargs={"output_path": RAW_WEATHER_PATH},
        doc_md="Fetch hourly history (last 3 days + today) and write raw files.",
    )

    transform_task = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
        op_kwargs={"input_path": RAW_WEATHER_PATH, "output_path": PROCESSED_WEATHER_PATH},
        doc_md="Clean and cast raw JSON-lines with PySpark, output CSV.",
    )

    load_task = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather_data,
        op_kwargs={
            "input_dir": PROCESSED_WEATHER_PATH,
            "database_url": DATABASE_URL,
            "table_name": WEATHER_TABLE_NAME,
        },
        doc_md="Truncate `weather_current` and reload the full 3-day window.",
    )

    quality_check_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality_checks,
        op_kwargs={
            "database_url": DATABASE_URL,
            "table_name": WEATHER_TABLE_NAME,
        },
        doc_md="""
**Checks performed:**
- Row count ≥ 72
- No NULLs in `location_name`, `local_time`, `temp_c`, `humidity`
- `temp_c` in range [-10, 60]
- `humidity` in range [0, 100]
- Most recent `extracted_at` (UTC) within the last 25 hours
""",
    )

    extract_task >> transform_task >> load_task >> quality_check_task
