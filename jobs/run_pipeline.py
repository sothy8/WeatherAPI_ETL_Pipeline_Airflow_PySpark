"""Standalone runner for the Weather ETL pipeline.

Runs the full pipeline end-to-end without Airflow:
    extract → transform → load → data quality check

Usage:
    python run_pipeline.py

Environment variables are loaded from .env automatically.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.extract_weather import extract_weather_data
from jobs.transform_weather import transform_weather
from jobs.load_weather import load_weather_data

RAW_WEATHER_PATH     = os.getenv("RAW_WEATHER_PATH",     str(PROJECT_ROOT / "data" / "raw" / "weather.json"))
PROCESSED_WEATHER_PATH = os.getenv("PROCESSED_WEATHER_PATH", str(PROJECT_ROOT / "data" / "processed" / "weather_cleaned_csv"))
DATABASE_URL         = os.getenv("DATABASE_URL")
WEATHER_TABLE_NAME   = os.getenv("WEATHER_TABLE_NAME", "weather_current")


def _banner(step: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {step}")
    print(f"{'='*60}")


def run_quality_checks(database_url: str, table_name: str) -> None:
    """Same checks as the Airflow DAG task."""
    import sqlalchemy as sa
    from sqlalchemy import text

    engine = sa.create_engine(database_url)
    failures: list[str] = []

    with engine.connect() as conn:
        row_count = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()
        print(f"  [QC] Row count: {row_count}")
        if row_count < 72:
            failures.append(f"Row count too low: {row_count} (expected >= 72)")

        for col in ("location_name", "local_time", "temp_c", "humidity"):
            nulls = conn.execute(
                text(f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col}` IS NULL")
            ).scalar()
            print(f"  [QC] NULLs in {col}: {nulls}")
            if nulls > 0:
                failures.append(f"NULL values in '{col}': {nulls} rows")

        bad_temp = conn.execute(
            text(f"SELECT COUNT(*) FROM `{table_name}` WHERE temp_c < -10 OR temp_c > 60")
        ).scalar()
        print(f"  [QC] Out-of-range temp_c: {bad_temp}")
        if bad_temp > 0:
            failures.append(f"temp_c out of range [-10, 60]: {bad_temp} rows")

        bad_humidity = conn.execute(
            text(f"SELECT COUNT(*) FROM `{table_name}` WHERE humidity < 0 OR humidity > 100")
        ).scalar()
        print(f"  [QC] Out-of-range humidity: {bad_humidity}")
        if bad_humidity > 0:
            failures.append(f"humidity out of range [0, 100]: {bad_humidity} rows")

        latest = conn.execute(
            text(f"SELECT MAX(extracted_at) FROM `{table_name}`")
        ).scalar()
        print(f"  [QC] Most recent extracted_at: {latest}")
        if latest is None:
            failures.append("Table appears empty")
        else:
            age_hours = (datetime.utcnow() - latest).total_seconds() / 3600
            print(f"  [QC] Data age: {age_hours:.1f} h")
            if age_hours > 25:
                failures.append(f"Data stale: {age_hours:.1f} h old (threshold: 25 h)")

    if failures:
        print("\n  FAILED:")
        for f in failures:
            print(f"    ✗ {f}")
        raise SystemExit(1)

    print(f"\n  ✓ All checks passed ({row_count} rows)")


def main() -> None:
    start = datetime.now()
    print(f"\nWeather ETL Pipeline — {start.strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Extract
    _banner("Step 1 / 4 — Extract")
    extract_weather_data(output_path=RAW_WEATHER_PATH)

    # 2. Transform
    _banner("Step 2 / 4 — Transform")
    transform_weather(input_path=RAW_WEATHER_PATH, output_path=PROCESSED_WEATHER_PATH)

    # 3. Load
    _banner("Step 3 / 4 — Load")
    if not DATABASE_URL:
        print("  ERROR: DATABASE_URL is not set in .env — skipping load and quality check.")
        raise SystemExit(1)
    load_weather_data(input_dir=PROCESSED_WEATHER_PATH, database_url=DATABASE_URL, table_name=WEATHER_TABLE_NAME)

    # 4. Data quality check
    _banner("Step 4 / 4 — Data Quality Check")
    run_quality_checks(database_url=DATABASE_URL, table_name=WEATHER_TABLE_NAME)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'='*60}")
    print(f"  Pipeline completed in {elapsed:.1f}s")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
