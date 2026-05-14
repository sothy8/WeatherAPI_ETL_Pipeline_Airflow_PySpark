"""Load transformed weather data into a relational database.

On each run the table is truncated and fully reloaded so the hourly
history window (last 3 days + today) stays consistent and duplicates
are avoided.
"""

from __future__ import annotations

import argparse
import glob
import os
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

from dotenv import load_dotenv

load_dotenv()

DEFAULT_TABLE_NAME = "weather_current"
DEFAULT_PROCESSED_PATH = os.getenv("PROCESSED_WEATHER_PATH", "./data/processed/weather_cleaned")


def _read_merged_csv(processed_dir: str) -> pd.DataFrame:
    """
    Read the single merged CSV produced by the transform step.
    Prefers weather_cleaned.csv (one level above the Spark directory).
    Falls back to part-*.csv files inside the Spark directory.
    """
    base = Path(processed_dir)
    merged = base.parent / "weather_cleaned.csv"
    if merged.exists():
        return pd.read_csv(merged)

    part_files = sorted(glob.glob(str(base / "part-*.csv")))
    if not part_files:
        raise ValueError(f"No transformed weather CSV found in {processed_dir} or {merged}")
    return pd.concat([pd.read_csv(f) for f in part_files], ignore_index=True)


def load_weather_data(
    input_dir: str,
    database_url: str | None = None,
    table_name: str | None = None,
) -> int:
    df = _read_merged_csv(input_dir)

    target_db_url = database_url or os.getenv("DATABASE_URL")
    if not target_db_url:
        raise ValueError("DATABASE_URL is required to load weather data")

    target_table = table_name or os.getenv("WEATHER_TABLE_NAME", DEFAULT_TABLE_NAME)
    engine = sa.create_engine(target_db_url)

    # Replace NaN with None so MySQL gets proper NULLs.
    # local_time comes through as plain "YYYY-MM-DD HH:MM" (no timezone) — parse directly.
    # extracted_at still has a UTC offset from the extract step — strip it without converting.
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if not isinstance(v, str) and pd.isna(v):
                clean[k] = None
            elif k == "local_time" and isinstance(v, str):
                try:
                    clean[k] = pd.to_datetime(v).to_pydatetime().replace(tzinfo=None)
                except Exception:
                    clean[k] = None
            elif k == "extracted_at" and isinstance(v, str):
                # e.g. "2026-05-14T10:36:52.470627+00:00" — drop offset, keep wall-clock
                try:
                    clean[k] = pd.to_datetime(v).to_pydatetime().replace(tzinfo=None)
                except Exception:
                    clean[k] = None
            else:
                clean[k] = v
        records.append(clean)

    with engine.begin() as conn:
        # Truncate — clean full-reload every run
        conn.execute(text(f"TRUNCATE TABLE `{target_table}`"))

        if records:
            # Reflect the table so SQLAlchemy knows the column types
            meta = sa.MetaData()
            table = sa.Table(target_table, meta, autoload_with=conn)
            # Insert in chunks to avoid oversized packets
            chunk_size = 500
            for i in range(0, len(records), chunk_size):
                conn.execute(table.insert(), records[i : i + chunk_size])

    print(f"Loaded {len(records)} rows into `{target_table}`")
    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load transformed weather data into the database.")
    parser.add_argument("--input", default=DEFAULT_PROCESSED_PATH, help="Path to the processed weather directory")
    parser.add_argument("--database-url", default=None, help="SQLAlchemy database URL")
    parser.add_argument("--table-name", default=None, help="Target database table")
    args = parser.parse_args()
    load_weather_data(args.input, args.database_url, args.table_name)


if __name__ == "__main__":
    main()
