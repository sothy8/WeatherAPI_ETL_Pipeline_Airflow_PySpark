"""Transform WeatherAPI.com history data (JSON-lines) with PySpark.

Output
------
Two files are written:
  1. Spark CSV directory  (output_path/)             - partitioned Spark output
  2. Single merged CSV    (output_path/../weather_cleaned.csv)
    - one clean file for easy inspection / downstream use
"""

from __future__ import annotations

import argparse
import glob
import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, trim, to_timestamp

from dotenv import load_dotenv

load_dotenv()

DEFAULT_RAW_PATH = os.getenv("RAW_WEATHER_PATH", "./data/raw/weather.json")
DEFAULT_PROCESSED_PATH = os.getenv("PROCESSED_WEATHER_PATH", "./data/processed/weather_cleaned")


def transform_weather(input_path: str, output_path: str) -> str:
    # Use the Spark cluster if SPARK_MASTER_URL is set (Docker),
    # otherwise fall back to local mode (running outside Docker).
    spark_master = os.getenv("SPARK_MASTER_URL", "local[*]")

    spark = (
        SparkSession.builder.appName("weather-etl-transform")
        .master(spark_master)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    print(f"Spark master: {spark_master}")

    # Extract writes flat JSON-lines — one object per line
    raw_df = spark.read.json(input_path)

    # Small helper to normalise textual columns: remove non-alphanumerics,
    # trim whitespace and convert to lowercase so downstream comparisons are
    # consistent and predictable.
    def _clean_str(c):
        return lower(trim(regexp_replace(col(c), r"[^A-Za-z0-9\s\-]", "")))

    # Select, clean and cast the required columns. Keep `local_time` and
    # `extracted_at` as strings here (parsing / timezone handling happens
    # later in the load step). Filter out any rows missing a location name
    # and order by local time for deterministic output.
    cleaned_df = (
        raw_df.select(
            _clean_str("location_name").alias("location_name"),
            _clean_str("region").alias("region"),
            _clean_str("country").alias("country"),
            col("latitude").cast("double"),
            col("longitude").cast("double"),
            col("timezone"),
            # Keep local_time as a plain string — no timezone conversion needed
            # since the extract already writes "YYYY-MM-DD HH:mm" in local time
            col("local_time").alias("local_time"),
            col("temp_c").cast("double"),
            _clean_str("condition_text").alias("condition_text"),
            col("wind_kph").cast("double"),
            col("humidity").cast("double"),
            col("feelslike_c").cast("double"),
            col("pressure_mb").cast("double"),
            col("precip_mm").cast("double"),
            col("uv").cast("double"),
            col("cloud").cast("double"),
            col("wind_dir"),
            col("source_query"),
            # Keep extracted_at as a plain string too — strip timezone in load step
            col("extracted_at").alias("extracted_at"),
        )
        .where(col("location_name").isNotNull())
        .orderBy("local_time")
    )

    target_path = Path(output_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    # Write a partitioned CSV directory. We coalesce to 1 partition so the
    # output contains a single `part-*.csv` file which we then merge below
    # into a single named CSV for easy downstream consumption.
    cleaned_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(target_path))

    # Stop the Spark session to free resources before performing local IO.
    spark.stop()

    # Merge the produced `part-*.csv` file(s) into a single, human-friendly
    # `weather_cleaned.csv` file one level above the Spark output directory.
    merged_csv = target_path.parent / "weather_cleaned.csv"
    part_files = sorted(glob.glob(str(target_path / "part-*.csv")))
    if part_files:
        with merged_csv.open("w", encoding="utf-8") as out_fh:
            for i, part in enumerate(part_files):
                with open(part, encoding="utf-8") as in_fh:
                    if i > 0:
                        next(in_fh, None)  # skip header on subsequent parts
                    shutil.copyfileobj(in_fh, out_fh)
        print(f"Merged CSV written → {merged_csv}")

    return str(target_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform weather history data.")
    parser.add_argument("--input", default=DEFAULT_RAW_PATH, help="Path to the raw JSON-lines file")
    parser.add_argument("--output", default=DEFAULT_PROCESSED_PATH, help="Directory for transformed CSV output")
    args = parser.parse_args()
    transform_weather(args.input, args.output)


if __name__ == "__main__":
    main()
