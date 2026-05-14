"""Extract weather data from WeatherAPI.com.

Strategy
--------
* On every run we fetch **history** for the last 3 full days plus today.
* WeatherAPI /history.json returns one record per hour → we keep exactly
  one row per hour (24 rows per past day, hours 00–now for today).
* Output is written in two formats side-by-side:
    - JSON-lines  (RAW_WEATHER_PATH, e.g. data/raw/weather.json)
      → used by the Spark transform step
    - CSV         (same directory, weather.csv)
      → human-readable raw snapshot
"""

from __future__ import annotations

import csv
import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

load_dotenv()

DEFAULT_OUTPUT_PATH = Path("./data/raw/weather.json")


def _api_key() -> str:
    key = os.getenv("WEATHER_API_KEY", "")
    if not key:
        raise ValueError("WEATHER_API_KEY is not set")
    return key


def _base_url() -> str:
    return os.getenv("WEATHER_API_BASE_URL", "https://api.weatherapi.com/v1").rstrip("/")


def _query() -> str:
    return os.getenv("WEATHER_QUERY", "London")


def _fetch_history_day(target_date: date) -> dict[str, Any]:
    """Fetch hourly history for a single calendar day."""
    url = f"{_base_url()}/history.json"
    params = {
        "key": _api_key(),
        "q": _query(),
        "dt": target_date.strftime("%Y-%m-%d"),
        "aqi": os.getenv("WEATHER_AQI", "no"),
        "lang": os.getenv("WEATHER_LANGUAGE", "en"),
    }
    resp = requests.get(url, params=params, timeout=int(os.getenv("WEATHER_TIMEOUT_SECONDS", "30")))
    resp.raise_for_status()
    return resp.json()


def _hourly_rows(
    hourly_records: list[dict[str, Any]],
    location: dict[str, Any],
    cutoff_hour: int | None,
    extracted_at: str,
) -> list[dict[str, Any]]:
    """
    Build one flat row per hourly record.
    If *cutoff_hour* is set, only include records whose hour <= cutoff_hour.
    """
    rows: list[dict[str, Any]] = []
    for rec in hourly_records:
        # rec["time"] looks like "2026-05-14 07:00"
        rec_hour = int(rec.get("time", "0000-00-00 00:00").split(" ")[-1].split(":")[0])
        if cutoff_hour is not None and rec_hour > cutoff_hour:
            continue

        condition = rec.get("condition", {})
        row = {
            "location_name": location.get("name", ""),
            "region":        location.get("region", ""),
            "country":       location.get("country", ""),
            "latitude":      location.get("lat"),
            "longitude":     location.get("lon"),
            "timezone":      location.get("tz_id", ""),
            "local_time":    rec.get("time", ""),          # e.g. "2026-05-14 07:00"
            "temp_c":        rec.get("temp_c"),
            "condition_text": condition.get("text", ""),
            "wind_kph":      rec.get("wind_kph"),
            "humidity":      rec.get("humidity"),
            "feelslike_c":   rec.get("feelslike_c"),
            "pressure_mb":   rec.get("pressure_mb"),
            "precip_mm":     rec.get("precip_mm"),
            "uv":            rec.get("uv"),
            "cloud":         rec.get("cloud"),
            "wind_dir":      rec.get("wind_dir", ""),
            "source_query":  _query(),
            "extracted_at":  extracted_at,
        }
        rows.append(row)
    return rows


def extract_weather_data(output_path: str | None = None) -> str:
    """
    Fetch history for the last 3 full days + today (one row per hour)
    and write JSON-lines + CSV to the raw data directory.

    Returns the path of the JSON-lines file.
    """
    target_path = Path(output_path or os.getenv("RAW_WEATHER_PATH", str(DEFAULT_OUTPUT_PATH)))
    target_path.parent.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    extracted_at = now_utc.isoformat()

    # Resolve location timezone first so "today" and cutoff_hour are in local time.
    # We do a quick probe fetch to get tz_id before building the date list.
    probe = _fetch_history_day(now_utc.date())
    tz_id = probe.get("location", {}).get("tz_id", "UTC")
    try:
        location_tz: Any = ZoneInfo(tz_id)
    except Exception:
        location_tz = timezone.utc

    now_local = now_utc.astimezone(location_tz)
    today = now_local.date()                      # today in the location's timezone
    cutoff_hour_today = now_local.hour            # current hour locally

    # Last 3 full days (not including today, in local time)
    history_dates = [today - timedelta(days=d) for d in range(3, 0, -1)]
    all_dates = history_dates + [today]

    all_rows: list[dict[str, Any]] = []

    for target_date in all_dates:
        # Reuse the probe response for today to avoid a duplicate API call
        if target_date == today:
            payload = probe
        else:
            payload = _fetch_history_day(target_date)
        location = payload.get("location", {})

        forecast_days = payload.get("forecast", {}).get("forecastday", [])
        if not forecast_days:
            continue
        hourly_records = forecast_days[0].get("hour", [])

        # For today: only include hours up to the current local hour
        cutoff_hour = cutoff_hour_today if target_date == today else None

        rows = _hourly_rows(hourly_records, location, cutoff_hour, extracted_at)
        all_rows.extend(rows)

    # JSON-lines — consumed by the Spark transform
    lines = "\n".join(json.dumps(row) for row in all_rows)
    target_path.write_text(lines, encoding="utf-8")

    # CSV — human-readable raw snapshot
    csv_path = target_path.with_suffix(".csv")
    if all_rows:
        fieldnames = list(all_rows[0].keys())
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)

    print(f"Extracted {len(all_rows)} hourly records → {target_path} + {csv_path}")
    return str(target_path)


if __name__ == "__main__":
    print(extract_weather_data())
