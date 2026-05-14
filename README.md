# Weather API ETL Pipeline

An end-to-end ETL pipeline that fetches hourly weather history from [WeatherAPI.com](https://www.weatherapi.com/), transforms it with PySpark, loads it into MySQL, and runs automated data quality checks — all orchestrated by Apache Airflow running in Docker.

---

## Architecture

```
WeatherAPI.com
      │
      ▼
┌─────────────┐     JSON-lines      ┌──────────────────┐     CSV
│   Extract   │ ──────────────────► │    Transform     │ ──────────────►
│  (Python)   │   data/raw/         │   (PySpark)      │  data/processed/
└─────────────┘   weather.json      └──────────────────┘
                  weather.csv         weather_cleaned.csv
                                              │
                                              ▼
                                    ┌──────────────────┐
                                    │      Load        │
                                    │  (SQLAlchemy)    │
                                    └──────────────────┘
                                              │
                                              ▼
                                    ┌──────────────────┐
                                    │  Data Quality    │
                                    │    Checks        │
                                    └──────────────────┘
                                              │
                                              ▼
                                       MySQL Database
                                      weather_current
```

All four steps run as tasks inside a single Airflow DAG scheduled hourly.

---

## Project Structure

```
Weather_API_ETL_Pipelines/
├── dags/
│   └── weather_etl_dag.py        # Airflow DAG definition + data quality checks
├── jobs/
│   ├── extract_weather.py        # Step 1 – fetch from WeatherAPI.com
│   ├── transform_weather.py      # Step 2 – clean & cast with PySpark
│   └── load_weather.py           # Step 3 – insert into MySQL
├── data/
│   ├── raw/                      # weather.json + weather.csv (extract output)
│   └── processed/                # weather_cleaned_csv/ + weather_cleaned.csv
├── sql/
│   └── create_weather_table.sql  # Table DDL (reference / local setup)
├── docker/
│   └── mysql/initdb/
│       └── 01_weather_etl.sql    # Auto-runs on first Docker MySQL start
├── Dockerfile                    # Custom Airflow image with Java 17 + PySpark
├── docker-compose.yml            # MySQL + Airflow (webserver + scheduler)
├── requirements.txt
└── .env                          # Local environment variables (not committed)
```

---

## Pipeline Steps

### 1. Extract (`jobs/extract_weather.py`)

- Calls `/history.json` on WeatherAPI.com for the **last 3 full days + today**
- Resolves the location's timezone from the API response so "today" and the cutoff hour are always in **local time**, not UTC
- Keeps **one row per hour** — 24 rows per past day, hours 00:00 → current local hour for today
- Writes two files to `data/raw/`:
  - `weather.json` — JSON-lines format consumed by the Spark transform
  - `weather.csv` — human-readable raw snapshot

### 2. Transform (`jobs/transform_weather.py`)

- Reads the JSON-lines file with PySpark
- Cleans string columns (lowercase, strips special characters)
- Casts numeric and datetime columns to proper types
- Orders rows by `local_time`
- Writes two outputs to `data/processed/`:
  - `weather_cleaned_csv/` — Spark partitioned directory
  - `weather_cleaned.csv` — single merged CSV for easy inspection

### 3. Load (`jobs/load_weather.py`)

- Reads `weather_cleaned.csv`
- **Truncates** `weather_current` then reloads the full window on every run — no duplicates, always consistent
- Normalises datetime strings: Spark writes ISO 8601 with timezone offset (e.g. `2026-05-11T00:00:00.000+07:00`); the loader strips the offset while preserving the **wall-clock time** so MySQL `DATETIME` receives the correct local value
- Inserts in chunks of 500 rows via SQLAlchemy

### 4. Data Quality Check (`dags/weather_etl_dag.py`)

Runs automatically after every successful load. The Airflow task turns red and the run is marked failed if any check does not pass:

| Check | Rule |
|---|---|
| Row count | ≥ 72 rows (3 days × 24 h) |
| Null check | No NULLs in `location_name`, `local_time`, `temp_c`, `humidity` |
| Temperature range | `temp_c` between -10 °C and 60 °C |
| Humidity range | `humidity` between 0 and 100 |
| Freshness | Most recent `extracted_at` (UTC) within the last 25 hours |

---

## Database Schema

```sql
CREATE TABLE weather_current (
    id             BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    location_name  VARCHAR(120)  NOT NULL,
    region         VARCHAR(120),
    country        VARCHAR(120),
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    timezone       VARCHAR(120),
    local_time     DATETIME,             -- hourly slot in the location's local time
    temp_c         DOUBLE PRECISION,
    condition_text VARCHAR(255),
    wind_kph       DOUBLE PRECISION,
    humidity       DOUBLE PRECISION,
    feelslike_c    DOUBLE PRECISION,
    pressure_mb    DOUBLE PRECISION,
    precip_mm      DOUBLE PRECISION,
    uv             DOUBLE PRECISION,
    cloud          DOUBLE PRECISION,
    wind_dir       VARCHAR(10),
    source_query   VARCHAR(255),
    extracted_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_location_slot (location_name, local_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

> If you created the table before the `last_updated` column was removed, run this migration in MySQL Workbench:
> ```sql
> ALTER TABLE weather_current DROP COLUMN last_updated;
> ```

---

## Quick Start (Docker)

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- A free API key from [weatherapi.com](https://www.weatherapi.com/)

### 1. Configure environment

Edit `.env` and set your API key and target location:

```env
WEATHER_API_KEY=your_api_key_here
WEATHER_QUERY=Cambodia          # city, country, or lat,lon
```

> **Apple Silicon vs Intel/AMD:** The `Dockerfile` and `docker-compose.yml` default to `java-17-openjdk-arm64` (Apple Silicon). If you are on an Intel/AMD machine, change both occurrences to `java-17-openjdk-amd64`.

### 2. Build and start

```bash
docker compose build
docker compose up -d
```

First start takes ~2 minutes. The `airflow-init` container runs once to migrate the Airflow metadata database and create the admin user, then exits automatically.

### 3. Open Airflow

```
http://localhost:8080
Username: admin
Password: admin
```

### 4. Trigger a run

In the Airflow UI, find `weather_api_etl_pipeline` and click the ▶ play button. Or from the terminal:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger weather_api_etl_pipeline
```

The four tasks run in sequence: `extract_weather → transform_weather → load_weather → data_quality_check`.

### 5. Connect to the Docker MySQL (optional)

The Docker MySQL is exposed on host port **3307** to avoid conflicts with a local MySQL instance:

```
Host:     127.0.0.1
Port:     3307
Database: weather_etl
User:     weather
Password: weather
```

### Stop everything

```bash
docker compose down        # stop containers, keep data volumes
docker compose down -v     # stop containers and delete all data
```

---

## Running Locally (without Docker)

### Prerequisites

- Python 3.9+
- Java 17 (required by PySpark)
- A running MySQL instance

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create the database and table:

```bash
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS weather_etl CHARACTER SET utf8mb4;"
mysql -u root -p weather_etl < sql/create_weather_table.sql
```

Update `DATABASE_URL` in `.env` to point to your local MySQL, e.g.:

```env
DATABASE_URL=mysql+pymysql://root:yourpassword@127.0.0.1:3306/weather_etl
```

### Run each step manually

```bash
python jobs/extract_weather.py
python jobs/transform_weather.py
python jobs/load_weather.py
```

---

## Configuration Reference

All settings are controlled via environment variables (`.env` for local runs, `docker-compose.yml` for Docker).

| Variable | Default | Description |
|---|---|---|
| `WEATHER_API_KEY` | *(required)* | WeatherAPI.com API key |
| `WEATHER_API_BASE_URL` | `https://api.weatherapi.com/v1` | API base URL |
| `WEATHER_QUERY` | `Cambodia` | Location to fetch (city, country, or lat,lon) |
| `WEATHER_AQI` | `no` | Include air quality index (`yes`/`no`) |
| `WEATHER_LANGUAGE` | `en` | Response language |
| `WEATHER_TIMEOUT_SECONDS` | `30` | HTTP request timeout in seconds |
| `RAW_WEATHER_PATH` | `./data/raw/weather.json` | Output path for the extract step |
| `PROCESSED_WEATHER_PATH` | `./data/processed/weather_cleaned_csv` | Output path for the transform step |
| `DATABASE_URL` | *(required)* | SQLAlchemy connection string |
| `WEATHER_TABLE_NAME` | `weather_current` | Target MySQL table name |

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.3 |
| Data processing | PySpark 3.5.1 |
| Data extraction | Python `requests` 2.32 |
| Database | MySQL 8.0 |
| ORM / DB client | SQLAlchemy 1.4 + PyMySQL 1.1 |
| Containerisation | Docker + Docker Compose |
| Language | Python 3.11 |
# WeatherAPI_ETL_Pipeline_Airflow_PySpark
