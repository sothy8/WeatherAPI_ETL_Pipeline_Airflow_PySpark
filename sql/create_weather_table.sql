CREATE TABLE IF NOT EXISTS weather_current (
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

    -- one row per location per hour
    UNIQUE KEY uq_location_slot (location_name, local_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
