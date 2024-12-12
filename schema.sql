CREATE TABLE IF NOT EXISTS weather_raw (
    id SERIAL PRIMARY KEY,
    city TEXT,
    data JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    country TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    UNIQUE (city, country)
);

CREATE TABLE IF NOT EXISTS dim_time (
    timestamp_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL,
    minute INT NOT NULL,
    UNIQUE (year, month, day, hour, minute)
);

CREATE TABLE IF NOT EXISTS weather_fact (
    fact_id SERIAL PRIMARY KEY,
    timestamp_id INT REFERENCES dim_time(timestamp_id),
    location_id INT REFERENCES dim_location(location_id),
    temperature DOUBLE PRECISION,
    humidity INT,
    wind_speed DOUBLE PRECISION,
    pressure INT
);
