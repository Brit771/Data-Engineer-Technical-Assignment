# Weather Data Pipeline Project

## Overview

This project is a data engineering assignment that fetches weather data from the OpenWeather API, stores raw data in a PostgreSQL database, and transforms it into a structured data warehouse for analysis.

### Features

- Fetch weather data for predefined cities.
- Store raw data in a `weather_raw` table.
- Transform and populate a data warehouse with `dim_location`, `dim_time`, and `weather_fact` tables.
- Perform data quality checks to ensure valid data.
- Scheduler for periodic updates.

---

## Project Structure

    data-engineer-technical-assignment/

    ├── main.py               # Main Python script for weather service
    ├── config/            
    ├────── constant.py           
    ├────── logging_config.py           
    ├── db/            
    ├────── connection.py
    ├────── initialize_database.py 
    ├────── schema.sql            # SQL schema for database initialization
    ├── extract/            
    ├────── fetch_data.py
    ├────── validate_data.py
    ├── transform/            
    ├────── transform_weather.py
    ├── load/            
    ├────── insert_raw_data.py
    ├────── insert_transformed_data.py
    ├── workflows/            
    ├────── process_weather_data.py
    ├────── scheduler.py
    ├── docker-compose.yml    # Docker setup for PostgreSQL and weather service
    ├── Dockerfile            # Docker image for the weather service
    ├── requirements.txt      # Python dependencies
    └── README.md             # Project documentation

---

## Prerequisites

1. **Docker**: Install Docker and Docker Compose.
2. **OpenWeather API Key**: Obtain an API key from [OpenWeather](https://openweathermap.org/api).

---

## Setup

1. Clone the repository

```bash
git clone https://github.com/Brit771/Data-Engineer-Technical-Assignment.git
cd data-engineer-technical-assignment
```

2. Save your API key as a Docker secret:

```bash
echo "your_api_key" > openweather_api_key
```

3. Start the project

```bash
docker-compose up --build
```

## Functionality

### Tables

- weather_raw: Stores raw JSON weather data.
- dim_location: Contains city and geographic data.
- dim_time: Stores date-time information.
- weather_fact: Stores weather metrics for analysis.

### Workflow

1. Raw Data

    - Fetch data from OpenWeather API for predefined cities.
    - Save new data to weather_raw with a processed flag.

2. Data Warehouse

    - Transform raw data into structured tables
        - dim_location: Location details.
        - dim_time: Time details.
        - weather_fact: Weather metrics.
    - Mark raw data as processed.

3. Scheduler
    - Runs every 60 minutes to fetch and process data.

## Future Improvements

1. **Parallel Execution**: Optimize performance by running data fetching and processing for each city in parallel using Python's multiprocessing or threading modules.

2. **Handle Invalid Data**: Save invalid data into a separate file (e.g., `invalid_data.json`) for review and correction, enabling reprocessing and proper database insertion.

3. **Add Tests**: Implement unit and integration tests for key functions, including API calls, data validation, and database operations.

## Logs and Validation

- Check logs to view pipeline activity:

```bash
docker logs weather_service
```

- Data quality checks ensure
  - No null values in critical fields.
  - Valid ranges for weather metrics.
  - Deduplication of rows.
