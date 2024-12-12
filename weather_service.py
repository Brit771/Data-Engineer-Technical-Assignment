import os
import time
import logging
from datetime import datetime, timezone
import requests
import psycopg2
from psycopg2.extras import Json
from apscheduler.schedulers.background import BackgroundScheduler

# -----------------------
# Configuration & Logging
# -----------------------
# Environment Variables
API_KEY = os.getenv("OPENWEATHER_API_KEY")
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}
OPENWEATHER_URL = "http://api.openweathermap.org/data/2.5/weather"
CITIES = ['New York', 'London', 'Tokyo'] 

# Logging Configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# ---------------------
# Helper Functions
# ---------------------
def get_db_connection():
    """Create and return a new database connection."""
    return psycopg2.connect(**DB_CONFIG)

 
def initialize_database():
    """Initialize the database schema from a schema.sql file."""
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            with open('schema.sql', 'r') as sql_file:
                schema = sql_file.read()
            cursor.execute(schema)
            conn.commit()
            logger.info("Database schema initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing database schema: {e}")

# ---------------------
# Raw Data Extraction
# ---------------------
def fetch_weather_data(city):
    """Fetch weather data from OpenWeather API for a given city."""
    params = {"q": city, "appid": API_KEY}
    try:
        response = requests.get(OPENWEATHER_URL, params=params)
        response.raise_for_status()
        logger.info(f"Weather data fetched for {city}.")
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching weather data for {city}: {e}")
        return None
    

def save_raw_data(city, data):
    """Save raw weather data to the weather_raw table."""
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            # Check if data already exists for the city and timestamp
            cursor.execute(
                "SELECT id FROM weather_raw WHERE city = %s AND data->>'dt' = %s",
                (city, str(data["dt"]))
            )
            existing = cursor.fetchone()

            if not existing:
                # Insert only if the data is not already in the raw table
                cursor.execute(
                    "INSERT INTO weather_raw (city, data, processed) VALUES (%s, %s, %s)",
                    (city, Json(data), False)
                )
                conn.commit()
                logger.info(f"Raw data for {city} saved to database.")
            else:
                logger.info(f"Raw data for {city} with timestamp {data['dt']} already exists.")
    except Exception as e:
        logger.error(f"Error saving raw data for {city}: {e}")

# ---------------------
# Data Transformation
# ---------------------
def extract_location_data(city, data):
    """Extract location information from raw data."""
    return city, data.get("sys", {}).get("country"), data.get("coord", {}).get("lat"), data.get("coord", {}).get("lon")


def extract_time_data(timestamp):
    """Extract time dimension data from a UNIX timestamp."""
    from datetime import datetime
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.year, dt.month, dt.day, dt.hour, dt.minute

def extract_fact_data(location_id, time_id, data):
    """Extract fact weather data from raw data."""
    return (
        location_id,
        time_id,
        data.get("main", {}).get("temp"),
        data.get("main", {}).get("humidity"),
        data.get("wind", {}).get("speed"),
        data.get("main", {}).get("pressure")
    )

# ---------------------
# Data Insertion Functions
# ---------------------

def insert_location_data(cursor, city, data):
    """Insert location data into the dim_location table."""
    location_data = extract_location_data(city, data)
    cursor.execute(
        """
        INSERT INTO dim_location (city, country, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (city, country) DO UPDATE SET latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
        RETURNING location_id;
        """,
        location_data
    )
    return cursor.fetchone()[0]

def insert_time_data(cursor, timestamp):
    """Insert time data into the dim_time table."""
    time_data = extract_time_data(timestamp)
    cursor.execute(
        """
        INSERT INTO dim_time (year, month, day, hour, minute)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (year, month, day, hour, minute) DO NOTHING
        RETURNING timestamp_id;
        """,
        time_data
    )
    return cursor.fetchone()[0]

def insert_fact_data(cursor, location_id, time_id, data):
    """Insert weather fact data into the weather_fact table."""
    fact_data = extract_fact_data(location_id, time_id, data)
    cursor.execute(
        """
        INSERT INTO weather_fact (timestamp_id, location_id, temperature, humidity, wind_speed, pressure)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        fact_data
    )


# ---------------------
# Warehouse Population
# ---------------------

def populate_data_warehouse():
    """Transform raw data into a structured data warehouse format."""
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT id, city, data FROM weather_raw WHERE NOT processed")
            raw_records = cursor.fetchall()

            for record in raw_records:
                raw_id, city, data = record

                location_id = insert_location_data(cursor, city, data)
                time_id = insert_time_data(cursor, data["dt"])
                insert_fact_data(cursor, location_id, time_id, data)

                cursor.execute("UPDATE weather_raw SET processed = TRUE WHERE id = %s", (raw_id,))

            conn.commit()
            logger.info("Data warehouse updated successfully.")
    except Exception as e:
        logger.error(f"Error populating data warehouse: {e}")



# ---------------------
# Data Workflow
# ---------------------
def process_weather_data():
    """Fetch, store raw data, and update the data warehouse."""
    for city in CITIES:
        data = fetch_weather_data(city)
        if data:
            save_raw_data(city, data)
    populate_data_warehouse()


# ---------------------
# Scheduler Functions
# ---------------------
def start_scheduler():
    """Start the APScheduler for periodic weather data processing."""
    scheduler = BackgroundScheduler()
    scheduler.add_job(process_weather_data, 'interval', minutes=60)
    scheduler.start()
    logger.info("Scheduler started.")
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    logger.info("Initializing the database schema...")
    initialize_database()

    logger.info("Starting the initial data fetch and processing...")
    process_weather_data()

    logger.info("Starting the scheduler...")
    start_scheduler()