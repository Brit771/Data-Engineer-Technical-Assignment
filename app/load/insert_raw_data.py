from psycopg2.extras import Json
from config.logging_config import logger
from db.connection import get_db_connection

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