
from config.logging_config import logger
from db.connection import get_db_connection
from extract.validate_data import validate_weather_data
from transform.transform_weather import extract_location_data, extract_time_data, extract_fact_data

def insert_location_data(cursor, city, data):
    """Insert location data into dim_location and return location_id"""
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


def populate_data_warehouse():
    """Transform raw data into a structured data warehouse format."""
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT id, city, data FROM weather_raw WHERE NOT processed")
            raw_records = cursor.fetchall()

            for record in raw_records:
                raw_id, city, data = record

                if not validate_weather_data(city, data):
                    continue

                location_id = insert_location_data(cursor, city, data)
                time_id = insert_time_data(cursor, data["dt"])
                insert_fact_data(cursor, location_id, time_id, data)

                cursor.execute("UPDATE weather_raw SET processed = TRUE WHERE id = %s", (raw_id,))

            conn.commit()
            logger.info("Data warehouse updated successfully.")
    except Exception as e:
        logger.error(f"Error populating data warehouse: {e}")
