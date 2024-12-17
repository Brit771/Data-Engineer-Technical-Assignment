from config.logging_config import logger
from db.connection import get_db_connection
from extract.validate_data import validate_weather_data
from transform.transform_weather import extract_location_data, extract_time_data, extract_fact_data


def execute_with_fetch(cursor, query, params=None, fetchone=False):
    """Execute a query and optionally fetch one or all results."""
    try:
        cursor.execute(query, params or ())
        return cursor.fetchone() if fetchone else cursor.fetchall()
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise


def fetch_single_value(cursor, query, params):
    """Fetch a single value from the database."""
    result = execute_with_fetch(cursor, query, params, fetchone=True)
    return result[0] if result else None


def get_or_insert_location(cursor, city, data):
    """Retrieve or insert location data into dim_location and return location_id."""
    location_data = extract_location_data(city, data)
    if not location_data:
        logger.error(f"Failed to extract location data for city '{city}'.")
        return None

    # Check if location already exists
    location_id = fetch_single_value(
        cursor,
        "SELECT location_id FROM dim_location WHERE city = %s AND country = %s;",
        (location_data[0], location_data[1]),
    )
    if location_id:
        logger.debug(f"Location ID {location_id} already exists for city '{city}'.")
        return location_id

    # Insert new location
    location_id = fetch_single_value(
        cursor,
        """
        INSERT INTO dim_location (city, country, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        RETURNING location_id;
        """,
        location_data,
    )
    logger.debug(f"Inserted new location '{city}' with ID {location_id}.")
    return location_id


def get_or_insert_time(cursor, timestamp):
    """Insert time data into dim_time table if not exists and return timestamp_id."""
    time_data = extract_time_data(timestamp)
    return fetch_single_value(
        cursor,
        """
        INSERT INTO dim_time (year, month, day, hour, minute)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (year, month, day, hour, minute) DO UPDATE SET year = EXCLUDED.year
        RETURNING timestamp_id;
        """,
        time_data,
    )


def fact_data_exists(cursor, location_id, time_id):
    """Check if fact data already exists for the given location and time."""
    return fetch_single_value(
        cursor,
        "SELECT 1 FROM weather_fact WHERE location_id = %s AND timestamp_id = %s;",
        (location_id, time_id),
    ) is not None


def insert_fact_data(cursor, location_id, time_id, data):
    """Insert weather fact data into weather_fact table."""
    if fact_data_exists(cursor, location_id, time_id):
        logger.info(f"Fact data already exists for location_id {location_id}, time_id {time_id}. Skipping.")
        return

    fact_data = extract_fact_data(location_id, time_id, data)
    cursor.execute(
        """
        INSERT INTO weather_fact (timestamp_id, location_id, temperature, humidity, wind_speed, pressure)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        fact_data,
    )
    logger.debug(f"Inserted fact data for location_id {location_id}, time_id {time_id}.")


def process_raw_record(cursor, raw_id, city, data):
    """Process a single raw record: insert location, time, and fact data."""
    try:
        cursor.execute("SAVEPOINT before_insert")  # Partial rollback for this record

        location_id = get_or_insert_location(cursor, city, data)
        if location_id is None:
            logger.error(f"Failed to process location data for raw_id {raw_id}.")
            cursor.execute("ROLLBACK TO SAVEPOINT before_insert")
            return False

        time_id = get_or_insert_time(cursor, data["dt"])
        insert_fact_data(cursor, location_id, time_id, data)

        cursor.execute("UPDATE weather_raw SET processed = TRUE WHERE id = %s;", (raw_id,))
        cursor.execute("RELEASE SAVEPOINT before_insert")  # Finalize changes
        logger.info(f"Processed raw_id {raw_id}, city '{city}'.")
        return True

    except Exception as e:
        logger.error(f"Error processing raw_id {raw_id}, city '{city}': {e}")
        cursor.execute("ROLLBACK TO SAVEPOINT before_insert")
        return False


def populate_data_warehouse():
    """Transform raw data into a structured data warehouse format."""
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            raw_records = execute_with_fetch(
                cursor, "SELECT id, city, data FROM weather_raw WHERE NOT processed"
            )
            logger.info(f"Found {len(raw_records)} unprocessed records.")

            for raw_id, city, data in raw_records:
                logger.debug(f"Processing record - raw_id: {raw_id}, city: '{city}', data: {data}")
                if not data or not validate_weather_data(city, data):
                    logger.error(f"Invalid data for raw_id {raw_id}, city '{city}'. Skipping.")
                    continue

                process_raw_record(cursor, raw_id, city, data)

            conn.commit()
            logger.info("Data warehouse update completed successfully.")
    except Exception as e:
        logger.error(f"Critical error while populating data warehouse: {e}")
