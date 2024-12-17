from datetime import datetime, timezone
from config.logging_config import logger

def extract_location_data(city, data):
    """Extract location information from raw data."""
    try:
        country = data.get("sys", {}).get("country")
        latitude = data.get("coord", {}).get("lat")
        longitude = data.get("coord", {}).get("lon")

        # Validate extracted data
        if latitude is None or longitude is None:
            logger.error(f"Missing latitude/longitude for city '{city}': {data}")
            return None

        return city, country, latitude, longitude
    except Exception as e:
        logger.error(f"Error extracting location data for city '{city}': {e}")
        return None


def extract_time_data(timestamp):
    """Extract time dimension data from a UNIX timestamp."""
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.year, dt.month, dt.day, dt.hour, dt.minute

def extract_fact_data(location_id, time_id, data):
    """Extract fact weather data from raw data."""
    main = data.get("main", {})
    fact_data = ( 
                time_id,
                location_id,
                main.get("temp"),
                main.get("humidity"),
                data.get("wind", {}).get("speed"),
                main.get("pressure")
                )
    
    return fact_data
