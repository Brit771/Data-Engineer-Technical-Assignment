from datetime import datetime, timezone

def extract_location_data(city, data):
    """Extract location information from raw data."""
    return city, data.get("sys", {}).get("country"), data.get("coord", {}).get("lat"), data.get("coord", {}).get("lon")

def extract_time_data(timestamp):
    """Extract time dimension data from a UNIX timestamp."""
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
