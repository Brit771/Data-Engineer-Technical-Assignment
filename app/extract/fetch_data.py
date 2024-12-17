import requests
from config.logging_config import logger
from config.constants import API_KEY, OPENWEATHER_URL

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