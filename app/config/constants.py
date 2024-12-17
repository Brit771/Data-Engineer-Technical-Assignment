import os

# Load API key from Docker secret
with open('/run/secrets/openweather_api_key', 'r') as f:
    API_KEY = f.read().strip()

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}

# OpenWeather API configuration
OPENWEATHER_URL = "http://api.openweathermap.org/data/2.5/weather"
CITIES = ['New York', 'London', 'Tokyo']
