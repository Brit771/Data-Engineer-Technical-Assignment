import requests
import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# Fetch weather data from OpenWeatherMap
def fetch_weather_data(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Weather data fetched for {city}.")
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

# Save raw data to PostgreSQL
def save_to_postgres(data):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )

        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_raw (
                id SERIAL PRIMARY KEY,
                city TEXT,
                data JSONB,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute(
            "INSERT INTO weather_raw (city, data) VALUES (%s, %s)",
            (data['name'], data)
        )
        conn.commit()
        cursor.close()
        conn.close()
        print("Data saved to PostgreSQL.")

    except Exception as e:
        print(f"Error saving data to Postgres: {e}")

# Periodic task to fetch and store data
def fetch_and_store():
    city = "London"
    data = fetch_weather_data(city)
    if data:
        save_to_postgres(data)

# Schedule tasks using APScheduler
def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_and_store, 'interval', minutes=60)  # Fetch every hour
    scheduler.start()
    print("Scheduler started. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Scheduler stopped.")

if __name__ == "__main__":
    start_scheduler()
