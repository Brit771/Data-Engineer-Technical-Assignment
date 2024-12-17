from config.constants import CITIES
from extract.fetch_data import fetch_weather_data
from load.insert_raw_data import save_raw_data
from load.insert_transformed_data import populate_data_warehouse

def process_weather_data():
    """Fetch, store raw data, and update the data warehouse."""
    for city in CITIES:
        data = fetch_weather_data(city)
        if data:
            save_raw_data(city, data)
    populate_data_warehouse()
