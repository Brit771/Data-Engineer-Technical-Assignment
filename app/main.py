from config.logging_config import logger
from workflows.scheduler import start_scheduler
from db.initialize_database import initialize_database
from workflows.process_weather_data import process_weather_data

if __name__ == "__main__":
    logger.info("Initializing the database schema...")
    initialize_database()

    logger.info("Starting the initial data fetch and processing...")
    process_weather_data()

    logger.info("Starting the scheduler...")
    start_scheduler()