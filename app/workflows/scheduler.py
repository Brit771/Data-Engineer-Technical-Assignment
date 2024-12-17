import time
from config.logging_config import logger
from workflows.process_weather_data import process_weather_data
from apscheduler.schedulers.background import BackgroundScheduler


def start_scheduler():
    """Start the APScheduler for periodic weather data processing."""
    scheduler = BackgroundScheduler()
    scheduler.add_job(process_weather_data, 'interval', minutes=60)
    scheduler.start()
    logger.info("Scheduler started.")
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped.")