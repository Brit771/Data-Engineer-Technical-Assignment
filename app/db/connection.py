from config.constants import DB_CONFIG
import psycopg2


def get_db_connection():
    """Create and return a new database connection."""
    return psycopg2.connect(**DB_CONFIG)
