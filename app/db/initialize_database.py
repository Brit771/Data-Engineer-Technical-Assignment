from config.logging_config import logger
from db.connection import get_db_connection

def initialize_database():
    """Initialize the database schema from a schema.sql file."""
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            with open('db/schema.sql', 'r') as sql_file:
                schema = sql_file.read()
            cursor.execute(schema)
            conn.commit()
            logger.info("Database schema initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing database schema: {e}")