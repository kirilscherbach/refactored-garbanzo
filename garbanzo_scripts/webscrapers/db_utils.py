import psycopg2
import os
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

@contextmanager
def db_cursor():
    db_host = "localhost"
    db_name = "scraper_db"
    db_user = "scraper"
    db_password = os.environ.get("KS_PG_PASSWORD")
    conn = psycopg2.connect(f"dbname={db_name} user={db_user} host={db_host} password='{db_password}'")
    cur = conn.cursor()
    try:
        yield cur
        logger.info("Committing transaction")
        conn.commit()
    finally:
        logger.info("Closing cursor and connection")
        cur.close()
        conn.close()

