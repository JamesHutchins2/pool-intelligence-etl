import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2

LISTING_DATABASE_URL = os.getenv("LISTING_DATABASE_URL")
MASTER_DB_URL = os.getenv("MASTER_DB_URL")


def get_listing_database_url():
    return LISTING_DATABASE_URL


def get_master_database_url():
    return MASTER_DB_URL


def get_listing_db_connection():
    conn = psycopg2.connect(get_listing_database_url())
    return conn


def get_master_db_connection(db_url: str | None = None):
    """Get connection to master database (licenses table)"""
    if not MASTER_DB_URL and not db_url:
        raise ValueError("MASTER_DB_URL not found in environment")
    conn = psycopg2.connect(db_url or MASTER_DB_URL)
    return conn