"""
Stage database connection management.

Provides connection utilities for the staging database used in OSM collection
and other intermediate data processing workflows.
"""

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

STAGE_DB_URL = os.getenv("STAGE_DB_URL")


def get_stage_db_url():
    """Get the stage database URL from environment."""
    return STAGE_DB_URL


def get_stage_db_connection():
    """
    Get connection to stage database.
    
    Returns:
        psycopg2 connection object
        
    Raises:
        ValueError: If STAGE_DB_URL not found in environment
    """
    if not STAGE_DB_URL:
        raise ValueError("STAGE_DB_URL not found in environment")
    conn = psycopg2.connect(STAGE_DB_URL)
    return conn
