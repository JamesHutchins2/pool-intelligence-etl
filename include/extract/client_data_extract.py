"""
Extract module for client_data_update_etl pipeline.

This module handles:
1. Extracting new pool-equipped listings from realestate DB
2. Extracting removed pool-equipped listings from realestate DB
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, Any
import logging

import pandas as pd

from include.db.connections import get_listing_db_connection

logger = logging.getLogger(__name__)


def extract_new_pool_listings(workdir: str) -> Dict[str, Any]:
    """
    Extract all listings added in the last 2 days that have pools.
    
    Args:
        workdir: Working directory for output files
        
    Returns:
        Dict with:
            - parquet_path: Path to output file
            - row_count: Number of listings extracted
    """
    logger.info("="*60)
    logger.info("EXTRACTING NEW POOL LISTINGS")
    logger.info("="*60)
    
    # Look back 2 days (today + yesterday)
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=10)).replace(hour=0, minute=0, second=0, microsecond=0)
    logger.info(f"Cutoff date: {cutoff_date}")
    
    query = """
        SELECT 
            l.mls_id,
            l.address_number,
            l.street_name,
            l.municipality,
            l.province_state,
            l.postal_code,
            l.lat,
            l.lon,
            l.date_collected,
            l.description,
            l.bedrooms,
            l.bathrooms,
            l.size_sqft,
            l.stories,
            l.house_cat,
            l.price,
            l.pool_type,
            l.pool_mentioned
        FROM listing l
        WHERE l.date_collected >= %s
          AND l.pool_mentioned = true
        ORDER BY l.date_collected DESC;
    """
    
    conn = get_listing_db_connection()
    try:
        df = pd.read_sql(query, conn, params=(cutoff_date,))
        logger.info(f"Extracted {len(df)} new pool listings")
        
        if df.empty:
            logger.warning("No new pool listings found")
            out_path = f"{workdir}/new_pool_listings.parquet"
            df.to_parquet(out_path, index=False)
            return {
                "parquet_path": out_path,
                "row_count": 0,
            }
        
        # Log sample
        logger.info(f"Sample MLS IDs: {df['mls_id'].head().tolist()}")
        logger.info(f"Pool types distribution:")
        for pool_type, count in df['pool_type'].value_counts().items():
            logger.info(f"  {pool_type}: {count}")
        
        out_path = f"{workdir}/new_pool_listings.parquet"
        df.to_parquet(out_path, index=False)
        logger.info(f"Saved to: {out_path}")
        logger.info("="*60)
        
        return {
            "parquet_path": out_path,
            "row_count": len(df),
        }
        
    finally:
        conn.close()


def extract_removed_pool_listings(workdir: str) -> Dict[str, Any]:
    """
    Extract all listings removed in the last 2 days that have pools.
    
    Args:
        workdir: Working directory for output files
        
    Returns:
        Dict with:
            - parquet_path: Path to output file
            - row_count: Number of listings extracted
    """
    logger.info("="*60)
    logger.info("EXTRACTING REMOVED POOL LISTINGS")
    logger.info("="*60)
    
    # Look back 30 days
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    logger.info(f"Cutoff date: {cutoff_date}")
    
    query = """
        SELECT 
            l.mls_id,
            l.address_number,
            l.street_name,
            l.municipality,
            l.province_state,
            l.postal_code,
            l.lat,
            l.lon,
            l.pool_type,
            lr.removal_date
        FROM listing l
        JOIN listing_removal lr ON lr.mls_id = l.mls_id
        WHERE lr.removal_date >= %s
          AND l.pool_mentioned = true
        ORDER BY lr.removal_date DESC;
    """
    
    conn = get_listing_db_connection()
    try:
        df = pd.read_sql(query, conn, params=(cutoff_date,))
        logger.info(f"Extracted {len(df)} removed pool listings")
        
        if df.empty:
            logger.warning("No removed pool listings found")
            out_path = f"{workdir}/removed_pool_listings.parquet"
            df.to_parquet(out_path, index=False)
            return {
                "parquet_path": out_path,
                "row_count": 0,
            }
        
        # Log sample
        logger.info(f"Sample MLS IDs: {df['mls_id'].head().tolist()}")
        
        out_path = f"{workdir}/removed_pool_listings.parquet"
        df.to_parquet(out_path, index=False)
        logger.info(f"Saved to: {out_path}")
        logger.info("="*60)
        
        return {
            "parquet_path": out_path,
            "row_count": len(df),
        }
        
    finally:
        conn.close()
