"""
Stage Data Extraction Module

Extracts pending assignments and related pool/address data from stage database.
"""

import logging
from typing import Dict, Any, List, Tuple
import pandas as pd
from include.db.stage_connections import get_stage_db_connection

logger = logging.getLogger(__name__)


def extract_pending_assignments(workdir: str) -> Dict[str, Any]:
    """
    Extract all unprocessed assignments from stage database.
    
    Args:
        workdir: Working directory for output files
    
    Returns:
        Dict with:
            - parquet_path: Path to assignments parquet
            - assignment_count: Number of pending assignments
            - unique_pool_ids: List of unique pool IDs
            - unique_address_ids: List of unique address IDs
    """
    logger.info("="*60)
    logger.info("STAGE EXTRACTION - Starting")
    logger.info("="*60)
    
    get_assignments_sql = """
    SELECT * FROM assignment WHERE uploaded = FALSE OR uploaded IS NULL;
    """
    
    conn = get_stage_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute(get_assignments_sql)
        assignments = cur.fetchall()
        logger.info(f"Found {len(assignments)} pending assignments")
        
        if not assignments:
            logger.info("No pending assignments to process")
            return {
                "parquet_path": None,
                "assignment_count": 0,
                "unique_pool_ids": [],
                "unique_address_ids": [],
            }
        
        # Create DataFrame
        columns = ["id", "address", "pool", "uploaded"]
        df = pd.DataFrame(assignments, columns=columns)
        
        # Get unique IDs
        unique_pool_ids = df['pool'].unique().tolist()
        unique_address_ids = df['address'].unique().tolist()
        
        logger.info(f"Unique pools: {len(unique_pool_ids)}")
        logger.info(f"Unique addresses: {len(unique_address_ids)}")
        
        # Save to parquet
        output_path = f"{workdir}/assignments.parquet"
        df.to_parquet(output_path, index=False)
        
        logger.info("="*60)
        logger.info("STAGE EXTRACTION - Complete")
        logger.info("="*60)
        logger.info(f"Saved to: {output_path}")
        logger.info("="*60)
        
        return {
            "parquet_path": output_path,
            "assignment_count": len(df),
            "unique_pool_ids": unique_pool_ids,
            "unique_address_ids": unique_address_ids,
        }
    
    except Exception as e:
        logger.error(f"Failed to extract assignments: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()


def fetch_pool_and_address_details(
    pool_ids: List[int],
    address_ids: List[int],
    workdir: str
) -> Dict[str, Any]:
    """
    Fetch detailed pool and address records from stage database.
    
    Args:
        pool_ids: List of pool IDs to fetch
        address_ids: List of address IDs to fetch
        workdir: Working directory for output files
    
    Returns:
        Dict with:
            - pools_parquet: Path to pools parquet
            - addresses_parquet: Path to addresses parquet
            - pool_count: Number of pools fetched
            - address_count: Number of addresses fetched
    """
    logger.info("="*60)
    logger.info("DETAIL FETCH - Starting")
    logger.info("="*60)
    
    if not pool_ids or not address_ids:
        logger.warning("No pool or address IDs to fetch")
        return {
            "pools_parquet": None,
            "addresses_parquet": None,
            "pool_count": 0,
            "address_count": 0,
        }
    
    get_pools_sql = "SELECT * FROM pool WHERE id IN %s;"
    get_addresses_sql = "SELECT * FROM address WHERE id IN %s;"
    
    conn = get_stage_db_connection()
    cur = conn.cursor()
    
    try:
        # Fetch pools
        cur.execute(get_pools_sql, (tuple(pool_ids),))
        pool_details = cur.fetchall()
        pool_columns = ['id', 'pool_id', 'lat', 'lon', 'pool_type']
        pools_df = pd.DataFrame(pool_details, columns=pool_columns)
        logger.info(f"Fetched {len(pools_df)} pool records")
        
        # Fetch addresses
        cur.execute(get_addresses_sql, (tuple(address_ids),))
        address_details = cur.fetchall()
        address_columns = [
            'id', 'address_id', 'address_number', 'lat', 'lon', 
            'postal_code', 'street_name', 'province_state', 
            'country', 'geom', 'municipality'
        ]
        addresses_df = pd.DataFrame(address_details, columns=address_columns)
        logger.info(f"Fetched {len(addresses_df)} address records")
        
        # Save to parquet
        pools_path = f"{workdir}/pools_details.parquet"
        addresses_path = f"{workdir}/addresses_details.parquet"
        
        pools_df.to_parquet(pools_path, index=False)
        addresses_df.to_parquet(addresses_path, index=False)
        
        logger.info("="*60)
        logger.info("DETAIL FETCH - Complete")
        logger.info("="*60)
        logger.info(f"Pools saved to: {pools_path}")
        logger.info(f"Addresses saved to: {addresses_path}")
        logger.info("="*60)
        
        return {
            "pools_parquet": pools_path,
            "addresses_parquet": addresses_path,
            "pool_count": len(pools_df),
            "address_count": len(addresses_df),
        }
    
    except Exception as e:
        logger.error(f"Failed to fetch details: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()


def merge_assignment_data(
    assignments_parquet: str,
    pools_parquet: str,
    addresses_parquet: str,
    workdir: str
) -> Dict[str, Any]:
    """
    Merge assignments with pool and address details into single dataset.
    
    Args:
        assignments_parquet: Path to assignments parquet
        pools_parquet: Path to pools parquet
        addresses_parquet: Path to addresses parquet
        workdir: Working directory for output
    
    Returns:
        Dict with:
            - parquet_path: Path to merged data parquet
            - record_count: Number of merged records
    """
    logger.info("="*60)
    logger.info("DATA MERGE - Starting")
    logger.info("="*60)
    
    # Read data
    assignments_df = pd.read_parquet(assignments_parquet)
    pools_df = pd.read_parquet(pools_parquet)
    addresses_df = pd.read_parquet(addresses_parquet)
    
    logger.info(f"Merging {len(assignments_df)} assignments with pool/address details")
    
    # Merge pools
    merged_df = assignments_df.merge(
        pools_df,
        left_on='pool',
        right_on='id',
        how='left',
        suffixes=('', '_pool')
    )
    
    # Merge addresses
    merged_df = merged_df.merge(
        addresses_df,
        left_on='address',
        right_on='id',
        how='left',
        suffixes=('', '_addr')
    )
    
    logger.info(f"Merged dataset contains {len(merged_df)} records")
    
    # Save to parquet
    output_path = f"{workdir}/merged_data.parquet"
    merged_df.to_parquet(output_path, index=False)
    
    logger.info("="*60)
    logger.info("DATA MERGE - Complete")
    logger.info("="*60)
    logger.info(f"Saved to: {output_path}")
    logger.info("="*60)
    
    return {
        "parquet_path": output_path,
        "record_count": len(merged_df),
    }
