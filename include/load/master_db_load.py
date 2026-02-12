"""
Master Database Load Module

Loads addresses and pools to master database and updates stage status.
"""

import logging
from typing import Dict, Any
import pandas as pd
from include.db.connections import get_master_db_connection
from include.db.stage_connections import get_stage_db_connection

logger = logging.getLogger(__name__)


def load_addresses_to_master(
    cleaned_parquet: str,
    master_db_url: str,
    workdir: str
) -> Dict[str, Any]:
    """
    Load addresses to master database properties table.
    
    Args:
        cleaned_parquet: Path to cleaned data parquet
        master_db_url: Master database connection string
        workdir: Working directory for output
    
    Returns:
        Dict with:
            - inserted_count: Number of addresses inserted
            - failed_count: Number of addresses that failed
    """
    logger.info("="*60)
    logger.info("MASTER DB LOAD - Starting (Addresses)")
    logger.info("="*60)
    
    # Read cleaned data
    df = pd.read_parquet(cleaned_parquet)
    logger.info(f"Loading {len(df)} addresses to master database")
    
    # Prepare address columns
    cols_to_keep = [
        'address_id', 'address_number', 'lat_addr', 'lon_addr',
        'postal_code', 'street_name', 'province_state', 'country',
        'geom', 'municipality'
    ]
    address_df = df[cols_to_keep].copy()
    
    # Rename lat_addr/lon_addr to lat/lon for master schema
    address_df.rename(columns={'lat_addr': 'lat', 'lon_addr': 'lon'}, inplace=True)
    
    # Prepare insert statement
    insert_query = """
    INSERT INTO properties (
        address_id, address_number, country, lat, lon, 
        postal_code, street_name, municipality, province_state, geom
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeogFromText(%s))
    """
    
    conn = get_master_db_connection(master_db_url)
    cur = conn.cursor()
    
    inserted_count = 0
    failed_count = 0
    
    try:
        for idx, row in address_df.iterrows():
            try:
                # Create WKT representation
                wkt = f"POINT({row['lon']} {row['lat']})"
                
                values = (
                    int(row['address_id']),
                    row['address_number'],
                    row['country'],
                    float(row['lat']),
                    float(row['lon']),
                    row['postal_code'] if row['postal_code'] else None,
                    row['street_name'],
                    row['municipality'],
                    row['province_state'],
                    wkt
                )
                
                cur.execute(insert_query, values)
                inserted_count += 1
                
            except Exception as e:
                logger.warning(f"Failed to insert address {row['address_id']}: {e}")
                failed_count += 1
        
        conn.commit()
        
        logger.info("="*60)
        logger.info("MASTER DB LOAD - Complete (Addresses)")
        logger.info("="*60)
        logger.info(f"Successfully inserted: {inserted_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info("="*60)
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load addresses: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()
    
    return {
        "inserted_count": inserted_count,
        "failed_count": failed_count,
    }


def load_pools_to_master(
    cleaned_parquet: str,
    master_db_url: str,
) -> Dict[str, Any]:
    """
    Load pools to master database pools table.
    
    Args:
        cleaned_parquet: Path to cleaned data parquet
        master_db_url: Master database connection string
    
    Returns:
        Dict with:
            - inserted_count: Number of pools inserted
            - failed_count: Number of pools that failed
    """
    logger.info("="*60)
    logger.info("MASTER DB LOAD - Starting (Pools)")
    logger.info("="*60)
    
    # Read cleaned data
    df = pd.read_parquet(cleaned_parquet)
    
    # Prepare pool data
    pool_df = df[['address_id', 'pool_type']].copy()
    
    conn = get_master_db_connection(master_db_url)
    cur = conn.cursor()
    
    # First, get property_id (UUID) mappings for address_ids
    address_ids = pool_df['address_id'].tolist()
    
    query = """
    SELECT id, address_id 
    FROM properties 
    WHERE address_id = ANY(%s)
    """
    
    try:
        cur.execute(query, (address_ids,))
        property_mappings = cur.fetchall()
        
        # Create mapping dict: address_id -> property_id (UUID)
        address_to_property = {row[1]: row[0] for row in property_mappings}
        
        # Add property_id column
        pool_df['property_id'] = pool_df['address_id'].map(address_to_property)
        
        # Check for unmapped pools
        unmapped = pool_df[pool_df['property_id'].isnull()]
        if len(unmapped) > 0:
            logger.warning(f"{len(unmapped)} pools couldn't be mapped to properties")
        
        # Filter out unmapped pools
        pool_df_upload = pool_df[pool_df['property_id'].notnull()].copy()
        
        logger.info(f"Loading {len(pool_df_upload)} pools to master database")
        
        # Insert pools
        insert_query = """
        INSERT INTO pools (property_id, pool_type)
        VALUES (%s, %s)
        """
        
        inserted_count = 0
        failed_count = 0
        
        for idx, row in pool_df_upload.iterrows():
            try:
                values = (
                    row['property_id'],
                    row['pool_type'] if pd.notna(row['pool_type']) else None
                )
                
                cur.execute(insert_query, values)
                inserted_count += 1
                
            except Exception as e:
                logger.warning(f"Failed to insert pool for property {row['property_id']}: {e}")
                failed_count += 1
        
        conn.commit()
        
        logger.info("="*60)
        logger.info("MASTER DB LOAD - Complete (Pools)")
        logger.info("="*60)
        logger.info(f"Successfully inserted: {inserted_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info("="*60)
        
        return {
            "inserted_count": inserted_count,
            "failed_count": failed_count,
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load pools: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()


def update_stage_upload_status(assignment_ids: list) -> Dict[str, Any]:
    """
    Mark assignments as uploaded in stage database.
    
    Args:
        assignment_ids: List of assignment IDs to update
    
    Returns:
        Dict with:
            - updated_count: Number of assignments updated
    """
    logger.info("="*60)
    logger.info("STAGE UPDATE - Starting")
    logger.info("="*60)
    
    if not assignment_ids:
        logger.warning("No assignment IDs to update")
        return {"updated_count": 0}
    
    logger.info(f"Marking {len(assignment_ids)} assignments as uploaded")
    
    change_uploaded_state = """
    UPDATE assignment
    SET uploaded = TRUE
    WHERE id IN %s;
    """
    
    conn = get_stage_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute(change_uploaded_state, (tuple(assignment_ids),))
        conn.commit()
        updated_count = cur.rowcount
        
        logger.info("="*60)
        logger.info("STAGE UPDATE - Complete")
        logger.info("="*60)
        logger.info(f"Updated {updated_count} assignment records")
        logger.info("="*60)
        
        return {"updated_count": updated_count}
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update stage: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()
