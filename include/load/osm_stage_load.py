"""
OSM Stage Load Module

Handles loading addresses and pools to the stage database.
"""

import logging
from typing import Dict, Any
import pandas as pd
from include.db.stage_connections import get_stage_db_connection

logger = logging.getLogger(__name__)


def load_addresses_to_stage(addresses_parquet: str) -> Dict[str, Any]:
    """
    Load unique addresses to stage database.
    
    Args:
        addresses_parquet: Path to parquet file with unique addresses
    
    Returns:
        Dict with:
            - inserted_count: Number of addresses inserted
            - skipped_count: Number of addresses skipped (errors)
    """
    logger.info("="*60)
    logger.info("ADDRESS LOADING - Starting")
    logger.info("="*60)
    
    # Read addresses
    df = pd.read_parquet(addresses_parquet)
    logger.info(f"Loading {len(df)} addresses to stage database")
    
    conn = get_stage_db_connection()
    cur = conn.cursor()
    
    inserted_count = 0
    skipped_count = 0
    
    try:
        for _, row in df.iterrows():
            try:
                # Convert address_number to int if not empty
                address_num = None
                if row.get("address_number") and str(row["address_number"]).strip():
                    try:
                        address_num = int(row["address_number"])
                    except (ValueError, TypeError):
                        pass
                
                # Insert address with PostGIS geometry
                cur.execute("""
                    INSERT INTO address (
                        address_number, lat, lon, postal_code, 
                        street_name, province_state, country, municipality, geom
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, 
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                    )
                """, (
                    address_num,
                    float(row["lat"]),
                    float(row["lon"]),
                    row.get("postal_code", "") or None,
                    row.get("street_name", "") or None,
                    row.get("province_state", "") or None,
                    row.get("country", "") or None,
                    row.get("municipality", "") or None,
                    float(row["lon"]),  # For ST_MakePoint (longitude first)
                    float(row["lat"]),
                ))
                inserted_count += 1
            
            except Exception as e:
                logger.warning(f"Failed to insert address at ({row['lat']}, {row['lon']}): {e}")
                skipped_count += 1
        
        conn.commit()
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load addresses: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()
    
    logger.info("="*60)
    logger.info("ADDRESS LOADING - Complete")
    logger.info("="*60)
    logger.info(f"Inserted: {inserted_count}")
    logger.info(f"Skipped: {skipped_count}")
    logger.info("="*60)
    
    return {
        "inserted_count": inserted_count,
        "skipped_count": skipped_count,
    }


def load_pools_to_stage(pools_parquet: str) -> Dict[str, Any]:
    """
    Load pools to stage database.
    
    Args:
        pools_parquet: Path to parquet file with pool data
    
    Returns:
        Dict with:
            - inserted_count: Number of pools inserted
            - skipped_count: Number of pools skipped
    """
    logger.info("="*60)
    logger.info("POOL LOADING - Starting")
    logger.info("="*60)
    
    # Read pools
    df = pd.read_parquet(pools_parquet)
    logger.info(f"Loading {len(df)} pools to stage database")
    
    conn = get_stage_db_connection()
    cur = conn.cursor()
    
    inserted_count = 0
    skipped_count = 0
    
    try:
        for _, row in df.iterrows():
            try:
                # Extract pool type from tags
                pool_type = None
                if "tags" in row and isinstance(row["tags"], dict):
                    tags = row["tags"]
                    if "leisure" in tags and tags["leisure"] == "swimming_pool":
                        pool_type = tags.get("access", "unknown")
                    elif "swimming_pool" in tags:
                        pool_type = tags.get("access", "unknown")
                
                # Insert pool
                cur.execute("""
                    INSERT INTO pool (lat, lon, pool_type)
                    VALUES (%s, %s, %s)
                """, (
                    float(row["lat"]),
                    float(row["lon"]),
                    pool_type,
                ))
                inserted_count += 1
            
            except Exception as e:
                logger.warning(f"Failed to insert pool at ({row['lat']}, {row['lon']}): {e}")
                skipped_count += 1
        
        conn.commit()
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load pools: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()
    
    logger.info("="*60)
    logger.info("POOL LOADING - Complete")
    logger.info("="*60)
    logger.info(f"Inserted: {inserted_count}")
    logger.info(f"Skipped: {skipped_count}")
    logger.info("="*60)
    
    return {
        "inserted_count": inserted_count,
        "skipped_count": skipped_count,
    }


def create_pool_address_assignments(pools_parquet: str, workdir: str) -> Dict[str, Any]:
    """
    Create mapping between pools and nearest addresses.
    Uses spatial distance to assign each pool to its nearest address.
    
    Args:
        pools_parquet: Path to parquet with pool data
        workdir: Working directory for output
    
    Returns:
        Dict with:
            - parquet_path: Path to assignments parquet
            - assignment_count: Number of assignments created
    """
    logger.info("="*60)
    logger.info("POOL-ADDRESS ASSIGNMENT - Starting")
    logger.info("="*60)
    
    # Read pools
    pools_df = pd.read_parquet(pools_parquet)
    logger.info(f"Creating assignments for {len(pools_df)} pools")
    
    conn = get_stage_db_connection()
    cur = conn.cursor()
    
    assignments = []
    
    try:
        for _, pool_row in pools_df.iterrows():
            pool_lat = pool_row["lat"]
            pool_lon = pool_row["lon"]
            
            # Find nearest address using PostGIS distance
            cur.execute("""
                SELECT id, address_number, street_name, municipality,
                       ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) as distance
                FROM address
                WHERE geom IS NOT NULL
                ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                LIMIT 1
            """, (pool_lon, pool_lat, pool_lon, pool_lat))
            
            result = cur.fetchone()
            
            if result:
                assignments.append({
                    "pool_lat": pool_lat,
                    "pool_lon": pool_lon,
                    "address_id": result[0],
                    "address_number": result[1],
                    "street_name": result[2],
                    "municipality": result[3],
                    "distance_meters": result[4],
                })
    
    except Exception as e:
        logger.error(f"Failed to create assignments: {e}")
        raise
    
    finally:
        cur.close()
        conn.close()
    
    # Convert to DataFrame
    assignments_df = pd.DataFrame(assignments)
    
    logger.info("="*60)
    logger.info("POOL-ADDRESS ASSIGNMENT - Complete")
    logger.info("="*60)
    logger.info(f"Created {len(assignments_df)} pool-address assignments")
    
    # Save to parquet
    output_path = f"{workdir}/pool_address_assignments.parquet"
    assignments_df.to_parquet(output_path, index=False)
    logger.info(f"Saved to: {output_path}")
    logger.info("="*60)
    
    return {
        "parquet_path": output_path,
        "assignment_count": len(assignments_df),
    }
