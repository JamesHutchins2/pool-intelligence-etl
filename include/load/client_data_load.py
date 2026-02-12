"""
Load module for client_data_update_etl pipeline.

This module handles:
1. Upserting properties to master DB
2. Upserting pools to master DB
3. Upserting listings to master DB
4. Updating removed listings in master DB
"""

import logging
import random
from typing import Dict, Any, List

import pandas as pd
from psycopg2.extras import execute_values

from include.db.connections import get_master_db_connection
import re

logger = logging.getLogger(__name__)


def generate_pool_id() -> int:
    """Generate a unique pool_id (BIGINT)."""
    return random.randint(1000000000, 9223372036854775807)


def upsert_properties(properties_path: str) -> Dict[str, Any]:
    """
    Upsert properties to master DB.
    
    Args:
        properties_path: Path to properties_to_insert.parquet
        
    Returns:
        Dict with address_id -> property_id mappings
    """
    logger.info("="*60)
    logger.info("UPSERTING PROPERTIES")
    logger.info("="*60)
    
    df = pd.read_parquet(properties_path)
    
    # CRITICAL FIX: Ensure address_id is int64 to prevent precision loss
    if 'address_id' in df.columns:
        df['address_id'] = df['address_id'].astype('int64')
        
    logger.info(f"Loading {len(df)} properties")
    
    if df.empty:
        logger.info("No properties to insert")
        logger.info("="*60)
        return {"property_records": []}
    
    conn = get_master_db_connection()
    
    try:
        # Prepare rows
        rows = []
        for _, row in df.iterrows():
            rows.append((
                int(row['address_id']),
                str(row['address_number']),
                str(row['street_name']) if pd.notna(row['street_name']) else None,
                str(row['municipality']) if pd.notna(row['municipality']) else None,
                str(row['province_state']) if pd.notna(row['province_state']) else None,
                str(row['postal_code']) if pd.notna(row['postal_code']) else None,
                str(row['country']),
                float(row['lat']),
                float(row['lon']),
            ))
        
        upsert_sql = """
            INSERT INTO properties (
                address_id,
                address_number,
                street_name,
                municipality,
                province_state,
                postal_code,
                country,
                lat,
                lon,
                geom
            ) VALUES %s
            ON CONFLICT (address_id) DO UPDATE SET
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                geom = EXCLUDED.geom,
                updated_at = NOW()
            RETURNING address_id, id;
        """
        
        with conn.cursor() as cur:
            # Execute with template that includes geom calculation
            template = """(
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            )"""
            
            execute_values(
                cur,
                upsert_sql,
                [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[8], r[7]) for r in rows],
                template=template,
                page_size=100
            )
            
            conn.commit()
            
            # Query all properties that were just processed to get proper address_id -> property_id mappings
            address_ids = [r[0] for r in rows]
            cur.execute("""
                SELECT address_id, id 
                FROM properties 
                WHERE address_id = ANY(%s)
            """, (address_ids,))
            
            # Get property records with UUIDs for all processed properties
            property_records = []
            for result in cur.fetchall():
                property_records.append({
                    'address_id': result[0],
                    'property_id': result[1]
                })
            
            logger.info(f"Upserted {len(property_records)} properties")
            logger.info("="*60)
            
            return {"property_records": property_records}
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error upserting properties: {e}")
        raise
    finally:
        conn.close()


def upsert_pools(pools_new_path: str, pools_existing_path: str, property_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Upsert pools to master DB.
    
    For new properties: Create pools linked to property UUIDs from pools_new
    For existing properties: Update pools if they exist from pools_existing
    
    Args:
        pools_new_path: Path to pools_new.parquet (or None if no new pools)
        pools_existing_path: Path to pools_existing.parquet (or None if no existing pools)
        property_records: List of property records with address_id and property_id
        
    Returns:
        Dict with insert count
    """
    logger.info("="*60)
    logger.info("UPSERTING POOLS")
    logger.info("="*60)
    
    total_pools_processed = 0
    
    # Process new pools
    if pools_new_path and property_records:
        logger.info("Processing new property pools...")
        df_new = pd.read_parquet(pools_new_path)
        
        if not df_new.empty:
            logger.info(f"Loading {len(df_new)} new pools")
            total_pools_processed += len(df_new)
            
            # Create pools for new properties
            rows = []
            for property_record in property_records:
                pool_id = generate_pool_id()
                property_id = str(property_record['property_id'])
                
                # Find the pool type for this address_id
                address_id = property_record['address_id']
                matching_pools = df_new[df_new['address_id'] == address_id]
                
                if not matching_pools.empty:
                    pool_type = str(matching_pools.iloc[0]['pool_type'])
                    rows.append((pool_id, pool_type, property_id))
                    logger.debug(f"Creating pool {pool_id} for property {property_id} with type {pool_type}")
            
            if rows:
                conn = get_master_db_connection()
                try:
                    upsert_sql = """
                        INSERT INTO pools (id, pool_type, property_id, created_at, updated_at)
                        VALUES %s
                        ON CONFLICT (id) DO UPDATE SET
                            pool_type = EXCLUDED.pool_type,
                            property_id = EXCLUDED.property_id,
                            updated_at = NOW();
                    """
                    
                    with conn.cursor() as cur:
                        execute_values(cur, upsert_sql, rows, page_size=100)
                        conn.commit()
                        
                        logger.info(f"Upserted {len(rows)} new pools")
                        
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error upserting new pools: {e}")
                    raise
                finally:
                    conn.close()
    else:
        logger.info("No new pools to process")
    
    # Process existing pools (pools linked to existing properties)
    if pools_existing_path:
        logger.info("Processing existing property pools...")
        df_existing = pd.read_parquet(pools_existing_path)
        
        if not df_existing.empty:
            logger.info(f"Loading {len(df_existing)} existing pools")
            total_pools_processed += len(df_existing)
            
            # For existing properties, we just verify they exist
            # The pools should already exist, so this is mainly for verification
            conn = get_master_db_connection()
            try:
                with conn.cursor() as cur:
                    # Count existing pools for these properties
                    property_ids = [str(prop_id) for prop_id in df_existing['property_id'].unique()]
                    cur.execute("""
                        SELECT COUNT(*) FROM pools 
                        WHERE property_id = ANY(%s)
                    """, (property_ids,))
                    
                    existing_count = cur.fetchone()[0]
                    logger.info(f"Found {existing_count} existing pools in database for {len(property_ids)} properties")
                    
            except Exception as e:
                logger.error(f"Error checking existing pools: {e}")
                raise
            finally:
                conn.close()
    else:
        logger.info("No existing pools to process")
    
    logger.info(f"Total pools processed: {total_pools_processed}")
    logger.info("="*60)
    
    return {"pool_count": total_pools_processed}


def upsert_listings(listings_path: str, property_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Upsert listings to master DB.
    
    Args:
        listings_path: Path to listings_to_insert.parquet
        property_records: List of property records with address_id and property_id
        
    Returns:
        Dict with insert count
    """
    logger.info("="*60)
    logger.info("UPSERTING LISTINGS")
    logger.info("="*60)
    
    df = pd.read_parquet(listings_path)
    logger.info(f"Loading {len(df)} listings")
    
    if df.empty:
        logger.info("No listings to insert")
        logger.info("="*60)
        return {"listing_count": 0}
    
    conn = get_master_db_connection()
    
    try:
        # Prepare rows
        rows = []
        skipped = 0
        
        for _, row in df.iterrows():
            address_id = int(row['address_id'])
            property_address_id = address_id  # listings.property_address_id links to properties.address_id
            
            # Handle bathrooms like "3 + 1" by summing numbers
            def parse_bathrooms(val):
                if pd.isna(val):
                    return None
                val_str = str(val)
                if "+" in val_str:
                    parts = [float(p.strip()) for p in re.findall(r'[\d\.]+', val_str)]
                    return sum(parts)
                try:
                    return float(re.sub(r'[^\d\.]', '', val_str))
                except Exception:
                    return None

            # Handle bedrooms like "3 + 1" by summing numbers and casting to int
            def parse_bedrooms(val):
                if pd.isna(val):
                    return None
                val_str = str(val)
                if "+" in val_str:
                    parts = [int(float(p.strip())) for p in re.findall(r'[\d\.]+', val_str)]
                    return sum(parts)
                try:
                    return int(val_str)
                except Exception:
                    return None

            rows.append((
                str(row['mls_id']),
                property_address_id,
                parse_bathrooms(row['bathrooms']),
                parse_bedrooms(row['bedrooms']),
                row['date_collected'],
                str(row['description']) if pd.notna(row['description']) else None,
                str(row['house_cat']) if pd.notna(row['house_cat']) else None,
                False,  # is_removed
                int(row['listing_address_number']) if pd.notna(row['listing_address_number']) else None,
                int(float(re.sub(r'[$,]', '', str(row['price'])))) if pd.notna(row['price']) else None,
                None,  # removal_date
                float(re.sub(r'[$,]', '', str(row['size_sqft']))) if pd.notna(row['size_sqft']) else None,
                int(row['stories']) if pd.notna(row['stories']) else None,
            ))
        
        if not rows:
            logger.warning("No valid listings to insert")
            logger.info("="*60)
            return {"listing_count": 0}
        
        upsert_sql = """
            INSERT INTO listings (
                mls_id,
                property_address_id,
                bathrooms,
                bedrooms,
                date_collected,
                description,
                house_cat,
                is_removed,
                listing_address_number,
                price,
                removal_date,
                size_sqft,
                stories
            ) VALUES %s
            ON CONFLICT (mls_id) DO UPDATE SET
                bathrooms = EXCLUDED.bathrooms,
                bedrooms = EXCLUDED.bedrooms,
                price = EXCLUDED.price,
                size_sqft = EXCLUDED.size_sqft,
                stories = EXCLUDED.stories,
                description = EXCLUDED.description,
                updated_at = NOW();
        """
        
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows, page_size=100)
            conn.commit()
            
            logger.info(f"Upserted {len(rows)} listings")
            if skipped > 0:
                logger.warning(f"Skipped {skipped} listings (no property match)")
            logger.info("="*60)
            
            return {"listing_count": len(rows)}
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error upserting listings: {e}")
        raise
    finally:
        conn.close()


def update_removed_listings(updates_path: str) -> Dict[str, Any]:
    """
    Update removed listings in master DB.
    
    Args:
        updates_path: Path to listings_to_update.parquet
        
    Returns:
        Dict with update count
    """
    logger.info("="*60)
    logger.info("UPDATING REMOVED LISTINGS")
    logger.info("="*60)
    
    df = pd.read_parquet(updates_path)
    logger.info(f"Loading {len(df)} updates")
    
    if df.empty:
        logger.info("No listings to update")
        logger.info("="*60)
        return {"update_count": 0}
    
    # Log sample of data being processed
    logger.info(f"Sample removal data:")
    logger.info(f"  MLS IDs: {list(df['mls_id'].head())}")
    logger.info(f"  Removal dates: {list(df['removal_date'].head())}")
    
    conn = get_master_db_connection()
    
    try:
        # Enhanced update SQL that ensures we capture all removal entries
        # and provides better logging of what was actually updated
        update_sql = """
            UPDATE listings
            SET 
                is_removed = true,
                removal_date = %s,
                updated_at = NOW()
            WHERE mls_id = %s 
                AND (is_removed = false OR removal_date IS NULL);
        """
        
        # Also add a query to check what listings exist before update
        check_sql = """
            SELECT mls_id, is_removed, removal_date 
            FROM listings 
            WHERE mls_id = ANY(%s);
        """
        
        updated = 0
        not_found = 0
        already_removed = 0
        
        with conn.cursor() as cur:
            # Check existing status of listings to be updated
            mls_ids = [str(mls_id) for mls_id in df['mls_id']]
            cur.execute(check_sql, (mls_ids,))
            existing_listings = {row[0]: {'is_removed': row[1], 'removal_date': row[2]} 
                               for row in cur.fetchall()}
            
            logger.info(f"Found {len(existing_listings)} existing listings out of {len(df)} removal requests")
            
            for _, row in df.iterrows():
                mls_id = str(row['mls_id'])
                removal_date = row['removal_date']
                
                if mls_id not in existing_listings:
                    not_found += 1
                    logger.warning(f"Listing {mls_id} not found in database")
                    continue
                
                existing = existing_listings[mls_id]
                if existing['is_removed'] and existing['removal_date']:
                    already_removed += 1
                    logger.debug(f"Listing {mls_id} already removed on {existing['removal_date']}")
                    continue
                
                cur.execute(update_sql, (removal_date, mls_id))
                if cur.rowcount > 0:
                    updated += 1
                    logger.debug(f"Updated listing {mls_id} with removal_date {removal_date}")
            
            conn.commit()
            
            logger.info(f"Update summary:")
            logger.info(f"  ✓ Successfully updated: {updated}")
            logger.info(f"  ⚠ Not found in DB: {not_found}")
            logger.info(f"  ℹ Already removed: {already_removed}")
            logger.info(f"  Total processed: {len(df)}")
            logger.info("="*60)
            
            return {
                "update_count": updated,
                "not_found_count": not_found,
                "already_removed_count": already_removed,
                "total_processed": len(df)
            }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating removed listings: {e}")
        raise
    finally:
        conn.close()

