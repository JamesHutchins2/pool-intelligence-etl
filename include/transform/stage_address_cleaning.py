"""
Address Cleaning and Transformation Module

Standardizes, deduplicates, and filters address data before master DB upload.
"""

import logging
from typing import Dict, Any
import pandas as pd
import geopandas as gpd
from geopy.distance import distance
from shapely.geometry import box
import secrets

logger = logging.getLogger(__name__)

MAX_BIGINT = 2**63 - 1  # PostgreSQL signed BIGINT max


def standardize_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize text fields for consistent formatting.
    
    Args:
        df: DataFrame with address fields
    
    Returns:
        DataFrame with standardized fields
    """
    df = df.copy()
    
    if 'postal_code' in df.columns:
        df['postal_code'] = df['postal_code'].fillna('').str.upper().str.replace(' ', '', regex=False)
    if 'street_name' in df.columns:
        df['street_name'] = df['street_name'].fillna('').str.strip().str.title()
    if 'municipality' in df.columns:
        df['municipality'] = df['municipality'].fillna('').str.strip().str.title()
    
    return df


def remove_internal_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate addresses within the dataset.
    
    Duplicates are identified by matching street_name + address_number + 
    (municipality OR postal_code).
    
    Args:
        df: DataFrame with address data
    
    Returns:
        DataFrame with duplicates removed
    """
    # Drop duplicates where street_name AND address_number match, 
    # along with EITHER municipality OR postal code
    mask = (
        df.duplicated(subset=['street_name', 'address_number', 'municipality'], keep='first') |
        df.duplicated(subset=['street_name', 'address_number', 'postal_code'], keep='first')
    )
    df_no_duplicates = df[~mask]
    
    removed_count = mask.sum()
    if removed_count > 0:
        logger.info(f"Removed {removed_count} internal duplicate addresses")
    
    return df_no_duplicates


def filter_invalid_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out records with missing critical data.
    
    Critical fields:
    - address_number (must be present and valid)
    - street_name (must be present)
    - lat/lon (must be valid coordinates)
    
    Args:
        df: DataFrame with address data
    
    Returns:
        DataFrame with invalid records removed
    """
    logger.info(f"Filtering invalid records from {len(df)} total records")
    
    initial_count = len(df)
    
    # Filter: address_number must exist and be non-empty
    df = df[df['address_number'].notna()]
    df = df[df['address_number'].astype(str).str.strip() != '']
    logger.info(f"  After address_number filter: {len(df)} records")
    
    # Filter: street_name must exist and be non-empty
    df = df[df['street_name'].notna()]
    df = df[df['street_name'].astype(str).str.strip() != '']
    logger.info(f"  After street_name filter: {len(df)} records")
    
    # Filter: lat/lon must be valid
    df = df[df['lat_addr'].notna() & df['lon_addr'].notna()]
    df = df[(df['lat_addr'] >= -90) & (df['lat_addr'] <= 90)]
    df = df[(df['lon_addr'] >= -180) & (df['lon_addr'] <= 180)]
    logger.info(f"  After coordinate filter: {len(df)} records")
    
    # Filter: municipality or postal_code should exist
    df = df[df['municipality'].notna() | df['postal_code'].notna()]
    logger.info(f"  After location identifier filter: {len(df)} records")
    
    filtered_count = initial_count - len(df)
    logger.info(f"Filtered out {filtered_count} invalid records")
    
    return df


def build_bounding_box(df: pd.DataFrame, buffer_km: float = 5.0) -> tuple:
    """
    Build a bounding box around the data with buffer.
    
    Args:
        df: DataFrame with lat/lon columns
        buffer_km: Buffer distance in kilometers
    
    Returns:
        Tuple (min_lon, min_lat, max_lon, max_lat) with buffer applied
    """
    min_lat = df['lat_addr'].min()
    max_lat = df['lat_addr'].max()
    min_lon = df['lon_addr'].min()
    max_lon = df['lon_addr'].max()
    
    logger.info(f"Base bounding box: ({min_lon:.4f}, {min_lat:.4f}, {max_lon:.4f}, {max_lat:.4f})")
    
    # Apply buffer using geopy
    bottom_left = (min_lat, min_lon)
    top_right = (max_lat, max_lon)
    
    # 225 degrees is southwest, 45 degrees is northeast
    bottom_left_buffered = distance(kilometers=buffer_km).destination(bottom_left, 225)
    top_right_buffered = distance(kilometers=buffer_km).destination(top_right, 45)
    
    bbox_buffered = (
        bottom_left_buffered.longitude,
        bottom_left_buffered.latitude,
        top_right_buffered.longitude,
        top_right_buffered.latitude
    )
    
    logger.info(f"Buffered bbox ({buffer_km}km): ({bbox_buffered[0]:.4f}, {bbox_buffered[1]:.4f}, {bbox_buffered[2]:.4f}, {bbox_buffered[3]:.4f})")
    
    return bbox_buffered


def remove_master_duplicates(input_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove addresses that already exist in master database.
    
    Args:
        input_df: DataFrame with new addresses
        master_df: DataFrame with existing master addresses
    
    Returns:
        DataFrame with master duplicates removed
    """
    logger.info(f"Checking {len(input_df)} addresses against {len(master_df)} master records")
    
    # Standardize master data
    master_df = standardize_text_fields(master_df)
    
    # Create mask for duplicates
    mask = input_df.apply(
        lambda row: (
            (
                (master_df['street_name'] == row['street_name']) &
                (master_df['address_number'] == row['address_number']) &
                (master_df['municipality'] == row['municipality'])
            ).any() or
            (
                (master_df['street_name'] == row['street_name']) &
                (master_df['address_number'] == row['address_number']) &
                (master_df['postal_code'] == row['postal_code'])
            ).any()
        ),
        axis=1
    )
    
    df_no_duplicates = input_df[~mask]
    
    removed_count = mask.sum()
    logger.info(f"Removed {removed_count} addresses already in master database")
    logger.info(f"Remaining unique addresses: {len(df_no_duplicates)}")
    
    return df_no_duplicates


def generate_random_bigint() -> int:
    """Generate random BIGINT within PostgreSQL limits."""
    return secrets.randbelow(MAX_BIGINT)


def assign_address_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign unique address_id to each record.
    
    Args:
        df: DataFrame needing address_ids
    
    Returns:
        DataFrame with address_id column added
    """
    df = df.copy()
    df['address_id'] = [generate_random_bigint() for _ in range(len(df))]
    logger.info(f"Assigned {len(df)} unique address_ids")
    return df


def clean_and_transform_data(
    merged_parquet: str,
    master_db_url: str,
    workdir: str
) -> Dict[str, Any]:
    """
    Complete cleaning and transformation pipeline.
    
    Args:
        merged_parquet: Path to merged data parquet
        master_db_url: Master database connection string
        workdir: Working directory for output
    
    Returns:
        Dict with:
            - parquet_path: Path to cleaned data
            - assignment_ids: List of assignment IDs to mark as uploaded
            - record_count: Number of records after cleaning
            - bbox: Bounding box used for master query
    """
    logger.info("="*60)
    logger.info("DATA CLEANING - Starting")
    logger.info("="*60)
    
    # Read merged data
    df = pd.read_parquet(merged_parquet)
    logger.info(f"Input records: {len(df)}")
    
    # Step 1: Standardize text fields
    df = standardize_text_fields(df)
    logger.info("Standardized text fields")
    
    # Step 2: Remove internal duplicates
    df = remove_internal_duplicates(df)
    
    # Step 3: Filter invalid records
    df = filter_invalid_records(df)
    
    if len(df) == 0:
        logger.warning("No valid records after filtering")
        return {
            "parquet_path": None,
            "assignment_ids": [],
            "record_count": 0,
            "bbox": None,
        }
    
    # Step 4: Build bounding box
    bbox = build_bounding_box(df)
    
    # Step 5: Query master database for existing addresses
    from include.db.connections import get_master_db_connection
    
    master_conn = get_master_db_connection(master_db_url)
    
    query = f"""
    SELECT id, address_number, street_name, municipality, postal_code, lat, lon, geom
    FROM properties 
    WHERE ST_Intersects(geom, ST_MakeEnvelope({bbox[0]}, {bbox[1]}, {bbox[2]}, {bbox[3]}, 4326))
    """
    
    try:
        master_addresses = gpd.read_postgis(query, master_conn, geom_col='geom')
        logger.info(f"Retrieved {len(master_addresses)} addresses from master database")
    except Exception as e:
        logger.error(f"Failed to query master database: {e}")
        raise
    finally:
        master_conn.close()
    
    # Step 6: Remove master duplicates
    df = remove_master_duplicates(df, master_addresses)
    
    if len(df) == 0:
        logger.warning("No unique records after master duplicate removal")
        return {
            "parquet_path": None,
            "assignment_ids": [],
            "record_count": 0,
            "bbox": bbox,
        }
    
    # Step 7: Assign address IDs
    df = assign_address_ids(df)
    
    # Step 8: Store assignment IDs for later stage update
    assignment_ids = df['id'].tolist()
    
    logger.info("="*60)
    logger.info("DATA CLEANING - Complete")
    logger.info("="*60)
    logger.info(f"Final record count: {len(df)}")
    logger.info(f"Assignment IDs to mark as uploaded: {len(assignment_ids)}")
    
    # Save to parquet
    output_path = f"{workdir}/cleaned_data.parquet"
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to: {output_path}")
    logger.info("="*60)
    
    return {
        "parquet_path": output_path,
        "assignment_ids": assignment_ids,
        "record_count": len(df),
        "bbox": bbox,
    }
