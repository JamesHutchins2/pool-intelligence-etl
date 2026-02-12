
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from include.db.connections import get_listing_db_connection


# Column order for listing_staging and listing tables
STAGING_COLS = (
    "mls_id, date_collected, description, bedrooms, bathrooms, "
    "size_sqft, stories, house_cat, price, address_number, "
    "address_number_suffix, address_predir, street_name, "
    "street_posttype, street_postdir, full_street_name, "
    "locality, municipality, province_state, postal_code, "
    "pool_mentioned, pool_type, lat, lon"
)


def _safe_int(val) -> int | None:
    """Convert value to int, return None if not possible."""
    if pd.isna(val):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    """Convert value to float, return None if not possible."""
    if pd.isna(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def prepare_load_data(df: pd.DataFrame, run_ts: datetime) -> Tuple[List[Tuple], List[Dict], Dict[str, str]]:
    """
    Prepare DataFrame for database load.
    
    Expects columns from transform pipeline:
      - MLS (required)
      - address_number (from address_cleaning - already parsed)
      - street_address (from address_cleaning)
      - city (from address_cleaning)
      - postal_code (from address_cleaning)
      - pool_flag, pool_type (from pool_inference)
      - Bedrooms, Bathrooms, Stories, Price (from base_cleaning)
      - Size_sqft (from base_cleaning)
      - Latitude, Longitude (original or corrected)
      - search_location (from extraction - format: "CA-ON-Fort Erie")
    
    Args:
        df: DataFrame with transformed listing data
        run_ts: Timestamp for this ETL run
        
    Returns:
        (rows_for_db, bad_rows_info, mls_to_location)
        - rows_for_db: List of tuples ready for bulk insert
        - bad_rows_info: List of dicts with info about skipped rows
        - mls_to_location: Dict mapping mls_id -> search_location
    """
    rows = []
    bad_rows = []
    mls_to_location = {}
    
    for _, row in df.iterrows():
        # MLS ID (required)
        mls_id = row.get("MLS")
        if pd.isna(mls_id):
            continue
        mls_id = str(mls_id).strip()
        
        # Description (may include amenities)
        desc = row.get("Description")
        amn = row.get("Ammenities")
        if pd.notna(amn) and str(amn).strip():
            desc = (str(desc).strip() + " | Amenities: " + str(amn).strip()) if pd.notna(desc) else str(amn).strip()
        
        # Cleaned numeric fields from base_cleaning
        bedrooms = _safe_float(row.get("Bedrooms"))
        bathrooms = _safe_float(row.get("Bathrooms"))
        size_sqft = _safe_float(row.get("Size_sqft"))
        stories = _safe_float(row.get("Stories"))
        price = _safe_float(row.get("Price"))
        house_cat = row.get("House Category")
        
        # Address fields from address_cleaning transform
        # These are already parsed and validated!
        address_number = _safe_int(row.get("address_number"))
        street_address = row.get("street_address")  # Full street line
        city = row.get("city")
        postal_code = row.get("postal_code")
        
        # Original Address field (kept for reference)
        full_street_name = row.get("Address")
        
        # Skip if address_number is missing (required field)
        # Note: address_cleaning already validated this, but double-check
        if address_number is None:
            bad_rows.append({
                "mls_id": mls_id,
                "reason": "missing_address_number",
                "address_number": address_number,
                "street_address": street_address
            })
            continue
        
        # address_number_suffix: not currently used, set to None
        address_number_suffix = None
        
        # Coordinates (may be corrected by address_correction step)
        lat = _safe_float(row.get("Latitude"))
        lon = _safe_float(row.get("Longitude"))
        
        # Skip if coordinates missing (required fields)
        if lat is None or lon is None:
            bad_rows.append({
                "mls_id": mls_id,
                "reason": "missing_coordinates",
                "lat": lat,
                "lon": lon
            })
            continue
        
        # Pool fields from pool_inference transform
        pool_flag = row.get("pool_flag", False)
        pool_type = row.get("pool_type", "none")
        
        # Convert pool_flag to pool_mentioned for DB
        pool_mentioned = bool(pool_flag) if pd.notna(pool_flag) else False
        
        # Search location tracking (added during extraction)
        search_location = row.get("search_location")
        
        # Additional address components (not currently populated by transform)
        # Set to None for now - can be enhanced later if needed
        locality = None  # Could map from city if needed
        municipality = city  # Use city from address_cleaning
        province_state = None  # Not extracted by current transform
        address_predir = None
        street_posttype = None
        street_postdir = None
        
        rows.append((
            mls_id,
            run_ts,
            None if pd.isna(desc) else str(desc),
            None if bedrooms is None else str(bedrooms),
            None if bathrooms is None else str(bathrooms),
            None if size_sqft is None else str(size_sqft),
            stories,
            None if pd.isna(house_cat) else str(house_cat),
            price,
            address_number,
            address_number_suffix,
            address_predir,
            None if pd.isna(street_address) else str(street_address),
            street_posttype,
            street_postdir,
            None if pd.isna(full_street_name) else str(full_street_name),
            locality,
            None if pd.isna(municipality) else str(municipality),
            province_state,
            None if pd.isna(postal_code) else str(postal_code),
            pool_mentioned,
            None if pd.isna(pool_type) else str(pool_type),
            lat,
            lon
        ))
        
        # Track search_location for this listing
        if pd.notna(search_location):
            mls_to_location[mls_id] = str(search_location)
    
    return rows, bad_rows, mls_to_location


def load_to_staging(conn, rows: List[Tuple]) -> int:
    """
    Load prepared rows into listing_staging table.
    
    Args:
        conn: Database connection
        rows: List of tuples matching STAGING_COLS order
        
    Returns:
        Number of rows inserted
    """
    # Clear staging table
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE listing_staging;")
    
    # Bulk insert
    insert_sql = f"INSERT INTO listing_staging ({STAGING_COLS}) VALUES %s"
    
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=1000)
    
    return len(rows)


def filter_valid_mls_ids(conn, mls_to_location: Dict[str, str]) -> Dict[str, str]:
    """
    Filter mls_to_location dict to only include MLS IDs that exist in the listing table.
    
    Args:
        conn: Database connection
        mls_to_location: Dict mapping mls_id -> search_location
        
    Returns:
        Filtered dict with only valid mls_ids
    """
    if not mls_to_location:
        return {}
    
    mls_ids = list(mls_to_location.keys())
    
    # Query existing MLS IDs in batches to avoid too large query
    valid_mls_ids = set()
    
    with conn.cursor() as cur:
        # Use ANY operator to check which MLS IDs exist
        cur.execute("SELECT mls_id FROM listing WHERE mls_id = ANY(%s)", (mls_ids,))
        valid_mls_ids = {row[0] for row in cur.fetchall()}
    
    # Filter the original dict
    return {mls_id: location for mls_id, location in mls_to_location.items() 
            if mls_id in valid_mls_ids}


def update_search_location_tracking(conn, mls_to_location: Dict[str, str], run_ts: datetime) -> int:
    """
    Update listing_search_location table with current run data.
    
    For each listing:
    1. INSERT if (mls_id, search_location) doesn't exist
    2. UPDATE last_seen if it already exists
    
    Args:
        conn: Database connection
        mls_to_location: Dict mapping mls_id -> search_location
        run_ts: Timestamp for this run
        
    Returns:
        Number of location mappings updated
    """
    if not mls_to_location:
        return 0
    
    update_sql = """
        INSERT INTO listing_search_location (mls_id, search_location, first_seen, last_seen)
        VALUES %s
        ON CONFLICT (mls_id, search_location) 
        DO UPDATE SET last_seen = EXCLUDED.last_seen;
    """
    
    rows = [(mls_id, location, run_ts, run_ts) for mls_id, location in mls_to_location.items()]
    
    with conn.cursor() as cur:
        execute_values(cur, update_sql, rows, page_size=1000)
        return cur.rowcount


def upsert_new_listings(conn) -> int:
    """
    Insert new listings from staging to main listing table.
    Only inserts MLS IDs that don't already exist.
    
    Args:
        conn: Database connection
        
    Returns:
        Number of new listings inserted
    """
    upsert_sql = f"""
        INSERT INTO listing ({STAGING_COLS})
        SELECT {STAGING_COLS}
        FROM listing_staging s
        WHERE NOT EXISTS (
            SELECT 1
            FROM listing l
            WHERE l.mls_id = s.mls_id
        );
    """
    
    with conn.cursor() as cur:
        cur.execute(upsert_sql)
        return cur.rowcount


def detect_removed_listings(conn, run_ts: datetime, search_locations: List[str]) -> int:
    """
    Detect listings removed from specific search areas that were queried in this run.
    
    A listing is considered removed from a search_location if:
    1. It exists in listing_search_location for that location
    2. That location was queried in this run (in search_locations list)
    3. It was NOT seen in this run (last_seen < run_ts)
    
    Args:
        conn: Database connection
        run_ts: Timestamp for this run
        search_locations: List of search_location strings that were queried
        
    Returns:
        Number of listings marked as removed
    """
    if not search_locations:
        return 0
    
    removal_sql = """
        WITH queried_areas AS (
            SELECT unnest(%s::text[]) AS search_location
        ),
        removed_from_areas AS (
            SELECT DISTINCT lsl.mls_id, lsl.search_location
            FROM listing_search_location lsl
            JOIN queried_areas qa ON qa.search_location = lsl.search_location
            WHERE lsl.last_seen < %s
        )
        INSERT INTO listing_removal (mls_id, removal_date, search_location)
        SELECT rfa.mls_id, %s, rfa.search_location
        FROM removed_from_areas rfa
        WHERE NOT EXISTS (
            SELECT 1
            FROM listing_removal lr
            WHERE lr.mls_id = rfa.mls_id
              AND lr.search_location = rfa.search_location
        );
    """
    
    with conn.cursor() as cur:
        cur.execute(removal_sql, (search_locations, run_ts, run_ts))
        return cur.rowcount


def detect_relistings(conn) -> int:
    """
    Detect re-listings: properties that were removed and now appear again
    at the same address but with a different MLS ID.
    
    Match based on street_name, locality, postal_code (at least 2 must match).
    Only records if there's a price change.
    
    Args:
        conn: Database connection
        
    Returns:
        Number of re-listings detected
    """
    relisting_sql = """
        WITH removed_history AS (
            SELECT l.*
            FROM listing l
            JOIN listing_removal r ON r.mls_id = l.mls_id
        ),
        relisted_candidates AS (
            SELECT
                oh.mls_id AS old_mls_id,
                s.mls_id AS new_mls_id,
                s.date_collected AS re_list_date,
                s.price - oh.price AS price_change,
                (CASE WHEN s.street_name = oh.street_name THEN 1 ELSE 0 END +
                 CASE WHEN s.locality = oh.locality THEN 1 ELSE 0 END +
                 CASE WHEN s.postal_code = oh.postal_code THEN 1 ELSE 0 END) AS match_score
            FROM listing_staging s
            JOIN removed_history oh ON (
                s.street_name = oh.street_name OR
                s.locality = oh.locality OR
                s.postal_code = oh.postal_code
            )
        )
        INSERT INTO re_listing (mls_id, re_listing_id, re_list_date, price_change)
        SELECT rc.old_mls_id, rc.new_mls_id, rc.re_list_date, rc.price_change
        FROM relisted_candidates rc
        WHERE rc.match_score >= 2
          AND rc.price_change IS NOT NULL
          AND rc.price_change <> 0
          AND NOT EXISTS (
              SELECT 1
              FROM re_listing r
              WHERE r.mls_id = rc.old_mls_id
                AND r.re_listing_id = rc.new_mls_id
          );
    """
    
    with conn.cursor() as cur:
        cur.execute(relisting_sql)
        return cur.rowcount


def load_listings_to_db(parquet_path: str, run_ts: datetime | None = None) -> Dict[str, Any]:
    """
    Complete load operation: read parquet, load to staging, upsert, detect removals and re-listings.
    
    Args:
        parquet_path: Path to transformed parquet file
        run_ts: Timestamp for this run (defaults to now)
        
    Returns:
        Dict with statistics about the load operation
    """
    if run_ts is None:
        run_ts = datetime.now(timezone.utc)
    
    # Read transformed data
    df = pd.read_parquet(parquet_path)
    print(f"Read {len(df)} records from {parquet_path}")
    
    # Prepare data
    rows, bad_rows, mls_to_location = prepare_load_data(df, run_ts)
    print(f"Prepared {len(rows)} valid rows for database load")
    
    if bad_rows:
        print(f"WARNING: Skipped {len(bad_rows)} rows due to validation errors")
        for i, bad in enumerate(bad_rows[:10], 1):
            print(f"  {i}. MLS {bad['mls_id']}: {bad['reason']}")
        if len(bad_rows) > 10:
            print(f"  ... and {len(bad_rows) - 10} more")
    
    # Get unique search_locations from this run
    search_locations = list(set(mls_to_location.values()))
    print(f"Search locations in this run: {len(search_locations)}")
    
    # Get database connection
    conn = get_listing_db_connection()
    
    try:
        # Execute all operations in a transaction
        staging_count = load_to_staging(conn, rows)
        print(f"✓ Loaded {staging_count} rows to listing_staging")
        
        # Insert new listings FIRST before updating location tracking
        new_listings = upsert_new_listings(conn)
        print(f"✓ Inserted {new_listings} new listings")
        
        # Now update location tracking - all mls_ids should exist in listing table
        try:
            location_updates = update_search_location_tracking(conn, mls_to_location, run_ts)
            print(f"✓ Updated {location_updates} listing-location mappings")
        except psycopg2.IntegrityError as e:
            if "foreign key constraint" in str(e) and "listing_search_location_mls_id_fkey" in str(e):
                print(f"⚠️  Foreign key error in location tracking - some listings may not exist: {e}")
                # Filter out mls_ids that don't exist in the listing table
                conn.rollback()
                
                # Start a new transaction and filter valid mls_ids
                valid_mls_to_location = filter_valid_mls_ids(conn, mls_to_location)
                print(f"✓ Filtered to {len(valid_mls_to_location)} valid MLS IDs out of {len(mls_to_location)}")
                
                location_updates = update_search_location_tracking(conn, valid_mls_to_location, run_ts)
                print(f"✓ Updated {location_updates} listing-location mappings (filtered)")
            else:
                raise
        
        removals = detect_removed_listings(conn, run_ts, search_locations)
        print(f"✓ Detected {removals} removed listings from queried areas")
        
        relistings = detect_relistings(conn)
        print(f"✓ Detected {relistings} re-listings")
        
        # Commit transaction
        conn.commit()
        print("✓ Transaction committed successfully")
        
        return {
            "staging_rows": staging_count,
            "location_mappings": location_updates,
            "new_listings": new_listings,
            "removals": removals,
            "relistings": relistings,
            "skipped_rows": len(bad_rows),
            "total_input_rows": len(df),
            "search_locations_count": len(search_locations),
        }
        
    except Exception as e:
        conn.rollback()
        print(f"✗ Error during database load: {e}")
        raise
    finally:
        conn.close()
