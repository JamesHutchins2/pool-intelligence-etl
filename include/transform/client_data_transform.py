

import logging
import random
from typing import Dict, Any, List, Tuple, Set, Optional

import pandas as pd

from include.db.connections import get_master_db_connection

logger = logging.getLogger(__name__)


def generate_address_id() -> int:
    """Generate a unique address_id (BIGINT)."""
    # Generate random BIGINT (positive, within PostgreSQL BIGINT range)
    # Range: 1 to 9223372036854775807
    return random.randint(1000000000, 9223372036854775807)


def normalize_address_for_matching(address_number: Any, street_name: str, postal_code: str, municipality: str = None) -> Set[str]:
    """
    Generate all normalized address expansions using comprehensive address normalization.
    
    Args:
        address_number: House/building number
        street_name: Street name
        postal_code: Postal/ZIP code
        municipality: City/municipality name (optional)
        
    Returns:
        Set of normalized address strings
    """
    try:
        # Build full address string
        parts = []
        if address_number:
            parts.append(str(address_number))
        if street_name:
            parts.append(str(street_name))
        if municipality:
            parts.append(str(municipality))
        if postal_code:
            parts.append(str(postal_code))
        parts.append("Canada")
        
        full_address = ", ".join(parts)
        
        # Generate comprehensive address variations for matching
        base_address = f"{address_number} {street_name}".strip()
        variations = set()
        
        # Comprehensive abbreviation mappings based on libpostal patterns
        street_types = {
            'st': ['street', 'st'], 'ave': ['avenue', 'ave'], 'rd': ['road', 'rd'],
            'blvd': ['boulevard', 'blvd'], 'dr': ['drive', 'dr'], 'ct': ['court', 'ct'],
            'ln': ['lane', 'ln'], 'pl': ['place', 'pl'], 'cir': ['circle', 'cir'],
            'way': ['way'], 'pkwy': ['parkway', 'pkwy'], 'hwy': ['highway', 'hwy'],
            'sq': ['square', 'sq'], 'ter': ['terrace', 'ter'], 'trl': ['trail', 'trl'],
            'cres': ['crescent', 'cres'], 'crescent': ['crescent', 'cres']
        }
        
        # Directionals
        directionals = {
            'n': ['north', 'n'], 'north': ['north', 'n'], 's': ['south', 's'], 'south': ['south', 's'],
            'e': ['east', 'e'], 'east': ['east', 'e'], 'w': ['west', 'w'], 'west': ['west', 'w'],
            'ne': ['northeast', 'ne'], 'nw': ['northwest', 'nw'], 'se': ['southeast', 'se'], 'sw': ['southwest', 'sw']
        }
        
        def expand_address_variants(text):
            """Generate address variations by applying abbreviation mappings"""
            results = set([text, text.lower()])
            words = text.split()
            
            # Try replacing each word with abbreviations/expansions
            for i, word in enumerate(words):
                clean_word = word.lower().rstrip('.,;')
                
                # Street type expansions
                if clean_word in street_types:
                    for replacement in street_types[clean_word]:
                        new_words = words.copy()
                        new_words[i] = replacement
                        results.add(' '.join(new_words))
                        results.add(' '.join(new_words).lower())
                
                # Directional expansions  
                if clean_word in directionals:
                    for replacement in directionals[clean_word]:
                        new_words = words.copy()
                        new_words[i] = replacement
                        results.add(' '.join(new_words))
                        results.add(' '.join(new_words).lower())
            
            return results
        
        # Generate variations for different address formats
        address_formats = [
            base_address,
            f"{address_number} {street_name} {municipality}" if municipality else base_address,
            f"{address_number} {street_name} {postal_code}" if postal_code else base_address,
            full_address
        ]
        
        for addr_format in address_formats:
            if addr_format:
                variations.update(expand_address_variants(addr_format.strip()))
        
        # Clean up variations (remove empty strings, normalize whitespace)
        cleaned_variations = set()
        for var in variations:
            if var and var.strip():
                normalized = ' '.join(var.strip().split())  # normalize whitespace
                cleaned_variations.add(normalized)
        
        return cleaned_variations
        
    except Exception as e:
        logger.warning(f"Failed to expand address '{full_address}': {e}")
        # Fallback: return normalized version of original
        normalized = f"{address_number} {street_name} {postal_code}".lower().strip()
        return {normalized}


def match_property_by_coordinates(conn, lat: float, lon: float, radius_m: int = 50) -> Optional[Dict[str, Any]]:
    """
    Find property by coordinate proximity.
    
    Args:
        conn: Master DB connection
        lat: Latitude
        lon: Longitude
        radius_m: Search radius in meters
        
    Returns:
        Dict with property data or None if not found
    """
    query = """
        SELECT 
            id as property_id,
            address_id,
            address_number,
            street_name,
            municipality,
            province_state,
            postal_code,
            ST_Distance(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) as distance_m
        FROM properties
        WHERE ST_DWithin(
            geom,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            %s
        )
        ORDER BY distance_m
        LIMIT 1;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (lon, lat, lon, lat, radius_m))
        row = cur.fetchone()
        
        if row:
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))
        return None


def match_property_by_address(conn, address_number: int, street_name: str, postal_code: str, 
                               municipality: str = None, lat: float = None, lon: float = None) -> Optional[Tuple[Dict[str, Any], str]]:
    """
    Find existing property in master DB using fuzzy address matching.
    
    Uses a multi-strategy approach:
    1. Address expansion matching (libpostal) - catches variations like "St" vs "Street"
    2. Coordinate proximity fallback (50m radius) - for addresses with geocoding issues
    
    Args:
        conn: Master DB connection
        address_number: House number
        street_name: Street name
        postal_code: Postal code
        municipality: City/municipality (optional, improves matching)
        lat: Latitude (optional, enables coordinate fallback)
        lon: Longitude (optional, enables coordinate fallback)
        
    Returns:
        Tuple of (property_dict, match_type) or None if not found
        match_type: 'address_expansion', 'coordinate_proximity'
    """
    # Generate expansions for the listing address
    listing_expansions = normalize_address_for_matching(address_number, street_name, postal_code, municipality)
    
    # Query candidates from same postal code for efficiency
    query = """
        SELECT 
            id as property_id,
            address_id,
            address_number,
            street_name,
            municipality,
            province_state,
            postal_code
        FROM properties
        WHERE postal_code = %s;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (postal_code,))
        candidates = cur.fetchall()
        
        if candidates:
            columns = [desc[0] for desc in cur.description]
            
            for candidate_row in candidates:
                candidate = dict(zip(columns, candidate_row))
                
                # Generate expansions for candidate
                candidate_expansions = normalize_address_for_matching(
                    candidate['address_number'],
                    candidate['street_name'],
                    candidate['postal_code'],
                    candidate['municipality']
                )
                
                # Check for intersection
                intersection = listing_expansions & candidate_expansions
                if intersection:
                    logger.debug(f"Address expansion match found: {intersection}")
                    return candidate, 'address_expansion'
    
    # Fallback: Coordinate-based matching if coordinates provided
    if lat is not None and lon is not None:
        coord_match = match_property_by_coordinates(conn, lat, lon, radius_m=50)
        if coord_match:
            logger.debug(f"Coordinate proximity match found: {coord_match['distance_m']:.1f}m away")
            return coord_match, 'coordinate_proximity'
    
    return None


def match_existing_properties(new_listings_path: str, removed_listings_path: str, workdir: str) -> Dict[str, Any]:
    """
    Match new and removed listings to existing properties in master DB.
    
    Args:
        new_listings_path: Path to new_pool_listings.parquet
        removed_listings_path: Path to removed_pool_listings.parquet
        workdir: Working directory for output files
        
    Returns:
        Dict with paths to matched DataFrames
    """
    logger.info("="*60)
    logger.info("MATCHING EXISTING PROPERTIES")
    logger.info("="*60)
    
    # Load new listings
    new_df = pd.read_parquet(new_listings_path)
    logger.info(f"Loaded {len(new_df)} new listings")
    
    # Load removed listings
    removed_df = pd.read_parquet(removed_listings_path)
    logger.info(f"Loaded {len(removed_df)} removed listings")
    
    conn = get_master_db_connection()
    
    try:
        # Match new listings
        new_df['property_id'] = None
        new_df['address_id'] = None
        new_df['match_type'] = None
        
        matched_count = 0
        match_types = {'address_expansion': 0, 'coordinate_proximity': 0}
        
        for idx, row in new_df.iterrows():
            if pd.isna(row['address_number']) or pd.isna(row['street_name']) or pd.isna(row['postal_code']):
                continue
            
            # Pass optional fields for better matching
            municipality = str(row['municipality']) if not pd.isna(row.get('municipality')) else None
            lat = float(row['latitude']) if not pd.isna(row.get('latitude')) else None
            lon = float(row['longitude']) if not pd.isna(row.get('longitude')) else None
            
            match_result = match_property_by_address(
                conn,
                int(row['address_number']),
                str(row['street_name']),
                str(row['postal_code']),
                municipality=municipality,
                lat=lat,
                lon=lon
            )
            
            if match_result:
                match, match_type = match_result
                new_df.at[idx, 'property_id'] = match['property_id']
                new_df.at[idx, 'address_id'] = match['address_id']
                new_df.at[idx, 'match_type'] = match_type
                matched_count += 1
                match_types[match_type] = match_types.get(match_type, 0) + 1
        
        logger.info(f"New listings matched to existing properties: {matched_count}/{len(new_df)}")
        logger.info(f"Match breakdown - Address expansion: {match_types['address_expansion']}, Coordinate: {match_types['coordinate_proximity']}")
        
        # Match removed listings
        removed_df['property_id'] = None
        removed_df['address_id'] = None
        removed_df['match_type'] = None
        
        matched_removed = 0
        removed_match_types = {'address_expansion': 0, 'coordinate_proximity': 0}
        
        for idx, row in removed_df.iterrows():
            if pd.isna(row['address_number']) or pd.isna(row['street_name']) or pd.isna(row['postal_code']):
                continue
            
            # Pass optional fields for better matching
            municipality = str(row['municipality']) if not pd.isna(row.get('municipality')) else None
            lat = float(row['latitude']) if not pd.isna(row.get('latitude')) else None
            lon = float(row['longitude']) if not pd.isna(row.get('longitude')) else None
            
            match_result = match_property_by_address(
                conn,
                int(row['address_number']),
                str(row['street_name']),
                str(row['postal_code']),
                municipality=municipality,
                lat=lat,
                lon=lon
            )
            
            if match_result:
                match, match_type = match_result
                removed_df.at[idx, 'property_id'] = match['property_id']
                removed_df.at[idx, 'address_id'] = match['address_id']
                removed_df.at[idx, 'match_type'] = match_type
                matched_removed += 1
                removed_match_types[match_type] = removed_match_types.get(match_type, 0) + 1
            else:
                logger.warning(f"Removed listing {row['mls_id']} has no property match")
        
        logger.info(f"Removed listings matched to existing properties: {matched_removed}/{len(removed_df)}")
        logger.info(f"Match breakdown - Address expansion: {removed_match_types['address_expansion']}, Coordinate: {removed_match_types['coordinate_proximity']}")
        
        # Save matched DataFrames
        new_out = f"{workdir}/new_listings_matched.parquet"
        removed_out = f"{workdir}/removed_listings_matched.parquet"
        
        new_df.to_parquet(new_out, index=False)
        removed_df.to_parquet(removed_out, index=False)
        
        logger.info(f"Saved matched new listings to: {new_out}")
        logger.info(f"Saved matched removed listings to: {removed_out}")
        logger.info("="*60)
        
        return {
            "new_matched_path": new_out,
            "removed_matched_path": removed_out,
            "new_matched_count": matched_count,
            "removed_matched_count": matched_removed,
        }
        
    finally:
        conn.close()


def transform_create_property_pool_objects(new_matched_path: str, workdir: str) -> Dict[str, Any]:
    """
    Create property and pool objects for new listings without existing properties.
    
    Args:
        new_matched_path: Path to new_listings_matched.parquet
        workdir: Working directory for output files
        
    Returns:
        Dict with paths to transformed DataFrames
    """
    logger.info("="*60)
    logger.info("TRANSFORMING: CREATE PROPERTY/POOL OBJECTS")
    logger.info("="*60)
    
    df = pd.read_parquet(new_matched_path)
    logger.info(f"Loaded {len(df)} listings")
    
    # Split into new properties vs existing properties
    new_properties_df = df[df['property_id'].isna()].copy()
    existing_properties_df = df[df['property_id'].notna()].copy()
    
    logger.info(f"Creating properties for {len(new_properties_df)} new listings")
    logger.info(f"Existing properties matched: {len(existing_properties_df)}")
    
    # Generate address_ids for new properties (reset any existing values to ensure consistency)
    new_properties_df['address_id'] = [generate_address_id() for _ in range(len(new_properties_df))]
    
    # Prepare properties DataFrame (only for new properties)
    properties = new_properties_df[[
        'address_id', 'address_number', 'street_name', 'municipality',
        'province_state', 'postal_code', 'lat', 'lon'
    ]].copy()
    properties['country'] = 'Canada'
    
    # Ensure address_id is int64 (should already be integers from generate_address_id)
    properties['address_id'] = properties['address_id'].astype('int64')
    
    # Prepare pools DataFrame - combine new properties + existing properties
    # For new properties: use address_id (will be mapped to property_id in load step)
    pools_new = new_properties_df[['address_id', 'pool_type']].copy() if len(new_properties_df) > 0 else pd.DataFrame(columns=['address_id', 'pool_type'])
    
    # Ensure address_id is int64 for pools
    if len(pools_new) > 0:
        pools_new['address_id'] = pools_new['address_id'].astype('int64')
    
    # For existing properties: use property_id directly (no address_id mapping needed)
    pools_existing = existing_properties_df[['property_id', 'pool_type']].copy() if len(existing_properties_df) > 0 else pd.DataFrame(columns=['property_id', 'pool_type'])
    
    # Keep pools separate - don't mix address_id (int) with property_id (UUID)
    if len(pools_new) > 0:
        pools_new['is_new_property'] = True
    if len(pools_existing) > 0:
        pools_existing['is_new_property'] = False
    
    # Save pools_new and pools_existing separately, then combine in load step
    if len(pools_new) > 0:
        pools_new_out = f"{workdir}/pools_new.parquet"
        pools_new.to_parquet(pools_new_out, index=False)
    
    if len(pools_existing) > 0:
        pools_existing_out = f"{workdir}/pools_existing.parquet"
        pools_existing.to_parquet(pools_existing_out, index=False)
    
    # Create a combined metadata for the load step
    pools = pd.DataFrame({
        'new_count': [len(pools_new)],
        'existing_count': [len(pools_existing)],
        'total_count': [len(pools_new) + len(pools_existing)]
    })
    
    logger.info(f"Pools from new properties: {len(pools_new)}")
    logger.info(f"Pools from existing properties: {len(pools_existing)}")
    
    # Prepare listings DataFrame (only for new properties)
    listings = new_properties_df[[
        'mls_id', 'address_id', 'bathrooms', 'bedrooms', 'date_collected',
        'description', 'house_cat', 'address_number', 'price', 'size_sqft', 'stories'
    ]].copy()
    listings['is_removed'] = False
    listings['removal_date'] = None
    listings.rename(columns={'address_number': 'listing_address_number'}, inplace=True)
    
    # Save outputs
    properties_out = f"{workdir}/properties_to_insert.parquet"
    pools_out = f"{workdir}/pools_metadata.parquet"  # Just metadata now
    listings_out = f"{workdir}/listings_to_insert.parquet"
    
    properties.to_parquet(properties_out, index=False)
    pools.to_parquet(pools_out, index=False)  # Save metadata
    listings.to_parquet(listings_out, index=False)
    
    logger.info(f"Properties to insert: {len(properties)}")
    logger.info(f"Pools new to insert: {len(pools_new)}")
    logger.info(f"Pools existing: {len(pools_existing)}")
    logger.info(f"Listings to insert: {len(listings)}")
    logger.info("="*60)
    
    return {
        "properties_path": properties_out,
        "pools_path": pools_out,  # metadata
        "pools_new_path": f"{workdir}/pools_new.parquet" if len(pools_new) > 0 else None,
        "pools_existing_path": f"{workdir}/pools_existing.parquet" if len(pools_existing) > 0 else None,
        "listings_path": listings_out,
        "property_count": len(properties),
        "pool_new_count": len(pools_new),
        "pool_existing_count": len(pools_existing),
    }


def transform_mark_removed_listings(removed_matched_path: str, workdir: str) -> Dict[str, Any]:
    """
    Prepare update records for removed listings.
    
    Args:
        removed_matched_path: Path to removed_listings_matched.parquet
        workdir: Working directory for output files
        
    Returns:
        Dict with path to update DataFrame
    """
    logger.info("="*60)
    logger.info("TRANSFORMING: MARK REMOVED LISTINGS")
    logger.info("="*60)
    
    df = pd.read_parquet(removed_matched_path)
    logger.info(f"Loaded {len(df)} removed listings")
    
    # Filter to listings with property match
    updates = df[df['property_id'].notna()].copy()
    logger.info(f"Listings to mark as removed: {len(updates)}")
    
    if len(updates) < len(df):
        unmatched = len(df) - len(updates)
        logger.warning(f"{unmatched} removed listings have no property match - skipping")
    
    # Prepare update DataFrame
    updates_df = updates[['mls_id', 'removal_date']].copy()
    
    out_path = f"{workdir}/listings_to_update.parquet"
    updates_df.to_parquet(out_path, index=False)
    
    logger.info(f"Saved updates to: {out_path}")
    logger.info("="*60)
    
    return {
        "updates_path": out_path,
        "update_count": len(updates_df),
    }
