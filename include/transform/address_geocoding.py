"""
Address Geocoding Module

Handles reverse geocoding of pool coordinates to structured addresses
using Geocodio API.
"""

import os
import time
import logging
import requests
from typing import Dict, Any, List, Optional
import pandas as pd

logger = logging.getLogger(__name__)


def get_geocodio_api_key() -> str:
    """Get Geocodio API key from environment."""
    api_key = os.getenv("GEOCODIO_API_KEY")
    if not api_key:
        raise ValueError("GEOCODIO_API_KEY environment variable not set")
    return api_key


def geocode_pool_location(lat: float, lon: float, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Reverse geocode a single pool location using Geocodio.
    
    Args:
        lat: Latitude
        lon: Longitude
        api_key: Geocodio API key
    
    Returns:
        Dict with address components or None if geocoding fails
    """
    try:
        url = "https://api.geocod.io/v1.7/reverse"
        params = {
            "q": f"{lat},{lon}",
            "api_key": api_key
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if not data.get("results"):
            return None
        
        result = data["results"][0]
        addr_comp = result.get("address_components", {})
        
        # Map Geocodio components to expected format (same structure as previous implementation)
        address_components = {
            "street_number": addr_comp.get("number"),
            "route": addr_comp.get("formatted_street") or addr_comp.get("street"),
            "locality": addr_comp.get("city"),
            "administrative_area_level_1": addr_comp.get("state"),
            "country": addr_comp.get("country"),
            "postal_code": addr_comp.get("zip")
        }
        
        return address_components
    
    except requests.exceptions.Timeout:
        logger.warning(f"Geocoding timeout for ({lat}, {lon})")
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logger.error("Geocodio API rate limit exceeded")
            raise
        logger.warning(f"Geocoding failed for ({lat}, {lon}): {e}")
        return None
    except Exception as e:
        logger.warning(f"Geocoding failed for ({lat}, {lon}): {e}")
        return None


def batch_geocode_pools(pools_df: pd.DataFrame, workdir: str, 
                        batch_size: int = 100, delay: float = 0.06) -> Dict[str, Any]:  # 0.06s = 1000 req/min (Geocodio rate limit)
    """
    Geocode all pools with rate limiting and progress tracking.
    
    Args:
        pools_df: DataFrame with 'lat' and 'lon' columns
        workdir: Working directory for output
        batch_size: Number of requests before longer pause
        delay: Delay between requests in seconds (default 0.06s = 1000 req/min max)
    
    Returns:
        Dict with:
            - parquet_path: Path to geocoded results
            - success_count: Number successfully geocoded
            - total_count: Total pools processed
    """
    logger.info("="*60)
    logger.info("ADDRESS GEOCODING - Starting")
    logger.info("="*60)
    logger.info(f"Geocoding {len(pools_df)} pool locations")
    
    # Initialize Geocodio API key
    api_key = get_geocodio_api_key()
    
    # Geocode each pool
    geocoded_results = []
    success_count = 0
    
    for i, row in pools_df.iterrows():
        if i > 0 and i % batch_size == 0:
            logger.info(f"Progress: {i}/{len(pools_df)} pools geocoded")
            time.sleep(1)  # Longer pause every batch
        
        address_components = geocode_pool_location(row["lat"], row["lon"], api_key)
        
        if address_components:
            geocoded_results.append({
                "lat": row["lat"],
                "lon": row["lon"],
                "tags": row.get("tags", {}),
                **address_components
            })
            success_count += 1
        else:
            # Keep the pool but without address
            geocoded_results.append({
                "lat": row["lat"],
                "lon": row["lon"],
                "tags": row.get("tags", {}),
            })
        
        time.sleep(delay)
    
    # Convert to DataFrame
    geocoded_df = pd.DataFrame(geocoded_results)
    
    logger.info("="*60)
    logger.info("ADDRESS GEOCODING - Complete")
    logger.info("="*60)
    logger.info(f"Successfully geocoded: {success_count}/{len(pools_df)}")
    
    # Save to parquet
    output_path = f"{workdir}/geocoded_pools.parquet"
    geocoded_df.to_parquet(output_path, index=False)
    logger.info(f"Saved to: {output_path}")
    logger.info("="*60)
    
    return {
        "parquet_path": output_path,
        "success_count": success_count,
        "total_count": len(pools_df),
    }


def extract_unique_addresses(geocoded_df: pd.DataFrame, workdir: str) -> Dict[str, Any]:
    """
    Extract and deduplicate addresses from geocoded pools.
    
    Args:
        geocoded_df: DataFrame with geocoded pool data
        workdir: Working directory for output
    
    Returns:
        Dict with:
            - parquet_path: Path to unique addresses
            - address_count: Number of unique addresses
    """
    logger.info("="*60)
    logger.info("ADDRESS EXTRACTION - Starting")
    logger.info("="*60)
    
    # Extract address columns
    address_columns = [
        "lat", "lon", "street_number", "route", 
        "locality", "administrative_area_level_1", 
        "country", "postal_code"
    ]
    
    # Filter rows with at least some address data
    has_address = geocoded_df["route"].notna() | geocoded_df["locality"].notna()
    addresses_df = geocoded_df[has_address][address_columns].copy()
    
    logger.info(f"Found {len(addresses_df)} pools with address data")
    
    # Standardize and deduplicate
    addresses_df = addresses_df.fillna("")
    
    # Create deduplication key
    addresses_df["address_key"] = (
        addresses_df["street_number"].astype(str) + "|" +
        addresses_df["route"].astype(str) + "|" +
        addresses_df["locality"].astype(str) + "|" +
        addresses_df["postal_code"].astype(str)
    )
    
    # Deduplicate
    unique_addresses = addresses_df.drop_duplicates(subset=["address_key"])
    unique_addresses = unique_addresses.drop(columns=["address_key"])
    
    # Rename columns to match database schema
    unique_addresses = unique_addresses.rename(columns={
        "street_number": "address_number",
        "route": "street_name",
        "locality": "municipality",
        "administrative_area_level_1": "province_state",
    })
    
    logger.info("="*60)
    logger.info("ADDRESS EXTRACTION - Complete")
    logger.info("="*60)
    logger.info(f"Unique addresses: {len(unique_addresses)}")
    
    # Save to parquet
    output_path = f"{workdir}/unique_addresses.parquet"
    unique_addresses.to_parquet(output_path, index=False)
    logger.info(f"Saved to: {output_path}")
    logger.info("="*60)
    
    return {
        "parquet_path": output_path,
        "address_count": len(unique_addresses),
    }
