"""
OSM Pool Extraction Module

Handles querying OpenStreetMap Overpass API for swimming pool data
within specified geographic polygons.
"""

import time
import logging
from typing import Dict, Any, List, Tuple
import requests
from shapely.geometry import shape, box, Polygon
import pandas as pd

logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def parse_polygon_input(polygon_config: Dict[str, Any]) -> Polygon:
    """
    Parse polygon input from various formats.
    
    Args:
        polygon_config: Dict with 'type' and 'data' keys
            type: 'geojson' or 'wkt'
            data: GeoJSON polygon object or WKT string
    
    Returns:
        shapely.Polygon object
        
    Raises:
        ValueError: If format is invalid or unsupported
    """
    polygon_type = polygon_config.get("type", "").lower()
    polygon_data = polygon_config.get("data")
    
    if not polygon_data:
        raise ValueError("Polygon data is required")
    
    if polygon_type == "geojson":
        try:
            return shape(polygon_data)
        except Exception as e:
            raise ValueError(f"Invalid GeoJSON polygon: {e}")
    
    elif polygon_type == "wkt":
        try:
            from shapely import wkt
            return wkt.loads(polygon_data)
        except Exception as e:
            raise ValueError(f"Invalid WKT polygon: {e}")
    
    else:
        raise ValueError(f"Unsupported polygon type: {polygon_type}. Use 'geojson' or 'wkt'")


def subdivide_bbox(south: float, west: float, north: float, east: float, 
                    side_deg: float = 0.02) -> List[Tuple[float, float, float, float]]:
    """
    Subdivide a large bounding box into smaller tiles.
    
    Args:
        south: Southern latitude bound
        west: Western longitude bound
        north: Northern latitude bound
        east: Eastern longitude bound
        side_deg: Tile size in degrees (approximately 2km at mid-latitudes)
    
    Returns:
        List of (south, west, north, east) tuples for each tile
    """
    boxes = []
    lat = south
    while lat < north:
        lon = west
        next_lat = min(lat + side_deg, north)
        while lon < east:
            next_lon = min(lon + side_deg, east)
            boxes.append((lat, lon, next_lat, next_lon))
            lon = next_lon
        lat = next_lat
    return boxes


def make_overpass_query(south: float, west: float, north: float, east: float) -> str:
    """
    Generate Overpass QL query for swimming pools in bounding box.
    
    Args:
        south, west, north, east: Bounding box coordinates
    
    Returns:
        Overpass QL query string
    """
    return f"""
    [out:json][timeout:120];
    (
      node["leisure"="swimming_pool"]({south},{west},{north},{east});
      way["leisure"="swimming_pool"]({south},{west},{north},{east});
      relation["leisure"="swimming_pool"]({south},{west},{north},{east});
      node["swimming_pool"="yes"]({south},{west},{north},{east});
      way["swimming_pool"="yes"]({south},{west},{north},{east});
      relation["swimming_pool"="yes"]({south},{west},{north},{east});
    );
    out center tags;
    """


def calculate_bbox_size_meters(south: float, west: float, north: float, east: float) -> Tuple[float, float]:
    """
    Calculate approximate bbox dimensions in meters.
    
    Args:
        south, west, north, east: Bounding box coordinates
    
    Returns:
        Tuple of (width_meters, height_meters)
    """
    from math import radians, cos, sin, sqrt, atan2
    
    # Earth radius in meters
    R = 6371000
    
    # Calculate height (north-south distance)
    lat1, lat2 = radians(south), radians(north)
    lon_avg = radians((west + east) / 2)
    
    dlat = lat2 - lat1
    a = sin(dlat/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    height = R * c
    
    # Calculate width (east-west distance at average latitude)
    lat_avg = radians((south + north) / 2)
    lon1, lon2 = radians(west), radians(east)
    
    dlon = lon2 - lon1
    a = cos(lat_avg)**2 * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    width = R * c
    
    return width, height


def query_overpass_api(south: float, west: float, north: float, east: float,
                       retry_count: int = 0, max_retries: int = 3) -> List[Dict]:
    """
    Query Overpass API with retry logic and subdivision on failure.
    
    Args:
        south, west, north, east: Bounding box coordinates
        retry_count: Current retry attempt
        max_retries: Maximum retry attempts for rate limiting
    
    Returns:
        List of OSM elements (nodes, ways, relations)
    """
    query = make_overpass_query(south, west, north, east)
    
    try:
        response = requests.post(OVERPASS_URL, data={"data": query}, timeout=180)
    except requests.exceptions.RequestException as err:
        logger.warning(f"Request failed for bbox ({south},{west},{north},{east}): {err}")
        return []
    
    # Handle rate limiting
    if response.status_code == 429:
        if retry_count < max_retries:
            wait_time = 2 ** retry_count * 15  # Exponential backoff: 5, 10, 20 seconds
            logger.warning(f"Rate limited (429). Waiting {wait_time}s before retry {retry_count + 1}/{max_retries}")
            time.sleep(wait_time)
            return query_overpass_api(south, west, north, east, retry_count + 1, max_retries)
        else:
            logger.error(f"Rate limit exceeded after {max_retries} retries")
            return []
    
    # Handle gateway timeout by subdividing
    if response.status_code == 504:
        # Check minimum bbox size (500m x 500m)
        width_m, height_m = calculate_bbox_size_meters(south, west, north, east)
        min_dimension = min(width_m, height_m)
        
        logger.warning(f"Gateway timeout (504) for bbox ({south},{west},{north},{east})")
        logger.info(f"Bbox dimensions: {width_m:.0f}m x {height_m:.0f}m")
        
        if min_dimension <= 500:
            # Already at minimum size - wait 3 minutes and retry once
            logger.warning(f"Bbox already at minimum size ({min_dimension:.0f}m). Waiting 180 seconds before final retry...")
            time.sleep(180)
            
            # Final retry with same bbox
            try:
                response = requests.post(OVERPASS_URL, data={"data": query}, timeout=180)
                if response.status_code == 200:
                    data = response.json()
                    return data.get("elements", [])
                else:
                    logger.error(f"Final retry failed with status {response.status_code}")
                    return []
            except Exception as e:
                logger.error(f"Final retry failed: {e}")
                return []
        else:
            # Subdivide into 4 quadrants
            logger.info("Subdividing bbox into 4 quadrants")
            mid_lat = (south + north) / 2
            mid_lon = (west + east) / 2
            return (
                query_overpass_api(south, west, mid_lat, mid_lon) +
                query_overpass_api(south, mid_lon, mid_lat, east) +
                query_overpass_api(mid_lat, west, north, mid_lon) +
                query_overpass_api(mid_lat, mid_lon, north, east)
            )
    
    # Handle other errors
    if response.status_code != 200:
        logger.error(f"Overpass API error {response.status_code} for bbox ({south},{west},{north},{east})")
        return []
    
    # Parse response
    try:
        data = response.json()
        return data.get("elements", [])
    except Exception as e:
        logger.error(f"Failed to parse JSON response: {e}")
        return []


def extract_pool_coordinates(elements: List[Dict]) -> List[Dict[str, Any]]:
    """
    Extract pool coordinates and tags from OSM elements.
    
    Args:
        elements: List of OSM elements from Overpass API
    
    Returns:
        List of dicts with 'lat', 'lon', and 'tags' keys
    """
    results = []
    for element in elements:
        # Get coordinates
        if "lat" in element and "lon" in element:
            lat, lon = element["lat"], element["lon"]
        elif "center" in element:
            lat, lon = element["center"]["lat"], element["center"]["lon"]
        else:
            continue
        
        results.append({
            "lat": lat,
            "lon": lon,
            "tags": element.get("tags", {})
        })
    
    return results


def extract_pools_from_polygon(polygon: Polygon, workdir: str) -> Dict[str, Any]:
    """
    Extract all swimming pools within a polygon from OSM.
    
    Args:
        polygon: Shapely polygon defining search area
        workdir: Working directory for output files
    
    Returns:
        Dict with:
            - parquet_path: Path to output parquet file
            - pool_count: Number of unique pools found
            - bounds: (west, south, east, north) tuple
    """
    logger.info("="*60)
    logger.info("OSM POOL EXTRACTION - Starting")
    logger.info("="*60)
    
    # Get polygon bounds
    west, south, east, north = polygon.bounds
    logger.info(f"Polygon bounds: South={south:.4f}, West={west:.4f}, North={north:.4f}, East={east:.4f}")
    
    # Subdivide into manageable tiles
    tiles = subdivide_bbox(south, west, north, east)
    logger.info(f"Subdivided area into {len(tiles)} tiles")
    
    # Filter tiles that intersect polygon
    tiles_to_process = []
    for (ts, tw, tn, te) in tiles:
        tile = box(tw, ts, te, tn)
        if polygon.intersects(tile):
            tiles_to_process.append((ts, tw, tn, te))
    
    logger.info(f"Processing {len(tiles_to_process)} tiles intersecting polygon")
    
    # Query each tile
    all_pools = []
    seen_coordinates = set()
    
    for i, (ts, tw, tn, te) in enumerate(tiles_to_process, 1):
        logger.info(f"[Tile {i}/{len(tiles_to_process)}] Querying bbox ({ts:.4f},{tw:.4f},{tn:.4f},{te:.4f})")
        
        elements = query_overpass_api(ts, tw, tn, te)
        
        if elements:
            pools = extract_pool_coordinates(elements)
            logger.info(f"  Found {len(elements)} raw elements, extracted {len(pools)} pools")
            
            # Deduplicate by coordinates
            new_count = 0
            for pool in pools:
                coord_key = (pool["lat"], pool["lon"])
                if coord_key not in seen_coordinates:
                    seen_coordinates.add(coord_key)
                    all_pools.append(pool)
                    new_count += 1
            
            if new_count > 0:
                logger.info(f"  Added {new_count} new unique pools")
        
        # Be nice to the API
        time.sleep(0.1)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_pools)
    
    logger.info("="*60)
    logger.info("OSM POOL EXTRACTION - Complete")
    logger.info("="*60)
    logger.info(f"Total unique pools found: {len(df)}")
    
    # Save to parquet
    output_path = f"{workdir}/osm_pools.parquet"
    df.to_parquet(output_path, index=False)
    logger.info(f"Saved to: {output_path}")
    logger.info("="*60)
    
    return {
        "parquet_path": output_path,
        "pool_count": len(df),
        "bounds": (west, south, east, north),
    }
