"""
OSM Collection DAG

Collects swimming pool data from OpenStreetMap for a specified polygon,
geocodes the locations, and loads to the stage database.

Triggered via Airflow REST API with polygon configuration in dag_run.conf.

Example trigger:
POST /api/v1/dags/osm_collection/dagRuns
{
    "conf": {
        "polygon": {
            "type": "geojson",
            "data": {
                "type": "Polygon",
                "coordinates": [[[-64.8, 46.0], [-64.7, 46.0], [-64.7, 46.1], [-64.8, 46.1], [-64.8, 46.0]]]
            }
        }
    }
}

Or with WKT:
{
    "conf": {
        "polygon": {
            "type": "wkt",
            "data": "POLYGON((-64.8 46.0, -64.7 46.0, -64.7 46.1, -64.8 46.1, -64.8 46.0))"
        }
    }
}
"""

import logging
import os
from datetime import datetime
from airflow.decorators import dag, task
import pandas as pd

from include.extract.osm_pool_extraction import (
    parse_polygon_input,
    extract_pools_from_polygon,
)
from include.transform.address_geocoding import (
    batch_geocode_pools,
    extract_unique_addresses,
)
from include.load.osm_stage_load import (
    load_addresses_to_stage,
    load_pools_to_stage,
    create_pool_address_assignments,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="osm_collection",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Triggered via API only
    catchup=False,
    tags=["osm", "pools", "geocoding", "stage"],
    description="Extract OSM pool data for a polygon and load to stage database",
    doc_md=__doc__,
)
def osm_collection_dag():
    """OSM Collection ETL Pipeline"""
    
    @task()
    def parse_and_extract_pools(**context) -> dict:
        """
        Parse input polygon and extract all pools from OSM.
        
        Returns:
            Dict with pool extraction metadata
        """
        logger.info("="*80)
        logger.info("OSM COLLECTION PIPELINE - STARTED")
        logger.info("="*80)
        
        # Get polygon configuration from dag_run.conf
        dag_run = context["dag_run"]
        polygon_config = dag_run.conf.get("polygon")
        
        if not polygon_config:
            raise ValueError("No polygon configuration provided in dag_run.conf")
        
        logger.info(f"Polygon configuration type: {polygon_config.get('type')}")
        
        # Parse polygon
        polygon = parse_polygon_input(polygon_config)
        logger.info(f"Parsed polygon with bounds: {polygon.bounds}")
        
        # Create working directory
        run_id = dag_run.run_id
        workdir = f"/opt/airflow/data/tmp/osm_collection/{run_id}"
        os.makedirs(workdir, exist_ok=True)
        logger.info(f"Working directory: {workdir}")
        
        # Extract pools
        result = extract_pools_from_polygon(polygon, workdir)
        result["workdir"] = workdir
        
        return result
    
    @task()
    def geocode_pools(extraction_result: dict) -> dict:
        """
        Geocode all extracted pools to get structured addresses.
        
        Args:
            extraction_result: Output from parse_and_extract_pools
        
        Returns:
            Dict with geocoding metadata
        """
        pools_parquet = extraction_result["parquet_path"]
        workdir = extraction_result["workdir"]
        
        # Read pools
        pools_df = pd.read_parquet(pools_parquet)
        
        # Geocode
        result = batch_geocode_pools(pools_df, workdir)
        result["workdir"] = workdir
        
        return result
    
    @task()
    def extract_addresses(geocoding_result: dict) -> dict:
        """
        Extract and deduplicate unique addresses from geocoded data.
        
        Args:
            geocoding_result: Output from geocode_pools
        
        Returns:
            Dict with address extraction metadata
        """
        geocoded_parquet = geocoding_result["parquet_path"]
        workdir = geocoding_result["workdir"]
        
        # Read geocoded data
        geocoded_df = pd.read_parquet(geocoded_parquet)
        
        # Extract unique addresses
        result = extract_unique_addresses(geocoded_df, workdir)
        result["workdir"] = workdir
        
        return result
    
    @task()
    def load_to_stage_db(
        extraction_result: dict,
        address_result: dict,
    ) -> dict:
        """
        Load addresses and pools to stage database.
        
        Args:
            extraction_result: Output from parse_and_extract_pools
            address_result: Output from extract_addresses
        
        Returns:
            Dict with loading metadata and summary
        """
        pools_parquet = extraction_result["parquet_path"]
        addresses_parquet = address_result["parquet_path"]
        workdir = extraction_result["workdir"]
        
        # Load addresses
        address_load_result = load_addresses_to_stage(addresses_parquet)
        logger.info(f"Loaded {address_load_result['inserted_count']} addresses to stage")
        
        # Load pools
        pool_load_result = load_pools_to_stage(pools_parquet)
        logger.info(f"Loaded {pool_load_result['inserted_count']} pools to stage")
        
        # Create pool-address assignments
        assignment_result = create_pool_address_assignments(pools_parquet, workdir)
        logger.info(f"Created {assignment_result['assignment_count']} pool-address assignments")
        
        # Summary
        logger.info("="*80)
        logger.info("OSM COLLECTION PIPELINE - COMPLETED")
        logger.info("="*80)
        logger.info(f"Pools extracted: {extraction_result['pool_count']}")
        logger.info(f"Unique addresses: {address_result['address_count']}")
        logger.info(f"Addresses loaded: {address_load_result['inserted_count']}")
        logger.info(f"Pools loaded: {pool_load_result['inserted_count']}")
        logger.info(f"Pool-address assignments: {assignment_result['assignment_count']}")
        logger.info("="*80)
        
        return {
            "pools_extracted": extraction_result["pool_count"],
            "addresses_found": address_result["address_count"],
            "addresses_loaded": address_load_result["inserted_count"],
            "addresses_skipped": address_load_result["skipped_count"],
            "pools_loaded": pool_load_result["inserted_count"],
            "pools_skipped": pool_load_result["skipped_count"],
            "assignments_created": assignment_result["assignment_count"],
            "workdir": workdir,
        }
    
    # Define task dependencies
    extraction = parse_and_extract_pools()
    geocoding = geocode_pools(extraction)
    addresses = extract_addresses(geocoding)
    loading = load_to_stage_db(extraction, addresses)


# Instantiate the DAG
osm_collection_dag()
