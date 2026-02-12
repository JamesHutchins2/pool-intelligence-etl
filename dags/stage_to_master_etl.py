"""
Stage to Master ETL DAG

Extracts pending assignments from stage database, cleans and deduplicates data,
and loads new addresses and pools to the master database.

Schedule:
- Every Tuesday at 2:00 AM
- Can also be triggered via webhook (Airflow REST API)

Note:
- MASTER_DB_URL must be set in Airflow environment variables
"""

import logging
import os
from datetime import datetime
from airflow.decorators import dag, task
import pandas as pd

from include.extract.stage_data_extraction import (
    extract_pending_assignments,
    fetch_pool_and_address_details,
    merge_assignment_data,
)
from include.transform.stage_address_cleaning import (
    clean_and_transform_data,
)
from include.load.master_db_load import (
    load_addresses_to_master,
    load_pools_to_master,
    update_stage_upload_status,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="stage_to_master_etl",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * 2",  # Every Tuesday at 2:00 AM (cron: minute hour day month day_of_week)
    catchup=False,
    tags=["stage", "master", "etl", "addresses", "pools"],
    description="Extract from stage DB, clean, and load to master DB",
    doc_md=__doc__,
    max_active_runs=1,  # Prevent concurrent runs
)
def stage_to_master_etl_dag():
    """Stage to Master ETL Pipeline"""
    
    @task()
    def extract_stage_assignments(**context) -> dict:
        """
        Extract pending assignments from stage database.
        
        Returns:
            Dict with extraction metadata
        """
        logger.info("="*80)
        logger.info("STAGE TO MASTER ETL - STARTED")
        logger.info("="*80)
        
        # Create working directory
        dag_run = context["dag_run"]
        run_id = dag_run.run_id
        workdir = f"/opt/airflow/data/tmp/stage_to_master/{run_id}"
        os.makedirs(workdir, exist_ok=True)
        logger.info(f"Working directory: {workdir}")
        
        # Extract assignments
        result = extract_pending_assignments(workdir)
        result["workdir"] = workdir
        
        # Check if there's work to do
        if result["assignment_count"] == 0:
            logger.info("="*80)
            logger.info("No pending assignments - pipeline complete")
            logger.info("="*80)
        
        return result
    
    @task()
    def fetch_details(extraction_result: dict) -> dict:
        """
        Fetch detailed pool and address records.
        
        Args:
            extraction_result: Output from extract_stage_assignments
        
        Returns:
            Dict with fetch metadata
        """
        # Skip if no assignments
        if extraction_result["assignment_count"] == 0:
            return {"skip": True, "workdir": extraction_result["workdir"]}
        
        workdir = extraction_result["workdir"]
        pool_ids = extraction_result["unique_pool_ids"]
        address_ids = extraction_result["unique_address_ids"]
        
        result = fetch_pool_and_address_details(pool_ids, address_ids, workdir)
        result["workdir"] = workdir
        
        return result
    
    @task()
    def merge_data(extraction_result: dict, fetch_result: dict) -> dict:
        """
        Merge assignments with pool and address details.
        
        Args:
            extraction_result: Output from extract_stage_assignments
            fetch_result: Output from fetch_details
        
        Returns:
            Dict with merge metadata
        """
        # Skip if no assignments
        if fetch_result.get("skip"):
            return {"skip": True, "workdir": fetch_result["workdir"]}
        
        workdir = fetch_result["workdir"]
        assignments_parquet = extraction_result["parquet_path"]
        pools_parquet = fetch_result["pools_parquet"]
        addresses_parquet = fetch_result["addresses_parquet"]
        
        result = merge_assignment_data(
            assignments_parquet,
            pools_parquet,
            addresses_parquet,
            workdir
        )
        result["workdir"] = workdir
        
        return result
    
    @task()
    def clean_and_filter_data(merge_result: dict, **context) -> dict:
        """
        Clean, standardize, deduplicate, and filter data.
        
        Args:
            merge_result: Output from merge_data
        
        Returns:
            Dict with cleaning metadata
        """
        # Skip if no data
        if merge_result.get("skip"):
            return {"skip": True, "workdir": merge_result["workdir"]}
        
        workdir = merge_result["workdir"]
        merged_parquet = merge_result["parquet_path"]
        
        # Get master DB URL from environment
        master_db_url = os.getenv("MASTER_DB_URL")
        
        if not master_db_url:
            raise ValueError("MASTER_DB_URL must be set in environment variables")
        
        result = clean_and_transform_data(merged_parquet, master_db_url, workdir)
        result["workdir"] = workdir
        result["master_db_url"] = master_db_url
        
        # Check if any records remain after cleaning
        if result["record_count"] == 0:
            logger.info("="*80)
            logger.info("No valid records after cleaning - pipeline complete")
            logger.info("="*80)
        
        return result
    
    @task()
    def load_to_master_db(clean_result: dict) -> dict:
        """
        Load addresses and pools to master database.
        
        Args:
            clean_result: Output from clean_and_filter_data
        
        Returns:
            Dict with load metadata
        """
        # Skip if no data
        if clean_result.get("skip") or clean_result["record_count"] == 0:
            return {
                "skip": True,
                "workdir": clean_result["workdir"],
                "assignment_ids": clean_result.get("assignment_ids", [])
            }
        
        cleaned_parquet = clean_result["parquet_path"]
        master_db_url = clean_result["master_db_url"]
        workdir = clean_result["workdir"]
        
        # Load addresses
        address_result = load_addresses_to_master(cleaned_parquet, master_db_url, workdir)
        logger.info(f"Loaded {address_result['inserted_count']} addresses to master")
        
        # Load pools
        pool_result = load_pools_to_master(cleaned_parquet, master_db_url)
        logger.info(f"Loaded {pool_result['inserted_count']} pools to master")
        
        return {
            "addresses_inserted": address_result["inserted_count"],
            "addresses_failed": address_result["failed_count"],
            "pools_inserted": pool_result["inserted_count"],
            "pools_failed": pool_result["failed_count"],
            "assignment_ids": clean_result["assignment_ids"],
            "workdir": workdir,
        }
    
    @task()
    def update_stage_status(load_result: dict) -> dict:
        """
        Mark processed assignments as uploaded in stage database.
        
        Args:
            load_result: Output from load_to_master_db
        
        Returns:
            Dict with final summary
        """
        assignment_ids = load_result.get("assignment_ids", [])
        
        # Update stage even if loading was skipped (no new records to load)
        if not assignment_ids:
            logger.info("No assignments to mark as uploaded")
            logger.info("="*80)
            logger.info("STAGE TO MASTER ETL - COMPLETED")
            logger.info("="*80)
            return {
                "updated_assignments": 0,
                "addresses_inserted": 0,
                "pools_inserted": 0,
            }
        
        update_result = update_stage_upload_status(assignment_ids)
        
        # Final summary
        logger.info("="*80)
        logger.info("STAGE TO MASTER ETL - COMPLETED")
        logger.info("="*80)
        logger.info(f"Addresses inserted: {load_result.get('addresses_inserted', 0)}")
        logger.info(f"Addresses failed: {load_result.get('addresses_failed', 0)}")
        logger.info(f"Pools inserted: {load_result.get('pools_inserted', 0)}")
        logger.info(f"Pools failed: {load_result.get('pools_failed', 0)}")
        logger.info(f"Assignments marked as uploaded: {update_result['updated_count']}")
        logger.info("="*80)
        
        return {
            "updated_assignments": update_result["updated_count"],
            "addresses_inserted": load_result.get("addresses_inserted", 0),
            "addresses_failed": load_result.get("addresses_failed", 0),
            "pools_inserted": load_result.get("pools_inserted", 0),
            "pools_failed": load_result.get("pools_failed", 0),
        }
    
    # Define task dependencies
    extraction = extract_stage_assignments()
    details = fetch_details(extraction)
    merged = merge_data(extraction, details)
    cleaned = clean_and_filter_data(merged)
    loaded = load_to_master_db(cleaned)
    final = update_stage_status(loaded)


# Instantiate the DAG
stage_to_master_etl_dag()
