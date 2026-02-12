from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd
from include.extract import listing_query, search_locations
from include.transform.base_cleaning import clean_extracted_listings
from include.transform.address_cleaning import clean_addresses
from include.transform.address_correction import correct_addresses
from include.transform.pool_inference import add_pool_inference_columns
from include.transform.listing_filters import remove_non_home_values

DEFAULT_ARGS = {
    "owner": "james",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="weekly_listings_etl",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * 1",  # Mondays 6:00 AM
    catchup=False,
    max_active_runs=1,
    tags=["etl", "listings"],
)
def weekly_listings_etl():

    @task
    def get_search_areas() -> List[Dict[str, Any]]:
        return search_locations.get_all_search_locations()

    @task
    def extract_listings(search_areas: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Runs the external listing query step and writes results to parquet.

        Returns:
          {
            "parquet_path": "...",
            "failed_locations": [...],
            "workdir": "..."
          }
        """
        logger = logging.getLogger(__name__)
        ctx = get_current_context()
        run_id = ctx["run_id"]
        
        logger.info("="*60)
        logger.info("EXTRACT LISTINGS - Starting")
        logger.info("="*60)
        logger.info(f"Run ID: {run_id}")
        logger.info(f"Total search locations: {len(search_areas)}")
        
        base_output_dir = "/opt/airflow/data/tmp"
        workdir = f"{base_output_dir}/weekly_listings/{run_id}"
        
        parquet_path, failed_locations = listing_query.query_listing_data_to_parquet(
            locations=search_areas,
            sleep_s=2.0,
            max_locations=None,
            base_output_dir=base_output_dir,
            run_subdir=f"weekly_listings/{run_id}",
        )
        
        # Read parquet to get row count
        df = pd.read_parquet(parquet_path)
        
        logger.info("="*60)
        logger.info("EXTRACT RESULTS")
        logger.info("="*60)
        logger.info(f"Total listings extracted: {len(df)}")
        logger.info(f"Successful locations: {len(search_areas) - len(failed_locations)}")
        logger.info(f"Failed locations: {len(failed_locations)}")
        logger.info(f"Output parquet: {parquet_path}")
        
        if failed_locations:
            logger.warning("::group::Failed Locations Details")
            for loc in failed_locations:
                logger.warning(f"  - {loc.get('country_code')}, {loc.get('province_state')}, {loc.get('search_area')}")
            logger.warning("::endgroup::")
        
        if df.empty:
            logger.error("⚠️  WARNING: No listings extracted! All locations may have failed.")
            logger.error(f"  Columns in DataFrame: {list(df.columns)}")
        
        logger.info("="*60)

        return {
            "parquet_path": parquet_path,
            "failed_locations": failed_locations,
            "workdir": workdir,
        }
    @task
    def clean_listings(extract_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Base cleaning step:
        - dedupe by MLS
        - drop NA MLS
        - strip MLS
        - parse numeric columns
        - standardize size -> Size_sqft

        Writes parquet and returns metadata only.
        """
        logger = logging.getLogger(__name__)
        in_path = extract_result["parquet_path"]
        workdir = extract_result["workdir"]

        df = pd.read_parquet(in_path)
        before_rows = len(df)
        
        logger.info("="*60)
        logger.info("BASE CLEANING - Starting")
        logger.info("="*60)
        logger.info(f"Input rows: {before_rows}")
        logger.info(f"Input file: {in_path}")

        df = clean_extracted_listings(df)

        after_rows = len(df)
        removed = before_rows - after_rows
        
        logger.info("="*60)
        logger.info("BASE CLEANING RESULTS")
        logger.info("="*60)
        logger.info(f"Rows after cleaning: {after_rows}")
        logger.info(f"Rows removed: {removed} ({(removed/before_rows*100) if before_rows > 0 else 0:.1f}%)")
        
        if df.empty:
            logger.error("⚠️  WARNING: No listings after cleaning step!")
            logger.error("  This means either:")
            logger.error("  1. All locations failed extraction (check extract_listings logs)")
            logger.error("  2. All records had missing/invalid MLS numbers")
            logger.error("  3. DataFrame has no columns (check extract step)")

        out_path = f"{workdir}/listings_cleaned.parquet"
        df.to_parquet(out_path, index=False)
        logger.info(f"Output file: {out_path}")
        logger.info("="*60)

        return {
            "parquet_path": out_path,
            "rows_before": before_rows,
            "rows_after": after_rows,
            "workdir": workdir,
            "failed_locations": extract_result.get("failed_locations", []),
        }

    @task
    def filter_listings(cleaned_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove non-home listings early in transform phase.
        """
        logger = logging.getLogger(__name__)
        in_path = cleaned_result["parquet_path"]
        workdir = cleaned_result["workdir"]

        df = pd.read_parquet(in_path)
        before_rows = len(df)
        
        logger.info("="*60)
        logger.info("FILTER LISTINGS - Starting")
        logger.info("="*60)
        logger.info(f"Input rows: {before_rows}")

        df = remove_non_home_values(df)

        after_rows = len(df)
        filtered = before_rows - after_rows
        
        logger.info("="*60)
        logger.info("FILTER RESULTS")
        logger.info("="*60)
        logger.info(f"Rows after filtering: {after_rows}")
        logger.info(f"Non-home listings removed: {filtered} ({(filtered/before_rows*100) if before_rows > 0 else 0:.1f}%)")
        
        if df.empty:
            logger.warning("⚠️  No listings after filtering - all were non-home types")

        out_path = f"{workdir}/listings_filtered.parquet"
        df.to_parquet(out_path, index=False)
        logger.info(f"Output file: {out_path}")
        logger.info("="*60)

        return {
            "parquet_path": out_path,
            "rows_before": before_rows,
            "rows_after": after_rows,
            "workdir": workdir,
            "failed_locations": cleaned_result.get("failed_locations", []),
        }

    @task
    def add_pool_inference(filtered_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply pool inference to listings (Transform).
        Reads parquet -> transforms -> writes parquet.
        """
        logger = logging.getLogger(__name__)
        in_path = filtered_result["parquet_path"]
        workdir = filtered_result.get("workdir")
        
        if not workdir:
            import os
            workdir = os.path.dirname(in_path)

        df = pd.read_parquet(in_path)
        before_rows = len(df)
        
        logger.info("="*60)
        logger.info("POOL INFERENCE - Starting")
        logger.info("="*60)
        logger.info(f"Input rows: {before_rows}")

        if df.empty:
            logger.warning("⚠️  Empty DataFrame in pool inference. Skipping.")
            out_path = f"{workdir}/listings_pool_inferred.parquet"
            df.to_parquet(out_path, index=False)
            return {
                "parquet_path": out_path,
                "rows_before": 0,
                "rows_after": 0,
                "workdir": workdir,
            }
        
        if "Description" not in df.columns:
            raise ValueError("Pool inference requires column 'Description' to exist")

        df = add_pool_inference_columns(df)
        after_rows = len(df)
        
        # Calculate pool statistics
        pool_count = df['pool_flag'].sum() if 'pool_flag' in df.columns else 0
        pool_private = len(df[df.get('pool_type', '') == 'private']) if 'pool_type' in df.columns else 0
        pool_community = len(df[df.get('pool_type', '') == 'community']) if 'pool_type' in df.columns else 0
        
        logger.info("="*60)
        logger.info("POOL INFERENCE RESULTS")
        logger.info("="*60)
        logger.info(f"Total listings: {after_rows}")
        logger.info(f"Listings with pools: {pool_count} ({(pool_count/after_rows*100) if after_rows > 0 else 0:.1f}%)")
        logger.info(f"  - Private pools: {pool_private}")
        logger.info(f"  - Community pools: {pool_community}")

        out_path = f"{workdir}/listings_pool_inferred.parquet"
        df.to_parquet(out_path, index=False)
        logger.info(f"Output file: {out_path}")
        logger.info("="*60)

        return {
            "parquet_path": out_path,
            "rows_before": before_rows,
            "rows_after": after_rows,
            "workdir": workdir,
        }
    @task
    def clean_and_correct_addresses(pool_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Address parsing/validation + Google correction in ONE step to avoid index instability.

        Reads parquet -> clean_addresses() -> correct_addresses(bad rows only) -> write parquet.

        Returns XCom-safe metadata only.
        """
        logger = logging.getLogger(__name__)
        in_path = pool_result["parquet_path"]
        workdir = pool_result["workdir"]

        df = pd.read_parquet(in_path)
        before_rows = len(df)
        
        logger.info("="*60)
        logger.info("ADDRESS CLEANING & CORRECTION - Starting")
        logger.info("="*60)
        logger.info(f"Input rows: {before_rows}")

        if df.empty:
            logger.warning("Empty DataFrame in address correction. Skipping.")
            out_path = f"{workdir}/listings_addr_fixed.parquet"
            df.to_parquet(out_path, index=False)
            return {
                "parquet_path": out_path,
                "rows_before": 0,
                "rows_after": 0,
                "initial_bad_rows": 0,
                "still_bad_rows": 0,
                "workdir": workdir,
            }
        
        if "Address" not in df.columns:
            raise ValueError("Address step requires column 'Address' to exist")

        # 1) Parse + validate
        logger.info("::group::Address Parsing & Validation")
        df_parsed, issue_inds = clean_addresses(df)
        initial_bad = len(issue_inds)
        logger.info(f"Total addresses: {before_rows}")
        logger.info(f"Bad addresses found: {initial_bad} ({(initial_bad/before_rows*100) if before_rows > 0 else 0:.1f}%)")
        logger.info("::endgroup::")

        # 2) Correct ONLY the bad rows (if API key is present)
        import os
        api_key = os.getenv("GOOGLE_GEOCODE_API_KEY")

        still_bad = issue_inds
        df_fixed = df_parsed

        if api_key and issue_inds:
            logger.info("::group::Google Geocoding API Correction")
            logger.info(f"Attempting to fix {len(issue_inds)} addresses (max 250)...")
            df_fixed, still_bad = correct_addresses(
                df=df_parsed,
                issue_inds=issue_inds,
                api_key=api_key,
                max_fix=250,
                sleep_s=0.05,
            )
            fixed_count = initial_bad - len(still_bad)
            logger.info(f"Successfully corrected: {fixed_count}")
            logger.info(f"Still bad after correction: {len(still_bad)}")
            logger.info("::endgroup::")
        else:
            if not api_key:
                logger.warning("⚠️  GOOGLE_GEOCODE_API_KEY not set - skipping address correction")
            
        # Remove rows at indexes where address is still bad after parsing
        if still_bad:
            still_bad = [i for i in still_bad if i in df_fixed.index]
            if still_bad:
                logger.info(f"Dropping {len(still_bad)} rows with unfixable addresses")
                df_fixed = df_fixed.drop(index=still_bad).reset_index(drop=True)
        
        logger.info("="*60)
        logger.info("ADDRESS CORRECTION RESULTS")
        logger.info("="*60)
        logger.info(f"Final row count: {len(df_fixed)}")
        logger.info(f"Rows dropped: {before_rows - len(df_fixed)}")
        
        # Fix data types for parquet compatibility (Latitude/Longitude may be object type)
        if 'Latitude' in df_fixed.columns:
            df_fixed['Latitude'] = pd.to_numeric(df_fixed['Latitude'], errors='coerce')
        if 'Longitude' in df_fixed.columns:
            df_fixed['Longitude'] = pd.to_numeric(df_fixed['Longitude'], errors='coerce')

        out_path = f"{workdir}/listings_addr_fixed.parquet"
        df_fixed.to_parquet(out_path, index=False)
        logger.info(f"Output file: {out_path}")
        logger.info("="*60)

        return {
            "parquet_path": out_path,
            "rows_before": before_rows,
            "rows_after": len(df_fixed),
            "initial_bad_rows": initial_bad,
            "still_bad_rows": len(still_bad) if still_bad else 0,
            "workdir": workdir,
        }


    @task
    def load_to_database(addr_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load transformed data to database and detect removals/relistings.
        """
        logger = logging.getLogger(__name__)
        from datetime import datetime, timezone
        from include.load.load_listings import load_listings_to_db
        
        in_path = addr_result["parquet_path"]
        
        logger.info("="*60)
        logger.info("DATABASE LOAD - Starting")
        logger.info("="*60)
        
        # Check if there's data to load
        df_check = pd.read_parquet(in_path)
        if df_check.empty:
            logger.warning("\u26a0\ufe0f  No listings to load to database. All search areas may have failed.")
            return {
                "stats": {
                    "total_input_rows": 0,
                    "staging_rows": 0,
                    "new_listings": 0,
                    "removals": 0,
                    "relistings": 0,
                    "skipped_rows": 0,
                },
                "parquet_path": in_path,
                "workdir": addr_result["workdir"]
            }
        
        run_ts = datetime.now(timezone.utc)
        logger.info(f"Loading {len(df_check)} listings to database...")
        
        stats = load_listings_to_db(in_path, run_ts)
        
        logger.info("="*60)
        logger.info("DATABASE LOAD RESULTS")
        logger.info("="*60)
        logger.info(f"Total input rows:     {stats['total_input_rows']}")
        logger.info(f"Staged rows:          {stats['staging_rows']}")
        logger.info(f"New listings:         {stats['new_listings']}")
        logger.info(f"Removed listings:     {stats['removals']}")
        logger.info(f"Re-listings:          {stats['relistings']}")
        logger.info(f"Skipped rows:         {stats['skipped_rows']}")
        logger.info("="*60)
        
        return {
            "stats": stats,
            "parquet_path": in_path,
            "workdir": addr_result["workdir"]
        }

    @task
    def cleanup_files(load_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete temporary parquet files after successful database load.
        """
        logger = logging.getLogger(__name__)
        import os
        import glob
        
        workdir = load_result["workdir"]
        parquet_files = glob.glob(f"{workdir}/*.parquet")
        
        logger.info("="*60)
        logger.info("CLEANUP TEMPORARY FILES - Starting")
        logger.info("="*60)
        logger.info(f"Workdir: {workdir}")
        logger.info(f"Parquet files to delete: {len(parquet_files)}")
        
        deleted = 0
        failed = 0
        for f in parquet_files:
            try:
                os.remove(f)
                deleted += 1
                logger.debug(f"Deleted: {f}")
            except Exception as e:
                failed += 1
                logger.warning(f"Could not delete {f}: {e}")
        
        logger.info("="*60)
        logger.info("CLEANUP RESULTS")
        logger.info("="*60)
        logger.info(f"Files deleted: {deleted}")
        if failed > 0:
            logger.warning(f"Files failed to delete: {failed}")
        logger.info("="*60)
        
        return {
            "stats": load_result["stats"],
            "cleanup_count": len(parquet_files)
        }

    @task
    def send_email_reports(cleanup_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send email reports to license holders with pool listings (new/removed).
        """
        logger = logging.getLogger(__name__)
        from include.inform.send_reports import send_weekly_reports
        from include.db.connections import get_master_db_connection, get_listing_db_connection
        
        logger.info("="*60)
        logger.info("EMAIL REPORTS - POOL LISTINGS")
        logger.info("="*60)
        
        # Get database connections
        master_conn = get_master_db_connection()
        listing_conn = get_listing_db_connection()
        
        try:
            # Send reports to all active licenses
            email_stats = send_weekly_reports(master_conn, listing_conn)
            
            logger.info("="*60)
            logger.info("EMAIL REPORTS RESULTS")
            logger.info("="*60)
            logger.info(f"Licenses processed:   {email_stats['total_licenses']}")
            logger.info(f"Emails sent:          {email_stats['emails_sent']}")
            logger.info(f"Emails failed:        {email_stats['emails_failed']}")
            logger.info(f"New pool listings:    {email_stats['total_new_listings']}")
            logger.info(f"Removed pool listings:{email_stats['total_removed_listings']}")
            logger.info("="*60)
            
            return {
                "email_stats": email_stats,
                "load_stats": cleanup_result["stats"]
            }
            
        finally:
            master_conn.close()
            listing_conn.close()

    # wiring
    search_areas = get_search_areas()
    extract_result = extract_listings(search_areas)
    cleaned_result = clean_listings(extract_result)
    filtered_result = filter_listings(cleaned_result)
    pool_inferred_result = add_pool_inference(filtered_result)
    addr_fixed_result = clean_and_correct_addresses(pool_inferred_result)
    load_result = load_to_database(addr_fixed_result)
    cleanup_result = cleanup_files(load_result)
    email_result = send_email_reports(cleanup_result)
    
    # Trigger client data update DAG after successful completion
    trigger_client_update = TriggerDagRunOperator(
        task_id='trigger_client_data_update',
        trigger_dag_id='client_data_update_etl',
        wait_for_completion=False,
    )
    
    email_result >> trigger_client_update

dag = weekly_listings_etl()
