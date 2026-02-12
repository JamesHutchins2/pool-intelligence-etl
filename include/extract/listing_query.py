import os
import time
import glob
import tempfile
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import pandas as pd
import pyRealtor


def run_query(state: str, search_area: str) -> pd.DataFrame:
    try:
        house_obj = pyRealtor.HousesFacade()
        results = house_obj.search_save_houses(state=state, search_area=search_area)
        return results if isinstance(results, pd.DataFrame) else pd.DataFrame()
    except ValueError as e:
        print(f"ValueError for {search_area}, {state}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error for {search_area}, {state}: {e}")
        return pd.DataFrame()


def query_listing_data_to_parquet(
    locations: List[Dict[str, str]],
    sleep_s: float = 2.0,
    max_locations: Optional[int] = None,
    base_output_dir: Optional[str] = None,
    run_subdir: Optional[str] = None,
) -> Tuple[str, List[Dict[str, str]]]:
    """
    Writes output into:
      workdir = (base_output_dir or system temp) / (run_subdir or unique folder)

    Returns:
      (parquet_path, failed_locations)
    """
    run_ts = datetime.now(timezone.utc).isoformat()

    # Choose base directory (designated temp dir)
    if base_output_dir:
        os.makedirs(base_output_dir, exist_ok=True)
        base_dir = base_output_dir
    else:
        base_dir = tempfile.gettempdir()  # e.g., /tmp

    # Create a unique per-run folder inside the base directory
    # (Never write multiple runs into the same folder)
    if run_subdir is None:
        safe = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_subdir = f"weekly_listings_{safe}_{os.getpid()}"
    workdir = os.path.join(base_dir, run_subdir)
    os.makedirs(workdir, exist_ok=True)

    listing_dfs: List[pd.DataFrame] = []
    failed_locations: List[Dict[str, str]] = []

    locs = locations[:max_locations] if max_locations else locations

    for loc in locs:
        country_code = loc.get("country_code")
        province_state = loc.get("province_state")
        search_area = loc.get("search_area")

        print(f"Querying listings for {country_code}, {province_state}, {search_area}")

        df = run_query(province_state, search_area)

        if not df.empty:
            # Add location metadata
            df["country_code"] = country_code
            df["province_state"] = province_state
            df["search_area"] = search_area
            
            # Create composite search_location identifier
            # Format: "CA-ON-Fort Erie"
            df["search_location"] = f"{country_code}-{province_state}-{search_area}"
            
            df["run_ts"] = run_ts
            listing_dfs.append(df)
        else:
            failed_locations.append(loc)

        time.sleep(sleep_s)

    # Cleanup ONLY within workdir
    xlsx_files = glob.glob(os.path.join(workdir, "*.xlsx"))
    for f in xlsx_files:
        os.remove(f)
    if xlsx_files:
        print(f"Removed {len(xlsx_files)} .xlsx files from {workdir}")

    combined_df = pd.concat(listing_dfs, ignore_index=True) if listing_dfs else pd.DataFrame()

    out_path = os.path.join(workdir, "combined_listings.parquet")
    combined_df.to_parquet(out_path, index=False)

    return out_path, failed_locations
