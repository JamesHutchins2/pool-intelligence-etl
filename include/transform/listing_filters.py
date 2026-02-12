import pandas as pd
import logging
import re

logger = logging.getLogger(__name__)

def remove_non_home_values(df: pd.DataFrame) -> pd.DataFrame:
    # Handle empty DataFrame from upstream failures
    if df.empty or "House Category" not in df.columns:
        logger.warning(f"Empty DataFrame or missing House Category column. Columns: {df.columns.tolist()}")
        return pd.DataFrame()

    non_home_values = [
        "Recreation",
        "Apartment",
        "Other",
        "Unknown",
        "Mobile Home",
        "Manufactured Home/Mobile",
        "Parking",
        "Residential Commercial Mix",
    ]

    # Build safe regex pattern
    pattern = "|".join(map(re.escape, non_home_values))

    mask = ~df["House Category"].astype(str).str.contains(
        pattern, case=False, na=False
    )

    df = df.loc[mask].reset_index(drop=True)
    return df
