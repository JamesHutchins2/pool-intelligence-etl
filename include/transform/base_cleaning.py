import re
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Matches integers/decimals in strings like "4 + 1", "4+1", "4", "4.0"
_NUM_RE = re.compile(r"[-+]?\d*\.?\d+")

def _parse_bedrooms_value(x) -> float | None:
    """
    Safely parse bedroom counts.

    Handles:
      - 4
      - "4"
      - "4 + 1" / "4+1"  -> 5.0
      - "4+ 1"           -> 5.0
    Returns None if it can't extract any numbers.
    """
    if pd.isna(x):
        return None

    s = str(x).strip()
    if not s:
        return None

    # common case: "4 + 1"
    if "+" in s:
        parts = [p.strip() for p in s.split("+") if p.strip()]
        nums = []
        for p in parts:
            m = _NUM_RE.search(p)
            if m:
                nums.append(float(m.group(0)))
        return float(sum(nums)) if nums else None

    # fallback: first number in string
    m = _NUM_RE.search(s)
    return float(m.group(0)) if m else None


def _parse_float_simple(x) -> float | None:
    """
    Safe float parse for numeric-ish columns like Stories/Price.
    Pulls the first numeric token if present.
    """
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    m = _NUM_RE.search(s)
    return float(m.group(0)) if m else None


def clean_extracted_listings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Handle empty DataFrame from failed extractions
    if df.empty or "MLS" not in df.columns:
        logger.warning(f"Empty DataFrame or missing MLS column. Columns: {df.columns.tolist()}")
        logger.warning("This indicates:")
        logger.warning("  - All search locations failed to return data")
        logger.warning("  - OR pyRealtor API returned data with different column names")
        return pd.DataFrame()

    # first we drop duplicates based on MLS
    df = df.drop_duplicates(subset=["MLS"], keep="first")

    # remove any with a na for mls
    df = df[pd.notna(df["MLS"])].reset_index(drop=True)

    # strip down the mls_id values
    df["MLS"] = df["MLS"].astype(str).str.strip()

    # clean the numeric columns (NO eval)
    df["Bedrooms"] = df["Bedrooms"].apply(_parse_bedrooms_value)
    df["Stories"] = df["Stories"].apply(_parse_float_simple)
    df["Price"] = df["Price"].apply(_parse_float_simple)

    df = standardize_Size(df)
    return df


def standardize_Size(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ensure string ops don't crash on NaN
    size_s = df["Size"].fillna("").astype(str)

    # create a new column called Size_letters with only the letter part of the Size column
    df["Size_letters"] = size_s.str.extract(r"([A-Za-z]+)", expand=False)

    # now get all values before the first letter as the numeric part
    df["Size_numeric"] = size_s.str.extract(r"([\d,.]+)", expand=False)

    # need to convert all m to sqft, set all with ac to zero
    def convert_size(row):
        size_num = row["Size_numeric"]
        size_unit = row["Size_letters"]

        if pd.isna(size_num) or not str(size_num).strip():
            return None

        try:
            size_value = float(str(size_num).replace(",", ""))
        except Exception:
            return None

        if pd.isna(size_unit) or not str(size_unit).strip():
            return size_value  # assume sqft if no unit

        u = str(size_unit).lower()

        if u in ["sqft", "ft2", "sf"]:
            return size_value
        elif u in ["sqm", "m2", "m"]:
            return size_value * 10.7639  # convert sqm to sqft
        elif u in ["ac", "acre", "acres"]:
            return 0.0  # set acres to zero as per instruction
        else:
            return None  # unknown unit

    df["Size_sqft"] = df.apply(convert_size, axis=1)
    df = df.drop(columns=["Size", "Size_letters", "Size_numeric"])
    return df
