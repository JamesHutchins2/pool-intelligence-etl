
import re
import pandas as pd

# -----------------------------
# Regex (Canada postal code)
# -----------------------------
CA_POSTAL_RE = re.compile(
    r"\b[ABCEGHJ-NPRSTVXY]\d[ABCEGHJ-NPRSTV-Z][ -]?\d[ABCEGHJ-NPRSTV-Z]\d\b",
    re.IGNORECASE,
)

# -----------------------------
# Parsing helpers
# -----------------------------

def extract_postal_code(address: str) -> str | None:
    """Extract Canadian postal code as 'A1A1A1' (no spaces)."""
    if not address:
        return None
    m = CA_POSTAL_RE.search(address)
    if not m:
        return None
    return m.group(0).upper().replace(" ", "")

def get_street_address(s: str) -> str | None:
    """Return everything before the first '|'."""
    if not s or "|" not in s:
        return None
    left = s.split("|", 1)[0].strip()
    return left or None

def get_city(s: str) -> str | None:
    """
    Return the text after the first '|' up to the first comma,
    with any parenthetical removed.
    """
    if not s or "|" not in s:
        return None

    right = s.split("|", 1)[1]

    # Before comma only
    if "," in right:
        right = right.split(",", 1)[0]

    right = right.strip()

    # Remove parenthetical e.g. "(Palgrave)"
    right = re.sub(r"\(.*?\)", "", right).strip()

    return right or None

def get_address_number(street_part: str | None) -> str | None:
    """Return leading digits from street part (e.g., '12506 HEART...' -> '12506')."""
    if not street_part:
        return None
    match = re.match(r"^\s*(\d+)", street_part)
    return match.group(1) if match else None

def get_province_from_address(s: str) -> str | None:
    
    #split after the |
    if not s or "|" not in s:
        return None

    right = s.split("|", 1)[1]
    if "," in right:
        #get contents after ,
        right = right.split(",", 1)[1]

    #remove the postal code if present
    postal_code = extract_postal_code(right)
    if postal_code:
        right = right.replace(postal_code, "")

    province_state = right.strip()
    valid_provinces = [
        "Alberta",
        "British Columbia",
        "Manitoba",
        "New Brunswick",
        "Newfoundland and Labrador",
        "Nova Scotia",
        "Ontario",
        "Prince Edward Island",
        "Quebec",
        "Saskatchewan",
        "Northwest Territories",
        "Nunavut",
        "Yukon"
    ]

    if province_state in valid_provinces:
        return province_state
    else:
        #see if the sub-string matches a valid province
        for province in valid_provinces:
            if province in province_state:
                return province
            


    return right.strip()

def parse_address(address: str) -> dict:
    """Parse minimal components from your 'street|city, province postal' style strings."""
    street_address = get_street_address(address)
    return {
        "full_address": address,
        "address_number": get_address_number(street_address),
        "street_address": street_address,
        "city": get_city(address),
        "postal_code": extract_postal_code(address),
        "province_state": get_province_from_address(address),
    }

# -----------------------------
# Validation
# -----------------------------

def validate_parsed_addresses(
    df: pd.DataFrame,
    require: tuple[str, ...] = ("street_address", "city", "postal_code", "address_number"),
) -> list[int]:
    """
    Validate parsed address fields and return list of df indexes where validation fails.

    Rules (relaxed for coordinate preservation):
      - Required columns must exist
      - Required values must not be NaN/None/empty after strip
      - address_number must start with digits (allows 123A, 123-B, etc.)
      - postal_code must match Canadian postal format (accepts spacing/case variations)
      - street_address must contain at least one letter (avoid number-only)
    """
    missing_cols = [c for c in require if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns for validation: {missing_cols}")

    def _is_nonempty_str(s: pd.Series) -> pd.Series:
        x = s.fillna("").astype(str).str.strip()
        # treat literal "nan"/"none" as empty too
        return x.ne("") & x.ne("nan") & x.ne("none")

    fail = pd.Series(False, index=df.index)

    # Required fields non-empty
    for col in require:
        fail |= ~_is_nonempty_str(df[col])

    # address_number: allow alphanumeric (123, 123A, 123-B all valid)
    addr = df["address_number"].fillna("").astype(str).str.strip()
    addr_ok = addr.str.match(r"^\d+[A-Z]?$", na=False) | addr.str.match(r"^\d+-?[A-Z]?$", na=False)
    fail |= ~addr_ok

    # postal_code: normalize then match (accept M5V3A8, M5V 3A8, m5v-3a8)
    pc = df["postal_code"].fillna("").astype(str).str.upper().str.replace(r"[\s-]", "", regex=True).str.strip()
    pc_ok = pc.str.match(r"^[A-Z]\d[A-Z]\d[A-Z]\d$", na=False)
    fail |= ~pc_ok

    # street_address should contain at least one letter
    street = df["street_address"].fillna("").astype(str).str.strip()
    street_ok = street.str.contains(r"[A-Za-z]", regex=True, na=False)
    fail |= ~street_ok

    return df.index[fail].tolist()

# -----------------------------
# One-call pipeline
# -----------------------------

def clean_addresses(df: pd.DataFrame) -> tuple[pd.DataFrame, list[int]]:
    """
    Adds parsed address columns + returns list of failing row indexes.

    Returns:
      (df_out, issue_inds)
    """
    if "Address" not in df.columns:
        raise ValueError("clean_addresses expects a column named 'Address'")

    df_out = df.copy()

    parsed = df_out["Address"].apply(parse_address)
    df_out["address_number"] = parsed.apply(lambda x: x["address_number"])
    df_out["street_address"] = parsed.apply(lambda x: x["street_address"])
    df_out["city"] = parsed.apply(lambda x: x["city"])
    df_out["postal_code"] = parsed.apply(lambda x: x["postal_code"])
    df_out["province_state"] = parsed.apply(lambda x: x["province_state"])

    issue_inds = validate_parsed_addresses(df_out)
    return df_out, issue_inds
