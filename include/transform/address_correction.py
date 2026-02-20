
import re
import time
import logging
import requests
import pandas as pd

from include.transform.address_cleaning import validate_parsed_addresses

logger = logging.getLogger(__name__)


def categorize_address_issues(
    df: pd.DataFrame,
    issue_inds: list[int]
) -> tuple[list[int], list[int]]:
    """
    Split address issues into critical (need Geocodio API) vs minor (skip API).
    
    Critical: Missing coordinates AND parsing issues -> needs geocoding
    Minor: Has valid coordinates but parsing issues -> keep original coords
    
    Returns:
        (critical_inds, minor_inds)
    """
    critical = []
    minor = []
    
    has_lat = "Latitude" in df.columns
    has_lon = "Longitude" in df.columns
    
    for idx in issue_inds:
        # Check if listing already has valid coordinates from MLS
        lat = df.at[idx, "Latitude"] if has_lat else None
        lon = df.at[idx, "Longitude"] if has_lon else None
        
        has_coords = (
            lat is not None 
            and lon is not None 
            and pd.notna(lat) 
            and pd.notna(lon)
            and lat != 0.0 
            and lon != 0.0
        )
        
        if has_coords:
            # Has coordinates - parsing issues are minor, keep original coords
            minor.append(idx)
        else:
            # Missing coordinates - needs geocoding to get location
            critical.append(idx)
    
    return critical, minor



def parse_address_components(components):
    """Extract structured address data from Geocodio API components."""
    parsed = {
        "address_number": None,
        "street_name": None,
        "city": None,
        "province_state": None,
        "country": None,
        "postal_code": None,
    }

    # Geocodio returns a dict, not a list
    if not components:
        return parsed

    # Map Geocodio fields to our structure
    if components.get("number"):
        parsed["address_number"] = components.get("number")
    if components.get("formatted_street") or components.get("street"):
        parsed["street_name"] = components.get("formatted_street") or components.get("street")
    if components.get("city"):
        parsed["city"] = components.get("city")
    if components.get("state"):
        parsed["province_state"] = components.get("state")
    if components.get("country"):
        parsed["country"] = components.get("country")
    if components.get("zip"):
        parsed["postal_code"] = components.get("zip")

    return parsed


def correct_address_components(formatted_address, parsed_components):
    """
    Fill missing components by parsing formatted address as a fallback.
    Keeps this conservative: only fills fields that are missing.
    """
    corrected = dict(parsed_components or {})

    if not formatted_address:
        return corrected

    # e.g. "123 Main St, Toronto, ON M5V 3A8, Canada"
    s = formatted_address.strip()

    # Remove trailing ", Canada" for easier parsing
    s = re.sub(r",\s*Canada\s*$", "", s, flags=re.IGNORECASE).strip()

    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        return corrected

    # First part: "123 Main St"
    first = parts[0]
    if (not corrected.get("address_number")) or (not corrected.get("street_name")):
        m = re.match(r"^(\d+)\s+(.+)$", first)
        if m:
            corrected["address_number"] = corrected.get("address_number") or m.group(1)
            corrected["street_name"] = corrected.get("street_name") or m.group(2)
        else:
            corrected["street_name"] = corrected.get("street_name") or first

    # Second part: city
    if not corrected.get("city") and len(parts) >= 2:
        corrected["city"] = parts[1]

    # Last part often: "ON M5V 3A8" (could be just "ON")
    last = parts[-1]
    prov_postal = re.match(r"^([A-Z]{2})\s*([A-Z]\d[A-Z]\s*\d[A-Z]\d)?$", last)
    if prov_postal:
        corrected["province_state"] = corrected.get("province_state") or prov_postal.group(1)
        if not corrected.get("postal_code") and prov_postal.group(2):
            corrected["postal_code"] = prov_postal.group(2)

    # Normalize postal: uppercase + remove spaces (to match your storage)
    if corrected.get("postal_code"):
        pc = re.sub(r"[^A-Z0-9]", "", str(corrected["postal_code"]).upper())
        # If it looks like Canadian postal, keep as A1A1A1
        if len(pc) == 6:
            corrected["postal_code"] = pc

    # Ensure country present
    if not corrected.get("country"):
        corrected["country"] = "Canada"

    return corrected


def geocode_correct_address(
    api_key: str,
    street_address: str | None,
    address_number: str | None,
    city: str | None,
    postal_code: str | None,
    province_state: str | None = None,
    country: str = "Canada",
    timeout_s: int = 20,
) -> dict:
    """
    Call Geocodio Geocode API and return:
      { formatted_address, lat, lon, components }
    where components include address_number, street_name, city, province_state, postal_code, country.
    """
    # Build structured parameters for Geocodio
    params = {"api_key": api_key}
    
    # Construct street parameter
    street_parts = []
    if address_number:
        street_parts.append(str(address_number))
    if street_address:
        street_parts.append(str(street_address))
    if street_parts:
        params["street"] = " ".join(street_parts)
    
    if city:
        params["city"] = str(city)
    if postal_code:
        params["postal_code"] = str(postal_code)
    if province_state:
        params["state"] = str(province_state)
    if country:
        params["country"] = str(country)

    url = "https://api.geocod.io/v1.7/geocode"

    resp = requests.get(url, params=params, timeout=timeout_s)
    resp.raise_for_status()
    payload = resp.json()

    if not payload.get("results"):
        query_str = ", ".join([f"{k}={v}" for k, v in params.items() if k != "api_key"])
        raise ValueError(f"No geocoding results for: {query_str}")

    top = payload["results"][0]
    formatted_address = top.get("formatted_address")
    loc = top.get("location") or {}
    lat = loc.get("lat")
    lon = loc.get("lng")

    parsed = parse_address_components(top.get("address_components") or {})
    corrected = correct_address_components(formatted_address, parsed)

    return {
        "formatted_address": formatted_address,
        "lat": lat,
        "lon": lon,
        "components": corrected,
    }


def correct_addresses(
    df: pd.DataFrame,
    issue_inds: list[int],
    api_key: str,
    max_fix: int = 250,
    sleep_s: float = 0.06,  # 0.06s = 1000 req/min (Geocodio rate limit)
) -> tuple[pd.DataFrame, list[int]]:
    """
    Applies Geocodio correction ONLY to the provided issue_inds (bad rows from prior validation).

    Returns:
      (df_corrected, still_bad_inds_after_attempts)
    """
    # Handle empty DataFrame from upstream failures
    if df.empty:
        logger.warning("Empty DataFrame passed to address correction")
        return pd.DataFrame(), []
    
    if not api_key:
        raise ValueError("api_key is required for Geocodio correction")

    df_out = df.copy()

    # If caller passes empty list, do nothing
    if not issue_inds:
        return df_out, []

    # Safety: keep only indexes that exist in df
    issue_inds = [i for i in issue_inds if i in df_out.index]

    # Hard cap to prevent runaway billing
    to_fix = issue_inds[:max_fix]

    cache: dict[str, dict] = {}
    has_lat = "Latitude" in df_out.columns
    has_lon = "Longitude" in df_out.columns

    # ONLY iterate bad rows
    for idx in to_fix:
        street = df_out.at[idx, "street_address"] if "street_address" in df_out.columns else None
        city = df_out.at[idx, "city"] if "city" in df_out.columns else None
        postal = df_out.at[idx, "postal_code"] if "postal_code" in df_out.columns else None
        addr_num = df_out.at[idx, "address_number"] if "address_number" in df_out.columns else None

        key = f"{addr_num}|{street}|{city}|{postal}"

        try:
            if key in cache:
                result = cache[key]
            else:
                result = geocode_correct_address(
                    api_key=api_key,
                    street_address=None if pd.isna(street) else str(street),
                    address_number=None if pd.isna(addr_num) else str(addr_num),
                    city=None if pd.isna(city) else str(city),
                    postal_code=None if pd.isna(postal) else str(postal),
                    province_state=None,
                    country="Canada",
                )
                cache[key] = result

            comps = result.get("components") or {}

            if comps.get("address_number"):
                df_out.at[idx, "address_number"] = str(comps["address_number"]).strip()
            if comps.get("street_name"):
                df_out.at[idx, "street_address"] = str(comps["street_name"]).strip()
            if comps.get("city"):
                df_out.at[idx, "city"] = str(comps["city"]).strip()
            if comps.get("postal_code"):
                df_out.at[idx, "postal_code"] = str(comps["postal_code"]).upper().replace(" ", "").strip()

            lat = result.get("lat")
            lon = result.get("lon")
            if lat is not None and lon is not None and has_lat and has_lon:
                df_out.at[idx, "Latitude"] = float(lat)
                df_out.at[idx, "Longitude"] = float(lon)

        except Exception:
            # leave row unchanged
            pass

        if sleep_s:
            time.sleep(sleep_s)

    # Only after attempts, re-validate the whole df to see what's still bad
    still_bad_inds = validate_parsed_addresses(df_out)
    return df_out, still_bad_inds



