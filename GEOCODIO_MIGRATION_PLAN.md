# Google Geocoding API to Geocodio Migration Plan

## Summary

This document outlines the migration from Google Geocoding API to Geocodio API across all Airflow DAGs and supporting modules.

**Date Created:** February 14, 2026  
**API Key Environment Variable:** `GEOCODIO_API_KEY` (already configured in .env)

---

## Google API Usage Points Identified

### 1. **Address Correction Module** (`include/transform/address_correction.py`)
- **Function:** `geocode_correct_address()`
- **Lines:** 100-155
- **Purpose:** Forward geocoding to correct bad addresses
- **Usage:** Called from `correct_addresses()` function
- **DAG:** `weekly_listings_etl`
- **Google API Endpoint:** `https://maps.googleapis.com/maps/api/geocode/json`
- **Request Type:** Forward Geocoding (address → coordinates + structured components)
- **Current Behavior:**
  - Takes: street_address, address_number, city, postal_code, province_state, country
  - Returns: formatted_address, lat, lon, components (address_number, street_name, city, province_state, postal_code, country)

### 2. **Address Geocoding Module** (`include/transform/address_geocoding.py`)
- **Function:** `batch_geocode_pools()`
- **Lines:** 76-130
- **Purpose:** Reverse geocoding of pool coordinates from OpenStreetMap
- **Usage:** Called from `geocode_pools()` task in `osm_collection` DAG
- **Google API:** Uses `geopy.geocoders.GoogleV3`
- **Request Type:** Reverse Geocoding (coordinates → address)
- **Current Behavior:**
  - Takes: latitude, longitude
  - Returns: address_components (street_number, route, locality, administrative_area_level_1, country, postal_code)

---

## Geocodio API Overview

### Key Differences from Google:
1. **Base URL:** `https://api.geocod.io/v1.9/`
2. **Authentication:** API key passed as `api_key` query parameter or `Authorization: Bearer` header
3. **Country Support:** US and Canada only (our use case)
4. **Rate Limiting:** Different structure, but supports our volume
5. **Response Format:** Different JSON structure but similar data

### Geocodio Endpoints We'll Use:
1. **Forward Geocoding:** `GET /v1.9/geocode`
2. **Reverse Geocoding:** `GET /v1.9/reverse`
3. **Batch Forward:** `POST /v1.9/geocode`
4. **Batch Reverse:** `POST /v1.9/reverse`

### Geocodio Address Component Mapping:
| Google Component | Geocodio Component | Notes |
|-----------------|-------------------|-------|
| `street_number` | `number` | House number |
| `route` | `formatted_street` | Full street name with directionals |
| `locality` | `city` | City name |
| `administrative_area_level_1` | `state` | State/Province (full name, not abbreviation) |
| `country` | `country` | `US` or `CA` |
| `postal_code` | `zip` (US) or `zip` (CA FSA only) | Canada returns 3-char FSA only |
| `formatted_address` | `formatted_address` | Full formatted address |
| `lat` | `location.lat` | Latitude |
| `lng` | `location.lng` | Longitude |

---

## Migration Plan by Module

### Module 1: Address Correction (`include/transform/address_correction.py`)

#### Changes Required:

1. **Replace `geocode_correct_address()` function:**
   - Remove Google Maps API requests library usage
   - Implement Geocodio forward geocoding API call
   - Update response parsing to match Geocodio format

2. **New Geocodio Request Format:**
```python
# Single address
url = "https://api.geocod.io/v1.9/geocode"
params = {
    "street": "{address_number} {street_address}",
    "city": "{city}",
    "postal_code": "{postal_code}",
    "state": "{province_state}",  # optional
    "country": "Canada",  # or omit for US
    "api_key": api_key
}
```

3. **Response Mapping:**
```python
# Google response structure:
{
  "results": [{
    "formatted_address": "...",
    "geometry": {"location": {"lat": x, "lng": y}},
    "address_components": [...]
  }]
}

# Geocodio response structure:
{
  "results": [{
    "formatted_address": "...",
    "location": {"lat": x, "lng": y},
    "address_components": {
      "number": "...",
      "street": "...",
      "city": "...",
      "state": "...",
      "zip": "...",
      "country": "..."
    }
  }]
}
```

4. **Component Extraction Updates:**
   - `address_number`: Map from `results[0].address_components.number`
   - `street_name`: Map from `results[0].address_components.formatted_street` or `street`
   - `city`: Map from `results[0].address_components.city`
   - `province_state`: Map from `results[0].address_components.state`
   - `postal_code`: Map from `results[0].address_components.zip`
   - `country`: Map from `results[0].address_components.country`

5. **Error Handling:**
   - Google: Checks `payload.get("results")`
   - Geocodio: Check `response.json().get("results")` - same pattern
   - Geocodio returns empty array if no results (not an error response)

#### Testing Considerations:
- Test with valid Canadian addresses
- Test with addresses missing components
- Test with invalid addresses (should gracefully fail)
- Verify postal code format (Geocodio returns 3-char FSA for Canada)
- Confirm rate limiting behavior

---

### Module 2: Address Geocoding (`include/transform/address_geocoding.py`)

#### Changes Required:

1. **Remove geopy dependency:**
   - Remove `from geopy.geocoders import GoogleV3`
   - Remove `from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded`
   - Add `import requests` for direct API calls

2. **Replace `get_google_maps_api_key()` function:**
```python
def get_geocodio_api_key() -> str:
    """Get Geocodio API key from environment."""
    api_key = os.getenv("GEOCODIO_API_KEY")
    if not api_key:
        raise ValueError("GEOCODIO_API_KEY environment variable not set")
    return api_key
```

3. **Replace `geocode_pool_location()` function:**
   - Change from geopy's `.reverse()` to direct HTTP request to Geocodio
   - Geocodio reverse endpoint: `GET https://api.geocod.io/v1.9/reverse?q={lat},{lon}&api_key={key}`

4. **New Reverse Geocoding Implementation:**
```python
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
        url = "https://api.geocod.io/v1.9/reverse"
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
        
        # Map to expected format
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
```

5. **Update `batch_geocode_pools()` function:**
   - Replace geolocator initialization
   - Update call to `geocode_pool_location()` to pass api_key instead of geolocator
   - Keep rate limiting logic (delay parameter)

6. **Batch Optimization (Optional Enhancement):**
   - Geocodio supports batch reverse geocoding via POST
   - Can process up to 10,000 coordinates at once
   - This could significantly speed up the OSM collection pipeline
   - Implementation:
```python
# Batch reverse geocode (example)
url = "https://api.geocod.io/v1.9/reverse"
coords = [f"{row['lat']},{row['lon']}" for _, row in pools_df.iterrows()]
response = requests.post(
    url,
    params={"api_key": api_key},
    json=coords,
    timeout=120
)
```

#### Testing Considerations:
- Test with pool coordinates from OpenStreetMap
- Verify address component extraction
- Test rate limiting and delays
- Validate that addresses are properly formatted for database insertion
- Test with coordinates that have no nearby addresses

---

## Implementation Steps

### Phase 1: Preparation
1. ✅ Review current usage (COMPLETE)
2. ✅ Review Geocodio API documentation (COMPLETE)
3. ✅ Create migration plan (COMPLETE)
4. ⬜ Create backup of current code
5. ⬜ Set up feature branch for migration

### Phase 2: Create New Geocodio Utility Module
1. Create `include/transform/geocodio_client.py` with:
   - Forward geocoding function
   - Reverse geocoding function
   - Batch geocoding functions (optional)
   - Error handling and rate limiting
   - Response parsing utilities

### Phase 3: Update Address Correction Module
1. Modify `include/transform/address_correction.py`:
   - Update `geocode_correct_address()` to use Geocodio
   - Update component parsing in `parse_address_components()`
   - Test with sample addresses
2. Run unit tests (if available)
3. Test with `weekly_listings_etl` DAG in dev environment

### Phase 4: Update Address Geocoding Module
1. Modify `include/transform/address_geocoding.py`:
   - Replace geopy with direct Geocodio API calls
   - Update `geocode_pool_location()`
   - Update `batch_geocode_pools()`
2. Run unit tests (if available)
3. Test with `osm_collection` DAG in dev environment

### Phase 5: Update DAG References
1. Update `weekly_listings_etl.py`:
   - Verify environment variable usage (switch from `GOOGLE_GEOCODE_API_KEY` to `GEOCODIO_API_KEY`)
   - Update log messages to reference Geocodio instead of Google
2. Update `osm_collection.py`:
   - Same environment variable updates
   - Update log messages

### Phase 6: Testing
1. Unit tests for each modified function
2. Integration tests for each DAG
3. Test with production-like data volume
4. Verify accuracy of geocoding results
5. Monitor API usage and costs

### Phase 7: Deployment
1. Update .env in production
2. Deploy new code
3. Monitor first few runs closely
4. Validate data quality

### Phase 8: Cleanup
1. Remove Google API dependencies from requirements.txt
2. Remove unused Google API key environment variables
3. Update documentation

---

## Code Quality Checklist

- [ ] All Google API references removed
- [ ] New Geocodio functions have docstrings
- [ ] Error handling covers all edge cases
- [ ] Logging is informative and consistent
- [ ] Rate limiting is properly implemented
- [ ] Timeouts are configured appropriately
- [ ] API key is read from correct environment variable
- [ ] Response parsing handles missing/null values
- [ ] Batch operations are optimized where possible
- [ ] Code follows existing project style

---

## Risk Assessment

### Low Risk:
- Geocodio API is stable and well-documented
- Similar response structure to Google
- Easy to test in isolation

### Medium Risk:
- Rate limiting differences may require tuning
- Response time differences may affect pipeline duration
- Canadian postal codes are FSA only (3 characters vs full 6)

### Mitigation:
- Test thoroughly in dev environment
- Monitor first production runs closely
- Have rollback plan ready
- Keep Google API key available for 30 days as backup

---

## Cost Comparison

### Google Geocoding API:
- Current usage: Unknown
- Pricing: $0.005 per request (forward/reverse)

### Geocodio API:
- Pricing: Pay-as-you-go or monthly plans
- $0.0005 per lookup (10x cheaper than Google)
- Batch operations count as single lookup with multiplier
- Distance endpoints: 2x lookup cost for driving mode

### Expected Savings:
- 90% cost reduction for geocoding operations
- Better pricing for batch operations

---

## Rollback Plan

If issues arise after migration:

1. **Immediate Rollback:**
   - Revert code to previous commit
   - Switch .env back to `GOOGLE_GEOCODE_API_KEY`
   - Redeploy

2. **Partial Rollback:**
   - Keep address_correction on Geocodio
   - Roll back address_geocoding to Google
   - Investigate specific failures

3. **Data Validation:**
   - Compare geocoding results before/after migration
   - Check for accuracy regressions
   - Verify all address fields are populated correctly

---

## Future Enhancements (Post-Migration)

1. **Batch Optimization:**
   - Implement batch forward geocoding for address correction
   - Implement batch reverse geocoding for OSM pools
   - Could reduce API calls by 90%+

2. **Additional Geocodio Features:**
   - Census data appends (demographics)
   - Congressional district lookups
   - Timezone information
   - School district data

3. **Caching:**
   - Implement local cache for frequently geocoded addresses
   - Reduce API calls for duplicate addresses

4. **Monitoring:**
   - Track API usage and costs
   - Alert on rate limit approaching
   - Monitor geocoding success rate

---

## Success Criteria

Migration is considered successful when:

1. ✅ All Google API references removed
2. ✅ All DAGs run successfully with Geocodio
3. ✅ Geocoding accuracy is equivalent or better
4. ✅ API costs are reduced
5. ✅ No data quality regressions
6. ✅ Pipeline performance is maintained or improved
7. ✅ Error handling is robust
8. ✅ Logging is clear and actionable

---

## Questions & Decisions Needed

1. **Batch Processing:** Should we implement batch geocoding immediately or as a future enhancement?
   - **Recommendation:** Implement for OSM collection (pools) immediately, as it processes many coordinates at once
   - For address correction, defer to Phase 2 as it processes fewer addresses

2. **Postal Code Handling:** How should we handle Canadian postal codes (3-char FSA vs 6-char full)?
   - **Recommendation:** Store FSA, document limitation, consider supplementary data source if full postal needed

3. **Rate Limiting Strategy:** What delays should we use?
   - **Recommendation:** Start with same delays as Google (0.05s), adjust based on monitoring

4. **Error Threshold:** How many geocoding failures are acceptable before alerting?
   - **Recommendation:** Alert if >5% of addresses fail to geocode in a single run

---

## Timeline Estimate

- **Phase 1 (Preparation):** 1 hour
- **Phase 2 (Geocodio Utility):** 4 hours
- **Phase 3 (Address Correction):** 4 hours
- **Phase 4 (Address Geocoding):** 4 hours
- **Phase 5 (DAG Updates):** 2 hours
- **Phase 6 (Testing):** 6 hours
- **Phase 7 (Deployment):** 2 hours
- **Phase 8 (Cleanup):** 1 hour

**Total Estimated Time:** 24 hours (3 working days)

---

## Contact & References

- **Geocodio Documentation:** https://www.geocod.io/docs/
- **Geocodio Dashboard:** https://dash.geocod.io/
- **API Status:** https://status.geocod.io/
- **Support:** support@geocod.io

---

## Appendix: API Request Examples

### Forward Geocoding (Address Correction)

**Google (Current):**
```
GET https://maps.googleapis.com/maps/api/geocode/json
  ?address=1109+N+Highland+St,+Arlington,+VA
  &key=AIzaSyAg...
```

**Geocodio (New):**
```
GET https://api.geocod.io/v1.9/geocode
  ?street=1109+N+Highland+St
  &city=Arlington
  &state=VA
  &api_key=509596...
```

### Reverse Geocoding (Pool Addresses)

**Google (Current via geopy):**
```python
location = geolocator.reverse("38.8977, -77.0365")
```

**Geocodio (New):**
```
GET https://api.geocod.io/v1.9/reverse
  ?q=38.8977,-77.0365
  &api_key=509596...
```

### Batch Reverse Geocoding (Future Enhancement)

**Geocodio:**
```
POST https://api.geocod.io/v1.9/reverse?api_key=509596...
Content-Type: application/json

[
  "38.8977,-77.0365",
  "40.7128,-74.0060",
  "41.8781,-87.6298"
]
```

---

**End of Migration Plan**
