from include.db import connections




def get_all_search_locations():
    conn = connections.get_listing_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT country_code, province_state, search_area
                    FROM search_locations
                    ORDER BY country_code, province_state, search_area;
                """)
                rows = cur.fetchall()
        return [{"country_code": r[0], "province_state": r[1], "search_area": r[2]} for r in rows]
    finally:
        conn.close()



