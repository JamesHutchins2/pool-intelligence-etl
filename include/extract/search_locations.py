from include.db import connections




def get_all_search_locations():
    conn = connections.get_master_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT country, province_state, city
                    FROM search_domains
                    ORDER BY country, province_state, city;
                """)
                rows = cur.fetchall()
        return [{"country_code": r[0], "province_state": r[1], "search_area": r[2]} for r in rows]
    finally:
        conn.close()



