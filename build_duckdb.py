# 0_build_duckdb.py
import duckdb, os
from config import USERS_PATH, EVENTS_PATH, DUCKDB_FILE

os.makedirs("data", exist_ok=True)
con = duckdb.connect(DUCKDB_FILE)

# Register lazily (no heavy load)
con.execute(f"CREATE OR REPLACE VIEW users AS SELECT * FROM read_parquet('{USERS_PATH}')")
con.execute(f"CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('{EVENTS_PATH}')")

# Basic per-user funnel flags
con.execute("""
CREATE OR REPLACE TABLE user_events AS
SELECT
  e.muid,
  any_value(e.device_os)           AS last_device,        -- rough
  COUNT(*)                         AS total_events,
  COUNT_IF(event_name ILIKE '%view%')         AS views,
  COUNT_IF(event_name ILIKE '%add%cart%')     AS add_to_cart,
  COUNT_IF(event_name ILIKE '%purchase%' OR category ILIKE '%purchase%') AS purchases,
  COUNT(DISTINCT session_id)       AS sessions,
  COUNT_IF(event_name ILIKE '%scroll%')       AS scrolls,
  LIST(DISTINCT LOWER(NULLIF(device_os,'')))  AS device_list,
  LIST(DISTINCT LOWER(NULLIF(category,'')))   AS categories_seen,
  max(event_time)                  AS last_event_time
FROM events e
GROUP BY e.muid
""")

# Funnel flags
con.execute("""
CREATE OR REPLACE TABLE user_features AS
SELECT
  uev.muid,
  COALESCE(uev.total_events,0) AS total_events,
  uev.views, uev.add_to_cart, uev.purchases, uev.sessions, uev.scrolls,
  uev.device_list, uev.categories_seen,
  (uev.views > 0 AND uev.add_to_cart = 0)    AS viewed_no_cart,
  (uev.add_to_cart > 0 AND uev.purchases=0)  AS cart_no_purchase,
  (uev.purchases > 0)                         AS has_purchase,
  uev.last_device,
  uev.last_event_time
FROM user_events uev
""")

# For clustering: turn lists into strings (simple bag-of-words style) + numeric features
con.execute("""
COPY (
  SELECT
    muid,
    total_events, views, add_to_cart, purchases, sessions, scrolls,
    array_to_string(device_list, ' ')     AS device_bow,
    array_to_string(categories_seen, ' ') AS category_bow,
    viewed_no_cart::INT AS f_viewed_no_cart,
    cart_no_purchase::INT AS f_cart_no_purchase,
    has_purchase::INT AS f_has_purchase
  FROM user_features
) TO 'data/features.parquet' (FORMAT PARQUET)
""")

print("✅ DuckDB built. Wrote data/features.parquet")
