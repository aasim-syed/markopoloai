import os
USERS_PATH   = os.getenv("USERS_PATH",   r".\\users\\*.parquet")
EVENTS_PATH  = os.getenv("EVENTS_PATH",  r".\\events\\*.parquet")
DUCKDB_FILE  = os.getenv("DUCKDB_FILE",  "data/events.duckdb")

NEO4J_URI    = os.getenv("NEO4J_URI",    "neo4j://127.0.0.1:7687/neo4j")
NEO4J_USER   = os.getenv("NEO4J_USER",   "neo4j")
NEO4J_PASS   = os.getenv("NEO4J_PASS",   "1234@neo4j")

N_CLUSTERS   = int(os.getenv("N_CLUSTERS", "64"))
SAMPLES_PER_CLUSTER = int(os.getenv("SAMPLES_PER_CLUSTER", "10"))

EMBED_MODEL  = os.getenv("EMBED_MODEL", "all-MiniLM-L6-v2")
TOPK_RETURN  = int(os.getenv("TOPK_RETURN", "5"))
