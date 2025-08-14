# 2_load_neo4j.py
import pandas as pd, random
from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASS, SAMPLES_PER_CLUSTER

clusters = pd.read_parquet("data/clusters.parquet")
members  = pd.read_parquet("data/cluster_members.parquet")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

def create_schema():
    with driver.session() as s:
        s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Cluster) REQUIRE c.cluster_id IS UNIQUE")
        s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:Customer) REQUIRE u.id IS UNIQUE")

def load_clusters():
    rows = clusters.to_dict(orient="records")

    def to_str_list(x):
        if x is None:
            return []
        if isinstance(x, (list, tuple)):
            return [str(v) for v in x if v is not None]
        try:
            import numpy as np
            if isinstance(x, np.ndarray):
                return [str(v) for v in x.tolist() if v is not None]
        except Exception:
            pass
        return [str(x)]

    # flatten + coerce to primitives/arrays of primitives
    for r in rows:
        ff = r.get("funnel_flags", {}) or {}
        r["conversion_rate"]   = float(ff.get("conversion_rate", 0.0) or 0.0)
        r["add_no_buy_rate"]   = float(ff.get("add_no_buy_rate", 0.0) or 0.0)
        r["view_no_cart_rate"] = float(ff.get("view_no_cart_rate", 0.0) or 0.0)
        r.pop("funnel_flags", None)
        r.pop("embedding", None)  # optional/unused in TF-IDF path

        r["cluster_id"]     = int(r.get("cluster_id"))
        r["size"]           = int(r.get("size", 0) or 0)
        r["summary_text"]   = str(r.get("summary_text", "") or "")
        r["device_list"]    = to_str_list(r.get("device_list"))
        r["top_categories"] = to_str_list(r.get("top_categories"))

    with driver.session() as s:
        s.run("""
        UNWIND $rows AS row
        MERGE (c:Cluster {cluster_id: row.cluster_id})
        SET c.size              = row.size,
            c.summary_text      = row.summary_text,
            c.device_list       = row.device_list,
            c.top_categories    = row.top_categories,
            c.conversion_rate   = row.conversion_rate,
            c.add_no_buy_rate   = row.add_no_buy_rate,
            c.view_no_cart_rate = row.view_no_cart_rate
        """, rows=rows)

def sample_and_link():
    # sample up to SAMPLES_PER_CLUSTER muids per cluster
    out = []
    for cid, g in members.groupby("cluster_id"):
        muids = g["muid"].astype(str).tolist()
        if not muids:
            continue
        sample = random.sample(muids, min(len(muids), SAMPLES_PER_CLUSTER))
        for m in sample:
            out.append({"cluster_id": int(cid), "muid": m})

    if not out:
        print("⚠️ No sampled customers to link.")
        return

    with driver.session() as s:
        s.run("""
        UNWIND $rows AS row
        MERGE (u:Customer {id: row.muid})
        WITH u, row
        MATCH (c:Cluster {cluster_id: row.cluster_id})
        MERGE (u)-[:IN_CLUSTER]->(c)
        """, rows=out)

    # Optional: tiny Journey node — **scalar property only**
    with driver.session() as s:
        s.run("""
        MATCH (u:Customer)-[:IN_CLUSTER]->(c:Cluster)
        WITH u, c LIMIT 1000
        MERGE (j:Journey {jid: u.id + '-' + toString(c.cluster_id)})
        SET j.mini = true
        MERGE (u)-[:HAS_JOURNEY]->(j)
        """)

if __name__ == "__main__":
    create_schema()
    load_clusters()
    sample_and_link()
    driver.close()
    print("✅ Loaded clusters + samples into Neo4j.")
