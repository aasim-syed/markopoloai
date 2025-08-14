# ask.py  -- TF-IDF semantic ranker + flattened graph filters (Py 3.13 friendly)
import sys, json
from typing import List, Dict, Any
from neo4j import GraphDatabase
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASS, TOPK_RETURN

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

DEVICE_ALIASES = {
    "ios": ["ios", "iphone", "ipad"],
    "android": ["android"],
    "desktop": ["desktop", "web", "windows", "mac", "linux"]
}
KNOWN_CATS = ["coffee","electronics","fashion","grocery","beauty","books","toys"]

def parse_filters(q: str) -> Dict[str, Any]:
    ql = q.lower()
    device = None
    for d, kws in DEVICE_ALIASES.items():
        if any(k in ql for k in kws):
            device = d
            break
    cats = [c for c in KNOWN_CATS if c in ql]
    flags = {
        "view_no_cart": any(x in ql for x in ["viewing but not adding","not adding to cart","view but no cart","viewers not adding"]),
        "cart_no_purchase": any(x in ql for x in ["add to cart but not purchase","no purchase","abandon cart"]),
        "repeat_purchasers": any(x in ql for x in ["repeat purchasers","repeat buyers","repeat purchase"])
    }
    return {"device": device, "categories": cats, "flags": flags}

def graph_filter(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    cy = "MATCH (c:Cluster) WHERE 1=1 "
    params = {}
    if filters["device"]:
        cy += " AND $device IN c.device_list "
        params["device"] = filters["device"]
    if filters["categories"]:
        cy += " AND any(cat IN $cats WHERE cat IN c.top_categories) "
        params["cats"] = filters["categories"]

    # flattened rates on Cluster
    if filters["flags"]["view_no_cart"]:
        cy += " AND c.view_no_cart_rate > 0.20 "
    if filters["flags"]["cart_no_purchase"]:
        cy += " AND c.add_no_buy_rate > 0.20 "
    if filters["flags"]["repeat_purchasers"]:
        cy += " AND c.conversion_rate > 0.30 "

    cy += """
    OPTIONAL MATCH (u:Customer)-[:IN_CLUSTER]->(c)
    WITH c, collect(u.id)[0..5] AS sample_muids
    RETURN c.cluster_id AS cluster_id, c.summary_text AS summary_text, c.size AS size,
           sample_muids
    """

    with driver.session() as s:
        return [dict(r) for r in s.run(cy, **params)]

def rank_semantic(rows: List[Dict[str, Any]], query: str, topk: int):
    if not rows:
        return []
    docs = [r["summary_text"] for r in rows]
    vec = TfidfVectorizer(ngram_range=(1,2), min_df=1)
    X = vec.fit_transform(docs + [query])   # last row is the query
    sims = cosine_similarity(X[-1], X[:-1]).ravel()
    order = sims.argsort()[::-1][:min(topk, len(rows))]
    out = []
    for rank, idx in enumerate(order, start=1):
        r = rows[idx]
        out.append({
            "rank": rank,
            "cluster_id": r["cluster_id"],
            "size": r["size"],
            "summary_text": r["summary_text"],
            "sample_muids": r["sample_muids"]
        })
    return out

def ask(q: str):
    f = parse_filters(q)
    candidates = graph_filter(f)
    ranked = rank_semantic(candidates, q, TOPK_RETURN)
    return {"query": q, "filters": f, "results": ranked}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python ask.py "your natural language question"')
        sys.exit(1)
    print(json.dumps(ask(sys.argv[1]), indent=2))
