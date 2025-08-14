# Customer-Journey GraphRAG (Neo4j, Parquet-scale, Py 3.13)

## Setup
1) Python 3.13 venv
   - `python -m venv .venv && .\.venv\Scripts\Activate.ps1`
2) Install deps (Py 3.13 friendly)
   - `pip install -r requirements.txt`
3) Configure (optional): edit `config.py` paths (default: `.\events\*.parquet`, `.\users\*.parquet`)

## Build pipeline
1) Features from Parquet (streams all 13×2GB events)
   - `python 0_build_features.py`
2) Cluster & summarize
   - `python 1_cluster_and_summarize.py`
3) Load tiny graph to Neo4j (clusters + samples + journey stubs)
   - `python 2_load_neo4j.py`

## Ask (GraphRAG CLI)
Examples:
- `python ask.py "iOS users viewing coffee but not adding to cart"`
- `python ask.py "Desktop repeat purchasers for electronics"`
- `python ask.py "Android app users who scroll a lot but don’t purchase"`

Output: ranked clusters with `summary_text`, `size`, `sample_muids`.

## Notes
- Raw events remain in Parquet; Neo4j holds only clusters + samples.
- Semantic retrieval = TF-IDF cosine on cluster summaries (no heavy deps).
- Tune thresholds in `ask.py` (`view_no_cart_rate`, `add_no_buy_rate`, `conversion_rate`).

| Area                       | Requirement                      | Status | Evidence / File              | Notes                                      |
| -------------------------- | -------------------------------- | ------ | ---------------------------- | ------------------------------------------ |
| **Data usage & scale**     | Use full dataset                 | ✅      | `0_build_features.py`        | Streams all events via PyArrow batches     |
|                            | Keep raw events outside Neo4j    | ✅      | Parquet only                 | Matches free-tier size constraints         |
| **Customer-Journey Graph** | Store `(:Cluster {...})`         | ✅      | `2_load_neo4j.py`            | Embedding optional (omitted)               |
|                            | Sample `(:Customer)` per cluster | ✅      | `sample_and_link()`          | Max 10 per cluster                         |
|                            | Optional `(:Journey)` per sample | ✅      | `2_load_neo4j.py`            | Minimal props                              |
|                            | Custom clustering                | ✅      | `1_cluster_and_summarize.py` | MiniBatchKMeans                            |
| **Cluster metadata**       | `summary_text`                   | ✅      | `clusters.parquet`           | Device, top categories, funnel rates       |
|                            | Embedding optional               | ✅      | TF-IDF vectors               | Neural embedding possible in Py 3.10 venv  |
| **GraphRAG retrieval**     | Graph filters                    | ✅      | `ask.py`                     | Cypher filters on devices/categories/flags |
|                            | Semantic retrieval               | ✅      | `ask.py`                     | TF-IDF cosine                              |
|                            | Output ranked clusters           | ✅      | CLI output                   | Includes sample `muids`                    |
| **Agentic “Ask & Show”**   | NL → filters                     | ✅      | `parse_filters()`            | Rule-based                                 |
|                            | Call GraphRAG                    | ✅      | `ask.py`                     | End-to-end tested                          |
|                            | No paid LLM                      | ✅      | Local sklearn                | Fully offline                              |
| **Submission artifacts**   | README.md                        | ✅      | This file                    | Contains setup + coverage                  |
|                            | Loader for Neo4j                 | ✅      | `2_load_neo4j.py`            | Works                                      |
|                            | GraphRAG CLI                     | ✅      | `ask.py`                     | Works                                      |
|                            | 2–3 example outputs              | ✅      | `/examples/*.json`           | Save outputs via CLI                       |
