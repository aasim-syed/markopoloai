# 1_cluster_and_summarize.py
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import MiniBatchKMeans
from sentence_transformers import SentenceTransformer
from config import N_CLUSTERS, EMBED_MODEL

df = pd.read_parquet("data/features.parquet")

# Numeric features
num = df[["total_events","views","add_to_cart","purchases","sessions","scrolls",
          "f_viewed_no_cart","f_cart_no_purchase","f_has_purchase"]].fillna(0)
num_scaled = StandardScaler().fit_transform(num)

# Text-ish features (fast, no vocab): devices + categories
hvec = HashingVectorizer(n_features=2**16, alternate_sign=False, norm="l2")
txt = (df["device_bow"].fillna("") + " " + df["category_bow"].fillna("")).tolist()
X_text = hvec.transform(txt)  # sparse

# Combine: numeric (dense) + text (sparse)
# Stack by hstack; to keep code short, convert numeric to sparse
from scipy.sparse import csr_matrix, hstack
X = hstack([csr_matrix(num_scaled), X_text])

# Cluster
k = N_CLUSTERS if len(df) >= N_CLUSTERS*20 else max(8, len(df)//50)
kmeans = MiniBatchKMeans(n_clusters=k, random_state=42, batch_size=2048, n_init="auto")
labels = kmeans.fit_predict(X)

df["cluster_id"] = labels

# Summaries per cluster
def summarize_cluster(g):
    size = len(g)
    top_devs = (g["device_bow"].str.split().explode().value_counts().head(3).index.tolist())
    top_cats = (g["category_bow"].str.split().explode().value_counts().head(5).index.tolist())
    conv_rate = (g["f_has_purchase"].mean())
    add_no_buy = (g["f_cart_no_purchase"].mean())
    view_no_cart = (g["f_viewed_no_cart"].mean())
    summary = (
        f"Cluster of {size} users. Devices: {top_devs}. "
        f"Top categories: {top_cats}. Conversion={conv_rate:.2f}, "
        f"AddNoBuy={add_no_buy:.2f}, ViewNoCart={view_no_cart:.2f}."
    )
    return pd.Series({
        "size": size,
        "device_list": top_devs,
        "top_categories": top_cats,
        "funnel_flags": {
            "conversion_rate": float(round(conv_rate,4)),
            "add_no_buy_rate": float(round(add_no_buy,4)),
            "view_no_cart_rate": float(round(view_no_cart,4)),
        },
        "summary_text": summary
    })

clusters = df.groupby("cluster_id").apply(summarize_cluster).reset_index()

# Optional embeddings for GraphRAG
model = SentenceTransformer(EMBED_MODEL)
clusters["embedding"] = clusters["summary_text"].apply(lambda t: model.encode(t).astype(np.float32).tolist())

# Save artifacts
df[["muid","cluster_id"]].to_parquet("data/cluster_members.parquet", index=False)
clusters.to_parquet("data/clusters.parquet", index=False)
print(f"✅ Clustering done. k={k}, wrote clusters + members parquet.")
