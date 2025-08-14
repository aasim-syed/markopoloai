import pyarrow.parquet as pq
import glob
from neo4j import GraphDatabase

USERS_PATH = r"C:\Users\syeda\Downloads\markopoloai\markopoloai\users\*.parquet"
EVENTS_PATH = r"C:\Users\syeda\Downloads\markopoloai\markopoloai\events\*.parquet"

NEO4J_URI = "neo4j://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "1234@neo4j"

BATCH_SIZE = 1000


def load_parquet_in_batches(path_pattern):
    for file in glob.glob(path_pattern):
        table = pq.read_table(file)
        df = table.to_pandas()
        for start in range(0, len(df), BATCH_SIZE):
            yield df.iloc[start:start+BATCH_SIZE]
        print(f"✅ Loaded {len(df)} rows from {file}")


def detect_id_column(df):
    possible_names = ["id", "user_id", "event_id", "uuid"]
    for col in df.columns:
        if col.lower() in possible_names:
            return col
    raise ValueError(f"No suitable ID column found in columns: {list(df.columns)}")


def create_constraints(driver):
    with driver.session() as s:
        s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE")
        s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (e:Event) REQUIRE e.id IS UNIQUE")
        s.run("CREATE INDEX user_muid_index IF NOT EXISTS FOR (u:User) ON (u.muid)")
        s.run("CREATE INDEX event_muid_index IF NOT EXISTS FOR (e:Event) ON (e.muid)")
    print("✅ Constraints & indexes created")


def upload_nodes(driver, path_pattern, label):
    print(f"⬆️ Uploading {label} data ...")
    total = 0
    id_col = None
    with driver.session() as s:
        for batch in load_parquet_in_batches(path_pattern):
            if id_col is None:
                id_col = detect_id_column(batch)

            records = []
            for _, row in batch.iterrows():
                records.append({"id": row[id_col], "props": row.to_dict()})

            s.run(
                f"""
                UNWIND $rows AS row
                MERGE (n:{label} {{id: row.id}})
                SET n += row.props
                """,
                rows=records
            )

            total += len(records)
            print(f"   ... {total:,} {label.lower()}s uploaded")
    print(f"✅ All {label.lower()}s uploaded ({total:,})")


def link_events_to_users(driver):
    print("🔗 Linking events to users by muid ...")
    with driver.session() as s:
        s.run("""
        MATCH (u:User), (e:Event)
        WHERE u.muid = e.muid
        MERGE (u)-[:PERFORMED]->(e)
        """)
    print("✅ Linking complete")


def main():
    print("🔌 Connecting to Neo4j ...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    create_constraints(driver)

    # Upload events first
    upload_nodes(driver, EVENTS_PATH, "Event")
    upload_nodes(driver, USERS_PATH, "User")

    link_events_to_users(driver)

    driver.close()
    print("🏁 Done!")



if __name__ == "__main__":
    main()
