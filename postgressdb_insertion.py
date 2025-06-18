import json
import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm

# Configuration
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "graphdb",
    "user": "igorkoishman",
    "password": "igor1989"
}
BATCH_SIZE = 500
INPUT_FILE = "/Volumes/work/bnmd_all.json" # Replace with your actual path

def stream_json_lines(path):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"Skipping bad JSON line: {e}")

def insert_data(path):
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = False  # Explicit commit
    cur = conn.cursor()

    node_batch = []
    rel_batch = []
    total_nodes = total_rels = 0

    try:
        for i, record in enumerate(stream_json_lines(path), 1):
            if record["type"] == "node":
                node_batch.append((
                    record["id"],
                    record["labels"],
                    json.dumps(record["properties"])
                ))
                if len(node_batch) >= BATCH_SIZE:
                    execute_values(
                        cur,
                        "INSERT INTO nodes (id, labels, properties) VALUES %s ON CONFLICT (id) DO NOTHING",
                        node_batch
                    )
                    conn.commit()
                    total_nodes += len(node_batch)
                    print(f"Inserted {total_nodes} nodes...")
                    node_batch.clear()

            elif record["type"] == "relationship":
                rel_batch.append((
                    record["id"],
                    record["label"],
                    record["start"]["id"],
                    record["end"]["id"],
                    json.dumps(record["start"]["properties"]),
                    json.dumps(record["end"]["properties"])
                ))
                if len(rel_batch) >= BATCH_SIZE:
                    execute_values(
                        cur,
                        """INSERT INTO relationships
                        (id, label, start_id, end_id, start_properties, end_properties)
                        VALUES %s ON CONFLICT (id) DO NOTHING""",
                        rel_batch
                    )
                    conn.commit()
                    total_rels += len(rel_batch)
                    print(f"Inserted {total_rels} relationships...")
                    rel_batch.clear()

        # Final flush
        if node_batch:
            execute_values(
                cur,
                "INSERT INTO nodes (id, labels, properties) VALUES %s ON CONFLICT (id) DO NOTHING",
                node_batch
            )
            conn.commit()
            total_nodes += len(node_batch)
            print(f"Inserted final {len(node_batch)} nodes (total {total_nodes})")

        if rel_batch:
            execute_values(
                cur,
                """INSERT INTO relationships
                (id, label, start_id, end_id, start_properties, end_properties)
                VALUES %s ON CONFLICT (id) DO NOTHING""",
                rel_batch
            )
            conn.commit()
            total_rels += len(rel_batch)
            print(f"Inserted final {len(rel_batch)} relationships (total {total_rels})")

    except Exception as e:
        print("❌ Error during ETL:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        print("✅ Finished ETL")
if __name__ == "__main__":
    insert_data(INPUT_FILE)