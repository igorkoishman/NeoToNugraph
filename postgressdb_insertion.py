import json
import time
import psycopg2
from psycopg2.extras import execute_values

from utils.db_utils.db_util import DB_PARAMS
from utils.file_utils.read_right_utils import count_lines, stream_json_lines

BATCH_SIZE = 10000
INPUT_FILE = "/Volumes/work/bnmd_all.json"  # Replace with your actual path


def connect_db():
    """Establish and return DB connection and cursor."""
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = False
    cur = conn.cursor()
    return conn, cur


def close_db(conn, cur):
    """Close cursor and connection."""
    cur.close()
    conn.close()


def insert_batch(cur, conn, query, batch, batch_type):
    """Insert a batch of records with timing and commit."""
    start = time.time()
    execute_values(cur, query, batch)
    conn.commit()
    duration = time.time() - start
    print(f"Inserted {len(batch)} {batch_type} in {duration:.2f} seconds")
    return duration


def build_node_record(record):
    """Build a tuple for node insertion."""
    return (
        record["id"],
        record["labels"],
        json.dumps(record["properties"])
    )


def build_relationship_record(record):
    """Build a tuple for relationship insertion."""
    return (
        record["id"],
        record["label"],
        record["start"]["id"],
        record["end"]["id"],
        json.dumps(record["start"]["properties"]),
        json.dumps(record["end"]["properties"])
    )


def process_record(record, node_batch, rel_batch):
    """Append record to the appropriate batch list."""
    if record["type"] == "node":
        node_batch.append(build_node_record(record))
    elif record["type"] == "relationship":
        rel_batch.append(build_relationship_record(record))


def should_flush_batch(batch):
    """Check if batch size reached threshold."""
    return len(batch) >= BATCH_SIZE


def flush_node_batch(cur, conn, node_batch, total_nodes):
    """Insert and clear node batch, update count."""
    if node_batch:
        insert_batch(
            cur, conn,
            "INSERT INTO nodes (id, labels, properties) VALUES %s ON CONFLICT (id) DO NOTHING",
            node_batch,
            "nodes"
        )
        total_nodes += len(node_batch)
        node_batch.clear()
    return total_nodes


def flush_relationship_batch(cur, conn, rel_batch, total_rels):
    """Insert and clear relationship batch, update count."""
    if rel_batch:
        insert_batch(
            cur, conn,
            """INSERT INTO relationships
            (id, label, start_id, end_id, start_properties, end_properties)
            VALUES %s ON CONFLICT (id) DO NOTHING""",
            rel_batch,
            "relationships"
        )
        total_rels += len(rel_batch)
        rel_batch.clear()
    return total_rels


def print_eta(start_time, processed, total_records):
    """Print ETA based on records processed."""
    elapsed = time.time() - start_time
    avg_time_per_record = elapsed / processed
    remaining = total_records - processed
    eta_seconds = avg_time_per_record * remaining
    print(f"Processed {processed}/{total_records} records, ETA ~ {eta_seconds / 60:.2f} minutes")


def insert_data(path, total_records=None):
    start_time = time.time()
    conn, cur = connect_db()

    node_batch = []
    rel_batch = []
    total_nodes = 0
    total_rels = 0

    try:
        for i, record in enumerate(stream_json_lines(path), 1):
            process_record(record, node_batch, rel_batch)

            if should_flush_batch(node_batch):
                total_nodes = flush_node_batch(cur, conn, node_batch, total_nodes)
                print(f"Total nodes inserted: {total_nodes}")

            if should_flush_batch(rel_batch):
                total_rels = flush_relationship_batch(cur, conn, rel_batch, total_rels)
                print(f"Total relationships inserted: {total_rels}")

            if total_records and i % BATCH_SIZE == 0:
                print_eta(start_time, i, total_records)

        # Final flush
        total_nodes = flush_node_batch(cur, conn, node_batch, total_nodes)
        total_rels = flush_relationship_batch(cur, conn, rel_batch, total_rels)

        print(f"Final nodes inserted (total {total_nodes})")
        print(f"Final relationships inserted (total {total_rels})")

    except Exception as e:
        print("❌ Error during ETL:", e)
        conn.rollback()
    finally:
        close_db(conn, cur)
        duration = time.time() - start_time
        print(f"✅ Finished ETL in {duration:.2f} seconds")


if __name__ == "__main__":
    total_lines = count_lines(INPUT_FILE)
    insert_data(INPUT_FILE, total_lines)