import csv
import time


import psycopg2

from converters.db_to_model import convert_relationship_to_row


from utils.db_utils.db_util import get_total_relations_count, handle_bulk_relationships, DB_PARAMS, edge_batch_size
from utils.format.monitoring import  print_relations_process



def fetch_relations_from_postgres(batch_size):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor(name='node_cursor')
    cursor.itersize = batch_size

    cursor.execute("SELECT id, label, start_id, end_id, start_properties, end_properties FROM relationships;")

    meta_cursor = conn.cursor()
    total_relations = get_total_relations_count(meta_cursor)
    meta_cursor.close()
    processed_relations = 0

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        batch_relations = handle_bulk_relationships(rows)
        processed_relations += len(batch_relations)
        yield batch_relations, processed_relations, total_relations

    cursor.close()
    conn.close()



def write_edges_to_csv(file_path, batches):
    with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['edge_id', 'edge_label', 'source_vertex_id', 'destination_vertex_id', 'property_name',
                      'property_data_type', 'property_value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # writer.writeheader()

        total_rows_written = 0
        for batch_relations, processed_relations, total_relations in batches:
            batch_start_time = time.time()
            for relation in batch_relations:
                row = convert_relationship_to_row(relation)
                writer.writerow(row)
                total_rows_written += 1
            elapsed_since_start = time.time() - start_time
            batch_duration = time.time() - batch_start_time
            # Estimate time remaining
            if processed_relations > 0:
                avg_time_per_node = elapsed_since_start / processed_relations
                remaining_nodes = total_relations - processed_relations
                est_remaining_time = avg_time_per_node * remaining_nodes
            else:
                est_remaining_time = 0
            print_relations_process(batch_duration, elapsed_since_start, est_remaining_time, processed_relations,
                                    total_relations, total_rows_written)
            csvfile.flush()
        print(f"✅ Finished writing {total_rows_written} rows to CSV in {time.time() - start_time:.2f}s.")


if __name__ == "__main__":
    start_time = time.time()
    batches = fetch_relations_from_postgres(edge_batch_size)
    write_edges_to_csv("./nugraph/edges.csv", batches)
