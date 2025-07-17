import csv
import json
import time


import psycopg2

from converters.db_to_model import convert_node_to_dict
from model import Property, Node
from utils.LabelMapper import LabelMapper
from utils.db_utils.db_util import get_total_node_count, handle_vertex_bulk, vertex_batch_size, DB_PARAMS
from utils.format.monitoring import print_vertex_process


# Configuration



def fetch_nodes_from_postgres(batch_size):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor(name='node_cursor')
    cursor.itersize = batch_size

    cursor.execute("SELECT id, labels, properties FROM nodes")

    meta_cursor = conn.cursor()
    total_nodes = get_total_node_count(meta_cursor)
    meta_cursor.close()
    processed_nodes = 0
    mapper = LabelMapper()
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        batch_nodes = handle_vertex_bulk(mapper,rows)
        processed_nodes += len(batch_nodes)
        yield batch_nodes, processed_nodes, total_nodes

    cursor.close()
    conn.close()

def convert_node_to_vertex_delta_row(node):
    # node should have id (or node_id), labels, properties
    # Pick the label using your LabelMapper (e.g., main_label)
    vertex_id = getattr(node, 'id', None)
    label = getattr(node, 'label', [])
    properties = getattr(node, 'properties', {})

    # Use your LabelMapper logic here if needed
    # mapper = LabelMapper()
    # if isinstance(labels, str):
    #     labels = json.loads(labels)
    # label_info = mapper.get_mapping(labels)
    # main_label = label_info['label']

    unique_key = {"vertex_id": int(vertex_id)}
    # CSV: JSON dump, but let csv.writer do quoting
    def escape_json_field(obj):
        return json.dumps(obj, separators=(',', ':'))

    row = [
        'vertex_add',
        label,
        escape_json_field(unique_key),
        escape_json_field(properties)
    ]
    return row

# def write_nodes_to_csv(file_path, batches):
#     with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
#         fieldnames = ['vertex_id', 'vertex_label', 'property_name', 'property_data_type', 'property_value']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         # writer.writeheader()
#
#         total_rows_written = 0
#         for batch_nodes, processed_nodes, total_nodes in batches:
#             batch_start_time = time.time()
#             for node in batch_nodes:
#                 rows = convert_node_to_dict(node)
#                 for row in rows:
#                     writer.writerow(row)
#                     total_rows_written += 1
#             elapsed_since_start = time.time() - start_time
#             batch_duration = time.time() - batch_start_time
#
#             # Estimate time remaining
#             if processed_nodes > 0:
#                 avg_time_per_node = elapsed_since_start / processed_nodes
#                 remaining_nodes = total_nodes - processed_nodes
#                 est_remaining_time = avg_time_per_node * remaining_nodes
#             else:
#                 est_remaining_time = 0
#
#             print_vertex_process(batch_duration, elapsed_since_start, est_remaining_time, processed_nodes, total_nodes,
#                         total_rows_written)
#
#             csvfile.flush()
#
#         print(f"✅ Finished writing {total_rows_written} rows to CSV in {time.time() - start_time:.2f}s.")
#

def write_nodes_to_csv(file_path, batches, new_vertex_path=None):
    with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile, \
            (open(new_vertex_path, mode='w', newline='', encoding='utf-8') if new_vertex_path else open('/dev/null',
                                                                                                        'w')) as newcsv:

        fieldnames = ['vertex_id', 'vertex_label', 'property_name', 'property_data_type', 'property_value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # New format writer
        vertex_fieldnames = ['delta_type', 'label', 'unique_key', 'properties']
        new_writer = csv.writer(newcsv, quoting=csv.QUOTE_MINIMAL)
        new_writer.writerow(vertex_fieldnames)  # Write header

        total_rows_written = 0
        total_vertex_rows_written = 0
        for batch_nodes, processed_nodes, total_nodes in batches:
            batch_start_time = time.time()
            for node in batch_nodes:
                # Old format
                rows = convert_node_to_dict(node)
                for row in rows:
                    writer.writerow(row)
                    total_rows_written += 1

                # New vertex format
                new_row = convert_node_to_vertex_delta_row(node)
                new_writer.writerow(new_row)
                total_vertex_rows_written += 1

            elapsed_since_start = time.time() - start_time
            batch_duration = time.time() - batch_start_time

            # Estimate time remaining
            if processed_nodes > 0:
                avg_time_per_node = elapsed_since_start / processed_nodes
                remaining_nodes = total_nodes - processed_nodes
                est_remaining_time = avg_time_per_node * remaining_nodes
            else:
                est_remaining_time = 0

            print_vertex_process(batch_duration, elapsed_since_start, est_remaining_time, processed_nodes, total_nodes,
                                 total_rows_written)
            csvfile.flush()
            newcsv.flush()

        print(f"✅ Finished writing {total_rows_written} rows to CSV in {time.time() - start_time:.2f}s.")
        print(f"✅ Finished writing {total_vertex_rows_written} rows to new vertex delta CSV.")


def format_seconds(seconds):
    minutes = int(seconds) // 60
    sec = int(seconds) % 60
    return f"{minutes}m {sec}s"

# if __name__ == "__main__":
#     start_time = time.time()
#     batches = fetch_nodes_from_postgres(vertex_batch_size)
#     write_nodes_to_csv("./nugraph/vertex.csv", batches)


if __name__ == "__main__":
    start_time = time.time()
    batches = fetch_nodes_from_postgres(vertex_batch_size)
    write_nodes_to_csv("./nugraph/vertex.csv", batches, new_vertex_path="./nugraph/vertex_delta.csv")



