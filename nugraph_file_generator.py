import csv
from dataclasses import dataclass
from typing import Dict, Any

import psycopg2

from model import Property, Node

# Configuration
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "graphdb",
    "user": "igorkoishman",
    "password": "igor1989"
}

batch_size = 1000

LABEL_PRIORITY = [
    "BuyerMetadataEntity",
    "ExternalMetadataEntity",
    "GroupEntity",
    "RuleEntity",
    "TokenEntity",
    "Operator",
    "MetadataEntity",
    "Entity"  # lowest priority fallback
]

def get_main_label(labels):
    if not isinstance(labels, list):
        return "Unknown"

    for priority_label in LABEL_PRIORITY:
        if priority_label in labels:
            return priority_label

    # fallback if none match
    return labels[0] if labels else "Unknown"

properties_list = [
    Property("categoryFriendlyName", "String", 1, lambda node: node.get_property("categoryFriendlyName")),
    Property("creationTimestamp", "Long", 7, lambda node: node.get_property("creationTimestamp")),
    Property("modificationTimestamp", "Long", 7, lambda node: node.get_property("modificationTimestamp")),
    Property("deleted", "Boolean", 6, lambda node: node.get_property("deleted")),
    Property("enabled", "Boolean", 6, lambda node: node.get_property("enabled")),
    Property("hidden", "Boolean", 6, lambda node: node.get_property("hidden")),
    Property("guid", "String", 1, lambda node: node.get_property("guid")),
    Property("modificationTimestamp", "Long", 7, lambda node: node.get_property("modificationTimestamp")),
    Property("name", "String", 1, lambda node: node.get_property("name")),
Property("normalizedValue", "String", 1, lambda node: node.get_property("normalizedValue")),
Property("value", "String", 1, lambda node: node.get_property("value")),
Property("type", "String", 1, lambda node: node.get_property("type")),
]

property_map = {prop.name: prop for prop in properties_list}

def get_total_node_count():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM nodes")
    total = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return total

# def fetch_nodes_from_postgres(batch_size):
#     conn = psycopg2.connect(**DB_PARAMS)
#     cursor = conn.cursor(name='node_cursor')  # server-side cursor
#     cursor.itersize = batch_size
#
#     cursor.execute("SELECT id, labels, properties FROM nodes")
#
#     total_nodes = get_total_node_count()
#     processed_nodes = 0
#
#     while True:
#         rows = cursor.fetchmany(batch_size)
#         if not rows:
#             break
#
#         batch_nodes = handle_bulk(rows)
#         write_nodes_to_csv("nodes_output.csv", batch_nodes)
#
#     cursor.close()
#     conn.close()


def fetch_nodes_from_postgres(batch_size):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor(name='node_cursor')
    cursor.itersize = batch_size

    cursor.execute("SELECT id, labels, properties FROM nodes")

    total_nodes = get_total_node_count()
    processed_nodes = 0

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        batch_nodes = handle_bulk(rows)
        processed_nodes += len(batch_nodes)
        yield batch_nodes, processed_nodes, total_nodes

    cursor.close()
    conn.close()

def handle_bulk(rows):
    batch_nodes = []
    for row in rows:
        main_label = get_main_label(row[1])
        node = Node(
            id=row[0],
            label=main_label,
            properties=row[2]
        )
        batch_nodes.append(node)
    return batch_nodes

# def convert_node_to_dict(node):
#     """
#     Convert Node object fields to a dictionary suitable for CSV writing.
#     Customize this to implement your field conversions.
#     """
#     return {
#         "id": node.id,
#         "label": node.label,
#         Example: convert properties dict to JSON string, or flatten as needed
        # "properties": str(node.properties)
    # }

# def convert_node_to_dict(node):
#     rows = []
#     for prop in properties_list:
#         value = prop.getter(node)
#         if value is not None:
#             rows.append({
#                 "vertex_id": node.id,
#                 "vertex_label": node.label,
#                 "property_name": prop.name,
#                 "property_data_type": prop.id,
#                 "property_value": value
#             })
#     return rows
def convert_node_to_dict(node):
    rows = []
    for key, val in node.properties.items():
        entity_guid = node.get_property('guid')
        prop = property_map.get(key)
        if prop:
            value = prop.getter(node)  # or just val if getter is simple
            if value is not None:
                rows.append({
                    "vertex_id": entity_guid,
                    "vertex_label": node.label,
                    "property_name": prop.name,
                    "property_data_type": prop.id,
                    "property_value": value
                })
    return rows



# def write_nodes_to_csv(file_path, batch_generator):
#     with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
#         fieldnames = ['vertex_id', 'vertex_label', 'property_name', 'property_data_type', 'property_value']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         # writer.writeheader()
#
#         batch_count = 0
#         for node in batch_generator:
#             rows = convert_node_to_dict(node)
#             for row in rows:  # ✅ Loop over each dictionary in the list
#                 writer.writerow(row)
#                 batch_count += 1
#                 if batch_count % batch_size == 0:
#                     csvfile.flush()
#                     print(f"Written {rows.count()} rows to CSV so far...")
#
#         # Final flush after all rows
#         csvfile.flush()
#         print(f"Finished writing {batch_count} rows to CSV.")

def write_nodes_to_csv(file_path, batches):
    with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['vertex_id', 'vertex_label', 'property_name', 'property_data_type', 'property_value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # writer.writeheader()

        total_rows_written = 0
        for batch_nodes, processed_nodes, total_nodes in batches:
            for node in batch_nodes:
                rows = convert_node_to_dict(node)
                for row in rows:
                    writer.writerow(row)
                    total_rows_written += 1

            print(f"Processed nodes: {processed_nodes}/{total_nodes} ({processed_nodes/total_nodes:.2%}), rows written so far: {total_rows_written}")
            csvfile.flush()

        print(f"Finished writing {total_rows_written} rows to CSV.")


if __name__ == "__main__":
    # for n in fetch_nodes_from_postgres(batch_size):
    #     print(n)
    batches = fetch_nodes_from_postgres(batch_size)
    write_nodes_to_csv("nodes_output.csv", batches)



