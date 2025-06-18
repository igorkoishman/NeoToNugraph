from dataclasses import dataclass
from typing import List, Dict, Any
from model import property,enrichedBrowseNode
import psycopg2

import csv

# Configuration
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "graphdb",
    "user": "igorkoishman",
    "password": "igor1989"
}

batch_size = 1000


@dataclass
class Node:
    id: str
    label: str
    properties: Dict[str, Any]


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

properties: List[Property] = [
    Property("guid", "string", 1, lambda node: node.properties["guid"]),
    Property("guid", "string", 1, lambda node: node.properties["guid"])

]


def get_main_label(labels):
    if not isinstance(labels, list):
        return "Unknown"

    for priority_label in LABEL_PRIORITY:
        if priority_label in labels:
            return priority_label

    # fallback if none match
    return labels[0] if labels else "Unknown"


def fetch_nodes_from_postgres(batch_size):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor(name='node_cursor')  # server-side cursor
    cursor.itersize = batch_size

    cursor.execute("SELECT id, labels, properties FROM nodes")

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        batch_nodes = handle_bulk(rows)
        write_nodes_to_csv("nodes_output.csv", batch_nodes, batch_size)

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


if __name__ == "__main__":
    for n in fetch_nodes_from_postgres():
        print(n)


def convert_node_to_dict(node):
    """
    Convert Node object fields to a dictionary suitable for CSV writing.
    Customize this to implement your field conversions.
    """
    return {
        "id": node.id,
        "label": node.label,
        # Example: convert properties dict to JSON string, or flatten as needed
        "properties": str(node.properties)
    }


def write_nodes_to_csv(file_path, batch_generator, batch_size=500):
    with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['vertex_id', 'vertex_label', 'property_name', 'property_data_type', 'property_value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        batch_count = 0
        for node in batch_generator:
            row = convert_node_to_dict(node)
            writer.writerow(row)
            batch_count += 1
            if batch_count % batch_size == 0:
                csvfile.flush()
                print(f"Written {batch_count} rows to CSV so far...")

        # Final flush after all rows
        csvfile.flush()
        print(f"Finished writing {batch_count} rows to CSV.")
