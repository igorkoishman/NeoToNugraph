import psycopg2

from utils.label_utils import get_main_label
from model import Relationship, Property, Node

# Configuration
edge_batch_size = 10000
vertex_batch_size = 5000

properties_list = [
    Property("categoryFriendlyName", "String", 1, lambda node: node.get_property("categoryFriendlyName")),
    Property("creationTimestamp", "Long", 7, lambda node: node.get_property("creationTimestamp")),
    Property("modificationTimestamp", "Long", 7, lambda node: node.get_property("modificationTimestamp")),
    Property("deleted", "Boolean", 6, lambda node: node.get_property("deleted")),
    Property("enabled", "Boolean", 6, lambda node: node.get_property("enabled")),
    Property("hidden", "Boolean", 6, lambda node: node.get_property("hidden")),
    Property("guid", "String", 1, lambda node: node.get_property("guid")),
    Property("name", "String", 1, lambda node: node.get_property("name")),
Property("normalizedValue", "String", 1, lambda node: node.get_property("normalizedValue")),
Property("value", "String", 1, lambda node: node.get_property("value")),
Property("type", "String", 1, lambda node: node.get_property("type")),
]

property_map = {prop.name: prop for prop in properties_list}

def get_total_relations_count(cursor):
    cursor.execute("SELECT COUNT(*) FROM relationships")
    return cursor.fetchone()[0]

def get_total_node_count(cursor):
    cursor.execute("SELECT COUNT(*) FROM nodes")
    return cursor.fetchone()[0]

def handle_bulk_relationships(rows):
    batch_relations = []
    for row in rows:
        relation = Relationship(
            id=row[0],
            label=row[1],
            start_id=row[2],
            end_id=row[3],
            start_properties=row[4] or {},
            end_properties=row[5] or {}
        )
        batch_relations.append(relation)
    return batch_relations

def handle_vertex_bulk(rows):
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

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "graphdb",
    "user": "igorkoishman",
    "password": "igor1989"
}