from urllib.parse import quote

from utils.db_utils.db_util import property_map


def convert_relationship_to_row(relationship):
    edge_id = f"{relationship.start_properties.get('guid')}-{relationship.label}->{relationship.end_properties.get('guid')}"
    encoded_edge_id = quote(edge_id, safe='')

    return {
        "edge_id": edge_id,
        "edge_label": relationship.label,
        "source_vertex_id": relationship.start_properties.get('guid'),
        "destination_vertex_id": relationship.end_properties.get('guid'),
        "property_name": "edge_id_encoded",
        "property_data_type": 1,
        "property_value": encoded_edge_id
    }


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
