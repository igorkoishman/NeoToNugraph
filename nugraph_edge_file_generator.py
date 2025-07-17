import csv
import json
import time


import psycopg2

from converters.db_to_model import convert_relationship_to_row
from utils.LabelMapper import LabelMapper

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


def ensure_dict(val):
    if isinstance(val, dict):
        return val
    return json.loads(val)


def convert_relationship_to_new_edge_row(relation, guid_label_mapping):
    label = relation.label
    start_id = relation.start_id
    end_id = relation.end_id
    start_properties = relation.start_properties
    end_properties = relation.end_properties

    start_props = ensure_dict(start_properties)
    end_props = ensure_dict(end_properties)
    # src_label = start_props.get("type", "")
    # if not src_label:
    guid = start_props.get("guid")
    src_label = guid_label_mapping.get(guid, "")
    # dst_label = end_props.get("type", "")
    # if not dst_label:
    dest_guid = end_props.get("guid")
    dst_label = guid_label_mapping.get(dest_guid, "")

    src_vertex = {"vertex_id": int(start_id)}
    dst_vertex = {"vertex_id": int(end_id)}
    unique_key = {
        "src_id": int(start_id),
        "rel_type": label,
        "dst_id": int(end_id)
    }

    def escape_json_field(obj):
        return json.dumps(obj, separators=(',', ':'))

    row = [
        'edge_add',
        label,
        src_label,
        escape_json_field(src_vertex),
        dst_label,
        escape_json_field(dst_vertex),
        escape_json_field(unique_key),
        '{}'
    ]
    return row

def write_edges_to_csv(file_path, new_file_path, batches, guid_type_mapping):
    with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile, \
         open(new_file_path, mode='w', newline='', encoding='utf-8') as newcsv:
        # Writer for old format
        fieldnames = ['edge_id', 'edge_label', 'source_vertex_id', 'destination_vertex_id', 'property_name',
                      'property_data_type', 'property_value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # Writer for new format
        # We do not write a header row, to match your example
        new_fieldnames = ['delta_type','label','src_label','src_unique_key','dst_label','dst_unique_key','unique_key','properties']
        new_writer = csv.writer(newcsv, quoting=csv.QUOTE_MINIMAL)
        new_writer.writerow(new_fieldnames)
        total_rows_written = 0
        for batch_relations, processed_relations, total_relations in batches:
            batch_start_time = time.time()
            for relation in batch_relations:
                # Write old format
                row = convert_relationship_to_row(relation)
                writer.writerow(row)
                # Write new format
                new_row = convert_relationship_to_new_edge_row(relation,guid_type_mapping)
                new_writer.writerow(new_row)
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
            newcsv.flush()
        print(f"✅ Finished writing {total_rows_written} rows to CSV in {time.time() - start_time:.2f}s.")



def build_guid_label_mapping():
    print("Building guid to label mapping...")
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    cursor.execute("SELECT properties, labels FROM nodes")
    mapping = {}
    mapper = LabelMapper()
    def ensure_dict(val):
        if isinstance(val, dict):
            return val
        return json.loads(val)
    for idx, (prop_val, label_val) in enumerate(cursor.fetchall()):
        if idx % 10000 == 0:
            print(f"Processed {idx} nodes for guid-label mapping...")
        try:
            prop = ensure_dict(prop_val)
            guid = prop.get("guid")
            # label_val is a Python list (if from json/jsonb), or a string
            if isinstance(label_val, str):
                # If label_val is a string, parse it as JSON list
                labels = json.loads(label_val)
                label_info = mapper.get_mapping(labels)
                mapping[guid] = label_info['label']
            else:
                labels = label_val
            # Now use your LabelMapper to choose the canonical label
                label_info = mapper.get_mapping(labels)
                mapping[guid] = label_info['label']
        except Exception as e:
            print(f"Error processing node: {prop_val}, {label_val}\n{e}")
    cursor.close()
    conn.close()
    print(f"Finished guid-label mapping, {len(mapping)} guids.")
    return mapping



if __name__ == "__main__":
    start_time = time.time()
    batches = fetch_relations_from_postgres(edge_batch_size)
    guid_label_mapping = build_guid_label_mapping()
    write_edges_to_csv("./nugraph/edges.csv", "./nugraph/edges_new.csv", batches, guid_label_mapping)
