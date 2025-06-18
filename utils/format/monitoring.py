def format_seconds(seconds):
    minutes = int(seconds) // 60
    sec = int(seconds) % 60
    return f"{minutes}m {sec}s"


def print_relations_process(batch_duration, elapsed_since_start, est_remaining_time, processed_relations,
                            total_relations, total_rows_written):
    print(f"Processed edges: {processed_relations}/{total_relations} "
          f"({processed_relations / total_relations:.2%}), "
          f"rows written: {total_rows_written}, "
          f"batch time: {batch_duration:.2f}s, "
          f"elapsed: {format_seconds(elapsed_since_start)}, "
          f"ETA: {format_seconds(est_remaining_time)}")

def print_vertex_process(batch_duration, elapsed_since_start, est_remaining_time, processed_nodes, total_nodes,
                total_rows_written):
    print(f"Processed vertexes: {processed_nodes}/{total_nodes} "
          f"({processed_nodes / total_nodes:.2%}), "
          f"rows written: {total_rows_written}, "
          f"batch time: {batch_duration:.2f}s, "
          f"elapsed: {format_seconds(elapsed_since_start)}, "
          f"ETA: {format_seconds(est_remaining_time)}")