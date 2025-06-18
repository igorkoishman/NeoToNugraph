import json


def count_lines(filepath):
    print(f"Counting lines in {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as f:
        for i, _ in enumerate(f, 1):
            pass
    print(f"Total lines in file: {i}")
    return i


def stream_json_lines(path):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"Skipping bad JSON line: {e}")