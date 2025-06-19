# NeoToNugraph ETL Tool

This project ingests a large `.json` file containing `node` and `relationship` data and inserts it into a PostgreSQL database. It also supports parallel generation of edge and vertex CSV files, along with readyfile signaling.

---

## Run ETL Script
python full_script.py /path/to/input.json

## json file from neo should contains 
```
{"type": "node", "id": "n1", "labels": ["Entity"], "properties": {"guid": "123", "name": "Example"}}
{"type": "relationship", "id": "r1", "label": "LINKS", "start": {"id": "n1", "properties": {}}, "end": {"id": "n2", "properties": {}}}
```
## final  files will be 

** vertex.csv: contains flattened node properties

** edge.csv: contains encoded relationship info
