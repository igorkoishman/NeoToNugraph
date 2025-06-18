from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class Node:
    def __init__(self, id: int, label: str, properties: Dict[str, Any]):
        self.id = id
        self.label = label
        self.properties = properties

    def get_property(self, key):
        return self.properties.get(key, None)