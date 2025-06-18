from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class Relationship:
    id: str
    label: str
    start_id: str
    end_id: str
    start_properties: Dict[str, Any]
    end_properties: Dict[str, Any]