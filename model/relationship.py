class Relationship:
    def __init__(self, start_id: int, end_id: int, type_: str, properties: dict):
        self.start_id = start_id
        self.end_id = end_id
        self.type = type_
        self.properties = properties