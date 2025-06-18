class Property:
    def __init__(self, name: str, type_of: str, id_: int, getter: Callable[['EnrichedBrowseNode'], Any]):
        self.name = name
        self.type_of = type_of
        self.id = id_
        self.getter = getter