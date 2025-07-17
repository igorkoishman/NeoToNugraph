class LabelMapper:
    def __init__(self):
        # Predefine mapping using frozenset keys for unordered matching
        self.mapping = {
            frozenset({'BuyerMetadataEntity', 'Entity', 'MetadataEntity'}): {
                'label': 'MetadataEntity', 'managedProp': 'BuyerMetadataEntity'
            },
            frozenset({'Entity', 'ExternalMetadataEntity', 'GroupEntity', 'MetadataEntity'}): {
                'label': 'CategoryEntity', 'managedProp': 'ExternalMetadataEntity'
            },
            frozenset({'Entity', 'ExternalMetadataEntity', 'MetadataEntity'}): {
                'label': 'MetadataEntity', 'managedProp': 'ExternalMetadataEntity'
            },
            frozenset({'Entity', 'RuleEntity'}): {
                'label': 'RuleEntity'
            },
            frozenset({'Entity', 'TokenEntity'}): {
                'label': 'TokenEntity'
            },
            frozenset({'Operator'}): {
                'label': 'Operator'
            }
        }

    def get_mapping(self, labels):
        key = frozenset(labels)
        return self.mapping.get(key, {'label': 'Unknown', 'managedProp': None})