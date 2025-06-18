
LABEL_PRIORITY = [
    "BuyerMetadataEntity",
    "ExternalMetadataEntity",
    "GroupEntity",
    "RuleEntity",
    "TokenEntity",
    "Operator",
    "MetadataEntity",
    "Entity"
]

def get_main_label(labels):
    if not isinstance(labels, list):
        return "Unknown"
    for priority_label in LABEL_PRIORITY:
        if priority_label in labels:
            return priority_label
    return labels[0] if labels else "Unknown"