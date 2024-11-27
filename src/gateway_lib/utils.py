
def get_item(mapping, val, default=None):
    if default is None:
        return mapping[val]
    return mapping.get(val, default)

def swap_key_value(mapping: dict):
    return {v: k for k, v in mapping.items()}