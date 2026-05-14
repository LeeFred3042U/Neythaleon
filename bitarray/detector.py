def detect_bit_array_columns(selected_columns, manifest):

    return [c for c in selected_columns if manifest.get(c) and manifest[c].is_bit_array]
