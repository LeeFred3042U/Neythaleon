def detect_bit_array_columns(selected_columns, manifest):
    """
    Returns the subset of selected_columns that are FIXED_LEN_BYTE_ARRAY.
    
    Args:
        selected_columns: list of column names
        manifest: dict of {col_name: ColumnMeta}
    """
    return [c for c in selected_columns if manifest.get(c) and manifest[c].is_bit_array]
