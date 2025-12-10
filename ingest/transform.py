# ingest/transform.py

import logging
from typing import List
import pandas as pd
import numpy as np
from shapely import wkb

logger = logging.getLogger(__name__)

def clean_null_bytes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimized to use vectorized string operations where possible.
    """
    df = df.copy()
    
    # Select object (string/bytes) columns once
    obj_cols = df.select_dtypes(include=[object]).columns

    for col in obj_cols:
        # 1. Vectorized decode for bytes (much faster than apply)
        # We identify actual byte columns by sampling or just trying to decode
        # Since mixed types are hard in vectorization, we force string conversion for safety
        # or use a specialized approach if we know it's bytes.
        # For mixed content, apply is safer, but we can optimize the string replace part.
        
        # 2. Vectorized removal of null bytes
        # This replaces .apply(lambda v: v.replace('\x00', ''))
        try:
            df[col] = df[col].astype(str).str.replace('\x00', '', regex=False)
        except Exception:
            # Fallback for complex mixed types if astype(str) fails
            pass

    return df


def clean_special_characters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimized to remove newlines, returns, and tabs using Regex.
    """
    df = df.copy()
    string_cols = df.select_dtypes(include=[object]).columns
    
    if len(string_cols) > 0:
        # Replace \n, \r, \t with space in one pass using Regex
        # regex=True is key here for performance
        df[string_cols] = df[string_cols].replace(
            {r'[\n\r\t]': ' '}, 
            regex=True
        )
        
    return df


def enforce_integer_types(df: pd.DataFrame, int_columns: List[str] = None) -> pd.DataFrame:
    df = df.copy()
    if not int_columns:
        return df
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    return df


def convert_geometry_to_wkt(df: pd.DataFrame, geom_col: str = 'geometry') -> pd.DataFrame:
    if geom_col not in df.columns:
        return df
    df = df.copy()

    def _to_wkt(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, (bytes, bytearray)):
            try:
                return wkb.loads(val).wkt
            except Exception:
                return None
        try:
            return str(val)
        except Exception:
            return None

    df[geom_col] = df[geom_col].apply(_to_wkt)
    return df


def process_chunk(df, int_columns: List[str] = None):
    # Order matters: decoding/cleaning first, then typing
    df = clean_null_bytes(df)
    df = clean_special_characters(df)
    df = enforce_integer_types(df, int_columns=int_columns)
    df = convert_geometry_to_wkt(df)
    return df