import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def coerce_df_to_schema(df: pd.DataFrame, schema_map: dict) -> pd.DataFrame:
    """
    Coerce df columns to types implied by schema_map (column -> sql-type-string).
    Rules:
      - integer-like SQL types (int, bigint, smallint) => try to coerce to Int64.
        non-integral values become <NA> (no rounding).
      - float/real/numeric/decimal => to_numeric(float)
      - timestamp/date => to_datetime
      - bool => pandas BooleanDtype (nullable)
      - others left as-is
    Returns new DataFrame and logs basic stats at DEBUG.
    """
    df = df.copy()
    for col, sqltype in schema_map.items():
        if col not in df.columns:
            continue
        sql = (sqltype or "").lower()
        series = df[col]

        # INTEGER family
        if any(k in sql for k in ("int", "bigint", "smallint")):
            s = pd.to_numeric(series, errors="coerce")  # floats or numeric strings -> float or NaN
            # mark non-integral floats as NaN (so we won't break COPY)
            non_integral = s.notna() & (s % 1 != 0)
            if non_integral.any():
                logger.debug(
                    "Column %s: %d non-integral values will become NA for integer target", 
                    col, 
                    int(non_integral.sum())
                    )
            s[non_integral] = pd.NA
            # cast to pandas nullable Int64
            try:
                df[col] = s.astype("Int64")
            except Exception:
                df[col] = s
            continue

        # FLOAT/NUMERIC family
        if any(k in sql for k in ("numeric", "decimal", "float", "double", "real")):
            df[col] = pd.to_numeric(series, errors="coerce")
            continue

        # TIMESTAMP/DATE
        if any(k in sql for k in ("timestamp", "date", "time")):
            df[col] = pd.to_datetime(series, errors="coerce")
            continue

        # BOOLEAN
        if "bool" in sql:
            # map common truthy/falsy strings; leave others as NA
            df[col] = series.map({True: True, False: False, "true": True, "false": False, "1": True, "0": False}).astype("boolean")
            continue

        # otherwise: leave column as-is (string/text)
    return df