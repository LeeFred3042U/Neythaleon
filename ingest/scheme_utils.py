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
            s = pd.to_numeric(series, errors="coerce")
            non_integral_mask = s.notna() & (s % 1 != 0)
            if non_integral_mask.any():
                logger.debug(
                    "Column %s: %d non-integral values will become NA for integer target", 
                    col, 
                    int(non_integral_mask.sum())
                    )
            s = s.where(~non_integral_mask, other=pd.NA)
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
            df[col] = pd.array([_to_nullable_bool(v) for v in series], dtype="boolean")
            continue

        # otherwise: leave column as-is (string/text)
    return df


def _to_nullable_bool(val):
    """Convert a value to a nullable boolean, handling common truthy/falsy representations."""
    if pd.isna(val):
        return pd.NA
    if isinstance(val, (bool, np.bool_)):
        return bool(val)
    if isinstance(val, (int, float)):
        if val == 1:
            return True
        if val == 0:
            return False
        return pd.NA
    if isinstance(val, str):
        lower = val.strip().lower()
        if lower in ("true", "t", "yes", "1"):
            return True
        if lower in ("false", "f", "no", "0"):
            return False
    return pd.NA