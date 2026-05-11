import io
import json
import logging
import traceback
from typing import List
from sqlalchemy import inspect, text
import pandas as pd
import numpy as np

from ingest.config import FAILED_DIR

logger = logging.getLogger(__name__)


def get_table_columns(engine, table_name: str) -> List[str]:
    with engine.connect() as conn:
        q = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :t
            ORDER BY ordinal_position
        """)
        rows = conn.execute(q, {"t": table_name}).fetchall()
        return [r[0] for r in rows]


def create_table_from_df(engine, table_name: str, df: pd.DataFrame):
    logger.info(
        "Creating table '%s' with %d columns", 
        table_name, 
        len(df.columns)
        )
    df.head(0).to_sql(table_name, engine, if_exists="fail", index=False)
    logger.info("Created table '%s'", table_name)


def copy_insert(engine, df: pd.DataFrame, table_name: str) -> int:
    """
    Inserts df into table_name using PostgreSQL COPY FROM STDIN (FORMAT TEXT).
    Returns number of rows inserted.
    Raises on failure — caller handles retry/skip logic.
    """
    if df.empty:
        logger.debug("Skipping empty DataFrame for table %s", table_name)
        return 0

    df = _prepare_for_copy(df)

    buf = io.StringIO()
    df.to_csv(
        buf,
        sep="\t",
        header=False,
        index=False,
        na_rep=r"\N",
        quoting=3,            # csv.QUOTE_NONE
        escapechar=None,
        encoding="utf-8",
    )
    buf.seek(0)

    quoted_columns = ", ".join(f'"{c}"' for c in df.columns)
    sql = (
        f"COPY {table_name} ({quoted_columns}) "
        f"FROM STDIN WITH (FORMAT TEXT, NULL '\\N', ENCODING 'UTF8')"
    )

    with engine.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            cur.copy_expert(sql, buf)

    row_count = len(df)
    logger.debug("COPY inserted %d rows into %s", row_count, table_name)
    return row_count


def _prepare_for_copy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise DataFrame for TEXT-format COPY:
    - Nullable Int64 / Int32 → standard Python int or NaN
    - Boolean → 't'/'f'/None (PostgreSQL TEXT boolean literals)
    - Bytes columns → skip (should have been decoded before reaching here)
    - Replace literal tab/backslash/newline in string columns
    """
    df = df.copy()

    for col in df.columns:
        s = df[col]
        dtype_str = str(s.dtype).lower()

        # Nullable integers — convert to object with None for NA
        if dtype_str in ("int8", "int16", "int32", "int64",
                         "uint8", "uint16", "uint32", "uint64") and hasattr(s, "_mask"):
            df[col] = s.to_numpy(dtype=object, na_value=None)

        # Pandas BooleanDtype → 't'/'f'/None
        elif dtype_str == "boolean":
            df[col] = s.map({True: "t", False: "f", pd.NA: None}, na_action=None)

        # Standard bool
        elif dtype_str == "bool":
            df[col] = s.map({True: "t", False: "f"})

        # String / object — sanitise control characters and TEXT-format special chars
        elif s.dtype == object:
            df[col] = (
                s.astype(str)
                 .str.replace("\t",  " ", regex=False)
                 .str.replace("\\",  "\\\\", regex=False)
                 .str.replace("\r\n", " ",  regex=False)
                 .str.replace("\n",  " ",   regex=False)
                 .str.replace("\r",  " ",   regex=False)
                 .where(s.notna(), other=None)
            )

    return df


def save_failed_chunk(df: pd.DataFrame, chunk_idx: int, exception: Exception):
    """
    Save failed chunk as Parquet (preserves dtypes and binary columns).
    Also save a sidecar JSON with exception context.
    """
    FAILED_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = FAILED_DIR / f"failed_chunk_{chunk_idx}.parquet"
    meta_path = FAILED_DIR / f"failed_chunk_{chunk_idx}.json"

    try:
        df.to_parquet(parquet_path, index=False)
        logger.warning("Saved failed chunk %d to %s", chunk_idx, parquet_path)
    except Exception:
        logger.exception("Could not save failed chunk %d as Parquet", chunk_idx)
        try:
            safe_df = df.select_dtypes(exclude=["object"])
            safe_df.to_csv(FAILED_DIR / f"failed_chunk_{chunk_idx}_numeric.csv", index=False)
        except Exception:
            logger.exception("Could not save even numeric-only CSV for chunk %d", chunk_idx)

    try:
        with open(meta_path, "w") as fh:
            json.dump({
                "chunk_idx": chunk_idx,
                "rows": len(df),
                "columns": list(df.columns),
                "exception_type": type(exception).__name__,
                "exception_msg": str(exception),
                "traceback": traceback.format_exc(),
            }, fh, indent=2)
    except Exception:
        pass


def get_table_schema(engine, table_name: str) -> dict:
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return {}
    cols = inspector.get_columns(table_name)
    return {c["name"]: str(c["type"]).lower() for c in cols}
