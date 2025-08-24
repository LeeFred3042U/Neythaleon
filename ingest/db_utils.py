import io
import csv
import logging
from typing import List
from sqlalchemy import inspect, text
import pandas as pd

from ingest.config import FAILED_DIR
from ingest.transform import clean_null_bytes

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
    """Create an empty table using df.head(0).to_sql(...)
    Caller should ensure df has final dtypes.
    """
    logger.info(
        "Creating table '%s' with %d columns", 
        table_name, 
        len(df.columns)
        )
    df.head(0).to_sql(table_name, engine, if_exists="fail", index=False)
    logger.info("Created table '%s'", table_name)


def copy_insert(engine, df: pd.DataFrame, table_name: str):
    """Fast COPY-based insert. Falls back to to_sql if COPY not possible.
    Requires Postgres-like DB.
    """
    if df.empty:
        logger.debug("No rows to insert into %s", table_name)
        return

    try:
        with engine.begin() as conn:
            raw_conn = conn.connection
            with raw_conn.cursor() as cur:
                output = io.StringIO()
                # Use TSV with quoting to be robust; adapt as needed
                df.to_csv(
                    output, 
                    sep="\t",
                    header=False, 
                    index=False, 
                    quoting=csv.QUOTE_MINIMAL, 
                    escapechar='\\', 
                    na_rep=''
                )
                output.seek(0)
                quoted_columns = ",".join(f'"{c}"' for c in df.columns)
                sql = f"COPY {table_name} ({quoted_columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL '')"
                cur.copy_expert(sql, output)
        logger.debug("Inserted %d rows into %s (COPY)", len(df), table_name)
    except Exception as e:
        logger.warning("COPY failed, falling back to pandas.to_sql: %s", e)
        try:
            df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
            logger.debug("Inserted %d rows into %s (to_sql)", len(df), table_name)
        except Exception:
            logger.exception("Both COPY and to_sql failed")
            raise


def save_failed_chunk(df: pd.DataFrame, chunk_id: int):
    try:
        df_clean = clean_null_bytes(df)
        path = FAILED_DIR / f"failed_chunk_{chunk_id}.csv"
        df_clean.to_csv(path, index=False)
        logger.warning("Saved failed chunk to %s", path)
    except Exception:
        logger.exception("Failed to save failed chunk %s", chunk_id)


def get_table_schema(engine, table_name: str) -> dict:
    """Return a mapping column_name -> textual SQL type (lowercased)."""
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return {}
    cols = inspector.get_columns(table_name)
    return {c["name"]: str(c["type"]).lower() for c in cols}