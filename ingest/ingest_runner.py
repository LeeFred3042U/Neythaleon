import logging
import time

from sqlalchemy import create_engine
import pandas as pd
import psutil

from ingest.config import (
    DATABASE_URL,
    DB_TABLE,
    PARQUET_DIR,
    CHUNK_SIZE,
    INT_COLUMNS,
    CPU_THRESHOLD,
    THROTTLE_DELAY_HIGH_CPU,
    THROTTLE_DELAY,
)

from ingest.db_utils import copy_insert, get_table_columns, create_table_from_df, save_failed_chunk, get_table_schema
from ingest.parquet_utils import stream_parquet_chunks, get_full_schema_from_parquet
from ingest.transform import process_chunk
from ingest.metrics import track_metrics, persist_metrics
from ingest.scheme_utils import coerce_df_to_schema

logger = logging.getLogger(__name__)


def _create_table_if_missing(engine):
    cols = get_table_columns(engine, DB_TABLE)
    if cols:
        return cols

    logger.info("Table '%s' missing. Inferring schema from parquet files...", DB_TABLE)
    all_cols = get_full_schema_from_parquet(PARQUET_DIR)
    if not all_cols:
        raise RuntimeError("No parquet schema found to create table")

    # get a small sample to infer dtypes after processing
    sample_iter = stream_parquet_chunks(PARQUET_DIR, max(100, CHUNK_SIZE))
    sample = next(sample_iter, None)
    if sample is None:
        raise RuntimeError("No data found in parquet files to infer schema")

    processed = process_chunk(sample, int_columns=INT_COLUMNS)

    # Build empty DataFrame with columns from all_cols and dtypes from processed where available
    schema_df = pd.DataFrame(columns=all_cols)
    for c in processed.columns:
        if c in schema_df.columns:
            try:
                schema_df[c] = schema_df[c].astype(processed[c].dtype)
            except Exception:
                pass

    create_table_from_df(engine, DB_TABLE, schema_df)
    return get_table_columns(engine, DB_TABLE)


def ingest_data():
    engine = create_engine(DATABASE_URL)

    # Ensure table exists (and return final db columns)
    db_cols = _create_table_if_missing(engine)

    total_rows = 0
    metrics_history = []

    # fetch schema_map once (outside the loop)
    schema_map = get_table_schema(engine, DB_TABLE)  # use the new function

    for i, chunk in enumerate(stream_parquet_chunks(PARQUET_DIR, CHUNK_SIZE)):
        start = time.time()
        try:
            df = process_chunk(chunk, int_columns=INT_COLUMNS)

            # Align schema
            if db_cols:
                df = df.reindex(columns=db_cols)

            
            # inside loop, after df = df.reindex(columns=db_cols)
            try:
                df = coerce_df_to_schema(df, schema_map)
            except Exception:
                logger.exception("Schema coercion failed for chunk %d; proceeding with original df", i)
            
            # Insert
            copy_insert(engine, df, DB_TABLE)

            metrics = track_metrics(start, len(df))
            persist_metrics(metrics)
            metrics_history.append(metrics)

            total_rows += len(df)
            logger.info("Chunk %d processed (%d rows). Total: %d", i, len(df), total_rows)

            # Throttle on high CPU
            if psutil.cpu_percent(interval=None) > CPU_THRESHOLD:
                logger.warning("High CPU detected. Sleeping %ss", THROTTLE_DELAY_HIGH_CPU)
                time.sleep(THROTTLE_DELAY_HIGH_CPU)
            else:
                if THROTTLE_DELAY:
                    time.sleep(THROTTLE_DELAY)

        except Exception as e:
            logger.exception("Failed to ingest chunk %d: %s", i, e)
            try:
                save_failed_chunk(df if 'df' in locals() else pd.DataFrame(), i)
            except Exception:
                logger.exception("Could not save failed chunk %d", i)
            continue

    logger.info("Ingestion complete. Rows processed: %d", total_rows)
    return metrics_history