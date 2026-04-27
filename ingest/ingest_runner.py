import gc
import itertools
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import psutil
from sqlalchemy import create_engine

from ingest.config import (
    CPU_THRESHOLD,
    CHUNK_SIZE,
    DATABASE_URL,
    DB_TABLE,
    INT_COLUMNS,
    MANIFEST_FILE,
    PARALLELISM,
    PARQUET_DIR,
    RAM_THRESHOLD,
    THROTTLE_DELAY,
    THROTTLE_DELAY_HIGH_CPU,
)
from ingest.db_utils import (
    copy_insert,
    create_table_from_df,
    get_table_columns,
    get_table_schema,
    save_failed_chunk,
)
from ingest.manifest import build_schema_df, infer_int_columns, load_manifest
from ingest.metrics import persist_metrics, track_metrics
from ingest.parquet_utils import get_full_schema_from_parquet, stream_parquet_chunks
from ingest.scheme_utils import coerce_df_to_schema
from ingest.transform import process_chunk

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _batched(iterable, n: int):
    """Yield successive non-overlapping batches of size n from an iterable."""
    it = iter(iterable)
    while batch := list(itertools.islice(it, n)):
        yield batch


def _transform_one(chunk: pd.DataFrame, int_columns: list) -> pd.DataFrame:
    """Stateless transform — safe for concurrent execution in a thread pool."""
    return process_chunk(chunk, int_columns=int_columns)


def _throttle_if_needed(cpu_usage: float, mem_usage: float) -> None:
    """Sleep (and optionally GC) when system resources are under pressure."""
    if cpu_usage > CPU_THRESHOLD:
        logger.warning(
            "High CPU (%.1f%%). Sleeping %.1fs", cpu_usage, THROTTLE_DELAY_HIGH_CPU
        )
        time.sleep(THROTTLE_DELAY_HIGH_CPU)
    elif mem_usage > RAM_THRESHOLD:
        logger.warning(
            "High RAM (%.1f%%). Running GC then sleeping %.1fs",
            mem_usage,
            THROTTLE_DELAY_HIGH_CPU,
        )
        gc.collect()
        time.sleep(THROTTLE_DELAY_HIGH_CPU)
    elif THROTTLE_DELAY:
        time.sleep(THROTTLE_DELAY)


# ---------------------------------------------------------------------------
# Table bootstrap
# ---------------------------------------------------------------------------

def _effective_int_columns(manifest) -> list:
    """
    Merge env-configured INT_COLUMNS with manifest-inferred integer columns.
    Explicit env config always wins.  If INT_COLUMNS is empty and a manifest
    is present, auto-populate from INT32/INT64 columns with density >= 50%.
    """
    if INT_COLUMNS:
        return INT_COLUMNS
    if manifest is not None:
        inferred = infer_int_columns(manifest, density_threshold=50.0)
        if inferred:
            logger.info(
                "INT_COLUMNS auto-populated from manifest (%d cols): %s%s",
                len(inferred),
                inferred[:10],
                "..." if len(inferred) > 10 else "",
            )
        return inferred
    return []


def _create_table_if_missing(engine, manifest=None) -> list:
    """
    Ensure the target table exists and return its ordered column list.

    Schema resolution order:
      1. Table already in DB   -> return existing columns unchanged.
      2. Go manifest available -> build schema from manifest (zero sample I/O).
      3. Fallback              -> stream a small sample chunk and infer dtypes.
    """
    cols = get_table_columns(engine, DB_TABLE)
    if cols:
        return cols

    logger.info("Table '%s' missing — bootstrapping schema.", DB_TABLE)

    if manifest is not None:
        logger.info("Using Go manifest for schema (no sample chunk needed).")
        schema_df = build_schema_df(manifest)
    else:
        logger.info("No manifest present; inferring schema from parquet sample.")
        all_cols = get_full_schema_from_parquet(PARQUET_DIR)
        if not all_cols:
            raise RuntimeError("No parquet schema found to create table")

        sample_iter = stream_parquet_chunks(PARQUET_DIR, max(100, CHUNK_SIZE))
        sample = next(sample_iter, None)
        if sample is None:
            raise RuntimeError("No data found in parquet files to infer schema")

        processed = process_chunk(sample, int_columns=_effective_int_columns(manifest))

        schema_df = pd.DataFrame(columns=all_cols)
        for c in processed.columns:
            if c in schema_df.columns:
                try:
                    schema_df[c] = schema_df[c].astype(processed[c].dtype)
                except Exception:
                    pass

    create_table_from_df(engine, DB_TABLE, schema_df)
    return get_table_columns(engine, DB_TABLE)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def ingest_data():
    """
    Stream every parquet file in PARQUET_DIR into the target PostgreSQL table.

    Parallelism model
    -----------------
    Chunks are read and transformed in parallel batches of size PARALLELISM
    using a ThreadPoolExecutor (overlaps I/O-bound parquet reads with
    CPU-bound transforms).  DB inserts remain serial — one connection, in
    order, no contention.
    """
    engine = create_engine(DATABASE_URL)

    # Load Go manifest if available (returns None gracefully when absent)
    manifest = load_manifest(MANIFEST_FILE)

    eff_int_cols = _effective_int_columns(manifest)

    db_cols = _create_table_if_missing(engine, manifest=manifest)

    # Fetch DB schema map once — used by coerce_df_to_schema inside the loop
    schema_map = get_table_schema(engine, DB_TABLE)

    total_rows = 0
    metrics_history = []
    chunk_idx = 0  # monotonic counter across all batches

    with ThreadPoolExecutor(max_workers=PARALLELISM) as executor:
        for batch in _batched(stream_parquet_chunks(PARQUET_DIR, CHUNK_SIZE), PARALLELISM):

            # Submit transforms for every chunk in this batch concurrently
            # Use a list to preserve submission order for ordered inserts
            futures = [
                executor.submit(_transform_one, chunk, eff_int_cols)
                for chunk in batch
            ]

            for future in futures:
                start = time.time()
                df = None
                try:
                    df = future.result()

                    if db_cols:
                        df = df.reindex(columns=db_cols)

                    try:
                        df = coerce_df_to_schema(df, schema_map)
                    except Exception:
                        logger.exception(
                            "Schema coercion failed for chunk %d; proceeding with original df",
                            chunk_idx,
                        )

                    copy_insert(engine, df, DB_TABLE)

                    metrics = track_metrics(start, len(df))
                    persist_metrics(metrics)
                    metrics_history.append(metrics)

                    total_rows += len(df)
                    logger.info(
                        "Chunk %d processed (%d rows). Total: %d",
                        chunk_idx,
                        len(df),
                        total_rows,
                    )

                    _throttle_if_needed(
                        psutil.cpu_percent(interval=None),
                        psutil.virtual_memory().percent,
                    )

                except Exception as e:
                    logger.exception("Failed to ingest chunk %d: %s", chunk_idx, e)
                    try:
                        save_failed_chunk(df if df is not None else pd.DataFrame(), chunk_idx)
                    except Exception:
                        logger.exception("Could not save failed chunk %d", chunk_idx)

                finally:
                    chunk_idx += 1

    logger.info("Ingestion complete. Rows processed: %d", total_rows)
    return metrics_history