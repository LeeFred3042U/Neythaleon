import gc
import itertools
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import psutil
from sqlalchemy import create_engine, text

from ingest.config import (
    CPU_THRESHOLD,
    CHUNK_SIZE,
    DATABASE_URL,
    DB_TABLE,
    FAIL_FAST,
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


def _batched(iterable, n: int):
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, n))
        if not batch:
            break
        yield batch


def _transform_one(chunk: pd.DataFrame, int_columns: list,
                    bit_array_metadata=None) -> pd.DataFrame:
    return process_chunk(chunk, int_columns=int_columns,
                         bit_array_metadata=bit_array_metadata)


def _throttle_if_needed(cpu_usage: float, mem_usage: float) -> None:
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


def _effective_int_columns(manifest) -> list:
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


def ingest_data(selected_columns=None, bit_array_metadata=None,
                checkpoint=None, chunk_size=None):
    """
    Run the ingestion pipeline.
    
    Args:
        selected_columns: list of column names to ingest (None = all)
        bit_array_metadata: BitArrayMetadata for decoding bit-array columns
        checkpoint: Checkpoint instance for progress tracking
        chunk_size: override for CHUNK_SIZE
    """
    engine = create_engine(DATABASE_URL)

    # Set session-level PG optimisations for bulk load
    try:
        with engine.begin() as conn:
            conn.execute(text("SET synchronous_commit = OFF"))
            conn.execute(text("SET work_mem = '256MB'"))
            conn.execute(text("SET maintenance_work_mem = '512MB'"))
    except Exception:
        logger.warning("Could not set bulk-load PG session params (non-fatal)")

    manifest = load_manifest(MANIFEST_FILE)

    eff_int_cols = _effective_int_columns(manifest)
    effective_chunk_size = chunk_size or CHUNK_SIZE

    db_cols = _create_table_if_missing(engine, manifest=manifest)

    schema_map = get_table_schema(engine, DB_TABLE)

    total_rows = 0
    metrics_history = []
    chunk_idx = 0
    failed_chunks = 0

    # Resume support via checkpoint
    skip_chunks = 0
    if checkpoint:
        state = checkpoint.load("ingestion")
        if state and not state.get("completed"):
            skip_chunks = state.get("chunks_completed", 0)
            total_rows = state.get("total_rows_ingested", 0)
            chunk_idx = skip_chunks
            if skip_chunks > 0:
                logger.info("Resuming ingestion from chunk %d (%d rows already ingested)",
                           skip_chunks, total_rows)

    with ThreadPoolExecutor(max_workers=PARALLELISM) as executor:
        chunks = stream_parquet_chunks(
            PARQUET_DIR, effective_chunk_size,
            columns=selected_columns,
            skip_chunks=skip_chunks
        )
        for batch in _batched(chunks, PARALLELISM):

            futures = [
                executor.submit(_transform_one, chunk, eff_int_cols,
                               bit_array_metadata)
                for chunk in batch
            ]

            # Process futures in order (not as_completed) to prevent concurrent DB writes
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

                    # Update checkpoint after each successful chunk
                    if checkpoint:
                        checkpoint.save("ingestion", {
                            "completed": False,
                            "chunks_completed": chunk_idx + 1,
                            "total_rows_ingested": total_rows,
                        })

                    _throttle_if_needed(
                        psutil.cpu_percent(interval=None),
                        psutil.virtual_memory().percent,
                    )

                except Exception as e:
                    logger.exception("COPY failed for chunk %d — saving and continuing", chunk_idx)
                    try:
                        save_failed_chunk(
                            df if df is not None else pd.DataFrame(),
                            chunk_idx, e
                        )
                    except Exception:
                        logger.exception("Could not save failed chunk %d", chunk_idx)
                    failed_chunks += 1

                    if FAIL_FAST:
                        logger.error("FAIL_FAST=true — aborting ingestion")
                        raise

                finally:
                    chunk_idx += 1

    # Mark ingestion complete in checkpoint
    if checkpoint:
        checkpoint.save("ingestion", {
            "completed": True,
            "chunks_completed": chunk_idx,
            "total_rows_ingested": total_rows,
        })

    logger.info(
        "Ingestion complete. Rows processed: %d, Failed chunks: %d",
        total_rows, failed_chunks
    )
    return metrics_history
