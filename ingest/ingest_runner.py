import gc
import logging
import time
import sys
import os
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import threading

try:
    if sys.platform == "win32":
        import msvcrt
    else:
        import select
except ImportError:
    pass

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
    N_TRANSFORM_WORKERS,
    N_WRITE_THREADS,
    PARQUET_DIR,
    QUEUE_DEPTH,
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
from rich.prompt import Prompt

logger = logging.getLogger(__name__)


class StorageFullAbort(Exception):
    pass


def _check_stop_key() -> bool:
    try:
        if sys.platform == "win32":
            if "msvcrt" in sys.modules:
                if msvcrt.kbhit():
                    key = msvcrt.getch()
                    if key.lower() == b"q":
                        return True
        else:
            if "select" in sys.modules:
                dr, dw, de = select.select([sys.stdin], [], [], 0)
                if dr:
                    key = sys.stdin.read(1)
                    if key.lower() == "q":
                        return True
    except Exception:
        pass
    return False


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
                "INT_COLUMNS auto-populated from manifest (%d cols)", len(inferred)
            )
        return inferred
    return []


def _create_table_if_missing(engine, manifest=None) -> list:
    cols = get_table_columns(engine, DB_TABLE)
    if cols:
        return cols

    logger.info("Table '%s' missing — bootstrapping schema.", DB_TABLE)
    if manifest is not None:
        logger.info("Using Go manifest for schema.")
        schema_df = build_schema_df(manifest)
    else:
        logger.info("Inferring schema from parquet sample.")
        all_cols = get_full_schema_from_parquet(PARQUET_DIR)
        if not all_cols:
            raise RuntimeError("No parquet schema found")
        sample_iter = stream_parquet_chunks(PARQUET_DIR, max(100, CHUNK_SIZE))
        sample = next(sample_iter, None)
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


# ---------------------------------------------------------
# WORKER FUNCTIONS (Must be at top-level for Pickling)
# ---------------------------------------------------------
def transform_worker(args):
    """Executes in ProcessPoolExecutor (Bypasses GIL)"""
    chunk_idx, chunk, int_columns, bit_array_metadata = args
    df = process_chunk(
        chunk, int_columns=int_columns, bit_array_metadata=bit_array_metadata
    )
    null_fraction = float(df.isnull().values.mean()) if not df.empty else 0.0
    return chunk_idx, df, null_fraction


def writer_worker(engine, df, chunk_idx, db_cols, schema_map, prompt_lock):
    """Executes in ThreadPoolExecutor (Releases GIL during network I/O)"""
    if db_cols:
        df = df.reindex(columns=db_cols)
    try:
        df = coerce_df_to_schema(df, schema_map)
    except Exception:
        logger.exception("Schema coercion failed for chunk %d", chunk_idx)

    while True:
        try:
            copy_insert(engine, df, DB_TABLE)
            break
        except Exception as inner_e:
            error_str = str(inner_e).lower()
            # Detect storage full via pgcode 53100, class name, or common message strings
            is_full = (
                getattr(inner_e, "pgcode", "") == "53100"
                or "diskfull" in type(inner_e).__name__.lower()
                or any(
                    msg in error_str
                    for msg in [
                        "no space",
                        "disk full",
                        "storage full",
                        "53100",
                        "quota exceeded",
                        "project size limit",
                        "limit exceeded",
                        "could not extend file",
                    ]
                )
            )

            if is_full:
                with prompt_lock:
                    logger.error("Storage full error detected from database!")
                    choice = Prompt.ask(
                        f"\n[bold red]Database storage full on chunk {chunk_idx}![/bold red]\n"
                        "The project size limit may have been exceeded. "
                        "Please clear space or upgrade your plan.\n"
                        "[R]etry, [S]kip, or [A]bort?",
                        choices=["R", "S", "A", "r", "s", "a"],
                        default="A",
                    ).upper()

                    if choice == "R":
                        logger.info("Retrying chunk %d...", chunk_idx)
                        continue
                    elif choice == "A":
                        raise StorageFullAbort()
                    else:
                        # Skip: save the chunk data then re-raise so main loop counts it as failed
                        save_failed_chunk(df, chunk_idx, inner_e)
                        raise inner_e
            else:
                save_failed_chunk(df, chunk_idx, inner_e)
                raise inner_e
    return chunk_idx, len(df)


def ingest_data(
    selected_columns=None, bit_array_metadata=None, checkpoint=None, chunk_size=None
):
    engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)

    try:
        with engine.begin() as conn:
            conn.execute(text("SET synchronous_commit = OFF"))
            conn.execute(text("SET work_mem = '256MB'"))
            conn.execute(text("SET maintenance_work_mem = '512MB'"))
    except Exception:
        pass

    manifest = load_manifest(MANIFEST_FILE)
    eff_int_cols = _effective_int_columns(manifest)
    effective_chunk_size = chunk_size or CHUNK_SIZE
    db_cols = _create_table_if_missing(engine, manifest=manifest)
    schema_map = get_table_schema(engine, DB_TABLE)

    total_rows = 0
    metrics_history = []
    chunk_idx = 0
    failed_chunks = 0
    skip_chunks = 0

    if checkpoint:
        state = checkpoint.load("ingestion")
        if state and not state.get("completed"):
            skip_chunks = state.get("chunks_completed", 0)
            total_rows = state.get("total_rows_ingested", 0)
            chunk_idx = skip_chunks
            if skip_chunks > 0:
                logger.info(
                    "Resuming ingestion from chunk %d (%d rows).",
                    skip_chunks,
                    total_rows,
                )

    logger.info("[Press 'q' to gracefully stop ingestion at any time]")

    # 1. Architecture Setup
    # Process pool for CPU work, Thread pool for I/O writes
    N_WORKERS = N_TRANSFORM_WORKERS
    N_WRITERS = N_WRITE_THREADS
    logger.info(
        "Parallelism: %d transform workers (ProcessPool), %d write threads (ThreadPool), queue depth %d",
        N_WORKERS,
        N_WRITERS,
        QUEUE_DEPTH,
    )

    prompt_lock = threading.Lock()
    abort_requested = False

    # 2. Main Orchestration
    with (
        ProcessPoolExecutor(max_workers=N_WORKERS) as process_pool,
        ThreadPoolExecutor(max_workers=N_WRITERS) as writer_pool,
    ):
        chunks_iter = stream_parquet_chunks(
            PARQUET_DIR,
            effective_chunk_size,
            columns=selected_columns,
            skip_chunks=skip_chunks,
        )

        active_futures = {}

        try:
            while not abort_requested:
                if _check_stop_key():
                    logger.warning("User requested stop. Aborting gracefully...")
                    abort_requested = True
                    break

                # Backpressure: Only submit up to QUEUE_DEPTH chunks at a time
                while len(active_futures) < QUEUE_DEPTH and not abort_requested:
                    try:
                        chunk = next(chunks_iter)
                        args = (chunk_idx, chunk, eff_int_cols, bit_array_metadata)

                        # Submit to Process Pool
                        future = process_pool.submit(transform_worker, args)
                        active_futures[future] = {
                            "type": "transform",
                            "start_time": time.time(),
                            "chunk_idx": chunk_idx,
                        }
                        chunk_idx += 1
                    except StopIteration:
                        break  # No more chunks to read

                if not active_futures:
                    break  # All done

                # Wait for at least one future to complete
                done_futures = set()
                try:
                    for future in as_completed(active_futures.keys(), timeout=0.5):
                        done_futures.add(future)
                        break  # Process one by one as they finish
                except TimeoutError:
                    pass

                for future in done_futures:
                    task_info = active_futures.pop(future)
                    c_idx = task_info["chunk_idx"]

                    try:
                        if task_info["type"] == "transform":
                            # Transform finished — submit to writer thread pool
                            _, transformed_df, null_frac = future.result()
                            writer_future = writer_pool.submit(
                                writer_worker,
                                engine,
                                transformed_df,
                                c_idx,
                                db_cols,
                                schema_map,
                                prompt_lock,
                            )
                            active_futures[writer_future] = {
                                "type": "write",
                                "start_time": task_info["start_time"],
                                "chunk_idx": c_idx,
                                "null_fraction": null_frac,
                            }

                        elif task_info["type"] == "write":
                            # Write finished! Track metrics
                            _, rows_inserted = future.result()
                            metrics = track_metrics(
                                task_info["start_time"],
                                rows_inserted,
                                nulls=task_info.get("null_fraction", 0.0),
                            )
                            persist_metrics(metrics)
                            metrics_history.append(metrics)
                            total_rows += rows_inserted

                            logger.info(
                                "Chunk %d processed (%d rows). Total: %d",
                                c_idx,
                                rows_inserted,
                                total_rows,
                            )

                            if checkpoint:
                                checkpoint.save(
                                    "ingestion",
                                    {
                                        "completed": False,
                                        "chunks_completed": c_idx + 1,
                                        "total_rows_ingested": total_rows,
                                    },
                                )

                            _throttle_if_needed(
                                psutil.cpu_percent(interval=None),
                                psutil.virtual_memory().percent,
                            )

                    except StorageFullAbort:
                        raise
                    except Exception as e:
                        logger.exception("Failed processing for chunk %d", c_idx)
                        failed_chunks += 1
                        if FAIL_FAST:
                            raise

        except StorageFullAbort:
            abort_requested = True
            logger.error("Ingestion aborted due to storage full.")

    if checkpoint:
        checkpoint.save(
            "ingestion",
            {
                "completed": not abort_requested,
                "chunks_completed": chunk_idx,
                "total_rows_ingested": total_rows,
            },
        )

    logger.info(
        "Ingestion complete. Rows processed: %d, Failed chunks: %d",
        total_rows,
        failed_chunks,
    )
    return metrics_history
