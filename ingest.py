import os
import io
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import psutil
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc, Engine, text
import duckdb
from pathlib import Path


# Constants
PARQUET_DIR = "./parquet_files"
CHUNK_SIZE = 2500 # Subject to Change
THROTTLE_DELAY = int(os.getenv("THROTTLE_DELAY", "5")) 
LOG_FILE = "ingestion.log"
METRICS_FILE = "metrics_log.csv"
FAILED_DIR = Path("failed_chunks")
FAILED_DIR.mkdir(exist_ok=True)


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")


def validate_folder(folder_path: str) -> bool:
    """
    Check if folder exists and contains .parquet files

    Equivalent of checking if an input file or directory exists
    before a data pipeline kicks off
    
    Basic sanity check
    """
    if not os.path.isdir(folder_path):
        logging.error("Directory not found: %s", folder_path)
        return False

    files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]
    if not files:
        logging.error("No .parquet files found in: %s", folder_path)
        return False

    return True


def stream_parquet_chunks(folder_path: str, chunk_size: int):
    """
    Uses DuckDB to read all .parquet files in folder
    then yields DataFrame slices (chunks) of `chunk_size` rows

    Memory-friendly vs loading full dataset in RAM
    """
    with duckdb.connect() as con:
        parquet_glob = os.path.join(folder_path, "*.parquet")

        query = f"SELECT * FROM read_parquet('{parquet_glob}')"
        df = con.execute(query).fetchdf()

        for start in range(0, len(df), chunk_size):
            yield df.iloc[start:start + chunk_size].copy()


def configure_logging() -> None:

    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(),
        ]
    )


def log_resources(step: str):
    logging.debug(f"[{step}] CPU: {psutil.cpu_percent()}%, RAM: {psutil.virtual_memory().percent}%")


def process_chunk(chunk: pd.DataFrame, null_threshold: float = 0.95, first_chunk: bool = False) -> Tuple[pd.DataFrame, int]:
    """
    Drops columns where % of NULLs > threshold.
    Removes rows missing lat/lon (required for spatial).
    Logs which columns are retained/dropped on first chunk only.

    Returns cleaned chunk and # of columns retained.
    """

    original_cols = chunk.columns.tolist()

    null_ratios = chunk.isnull().mean()
    cols_to_keep = null_ratios[null_ratios <= null_threshold].index.tolist()
    chunk = chunk.dropna(subset=["decimalLatitude", "decimalLongitude"]).copy()


    if first_chunk:
        dropped_cols = set(original_cols) - set(cols_to_keep)
        logging.info("Dropped %d columns due to >%.0f%% nulls: %s", len(dropped_cols), null_threshold * 100, list(dropped_cols))
        logging.info("Retained %d columns: %s", len(cols_to_keep), cols_to_keep)

    return chunk, len(cols_to_keep)


def persist_metrics(metrics_history: List[Dict[str, Any]]) -> None:
    if not metrics_history:
        return

    df = pd.DataFrame(metrics_history)
    df.to_csv(
        METRICS_FILE,
        mode="w",
        index=False,
        header=True
    )


def copy_insert(engine: Engine, df: pd.DataFrame) -> None:
    """
    Uses Postgres `COPY` (faster than row-by-row insert)
    to stream the chunk directly into the target table
    
    Will raise if the data format doesn't match schema
    """
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        cursor.copy_expert("COPY obis_data FROM STDIN WITH CSV", buffer)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def track_metrics(start_time: float, rows_processed: int, num_columns: int) -> Dict[str, Any]:
    """
    Logs how long the chunk took to process,
    the CPU usage, memory used, and rows/sec.
    
    Useful for cost analysis or benchmarking.
    """
    duration = time.time() - start_time

    metrics = {
        "timestamp": datetime.now().isoformat(),
        "processing_time": duration,
        "rows_processed": rows_processed,
        "rows_per_second": rows_processed / duration if duration > 0 else 0,
        "cpu_usage": psutil.cpu_percent(interval=None),
        "batch_size": CHUNK_SIZE,
        "memory_mb": psutil.virtual_memory().used / (1024 * 1024),
    }

    logging.info("Metrics: %s", metrics)
    return metrics


def ingest_data(engine: Engine) -> Optional[List[Dict[str, Any]]]:
    if not validate_folder(PARQUET_DIR):
        sys.exit(1)

    total_rows = 0
    metrics_history = []

    try:
        chunks = stream_parquet_chunks(PARQUET_DIR, CHUNK_SIZE)

        for i, chunk in enumerate(chunks):
            start_time = time.time()

            processed_chunk, num_columns = process_chunk(chunk, first_chunk=(i == 0))
            rows_processed = len(processed_chunk)

            if rows_processed == 0:
                continue  # Skip empty chunk

            try:
                copy_insert(engine, processed_chunk)
                total_rows += rows_processed

            except Exception as e:
                logging.error("Chunk insert failed: %s", e)
                save_failed_chunk(processed_chunk, i)

            metrics = track_metrics(start_time, rows_processed, num_columns)
            metrics_history.append(metrics)

            logging.info("Processed %s rows. Total so far: %s", rows_processed, total_rows)

            time.sleep(THROTTLE_DELAY)

            import gc
            del chunk
            del processed_chunk
            gc.collect()

        persist_metrics(metrics_history)
        logging.info("Ingestion complete. Total rows: %s", total_rows)

        return metrics_history

    except Exception as e:
        logging.critical("Fatal ingestion error: %s", e, exc_info=True)
        return None


def save_failed_chunk(df: pd.DataFrame, chunk_id: int):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = FAILED_DIR / f"failed_chunk_{chunk_id}_{timestamp}.csv"
    df.to_csv(path, index=False)
    logging.warning(f"Saved failed chunk to: {path}")


'''def fallback_insert(engine: Engine, df: pd.DataFrame):
    conn = engine.connect()
    for _, row in df.iterrows():
        try:
            conn.execute(text("INSERT INTO obis_data (...) VALUES (...)"), row.to_dict())
        except Exception as e:
            logging.error(f"Row insert failed: {e}")
    conn.close()
'''

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest OBIS .txt data with metrics")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    configure_logging()
    try:
        db_engine = create_engine(DATABASE_URL)

        # Validating DB connection prior
        with db_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except exc.SQLAlchemyError as e:
        logging.critical("DB connection failed: %s", e)
        sys.exit(1)

    ingest_data(db_engine)