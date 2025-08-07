import os
import io
import sys
import time
import glob
import inspect
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import psutil
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc, Engine, text
import duckdb
from pathlib import Path


load_dotenv()


# Constants
PARQUET_DIR = "./parquet_files"
CHUNK_SIZE = 10000 # Change According to your system
THROTTLE_DELAY = int(os.getenv("THROTTLE_DELAY", "5")) 
LOG_FILE = "ingestion.log"
METRICS_FILE = "metrics_log.csv"
FAILED_DIR = Path("failed_chunks")
FAILED_DIR.mkdir(exist_ok=True)
DB_TABLE = os.getenv("DB_TABLE")


DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")


def create_table_if_not_exists(engine, df, table_name):
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        df.head(0).to_sql(table_name, engine, index=False)  # Just headers
        logging.info(f"Table {table_name} created.")


def validate_folder(folder):
    """
    Validates that the folder exists and contains .parquet files.
    Raises ValueError instead of exiting directly.
    """
    if not os.path.exists(folder):
        raise ValueError(f"Folder {folder} does not exist.")
    files = glob.glob(os.path.join(folder, "*.parquet"))
    if not files:
        raise ValueError(f"No .parquet files found in {folder}.")
    return True


def get_table_columns(engine, table_name):
    """
    Retrieves column names from the target PostgreSQL table to validate schema match.
    """
    with engine.connect() as conn:
        result = conn.execute(f'''
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        ''')
        return [row[0] for row in result]


def safe_chunk_read(batch, file_path, i):
    """
    Converts a DuckDB record batch to a pandas DataFrame with safety checks.
    """
    try:
        chunk = batch.to_pandas()
        if chunk.empty:
            logging.warning(f"Empty chunk {i} from {file_path}")
            return None
        return chunk
    except Exception as e:
        logging.error(f"Failed to read chunk {i} from {file_path}: {e}")
        return None


def guard_required_columns(df, required_cols):
    """
    Ensures the required columns exist in the DataFrame before processing.
    Returns True if safe, False if missing.
    """
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        logging.warning(f"Missing required columns: {missing}")
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

            reader = con.execute(query).fetch_record_batch(chunk_size)
            while True:
                try:
                    chunk = reader.read_next_batch()
                    yield chunk.to_pandas()
                except StopIteration:
                    break # No more chunks


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


def log_resources(step):
    """
    Logs current CPU and RAM usage with context about the pipeline step.
    """
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    logging.info(f"[{step}] CPU: {cpu}%, RAM: {ram}%")


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
    required_columns = ["decimalLatitude", "decimalLongitude"]
    if not all(col in chunk.columns for col in required_columns):
        logging.warning(f"Skipping chunk missing lat/lon columns: {chunk.columns}")
    chunk = chunk.dropna(subset=required_columns).copy()


    if first_chunk:
        dropped_cols = set(original_cols) - set(cols_to_keep)
        logging.info("Dropped %d columns due to >%.0f%% nulls: %s", len(dropped_cols), null_threshold * 100, list(dropped_cols))
        logging.info("Retained %d columns: %s", len(cols_to_keep), cols_to_keep)

    return chunk, len(cols_to_keep)


def persist_metrics(metrics_history: List[Dict[str, Any]]) -> None:
    if not metrics_history:
        return

    df = pd.DataFrame(metrics_history)
    # A more efficient way to save metrics
    file_exists = os.path.exists(METRICS_FILE)
    df.to_csv(
        METRICS_FILE,
        mode="a", # 'a' for append
        index=False,
        header=not file_exists # Only write header if file is new
    )

def clean_null_bytes(df: pd.DataFrame) -> pd.DataFrame:
    def clean_value(val):
        if isinstance(val, (list, tuple, np.ndarray, pd.Series)):
            return val  # skip complex types for now

        # Convert bytes → string
        if isinstance(val, bytes):
            val = val.decode("utf-8", errors="replace")

        # Strip NULL bytes from strings
        if isinstance(val, str):
            val = val.replace('\x00', '')

        # Handle float NaN
        if isinstance(val, float) and np.isnan(val):
            return None

        return val

    return df.apply(lambda col: col.map(clean_value))


def copy_insert(engine, df, chunk_id, table_name):
    try:
        with engine.connect() as connection:
            output = io.StringIO()

            # Write as TSV
            df.to_csv(output, sep="\t", header=False, index=False)
            output.seek(0)

            cursor = connection.connection.cursor()
            cursor.copy_from(output, table_name, null="")

            connection.connection.commit()
    except Exception as e:
        logging.exception(f"Insert failed on chunk {chunk_id} — {e}")
        logging.debug("Offending chunk head:\n%s", df.head(3).to_string())


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

            processed_chunk = clean_null_bytes(processed_chunk)

            try:
                copy_insert(engine, processed_chunk, i, DB_TABLE)
                total_rows += rows_processed
            except Exception as e:
                logging.error("Chunk insert failed: %s", e)
                save_failed_chunk(processed_chunk, i)

            metrics = track_metrics(start_time, rows_processed, num_columns)
            metrics_history.append(metrics)

            logging.info("Processed %s rows. Total so far: %s", rows_processed, total_rows)
            time.sleep(THROTTLE_DELAY)

        persist_metrics(metrics_history)
        logging.info("Ingestion complete. Total rows: %s", total_rows)

        return metrics_history

    except Exception as e:
        logging.critical("Fatal ingestion error: %s", e, exc_info=True)
        return None


def save_failed_chunk(df: pd.DataFrame, chunk_id: int):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = FAILED_DIR / f"failed_chunk_{chunk_id}_{timestamp}.csv"
    df = clean_null_bytes(df)
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