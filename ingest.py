import os
import io
import sys
import csv
import time
import glob
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import psutil
import numpy as np
import pandas as pd
from shapely import wkb
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc, Engine, text
import duckdb
from pathlib import Path


load_dotenv()


# Constants
PARQUET_DIR = "./parquet_files"
CHUNK_SIZE = 10000 # Change According to your system
THROTTLE_DELAY = int(os.getenv("THROTTLE_DELAY", "0"))
LOG_FILE = "ingestion.log"
METRICS_FILE = "metrics_log.csv"
FAILED_DIR = Path("failed_chunks")
FAILED_DIR.mkdir(exist_ok=True)
DB_TABLE = os.getenv("DB_TABLE")
CPU_THRESHOLD = 60  
THROTTLE_DELAY_HIGH_CPU = 2


DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")


# Checks if the source directory exists and contains .parquet files before starting
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


# Fetches the exact column names and their order from the target database table
def get_table_columns(engine, table_name):
    """
    Retrieves column names from the target PostgreSQL table to validate schema match.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f'''
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        '''))
        return [row[0] for row in result]


# Memory-efficiently reads all Parquet files in a directory and yields data chunks.
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


# Sets up the logging configuration to output to both a file and the console.
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



# Performs initial data shaping by dropping sparse columns and rows with null coordinates.
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

# Converts columns that should be integers from float (e.g., 123.0) to a nullable integer.
def enforce_integer_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds columns that should be integers but are floats (e.g., 123.0)
    and converts them to nullable integers (123).
    """
    df_int = df.copy()
    
    # List of columns that should be whole numbers
    integer_columns = [
        'aphiaid', 'classid', 'familyid', 'genusid', 'infraorderid',
        'kingdomid', 'orderid', 'phylumid', 'speciesid', 'subclassid',
        'subfamilyid', 'suborderid', 'subphylumid', 'subsectionid',
        'superclassid', 'superfamilyid', 'superorderid', 'year', 'day', 'month',
        'individualCount'
    ]
    
    for col in integer_columns:
        if col in df_int.columns:
            # Coerce errors will turn non-numeric values into NaT/NaN
            # 'Int64' (capital I) is a pandas type that supports <NA>
            df_int[col] = pd.to_numeric(df_int[col], errors='coerce').astype('Int64')
            
    return df_int


# Appends performance metrics for the run to a persistent CSV file
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


# Cleans null bytes from data
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


# Removes newline and carriage return characters from string fields to prevent formatting errors
def clean_special_characters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Safely replaces newline and tab characters in all string-like columns.
    It ignores non-string values like numbers or NaNs.
    """
    df_clean = df.copy()
    
    # Select columns that might contain strings
    string_cols = df_clean.select_dtypes(include=['object']).columns
    
    for col in string_cols:
        # Use .apply() to handle each value individually and safely
        df_clean[col] = df_clean[col].apply(
            lambda val: val.replace('\n', ' ').replace('\r', ' ') if isinstance(val, str) else val
        )
        
    return df_clean


# Converts binary geometry data (WKB) into human-readable text (WKT) for safe insertion
def convert_geometry_to_wkt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts a 'geometry' column from WKB (binary) to WKT (text).
    Handles errors gracefully if some rows have invalid geometry.
    """
    if 'geometry' not in df.columns:
        return df

    df_conv = df.copy()

    def to_wkt(binary_geom):
        if binary_geom is None:
            return None
        try:
            # Parse the binary data and return its WKT representation
            return wkb.loads(binary_geom).wkt
        except Exception:
            # If parsing fails, return None (or an empty string)
            return None

    df_conv['geometry'] = df_conv['geometry'].apply(to_wkt)
    return df_conv


# Uses PostgreSQL's fast COPY command to bulk-load a DataFrame into the database
def copy_insert(engine, df, chunk_id, table_name):
    try:
        with engine.connect() as connection:
            output = io.StringIO()

            df.to_csv(
                output,
                sep="\t",
                header=False,
                index=False,
                quoting=csv.QUOTE_MINIMAL
            )
            output.seek(0)

            cursor = connection.connection.cursor()
            cursor.copy_expert(f"""
                COPY {table_name} FROM STDIN WITH (
                    FORMAT CSV,
                    DELIMITER E'\\t',
                    QUOTE '\"',
                    NULL ''
                )
            """, output)

            # The commit is handled by the 'with engine.connect()' block on successful exit

    except Exception as e:
        # Re-raising the exception is crucial to let the calling function know about the failure.
        logging.exception(f"Insert failed on chunk {chunk_id} — {e}")
        logging.debug("Offending chunk head:\n%s", df.head(3).to_string())
        raise e

# Calculates and logs performance metrics for each processed chunk
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


# Main orchestration function to run the entire ETL process
def ingest_data(engine: Engine) -> Optional[List[Dict[str, Any]]]:
    if not validate_folder(PARQUET_DIR):
        sys.exit(1)

    total_rows = 0
    metrics_history = []

    try:
        # Get the definitive list of columns from the database to ensure alignment
        logging.info("Fetching schema from database table '%s'...", DB_TABLE)
        db_columns = get_table_columns(engine, DB_TABLE)
        logging.info("Database table expects %d columns.", len(db_columns))

        chunks = stream_parquet_chunks(PARQUET_DIR, CHUNK_SIZE)

        for i, chunk in enumerate(chunks):
            start_time = time.time()

            # Perform all transformation and cleaning steps
            processed_chunk, num_columns = process_chunk(chunk, first_chunk=(i == 0))
            rows_processed = len(processed_chunk)

            if rows_processed == 0:
                continue

            processed_chunk = enforce_integer_types(processed_chunk)
            processed_chunk = clean_special_characters(processed_chunk)
            processed_chunk = convert_geometry_to_wkt(processed_chunk)

            # Force the DataFrame to match the database schema exactly
            try:
                processed_chunk = processed_chunk[db_columns]
            except KeyError as e:
                logging.error(
                    "DataFrame is missing a column that the database requires: %s", e
                )
                save_failed_chunk(processed_chunk, i)
                continue # Skip to the next chunk

            # Load the cleaned and aligned data into the database
            try:
                copy_insert(engine, processed_chunk, i, DB_TABLE)
                total_rows += rows_processed
            except Exception as e:
                logging.error("Chunk insert failed: %s", e)
                save_failed_chunk(processed_chunk, i)

            # Track performance and pause briefly to avoid overwhelming resources
            metrics = track_metrics(start_time, rows_processed, len(db_columns))
            metrics_history.append(metrics)
            logging.info("Processed %d rows. Total so far: %d", rows_processed, total_rows)
            time.sleep(THROTTLE_DELAY)

        persist_metrics(metrics_history)
        logging.info("Ingestion complete. Total rows: %d", total_rows)
        return metrics_history

    except Exception as e:
        logging.critical("Fatal ingestion error: %s", e, exc_info=True)
        return None

# Saves a DataFrame to a CSV file in a dedicated 'failed_chunks' directory for debugging
def save_failed_chunk(df: pd.DataFrame, chunk_id: int):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = FAILED_DIR / f"failed_chunk_{chunk_id}_{timestamp}.csv"
    # Note: Calling clean_null_bytes here might be redundant if the main pipeline is robust
    df = clean_null_bytes(df)
    df.to_csv(path, index=False)
    logging.warning(f"Saved failed chunk to: {path}")


# Main execution block that runs when the script is called directly
if __name__ == "__main__":
    import argparse
    from sqlalchemy import inspect as sa_inspect

    parser = argparse.ArgumentParser(description="Ingest OBIS data with metrics")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    # Set up logging and establish a database connection
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    configure_logging()

    try:
        db_engine = create_engine(DATABASE_URL)
        with db_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except exc.SQLAlchemyError as e:
        logging.critical("DB connection failed: %s", e)
        sys.exit(1)

    # Bootstrap Step: Check if the target table exists and create it if not
    try:
        inspector = sa_inspect(db_engine)
        if not inspector.has_table(DB_TABLE):
            logging.warning(
                "Table '%s' not found. Will attempt to create it from the first data chunk.",
                DB_TABLE
            )
            
            # Read only the first chunk to define the table's schema.
            first_chunk_iter = stream_parquet_chunks(PARQUET_DIR, CHUNK_SIZE)
            first_chunk = next(first_chunk_iter, None)

            if first_chunk is not None:
                # Apply ALL cleaning steps to get the final, correct schema.
                processed_chunk, _ = process_chunk(first_chunk, first_chunk=True)
                processed_chunk = enforce_integer_types(processed_chunk)
                processed_chunk = clean_special_characters(processed_chunk)
                processed_chunk = convert_geometry_to_wkt(processed_chunk)
                
                # Create the table using the cleaned chunk's structure.
                logging.info("Creating table '%s' with %d columns...", DB_TABLE, len(processed_chunk.columns))
                processed_chunk.head(0).to_sql(DB_TABLE, db_engine, if_exists='fail', index=False)
                logging.info("Table '%s' created successfully.", DB_TABLE)
            else:
                logging.error("No data found in source files, cannot create table.")
                sys.exit(1)

    except Exception as e:
        logging.critical("Failed to create table. Error: %s", e, exc_info=True)
        sys.exit(1)
    
    # Run the main ingestion logic
    ingest_data(db_engine)