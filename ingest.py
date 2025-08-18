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
from sqlalchemy import create_engine, exc, Engine, text, inspect
import duckdb
from pathlib import Path


load_dotenv()

# Constants
PARQUET_DIR = os.getenv("PARQUET_DIR", "ParquetData")
CHUNK_SIZE = 10000
THROTTLE_DELAY = int(os.getenv("THROTTLE_DELAY", "0"))
LOG_FILE = "ingestion.log"
METRICS_FILE = "metrics_log.csv"
FAILED_DIR = Path("failed_chunks")
FAILED_DIR.mkdir(exist_ok=True)
DB_TABLE = os.getenv("DB_TABLE", "TableName") 
CPU_THRESHOLD = 80
THROTTLE_DELAY_HIGH_CPU = 2

# --- Configurable settings for flexibility ---

# Column names for coordinate validation (not used by FHV data)
LAT_COLUMN = os.getenv("LAT_COLUMN", "latitude")
LON_COLUMN = os.getenv("LON_COLUMN", "longitude")

# Toggle for the coordinate validation step
VALIDATE_COORDS = os.getenv("VALIDATE_COORDS", "false").lower() == "true" 

# --- Restored Database Connection Check --- #
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")
try:
    db_engine = create_engine(DATABASE_URL)
    inspector = inspect(db_engine)
    print(f"Successfully connected to: {db_engine.url.render_as_string(hide_password=True)}")
    print(f"Tables found in public schema: {inspector.get_table_names(schema='public')}")
except Exception as e:
    print(f"Failed to connect or inspect database: {e}")
print()

def validate_folder(folder: str) -> bool:
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


def get_table_columns(engine: Engine, table_name: str) -> List[str]:
    """
    Retrieves column names from the target database table to validate schema match.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """))
        return [row[0] for row in result]


def stream_parquet_chunks(folder_path: str, chunk_size: int):
    """
    Reads data chunk by chunk, processing one Parquet file at a time
    to ensure memory is released between files.
    """
    # Find all Parquet files first
    all_files = glob.glob(os.path.join(folder_path, "*.parquet"))
    logging.info(f"Found {len(all_files)} Parquet files to process.")

    # Process one file at a time
    for file_path in all_files:
        logging.info(f"Streaming data from: {file_path}")
        try:
            # Use a new connection and reader for each file to release memory
            with duckdb.connect() as con:
                reader = con.execute(f"SELECT * FROM read_parquet('{file_path}')").fetch_record_batch(chunk_size)
                while True:
                    try:
                        chunk = reader.read_next_batch()
                        yield chunk.to_pandas()
                    except StopIteration:
                        break  # No more chunks in this file
        except Exception as e:
            logging.error(f"Failed to process file {file_path}: {e}")
            continue # Move to the next file

def configure_logging() -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout),
        ]
    )


def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows missing essential data (lat/lon), but only if validation is enabled.
    Column dropping is no longer done here to avoid schema conflicts.
    """
    # Check if coordinate validation should be skipped
    validate_coords = os.getenv("VALIDATE_COORDS", "true").lower() == "true"

    if not validate_coords:
        return chunk.copy() 

    required_columns = ["decimalLatitude", "decimalLongitude"]
    if not all(col in chunk.columns for col in required_columns):
        logging.warning(f"Skipping chunk: VALIDATE_COORDS is true but required columns are missing.")
        return pd.DataFrame() 
    
    return chunk.dropna(subset=required_columns).copy()

def enforce_integer_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds columns that should be integers but are floats (e.g., 123.0)
    and converts them to nullable integers (123).
    """
    df_int = df.copy()
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


def persist_metrics(metrics_history: List[Dict[str, Any]]) -> None:
    """Appends performance metrics for the run to a persistent CSV file."""
    if not metrics_history:
        return

    df = pd.DataFrame(metrics_history)
    file_exists = os.path.exists(METRICS_FILE)
    df.to_csv(
        METRICS_FILE,
        mode="a",
        index=False,
        header=not file_exists
    )


def clean_null_bytes(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans null bytes from data"""
    def clean_value(val):
        if isinstance(val, bytes):
            val = val.decode("utf-8", errors="replace")
        if isinstance(val, str):
            val = val.replace('\x00', '')
        if isinstance(val, float) and np.isnan(val):
            return None
        return val

    return df.map(clean_value)

def clean_special_characters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Safely replaces newline and tab characters in all string-like columns.
    """
    df_clean = df.copy()
    string_cols = df_clean.select_dtypes(include=['object']).columns
    
    for col in string_cols:
        # Check for string type within the lambda for safety
        df_clean[col] = df_clean[col].apply(
            lambda val: val.replace('\n', ' ').replace('\r', ' ') if isinstance(val, str) else val
        )
        
    return df_clean


def convert_geometry_to_wkt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts a 'geometry' column from WKB (binary) to WKT (text).
    """
    if 'geometry' not in df.columns:
        return df

    df_conv = df.copy()

    def to_wkt(binary_geom):
        if pd.isnull(binary_geom):
            return None
        try:
            return wkb.loads(binary_geom).wkt
        except Exception:
            return None # Gracefully handle parsing errors

    df_conv['geometry'] = df_conv['geometry'].apply(to_wkt)
    return df_conv


def copy_insert(engine: Engine, df: pd.DataFrame, chunk_id: int, table_name: str) -> None:
    """Uses PostgreSQL's fast COPY command to bulk-load a DataFrame."""
    try:
        quoted_columns = ",".join(f'"{col}"' for col in df.columns)
        
        with engine.connect() as connection:
            raw_connection = connection.connection
            with raw_connection.cursor() as cursor:
                output = io.StringIO()
                df.to_csv(
                    output,
                    sep="\t",
                    header=False,
                    index=False,
                    quoting=csv.QUOTE_MINIMAL,
                    escapechar='\\'
                )
                output.seek(0)
                
                cursor.copy_expert(f"""
                    COPY "{table_name}" ({quoted_columns}) FROM STDIN WITH (
                        FORMAT CSV,
                        DELIMITER E'\\t',
                        QUOTE '\"',
                        NULL '',
                        ESCAPE '\\'
                    )
                """, output)
            connection.commit()

    except Exception as e:
        logging.exception(f"Insert failed on chunk {chunk_id} — {e}")
        logging.debug("Offending chunk head:\n%s", df.head(3).to_string())
        raise e

def track_metrics(start_time: float, rows_processed: int) -> Dict[str, Any]:
    """Calculates and logs performance metrics for each processed chunk."""
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
    """Main orchestration function to run the entire ETL process."""
    validate_folder(PARQUET_DIR)

    total_rows = 0
    metrics_history = []

    try:
        logging.info("Fetching schema from database table '%s'...", DB_TABLE)
        db_columns = get_table_columns(engine, DB_TABLE)
        logging.info("Database table expects %d columns.", len(db_columns))

        chunks = stream_parquet_chunks(PARQUET_DIR, CHUNK_SIZE)

        for i, chunk in enumerate(chunks):
            start_time = time.time()

            # Perform all transformation and cleaning steps
            processed_chunk = process_chunk(chunk)
            rows_processed = len(processed_chunk)

            if rows_processed == 0:
                logging.info(f"Chunk {i} is empty after cleaning, skipping.")
                continue

            processed_chunk = enforce_integer_types(processed_chunk)
            processed_chunk = clean_special_characters(processed_chunk)
            processed_chunk = convert_geometry_to_wkt(processed_chunk)

            # **FIX**: Use reindex to align schema, preventing both errors and data loss.
            # This adds missing columns as NaN and drops extra columns.
            processed_chunk = processed_chunk.reindex(columns=db_columns)

            # Load the cleaned and aligned data into the database
            try:
                copy_insert(engine, processed_chunk, i, DB_TABLE)
                total_rows += rows_processed
            except Exception as e:
                logging.error("Chunk %d insert failed. Saving to failed_chunks.", i)
                save_failed_chunk(processed_chunk, i)

            metrics = track_metrics(start_time, rows_processed)
            metrics_history.append(metrics)
            logging.info("Processed chunk %d (%d rows). Total so far: %d", i, rows_processed, total_rows)
            
            # Throttle if CPU is high
            if psutil.cpu_percent(interval=None) > CPU_THRESHOLD:
                logging.warning("High CPU usage detected. Throttling for %d seconds.", THROTTLE_DELAY_HIGH_CPU)
                time.sleep(THROTTLE_DELAY_HIGH_CPU)
            else:
                time.sleep(THROTTLE_DELAY)

        persist_metrics(metrics_history)
        logging.info("Ingestion complete. Total rows: %d", total_rows)
        return metrics_history

    except Exception as e:
        logging.critical("Fatal ingestion error: %s", e, exc_info=True)
        return None


def save_failed_chunk(df: pd.DataFrame, chunk_id: int):
    """Saves a DataFrame to a CSV file in 'failed_chunks' for debugging."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = FAILED_DIR / f"failed_chunk_{chunk_id}_{timestamp}.csv"
    try:
        df_cleaned = clean_null_bytes(df)
        df_cleaned.to_csv(path, index=False)
        logging.warning(f"Saved failed chunk to: {path}")
    except Exception as e:
        logging.error(f"Could not save failed chunk {chunk_id} to {path}: {e}")


def get_full_schema_from_parquet(folder_path: str) -> List[str]:
    """
    **NEW FUNCTION**: Scans all parquet files to get the complete list of columns.
    """
    logging.info("Scanning all Parquet files to determine full schema...")
    with duckdb.connect() as con:
        parquet_glob = os.path.join(folder_path, "*.parquet")
        # DESCRIBE is very fast as it only reads metadata
        schema_df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_glob}')").fetchdf()
        column_names = schema_df['column_name'].tolist()
    logging.info(f"Found {len(column_names)} unique columns across all files.")
    return column_names

def run_plotting_script():
    """Checks for plot.py and runs it to generate the dashboard."""
    plot_script_path = Path("plot.py")
    if plot_script_path.is_file():
        logging.info("Found 'plot.py'. Attempting to generate dashboard...")
        try:
            # Use sys.executable to ensure we use the same python interpreter
            command = [sys.executable, str(plot_script_path)]
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True
            )
            logging.info("Successfully ran 'plot.py'.")
            # Print the output from plot.py (e.g., "Saved dashboard to: ...")
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to run 'plot.py'. Error:\n{e.stderr}")
        except FileNotFoundError:
            logging.error("Could not run 'plot.py'. Is python in your PATH?")
    else:
        logging.warning("'plot.py' not found. Skipping dashboard generation.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest OBIS data with metrics")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    configure_logging()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        db_engine = create_engine(DATABASE_URL)
        with db_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Database connection successful.")
    except exc.SQLAlchemyError as e:
        logging.critical("DB connection failed: %s", e)
        sys.exit(1)

    # **MODIFIED BOOTSTRAP STEP**
    try:
        inspector = inspect(db_engine)
        if not inspector.has_table(DB_TABLE):
            logging.warning(
                "Table '%s' not found. Creating it based on the full schema of all Parquet files.",
                DB_TABLE
            )
            
            # Step 1: Get the full, definitive list of columns from all files.
            all_columns = get_full_schema_from_parquet(PARQUET_DIR)
            
            # Step 2: Read the first chunk to infer data types after transformations.
            first_chunk_iter = stream_parquet_chunks(PARQUET_DIR, 100) # Need only a small sample
            first_chunk = next(first_chunk_iter, None)

            if first_chunk is not None:
                # Step 3: Apply transformations to infer final data types.
                processed_chunk = process_chunk(first_chunk)
                processed_chunk = enforce_integer_types(processed_chunk)
                processed_chunk = clean_special_characters(processed_chunk)
                processed_chunk = convert_geometry_to_wkt(processed_chunk)
                
                # Step 4: Create an empty DataFrame with the *full* schema to create the table.
                # This ensures all columns are created, even if not in the first chunk.
                final_schema_df = pd.DataFrame(columns=all_columns).astype(processed_chunk.dtypes)
                
                logging.info("Creating table '%s' with %d columns...", DB_TABLE, len(final_schema_df.columns))
                # Use head(0) to create table from schema without inserting data
                final_schema_df.head(0).to_sql(DB_TABLE, db_engine, if_exists='fail', index=False)
                logging.info("Table '%s' created successfully.", DB_TABLE)
            else:
                logging.error("No data found in source files, cannot create table.")
                sys.exit(1)

    except Exception as e:
        logging.critical("Failed during initial table setup. Error: %s", e, exc_info=True)
        sys.exit(1)
    
    # Run the main ingestion logic
    ingest_data(db_engine)

    # Automatically Run plot script
    run_plotting_script()