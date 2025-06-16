import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import psutil
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc, Engine, text
import duckdb

# Constants
PARQUET_DIR = "./parquet_files"
CHUNK_SIZE = 1000
THROTTLE_DELAY = int(os.getenv("THROTTLE_DELAY", "5")) 
LOG_FILE = "ingestion.log"
METRICS_FILE = "metrics_log.csv"


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")


def validate_folder(folder_path: str) -> bool:
    if not os.path.isdir(folder_path):
        logging.error("Directory not found: %s", folder_path)
        return False

    files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]
    if not files:
        logging.error("No .parquet files found in: %s", folder_path)
        return False

    return True


def stream_parquet_chunks(folder_path: str, chunk_size: int):
    con = duckdb.connect()
    parquet_glob = os.path.join(folder_path, "*.parquet")

    query = f"SELECT * FROM read_parquet('{parquet_glob}')"
    cursor = con.execute(query)

    column_names = [desc[0] for desc in cursor.description]

    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break

        yield pd.DataFrame(rows, columns=column_names)


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


def track_metrics(start_time: float, rows_processed: int) -> Dict[str, Any]:

    """Track and log performance metrics."""
    duration = time.time() - start_time

    metrics = {
        "timestamp": datetime.now().isoformat(),
        "processing_time": duration,
        "rows_processed": rows_processed,
        "rows_per_second": rows_processed / duration if duration > 0 else 0,
        "cpu_usage": psutil.cpu_percent(interval=1),
        "batch_size": CHUNK_SIZE,
        "memory_mb": psutil.virtual_memory().used / (1024 * 1024)
    }

    logging.info("Metrics: %s", metrics)
    return metrics



def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:

    """Remove rows missing lat/lon data."""
    return chunk.dropna(subset=["decimalLatitude", "decimalLongitude"])


def insert_row(row: pd.Series, engine: Engine) -> bool:

    """Fallback: Insert single row if batch fails."""
    try:
        pd.DataFrame([row]).to_sql("obis_data", con=engine, if_exists="append", index=False)
        return True
    except exc.SQLAlchemyError as e:
        logging.error("Row insert failed: %s | Data: %s", e, row.to_dict())
        return False


def persist_metrics(metrics_history: List[Dict[str, Any]]) -> None:

    """Save metrics to persistent storage."""
    try:
        pd.DataFrame(metrics_history).to_csv(METRICS_FILE, index=False)
    except (IOError, pd.errors.EmptyDataError) as e:
        logging.error("Failed to persist metrics: %s", e)


def ingest_data(engine: Engine) -> Optional[List[Dict[str, Any]]]:

    """Main ingestion function with comprehensive error handling."""
    if not validate_folder(PARQUET_DIR):
        sys.exit(1)

    total_rows = 0
    metrics_history = []

    try:
        chunks = stream_parquet_chunks(PARQUET_DIR, CHUNK_SIZE)
        for chunk in chunks:

            start_time = time.time()
            processed_chunk = process_chunk(chunk)
            rows_processed = len(processed_chunk)

            try:
                processed_chunk.to_sql(
                    "obis_data",
                    con=engine,
                    if_exists="append",
                    index=False,
                    method="multi"
                )
                total_rows += rows_processed

            except exc.SQLAlchemyError as e:
                logging.error("Chunk insert failed: %s", e)
                logging.info("Attempting row-by-row fallback...")
                successful_rows = sum(
                    insert_row(row, engine)
                    for _, row in processed_chunk.iterrows()
                )
                total_rows += successful_rows

            metrics_history.append(track_metrics(start_time, rows_processed))
            logging.info("Processed %s rows. Total: %s", rows_processed, total_rows)
            time.sleep(THROTTLE_DELAY)

        persist_metrics(metrics_history)
        logging.info("Ingestion complete. Total rows: %s", total_rows)
        return metrics_history

    except Exception as e:
        logging.critical("Fatal ingestion error: %s", e, exc_info=True)
        return None


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
        # Validate DB connection early
        with db_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except exc.SQLAlchemyError as e:
        logging.critical("DB connection failed: %s", e)
        sys.exit(1)

    ingest_data(db_engine)