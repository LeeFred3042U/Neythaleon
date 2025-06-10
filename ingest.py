import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import psutil
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc, Engine

# Constants
FILE_PATH = "./occurrence.txt" #Currently only for .txt datasets
CHUNK_SIZE = 1000
THROTTLE_DELAY = int(os.getenv("THROTTLE_DELAY", "5")) 
LOG_FILE = "ingestion.log"
METRICS_FILE = "metrics_log.csv"


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")


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

    """Process a single chunk of data."""
    return chunk.dropna(subset=["decimalLatitude", "decimalLongitude"])


def persist_metrics(metrics_history: List[Dict[str, Any]]) -> None:

    """Save metrics to persistent storage."""
    try:
        pd.DataFrame(metrics_history).to_csv(METRICS_FILE, index=False)
    except (IOError, pd.errors.EmptyDataError) as e:
        logging.error("Failed to persist metrics: %s", e)


def validate_file(file_path: str) -> bool:

    """Validate input file exists and is readable."""
    if not os.path.exists(file_path):
        logging.error("File not found: %s", file_path)
        return False
    if not os.access(file_path, os.R_OK):
        logging.error("File not readable: %s", file_path)
        return False
    return True


def insert_row(row: pd.Series, engine: Engine) -> bool:

    """Insert single row with error handling."""
    try:
        pd.DataFrame([row]).to_sql(
            "obis_data",
            con=engine,
            if_exists="append",
            index=False
        )
        return True
    
    except exc.SQLAlchemyError as e:
        logging.error("Row insert failed: %s | Data: %s", e, row.to_dict())
        return False


def ingest_data(engine: Engine) -> Optional[List[Dict[str, Any]]]:

    """Main ingestion function with comprehensive error handling."""
    if not validate_file(FILE_PATH):
        sys.exit(1)

    total_rows = 0
    metrics_history = []

    try:
        for chunk in pd.read_csv(FILE_PATH, sep="\t", chunksize=CHUNK_SIZE):
            start_time = time.time()
            processed_chunk = process_chunk(chunk)
            rows_processed = len(processed_chunk)

            try:
                processed_chunk.to_sql(
                    "obis_data",
                    con=engine,
                    if_exists="append",
                    index=False
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
    db_engine = create_engine(DATABASE_URL)
    ingest_data(db_engine)
