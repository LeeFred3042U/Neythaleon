import os
import time
import logging
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from typing import Dict, Any
import psutil

# Load environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Path to the occurrence.txt file
FILE_PATH = "./occurrence.txt"

# Define chunk size
CHUNK_SIZE = 1000  # Adjust based on your system's capacity

LOG_FILE = "ingestion_log.csv"


def configure_logging() -> None:

    """Set up logging configuration."""
    logging.basicConfig(

        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
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
        "batch_size": CHUNK_SIZE
    }

    logging.info("Metrics: %s", metrics)
    return metrics


# Read and process the file in chunks
for chunk in pd.read_csv(FILE_PATH, sep='\t', chunksize=CHUNK_SIZE):
    # Data cleaning and preprocessing steps go here

    # Insert into the database
    chunk.to_sql('obis_data', con=engine, if_exists='append', index=False)

    # Delay to manage resource usage
    time.sleep(5)