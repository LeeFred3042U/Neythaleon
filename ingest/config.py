import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
PARQUET_DIR = Path(os.getenv("PARQUET_DIR", BASE_DIR / "parquet"))
FAILED_DIR = Path(os.getenv("FAILED_DIR", BASE_DIR / "failed_chunks"))
MEDIA_DIR = Path(os.getenv("MEDIA_DIR", BASE_DIR / "media"))
FAILED_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

PARALLELISM = int(os.getenv("PARALLELISM", "4"))

DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_TABLE = os.getenv("DB_TABLE", "ingested_data")

# config.py — warn on large chunk sizes, suggestion Keep it 10k
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50000"))
if CHUNK_SIZE > 20000:
    import warnings
    warnings.warn(f"CHUNK_SIZE is large ({CHUNK_SIZE}). Consider lowering to avoid OOM.")


METRICS_FILE = Path(os.getenv("METRICS_FILE", MEDIA_DIR / "ingestion_metrics.csv"))
LOG_FILE = Path(os.getenv("LOG_FILE", BASE_DIR / "ingestion.log"))

# Integer columns: csv list in env or default small set
INT_COLUMNS = [c.strip() for c in os.getenv("INT_COLUMNS", "").split(",") if c.strip()]

# Throttling
CPU_THRESHOLD = int(os.getenv("CPU_THRESHOLD", "85"))
THROTTLE_DELAY_HIGH_CPU = float(os.getenv("THROTTLE_DELAY_HIGH_CPU", "2"))
THROTTLE_DELAY = float(os.getenv("THROTTLE_DELAY", "0"))
RAM_THRESHOLD = 85.0