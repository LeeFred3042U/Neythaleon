import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]

# FIX: default was "parquet" but shipped files live in "parquet_files"
PARQUET_DIR = Path(os.getenv("PARQUET_DIR", BASE_DIR / "parquet_files"))
FAILED_DIR = Path(os.getenv("FAILED_DIR", BASE_DIR / "failed_chunks"))
MEDIA_DIR = Path(os.getenv("MEDIA_DIR", BASE_DIR / "media"))
FAILED_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

# Number of parallel transform workers used in ingest_runner
PARALLELISM = int(os.getenv("PARALLELISM", "4"))

DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_TABLE = os.getenv("DB_TABLE", "ingested_data")

# CHUNK_SIZE lowered from 50000 → 5000 to prevent OOM with wide tables
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))

METRICS_FILE = Path(os.getenv("METRICS_FILE", MEDIA_DIR / "ingestion_metrics.csv"))
LOG_FILE = Path(os.getenv("LOG_FILE", BASE_DIR / "ingestion.log"))

# Integer columns: csv list in env. Left empty so manifest can auto-populate when present.
INT_COLUMNS = [c.strip() for c in os.getenv("INT_COLUMNS", "").split(",") if c.strip()]

# Throttling
CPU_THRESHOLD = int(os.getenv("CPU_THRESHOLD", "85"))
THROTTLE_DELAY_HIGH_CPU = float(os.getenv("THROTTLE_DELAY_HIGH_CPU", "2"))
THROTTLE_DELAY = float(os.getenv("THROTTLE_DELAY", "0"))
RAM_THRESHOLD = 85.0

# Coordinate validation flag (referenced in .env.example but was never read)
VALIDATE_COORDS = os.getenv("VALIDATE_COORDS", "false").lower() == "true"

# Optional JSON manifest produced by: go run goSchemeReader/main.go --json <folder>
# When present, skips sample-chunk schema inference and auto-populates INT_COLUMNS.
MANIFEST_FILE = Path(os.getenv("MANIFEST_FILE", BASE_DIR / "schema_manifest.json"))

# Fail-fast mode: abort on first chunk failure instead of saving and continuing
FAIL_FAST = os.getenv("FAIL_FAST", "false").lower() == "true"


def compute_safe_chunk_size(manifest: dict, target_mb: int = 256) -> int:
    """
    Estimate bytes per row from manifest column types, return a chunk size
    that keeps one chunk under target_mb MB.
    """
    TYPE_BYTES = {
        "INT32": 4, "INT64": 8, "FLOAT": 4, "DOUBLE": 8,
        "BOOLEAN": 1, "BYTE_ARRAY": 15, "FIXED_LEN_BYTE_ARRAY": 16,
        "INT96": 12,
    }
    cols = manifest.get("columns", {})
    bytes_per_row = sum(
        TYPE_BYTES.get(col.get("physical_type", ""), 8)
        for col in cols.values()
    )
    bytes_per_row = max(bytes_per_row, 1)
    target_bytes = target_mb * 1024 * 1024
    return max(1000, min(50000, target_bytes // bytes_per_row))