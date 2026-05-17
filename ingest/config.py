import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]

PARQUET_DIR = BASE_DIR / os.environ["PARQUET_DIR"]
FAILED_DIR = BASE_DIR / os.environ["FAILED_DIR"]
MEDIA_DIR = BASE_DIR / os.environ["MEDIA_DIR"]

FAILED_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

DATABASE_URL = os.environ["DATABASE_URL"]
DB_TABLE = os.environ["DB_TABLE"]
CHUNK_SIZE = int(os.environ["CHUNK_SIZE"])
METRICS_FILE = BASE_DIR / os.environ["METRICS_FILE"]
LOG_FILE = BASE_DIR / os.environ["LOG_FILE"]
INT_COLUMNS = [
    c.strip() for c in os.environ.get("INT_COLUMNS", "").split(",") if c.strip()
]

CPU_THRESHOLD = int(os.environ["CPU_THRESHOLD"])
THROTTLE_DELAY_HIGH_CPU = float(os.environ["THROTTLE_DELAY_HIGH_CPU"])
THROTTLE_DELAY = float(os.environ["THROTTLE_DELAY"])
RAM_THRESHOLD = float(os.environ["RAM_THRESHOLD"])

VALIDATE_COORDS = os.environ.get("VALIDATE_COORDS", "false").lower() == "true"
MANIFEST_FILE = BASE_DIR / os.environ["MANIFEST_FILE"]
FAIL_FAST = os.environ["FAIL_FAST"].lower() == "true"
GRAPH_SAMPLE_LIMIT = int(os.environ["GRAPH_SAMPLE_LIMIT"])
TARGET_CHUNK_MB = int(os.environ.get("TARGET_CHUNK_MB", "256"))

_cpu = os.cpu_count() or 2
N_TRANSFORM_WORKERS = int(os.environ.get("N_TRANSFORM_WORKERS", str(max(1, _cpu - 1))))
N_WRITE_THREADS = int(os.environ.get("N_WRITE_THREADS", "4"))
QUEUE_DEPTH = int(os.environ.get("QUEUE_DEPTH", "8"))
