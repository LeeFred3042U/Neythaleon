import time

import csv

import logging


from datetime import datetime

import psutil

from ingest.config import METRICS_FILE

logger = logging.getLogger(__name__)

_process = psutil.Process()
_process.cpu_percent()  # Initialize the baseline for CPU tracking

CANONICAL_FIELDNAMES = [
    "timestamp",
    "rows",
    "duration_sec",
    "throughput_rps",
    "errors",
    "duplicates",
    "null_fraction",
    "cpu_percent",
    "memory_mb",
]


def persist_metrics(metrics: dict):

    metrics = _normalize_metric_dict(metrics)

    METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)

    file_exists = METRICS_FILE.exists()

    with open(METRICS_FILE, "a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CANONICAL_FIELDNAMES)

        if not file_exists:
            writer.writeheader()

        row = {k: metrics.get(k, "") for k in CANONICAL_FIELDNAMES}

        writer.writerow(row)

    logger.debug("Persisted metrics: %s", metrics)


def track_metrics(
    start_time: float,
    row_count: int,
    error_count: int = 0,
    duplicates: int = 0,
    nulls: float = 0,
) -> dict:

    duration = time.time() - start_time

    cpu = _process.cpu_percent(interval=None) / psutil.cpu_count()

    memory = _process.memory_info().rss / (1024 * 1024)

    return {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "rows": int(row_count),
        "duration_sec": float(duration),
        "throughput_rps": float(row_count) / duration if duration > 0 else 0.0,
        "errors": int(error_count),
        "duplicates": int(duplicates),
        "null_fraction": float(nulls),
        "cpu_percent": float(cpu),
        "memory_mb": float(memory),
    }


def _normalize_metric_dict(metrics: dict) -> dict:

    m = metrics.copy()

    aliases = {
        "rows_per_second": "throughput_rps",
        "rows_per_sec": "throughput_rps",
        "processing_time": "duration_sec",
        "processing_time_s": "duration_sec",
        "duration": "duration_sec",
        "cpu_usage": "cpu_percent",
    }

    for k, v in list(m.items()):
        if k in aliases:
            m[aliases[k]] = m.pop(k)

    return m
