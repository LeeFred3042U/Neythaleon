import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_PHYS_TO_PANDAS: Dict[str, str] = {
    "INT32":                "Int32",
    "INT64":                "Int64",
    "FLOAT":                "Float32",
    "DOUBLE":               "Float64",
    "BOOLEAN":              "boolean",
    "BYTE_ARRAY":           "object",
    "FIXED_LEN_BYTE_ARRAY": "object",
    "INT96":                "object",
}

_INTEGER_PHYSICAL = {"INT32", "INT64"}


def load_manifest(path: str | Path) -> Optional[Dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        logger.debug("No manifest at %s; will infer schema from sample chunk", path)
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        logger.info(
            "Loaded Go manifest: %d columns, %d rows across %d valid files",
            len(data.get("columns", {})),
            data.get("total_rows", 0),
            data.get("valid_files", 0),
        )
        _log_storage_hints(data)
        return data
    except Exception:
        logger.exception("Failed to parse manifest at %s; falling back to sample inference", path)
        return None


def infer_int_columns(manifest: Dict[str, Any], density_threshold: float = 50.0) -> List[str]:
    result = [
        name
        for name, col in manifest.get("columns", {}).items()
        if col.get("physical_type") in _INTEGER_PHYSICAL
        and col.get("density_pct", 0.0) >= density_threshold
    ]
    return sorted(result)


def build_schema_df(manifest: Dict[str, Any]) -> pd.DataFrame:
    columns: Dict[str, Any] = manifest.get("columns", {})
    schema: Dict[str, str] = {
        name: _PHYS_TO_PANDAS.get(col.get("physical_type", ""), "object")
        for name, col in columns.items()
    }
    df = pd.DataFrame(columns=sorted(schema.keys()))
    for col, dtype in schema.items():
        try:
            df[col] = df[col].astype(dtype)
        except Exception:
            pass
    return df


def _log_storage_hints(manifest: Dict[str, Any]) -> None:
    est = manifest.get("storage_estimate_gb", {})
    if not est:
        return
    logger.info(
        "Storage estimates — Parquet: %.2f GB | PG unoptimised: ~%.2f GB | PG optimised: ~%.2f GB",
        est.get("parquet_gb", 0.0),
        est.get("pg_unoptimised_gb", 0.0),
        est.get("pg_optimised_gb", 0.0),
    )
