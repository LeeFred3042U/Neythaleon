import json
import logging
from dataclasses import dataclass
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


@dataclass
class ColumnMeta:
    physical_type: str
    logical_type: str
    total_values: int
    total_nulls: int
    density_pct: float
    type_length: int = 0
    compression: str = ""
    encoding: str = ""

    @property
    def is_bit_array(self) -> bool:
        return self.physical_type == "FIXED_LEN_BYTE_ARRAY"

    @property
    def is_integer(self) -> bool:
        return self.physical_type in ("INT32", "INT64")

    @property
    def is_float(self) -> bool:
        return self.physical_type in ("FLOAT", "DOUBLE")

    @property
    def is_string(self) -> bool:
        return self.physical_type == "BYTE_ARRAY" and "String" in self.logical_type

    @property
    def is_boolean(self) -> bool:
        return self.physical_type == "BOOLEAN"


def load_manifest(path: str | Path) -> Optional[Dict[str, Any]]:
    """Load raw manifest dict. Returns None if file missing or invalid."""
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


def load_manifest_typed(path: str | Path) -> Dict[str, ColumnMeta]:
    """
    Returns {col_name: ColumnMeta}. Raises on missing/invalid file.
    Used by the column selector and downstream phases.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"Manifest not found at {path}. "
            f"Run: go run goSchemeReader/main.go --json <folder> > schema_manifest.json"
        )
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    result = {}
    for name, col in raw.get("columns", {}).items():
        result[name] = ColumnMeta(
            physical_type=col.get("physical_type", ""),
            logical_type=col.get("logical_type", ""),
            total_values=col.get("total_values", 0),
            total_nulls=col.get("total_nulls", 0),
            density_pct=col.get("density_pct", 0.0),
            type_length=col.get("type_length", 0),
            compression=col.get("compression", ""),
            encoding=col.get("encoding", ""),
        )
    return result


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
