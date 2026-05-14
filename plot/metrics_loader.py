from pathlib import Path

import pandas as pd


NUMERIC_COLS = {
    "throughput_rps",
    "duration_sec",
    "cpu_percent",
    "memory_mb",
    "rows",
    "rows_per_second",
    "rows_processed",
}


def _maybe_coerce_numeric(s: pd.Series, thresh: float = 0.75) -> pd.Series:

    if s.dtype != object:
        return (
            pd.to_numeric(s, errors="coerce") if pd.api.types.is_numeric_dtype(s) else s
        )

    sample = s.dropna().astype(str)

    if sample.empty:
        return s

    numeric_like = sample.str.match(r"^-?\d+(\.\d+)?$").sum()

    if numeric_like / len(sample) >= thresh:
        return pd.to_numeric(s, errors="coerce")

    return s


def load_metrics(file_path: str | Path) -> pd.DataFrame:

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    df = pd.read_csv(file_path)

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    for k in NUMERIC_COLS:
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors="coerce")

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = _maybe_coerce_numeric(df[col])

    return df


def read_metrics_fuzzy(folder: str | Path, pattern: str = "*.csv") -> pd.DataFrame:

    folder = Path(folder)

    files = list(folder.glob(pattern))

    if not files:
        raise FileNotFoundError(folder)

    dfs = [load_metrics(f) for f in files]

    return pd.concat(dfs, ignore_index=True)
