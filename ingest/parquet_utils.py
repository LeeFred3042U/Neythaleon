import os
import logging
from typing import Iterator, Optional, List
import pyarrow.parquet as pq
import pandas as pd

logger = logging.getLogger(__name__)


def stream_parquet_chunks(folder: str, chunk_size: int, columns=None,
                          skip_chunks: int = 0, backend: str = "pyarrow"):
    """
    Yield DataFrames of chunk_size rows from all parquet files in folder.
    
    Args:
        folder: path to directory of parquet files
        chunk_size: rows per chunk
        columns: optional list of column names to project (PyArrow only)
        skip_chunks: number of leading chunks to skip (for resume)
        backend: "pyarrow" or "duckdb"
    """
    if backend == "duckdb":
        import duckdb
        skipped = 0
        for f in sorted(os.listdir(folder)):
            if not f.endswith(".parquet"): continue
            path = os.path.join(folder, f)
            with duckdb.connect() as con:
                reader = con.execute(f"SELECT * FROM read_parquet('{path}')").fetch_record_batch(chunk_size)
                while True:
                    try:
                        batch = reader.read_next_batch()
                        if skipped < skip_chunks:
                            skipped += 1
                            continue
                        yield batch.to_pandas()
                    except StopIteration:
                        break
    else:
        folder = os.fspath(folder)
        skipped = 0
        for file in sorted(os.listdir(folder)):
            if not file.endswith(".parquet"):
                continue
            path = os.path.join(folder, file)
            logger.info("Reading parquet: %s", path)
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=chunk_size, columns=columns):
                if skipped < skip_chunks:
                    skipped += 1
                    continue
                yield batch.to_pandas()

def get_full_schema_from_parquet(folder: str) -> Optional[List[str]]:
    folder = os.fspath(folder)
    names = set()
    for file in os.listdir(folder):
        if not file.endswith(".parquet"):
            continue
        path = os.path.join(folder, file)
        try:
            pf = pq.ParquetFile(path)
            names.update(pf.schema_arrow.names)
        except Exception:
            logger.exception("Failed to read schema from %s", path)
    if not names:
        return None
    return sorted(names)
