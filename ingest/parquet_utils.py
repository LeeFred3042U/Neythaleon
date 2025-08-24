import os
import logging
from typing import Iterator, Optional, List
import pyarrow.parquet as pq
import pandas as pd

logger = logging.getLogger(__name__)


def stream_parquet_chunks(folder: str, chunk_size: int, backend: str = "pyarrow"):
    if backend == "duckdb":
        import duckdb
        for f in sorted(os.listdir(folder)):
            if not f.endswith(".parquet"): continue
            path = os.path.join(folder, f)
            with duckdb.connect() as con:
                reader = con.execute(f"SELECT * FROM read_parquet('{path}')").fetch_record_batch(chunk_size)
                while True:
                    try:
                        batch = reader.read_next_batch()
                        yield batch.to_pandas()
                    except StopIteration:
                        break
    else:
        folder = os.fspath(folder)
        for file in sorted(os.listdir(folder)):
            if not file.endswith(".parquet"):
                continue
            path = os.path.join(folder, file)
            logger.info("Reading parquet: %s", path)
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=chunk_size):
                yield batch.to_pandas()

def get_full_schema_from_parquet(folder: str) -> Optional[List[str]]:
    """
    Return the union (list) of column names across all parquet files in folder.

    Notes:
    - Avoids pyarrow Schema merge API differences by collecting names.
    - dtype inference is handled later from a processed sample.
    """
    folder = os.fspath(folder)
    names = set()
    for file in os.listdir(folder):
        if not file.endswith(".parquet"):
            continue
        path = os.path.join(folder, file)
        try:
            pf = pq.ParquetFile(path)
            # pf.schema_arrow.names is available and consistent
            names.update(pf.schema_arrow.names)
        except Exception:
            logger.exception("Failed to read schema from %s", path)
    if not names:
        return None
    # return deterministic order (sorted) so table creation is stable
    return sorted(names)