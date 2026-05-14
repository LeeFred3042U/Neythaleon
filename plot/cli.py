from pathlib import Path

import pandas as pd

from .metrics_loader import read_metrics_fuzzy

from .dashboard import create_single_png

from ingest.config import MEDIA_DIR


def main(
    metrics_dir: str | Path = MEDIA_DIR,
    output: str | Path = MEDIA_DIR / "dashboard_yellow.png",
):

    df = read_metrics_fuzzy(metrics_dir)

    if "timestamp" in df.columns:
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        except Exception:
            pass

    create_single_png(df, output)


if __name__ == "__main__":
    main()
