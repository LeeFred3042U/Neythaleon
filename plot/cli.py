from pathlib import Path
import pandas as pd  # FIX: was missing; caused NameError on pd.to_datetime()
from .metrics_loader import read_metrics_fuzzy
from .dashboard import create_single_png


def main(metrics_dir: str = 'media', output: str = 'media/dashboard_yellow.png'):
    df = read_metrics_fuzzy(metrics_dir)
    if 'timestamp' in df.columns:
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        except Exception:
            pass
    create_single_png(df, output)

if __name__ == '__main__':
    main()