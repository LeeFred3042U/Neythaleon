from pathlib import Path
from .metrics_loader import read_metrics_fuzzy
from .dashboard import create_single_png


def main(metrics_dir: str = 'media', output: str = 'media/dashboard_yellow.png'):
    df = read_metrics_fuzzy(metrics_dir)
    # basic normalization
    if 'timestamp' in df.columns:
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        except Exception:
            pass
    create_single_png(df, output)

if __name__ == '__main__':
    main()