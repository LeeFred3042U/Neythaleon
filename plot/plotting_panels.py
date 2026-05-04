import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib.dates as mdates

def format_time_axis(ax):
    locator = mdates.AutoDateLocator()
    formatter = mdates.AutoDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    plt.setp(ax.get_xticklabels(), rotation=25, ha='right')


def _plot_time_series_with_gaps(ax, ts: pd.Series, y: pd.Series, *,
                               marker='o', alpha_raw=0.3, smooth_window=5,
                               zorder_raw=2, zorder_smooth=3, color="#1f77b4"):
    df = pd.DataFrame({'ts': ts, 'y': y})
    df['ts'] = pd.to_datetime(df['ts'], errors='coerce')
    df['y'] = pd.to_numeric(df['y'], errors='coerce')
    df = df.dropna(subset=['ts', 'y'])
    if df.empty:
        ax.text(0.5,0.5,'No data',ha='center')
        return

    df = df.sort_values('ts').reset_index(drop=True)
    deltas = df['ts'].diff().dt.total_seconds().fillna(0)
    dmed = deltas.median()
    threshold = (dmed * 1.5) if dmed > 0 else 60.0
    df['_segment'] = (deltas > threshold).cumsum()

    ax.scatter(df['ts'], df['y'], marker=marker, alpha=alpha_raw, zorder=zorder_raw, color=color, s=18)

    for _, seg in df.groupby('_segment'):
        if seg.empty:
            continue
        if len(seg) >= 3 and smooth_window > 1:
            smoothed = seg['y'].rolling(window=smooth_window, min_periods=1).mean()
            ax.plot(seg['ts'], smoothed, linewidth=1.4, zorder=zorder_smooth, color=color)
        else:
            ax.plot(seg['ts'], seg['y'], linewidth=0.9, zorder=zorder_smooth, color=color)


def _annotate_peaks(ax, ts, y, top_n=2, label_fmt="{:.0f}"):
    ts_parsed = pd.to_datetime(ts, errors='coerce')
    s = pd.Series(pd.to_numeric(y, errors='coerce').values, index=ts_parsed.values)
    s = s.dropna()
    if s.empty:
        return
    n = min(top_n, len(s))
    try:
        top_idx = s.nlargest(n).index
        for t in top_idx:
            ax.annotate(label_fmt.format(float(s.loc[t])), xy=(t, s.loc[t]), xytext=(5,5), textcoords='offset points', fontsize=8)
    except Exception:
        pass


def plot_throughput(ax, df):
    if 'timestamp' not in df.columns or 'throughput_rps' not in df.columns:
        ax.text(0.5,0.5,'No data',ha='center'); return

    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.sort_values('timestamp').dropna(subset=['timestamp', 'throughput_rps'])
    df['throughput_rps'] = pd.to_numeric(df['throughput_rps'], errors='coerce')

    if df.empty:
        ax.text(0.5,0.5,'No data',ha='center'); return

    x_num = mdates.date2num(df['timestamp'].to_numpy())
    jitter = (np.random.randn(len(x_num)) * 1e-6)
    x_jittered = x_num + jitter

    ax.scatter(mdates.num2date(x_jittered), df['throughput_rps'], s=22, alpha=0.28, label='raw', zorder=2)

    window = 7
    smoothed = df['throughput_rps'].rolling(window=window, min_periods=1).mean()
    ax.plot(df['timestamp'], smoothed, linewidth=2.2, zorder=3, label=f'smoothed({window})', color="#1f77b4")

    ax.set_title("Throughput (rps)")
    ax.set_ylabel("rps")
    ax.legend(loc='upper right', frameon=True)
    format_time_axis(ax)


def plot_cpu_vs_throughput(ax, df):
    if 'cpu_percent' not in df.columns or 'throughput_rps' not in df.columns:
        ax.text(0.5,0.5,'No data',ha='center')
        return

    if 'timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df = df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    cpu = pd.to_numeric(df['cpu_percent'], errors='coerce')
    rps = pd.to_numeric(df['throughput_rps'], errors='coerce')

    try:
        denom = float(np.nanpercentile(rps.dropna(), 75)) if not rps.dropna().empty else 1.0
    except Exception:
        denom = 1.0
    sizes = ((rps.fillna(0) / max(1.0, denom)) * 40) + 6

    ax.scatter(cpu, rps, s=sizes, alpha=0.75)
    ax.set_xlabel("CPU (%)")
    ax.set_ylabel("Rows/sec")
    ax.set_title("CPU (%)  vs  Throughput")


def plot_memory(ax, df):
    if 'timestamp' not in df.columns or 'memory_mb' not in df.columns:
        ax.text(0.5,0.5,'No data',ha='center'); return

    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df = df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    _plot_time_series_with_gaps(ax, df['timestamp'], df['memory_mb'], marker='o', smooth_window=15, alpha_raw=0.5)
    ax.set_title("Memory (MB)")
    format_time_axis(ax)


def plot_batch_time(ax, df):
    if 'timestamp' not in df.columns or 'duration_sec' not in df.columns:
        ax.text(0.5, 0.5, 'No data', ha='center')
        return

    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df = df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    plot_df = df[['timestamp', 'duration_sec']].dropna()
    if plot_df.empty:
        ax.text(0.5, 0.5, 'No data', ha='center')
        return

    x_dt = plot_df['timestamp'].to_numpy()
    y = pd.to_numeric(plot_df['duration_sec'], errors='coerce').to_numpy()

    x = mdates.date2num(x_dt)

    width_seconds = 1.0
    width = width_seconds / 86400.0

    if len(x) > 1:
        diffs_days = np.diff(x)
        median_diff_days = float(np.nanmedian(diffs_days)) if len(diffs_days) > 0 else 0.0
        if median_diff_days > 0:
            width = min(width, median_diff_days * 0.9)

    ax.bar(x, y, width=width, align='center', alpha=0.9)
    ax.set_title("Batch time (s)")

    try:
        valid_y = y[~np.isnan(y)]
        upper = max(5.0, float(np.nanpercentile(valid_y, 98) * 1.2)) if valid_y.size else 5.0
        ax.set_ylim(bottom=0, top=upper)
    except Exception:
        pass

    format_time_axis(ax)
