import os
import sys
import argparse
from pathlib import Path
import csv
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

try:
    import seaborn as sns  # type: ignore
    sns.set_theme(style="whitegrid")
except ImportError:
    pass

# Set figure-level rc for paper-friendly visuals
plt.rcParams.update({
    "figure.dpi": 160,
    "font.size": 12,
    "axes.titlesize": 16,
    "axes.labelsize": 13,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 11,
    "axes.titleweight": "bold",
    "axes.grid": True,
})

DEFAULT_METRICS_FILE = "metrics_log.csv"
DEFAULT_OUTPUT_DIR = "media"
DEFAULT_DPI = 160
DEFAULT_SMOOTH_WINDOW = 10
OUTPUT_FILENAME = "metrics_dashboard.png"

PALETTE = plt.get_cmap("tab10")
COLOR_RAW = PALETTE(0)
COLOR_SMTH = PALETTE(1)
COLOR_CPU = PALETTE(2)
COLOR_MEM = PALETTE(3)
COLOR_BAR = PALETTE(7) # <-- UPDATED: Changed from 4 (purple) to 7 (gray) for better contrast


def first_existing_col(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _read_raw_lines(path: str):
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return [ln.rstrip("\n") for ln in fh]


def _merge_unbalanced_quote_lines(lines):
    merged = []
    i = 0
    n = len(lines)
    while i < n:
        cur = lines[i]
        quote_count = cur.count('"')
        j = i
        while quote_count % 2 != 0 and j + 1 < n:
            j += 1
            cur = cur + "\n" + lines[j]
            quote_count = cur.count('"')
        merged.append(cur)
        i = j + 1
    return merged


def _csv_rows_from_lines(lines):
    reader = csv.reader(lines, delimiter=",", quotechar='"', skipinitialspace=False)
    it = iter(reader)
    try:
        header = next(it)
    except StopIteration:
        return [], []
    rows = list(it)
    return header, rows


def _normalize_rows_to_header(header, rows):
    n = len(header)
    out_rows = []
    for r in rows:
        if len(r) == n:
            out_rows.append(r)
        elif len(r) > n:
            folded = r[: n - 1] + [",".join(r[n - 1 :])]
            out_rows.append(folded)
        else:
            padded = r + [""] * (n - len(r))
            out_rows.append(padded)
    return out_rows


def read_metrics_fuzzy(path: str) -> pd.DataFrame:
    raw_lines = _read_raw_lines(path)
    if not raw_lines:
        return pd.DataFrame()
    merged = _merge_unbalanced_quote_lines(raw_lines)
    header, rows = _csv_rows_from_lines(merged)
    if not header:
        return pd.DataFrame()
    rows = _normalize_rows_to_header(header, rows)
    df = pd.DataFrame(rows, columns=header)
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"": pd.NA})
        sample = df[col].dropna().head(20).tolist()
        numeric_hits = sum(1 for v in sample if re.match(r"^-?\d+(\.\d+)?$", str(v)))
        if sample and numeric_hits / len(sample) >= 0.6:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_metrics(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Metrics file not found: {path}")
    try:
        df = read_metrics_fuzzy(path)
    except Exception as e:
        try:
            df = pd.read_csv(path, engine="python", on_bad_lines="skip")
        except Exception as e2:
            raise RuntimeError(f"Failed to load metrics file: {e} | {e2}")
    if df is None or df.empty:
        return pd.DataFrame()
    ts_col = first_existing_col(df, ["timestamp", "time", "ts", "started_at"])
    if ts_col:
        df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce")
        if ts_col != "timestamp" and ts_col in df.columns:
            try:
                df = df.drop(columns=[ts_col])
            except Exception:
                pass
    else:
        df["timestamp"] = pd.to_datetime(df.get("timestamp", None), errors="coerce")
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def resolve_columns(df: pd.DataFrame):
    cols = {}
    cols["rows_per_second"] = first_existing_col(df, ["rows_per_second", "rows_per_sec", "rps"])
    cols["rows_processed"] = first_existing_col(df, ["rows_processed", "rows"])
    cols["processing_time_s"] = first_existing_col(df, ["processing_time_s", "processing_time", "processing_time_seconds", "duration_s"])
    cols["cpu_percent"] = first_existing_col(df, ["cpu_percent", "cpu_usage", "cpu"])
    cols["memory_mb"] = first_existing_col(df, ["rss_memory_mb", "rss_mb", "memory_mb", "system_used_mem_mb", "system_used_mb", "memory_used_mb"])
    return cols


def ensure_rows_per_second(df: pd.DataFrame, cols: dict) -> pd.DataFrame:
    if cols.get("rows_per_second"):
        return df
    rp = cols.get("rows_processed")
    pt = cols.get("processing_time_s")
    if rp and pt and rp in df.columns and pt in df.columns:
        # Create a new column with the standard name 'rows_per_second'
        new_rps_col = "rows_per_second"
        df[new_rps_col] = df.apply(
            lambda r: float(r[rp]) / float(r[pt]) if pd.notna(r[rp]) and pd.notna(r[pt]) and float(r[pt]) > 0 else np.nan,
            axis=1,
        )
        cols["rows_per_second"] = new_rps_col
    return df


def _make_ax_empty(ax, text="No data"):
    ax.clear()
    ax.text(0.5, 0.5, text, ha="center", va="center", fontsize=13, alpha=0.8)
    ax.set_xticks([])
    ax.set_yticks([])


def plot_throughput_panel(ax, df: pd.DataFrame, rps_col: str, window: int):
    if "timestamp" not in df.columns or not rps_col or rps_col not in df.columns:
        _make_ax_empty(ax, "Throughput: missing throughput or timestamp column")
        return
    rps = pd.to_numeric(df[rps_col], errors="coerce").astype(float)
    smoothed = rps.rolling(window=window, min_periods=1).mean()
    ax.plot(df["timestamp"], rps, marker='o', linestyle='None', alpha=0.28, markersize=4, color=COLOR_RAW, label="raw")
    ax.plot(df["timestamp"], smoothed, linewidth=2.4, color=COLOR_SMTH, label=f"smoothed ({window})", zorder=3)
    try:
        roll_std = rps.rolling(window=window, min_periods=1).std().fillna(0)
        ax.fill_between(df["timestamp"], (smoothed - roll_std), (smoothed + roll_std), alpha=0.08, color=COLOR_SMTH, zorder=1)
    except Exception:
        pass
    ax.set_title("Ingestion Throughput (rows/sec)")
    ax.set_ylabel("Rows/sec")
    ax.legend(loc="upper right", frameon=True)
    locator = mdates.AutoDateLocator()
    formatter = mdates.AutoDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    plt.setp(ax.get_xticklabels(), rotation=25, ha="right")


def plot_cpu_vs_throughput_panel(ax, df: pd.DataFrame, rps_col: str, cpu_col: str):
    if not rps_col or rps_col not in df.columns:
        _make_ax_empty(ax, "CPU vs Throughput: missing throughput column")
        return
    if not cpu_col or cpu_col not in df.columns:
        _make_ax_empty(ax, "CPU vs Throughput: missing CPU column")
        return
    cpu = pd.to_numeric(df[cpu_col], errors="coerce").astype(float)
    rps = pd.to_numeric(df[rps_col], errors="coerce").astype(float)
    ax.scatter(cpu, rps, alpha=0.9, s=40, color=COLOR_CPU)
    ax.set_title("CPU (%)  vs  Throughput")
    ax.set_xlabel("CPU (%)")
    ax.set_ylabel("Rows/sec")
    try:
        top_idx = np.nanargmax(rps.values)
        ax.annotate("peak", xy=(cpu.iloc[top_idx], rps.iloc[top_idx]), xytext=(5, 5), textcoords="offset points", fontsize=9)
    except Exception:
        pass


def plot_memory_panel(ax, df: pd.DataFrame, mem_col: str):
    if not mem_col or mem_col not in df.columns or "timestamp" not in df.columns:
        _make_ax_empty(ax, "Memory: missing memory or timestamp column")
        return
    mem = pd.to_numeric(df[mem_col], errors="coerce").astype(float)
    ax.plot(df["timestamp"], mem, marker='o', linestyle='-', alpha=0.95, color=COLOR_MEM)
    ax.set_title(f"Memory Usage ({mem_col})")
    ax.set_ylabel("MB")
    locator = mdates.AutoDateLocator()
    formatter = mdates.AutoDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    plt.setp(ax.get_xticklabels(), rotation=25, ha="right")
    try:
        last_val = mem.dropna().iloc[-1]
        last_ts = df["timestamp"].dropna().iloc[-1]
        ax.annotate(f"{last_val:.0f} MB", xy=(last_ts, last_val), xytext=(8, 0), textcoords="offset points", fontsize=11, va="center", color=COLOR_MEM)
    except Exception:
        pass


def plot_batch_time_panel(ax, df: pd.DataFrame, pt_col: str):
    if not pt_col or pt_col not in df.columns:
        _make_ax_empty(ax, "Batch time: missing processing_time column")
        return
    pt = pd.to_numeric(df[pt_col], errors="coerce").astype(float)
    ax.bar(range(len(df)), pt, width=0.8, alpha=0.9, color=COLOR_BAR)
    ax.set_title("Processing Time per Batch")
    ax.set_xlabel("Batch index")
    ax.set_ylabel("Seconds")
    if len(pt.dropna()) > 0:
        ax.set_ylim(bottom=0, top=max(1.0, np.nanpercentile(pt.dropna(), 98) * 1.2))


def create_single_png(metrics_file: str, output_dir: str, window: int, dpi: int):
    df = load_metrics(metrics_file)
    if df.empty:
        print("Metrics file empty — nothing to plot.")
        return

    cols = resolve_columns(df)
    df = ensure_rows_per_second(df, cols) # This ensures rps exists if possible

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / OUTPUT_FILENAME

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    ax1, ax2, ax3, ax4 = axes.flatten()

    rps_col = cols.get("rows_per_second")
    cpu_col = cols.get("cpu_percent")
    mem_col = cols.get("memory_mb")
    pt_col = cols.get("processing_time_s")

    plot_throughput_panel(ax1, df, rps_col, window)
    plot_cpu_vs_throughput_panel(ax2, df, rps_col, cpu_col)
    plot_memory_panel(ax3, df, mem_col)
    plot_batch_time_panel(ax4, df, pt_col)

    plt.suptitle("Ingestion Pipeline Metrics Dashboard", fontsize=20, weight="bold")
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved dashboard to: {out_path}")


def parse_args():
    p = argparse.ArgumentParser(description="Generate single-PNG metrics dashboard")
    p.add_argument("--metrics", "-m", default=os.getenv("METRICS_FILE", DEFAULT_METRICS_FILE), help="Path to metrics CSV")
    p.add_argument("--out", "-o", default=os.getenv("OUTPUT_DIR", DEFAULT_OUTPUT_DIR), help="Output directory for image")
    p.add_argument("--window", "-w", default=int(os.getenv("SMOOTH_WINDOW", DEFAULT_SMOOTH_WINDOW)), type=int, help="Smoothing window for throughput (rows)")
    p.add_argument("--dpi", default=int(os.getenv("PLOT_DPI", DEFAULT_DPI)), type=int, help="Output image DPI")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        create_single_png(metrics_file=args.metrics, output_dir=args.out, window=args.window, dpi=args.dpi)
    except FileNotFoundError as e:
        print("Error:", e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)