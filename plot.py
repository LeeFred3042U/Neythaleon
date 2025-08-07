import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# --- Configuration ---
METRICS_FILE = "metrics_log.csv"
OUTPUT_DIR = "media"

def create_individual_plots():
    """
    Reads pipeline metrics and generates four separate plot images.
    """
    # --- 1. Load and Prepare Data ---
    if not os.path.exists(METRICS_FILE):
        print(f"Error: Metrics file not found at '{METRICS_FILE}'")
        sys.exit(1)

    df = pd.read_csv(METRICS_FILE)
    if df.empty:
        print("Metrics file is empty. No data to plot.")
        sys.exit(0)
        
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    df = df[df["rows_per_second"] > 0].copy()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sns.set_theme(style="whitegrid")
    print(f"Generating 4 plots from '{METRICS_FILE}'...")

    # --- Plot 1: Throughput Over Time ---
    plt.figure(figsize=(12, 7))
    df["smoothed_throughput"] = df["rows_per_second"].rolling(window=10, min_periods=1).mean()
    plt.plot(df["timestamp"], df["rows_per_second"], label="Raw Throughput", alpha=0.3, color='skyblue', marker='.', linestyle='None')
    plt.plot(df["timestamp"], df["smoothed_throughput"], label="Smoothed Trend", color="blue", linewidth=2.5)
    plt.title("Ingestion Throughput Over Time", fontsize=16, weight='bold')
    plt.xlabel("Time")
    plt.ylabel("Rows per Second")
    plt.xticks(rotation=20, ha='right')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "throughput_report.png"), dpi=120)
    plt.close() # Close the figure to free memory
    print(f"  -> Saved 'throughput_report.png'")

    # --- Plot 2: CPU vs. Throughput ---
    plt.figure(figsize=(10, 7))
    scatter = plt.scatter(df["cpu_usage"], df["rows_per_second"], c=df['cpu_usage'], cmap='viridis', alpha=0.8, s=50)
    plt.title("CPU Usage vs. Throughput", fontsize=16, weight='bold')
    plt.xlabel("CPU Usage (%)")
    plt.ylabel("Rows per Second")
    plt.colorbar(scatter, label='CPU Usage (%)')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "cpu_vs_throughput_report.png"), dpi=120)
    plt.close() # Close the figure
    print(f"  -> Saved 'cpu_vs_throughput_report.png'")

    # --- Plot 3: Memory Usage Over Time ---
    plt.figure(figsize=(12, 7))
    plt.plot(df['timestamp'], df['memory_mb'], marker='.', linestyle='-', color='green')
    plt.title('Memory Usage (MB) Over Time', fontsize=16, weight='bold')
    plt.xlabel('Time')
    plt.ylabel('Memory Used (MB)')
    plt.xticks(rotation=20, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "memory_usage_report.png"), dpi=120)
    plt.close() # Close the figure
    print(f"  -> Saved 'memory_usage_report.png'")

    # --- Plot 4: Batch Processing Time ---
    plt.figure(figsize=(12, 7))
    plt.bar(df.index, df['processing_time'], color='purple', alpha=0.8, width=1.0)
    plt.title('Processing Time per Batch', fontsize=16, weight='bold')
    plt.xlabel('Batch Number')
    plt.ylabel('Time (seconds)')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "batch_time_report.png"), dpi=120)
    plt.close() # Close the figure
    print(f"  -> Saved 'batch_time_report.png'")
    
    print("\nAll plots generated successfully in the 'media/' directory.")

if __name__ == "__main__":
    create_individual_plots()