import pandas as pd
import matplotlib.pyplot as plt

# Load data
df = pd.read_csv("metrics_log.csv")

# Clean data
df = df.dropna(subset=["timestamp", "rows_per_second"])
df = df[df["rows_per_second"] > 0]
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Smooth the throughput
df["smoothed"] = df["rows_per_second"].rolling(window=5).mean()

# Plot
fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(df["timestamp"], df["rows_per_second"], label="Raw Throughput", alpha=0.5)
ax.plot(df["timestamp"], df["smoothed"], label="Smoothed", color="orange", linewidth=2)

ax.set_xlabel("Time")
ax.set_ylabel("Rows/sec")
ax.set_title("Ingestion Throughput Over Time")
plt.xticks(rotation=45)
ax.legend()

plt.tight_layout()
plt.savefig("pipeline_metrics_1.png")
print("Saved plot to pipeline_metrics_1.png")