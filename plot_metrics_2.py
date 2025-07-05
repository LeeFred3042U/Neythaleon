import pandas as pd
import matplotlib.pyplot as plt

# Load data
df = pd.read_csv("metrics_log.csv")

# Clean data
df = df.dropna(subset=["cpu_usage", "rows_per_second"])
df = df[df["rows_per_second"] > 0]

# Plot
fig, ax = plt.subplots(figsize=(6, 5))
scatter = ax.scatter(df["cpu_usage"], df["rows_per_second"], c='orange', alpha=0.8)

ax.set_xlabel("CPU Usage (%)")
ax.set_ylabel("Rows/sec")
ax.set_title("CPU Load vs Throughput")
ax.grid(True)

# Annotate obvious outliers (e.g. CPU usage > 40%)
outliers = df[df["cpu_usage"] > 40]
for _, row in outliers.iterrows():
    ax.annotate(f"{int(row['cpu_usage'])}%", (row["cpu_usage"], row["rows_per_second"]),
                textcoords="offset points", xytext=(0, 10), ha='center', fontsize=8, color='red')

plt.tight_layout()
plt.savefig("pipeline_metrics_2.png")
print("Saved plot to pipeline_metrics_2.png")