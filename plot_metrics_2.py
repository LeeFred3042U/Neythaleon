import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("metrics_log.csv")

# CPU vs Rows/sec:
plt.scatter(df["cpu_usage"], df["rows_per_second"])
plt.xlabel("CPU Usage (%)")
plt.ylabel("Rows/sec")
plt.title("CPU Load vs Throughput")
plt.grid(True)

plt.tight_layout()
plt.savefig("pipeline_metrics_2.png")
print("Saved plot to pipeline_metrics_2.png")