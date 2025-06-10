import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("metrics_log.csv")

# Ingestion Speed Over Time:
plt.plot(df["timestamp"], df["rows_per_second"])
plt.xlabel("Time")
plt.ylabel("Rows/sec")
plt.title("Ingestion Throughput Over Time")
plt.xticks(rotation=45)
plt.tight_layout()

plt.tight_layout()
plt.savefig("pipeline_metrics_1.png")
print("Saved plot to pipeline_metrics_1.png")



