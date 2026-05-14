import matplotlib.pyplot as plt

from pathlib import Path

from .plotting_panels import (
    plot_throughput,
    plot_cpu_vs_throughput,
    plot_memory,
    plot_batch_time,
)


def create_single_png(df, output: str | Path = "dashboard.png"):

    fig, axs = plt.subplots(2, 2, figsize=(12, 8))

    plot_throughput(axs[0, 0], df)

    plot_cpu_vs_throughput(axs[0, 1], df)

    plot_memory(axs[1, 0], df)

    plot_batch_time(axs[1, 1], df)

    plt.tight_layout()

    Path(output).parent.mkdir(parents=True, exist_ok=True)

    fig.savefig(output, dpi=150)

    plt.close(fig)

    print(f"Dashboard saved to {output}")
