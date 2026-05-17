import time

import psutil

from rich.console import Console
from rich.panel import Panel
from rich.table import Table as RichTable

console = Console()

_CPU_INR_PER_VCPU_HOUR: float = 3.60
_RAM_INR_PER_GB_HOUR: float = 0.48


def take_snapshot() -> dict:
    proc = psutil.Process()
    cpu = proc.cpu_times()
    mem = proc.memory_info()

    net = psutil.net_io_counters()
    disk = psutil.disk_io_counters()

    return {
        "wall_time": time.monotonic(),
        "cpu_user": cpu.user,
        "cpu_system": cpu.system,
        "rss_bytes": mem.rss,
        "net_sent": net.bytes_sent,
        "net_recv": net.bytes_recv,
        "disk_read": disk.read_bytes if disk else 0,
        "disk_write": disk.write_bytes if disk else 0,
    }


def display_runtime_summary(start: dict, end: dict, cost_estimate: dict, rates: dict) -> None:
    elapsed_sec: float = end["wall_time"] - start["wall_time"]
    cpu_sec: float = (
        (end["cpu_user"] - start["cpu_user"])
        + (end["cpu_system"] - start["cpu_system"])
    )
    peak_rss_bytes: int = end["rss_bytes"]

    delta_net_sent: int = max(end["net_sent"] - start["net_sent"], 0)
    delta_disk_write: int = max(end["disk_write"] - start["disk_write"], 0)

    elapsed_hours = elapsed_sec / 3600.0
    cpu_cores = psutil.cpu_count(logical=False) or 1
    peak_rss_gb = peak_rss_bytes / 1024**3

    compute_inr = (
        elapsed_hours * cpu_cores * _CPU_INR_PER_VCPU_HOUR
        + elapsed_hours * peak_rss_gb * _RAM_INR_PER_GB_HOUR
    )

    network_inr = cost_estimate.get("network_inr", 0.0)
    storage_opt_inr = cost_estimate.get("storage_optimised_inr", 0.0)
    total_inr = compute_inr + network_inr + storage_opt_inr

    elapsed_str = _format_elapsed(int(elapsed_sec))

    t = RichTable.grid(padding=(0, 2))
    t.add_column(style="bold", min_width=22)
    t.add_column(justify="right")

    t.add_row("Time taken", elapsed_str)
    t.add_row("CPU time", f"{cpu_sec:.1f} s")
    t.add_row("Peak RSS", f"{peak_rss_bytes / 1024 / 1024:.1f} MB")
    t.add_row("Network sent", f"{delta_net_sent / 1024 / 1024:.2f} MB")
    t.add_row("Disk written", f"{delta_disk_write / 1024 / 1024:.2f} MB")
    t.add_row("", "")
    t.add_row("Compute cost", f"₹{compute_inr:.4f}")
    t.add_row("Total run cost", f"[bold green]₹{total_inr:.4f}[/bold green]")

    console.print(
        Panel(
            t,
            title="[bold cyan]Pipeline runtime summary[/bold cyan]",
            border_style="cyan",
            padding=(1, 2),
        )
    )


def _format_elapsed(total_seconds: int) -> str:
    if total_seconds <= 0:
        return "0s"

    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)

    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")

    return " ".join(parts)
