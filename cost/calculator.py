from rich.console import Console

from rich.prompt import Prompt

from rich.table import Table as RichTable

from ingest.config import EXPECTED_THROUGHPUT_RPS

console = Console()

DEFAULT_RATES = {
    "storage_gb_month": 0.023,
    "network_gb": 0.09,
    "cpu_hour": 0.048,
    "ram_gb_hour": 0.006,
}


def prompt_rates(cached=None):

    rates = dict(DEFAULT_RATES)

    if cached:
        rates.update(cached)

    console.print(
        "\n[bold]Cost rate inputs[/bold] (press enter to accept default/cached value)"
    )

    fields = [
        ("storage_gb_month", "Storage cost per GB/month (Rupees)"),
        ("network_gb", "Network transfer cost per GB (Rupees)"),
        ("cpu_hour", "CPU cost per hour (Rupees)"),
        ("ram_gb_hour", "RAM cost per GB/hour (Rupees)"),
    ]

    for key, label in fields:
        current = rates[key]

        raw = Prompt.ask(f"  {label}", default=str(current))

        try:
            rates[key] = float(raw)

        except ValueError:
            console.print(f"  [yellow]Invalid value, using {current}[/yellow]")

    return rates


def calculate_estimate(
    manifest_raw,
    selected_columns,
    rates,
    expected_throughput_rps=EXPECTED_THROUGHPUT_RPS,
):

    storage = manifest_raw.get("storage_estimate_gb", {})

    total_rows = manifest_raw.get("total_rows", 0)

    parquet_gb = storage.get("parquet_gb", 0.0)

    all_cols = list(manifest_raw.get("columns", {}).keys())

    col_fraction = len(selected_columns) / max(len(all_cols), 1)

    col_fraction = max(col_fraction, 0.001)

    pg_opt_gb = storage.get("pg_optimised_gb", 0.0) * col_fraction

    pg_unopt_gb = storage.get("pg_unoptimised_gb", 0.0) * col_fraction

    parquet_sel_gb = parquet_gb * col_fraction

    est_hours = (total_rows / max(expected_throughput_rps, 1)) / 3600

    ram_gb_estimate = 2.0

    costs = {
        "storage_optimised_monthly": round(pg_opt_gb * rates["storage_gb_month"], 4),
        "storage_unoptimised_monthly": round(
            pg_unopt_gb * rates["storage_gb_month"], 4
        ),
        "network_transfer": round(parquet_sel_gb * rates["network_gb"], 4),
        "compute_cpu": round(est_hours * rates["cpu_hour"], 4),
        "compute_ram": round(est_hours * ram_gb_estimate * rates["ram_gb_hour"], 4),
    }

    costs["total_estimate"] = round(sum(costs.values()), 4)

    costs["_meta"] = {
        "selected_columns": len(selected_columns),
        "total_columns": len(all_cols),
        "col_fraction": round(col_fraction, 3),
        "pg_opt_gb": round(pg_opt_gb, 4),
        "parquet_selected_gb": round(parquet_sel_gb, 4),
        "est_compute_hours": round(est_hours, 3),
    }

    if total_rows == 0:
        console.print(
            "[yellow]Warning: total_rows not available — compute estimate is $0.00 (unreliable)[/yellow]"
        )

    return costs


def display_estimate(costs):

    t = RichTable(title="Pre-transfer cost estimate (Rupees)", show_header=True)

    t.add_column("Item")

    t.add_column("Est. Rupees", justify="right")

    labels = {
        "storage_optimised_monthly": "Storage (Optimised) Monthly",
        "storage_unoptimised_monthly": "Storage (Unoptimised) Monthly",
        "network_transfer": "Network Transfer",
        "compute_cpu": "Compute (CPU)",
        "compute_ram": "Compute (RAM)",
        "total_estimate": "Total Estimate",
    }

    for key, label in labels.items():
        t.add_row(label, f"${costs[key]:.4f}")

    console.print(t)

    m = costs["_meta"]

    console.print(
        f"[dim]Based on {m['selected_columns']}/{m['total_columns']} columns "
        f"({m['col_fraction'] * 100:.1f}%), "
        f"~{m['pg_opt_gb']:.3f} GB PG (optimised), "
        f"~{m['est_compute_hours']:.2f} compute-hours estimated[/dim]"
    )
