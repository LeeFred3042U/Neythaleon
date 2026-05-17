import math

from rich.console import Console
from rich.table import Table as RichTable

console = Console()

_STORAGE_INR_PER_GB_MONTH: float = 1.90
_NETWORK_INR_PER_GB: float = 7.50

_UNOPT_BYTES: dict[str, int] = {
    "INT64": 8,
    "DOUBLE": 8,
    "INT32": 4,
    "FLOAT": 4,
    "BOOLEAN": 1,
    "BYTE_ARRAY": 15,
    "FIXED_LEN_BYTE_ARRAY": 0,
}

_OPT_BYTES: dict[str, int] = {
    "INT64": 8,
    "DOUBLE": 8,
    "INT32": 4,
    "FLOAT": 4,
    "BOOLEAN": 1,
    "BYTE_ARRAY": 4,
    "FIXED_LEN_BYTE_ARRAY": 0,
}


def _col_bytes(col_meta: dict, table: dict[str, int]) -> int:
    ptype = col_meta.get("physical_type", "")
    if ptype == "FIXED_LEN_BYTE_ARRAY":
        return col_meta.get("type_length", 0)
    return table.get(ptype, 8)


def calculate_estimate(manifest_raw: dict, selected_columns: list) -> dict:
    cols_manifest: dict = manifest_raw.get("columns", {})
    total_rows: int = manifest_raw.get("total_rows", 0)
    total_size_bytes: int = manifest_raw.get("total_size_bytes", 0)
    all_col_names = list(cols_manifest.keys())

    sel_set = set(selected_columns)

    pg_raw_bytes: int = 0
    pg_opt_bytes: int = 0
    sel_non_null_bytes: int = 0
    all_non_null_bytes: int = 0

    for name, meta in cols_manifest.items():
        non_null = meta.get("total_values", 0) - meta.get("total_nulls", 0)
        non_null = max(non_null, 0)

        unopt_w = _col_bytes(meta, _UNOPT_BYTES)
        opt_w = _col_bytes(meta, _OPT_BYTES)

        col_unopt_bytes = non_null * unopt_w
        col_opt_bytes = non_null * opt_w

        all_non_null_bytes += col_unopt_bytes

        if name in sel_set:
            pg_raw_bytes += col_unopt_bytes
            pg_opt_bytes += col_opt_bytes
            sel_non_null_bytes += col_unopt_bytes

    n_sel = max(len(selected_columns), 1)
    tuple_overhead = total_rows * (24 + math.ceil(n_sel / 8))
    pg_raw_bytes += tuple_overhead
    pg_opt_bytes += tuple_overhead

    if all_non_null_bytes > 0:
        col_fraction = sel_non_null_bytes / all_non_null_bytes
    else:
        col_fraction = len(selected_columns) / max(len(all_col_names), 1)

    network_bytes = total_size_bytes * col_fraction
    network_gb = network_bytes / 1024**3

    pg_raw_gb = pg_raw_bytes / 1024**3
    pg_opt_gb = pg_opt_bytes / 1024**3

    storage_raw_inr = pg_raw_gb * _STORAGE_INR_PER_GB_MONTH
    storage_opt_inr = pg_opt_gb * _STORAGE_INR_PER_GB_MONTH
    network_inr = network_gb * _NETWORK_INR_PER_GB

    if total_rows == 0:
        console.print(
            "[yellow]Warning: total_rows not in manifest — storage estimates may be zero.[/yellow]"
        )

    return {
        "pg_raw_gb": round(pg_raw_gb, 4),
        "pg_optimised_gb": round(pg_opt_gb, 4),
        "network_gb": round(network_gb, 4),
        "storage_raw_inr": round(storage_raw_inr, 4),
        "storage_optimised_inr": round(storage_opt_inr, 4),
        "network_inr": round(network_inr, 4),
        "_meta": {
            "selected_columns": len(selected_columns),
            "total_columns": len(all_col_names),
            "pg_raw_gb": round(pg_raw_gb, 4),
            "pg_optimised_gb": round(pg_opt_gb, 4),
            "network_gb": round(network_gb, 4),
        },
    }


def display_estimate(costs: dict) -> None:
    t = RichTable(
        title="Pre-transfer cost estimate (₹)",
        show_header=True,
        header_style="bold magenta",
        border_style="bright_black",
    )
    t.add_column("Item", min_width=32)
    t.add_column("₹ / month", justify="right", min_width=14)

    rows = [
        ("Storage — optimised (pg_optimised_gb)", costs["storage_optimised_inr"]),
        ("Storage — unoptimised (pg_raw_gb)", costs["storage_raw_inr"]),
        ("Network transfer (egress)", costs["network_inr"]),
    ]

    for label, inr in rows:
        t.add_row(label, f"₹{inr:.4f}")

    console.print()
    console.print(t)

    m = costs["_meta"]
    console.print(
        f"[dim]"
        f"{m['selected_columns']}/{m['total_columns']} columns — "
        f"PG optimised ~{m['pg_optimised_gb']:.3f} GB, "
        f"PG raw ~{m['pg_raw_gb']:.3f} GB, "
        f"network ~{m['network_gb']:.3f} GB"
        f"[/dim]"
    )
    console.print(
        "[dim italic]Compute cost shown at pipeline exit based on actual runtime.[/dim italic]"
    )
