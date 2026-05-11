import logging
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table as RichTable

from ingest.config import DB_TABLE

logger = logging.getLogger(__name__)
console = Console()


GRAPH_RULES = [
    # (x_type_check, y_type_check, chart_type, label)
    (lambda x: x.is_integer or x.is_float,
     lambda y: y.is_integer or y.is_float,
     "scatter",
     "scatter plot"),

    (lambda x: x.is_string or x.is_boolean,
     lambda y: y.is_integer or y.is_float,
     "bar",
     "bar chart"),

    (lambda x: "Date" in x.logical_type or "Timestamp" in x.logical_type,
     lambda y: y.is_integer or y.is_float,
     "line",
     "line chart (time series)"),

    (lambda x: x.is_boolean,
     lambda y: y is None,
     "pie",
     "pie chart (boolean distribution)"),

    (lambda x: x.is_integer or x.is_float,
     lambda y: y is None,
     "histogram",
     "histogram"),
]


def recommend_graphs(selected_columns, manifest):
    """
    Returns a list of recommended graph dicts:
    [{"x": col, "y": col_or_None, "type": "scatter", "label": "..."}]
    """
    recommendations = []
    cols = [c for c in selected_columns if c in manifest]

    for i, x_col in enumerate(cols):
        x_meta = manifest[x_col]
        # Two-column charts
        for y_col in cols[i+1:]:
            y_meta = manifest[y_col]
            for x_check, y_check, chart_type, label in GRAPH_RULES:
                if y_check is not None and x_check(x_meta) and y_check(y_meta):
                    recommendations.append({
                        "x": x_col, "y": y_col,
                        "type": chart_type, "label": label
                    })
                    break
        # Single-column charts
        for x_check, y_check, chart_type, label in GRAPH_RULES:
            if y_check is None and x_check(x_meta):
                recommendations.append({
                    "x": x_col, "y": None,
                    "type": chart_type, "label": label
                })
                break

    return recommendations[:12]


def run_graph_wizard(engine, selected_columns, manifest, media_dir):
    recs = recommend_graphs(selected_columns, manifest)

    if recs:
        console.print("\n[bold]Recommended graphs[/bold]")
        t = RichTable(show_header=True)
        t.add_column("#", width=3)
        t.add_column("Type")
        t.add_column("X column")
        t.add_column("Y column")
        for i, r in enumerate(recs):
            t.add_row(str(i+1), r["label"], r["x"], r.get("y") or "—")
        console.print(t)

    choice = Prompt.ask(
        "\n[A]ll recommendations  [S]elect by number  [C]ustom  [K]ip",
        choices=["A", "S", "C", "K", "a", "s", "c", "k"],
        default="A"
    ).upper()

    graphs_to_generate = []

    if choice == "A":
        graphs_to_generate = recs
    elif choice == "S":
        nums = Prompt.ask("Enter numbers (e.g. 1,3,5)")
        indices = [int(x.strip())-1 for x in nums.split(",") if x.strip().isdigit()]
        graphs_to_generate = [recs[i] for i in indices if 0 <= i < len(recs)]
    elif choice == "C":
        graphs_to_generate = _collect_custom_graphs(selected_columns)

    fmt = Prompt.ask("Export format", choices=["png", "jpg"], default="png")

    media_dir = Path(media_dir)
    media_dir.mkdir(parents=True, exist_ok=True)

    for g in graphs_to_generate:
        _generate_graph(engine, g, media_dir, fmt)

    _generate_overview(engine, selected_columns, manifest, media_dir, fmt)


def _collect_custom_graphs(selected_columns):
    graphs = []
    while True:
        x_col = Prompt.ask("x column name")
        if x_col not in selected_columns:
            console.print(f"[red]{x_col} not in selected columns[/red]")
            continue
        y_col = Prompt.ask("y column name (or press enter for single-column)", default="")
        if y_col and y_col not in selected_columns:
            console.print(f"[red]{y_col} not in selected columns[/red]")
            continue
        chart_type = Prompt.ask(
            "chart type",
            choices=["scatter", "bar", "line", "histogram", "pie"],
            default="scatter"
        )
        graphs.append({
            "x": x_col,
            "y": y_col or None,
            "type": chart_type,
            "label": f"custom {chart_type}"
        })
        more = Prompt.ask("Add another? [Y/n]", default="n")
        if more.upper() != "Y":
            break
    return graphs


def _generate_graph(engine, graph_spec, media_dir, fmt):
    x_col = graph_spec["x"]
    y_col = graph_spec.get("y")
    chart_type = graph_spec["type"]
    cols = [x_col] + ([y_col] if y_col else [])

    col_sql = ", ".join(f'"{c}"' for c in cols)
    try:
        df = pd.read_sql(f"SELECT {col_sql} FROM {DB_TABLE} LIMIT 500000", engine)
    except Exception:
        logger.exception("Failed to read columns %s from DB for graph", cols)
        return

    if df.empty:
        logger.warning("No data returned for graph: %s", graph_spec)
        return

    fig, ax = plt.subplots(figsize=(10, 6))

    try:
        if chart_type == "scatter":
            ax.scatter(df[x_col], df[y_col], alpha=0.4, s=6)
            ax.set_xlabel(x_col); ax.set_ylabel(y_col)
        elif chart_type == "bar":
            counts = df[x_col].value_counts().head(30)
            ax.bar(counts.index.astype(str), counts.values)
            ax.set_xlabel(x_col); ax.set_ylabel("count")
            plt.xticks(rotation=45, ha="right")
        elif chart_type == "line":
            df_sorted = df.sort_values(x_col)
            ax.plot(df_sorted[x_col], df_sorted[y_col], linewidth=0.8)
            ax.set_xlabel(x_col); ax.set_ylabel(y_col)
        elif chart_type == "histogram":
            ax.hist(df[x_col].dropna(), bins=50, edgecolor="none")
            ax.set_xlabel(x_col); ax.set_ylabel("frequency")
        elif chart_type == "pie":
            counts = df[x_col].value_counts()
            ax.pie(counts.values, labels=counts.index.astype(str), autopct="%1.1f%%")
    except Exception:
        logger.exception("Failed to generate %s chart for %s", chart_type, graph_spec)
        plt.close(fig)
        return

    safe_name = f"{x_col}_{'vs_' + y_col if y_col else 'dist'}_{chart_type}"
    safe_name = safe_name.replace("/", "_").replace(" ", "_")[:80]
    out_path = Path(media_dir) / f"{safe_name}.{fmt}"

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, format=fmt)
    plt.close(fig)
    logger.info("Graph saved: %s", out_path)


def _generate_overview(engine, selected_columns, manifest, media_dir, fmt):
    """
    One figure per selected column:
    - INT/FLOAT → histogram
    - BOOLEAN → value counts bar
    - STRING → top-20 value counts bar
    - FIXED_LEN_BYTE_ARRAY → skip
    """
    for col in selected_columns:
        meta = manifest.get(col)
        if not meta or meta.is_bit_array:
            continue
        try:
            df = pd.read_sql(
                f'SELECT "{col}" FROM {DB_TABLE} WHERE "{col}" IS NOT NULL LIMIT 100000',
                engine
            )
            if df.empty:
                continue
            fig, ax = plt.subplots(figsize=(8, 4))
            if meta.is_integer or meta.is_float:
                ax.hist(pd.to_numeric(df[col], errors="coerce").dropna(), bins=40)
            elif meta.is_boolean:
                df[col].value_counts().plot(kind="bar", ax=ax)
            else:
                df[col].value_counts().head(20).plot(kind="bar", ax=ax)
                plt.xticks(rotation=45, ha="right")
            ax.set_title(col)
            plt.tight_layout()
            fig.savefig(Path(media_dir) / f"overview_{col}.{fmt}", dpi=100, format=fmt)
            plt.close(fig)
        except Exception:
            logger.exception("Overview generation failed for column %s", col)
