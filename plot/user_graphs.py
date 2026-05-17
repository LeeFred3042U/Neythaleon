"""
plot/user_graphs.py — Graph Studio interactive session for Neythaleon.

Menu:
  [A] Generate all AI recommendations
  [S] Select by number (supports ranges: 1,3,5-8,10)
  [C] Custom graph  (optionally ask AI for a better chart type)
  [Q] AI query builder
  [P] Preview recommendation details
  [K] Done / exit graph studio
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table as RichTable

from ingest.config import DB_TABLE, GRAPH_SAMPLE_LIMIT

logger = logging.getLogger(__name__)
console = Console()

GRAPH_RULES = [
    (
        lambda x: x.is_integer or x.is_float,
        lambda y: y.is_integer or y.is_float,
        "scatter",
        "scatter plot",
    ),
    (
        lambda x: x.is_string or x.is_boolean,
        lambda y: y.is_integer or y.is_float,
        "bar",
        "bar chart",
    ),
    (
        lambda x: "DateTime" in x.logical_type or "Timestamp" in x.logical_type,
        lambda y: y.is_integer or y.is_float,
        "line",
        "line chart (time series)",
    ),
    (
        lambda x: x.is_boolean,
        lambda y: y is None,
        "pie",
        "pie chart (boolean distribution)",
    ),
    (
        lambda x: x.is_integer or x.is_float,
        lambda y: y is None,
        "histogram",
        "histogram",
    ),
]


def recommend_graphs_fallback(selected_columns: list, manifest: dict) -> list:
    recommendations = []
    cols = [c for c in selected_columns if c in manifest]

    for i, x_col in enumerate(cols):
        x_meta = manifest[x_col]

        for y_col in cols[i + 1 :]:
            y_meta = manifest[y_col]

            for x_check, y_check, chart_type, label in GRAPH_RULES:
                if y_check is not None and x_check(x_meta) and y_check(y_meta):
                    recommendations.append(
                        {
                            "x": x_col,
                            "y": y_col,
                            "type": chart_type,
                            "label": label,
                            "reason": "Rule-based recommendation (no AI)",
                        }
                    )
                    break

        for x_check, y_check, chart_type, label in GRAPH_RULES:
            if y_check is None and x_check(x_meta):
                recommendations.append(
                    {
                        "x": x_col,
                        "y": None,
                        "type": chart_type,
                        "label": label,
                        "reason": "Rule-based recommendation (no AI)",
                    }
                )
                break

    return recommendations[:12]


def run_graph_wizard(engine, selected_columns: list, manifest: dict, media_dir) -> None:
    media_dir = Path(media_dir)
    try:
        media_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass

    recs: list = []
    try:
        from ai_advisor import ai_graph_advisor  # noqa: PLC0415

        ai_recs = ai_graph_advisor(selected_columns, manifest)
        if ai_recs:
            recs = ai_recs
            logger.info("Using %d AI graph recommendations", len(recs))
        else:
            logger.info("AI graph advisor unavailable or returned <2 graphs — using fallback")
            recs = recommend_graphs_fallback(selected_columns, manifest)
    except Exception:
        logger.exception("ai_graph_advisor raised unexpectedly — using rule-based fallback")
        recs = recommend_graphs_fallback(selected_columns, manifest)

    fmt = Prompt.ask("\nExport format", choices=["png", "jpg"], default="png")

    session_files: list[Path] = []
    session_count = 0

    while True:
        _print_studio_header(len(recs), session_count)

        choice = Prompt.ask(
            "Choice",
            choices=["A", "S", "C", "Q", "P", "K", "a", "s", "c", "q", "p", "k"],
            default="A",
        ).upper()

        if choice == "K":
            break

        elif choice == "A":
            if not recs:
                console.print("[yellow]No recommendations available.[/yellow]")
                continue
            new_files = _generate_batch(engine, recs, media_dir, fmt)
            session_files.extend(new_files)
            session_count += len(new_files)
            console.print(
                f"[green]✓ Generated {len(new_files)} graph(s) this batch "
                f"({session_count} total this session)[/green]"
            )

        elif choice == "S":
            if not recs:
                console.print("[yellow]No recommendations to select from.[/yellow]")
                continue
            _print_recs_table(recs)
            nums_raw = Prompt.ask("Enter numbers / ranges (e.g. 1,3,5-8,10)")
            indices = _parse_range_string(nums_raw, max_val=len(recs))
            subset = [recs[i] for i in indices if 0 <= i < len(recs)]
            if not subset:
                console.print("[yellow]No valid selections.[/yellow]")
                continue
            new_files = _generate_batch(engine, subset, media_dir, fmt)
            session_files.extend(new_files)
            session_count += len(new_files)
            console.print(
                f"[green]✓ Generated {len(new_files)} graph(s) "
                f"({session_count} total this session)[/green]"
            )

        elif choice == "C":
            custom = _collect_custom_graphs(selected_columns, manifest)
            if custom:
                new_files = _generate_batch(engine, custom, media_dir, fmt)
                session_files.extend(new_files)
                session_count += len(new_files)
                console.print(
                    f"[green]✓ Generated {len(new_files)} custom graph(s) "
                    f"({session_count} total this session)[/green]"
                )

        elif choice == "Q":
            _run_ai_query(engine, selected_columns)

        elif choice == "P":
            if not recs:
                console.print("[yellow]No recommendations to preview.[/yellow]")
                continue
            _preview_recommendations(recs, manifest)

    _print_closing_summary(session_files, media_dir)


def _print_studio_header(n_recs: int, session_count: int) -> None:
    tag = f"[dim]{n_recs} AI recommendations loaded[/dim]" if n_recs else "[dim]no recommendations[/dim]"
    count_tag = f"  [bold green]{session_count} generated this session[/bold green]" if session_count else ""
    menu = (
        "[bold]  [A][/bold] Generate all recommendations\n"
        "[bold]  [S][/bold] Select by number\n"
        "[bold]  [C][/bold] Custom graph\n"
        "[bold]  [Q][/bold] AI query builder\n"
        "[bold]  [P][/bold] Preview recommendation details\n"
        "[bold]  [K][/bold] Done / exit graph studio\n"
    )
    console.print(
        Panel(
            menu + f"\n{tag}{count_tag}",
            title="[bold cyan]═══ Graph Studio ═══[/bold cyan]",
            border_style="cyan",
            padding=(1, 2),
        )
    )


def _preview_recommendations(recs: list, manifest: dict) -> None:
    _print_recs_table(recs)

    raw = Prompt.ask("Enter numbers to preview (blank = all)", default="")
    if raw.strip():
        indices = _parse_range_string(raw, max_val=len(recs))
        subset = [recs[i] for i in indices if 0 <= i < len(recs)]
    else:
        subset = recs

    for i, spec in enumerate(subset):
        x_meta = manifest.get(spec["x"])
        y_meta = manifest.get(spec["y"]) if spec.get("y") else None

        x_info = (
            f"[cyan]{spec['x']}[/cyan] ({x_meta.physical_type}, "
            f"density {x_meta.density_pct:.1f}%)"
            if x_meta
            else f"[cyan]{spec['x']}[/cyan]"
        )
        y_info = (
            f"[cyan]{spec['y']}[/cyan] ({y_meta.physical_type}, "
            f"density {y_meta.density_pct:.1f}%)"
            if y_meta
            else "[dim]—  (single-column chart)[/dim]"
        )

        hint = _pattern_hint(spec)

        body = (
            f"  [bold]Chart type:[/bold] {spec['type']}\n"
            f"  [bold]X axis:[/bold]    {x_info}\n"
            f"  [bold]Y axis:[/bold]    {y_info}\n"
            f"  [bold]AI reason:[/bold] {spec.get('reason', '—')}\n"
            f"  [bold]Look for:[/bold]  [dim italic]{hint}[/dim italic]"
        )
        console.print(
            Panel(
                body,
                title=f"[bold magenta]#{recs.index(spec)+1} — {spec['label']}[/bold magenta]",
                border_style="magenta",
                padding=(0, 2),
            )
        )

    Prompt.ask("[dim]Press enter to return to menu[/dim]", default="")


def _pattern_hint(spec: dict) -> str:
    hints = {
        "scatter": "Look for correlation clusters or outlier cells with extreme record counts.",
        "bar": "Identify dominant categories; long tails reveal taxonomic diversity.",
        "line": "Spot temporal trends — rising or declining occurrence over years.",
        "histogram": "Check for skewness; a heavy right tail is common in occurrence data.",
        "pie": "Relative proportions — e.g. GBIF vs OBIS source balance.",
    }
    return hints.get(spec.get("type", ""), "Explore the distribution.")


def _print_recs_table(recs: list) -> None:
    t = RichTable(show_header=True, header_style="bold magenta", border_style="bright_black")
    t.add_column("#", width=3, justify="right")
    t.add_column("Type", min_width=10)
    t.add_column("X column", min_width=16)
    t.add_column("Y column", min_width=16)
    t.add_column("Label")

    for i, r in enumerate(recs):
        t.add_row(
            str(i + 1),
            r["type"],
            r["x"],
            r.get("y") or "—",
            r.get("label", ""),
        )

    console.print()
    console.print(t)


def _parse_range_string(raw: str, max_val: int) -> list[int]:
    indices = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            parts = token.split("-", 1)
            try:
                lo, hi = int(parts[0].strip()), int(parts[1].strip())
                for v in range(lo, hi + 1):
                    if 1 <= v <= max_val:
                        indices.add(v - 1)
            except ValueError:
                pass
        else:
            try:
                v = int(token)
                if 1 <= v <= max_val:
                    indices.add(v - 1)
            except ValueError:
                pass
    return sorted(indices)


def _collect_custom_graphs(selected_columns: list, manifest: dict) -> list:
    graphs = []

    while True:
        x_col = Prompt.ask("X column name")
        if x_col not in selected_columns:
            console.print(f"[red]'{x_col}' is not in selected columns.[/red]")
            continue

        y_col = Prompt.ask("Y column name (or press enter for single-column)", default="")
        if y_col and y_col not in selected_columns:
            console.print(f"[red]'{y_col}' is not in selected columns.[/red]")
            continue

        chart_type = Prompt.ask(
            "Chart type",
            choices=["scatter", "bar", "line", "histogram", "pie"],
            default="scatter",
        )

        ai_suggestion = None
        try:
            ask_ai = Prompt.ask(
                "Ask AI for a better chart type suggestion? [Y/n]", default="n"
            ).upper()
            if ask_ai == "Y":
                ai_suggestion = _ai_chart_type_suggestion(
                    x_col, y_col or None, chart_type, manifest
                )
        except Exception:
            logger.debug("AI chart-type suggestion failed; continuing with user choice")

        if ai_suggestion and ai_suggestion["type"] != chart_type:
            console.print(
                Panel(
                    f"AI suggests [bold cyan]{ai_suggestion['type']}[/bold cyan] instead of "
                    f"[bold]{chart_type}[/bold].\n"
                    f"[dim]{ai_suggestion.get('reason', '')}[/dim]",
                    title="[cyan]AI Chart Type Suggestion[/cyan]",
                    border_style="cyan",
                )
            )
            use_ai = Prompt.ask(
                "Use AI suggestion?", choices=["Y", "N", "y", "n"], default="Y"
            ).upper()
            if use_ai == "Y":
                chart_type = ai_suggestion["type"]

        graphs.append(
            {
                "x": x_col,
                "y": y_col or None,
                "type": chart_type,
                "label": f"custom {chart_type}",
                "reason": "User-specified custom graph",
            }
        )

        more = Prompt.ask("Add another? [Y/n]", default="n")
        if more.upper() != "Y":
            break

    return graphs


def _ai_chart_type_suggestion(
    x_col: str, y_col: str | None, current_type: str, manifest: dict
) -> dict | None:
    try:
        from ai_advisor import LLMClient, _parse_json_response  # noqa: PLC0415

        x_meta = manifest.get(x_col)
        y_meta = manifest.get(y_col) if y_col else None

        x_desc = (
            f"{x_col} ({x_meta.physical_type}, density {x_meta.density_pct:.1f}%)"
            if x_meta
            else x_col
        )
        y_desc = (
            f"{y_col} ({y_meta.physical_type}, density {y_meta.density_pct:.1f}%)"
            if y_meta
            else "none (single-column chart)"
        )

        system_prompt = (
            "You are a data visualisation expert. Given two columns from an OBIS marine "
            "biodiversity dataset, recommend the best chart type from: scatter, bar, line, "
            "histogram, pie.\n"
            "Respond with STRICT JSON only:\n"
            '{"type": "scatter|bar|line|histogram|pie", "reason": "..."}'
        )
        user_prompt = (
            f"X column: {x_desc}\n"
            f"Y column: {y_desc}\n"
            f"User chose: {current_type}\n"
            "Is there a better chart type?"
        )

        client = LLMClient()
        raw = client.chat(system=system_prompt, user=user_prompt, max_tokens=256)
        parsed = _parse_json_response(raw, context="ai_chart_type_suggestion")
        if isinstance(parsed, dict) and parsed.get("type") in {
            "scatter", "bar", "line", "histogram", "pie"
        }:
            return parsed
    except Exception:
        logger.debug("_ai_chart_type_suggestion failed silently")
    return None


def _run_ai_query(engine, selected_columns: list) -> None:
    try:
        from ai_advisor import ai_query_builder  # noqa: PLC0415

        sql = ai_query_builder(engine, selected_columns, DB_TABLE)
        if sql is None:
            console.print("[yellow]AI query builder returned no SQL.[/yellow]")
            return

        console.print(
            Panel(
                f"[bold cyan]Generated SQL:[/bold cyan]\n\n[green]{sql}[/green]",
                border_style="cyan",
            )
        )

        run_it = Prompt.ask(
            "Execute this query and display results? [Y/n]", default="Y"
        ).upper()

        if run_it != "Y":
            return

        try:
            df = pd.read_sql(sql, engine)
            if df.empty:
                console.print("[yellow]Query returned no rows.[/yellow]")
                return

            display_df = df.head(50)
            t = RichTable(
                title=f"Query results ({len(df)} rows total, showing ≤50)",
                show_header=True,
                header_style="bold magenta",
                border_style="bright_black",
            )
            for col in display_df.columns:
                t.add_column(str(col))
            for _, row in display_df.iterrows():
                t.add_row(*[str(v) for v in row])
            console.print(t)

        except Exception as exc:
            logger.exception("Query execution failed")
            console.print(f"[red]Query execution error: {exc}[/red]")

    except Exception:
        logger.exception("AI query builder raised unexpectedly")
        console.print("[red]AI query builder failed unexpectedly.[/red]")


def _generate_batch(engine, graph_specs: list, media_dir: Path, fmt: str) -> list[Path]:
    saved = []
    for spec in graph_specs:
        path = _generate_graph(engine, spec, media_dir, fmt)
        if path is not None:
            saved.append(path)
    return saved


def _generate_graph(engine, graph_spec: dict, media_dir: Path, fmt: str) -> Path | None:
    x_col = graph_spec["x"]
    y_col = graph_spec.get("y")
    chart_type = graph_spec["type"]

    cols = [x_col] + ([y_col] if y_col else [])
    col_sql = ", ".join(f'"{c}"' for c in cols)

    try:
        df = pd.read_sql(
            f"SELECT {col_sql} FROM {DB_TABLE} LIMIT {GRAPH_SAMPLE_LIMIT}", engine
        )
    except Exception:
        logger.exception("Failed to read columns %s from DB for graph", cols)
        return None

    if df.empty:
        logger.warning("No data returned for graph: %s", graph_spec)
        return None

    fig, ax = plt.subplots(figsize=(10, 6))

    try:
        if chart_type == "scatter":
            ax.scatter(df[x_col], df[y_col], alpha=0.4, s=6)
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)

        elif chart_type == "bar":
            counts = df[x_col].value_counts().head(30)
            ax.bar(counts.index.astype(str), counts.values)
            ax.set_xlabel(x_col)
            ax.set_ylabel("count")
            plt.xticks(rotation=45, ha="right")

        elif chart_type == "line":
            df_sorted = df.sort_values(x_col)
            ax.plot(df_sorted[x_col], df_sorted[y_col], linewidth=0.8)
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)

        elif chart_type == "histogram":
            ax.hist(df[x_col].dropna(), bins=50, edgecolor="none")
            ax.set_xlabel(x_col)
            ax.set_ylabel("frequency")

        elif chart_type == "pie":
            counts = df[x_col].value_counts()
            ax.pie(counts.values, labels=counts.index.astype(str), autopct="%1.1f%%")

    except Exception:
        logger.exception("Failed to generate %s chart for %s", chart_type, graph_spec)
        plt.close(fig)
        return None

    ax.set_title(graph_spec.get("label", f"{x_col} — {chart_type}"))

    safe_name = f"{x_col}_{'vs_' + y_col if y_col else 'dist'}_{chart_type}"
    safe_name = safe_name.replace("/", "_").replace(" ", "_")[:80]
    out_path = media_dir / f"{safe_name}.{fmt}"

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, format=fmt)
    plt.close(fig)

    logger.info("Graph saved: %s", out_path)
    console.print(f"  [dim]Saved:[/dim] {out_path.name}")
    return out_path


def _generate_overview(engine, selected_columns, manifest, media_dir, fmt):
    for col in selected_columns:
        meta = manifest.get(col)

        if not meta or meta.is_bit_array:
            continue

        try:
            df = pd.read_sql(
                f'SELECT "{col}" FROM {DB_TABLE} WHERE "{col}" IS NOT NULL LIMIT 100000',
                engine,
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


def _print_closing_summary(session_files: list[Path], media_dir: Path) -> None:
    if not session_files:
        console.print(
            Panel(
                "[dim]No graphs were generated this session.[/dim]",
                title="[bold cyan]Graph Studio — Session Summary[/bold cyan]",
                border_style="cyan",
            )
        )
        return

    lines = [f"[green]✓[/green] [link={f.as_uri()}]{f.name}[/link]" for f in session_files]
    body = f"[bold]{len(session_files)} graph(s) saved to:[/bold] {media_dir}\n\n" + "\n".join(lines)

    console.print(
        Panel(
            body,
            title="[bold cyan]Graph Studio — Session Summary[/bold cyan]",
            border_style="cyan",
        )
    )

    open_folder = Prompt.ask(
        "Open output folder?", choices=["Y", "N", "y", "n"], default="n"
    ).upper()

    if open_folder == "Y":
        _open_folder(media_dir)


def _open_folder(path: Path) -> None:
    try:
        if sys.platform.startswith("win"):
            os.startfile(str(path))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception as exc:
        logger.debug("Could not open folder: %s", exc)
        console.print(f"[yellow]Could not open folder automatically: {exc}[/yellow]")
