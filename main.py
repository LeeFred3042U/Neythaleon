import sys

import logging


from rich.console import Console

from rich.prompt import Prompt

from sqlalchemy import create_engine, text

from ingest.logging_config import configure_logging

from ingest.config import DATABASE_URL, DB_TABLE, MANIFEST_FILE, MEDIA_DIR

from ingest.manifest import load_manifest, load_manifest_typed

from ingest.ingest_runner import ingest_data

from checkpoint.checkpoint import Checkpoint

from selector.column_selector import run_selector

from selector.recommender import recommend_exclusions

from bitarray.detector import detect_bit_array_columns

from bitarray.metadata_loader import BitArrayMetadata

from bitarray.lookup import attempt_lookup, CONTACT_MESSAGE

from cost.calculator import calculate_estimate, display_estimate

from ingest.runtime_stats import take_snapshot, display_runtime_summary

from plot.cli import main as dashboard_main

console = Console()


def main():

    run_start = take_snapshot()

    configure_logging()

    cp = Checkpoint()

    console.print("\n[bold cyan]═══ Neythaleon Ingestion Pipeline ═══[/bold cyan]\n")

    try:
        manifest_typed = load_manifest_typed(MANIFEST_FILE)

    except FileNotFoundError as e:
        console.print(f"[red]{e}[/red]")

        sys.exit(1)

    manifest_raw = load_manifest(MANIFEST_FILE)

    selected_columns = phase_column_selection(manifest_typed, cp)

    console.print(f"\n[green]✓ {len(selected_columns)} columns selected[/green]")

    bit_metadata = phase_bit_array_intake(selected_columns, manifest_typed, cp)

    costs = phase_cost_estimate(manifest_raw, selected_columns, cp)

    confirm = Prompt.ask("\n[bold]Proceed with ingestion?[/bold] [Y/n]", default="Y")

    if confirm.upper() != "Y":
        console.print("[yellow]Aborted by user.[/yellow]")

        sys.exit(0)

    phase_ingestion(selected_columns, bit_metadata, cp)

    phase_graphs(selected_columns, manifest_typed, cp)

    console.print("\n[bold green]═══ Pipeline complete ═══[/bold green]")

    if costs is not None:
        try:
            display_runtime_summary(run_start, take_snapshot(), costs, {})
        except Exception:
            logging.exception("Runtime summary failed (non-fatal)")


def phase_column_selection(manifest, cp):

    action = cp.prompt_resume_or_overwrite("column_selection", "Column selection")

    if action == "abort":
        sys.exit(0)

    if action == "resume":
        state = cp.load("column_selection")

        return state["selected_columns"]

    exclusion_candidates = recommend_exclusions(manifest)

    if exclusion_candidates:
        console.print(
            f"\n[yellow bold]⚠  {len(exclusion_candidates)} low-density column(s) suggested for exclusion[/yellow bold]"
            " [dim](< 5% non-null across all files)[/dim]"
        )
        for col in exclusion_candidates:
            console.print(f"  [dim]• {col}[/dim]")
        console.print()

    console.print("\n[bold]Column selection mode[/bold]")
    console.print("  [1] AI advisor   [dim](recommends columns based on your goal)[/dim]")
    console.print("  [2] Manual       [dim](interactive search or paste list)[/dim]\n")

    mode = Prompt.ask("Mode", choices=["1", "2"], default="2")

    selected = None

    if mode == "1":
        try:
            from ai_advisor import ai_column_advisor  # noqa: PLC0415

            selected = ai_column_advisor(manifest)

        except Exception:
            logging.exception("AI column advisor raised unexpectedly — falling back to manual")

    if selected is None:
        selected = run_selector(manifest)

    cp.save("column_selection", {"completed": True, "selected_columns": selected})

    return selected


def phase_bit_array_intake(selected_columns, manifest, cp):

    action = cp.prompt_resume_or_overwrite("bit_array_intake", "Bit-array intake")

    if action == "abort":
        sys.exit(0)

    if action == "resume":
        state = cp.load("bit_array_intake")

        if not state.get("bit_array_columns"):
            return None

        enc_file = state.get("encoding_file", "")

        if enc_file and enc_file != "<auto-resolved>":
            return BitArrayMetadata(enc_file)

        return None

    bit_cols = detect_bit_array_columns(selected_columns, manifest)

    if not bit_cols:
        cp.save(
            "bit_array_intake",
            {"completed": True, "bit_array_columns": [], "resolved": True},
        )

        console.print("[green]✓ No bit-array columns in selection[/green]")

        return None

    console.print(f"\n[bold]Bit-array columns in selection:[/bold] {bit_cols}")

    encoding_path = Prompt.ask(
        "Path to encoding metadata file (JSON), or press enter to attempt lookup",
        default="",
    )

    metadata = None

    if encoding_path:
        try:
            metadata = BitArrayMetadata(encoding_path)

            errors = metadata.validate_against_manifest(bit_cols, manifest)

            if errors:
                for e in errors:
                    console.print(f"[red]Validation error: {e}[/red]")

                metadata = None

        except Exception as exc:
            console.print(f"[red]Failed to load encoding file: {exc}[/red]")

    if metadata is None:
        metadata = attempt_lookup(bit_cols, manifest)

    if metadata is None:
        unresolved_str = "\n    ".join(bit_cols)

        console.print(CONTACT_MESSAGE.format(columns=unresolved_str))

        raise SystemExit(1)

    cp.save(
        "bit_array_intake",
        {
            "completed": True,
            "bit_array_columns": bit_cols,
            "encoding_file": encoding_path or "<auto-resolved>",
            "resolved": True,
        },
    )

    console.print("[green]✓ Bit-array encoding resolved[/green]")

    return metadata


def phase_cost_estimate(manifest_raw, selected_columns, cp):

    action = cp.prompt_resume_or_overwrite("cost_estimate", "Cost estimate")

    if action == "abort":
        sys.exit(0)

    if action == "resume":
        state = cp.load("cost_estimate")

        costs = state.get("estimate", {})

        if costs:
            display_estimate(costs)

        return costs or None

    costs = calculate_estimate(manifest_raw, selected_columns)

    display_estimate(costs)

    cp.save(
        "cost_estimate",
        {
            "completed": True,
            "estimate": costs,
        },
    )

    return costs


def phase_ingestion(selected_columns, bit_metadata, cp):

    action = cp.prompt_resume_or_overwrite("ingestion", "Ingestion")

    if action == "abort":
        sys.exit(0)

    if action == "resume":
        state = cp.load("ingestion")

        if state and state.get("completed"):
            console.print("[green]✓ Ingestion already complete[/green]")

            return

    elif action == "overwrite":
        cp.clear_phase("ingestion")

        try:
            engine = create_engine(DATABASE_URL)

            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {DB_TABLE}"))

        except Exception as e:
            console.print(f"[yellow]Could not drop table {DB_TABLE}: {e}[/yellow]")

    console.print("\n[bold]Starting ingestion...[/bold]")

    metrics = ingest_data(
        selected_columns=selected_columns,
        bit_array_metadata=bit_metadata,
        checkpoint=cp,
    )

    console.print(
        f"[green]✓ Ingestion complete — {len(metrics)} chunks processed[/green]"
    )


def phase_graphs(selected_columns, manifest, cp):

    action = cp.prompt_resume_or_overwrite("graph_generation", "Graph generation")

    if action == "abort":
        return

    if action == "resume":
        state = cp.load("graph_generation")

        if state and state.get("completed"):
            console.print("[green]✓ Graphs already generated[/green]")

            return

    console.print("\n[bold]Generating performance dashboard...[/bold]")

    try:
        dashboard_main()

        console.print("[green]✓ Dashboard saved[/green]")

    except Exception:
        logging.exception("Dashboard generation failed (non-fatal)")

    try:
        from plot.user_graphs import run_graph_wizard

        engine = create_engine(DATABASE_URL)

        run_graph_wizard(engine, selected_columns, manifest, MEDIA_DIR)

    except Exception:
        logging.exception("Graph wizard failed (non-fatal)")

    cp.save("graph_generation", {"completed": True})

    console.print("[green]✓ Graph generation complete[/green]")


if __name__ == "__main__":
    main()
