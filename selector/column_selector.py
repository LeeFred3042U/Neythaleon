import difflib

from rich.console import Console
from rich.prompt import Prompt

console = Console()


def run_selector(manifest):
    """
    Interactive column selection.
    Returns a sorted list of selected column names.
    Raises ValueError if no columns are selected.
    
    Args:
        manifest: dict of {col_name: ColumnMeta}
    """
    all_columns = sorted(manifest.keys())
    console.print(f"\n[bold]Column selection[/bold] — {len(all_columns)} columns available")
    console.print("  [A] Interactive search  [B] Paste comma-separated names\n")

    mode = Prompt.ask("Mode", choices=["A", "B", "a", "b"]).upper()

    if mode == "A":
        return _interactive_selector(all_columns)
    else:
        return _manual_entry(all_columns)


def _interactive_selector(all_columns):
    """
    Rich-based scrollable search selector.
    Uses questionary if available, falls back to text entry.
    """
    try:
        import questionary
        selected = questionary.checkbox(
            "Select columns (space to toggle, enter to confirm):",
            choices=all_columns,
            validate=lambda x: True if len(x) > 0 else "Select at least one column"
        ).ask()
        if selected is None:
            raise KeyboardInterrupt
        return sorted(selected)
    except ImportError:
        console.print("[yellow]questionary not installed — falling back to text entry[/yellow]")
        _print_columns(all_columns)
        return _manual_entry(all_columns)


def _print_columns(columns, page_size=40):
    """Print columns in pages."""
    for i in range(0, len(columns), page_size):
        page = columns[i:i+page_size]
        for j, col in enumerate(page):
            console.print(f"  {i+j+1:4d}  {col}")
        if i + page_size < len(columns):
            Prompt.ask("[dim]Press enter for next page[/dim]", default="")


def _manual_entry(all_columns):
    """
    Accept comma-separated column names.
    Validate against manifest. Suggest corrections for unknowns.
    """
    raw = Prompt.ask("Enter column names (comma-separated)")
    entries = [c.strip() for c in raw.split(",") if c.strip()]

    col_set = set(all_columns)
    valid, invalid = [], []
    for name in entries:
        if name in col_set:
            valid.append(name)
        else:
            invalid.append(name)

    if invalid:
        console.print(f"\n[red]Unknown columns ({len(invalid)}):[/red]")
        for name in invalid:
            suggestions = difflib.get_close_matches(name, all_columns, n=3, cutoff=0.6)
            hint = f"  → did you mean: {', '.join(suggestions)}" if suggestions else ""
            console.print(f"  {name}{hint}")
        retry = Prompt.ask("Fix and re-enter unknown columns? [Y/n]", default="Y")
        if retry.upper() == "Y":
            return _manual_entry(all_columns)

    if not valid:
        raise ValueError("No valid columns selected.")

    return sorted(valid)
