import json
import logging
import tempfile

from rich.console import Console
from rich.prompt import Prompt

logger = logging.getLogger(__name__)
console = Console()

CONTACT_MESSAGE = (
    "\n[bold red]HARD BLOCK — bit-array encoding unresolved[/bold red]\n\n"
    "One or more selected columns are bit-array encoded and no valid encoding\n"
    "metadata could be found or confirmed.\n\n"
    "This run cannot proceed until the encoding is defined.\n\n"
    "Please provide an encoding metadata JSON file and re-run.\n"
    "  The following unresolved column names:\n"
    "    {columns}\n"
)


def attempt_lookup(bit_cols, manifest):
    """
    Attempts to locate encoding metadata through guided prompts.
    Returns a BitArrayMetadata if resolved, None if not.
    """
    console.print("\n[yellow]Bit-array columns detected. No encoding file provided.[/yellow]")
    console.print("Attempting guided lookup...\n")

    source = Prompt.ask(
        "What is the data source?",
        choices=["OBIS", "GBIF", "Custom", "Unknown"],
        default="Unknown"
    )

    if source == "OBIS":
        return _lookup_obis(bit_cols, manifest)

    return None


def _lookup_obis(bit_cols, manifest):
    """
    OBIS datasets use WoRMS-derived bit flags.
    Provide a bundled default encoding for known OBIS bit columns.
    """
    KNOWN_OBIS_ENCODINGS = {
        "flags": {
            "bit_width": 8,
            "output_columns": [
                {"bit_index": 0, "name": "marine",      "dtype": "boolean"},
                {"bit_index": 1, "name": "brackish",    "dtype": "boolean"},
                {"bit_index": 2, "name": "freshwater",  "dtype": "boolean"},
                {"bit_index": 3, "name": "terrestrial", "dtype": "boolean"},
                {"bit_index": 4, "name": "extinct",     "dtype": "boolean"},
                {"bit_index": 5, "name": "uncertain",   "dtype": "boolean"},
            ]
        }
    }

    resolved = {}
    unresolved = []

    for col in bit_cols:
        if col in KNOWN_OBIS_ENCODINGS:
            resolved[col] = KNOWN_OBIS_ENCODINGS[col]
            logger.info("OBIS lookup resolved encoding for column: %s", col)
        else:
            unresolved.append(col)

    if unresolved:
        console.print(f"[yellow]Could not auto-resolve {len(unresolved)} column(s): {unresolved}[/yellow]")
        console.print("These columns are not in the built-in OBIS encoding table.")
        return None

    from bitarray.metadata_loader import BitArrayMetadata
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
        json.dump({"columns": resolved}, tmp)
        tmp_path = tmp.name
    return BitArrayMetadata(tmp_path)
