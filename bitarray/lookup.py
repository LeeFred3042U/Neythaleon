import json

import logging

import tempfile

from rich.console import Console

from rich.prompt import Prompt

logger = logging.getLogger(__name__)

console = Console()

CONTACT_MESSAGE = (
    "Could not resolve encoding for columns: {columns}. Please contact support."
)


def attempt_lookup(bit_cols, manifest):

    console.print(
        "\n[yellow]Bit-array columns detected. No encoding file provided.[/yellow]"
    )

    console.print("Attempting guided lookup...\n")

    source = Prompt.ask(
        "What is the data source?",
        choices=["OBIS", "GBIF", "Custom", "Unknown"],
        default="Unknown",
    )

    if source == "OBIS":
        return _lookup_obis(bit_cols, manifest)

    return None


def _lookup_obis(bit_cols, manifest):

    KNOWN_OBIS_ENCODINGS = {
        "flags": {
            "bit_width": 8,
            "output_columns": [
                {"bit_index": 0, "name": "marine", "dtype": "bool"},
                {"bit_index": 1, "name": "brackish", "dtype": "bool"},
                {"bit_index": 2, "name": "freshwater", "dtype": "bool"},
                {"bit_index": 3, "name": "terrestrial", "dtype": "bool"},
                {"bit_index": 4, "name": "verified", "dtype": "bool"},
                {"bit_index": 5, "name": "flagged", "dtype": "bool"},
            ],
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
        console.print(
            f"[yellow]Could not auto-resolve {len(unresolved)} column(s): {unresolved}[/yellow]"
        )

        console.print("These columns are not in the built-in OBIS encoding table.")

        return None

    from bitarray.metadata_loader import BitArrayMetadata

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        json.dump({"columns": resolved}, tmp)

        tmp_path = tmp.name

    return BitArrayMetadata(tmp_path)
