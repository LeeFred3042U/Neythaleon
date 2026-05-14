import json

import logging


logger = logging.getLogger(__name__)


class BitArrayMetadata:
    def __init__(self, path):

        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)

        self.columns = raw.get("columns", {})

    def validate_against_manifest(self, bit_cols, manifest):

        errors = []

        for col in bit_cols:
            if col not in self.columns:
                errors.append(
                    f"Column '{col}' has no encoding definition in metadata file."
                )

                continue

            defn = self.columns[col]

            type_length = manifest[col].type_length

            if type_length > 0:
                expected_bits = type_length * 8

                if defn.get("bit_width", 0) > expected_bits:
                    errors.append(
                        f"Column '{col}': bit_width={defn['bit_width']} exceeds "
                        f"type_length={type_length} ({expected_bits} bits)."
                    )

            else:
                logger.warning(
                    "Column '%s' is missing type_length (manifest may have been produced by older Go scanner)",
                    col,
                )

            max_bit = max(
                (o["bit_index"] for o in defn.get("output_columns", [])), default=-1
            )

            if max_bit >= defn.get("bit_width", 8):
                errors.append(
                    f"Column '{col}': bit_index {max_bit} exceeds bit_width {defn['bit_width']}."
                )

        return errors
