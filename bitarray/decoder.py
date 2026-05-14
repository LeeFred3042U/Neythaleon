import numpy as np

import pandas as pd

import logging

logger = logging.getLogger(__name__)


def decode_bit_array_columns(df, bit_cols, metadata):

    df = df.copy()

    for col in bit_cols:
        if col not in df.columns:
            logger.warning("Bit-array column '%s' not in DataFrame, skipping", col)

            continue

        defn = metadata.columns[col]

        bit_width = defn["bit_width"]

        output_defs = defn["output_columns"]

        raw_series = df[col]

        try:
            unpacked = _unpack_series(raw_series, bit_width)

        except Exception:
            logger.exception("Failed to unpack column '%s'", col)

            df.drop(columns=[col], inplace=True)

            continue

        for out in output_defs:
            bit_idx = out["bit_index"]

            out_name = out["name"]

            if bit_idx < unpacked.shape[1]:
                bool_vals = unpacked[:, bit_idx].astype(bool)

                null_mask = _null_mask(raw_series)

                arr = pd.array(bool_vals, dtype="boolean")

                arr[null_mask] = pd.NA

                df[out_name] = arr

            else:
                logger.warning(
                    "bit_index %d out of range for column '%s'", bit_idx, col
                )

        df.drop(columns=[col], inplace=True)

    return df


def _null_mask(series):

    mask = np.zeros(len(series), dtype=bool)

    for i, val in enumerate(series):
        if pd.isna(val):
            mask[i] = True

    return mask


def _unpack_series(series, bit_width):

    byte_count = (bit_width + 7) // 8

    result = np.zeros((len(series), bit_width), dtype=np.uint8)

    for i, val in enumerate(series):
        if pd.isna(val):
            continue

        if isinstance(val, (bytes, bytearray)):
            b = val[:byte_count].ljust(byte_count, b"\x00")

            bits = np.unpackbits(np.frombuffer(b, dtype=np.uint8), bitorder="little")

            result[i] = bits[:bit_width]

    return result
