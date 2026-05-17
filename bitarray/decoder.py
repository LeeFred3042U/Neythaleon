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

        null_mask = _null_mask(raw_series)

        for out in output_defs:
            bit_idx = out["bit_index"]

            out_name = out["name"]

            if bit_idx < unpacked.shape[1]:
                bool_vals = unpacked[:, bit_idx].astype(bool)

                arr = pd.array(bool_vals, dtype="boolean")

                arr[null_mask] = pd.NA

                df[out_name] = arr

            else:
                logger.warning(
                    "bit_index %d out of range for column '%s'", bit_idx, col
                )

        df.drop(columns=[col], inplace=True)

    return df


def _null_mask(series: pd.Series) -> np.ndarray:
    return pd.isna(series).to_numpy()


def _unpack_series(series: pd.Series, bit_width: int) -> np.ndarray:
    byte_count = (bit_width + 7) // 8
    n = len(series)

    raw_bytes = np.zeros((n, byte_count), dtype=np.uint8)
    null_mask = _null_mask(series)
    values = series.to_numpy(dtype=object)

    non_null_idx = np.where(~null_mask)[0]

    for i in non_null_idx:
        val = values[i]
        if isinstance(val, (bytes, bytearray)):
            b = val[:byte_count]
            raw_bytes[i, : len(b)] = np.frombuffer(b, dtype=np.uint8)

    flat = raw_bytes.reshape(-1)
    all_bits = np.unpackbits(flat, bitorder="little")
    all_bits = all_bits.reshape(n, byte_count * 8)
    result = all_bits[:, :bit_width]

    result[null_mask] = 0

    return result.astype(np.uint8)
