import sys

import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np

import pandas as pd


from bitarray.decoder import _null_mask, _unpack_series, decode_bit_array_columns


class TestNullMask:
    def test_none_is_null(self):

        s = pd.Series([None, "hello", None])

        mask = _null_mask(s)

        assert mask.tolist() == [True, False, True]

    def test_float_nan_is_null(self):

        s = pd.Series([float("nan"), 1.0, float("nan")])

        mask = _null_mask(s)

        assert mask.tolist() == [True, False, True]

    def test_pd_na_is_null(self):

        s = pd.array([pd.NA, 1, pd.NA], dtype="Int64")

        s = pd.Series(s)

        mask = _null_mask(s)

        assert mask[0]

        assert not mask[1]

        assert mask[2]

    def test_all_non_null(self):

        s = pd.Series([b"\x01", b"\x02", b"\x03"])

        mask = _null_mask(s)

        assert mask.tolist() == [False, False, False]

    def test_all_null(self):

        s = pd.Series([None, None, None])

        mask = _null_mask(s)

        assert all(mask)

    def test_returns_numpy_bool_array(self):

        s = pd.Series([None, 1])

        mask = _null_mask(s)

        assert isinstance(mask, np.ndarray)

        assert mask.dtype == bool


class TestUnpackSeries:
    def test_known_single_byte_little_endian(self):

        s = pd.Series([b"\x05"])

        result = _unpack_series(s, bit_width=8)

        assert result.shape == (1, 8)

        expected = [1, 0, 1, 0, 0, 0, 0, 0]

        assert result[0].tolist() == expected

    def test_null_rows_are_zeros(self):

        s = pd.Series([None, b"\xff"])

        result = _unpack_series(s, bit_width=8)

        assert result[0].tolist() == [0] * 8

        assert result[1].tolist() == [1] * 8

    def test_all_null_returns_zeros(self):

        s = pd.Series([None, None, None])

        result = _unpack_series(s, bit_width=4)

        assert result.shape == (3, 4)

        assert result.sum() == 0

    def test_bit_width_truncation(self):

        s = pd.Series([b"\x07"])

        result = _unpack_series(s, bit_width=3)

        assert result.shape == (1, 3)

        assert result[0].tolist() == [1, 1, 1]

    def test_multi_byte_value(self):

        s = pd.Series([b"\x01\x01"])

        result = _unpack_series(s, bit_width=16)

        assert result[0][0] == 1

        assert result[0][8] == 1

    def test_mixed_null_and_value(self):

        s = pd.Series([b"\x01", None, b"\x02"])

        result = _unpack_series(s, bit_width=8)

        assert result[0][0] == 1

        assert result[1].sum() == 0

        assert result[2][1] == 1

    def test_output_dtype_is_uint8(self):

        s = pd.Series([b"\x00"])

        result = _unpack_series(s, bit_width=8)

        assert result.dtype == np.uint8


class _FakeMeta:
    def __init__(self, columns):

        self.columns = columns


class TestDecodeRoundTrip:
    def _make_metadata(self, col_name="flags", bit_width=4):

        return _FakeMeta(
            {
                col_name: {
                    "bit_width": bit_width,
                    "output_columns": [
                        {"bit_index": 0, "name": "bit_0"},
                        {"bit_index": 1, "name": "bit_1"},
                        {"bit_index": 2, "name": "bit_2"},
                        {"bit_index": 3, "name": "bit_3"},
                    ],
                }
            }
        )

    def test_original_column_is_dropped(self):

        df = pd.DataFrame({"flags": [b"\x05", b"\x0a"]})

        meta = self._make_metadata()

        result = decode_bit_array_columns(df, ["flags"], meta)

        assert "flags" not in result.columns

    def test_output_columns_present(self):

        df = pd.DataFrame({"flags": [b"\x05"]})

        meta = self._make_metadata()

        result = decode_bit_array_columns(df, ["flags"], meta)

        for name in ["bit_0", "bit_1", "bit_2", "bit_3"]:
            assert name in result.columns

    def test_bit_values_correct(self):

        df = pd.DataFrame({"flags": [b"\x05"]})

        meta = self._make_metadata()

        result = decode_bit_array_columns(df, ["flags"], meta)

        assert result["bit_0"].iloc[0]

        assert not result["bit_1"].iloc[0]

        assert result["bit_2"].iloc[0]

        assert not result["bit_3"].iloc[0]

    def test_null_input_produces_pd_na_output(self):

        df = pd.DataFrame({"flags": [None, b"\x01"]})

        meta = self._make_metadata()

        result = decode_bit_array_columns(df, ["flags"], meta)

        assert result["bit_0"].iloc[0] is pd.NA

        assert result["bit_0"].iloc[1]

    def test_input_df_is_not_mutated(self):

        df = pd.DataFrame({"flags": [b"\x05"], "other": ["x"]})

        original_cols = list(df.columns)

        meta = self._make_metadata()

        decode_bit_array_columns(df, ["flags"], meta)

        assert list(df.columns) == original_cols

    def test_unrecognised_bit_col_is_skipped(self):

        df = pd.DataFrame({"other_col": [b"\x05"]})

        meta = self._make_metadata()

        result = decode_bit_array_columns(df, ["flags"], meta)

        assert "other_col" in result.columns

    def test_non_bit_columns_are_preserved(self):

        df = pd.DataFrame({"flags": [b"\x01"], "species": ["Gadus morhua"]})

        meta = self._make_metadata()

        result = decode_bit_array_columns(df, ["flags"], meta)

        assert "species" in result.columns

        assert result["species"].iloc[0] == "Gadus morhua"
