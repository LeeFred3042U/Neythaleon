import sys

import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd


from ingest.db_utils import _prepare_for_copy


class TestPrepareForCopyNullableInt:
    def test_int64_nullable_na_becomes_none(self):

        df = pd.DataFrame({"x": pd.array([1, pd.NA, 3], dtype="Int64")})

        result = _prepare_for_copy(df)

        assert pd.isna(result["x"].iloc[1])

    def test_int32_nullable_na_becomes_none(self):

        df = pd.DataFrame({"x": pd.array([42, pd.NA], dtype="Int32")})

        result = _prepare_for_copy(df)

        assert pd.isna(result["x"].iloc[1])

    def test_uint64_nullable_na_becomes_none(self):

        df = pd.DataFrame({"x": pd.array([100, pd.NA], dtype="UInt64")})

        result = _prepare_for_copy(df)

        assert pd.isna(result["x"].iloc[1])

    def test_int64_nullable_valid_values_preserved(self):

        df = pd.DataFrame({"x": pd.array([7, 42], dtype="Int64")})

        result = _prepare_for_copy(df)

        assert result["x"].iloc[0] == 7

        assert result["x"].iloc[1] == 42

    def test_na_not_stringified_as_angle_na(self):

        df = pd.DataFrame({"x": pd.array([pd.NA], dtype="Int64")})

        result = _prepare_for_copy(df)

        val = result["x"].iloc[0]

        if not pd.isna(val):
            assert val != "<NA>"

        assert pd.isna(val)


class TestPrepareForCopyBoolean:
    def test_pandas_boolean_true(self):

        df = pd.DataFrame({"b": pd.array([True], dtype="boolean")})

        result = _prepare_for_copy(df)

        assert result["b"].iloc[0] == "t"

    def test_pandas_boolean_false(self):

        df = pd.DataFrame({"b": pd.array([False], dtype="boolean")})

        result = _prepare_for_copy(df)

        assert result["b"].iloc[0] == "f"

    def test_pandas_boolean_na_becomes_none(self):

        df = pd.DataFrame({"b": pd.array([pd.NA], dtype="boolean")})

        result = _prepare_for_copy(df)

        assert pd.isna(result["b"].iloc[0])

    def test_numpy_bool_true(self):

        df = pd.DataFrame({"b": [True, False]})

        result = _prepare_for_copy(df)

        assert result["b"].iloc[0] == "t"

        assert result["b"].iloc[1] == "f"


class TestPrepareForCopyObjectColumn:
    def test_tab_replaced_with_space(self):

        df = pd.DataFrame({"s": ["hello\tworld"]})

        result = _prepare_for_copy(df)

        assert "\t" not in result["s"].iloc[0]

    def test_backslash_escaped(self):

        df = pd.DataFrame({"s": ["a\\b"]})

        result = _prepare_for_copy(df)

        assert result["s"].iloc[0] == "a\\\\b"

    def test_newline_replaced(self):

        df = pd.DataFrame({"s": ["line1\nline2"]})

        result = _prepare_for_copy(df)

        assert "\n" not in result["s"].iloc[0]

    def test_none_stays_none(self):

        df = pd.DataFrame({"s": [None, "real"]})

        result = _prepare_for_copy(df)

        assert pd.isna(result["s"].iloc[0])

        assert result["s"].iloc[1] == "real"

    def test_input_df_not_mutated(self):

        df = pd.DataFrame({"s": ["hello\tworld"]})

        original = df["s"].iloc[0]

        _prepare_for_copy(df)

        assert df["s"].iloc[0] == original
