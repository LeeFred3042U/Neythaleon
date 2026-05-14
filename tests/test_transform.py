import sys

import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


import pandas as pd


from ingest.transform import (
    clean_null_bytes,
    clean_special_characters,
    enforce_integer_types,
    convert_geometry_to_wkt,
    process_chunk,
)


class TestCleanNullBytes:
    def test_removes_embedded_null(self):

        df = pd.DataFrame({"col": ["hel\x00lo", "world"]})

        result = clean_null_bytes(df)

        assert result["col"].iloc[0] == "hello"

    def test_no_null_bytes_unchanged(self):

        df = pd.DataFrame({"col": ["hello", "world"]})

        result = clean_null_bytes(df)

        assert list(result["col"]) == ["hello", "world"]

    def test_numeric_columns_untouched(self):

        df = pd.DataFrame({"num": [1, 2], "txt": ["a\x00b", "c"]})

        result = clean_null_bytes(df)

        assert list(result["num"]) == [1, 2]

    def test_input_df_not_mutated(self):

        df = pd.DataFrame({"col": ["a\x00b"]})

        original = df["col"].iloc[0]

        clean_null_bytes(df)

        assert df["col"].iloc[0] == original

    def test_multiple_null_bytes(self):

        df = pd.DataFrame({"col": ["\x00a\x00b\x00"]})

        result = clean_null_bytes(df)

        assert result["col"].iloc[0] == "ab"


class TestCleanSpecialCharacters:
    def test_newline_replaced(self):

        df = pd.DataFrame({"col": ["line1\nline2"]})

        result = clean_special_characters(df)

        assert "\n" not in result["col"].iloc[0]

    def test_carriage_return_replaced(self):

        df = pd.DataFrame({"col": ["a\rb"]})

        result = clean_special_characters(df)

        assert "\r" not in result["col"].iloc[0]

    def test_tab_replaced(self):

        df = pd.DataFrame({"col": ["a\tb"]})

        result = clean_special_characters(df)

        assert "\t" not in result["col"].iloc[0]

    def test_clean_string_unchanged(self):

        df = pd.DataFrame({"col": ["Gadus morhua"]})

        result = clean_special_characters(df)

        assert result["col"].iloc[0] == "Gadus morhua"


class TestEnforceIntegerTypes:
    def test_valid_integers_coerced(self):

        df = pd.DataFrame({"AphiaID": ["123456", "654321"]})

        result = enforce_integer_types(df, int_columns=["AphiaID"])

        assert str(result["AphiaID"].dtype) == "Int64"

        assert result["AphiaID"].iloc[0] == 123456

    def test_none_becomes_pd_na(self):

        df = pd.DataFrame({"AphiaID": [None, "42"]})

        result = enforce_integer_types(df, int_columns=["AphiaID"])

        assert result["AphiaID"].iloc[0] is pd.NA

    def test_non_numeric_string_becomes_na(self):

        df = pd.DataFrame({"AphiaID": ["not_a_number", "99"]})

        result = enforce_integer_types(df, int_columns=["AphiaID"])

        assert result["AphiaID"].iloc[0] is pd.NA

        assert result["AphiaID"].iloc[1] == 99

    def test_no_int_columns_noop(self):

        df = pd.DataFrame({"col": ["a", "b"]})

        result = enforce_integer_types(df, int_columns=[])

        assert list(result["col"]) == ["a", "b"]

    def test_column_not_in_df_skipped(self):

        df = pd.DataFrame({"other": [1, 2]})

        result = enforce_integer_types(df, int_columns=["AphiaID"])

        assert list(result.columns) == ["other"]

    def test_input_df_not_mutated(self):

        df = pd.DataFrame({"AphiaID": ["1"]})

        original_dtype = df["AphiaID"].dtype

        enforce_integer_types(df, int_columns=["AphiaID"])

        assert df["AphiaID"].dtype == original_dtype


class TestConvertGeometryToWkt:
    def _point_wkb(self):

        import struct

        return struct.pack("<BId d", 1, 1, 1.0, 2.0)

    def test_valid_wkb_converted_to_wkt(self):

        wkb_bytes = self._point_wkb()

        df = pd.DataFrame({"geometry": [wkb_bytes]})

        result = convert_geometry_to_wkt(df)

        wkt = result["geometry"].iloc[0]

        assert wkt is not None

        assert "POINT" in wkt.upper()

    def test_none_stays_none(self):

        df = pd.DataFrame({"geometry": [None]})

        result = convert_geometry_to_wkt(df)

        assert result["geometry"].iloc[0] is None

    def test_garbage_bytes_returns_none(self):

        df = pd.DataFrame({"geometry": [b"\xde\xad\xbe\xef"]})

        result = convert_geometry_to_wkt(df)

        assert result["geometry"].iloc[0] is None

    def test_no_geometry_column_noop(self):

        df = pd.DataFrame({"species": ["Cod"]})

        result = convert_geometry_to_wkt(df)

        assert list(result.columns) == ["species"]

    def test_input_df_not_mutated(self):

        wkb_bytes = self._point_wkb()

        df = pd.DataFrame({"geometry": [wkb_bytes]})

        original = df["geometry"].iloc[0]

        convert_geometry_to_wkt(df)

        assert df["geometry"].iloc[0] == original


class TestProcessChunkSmoke:
    def test_runs_without_error(self):

        df = pd.DataFrame(
            {
                "species": ["Gadus\x00morhua", "Thunnus\nthynnus"],
                "AphiaID": ["126436", None],
                "depth": [100.0, 200.5],
            }
        )

        result = process_chunk(df, int_columns=["AphiaID"])

        assert "species" in result.columns

        assert "\x00" not in result["species"].iloc[0]

        assert str(result["AphiaID"].dtype) == "Int64"

        assert result["AphiaID"].iloc[0] == 126436

        assert result["AphiaID"].iloc[1] is pd.NA
