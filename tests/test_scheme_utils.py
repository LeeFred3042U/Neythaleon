import sys
import os
import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingest.scheme_utils import coerce_df_to_schema


class TestCoerceDfToSchema:
    def test_coerce_int_column(self):
        df = pd.DataFrame({"age": ["25", "30", "unknown"]})
        schema = {"age": "INTEGER"}

        result = coerce_df_to_schema(df, schema)

        assert str(result["age"].dtype) == "Int64"
        assert result["age"].iloc[0] == 25
        assert result["age"].iloc[2] is pd.NA

    def test_coerce_float_column(self):
        df = pd.DataFrame({"score": ["1.5", "2.0", "bad"]})
        schema = {"score": "DOUBLE PRECISION"}

        result = coerce_df_to_schema(df, schema)

        assert result["score"].iloc[0] == 1.5
        assert np.isnan(result["score"].iloc[2])

    def test_coerce_bool_column(self):
        df = pd.DataFrame({"active": ["True", "0", "yes", "no", None]})
        schema = {"active": "BOOLEAN"}

        result = coerce_df_to_schema(df, schema)

        assert str(result["active"].dtype) == "boolean"
        assert result["active"].iloc[0] == True
        assert result["active"].iloc[1] == False
        assert result["active"].iloc[2] == True
        assert result["active"].iloc[3] == False
        assert result["active"].iloc[4] is pd.NA

    def test_coerce_datetime_column(self):
        df = pd.DataFrame({"created": ["2026-05-14", "invalid"]})
        schema = {"created": "TIMESTAMP"}

        result = coerce_df_to_schema(df, schema)

        assert result["created"].iloc[0].year == 2026
        assert pd.isna(result["created"].iloc[1])

    def test_missing_column_ignored(self):
        df = pd.DataFrame({"existing": [1]})
        schema = {"missing": "INTEGER"}

        result = coerce_df_to_schema(df, schema)
        assert "existing" in result.columns
        assert "missing" not in result.columns

    def test_non_integral_to_int_logging(self, caplog):
        # We check if it logs when non-integral values are found
        import logging

        caplog.set_level(logging.DEBUG)

        df = pd.DataFrame({"id": [1.0, 2.5, 3.0]})
        schema = {"id": "BIGINT"}

        result = coerce_df_to_schema(df, schema)

        assert result["id"].iloc[0] == 1
        assert result["id"].iloc[1] is pd.NA
        assert "non-integral values will become NA" in caplog.text
