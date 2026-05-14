import sys

import os

import json


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from checkpoint.checkpoint import Checkpoint


@pytest.fixture
def tmp_checkpoint(tmp_path):

    cp_file = tmp_path / "checkpoint.json"

    return Checkpoint(path=cp_file)


class TestCheckpointRoundTrip:
    def test_save_and_load(self, tmp_checkpoint):

        tmp_checkpoint.save(
            "column_selection", {"selected_columns": ["AphiaID", "species"]}
        )

        state = tmp_checkpoint.load("column_selection")

        assert state["selected_columns"] == ["AphiaID", "species"]

    def test_load_missing_key_returns_none(self, tmp_checkpoint):

        result = tmp_checkpoint.load("nonexistent_phase")

        assert result is None

    def test_load_before_any_save_returns_none(self, tmp_checkpoint):

        result = tmp_checkpoint.load("ingestion")

        assert result is None

    def test_overwrite_existing_key(self, tmp_checkpoint):

        tmp_checkpoint.save("ingestion", {"completed": False, "chunks_completed": 3})

        tmp_checkpoint.save("ingestion", {"completed": True, "chunks_completed": 10})

        state = tmp_checkpoint.load("ingestion")

        assert state["completed"] is True

        assert state["chunks_completed"] == 10

    def test_multiple_phases_independent(self, tmp_checkpoint):

        tmp_checkpoint.save("phase_a", {"val": 1})

        tmp_checkpoint.save("phase_b", {"val": 2})

        assert tmp_checkpoint.load("phase_a")["val"] == 1

        assert tmp_checkpoint.load("phase_b")["val"] == 2

    def test_checkpoint_file_is_valid_json(self, tmp_checkpoint, tmp_path):

        tmp_checkpoint.save("test", {"key": "value"})

        cp_file = tmp_path / "checkpoint.json"

        with open(cp_file) as fh:
            data = json.load(fh)

        assert "test" in data["phases"]

    def test_nested_data_preserved(self, tmp_checkpoint):

        payload = {
            "completed": False,
            "chunks": 5,
            "rates": {"storage_gb_month": 0.023},
        }

        tmp_checkpoint.save("cost_estimate", payload)

        state = tmp_checkpoint.load("cost_estimate")

        assert state["rates"]["storage_gb_month"] == 0.023

    def test_boolean_values_preserved(self, tmp_checkpoint):

        tmp_checkpoint.save("ingestion", {"completed": True, "fail_fast": False})

        state = tmp_checkpoint.load("ingestion")

        assert state["completed"] is True

        assert state["fail_fast"] is False

    def test_load_corrupted_json_returns_none(self, tmp_checkpoint, tmp_path):

        cp_file = tmp_path / "checkpoint.json"

        with open(cp_file, "w") as fh:
            fh.write("invalid json {")

        result = tmp_checkpoint.load("any_phase")

        assert result is None

    def test_save_to_readonly_directory_fails_gracefully(self, tmp_path):
        if sys.platform == "win32":
            pytest.skip(
                "Windows chmod doesn't reliably prevent file creation in temp dirs"
            )

        ro_dir = tmp_path / "readonly"
        ro_dir.mkdir()
        cp_file = ro_dir / "checkpoint.json"

        cp = Checkpoint(path=cp_file)

        import stat

        os.chmod(ro_dir, stat.S_IREAD | stat.S_IEXEC)  # Read-only

        try:
            with pytest.raises(Exception):
                cp.save("test", {"x": 1})
        finally:
            os.chmod(ro_dir, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)
