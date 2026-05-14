import sys
import os
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingest.ingest_runner import writer_worker, StorageFullAbort


class TestWriterWorker:
    @patch("ingest.ingest_runner.copy_insert")
    def test_writer_worker_success(self, mock_copy_insert):
        # Setup
        engine = MagicMock()
        df = MagicMock()
        prompt_lock = MagicMock()

        # Execute
        idx, length = writer_worker(engine, df, 1, None, {}, prompt_lock)

        # Assert
        assert idx == 1
        mock_copy_insert.assert_called_once()

    @patch("ingest.ingest_runner.copy_insert")
    @patch("ingest.ingest_runner.Prompt.ask")
    def test_writer_worker_retry_then_success(self, mock_ask, mock_copy_insert):
        # Setup
        engine = MagicMock()
        df = MagicMock()
        prompt_lock = MagicMock()

        # First call fails with DiskFull, second succeeds
        mock_copy_insert.side_effect = [
            Exception("DiskFull: project size limit exceeded"),
            None,
        ]
        mock_ask.return_value = "R"

        # Execute
        idx, length = writer_worker(engine, df, 1, None, {}, prompt_lock)

        # Assert
        assert idx == 1
        assert mock_copy_insert.call_count == 2
        mock_ask.assert_called_once()

    @patch("ingest.ingest_runner.copy_insert")
    @patch("ingest.ingest_runner.Prompt.ask")
    def test_writer_worker_abort(self, mock_ask, mock_copy_insert):
        # Setup
        engine = MagicMock()
        df = MagicMock()
        prompt_lock = MagicMock()

        mock_copy_insert.side_effect = Exception(
            "could not extend file: no space left on device"
        )
        mock_ask.return_value = "A"

        # Execute & Assert
        with pytest.raises(StorageFullAbort):
            writer_worker(engine, df, 1, None, {}, prompt_lock)

        assert mock_copy_insert.call_count == 1
        mock_ask.assert_called_once()

    @patch("ingest.ingest_runner.copy_insert")
    @patch("ingest.ingest_runner.Prompt.ask")
    def test_writer_worker_skip(self, mock_ask, mock_copy_insert):
        # Setup
        engine = MagicMock()
        df = MagicMock()
        prompt_lock = MagicMock()

        original_error = Exception("storage full")
        mock_copy_insert.side_effect = original_error
        mock_ask.return_value = "S"

        # Execute & Assert
        with pytest.raises(Exception) as excinfo:
            writer_worker(engine, df, 1, None, {}, prompt_lock)

        assert excinfo.value == original_error
        mock_ask.assert_called_once()

    @patch("ingest.ingest_runner.copy_insert")
    def test_writer_worker_non_storage_error(self, mock_copy_insert):
        # Setup
        engine = MagicMock()
        df = MagicMock()
        prompt_lock = MagicMock()

        original_error = Exception("Connection refused")
        mock_copy_insert.side_effect = original_error

        # Execute & Assert
        with pytest.raises(Exception) as excinfo:
            writer_worker(engine, df, 1, None, {}, prompt_lock)

        assert excinfo.value == original_error
        assert mock_copy_insert.call_count == 1
