import sys
import os
import pytest
import time
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingest.metrics import track_metrics, _normalize_metric_dict


class TestMetrics:
    def test_normalize_metric_dict(self):
        raw = {
            "rows_per_second": 100,
            "processing_time": 5.0,
            "cpu_usage": 10.0,
            "extra": "value",
        }
        normalized = _normalize_metric_dict(raw)

        assert normalized["throughput_rps"] == 100
        assert normalized["duration_sec"] == 5.0
        assert normalized["cpu_percent"] == 10.0
        assert normalized["extra"] == "value"
        assert "rows_per_second" not in normalized

    @patch("ingest.metrics._process")
    def test_track_metrics(self, mock_proc):
        mock_proc.cpu_percent.return_value = 20.0
        mock_proc.memory_info.return_value.rss = 100 * 1024 * 1024  # 100MB

        with patch("psutil.cpu_count", return_value=4):
            start = time.time() - 10
            metrics = track_metrics(start, 1000)

            assert metrics["rows"] == 1000
            assert metrics["duration_sec"] >= 10
            assert metrics["throughput_rps"] <= 100
            assert metrics["cpu_percent"] == 5.0
            assert metrics["memory_mb"] == 100.0
            assert "timestamp" in metrics

    def test_track_metrics_zero_duration(self):
        # Should not divide by zero
        metrics = track_metrics(time.time() + 100, 1000)
        assert metrics["throughput_rps"] == 0.0
