import sys

import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dataclasses import dataclass


from selector.recommender import recommend_exclusions


@dataclass
class FakeMeta:
    density_pct: float

    physical_type: str = "BYTE_ARRAY"

    logical_type: str = ""

    total_values: int = 1000

    total_nulls: int = 0


class TestRecommendExclusions:
    def _manifest(self, **cols):

        return {name: FakeMeta(density_pct=pct) for name, pct in cols.items()}

    def test_zero_density_excluded(self):

        manifest = self._manifest(empty_col=0.0, good_col=90.0)

        result = recommend_exclusions(manifest)

        assert "empty_col" in result

        assert "good_col" not in result

    def test_low_density_excluded(self):

        manifest = self._manifest(sparse_col=2.5, good_col=80.0)

        result = recommend_exclusions(manifest)

        assert "sparse_col" in result

    def test_exactly_5_percent_not_excluded(self):

        manifest = self._manifest(borderline=5.0)

        result = recommend_exclusions(manifest)

        assert "borderline" not in result

    def test_high_density_not_excluded(self):

        manifest = self._manifest(rich_col=99.0)

        result = recommend_exclusions(manifest)

        assert "rich_col" not in result

    def test_empty_manifest_returns_empty_list(self):

        result = recommend_exclusions({})

        assert result == []

    def test_return_type_is_list(self):

        result = recommend_exclusions({})

        assert isinstance(result, list)

    def test_multiple_sparse_columns_all_returned(self):

        manifest = self._manifest(a=0.0, b=1.0, c=4.9, d=50.0)

        result = recommend_exclusions(manifest)

        assert set(result) == {"a", "b", "c"}
