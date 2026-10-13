"""Golden-layer tests: verify the golden-sample conventions are in place.

The golden sample fixture (``golden_prices.parquet`` + ``golden_expected.json``)
lands at S2-T2 (QUANT-11) and is deliberately absent here.  These tests
assert the constants and layout the golden tests will depend on.
"""

import pytest

from core.testing import TOLERANCE, TIME_BUDGET_SECONDS
from tests._layer import layer_of_path

pytestmark = pytest.mark.golden


class TestTolerance:
    """TOLERANCE = 1e-4 — golden_expected.json is hand-calculated to 4 decimals."""

    def test_tolerance_value(self):
        assert TOLERANCE == 1e-4

    def test_tolerance_precision(self):
        assert abs(TOLERANCE - 0.0001) < 1e-15


class TestBudgets:
    """Golden layer budget is 300s (5 min) per the three-tier design."""

    def test_golden_budget(self):
        assert TIME_BUDGET_SECONDS["golden"] == 300

    def test_all_budgets_ordered(self):
        assert TIME_BUDGET_SECONDS["unit"] < TIME_BUDGET_SECONDS["integration"]
        assert TIME_BUDGET_SECONDS["integration"] < TIME_BUDGET_SECONDS["golden"]


class TestMarkerCompleteness:
    """DoD row 6: every collected test carries a layer marker (unit/integration/golden)."""

    def test_every_test_has_layer_marker(self, request):
        layer_markers = ("unit", "integration", "golden")
        unmarked = [
            item.nodeid
            for item in request.session.items
            if not any(item.get_closest_marker(m) for m in layer_markers)
        ]
        assert not unmarked, (
            f"{len(unmarked)} test(s) missing layer marker: {unmarked}"
        )


class TestLayerPathClassification:
    """_layer_of_path classifies from components under tests/, not the absolute path.

    A checkout living in a directory named ``unit`` / ``integration`` /
    ``golden`` must not be misclassified.
    """

    def test_classification_scoped_to_tests_components(self):
        cases = [
            ("/home/dev/Quant_Platform/tests/golden/test_x.py", "golden"),
            ("/home/dev/unit/Quant_Platform/tests/golden/test_x.py", "golden"),
            ("/home/dev/golden/Quant_Platform/tests/test_legacy.py", "unit"),
            ("/home/dev/integration/Quant_Platform/tests/golden/test_x.py", "golden"),
        ]
        for path, expected in cases:
            assert layer_of_path(path) == expected, f"{path} -> {expected}"
