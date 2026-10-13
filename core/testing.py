"""Test conventions and shared constants (design/core/testing.md).

Constants and conventions only.  Golden-sample strategy logic lives in
``tests/golden/``; the five-step validation runner lands at S8.
"""

TOLERANCE = 1e-4

TIME_BUDGET_SECONDS = {"unit": 10, "integration": 60, "golden": 300}
