"""Layer classification for the three-layer test skeleton.

One shared implementation imported by both ``conftest.py`` and test modules
that need to call it directly, so the classifier under test is the same one
the pytest hooks run.
"""

from pathlib import Path

LAYER_MARKERS = ("unit", "integration", "golden")


def layer_of_path(path) -> str:
    """Return the layer name for a test path.

    Classification looks only at the first component after ``tests/`` so
    that a checkout living in a directory named ``unit`` / ``integration`` /
    ``golden`` is never misclassified, and a nested path like
    ``tests/golden/unit/test_x.py`` maps to ``golden`` (the first layer
    directory), not ``unit``.  Legacy root-level files (``tests/test_*.py``)
    default to ``unit`` — they are function-level and fast.
    """
    parts = Path(path).parts
    if "tests" in parts:
        # The LAST "tests" component: `parts.index` takes the first, so a
        # checkout under an ancestor directory named `tests`
        # (e.g. /home/tests/repo/tests/integration/x.py) would classify from
        # `repo` and fall through to the `unit` default.
        idx = max(i for i, part in enumerate(parts) if part == "tests") + 1
        if idx < len(parts) and parts[idx] in LAYER_MARKERS:
            return parts[idx]
    return "unit"
