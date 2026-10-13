"""Three-layer test skeleton: markers, time budgets, test doubles.

Layers (design/core/testing.md):
    tests/unit/         function-level, fast        budget < 10s
    tests/integration/  pipeline-level              budget < 60s
    tests/golden/       end-to-end golden sample    budget < 5min

Budgets are **reported** at session end with a WARNING on overrun.  They are
never asserted on wall-clock — machine speed varies and a timing assertion
just manufactures flaky tests.

Test doubles:
    clock           ``fixed_dates`` / ``clock`` fixtures — fixed-date injection
    DuckDB / disk   ``data_lake_dir`` / ``memory_frame`` on ``tmp_path``
    akshare network pre-stored Parquet fixture — lands at S2-T2 (QUANT-11)
    QMT broker      SimBroker (V1's only impl) — Broker lands at S7

Hard rules enforced by autouse fixtures:
    * no test may open a network socket
    * no test may read the real ``config/.env`` (use a ``tmp_path`` fake)
"""

import builtins
import time
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from core.testing import TIME_BUDGET_SECONDS

# ---------------------------------------------------------------------------
# Marker registration
# ---------------------------------------------------------------------------
LAYER_MARKERS = ("unit", "integration", "golden")


def pytest_configure(config):
    for layer in LAYER_MARKERS:
        config.addinivalue_line(
            "markers", f"{layer}: test layer ({TIME_BUDGET_SECONDS[layer]}s budget)"
        )


def _layer_of_path(path) -> str | None:
    """Return the layer name for a test path, or None if outside the skeleton."""
    parts = Path(path).parts
    for layer in LAYER_MARKERS:
        if layer in parts:
            return layer
    return None


# ---------------------------------------------------------------------------
# Collection: auto-assign layer markers, remember items for the summary
# ---------------------------------------------------------------------------
_collected: list = []


def pytest_collection_modifyitems(config, items):
    """Give every skeleton test its layer marker from directory location.

    Existing tests under ``tests/unit/`` predate marker usage; this is the
    single place that stamps them.  Tests outside the three layer directories
    (legacy root-level files) get no marker and are reported at session end.
    """
    _collected.clear()
    _collected.extend(items)
    for item in items:
        layer = _layer_of_path(item.fspath)
        if layer and not any(m.name == layer for m in item.iter_markers()):
            item.add_marker(getattr(pytest.mark, layer))


# ---------------------------------------------------------------------------
# Per-layer elapsed time — report and warn, never assert
# ---------------------------------------------------------------------------
_layer_elapsed: dict[str, float] = {layer: 0.0 for layer in LAYER_MARKERS}


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    layer = _layer_of_path(item.fspath)
    start = time.perf_counter()
    yield
    if layer is not None:
        _layer_elapsed[layer] += time.perf_counter() - start


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    terminalreporter.section("layer time budgets")
    for layer, budget in TIME_BUDGET_SECONDS.items():
        elapsed = _layer_elapsed.get(layer, 0.0)
        status = "OK" if elapsed < budget else "OVERRUN"
        terminalreporter.write_line(
            f"  {layer:<12} {elapsed:7.2f}s / {budget}s  [{status}]"
        )
        if elapsed >= budget:
            msg = (
                f"layer {layer!r} overran its {budget}s budget "
                f"({elapsed:.2f}s)"
            )
            warnings.warn(msg, stacklevel=1)
            terminalreporter.write_line(f"  WARNING: {msg}")

    unmarked = [
        item.nodeid for item in _collected if _layer_of_path(item.fspath) is None
    ]
    if unmarked:
        terminalreporter.write_line(
            f"  NOTE: {len(unmarked)} test(s) outside the three-layer skeleton "
            f"(no layer marker)"
        )


# ---------------------------------------------------------------------------
# Hard rule: no network
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Block outbound socket connections for every test."""

    def _blocked(*_args, **_kwargs):
        raise RuntimeError(
            "network access is forbidden in tests — use a fixture double "
            "(pre-stored Parquet / tmp_path / in-memory DataFrame)"
        )

    monkeypatch.setattr("socket.socket.connect", _blocked)
    monkeypatch.setattr("socket.socket.connect_ex", _blocked)
    monkeypatch.setattr("socket.create_connection", _blocked)


# ---------------------------------------------------------------------------
# Hard rule: no real config/.env reads
# ---------------------------------------------------------------------------
_REAL_ENV_PATH = Path(__file__).resolve().parent.parent / "config" / ".env"


@pytest.fixture(autouse=True)
def _no_real_env(monkeypatch):
    """Forbid opening the real ``config/.env``.

    Secrets tests write a fake ``.env`` under ``tmp_path`` and pass that path
    explicitly — the default ``config/.env`` is never read.
    """
    real_open = builtins.open

    def _guarded_open(file, *args, **kwargs):
        try:
            if Path(file).resolve() == _REAL_ENV_PATH:
                raise RuntimeError(
                    "reading the real config/.env is forbidden in tests — "
                    "write a fake under tmp_path and pass that path"
                )
        except (TypeError, ValueError, OSError):
            pass  # non-path arguments (file descriptors etc.) pass through
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _guarded_open)


# ---------------------------------------------------------------------------
# Test doubles: clock (fixed-date injection)
# ---------------------------------------------------------------------------
class _Clock:
    """Deterministic clock.  Never call ``datetime.now()`` in tests.

    ``today()`` walks the injected date list; ``advance()`` steps forward.
    """

    def __init__(self, dates: list[date]):
        self._dates = list(dates)
        self._index = 0

    def today(self) -> date:
        return self._dates[self._index]

    def now(self) -> datetime:
        d = self._dates[self._index]
        return datetime(d.year, d.month, d.day, 12, 0, 0)

    def advance(self) -> date:
        self._index = min(self._index + 1, len(self._dates) - 1)
        return self.today()


@pytest.fixture
def fixed_dates() -> list[date]:
    """Deterministic 60-day date sequence starting 2024-01-02 (clock double)."""
    start = date(2024, 1, 2)
    return [start + timedelta(days=i) for i in range(60)]


@pytest.fixture
def clock(fixed_dates) -> _Clock:
    """Injectable clock double backed by :func:`fixed_dates`."""
    return _Clock(fixed_dates)


# ---------------------------------------------------------------------------
# Test doubles: DuckDB / disk  →  tmp_path + in-memory DataFrame
# ---------------------------------------------------------------------------
@pytest.fixture
def data_lake_dir(tmp_path) -> Path:
    """Fake data-lake root on ``tmp_path`` — never the real ``data_lake/``."""
    root = tmp_path / "data_lake"
    root.mkdir()
    return root


@pytest.fixture
def memory_frame():
    """Factory for in-memory DataFrames — use instead of a DuckDB/parquet read.

    Usage::

        df = memory_frame({"close": [1.0, 2.0]}, index=dates)
    """
    import pandas as pd

    def _make(data=None, **kwargs):
        return pd.DataFrame(data, **kwargs)

    return _make


# ---------------------------------------------------------------------------
# Test double: QMT broker — placeholder only
# ---------------------------------------------------------------------------
# SimBroker is V1's only Broker implementation and lands at S7 together with
# the Broker ABC.  When it arrives, add a ``broker`` fixture here returning a
# ``SimBroker`` wired to the ``clock`` double.  Until then this comment is the
# whole placeholder (per the test-double table).
