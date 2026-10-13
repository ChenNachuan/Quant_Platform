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
    * no test may open a network socket (TCP or UDP, connect or DNS)
    * no test may read the real ``config/.env`` (use a ``tmp_path`` fake)
    * the ``ConfigLoader`` singleton is reset around every test
"""

import builtins
import time
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from core.testing import TIME_BUDGET_SECONDS
from tests._layer import LAYER_MARKERS, layer_of_path

# ---------------------------------------------------------------------------
# Marker registration
# ---------------------------------------------------------------------------


def pytest_configure(config):
    for layer in LAYER_MARKERS:
        config.addinivalue_line(
            "markers", f"{layer}: test layer ({TIME_BUDGET_SECONDS[layer]}s budget)"
        )


# ---------------------------------------------------------------------------
# Collection: auto-assign layer markers
# ---------------------------------------------------------------------------
def pytest_collection_modifyitems(config, items):
    """Give every test its layer marker from directory location.

    Existing tests under ``tests/unit/`` predate marker usage; this is the
    single place that stamps them.  Legacy root-level files (``tests/test_*.py``)
    default to the ``unit`` layer.
    """
    for item in items:
        layer = layer_of_path(item.fspath)
        if not any(m.name == layer for m in item.iter_markers()):
            item.add_marker(getattr(pytest.mark, layer))


# ---------------------------------------------------------------------------
# Per-layer elapsed time — report and warn, never assert
# ---------------------------------------------------------------------------
_layer_elapsed: dict[str, float] = {layer: 0.0 for layer in LAYER_MARKERS}


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    layer = layer_of_path(item.fspath)
    start = time.perf_counter()
    yield
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


# ---------------------------------------------------------------------------
# Test isolation: reset the ConfigLoader singleton around every test
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def reset_config_loader():
    """Clear the ConfigLoader singleton before and after every test.

    ``StorageManager`` caches ``ConfigLoader._config`` for the process;
    without this reset, later tests see stale config.
    """
    from infra.storage import ConfigLoader

    ConfigLoader.reset()
    yield
    ConfigLoader.reset()


# ---------------------------------------------------------------------------
# Hard rule: no network
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Block all outbound network access for every test (TCP, UDP, DNS)."""

    def _blocked(*_args, **_kwargs):
        raise RuntimeError(
            "network access is forbidden in tests — use a fixture double "
            "(pre-stored Parquet / tmp_path / in-memory DataFrame)"
        )

    monkeypatch.setattr("socket.socket.connect", _blocked)
    monkeypatch.setattr("socket.socket.connect_ex", _blocked)
    monkeypatch.setattr("socket.create_connection", _blocked)
    monkeypatch.setattr("socket.socket.sendto", _blocked)
    monkeypatch.setattr("socket.getaddrinfo", _blocked)
    monkeypatch.setattr("socket.gethostbyname", _blocked)
    monkeypatch.setattr("socket.gethostbyname_ex", _blocked)


# ---------------------------------------------------------------------------
# Hard rule: no real config/.env reads
# ---------------------------------------------------------------------------
_REAL_ENV_PATH = Path(__file__).resolve().parent.parent / "config" / ".env"


@pytest.fixture(autouse=True)
def _no_real_env(monkeypatch):
    """Forbid opening the real ``config/.env``.

    Secrets tests write a fake ``.env`` under ``tmp_path`` and pass that path
    explicitly — the default ``config/.env`` is never read.  Both sides are
    resolved at check time (not at module load) so a symlink planted at
    ``config/.env`` after import is caught too.
    """
    real_open = builtins.open

    def _guarded_open(file, *args, **kwargs):
        try:
            resolved = Path(file).resolve()
            real_resolved = _REAL_ENV_PATH.resolve()
            if resolved == real_resolved:
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


def _trading_days(start: date, count: int) -> list[date]:
    """Return ``count`` consecutive weekdays starting from ``start``."""
    days: list[date] = []
    d = start
    while len(days) < count:
        if d.weekday() < 5:  # Mon–Fri
            days.append(d)
        d += timedelta(days=1)
    return days


@pytest.fixture
def fixed_dates() -> list[date]:
    """Deterministic 60-trading-day date sequence starting 2024-01-02 (clock double).

    Weekends are skipped — this is an A-share platform, and a clock double
    that lands on Sat/Sun will make T+1 settlement and date-keyed joins
    disagree with production.
    """
    return _trading_days(date(2024, 1, 2), 60)


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
