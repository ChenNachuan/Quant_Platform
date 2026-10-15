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

Hard rules enforced at conftest import time (not per test):
    * no test may open a network socket (TCP or UDP, connect or DNS)
    * no test may read a real ``.env`` (use a ``tmp_path`` fake)
    * the ``ConfigLoader`` singleton is reset around every test

The guards are installed at **import** time on purpose. A function-scoped
autouse fixture only covers a test's call phase, so module/session-scoped
fixtures and collection-time code would run unguarded — and those are exactly
the paths that construct a ``StorageManager`` and read secrets.
"""

import builtins
import io
import os
import socket
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
    """Single registration point for the layer markers.

    `pyproject.toml` used to declare these too; two sources drift (the
    descriptions already disagreed) and `pytest --markers` listed each twice.
    """
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
# Hard rules — installed at import time so they cover every phase
# ---------------------------------------------------------------------------
# Both secret files are protected: `config/.env` is the documented one, but
# `infra/storage.py::ConfigLoader.load()` reads `<repo>/.env` (via
# python-dotenv) and `config/settings.toml` tells users to create exactly that
# file. Guarding only `config/.env` guarded the wrong path.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_REAL_ENV_PATHS = (_REPO_ROOT / "config" / ".env", _REPO_ROOT / ".env")
# Resolved at **check** time, not here. Hoisting the resolutions is faster, and
# it is also how a symlink planted at `config/.env` after import slips through:
# the whole point of resolving per call is that the late-symlink case is still
# caught. Two `resolve()` calls per open is the price; correctness first.

_ORIGINAL_OPEN = builtins.open
_ORIGINAL_IO_OPEN = io.open
_ORIGINAL_OS_OPEN = os.open
_ORIGINAL_FILEIO = io.FileIO
_ORIGINAL_SOCKET_CONNECT = socket.socket.connect
_ORIGINAL_SOCKET_CONNECT_EX = socket.socket.connect_ex
_ORIGINAL_CREATE_CONNECTION = socket.create_connection
_ORIGINAL_SOCKET_SENDTO = socket.socket.sendto
_ORIGINAL_GETADDRINFO = socket.getaddrinfo
_ORIGINAL_GETHOSTBYNAME = socket.gethostbyname
_ORIGINAL_GETHOSTBYNAME_EX = socket.gethostbyname_ex


def _blocked_network(*_args, **_kwargs):
    raise RuntimeError(
        "network access is forbidden in tests — use a fixture double "
        "(pre-stored Parquet / tmp_path / in-memory DataFrame)"
    )


def _is_real_env(file) -> bool:
    """True for a real secret path.  Fail **closed** on path-like input.

    ``bytes`` paths are legitimate arguments to ``open``/``os.open``, and
    ``Path(b"...")`` raises ``TypeError`` — swallowing that would return
    ``False`` and let ``open(os.fsencode(_REPO_ROOT / "config" / ".env"))``
    read the real secret unguarded.
    """
    if isinstance(file, (bytes, bytearray)):
        try:
            file = os.fsdecode(file)
        except Exception:
            return True
    try:
        resolved = Path(file).resolve()
    except (TypeError, ValueError, OSError):
        return False
    return resolved in {p.resolve() for p in _REAL_ENV_PATHS}


class _GuardedFileIO(io.FileIO):
    """``io.FileIO`` bypasses ``builtins.open``/``io.open`` entirely."""

    def __init__(self, file, *args, **kwargs):
        if _is_real_env(file):
            raise RuntimeError(
                "reading a real .env is forbidden in tests — write a fake "
                "under tmp_path and pass that path"
            )
        super().__init__(file, *args, **kwargs)


def _guarded_open(file, *args, **kwargs):
    """`builtins.open` and `io.open` (which `pathlib.Path.read_text` uses)."""
    if _is_real_env(file):
        raise RuntimeError(
            "reading a real .env is forbidden in tests — write a fake under "
            "tmp_path and pass that path"
        )
    return _ORIGINAL_OPEN(file, *args, **kwargs)


def _guarded_io_open(file, *args, **kwargs):
    if _is_real_env(file):
        raise RuntimeError(
            "reading a real .env is forbidden in tests — write a fake under "
            "tmp_path and pass that path"
        )
    return _ORIGINAL_IO_OPEN(file, *args, **kwargs)


def _guarded_os_open(path, *args, **kwargs):
    if _is_real_env(path):
        raise RuntimeError(
            "reading a real .env is forbidden in tests — write a fake under "
            "tmp_path and pass that path"
        )
    return _ORIGINAL_OS_OPEN(path, *args, **kwargs)


def _install_hard_rules() -> None:
    # `builtins.open` alone is not enough: `pathlib.Path.open()` / `read_text()`
    # delegate to `io.open`, a distinct module attribute, and `os.open` is a
    # third API. All three have to be covered or the rule is decorative.
    builtins.open = _guarded_open
    io.open = _guarded_io_open
    os.open = _guarded_os_open
    io.FileIO = _GuardedFileIO
    socket.socket.connect = _blocked_network
    socket.socket.connect_ex = _blocked_network
    socket.create_connection = _blocked_network
    socket.socket.sendto = _blocked_network
    socket.getaddrinfo = _blocked_network
    socket.gethostbyname = _blocked_network
    socket.gethostbyname_ex = _blocked_network


def _remove_hard_rules() -> None:
    builtins.open = _ORIGINAL_OPEN
    io.open = _ORIGINAL_IO_OPEN
    os.open = _ORIGINAL_OS_OPEN
    io.FileIO = _ORIGINAL_FILEIO
    socket.socket.connect = _ORIGINAL_SOCKET_CONNECT
    socket.socket.connect_ex = _ORIGINAL_SOCKET_CONNECT_EX
    socket.create_connection = _ORIGINAL_CREATE_CONNECTION
    socket.socket.sendto = _ORIGINAL_SOCKET_SENDTO
    socket.getaddrinfo = _ORIGINAL_GETADDRINFO
    socket.gethostbyname = _ORIGINAL_GETHOSTBYNAME
    socket.gethostbyname_ex = _ORIGINAL_GETHOSTBYNAME_EX


# Installed here, at import, so collection and module/session fixture setup
# run under the rules too.
_install_hard_rules()


@pytest.fixture(scope="session", autouse=True)
def _hard_rules():
    """Keep the rules installed for the whole session, then restore."""
    _install_hard_rules()
    yield
    _remove_hard_rules()


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


#: A-share (SSE/SZSE) full-day closures in 2024. Weekends alone are not a
#: trading calendar: the Spring Festival week is the largest date-keyed gap in
#: the year, and a clock double that treats 2024-02-12 as an open session hands
#: T+1 settlement, rolling windows and date joins a non-trading date.
_A_SHARE_HOLIDAYS_2024 = frozenset({
    date(2024, 1, 1),                      # New Year
    date(2024, 2, 9), date(2024, 2, 12), date(2024, 2, 13),
    date(2024, 2, 14), date(2024, 2, 15), date(2024, 2, 16),  # Spring Festival
    date(2024, 4, 4), date(2024, 4, 5),                       # Qingming
    date(2024, 5, 1), date(2024, 5, 2), date(2024, 5, 3),     # Labour Day
    date(2024, 6, 10),                                        # Dragon Boat
    date(2024, 9, 16), date(2024, 9, 17),                     # Mid-Autumn
    date(2024, 10, 1), date(2024, 10, 2), date(2024, 10, 3),
    date(2024, 10, 4), date(2024, 10, 7),                     # National Day
})


def _trading_days(start: date, count: int) -> list[date]:
    """Return ``count`` A-share trading days starting from ``start``.

    Skips weekends **and** :data:`_A_SHARE_HOLIDAYS_2024`. Starting on a
    non-trading day yields the first session on or after it.
    """
    days: list[date] = []
    d = start
    while len(days) < count:
        if d.weekday() < 5 and d not in _A_SHARE_HOLIDAYS_2024:
            days.append(d)
        d += timedelta(days=1)
    return days


@pytest.fixture
def fixed_dates() -> list[date]:
    """Deterministic 60-trading-day sequence starting 2024-01-02 (clock double).

    Models an A-share calendar: weekends **and** exchange holidays are
    excluded, so a date-keyed join or T+1 step is never handed a day the
    market was closed.
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
