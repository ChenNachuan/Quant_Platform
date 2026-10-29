"""Source adapter ABC — upstream abstraction for raw data fetching.

One subclass per external data source.  The adapter fetches **raw**
DataFrames (original column names / original values); format conversion is
the job of ``normalizers/{source}_normalizer.py`` (QUANT-36 S9-T1).

Rate-limiting, retry with exponential backoff, and timeout are applied
automatically via :meth:`SourceAdapter.__init_subclass__` — subclasses
implement :meth:`fetch` without worrying about those guards.

Watermark persistence: ``raw/{source}/.watermarks.json``.  The format is
subclass-defined (timestamp / offset / page number); the platform stores and
returns it verbatim, never parses it.

Field authority: ``docs/design/data/source_adapter.md``.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import pandas as pd

# requests exceptions are NOT subclasses of the builtin ConnectionError /
# TimeoutError — akshare/tushare transients live in the requests family (fix 4).
try:
    from requests.exceptions import (
        ConnectionError as RequestsConnectionError,
        HTTPError,
        Timeout as RequestsTimeout,
    )

    _REQUESTS_EXCEPTIONS: tuple[type[BaseException], ...] = (
        RequestsConnectionError,
        RequestsTimeout,
    )
except ImportError:  # pragma: no cover
    from urllib.error import HTTPError  # type: ignore[assignment]

    _REQUESTS_EXCEPTIONS = ()

logger = logging.getLogger(__name__)

__all__ = [
    "FetchResult",
    "SourceAdapter",
    "FetchTimeoutError",
    "read_watermark",
    "write_watermark",
    "RETRY_ON",
    "RETRY_MAX_RETRIES",
    "RETRY_BACKOFF_BASE",
    "FETCH_TIMEOUT",
    "WATERMARK_FILENAME",
]

# ---------------------------------------------------------------------------
# Guard configuration (C1 — hand-rolled, no tenacity/backoff dependency)
# ---------------------------------------------------------------------------
FETCH_TIMEOUT: float = 120.0
RETRY_MAX_RETRIES: int = 3
RETRY_BACKOFF_BASE: float = 2.0
RETRY_ON: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    HTTPError,
    *_REQUESTS_EXCEPTIONS,
)

WATERMARK_FILENAME = ".watermarks.json"


class FetchTimeoutError(TimeoutError):
    """Raised when a fetch exceeds ``FETCH_TIMEOUT``."""


# ---------------------------------------------------------------------------
# FetchResult
# ---------------------------------------------------------------------------
@dataclass
class FetchResult:
    """Raw fetch result — source-native columns and values."""

    df: pd.DataFrame
    source: str
    asset_type: str
    trade_date: date
    row_count: int
    watermark: str | None


# ---------------------------------------------------------------------------
# Hand-rolled decorators (~30 lines total per design doc C1)
# ---------------------------------------------------------------------------
def _rate_limit(requests_per_second: float) -> Callable:
    """Throttle calls to *requests_per_second*.

    State (last call monotonic time) lives in the closure so it persists
    across calls on the same wrapped function.
    """
    min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0

    def decorator(func: Callable) -> Callable:
        last_call: list[float] = [0.0]

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if min_interval > 0:
                elapsed = time.monotonic() - last_call[0]
                if elapsed < min_interval:
                    time.sleep(min_interval - elapsed)
            last_call[0] = time.monotonic()
            return func(*args, **kwargs)

        return wrapper

    return decorator


def _retry(
    max_retries: int = RETRY_MAX_RETRIES,
    backoff_base: float = RETRY_BACKOFF_BASE,
    initial_wait: float = 1.0,
    retry_on: tuple[type[BaseException], ...] = RETRY_ON,
) -> Callable:
    """Retry on *retry_on* exceptions with exponential backoff.

    Wait per attempt: ``initial_wait * backoff_base**attempt``.
    With defaults (1.0, 2.0) this is 1s / 2s / 4s.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as exc:
                    if attempt == max_retries:
                        raise
                    wait = initial_wait * backoff_base**attempt
                    logger.warning(
                        "retry %d/%d for %s after %s: %s — waiting %.1fs",
                        attempt + 1,
                        max_retries,
                        func.__qualname__,
                        type(exc).__name__,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
            raise RuntimeError("unreachable")  # pragma: no cover

        return wrapper

    return decorator


def _timeout(seconds: float) -> Callable:
    """Enforce a wall-clock timeout (thread-safe, no SIGALRM).

    Uses a single-worker executor so the call runs in a helper thread and
    the caller gets :class:`FetchTimeoutError` when *seconds* elapse.
    The helper thread is abandoned on timeout (``shutdown(wait=False)``) —
    a network call will eventually time out on its own.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            try:
                future = pool.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except concurrent.futures.TimeoutError:
                    raise FetchTimeoutError(
                        f"{func.__qualname__} timed out after {seconds}s"
                    ) from None
            finally:
                pool.shutdown(wait=False)

        return wrapper

    return decorator


def _apply_fetch_guards(fetch_fn: Callable, requests_per_second: float) -> Callable:
    """Layer retry → timeout → rate-limit around *fetch_fn*.

    Order matches ``source_adapter.md`` (@retry outermost): each retry
    attempt gets a fresh timeout and a fresh rate-limit gate, so a long
    backoff cannot eat into the timeout budget and a timeout cannot abort
    the retry loop.
    """
    guarded = _rate_limit(requests_per_second)(fetch_fn)  # innermost
    guarded = _timeout(FETCH_TIMEOUT)(guarded)  # middle
    guarded = _retry()(guarded)  # outermost
    return guarded


# ---------------------------------------------------------------------------
# Watermark persistence — raw/{source}/.watermarks.json
# ---------------------------------------------------------------------------
def _watermark_path(source: str, data_root: Path) -> Path:
    return data_root / "raw" / source / WATERMARK_FILENAME


def read_watermark(source: str, asset_type: str, data_root: Path) -> str | None:
    """Return the stored watermark for *source* / *asset_type*, or ``None``."""
    path = _watermark_path(source, data_root)
    if not path.exists():
        return None
    try:
        data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        logger.warning("corrupt watermark file %s — treating as absent", path)
        return None
    value = data.get(asset_type)
    return str(value) if value is not None else None


def write_watermark(
    source: str, asset_type: str, watermark: str, data_root: Path
) -> None:
    """Persist *watermark* for *source* / *asset_type* (advisory, idempotent-safe)."""
    path = _watermark_path(source, data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = {}
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("corrupt watermark file %s — overwriting", path)
    data[asset_type] = watermark
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# SourceAdapter ABC
# ---------------------------------------------------------------------------
class SourceAdapter(ABC):
    """Abstract base for one external data source.

    Subclasses implement :meth:`fetch` and :meth:`list_asset_types`.
    Rate-limit / retry / timeout guards are injected automatically when a
    subclass is defined (:meth:`__init_subclass__`).
    """

    requests_per_second: float = 5.0

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "fetch" in cls.__dict__:
            cls.fetch = _apply_fetch_guards(cls.fetch, cls.requests_per_second)  # type: ignore[method-assign]

    @abstractmethod
    def fetch(
        self,
        asset_type: str,
        trade_date: date,
        watermark: str | None = None,
    ) -> FetchResult:
        """Fetch raw data for *asset_type* on *trade_date*.

        *watermark* is the value returned by a previous call (or ``None``);
        its format is subclass-defined.  Return a :class:`FetchResult` whose
        ``df`` carries the source's original columns and values.
        """

    @abstractmethod
    def list_asset_types(self) -> list[str]:
        """Return the asset types this adapter can fetch."""

    def _latest_trade_date(self) -> date:
        """Best-known trade date: asset calendar if reachable, else wall clock."""
        cal_path = Path("data_platform/asset/calendar/sse.parquet")
        if cal_path.exists():
            try:
                import duckdb

                row = duckdb.sql(
                    f"SELECT MAX(trade_date) FROM read_parquet('{cal_path}')"
                ).fetchone()
                if row and row[0] is not None:
                    val = row[0]
                    return (
                        val if isinstance(val, date) else date.fromisoformat(str(val))
                    )
            except Exception:
                logger.debug("asset calendar unreadable — falling back to wall clock")
        return date.today()

    def health_check(self) -> bool:
        """Probe one real fetch to verify the source is reachable."""
        asset_types = self.list_asset_types()
        if not asset_types:
            return False
        try:
            trade_date = self._latest_trade_date()
            result = self.fetch(asset_types[0], trade_date)
            return result is not None
        except Exception:
            return False

    def close(self) -> None:
        """Release connections.  Override for sources with connection pools."""
