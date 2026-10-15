"""Data access layer — SQL-only DataAPI + load_bars + alphalens adapters.

The read side of the data domain.  Table structure authority is
:data:`data.storage_schema.SCHEMA` (T1); writes are owned by
:mod:`data.storage_ops` (T2).  This module reads through DuckDB
``read_parquet`` views and adapts output to consumer conventions.

``asset_type`` in :func:`load_bars` is a **table-family** selector
(equity / futures / continuous), not :class:`core.types.AssetType`.
See :data:`TABLE_MAP`.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Iterator

import duckdb
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# asset_type here means table family, not core.types.AssetType.
# "equity"     →  bars_daily / bars_minute      ← AssetType ∈ {stock, etf, fund}
# "futures"    →  bars_daily_futures            ← AssetType.futures
# "continuous" →  continuous                    ← table shape, not an asset type
# ---------------------------------------------------------------------------
# bars_daily holds stock + ETF + LOF rows together.  Selecting a table by the
# 6-member AssetType enum is the wrong dimension — pick the table by family;
# use core.types.EQUITY_TYPES when per-asset rules are needed downstream.
TABLE_MAP: dict[tuple[str, str], str] = {
    ("equity", "daily"): "bars_daily",
    ("equity", "minute"): "bars_minute",
    ("futures", "daily"): "bars_daily_futures",
    ("continuous", "daily"): "continuous",
}

# freq → base freq for table lookup (5min/60min aggregate from minute bars)
_FREQ_BASE: dict[str, str] = {
    "daily": "daily",
    "minute": "minute",
    "5min": "minute",
    "60min": "minute",
}

# Aggregation interval strings for DuckDB time_bucket
_AGG_INTERVAL: dict[str, str] = {
    "5min": "5 minutes",
    "60min": "60 minutes",
}

# Standard output columns in contract order.  adjfactor is appended when the
# source table carries it (bars_daily, continuous).
_STD_COLS: list[str] = [
    "date", "ticker", "open", "high", "low", "close", "volume", "amount",
]

# Price columns that receive the adjustment ratio.
_PRICE_COLS: list[str] = ["open", "high", "low", "close"]

# alphalens defaults (§27 periods=(1,5,10,20); quantiles is a Dev default)
ALPHALENS_PERIODS: tuple[int, ...] = (1, 5, 10, 20)
ALPHALENS_QUANTILES: int = 5

_DEFAULT_DATA_ROOT = "data_platform/asset"
_DEFAULT_START = "2020-01-01"
_DEFAULT_END = "2024-12-31"


def _base_freq(freq: str) -> str:
    """Map *freq* to its base table frequency (5min/60min → minute)."""
    try:
        return _FREQ_BASE[freq]
    except KeyError:
        raise ValueError(
            f"unknown freq {freq!r}; expected one of {sorted(_FREQ_BASE)}"
        ) from None


def _resolve_table(asset_type: str, freq: str) -> str:
    """Return the storage table for (*asset_type*, *freq*)."""
    base = _base_freq(freq)
    key = (asset_type, base)
    try:
        return TABLE_MAP[key]
    except KeyError:
        valid = sorted(TABLE_MAP)
        raise ValueError(
            f"no table for asset_type={asset_type!r} freq={freq!r}; "
            f"valid (asset_type, base_freq) pairs: {valid}"
        ) from None


# ---------------------------------------------------------------------------
# DataAPI — in-memory DuckDB, SQL-only (~80 lines by design)
# ---------------------------------------------------------------------------
class DataAPI:
    """SQL-only query layer over Parquet files via DuckDB ``read_parquet``.

    In-memory connection (no ``.duckdb`` file).  One instance per process;
    DuckDB connections are not process-safe.  Threads within one process may
    share an instance — DuckDB is thread-safe and uses all available CPUs.
    Connection *lifecycle* is not free-threaded though: ``reconnect`` replaces
    ``_conn``, so it and ``close`` serialize on ``_conn_lock``.
    """

    def __init__(
        self,
        data_root: str | Path = _DEFAULT_DATA_ROOT,
        *,
        statement_timeout: float = 60.0,
    ) -> None:
        self._root = Path(data_root)
        self._statement_timeout = statement_timeout
        self._conn_lock = threading.Lock()
        self._conn: duckdb.DuckDBPyConnection = duckdb.connect()
        self._apply_timeout()
        self._views_registered = False

    def _apply_timeout(self) -> None:
        """Apply statement timeout if supported by the DuckDB build."""
        ms = int(self._statement_timeout * 1000)
        try:
            self._conn.execute(f"SET statement_timeout = '{ms}ms'")
        except duckdb.CatalogException:
            # DuckDB 1.5.0 does not support statement_timeout; the [COST]
            # logging still reports slow queries.
            logger.debug("statement_timeout not supported in this DuckDB build")

    # -- view registration ---------------------------------------------------
    def register_views(self) -> None:
        """Create or replace a view per table directory under ``data_root``.

        Each immediate sub-directory that contains ``*.parquet`` becomes a
        view of the same name.  ``union_by_name=true`` aligns columns across
        schema evolution; ``hive_partitioning=false`` because the layout is
        flat filenames, not Hive partitions.
        """
        if not self._root.exists():
            logger.debug("data_root %s does not exist; no views registered", self._root)
            return
        for entry in sorted(self._root.iterdir()):
            if not entry.is_dir():
                continue
            glob = entry / "*.parquet"
            # DuckDB binds `read_parquet` at CREATE VIEW time and raises
            # `IO Error: No files found` on a zero match. One legitimately
            # empty table directory — e.g. `asset/fundamental/` created before
            # its first write lands — would otherwise abort registration, and
            # since `__enter__` calls this, every `load_bars()` on that root
            # would fail even though the other tables hold data.
            if not any(entry.glob("*.parquet")):
                logger.debug("no parquet files under %s; no view registered", entry)
                continue
            self._conn.execute(
                f"CREATE OR REPLACE VIEW {entry.name} AS "
                f"SELECT * FROM read_parquet('{glob}', "
                f"union_by_name=true, hive_partitioning=false)"
            )
        self._views_registered = True

    # -- query ---------------------------------------------------------------
    def query(self, sql: str, profile: bool = False) -> pd.DataFrame:
        """Execute *sql* and return a DataFrame.  Logs a ``[COST]`` line."""
        try:
            if profile:
                self._conn.execute("PRAGMA enable_profiling")
            t0 = time.perf_counter()
            result = self._conn.execute(sql).fetchdf()
            elapsed = time.perf_counter() - t0
        except duckdb.ConnectionException:
            # Jupyter long-session reconnect: one retry after reconnect.
            logger.warning("DuckDB connection lost; reconnecting once")
            self.reconnect()
            if profile:
                self._conn.execute("PRAGMA enable_profiling")
            t0 = time.perf_counter()
            result = self._conn.execute(sql).fetchdf()
            elapsed = time.perf_counter() - t0
        finally:
            if profile:
                self._conn.execute("PRAGMA disable_profiling")
        level = logging.INFO
        if elapsed > 30:
            level = logging.CRITICAL
        elif elapsed > 5:
            level = logging.WARNING
        logger.log(
            level,
            "[COST] time=%.3fs rows=%d sql=%s",
            elapsed, len(result), sql[:200],
        )
        return result

    def query_arrow(self, sql: str) -> pa.Table:
        """Execute *sql* and return a PyArrow table."""
        t0 = time.perf_counter()
        result = self._conn.execute(sql).to_arrow_table()
        elapsed = time.perf_counter() - t0
        logger.info("[COST] time=%.3fs rows=%d sql=%s", elapsed, result.num_rows, sql[:200])
        return result

    # -- lifecycle -----------------------------------------------------------
    def reconnect(self) -> None:
        """Recreate the in-memory connection and re-register views.

        For Jupyter kernels where the old connection has gone stale.
        Serialized: threads share one instance (see the class docstring), and
        closing the connection under a thread that is mid-`execute` produces
        sporadic failures that no one can reproduce on demand.
        """
        with self._conn_lock:
            try:
                self._conn.close()
            except Exception:
                logger.debug("error closing stale DuckDB connection", exc_info=True)
            self._conn = duckdb.connect()
            self._apply_timeout()
            if self._views_registered:
                self.register_views()

    def close(self) -> None:
        with self._conn_lock:
            self._conn.close()

    def __enter__(self) -> DataAPI:
        self.register_views()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# load_bars — table mapping + SQL assembly + adjustment + aggregation
# ---------------------------------------------------------------------------
def _sql_literal(value: str) -> str:
    """Escape a Python string for embedding in SQL."""
    return "'" + value.replace("'", "''") + "'"


def _validate_identifier(name: str) -> str:
    """Reject non-identifier column names before they reach SQL text."""
    if not name.isidentifier():
        raise ValueError(
            f"invalid column name {name!r}: must be a valid Python identifier "
            "(letters, digits, underscores; no SQL fragments)"
        )
    return name


def _symbol_list(symbol: str | list[str] | None) -> list[str]:
    """Normalize a ticker filter. ``None`` means "no filter"; [] raises.

    An **empty** list is not the same as ``None``: it means "nothing was
    selected", and silently widening it to "everything" turns an empty
    watchlist into a full-universe pull — a silent wrong result (and, on
    minute data, an OOM).
    """
    if symbol is None:
        return []
    symbols = [symbol] if isinstance(symbol, str) else list(symbol)
    if not symbols:
        raise ValueError("symbol list is empty; pass None to load all")
    return [str(s) for s in symbols]


def _symbol_filter(symbol: str | list[str] | None) -> str:
    """SQL predicate for a ticker filter; empty string when unfiltered."""
    symbols = _symbol_list(symbol)
    if not symbols:
        return ""
    joined = ", ".join(_sql_literal(s) for s in symbols)
    return f" AND ticker IN ({joined})"


def _build_daily_sql(
    table: str,
    start: str,
    end: str,
    symbol: str | list[str] | None,
    fields: list[str] | None,
) -> str:
    """Build the SELECT for a non-aggregated table (daily or raw minute)."""
    has_adj = table in ("bars_daily", "continuous")
    ticker_col = "dominant_contract" if table == "continuous" else "ticker"
    # bars_minute uses datetime as the bar timestamp; all other tables use trade_date
    date_col = "datetime" if table == "bars_minute" else "trade_date"

    select_parts = [f"{date_col} AS date", f"{ticker_col} AS ticker"]
    value_cols = fields if fields is not None else list(_STD_COLS[2:])
    for col in value_cols:
        if col == "adjfactor":
            if has_adj:
                select_parts.append("adjfactor")
            continue
        select_parts.append(_validate_identifier(col))
    if fields is None and has_adj:
        select_parts.append("adjfactor")

    # Filter by trade_date for date-range (all tables have trade_date);
    # output date_col may be datetime for bars_minute.
    where = (
        f"WHERE trade_date BETWEEN {_sql_literal(start)} AND {_sql_literal(end)}"
    )
    # One rule for both table shapes. The continuous branch used to
    # reimplement this inline and widened an empty list to "no filter", the
    # very bug `_symbol_filter` exists to prevent.
    symbols = _symbol_list(symbol)
    if symbols:
        joined = ", ".join(_sql_literal(s) for s in symbols)
        where += f" AND {ticker_col} IN ({joined})"

    cols_sql = ", ".join(select_parts)
    return (
        f"SELECT {cols_sql} FROM {table} {where} "
        f"ORDER BY date, ticker"
    )


def _build_aggregated_sql(
    table: str,
    start: str,
    end: str,
    symbol: str | list[str] | None,
    fields: list[str] | None,
    freq: str,
) -> str:
    """Build the SELECT for 5min/60min time_bucket aggregation over bars_minute.

    Aggregation boundary decision: group by ``time_bucket(...)`` and
    ``trade_date`` to prevent cross-day bars.  Lunch break (11:30–13:00)
    produces no bars, so empty buckets are absent from the output.
    """
    interval = _AGG_INTERVAL[freq]
    value_cols = fields if fields is not None else ["open", "high", "low", "close", "volume", "amount"]

    agg_exprs = {
        "open": "first(open ORDER BY datetime)",
        "high": "max(high)",
        "low": "min(low)",
        "close": "last(close ORDER BY datetime)",
        "volume": "sum(volume)",
        "amount": "sum(amount)",
    }
    select_parts = [
        f"time_bucket(INTERVAL '{interval}', datetime) AS date",
        "ticker",
    ]
    for col in value_cols:
        _validate_identifier(col)
        if col not in agg_exprs:
            raise ValueError(
                f"field {col!r} is not available in {freq!r} aggregation; "
                f"supported fields: {sorted(agg_exprs)}"
            )
        select_parts.append(f"{agg_exprs[col]} AS {col}")

    where = (
        f"WHERE trade_date BETWEEN {_sql_literal(start)} AND {_sql_literal(end)}"
    )
    where += _symbol_filter(symbol)

    cols_sql = ", ".join(select_parts)
    return (
        f"SELECT {cols_sql} FROM {table} {where} "
        f"GROUP BY date, ticker, trade_date "
        f"ORDER BY date, ticker"
    )


def _apply_adjustment(
    df: pd.DataFrame,
    api: DataAPI,
    table: str,
    adjust: str,
) -> pd.DataFrame:
    """Apply forward/backward price adjustment.

    Denominator uses full-history base (§43) — ``arg_max``/``arg_min`` over the
    entire table, not the query window, so a truncated window gets the correct
    scale.  Forward: ``qfq = raw × adj / adj_latest``.  Backward:
    ``hfq = raw × adj / adj_first``.

    Only price columns are scaled; volume and amount stay raw.
    """
    date_col = "trade_date"
    ticker_col = "dominant_contract" if table == "continuous" else "ticker"
    base_sql = (
        f"SELECT {ticker_col} AS ticker, "
        f"arg_max(adjfactor, {date_col}) AS adj_latest, "
        f"arg_min(adjfactor, {date_col}) AS adj_first "
        f"FROM {table} GROUP BY {ticker_col}"
    )
    base = api.query(base_sql)
    m = df.merge(base, on="ticker", how="left")

    if adjust == "forward":
        ratio = m["adjfactor"] / m["adj_latest"]
    elif adjust == "backward":
        ratio = m["adjfactor"] / m["adj_first"]
    else:
        raise ValueError(f"unknown adjust {adjust!r}; expected 'none', 'forward', or 'backward'")

    for col in _PRICE_COLS:
        if col in m.columns:
            m[col] = m[col] * ratio

    drop_cols = [c for c in ("adj_latest", "adj_first") if c in m.columns]
    return m.drop(columns=drop_cols)


def load_bars(
    symbol: str | list[str] | None = None,
    start: str = _DEFAULT_START,
    end: str = _DEFAULT_END,
    freq: str = "daily",
    fields: list[str] | None = None,
    adjust: str = "none",
    asset_type: str = "equity",
    *,
    data_root: str | Path = _DEFAULT_DATA_ROOT,
) -> pd.DataFrame:
    """Load bars from the storage layer.

    Returns columns ``date, ticker, open, high, low, close, volume, amount``
    plus ``adjfactor`` when the source table carries it, sorted by
    ``(date, ticker)``.

    Parameters
    ----------
    symbol : ticker or list of tickers; ``None`` loads all.
    start, end : date range (inclusive), ISO strings.
    freq : ``daily`` | ``minute`` | ``5min`` | ``60min``.
    fields : subset of value columns; ``None`` returns all standard ones.
    adjust : ``none`` | ``forward`` | ``backward``.
    asset_type : **table family** — ``equity`` / ``futures`` / ``continuous``.
    """
    table = _resolve_table(asset_type, freq)
    has_adj = table in ("bars_daily", "continuous")
    is_agg = freq in _AGG_INTERVAL

    if adjust != "none" and not has_adj:
        raise ValueError(
            f"adjust={adjust!r} requires adjfactor but table {table!r} has none"
        )

    # When adjusting, adjfactor must be in the SELECT even if fields omits it.
    # It is dropped from the result below if the caller did not request it.
    effective_fields = fields
    drop_adj_after = False
    if adjust != "none" and has_adj and fields is not None and "adjfactor" not in fields:
        effective_fields = [*fields, "adjfactor"]
        drop_adj_after = True

    with DataAPI(data_root) as api:
        if is_agg:
            sql = _build_aggregated_sql(table, start, end, symbol, effective_fields, freq)
        else:
            sql = _build_daily_sql(table, start, end, symbol, effective_fields)
        df = api.query(sql)

        if adjust != "none" and not df.empty:
            df = _apply_adjustment(df, api, table, adjust)

    if drop_adj_after and "adjfactor" in df.columns:
        df = df.drop(columns=["adjfactor"])

    # Ensure contract column order and sort
    if not df.empty:
        ordered = [c for c in _STD_COLS if c in df.columns]
        if "adjfactor" in df.columns:
            ordered.append("adjfactor")
        extras = [c for c in df.columns if c not in ordered]
        df = df[[*ordered, *extras]]
        df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    return df


def load_bars_chunked(
    symbol: str | list[str] | None = None,
    start: str = _DEFAULT_START,
    end: str = _DEFAULT_END,
    freq: str = "daily",
    fields: list[str] | None = None,
    adjust: str = "none",
    asset_type: str = "equity",
    chunk_by: str = "month",
    *,
    data_root: str | Path = _DEFAULT_DATA_ROOT,
) -> Iterator[pd.DataFrame]:
    """Yield :func:`load_bars` results in calendar-month chunks.

    Calendar-month (not trading-month): simpler and needs no calendar query;
    empty months yield nothing.  For minute data over >3 months this avoids
    OOM (≈20 GB/year for one year of minute bars).
    """
    if chunk_by != "month":
        raise ValueError(f"unsupported chunk_by {chunk_by!r}; only 'month' is implemented")
    for chunk_start, chunk_end in _date_chunks(start, end):
        df = load_bars(
            symbol, chunk_start, chunk_end,
            freq=freq, fields=fields, adjust=adjust,
            asset_type=asset_type, data_root=data_root,
        )
        if not df.empty:
            yield df


def _date_chunks(start: str, end: str) -> list[tuple[str, str]]:
    """Split [start, end] into calendar-month chunks.

    Chunking mode is the caller's concern (:func:`load_bars_chunked` validates
    ``chunk_by``); only month chunks exist.
    """
    from datetime import date, timedelta

    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    chunks: list[tuple[str, str]] = []
    current = s
    while current <= e:
        # End of current calendar month
        if current.month == 12:
            month_end = date(current.year, 12, 31)
        else:
            month_end = date(current.year, current.month + 1, 1) - timedelta(days=1)
        chunk_end = min(month_end, e)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return chunks


# ---------------------------------------------------------------------------
# alphalens adapters — adjustment + timestamp, nothing else
# ---------------------------------------------------------------------------
def to_alphalens_prices(
    start: str = _DEFAULT_START,
    end: str = _DEFAULT_END,
    symbols: list[str] | None = None,
    *,
    data_root: str | Path = _DEFAULT_DATA_ROOT,
) -> pd.DataFrame:
    """Return date × ticker close-price DataFrame for alphalens.

    Forward-adjusted (``adjust="forward"``) daily closes, pivoted to
    shape ``(dates, tickers)``.
    """
    df = load_bars(
        symbols, start, end,
        freq="daily", adjust="forward",
        asset_type="equity", data_root=data_root,
    )
    if df.empty:
        return pd.DataFrame()
    return df.pivot(index="date", columns="ticker", values="close")


def to_alphalens_factor(factor_df: pd.DataFrame) -> pd.Series:
    """Convert a factor DataFrame to an alphalens MultiIndex Series.

    Input: DataFrame with columns ``date``, ``ticker``, and one factor column.
    Output: Series with MultiIndex ``(date, ticker)``.
    """
    value_cols = [c for c in factor_df.columns if c not in ("date", "ticker")]
    if len(value_cols) != 1:
        raise ValueError(
            f"factor_df must have exactly one value column besides date/ticker; "
            f"got {len(value_cols)}: {value_cols}"
        )
    value_col = value_cols[0]
    indexed = factor_df.set_index(["date", "ticker"])[value_col]
    indexed.name = value_col
    return indexed.sort_index()
