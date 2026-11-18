"""Integration tests for data.data_api — DataAPI, load_bars, alphalens adapters.

Covers the QUANT-14 DoD rows:

* load_bars return columns exactly ``date, ticker, open, high, low, close,
  volume, amount, [adjfactor]`` sorted by ``(date, ticker)``
* forward adjustment ``qfq = raw × adj / adj_latest`` within 1e-6
* ``to_alphalens_prices`` shape is date × ticker
* 5-ETF daily ``load_bars`` under 1 s
* table-family mapping (equity / futures / continuous)

Test data is written through ``data.storage_ops.write_parquet_atomic`` so the
T1 schema is exercised on the write path before the read path under test.
"""

from __future__ import annotations

import time
from dataclasses import FrozenInstanceError
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import get_args

import pandas as pd
import pytest

from core.config import DATA_ROOT, get_data_root, use_data_root
from data.data_api import (
    ALPHALENS_PERIODS,
    ALPHALENS_QUANTILES,
    TABLE_SCHEMA,
    AdjustMode,
    BarFreq,
    BarsMeta,
    BarsResult,
    DataAPI,
    TableFamily,
    _resolve_table,
    clear_adj_cache,
    clear_api_cache,
    get_api,
    load_bars,
    load_bars_chunked,
    load_bars_result,
    to_alphalens_factor,
    to_alphalens_prices,
)
from data.storage_ops import write_parquet_atomic

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers — build valid rows matching T1 schemas
# ---------------------------------------------------------------------------
D = date(2024, 1, 2)


def _bars_daily_row(
    ticker: str = "600519.SH",
    trade_date: date = D,
    close: float = 10.5,
    adjfactor: float = 1.0,
    **overrides,
) -> dict:
    row = {
        "ticker": ticker,
        "trade_date": trade_date,
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": close,
        "volume": 1000,
        "amount": 10500.0,
        "adjfactor": adjfactor,
        "turnover": 0.5,
        "suspend": False,
        "is_limit_up": False,
        "is_limit_down": False,
        "quality_flag": 0,
    }
    row.update(overrides)
    return row


def _bars_minute_row(
    ticker: str = "600519.SH",
    trade_date: date = D,
    dt: datetime | None = None,
    **overrides,
) -> dict:
    if dt is None:
        dt = datetime(trade_date.year, trade_date.month, trade_date.day, 9, 31)
    row = {
        "ticker": ticker,
        "trade_date": trade_date,
        "datetime": dt,
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 100,
        "amount": 1050.0,
        "quality_flag": 0,
    }
    row.update(overrides)
    return row


def _bars_daily_futures_row(
    ticker: str = "IF2406.CFFEX",
    trade_date: date = D,
    **overrides,
) -> dict:
    row = {
        "ticker": ticker,
        "trade_date": trade_date,
        "open": 3500.0,
        "high": 3520.0,
        "low": 3480.0,
        "close": 3510.0,
        "settle": 3505.0,
        "volume": 50000,
        "amount": 1755000000.0,
        "open_interest": 80000,
        "quality_flag": 0,
    }
    row.update(overrides)
    return row


def _continuous_row(
    trade_date: date = D,
    dominant_contract: str = "IF2406.CFFEX",
    **overrides,
) -> dict:
    row = {
        "trade_date": trade_date,
        "dominant_contract": dominant_contract,
        "open": 3500.0,
        "high": 3520.0,
        "low": 3480.0,
        "close": 3510.0,
        "volume": 50000,
        "amount": 1755000000.0,
        "open_interest": 80000,
        "settle": 3505.0,
        "adjfactor": 1.0,
    }
    row.update(overrides)
    return row


def _write_daily(root: Path, trade_date: date, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    path = root / "bars_daily" / f"{trade_date.isoformat()}.parquet"
    write_parquet_atomic(df, path, table="bars_daily")


def _write_minute(root: Path, trade_date: date, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    path = root / "bars_minute" / f"{trade_date.isoformat()}.parquet"
    write_parquet_atomic(df, path, table="bars_minute")


def _write_futures_daily(root: Path, trade_date: date, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    path = root / "bars_daily_futures" / f"{trade_date.isoformat()}.parquet"
    write_parquet_atomic(df, path, table="bars_daily_futures")


def _write_continuous(root: Path, symbol: str, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    path = root / "continuous" / f"{symbol}.parquet"
    write_parquet_atomic(df, path, table="continuous")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_api_and_adj_caches():
    clear_api_cache()
    clear_adj_cache()
    yield
    clear_api_cache()
    clear_adj_cache()


@pytest.fixture
def asset_root(tmp_path: Path) -> Path:
    """Populated data root with bars_daily for two tickers over 3 days.

    Ticker A (600519.SH) has a 2:1 split on day 3 (adjfactor 1.0 → 2.0).
    Ticker B (000001.SZ) has no corporate action (adjfactor stays 1.0).
    """
    root = tmp_path / "asset"
    (root / "bars_daily").mkdir(parents=True)

    d1, d2, d3 = date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)

    _write_daily(
        root,
        d1,
        [
            _bars_daily_row("600519.SH", d1, close=10.0, adjfactor=1.0),
            _bars_daily_row("000001.SZ", d1, close=20.0, adjfactor=1.0),
        ],
    )
    _write_daily(
        root,
        d2,
        [
            _bars_daily_row("600519.SH", d2, close=11.0, adjfactor=1.0),
            _bars_daily_row("000001.SZ", d2, close=21.0, adjfactor=1.0),
        ],
    )
    _write_daily(
        root,
        d3,
        [
            # 2:1 split → adjfactor jumps to 2.0
            _bars_daily_row("600519.SH", d3, close=5.5, adjfactor=2.0),
            _bars_daily_row("000001.SZ", d3, close=22.0, adjfactor=1.0),
        ],
    )
    return root


@pytest.fixture
def minute_root(tmp_path: Path) -> Path:
    """bars_minute data: one ticker, 4 minute bars on a single day."""
    root = tmp_path / "asset"
    (root / "bars_minute").mkdir(parents=True)
    d = date(2024, 1, 2)
    rows = []
    for i, (h, m, c) in enumerate(
        [
            (9, 31, 10.0),
            (9, 32, 10.2),
            (9, 33, 10.1),
            (9, 34, 10.3),
        ]
    ):
        rows.append(
            _bars_minute_row(
                "600519.SH",
                d,
                dt=datetime(2024, 1, 2, h, m),
                close=c,
                volume=100 + i,
                amount=(100 + i) * c,
            )
        )
    _write_minute(root, d, rows)
    return root


@pytest.fixture
def futures_root(tmp_path: Path) -> Path:
    root = tmp_path / "asset"
    (root / "bars_daily_futures").mkdir(parents=True)
    d = date(2024, 1, 2)
    _write_futures_daily(root, d, [_bars_daily_futures_row("IF2406.CFFEX", d)])
    return root


@pytest.fixture
def continuous_root(tmp_path: Path) -> Path:
    root = tmp_path / "asset"
    (root / "continuous").mkdir(parents=True)
    rows = [
        _continuous_row(date(2024, 1, 2), "IF2406.CFFEX", close=3500.0),
        _continuous_row(date(2024, 1, 3), "IF2406.CFFEX", close=3510.0),
    ]
    _write_continuous(root, "IF", rows)
    return root


# ---------------------------------------------------------------------------
# DataAPI basics
# ---------------------------------------------------------------------------
class TestDataAPI:
    def test_register_views_and_query(self, asset_root):
        with DataAPI(asset_root) as api:
            df = api.query("SELECT * FROM bars_daily ORDER BY trade_date, ticker")
        assert len(df) == 6  # 2 tickers × 3 days
        assert "close" in df.columns

    def test_persistent_catalog_duckdb_created_and_dual_alias(self, asset_root):
        with DataAPI(asset_root) as api:
            assert api.catalog_path.is_file()
            assert api.catalog_path.name == "catalog.duckdb"
            df = api.query(
                "SELECT * FROM cn_stock_bars_daily ORDER BY trade_date, ticker"
            )
            assert len(df) == 6

    def test_query_returns_dataframe(self, asset_root):
        with DataAPI(asset_root) as api:
            df = api.query(
                "SELECT ticker, close FROM bars_daily WHERE trade_date = '2024-01-02'"
            )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_query_arrow_returns_arrow_table(self, asset_root):
        import pyarrow as pa

        with DataAPI(asset_root) as api:
            table = api.query_arrow("SELECT * FROM bars_daily")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 6

    def test_empty_root_no_crash(self, tmp_path):
        empty = tmp_path / "empty_asset"
        empty.mkdir()
        with DataAPI(empty) as api:
            # No views registered — querying a non-existent view raises
            with pytest.raises(Exception):
                api.query("SELECT * FROM nonexistent_table")

    def test_reconnect(self, asset_root):
        api = DataAPI(asset_root)
        api.register_views()
        df1 = api.query("SELECT COUNT(*) AS n FROM bars_daily")
        api.reconnect()
        df2 = api.query("SELECT COUNT(*) AS n FROM bars_daily")
        assert df1["n"].iloc[0] == df2["n"].iloc[0]
        api.close()

    def test_statement_timeout_set(self, asset_root):
        api = DataAPI(asset_root, statement_timeout=30.0)
        api.register_views()
        df = api.query("SELECT 1 AS x")
        assert df["x"].iloc[0] == 1
        api.close()

    def test_profile_true_does_not_crash(self, asset_root):
        """profile=True must execute inside the reconnect try, not crash on pragma."""
        api = DataAPI(asset_root)
        api.register_views()
        df = api.query("SELECT COUNT(*) AS n FROM bars_daily", profile=True)
        assert df["n"].iloc[0] == 6
        api.close()

    def test_profile_reissues_pragma_after_reconnect(self, asset_root, monkeypatch):
        """profile=True on a stale connection must re-enable profiling on the new one.

        Review Finding: PRAGMA enable_profiling was only issued before the
        failure and never re-issued after reconnect.  The test closes the
        underlying connection first and asserts the pragma ran on the new
        connection generation.
        """
        import duckdb as duckdb_mod

        class _SpyConn:
            """Proxy that records SQL per connection generation."""

            def __init__(self, real, gen, log):
                self._real = real
                self._gen = gen
                self._log = log

            def execute(self, sql, *a, **kw):
                self._log.setdefault(self._gen, []).append(str(sql))
                return self._real.execute(sql, *a, **kw)

            def close(self):
                return self._real.close()

        stmts_per_gen: dict[int, list[str]] = {}
        gen_counter = [0]
        orig_connect = duckdb_mod.connect

        def spy_connect(*a, **kw):
            conn = orig_connect(*a, **kw)
            gen_counter[0] += 1
            return _SpyConn(conn, gen_counter[0], stmts_per_gen)

        monkeypatch.setattr(duckdb_mod, "connect", spy_connect)

        api = DataAPI(asset_root)
        api.register_views()
        # Force the stale-connection path
        api._conn.close()

        df = api.query("SELECT COUNT(*) AS n FROM bars_daily", profile=True)
        assert df["n"].iloc[0] == 6

        # gen 1 = original (dead), gen 2 = reconnected
        assert gen_counter[0] == 2, (
            f"expected 2 connection generations, got {gen_counter[0]}"
        )
        new_gen_stmts = stmts_per_gen.get(2, [])
        assert any("enable_profiling" in s for s in new_gen_stmts), (
            f"enable_profiling not re-issued on reconnected connection; "
            f"gen2 statements: {new_gen_stmts}"
        )
        api.close()


# ---------------------------------------------------------------------------
# load_bars — return columns and sort order (DoD row)
# ---------------------------------------------------------------------------
class TestLoadBarsColumns:
    def test_default_columns_exact(self, asset_root):
        df = load_bars(start="2024-01-02", end="2024-01-04", data_root=asset_root)
        expected = [
            "date",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "adjfactor",
        ]
        assert list(df.columns) == expected

    def test_sorted_by_date_ticker(self, asset_root):
        df = load_bars(start="2024-01-02", end="2024-01-04", data_root=asset_root)
        pairs = list(zip(df["date"], df["ticker"]))
        assert pairs == sorted(pairs)

    def test_no_adjfactor_for_minute(self, minute_root):
        df = load_bars(
            start="2024-01-02",
            end="2024-01-02",
            freq="minute",
            data_root=minute_root,
        )
        expected = [
            "date",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
        ]
        assert list(df.columns) == expected

    def test_fields_subset(self, asset_root):
        df = load_bars(
            start="2024-01-02",
            end="2024-01-04",
            fields=["close"],
            data_root=asset_root,
        )
        assert "close" in df.columns
        assert "open" not in df.columns

    def test_symbol_filter(self, asset_root):
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02",
            end="2024-01-04",
            data_root=asset_root,
        )
        assert set(df["ticker"]) == {"600519.SH"}
        assert len(df) == 3

    def test_symbol_list_filter(self, asset_root):
        df = load_bars(
            symbol=["600519.SH", "000001.SZ"],
            start="2024-01-02",
            end="2024-01-04",
            data_root=asset_root,
        )
        assert set(df["ticker"]) == {"600519.SH", "000001.SZ"}

    def test_fields_rejects_sql_injection(self, asset_root):
        """fields entries must be valid identifiers — no SQL fragments."""
        with pytest.raises(ValueError, match="invalid column name"):
            load_bars(
                fields=["close; DROP TABLE bars_daily"],
                data_root=asset_root,
            )


# ---------------------------------------------------------------------------
# Forward adjustment — DoD: qfq = raw × adj / adj_latest within 1e-6
# ---------------------------------------------------------------------------
class TestAdjustment:
    def test_forward_adjustment_hand_calculated(self, asset_root):
        """Hand-calculate qfq for 600519.SH with the 2:1 split.

        adjfactor history: day1=1.0, day2=1.0, day3=2.0.
        adj_latest = 2.0 (arg_max by trade_date over full history).

        qfq = raw × adjfactor / adj_latest:
          day1: 10.0 × 1.0 / 2.0 = 5.0
          day2: 11.0 × 1.0 / 2.0 = 5.5
          day3:  5.5 × 2.0 / 2.0 = 5.5
        """
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02",
            end="2024-01-04",
            adjust="forward",
            data_root=asset_root,
        )
        expected_closes = [5.0, 5.5, 5.5]
        for i, expected in enumerate(expected_closes):
            assert abs(df["close"].iloc[i] - expected) < 1e-6, (
                f"row {i}: expected {expected}, got {df['close'].iloc[i]}"
            )

    def test_forward_all_price_cols_adjusted(self, asset_root):
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02",
            end="2024-01-04",
            adjust="forward",
            data_root=asset_root,
        )
        # day1: open=10.0, high=11.0, low=9.0 → all × 1.0/2.0
        assert abs(df["open"].iloc[0] - 5.0) < 1e-6
        assert abs(df["high"].iloc[0] - 5.5) < 1e-6
        assert abs(df["low"].iloc[0] - 4.5) < 1e-6

    def test_backward_adjustment(self, asset_root):
        """Backward: hfq = raw × adj / adj_first.

        adj_first = 1.0 (arg_min by trade_date).
          day1: 10.0 × 1.0 / 1.0 = 10.0
          day3:  5.5 × 2.0 / 1.0 = 11.0
        """
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02",
            end="2024-01-04",
            adjust="backward",
            data_root=asset_root,
        )
        assert abs(df["close"].iloc[0] - 10.0) < 1e-6
        assert abs(df["close"].iloc[2] - 11.0) < 1e-6

    def test_adjustment_base_not_window_limited(self, asset_root):
        """Adjustment denominator uses full history, not the query window.

        Query only day3 (adjfactor=2.0).  adj_latest is still 2.0 from the
        full table, so qfq = 5.5 × 2.0 / 2.0 = 5.5.  If we used the window,
        adj_latest would also be 2.0 — so query day1-2 only to distinguish.
        """
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02",
            end="2024-01-03",
            adjust="forward",
            data_root=asset_root,
        )
        # Full-history adj_latest = 2.0 (from day3), so:
        # day1: 10.0 × 1.0 / 2.0 = 5.0  (NOT 10.0 — window-limited would give 10.0)
        assert abs(df["close"].iloc[0] - 5.0) < 1e-6

    def test_no_adjustment_leaves_prices_raw(self, asset_root):
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02",
            end="2024-01-04",
            adjust="none",
            data_root=asset_root,
        )
        assert abs(df["close"].iloc[0] - 10.0) < 1e-6
        assert abs(df["close"].iloc[2] - 5.5) < 1e-6

    def test_adjust_without_adjfactor_raises(self, minute_root):
        with pytest.raises(ValueError, match="adjfactor"):
            load_bars(
                start="2024-01-02",
                end="2024-01-02",
                freq="minute",
                adjust="forward",
                data_root=minute_root,
            )

    def test_fields_with_forward_adjustment(self, asset_root):
        """fields=['close'] + adjust='forward' must not crash (Finding 1).

        adjfactor is needed for the ratio but the caller did not request it
        in ``fields`` — it is forced into the SELECT, used for the ratio,
        then dropped from the output.
        """
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02",
            end="2024-01-04",
            fields=["close"],
            adjust="forward",
            data_root=asset_root,
        )
        assert "close" in df.columns
        assert "adjfactor" not in df.columns
        assert "open" not in df.columns
        # qfq close: day1=5.0, day2=5.5, day3=5.5 (2:1 split, adj_latest=2.0)
        assert abs(df["close"].iloc[0] - 5.0) < 1e-6
        assert abs(df["close"].iloc[1] - 5.5) < 1e-6
        assert abs(df["close"].iloc[2] - 5.5) < 1e-6

    def test_fields_with_adjustment_keeps_adjfactor_when_requested(self, asset_root):
        """fields including adjfactor + adjust: adjfactor stays in output."""
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02",
            end="2024-01-04",
            fields=["close", "adjfactor"],
            adjust="forward",
            data_root=asset_root,
        )
        assert "close" in df.columns
        assert "adjfactor" in df.columns
        assert abs(df["close"].iloc[0] - 5.0) < 1e-6


# ---------------------------------------------------------------------------
# Table-family mapping (TableFamily / TABLE_SCHEMA / _resolve_table)
# ---------------------------------------------------------------------------
class TestTableFamilyMapping:
    def test_equity_aliases_map_to_canonical_stock_tables(self):
        for alias in ("equity", "stock", "cn_stock", "cn_equity"):
            assert _resolve_table(alias, "daily") == TableFamily.CN_STOCK_DAILY.value
            assert _resolve_table(alias, "minute") == TableFamily.CN_STOCK_MINUTE.value
        assert (
            _resolve_table(TableFamily.CN_STOCK_DAILY, "daily") == "cn_stock_bars_daily"
        )

    def test_etf_futures_and_continuous_map_to_canonical_tables(self):
        for alias in ("etf", "cn_etf"):
            assert _resolve_table(alias, "daily") == TableFamily.CN_ETF_DAILY.value
            assert _resolve_table(alias, "minute") == TableFamily.CN_ETF_MINUTE.value
        for alias in ("futures", "cn_futures"):
            assert _resolve_table(alias, "daily") == TableFamily.CN_FUTURES_DAILY.value
        for alias in ("continuous", "cn_continuous"):
            assert (
                _resolve_table(alias, "daily") == TableFamily.CN_CONTINUOUS_DAILY.value
            )

    def test_table_schema_declares_every_table_family_member(self):
        for member in TableFamily:
            assert member.value in TABLE_SCHEMA
            spec = TABLE_SCHEMA[member.value]
            assert {"date_col", "filter_col", "ticker_col", "has_adjfactor"} <= set(
                spec
            )

    def test_equity_aliases_load_identical_bars(self, asset_root):
        baseline = load_bars(
            start="2024-01-02",
            end="2024-01-04",
            adjust="forward",
            table_family="equity",
            data_root=asset_root,
        )
        for alias in ("stock", "cn_stock", "cn_equity", TableFamily.CN_STOCK_DAILY):
            got = load_bars(
                start="2024-01-02",
                end="2024-01-04",
                adjust="forward",
                table_family=alias,
                data_root=asset_root,
            )
            pd.testing.assert_frame_equal(baseline, got)

    def test_futures_load(self, futures_root):
        df = load_bars(
            start="2024-01-02",
            end="2024-01-02",
            table_family="futures",
            data_root=futures_root,
        )
        assert len(df) == 1
        assert df["ticker"].iloc[0] == "IF2406.CFFEX"

    def test_continuous_load(self, continuous_root):
        df = load_bars(
            start="2024-01-02",
            end="2024-01-03",
            table_family="continuous",
            data_root=continuous_root,
        )
        assert len(df) == 2
        # dominant_contract is mapped to ticker
        assert df["ticker"].iloc[0] == "IF2406.CFFEX"
        assert "adjfactor" in df.columns

    def test_invalid_table_family_raises(self, asset_root):
        with pytest.raises(ValueError, match="no table"):
            load_bars(table_family="bond", data_root=asset_root)

    def test_invalid_freq_raises(self, asset_root):
        with pytest.raises(ValueError, match="unknown freq"):
            load_bars(freq="weekly", data_root=asset_root)


# ---------------------------------------------------------------------------
# Singleton connection, _ADJ_CACHE, and use_data_root context isolation
# ---------------------------------------------------------------------------
class TestSingletonAdjCacheAndContextRoot:
    def test_singleton_and_adj_cache_across_repeated_calls(
        self, asset_root, monkeypatch
    ):
        import duckdb as duckdb_mod

        connect_calls = 0
        orig_connect = duckdb_mod.connect

        def spy_connect(*args, **kwargs):
            nonlocal connect_calls
            connect_calls += 1
            return orig_connect(*args, **kwargs)

        monkeypatch.setattr(duckdb_mod, "connect", spy_connect)

        adj_queries = 0
        orig_query = DataAPI.query

        def spy_query(self, sql: str, profile: bool = False):
            nonlocal adj_queries
            if "arg_max(adjfactor" in sql:
                adj_queries += 1
            return orig_query(self, sql, profile=profile)

        monkeypatch.setattr(DataAPI, "query", spy_query)

        for _ in range(10):
            df = load_bars(
                symbol="600519.SH",
                start="2024-01-02",
                end="2024-01-04",
                adjust="forward",
                data_root=asset_root,
            )
            assert abs(df["close"].iloc[0] - 5.0) < 1e-6

        assert connect_calls == 1
        assert adj_queries == 1

        # Clearing _ADJ_CACHE forces recompute on next adjusted query while keeping DataAPI singleton
        clear_adj_cache()
        load_bars(
            symbol="600519.SH",
            start="2024-01-02",
            end="2024-01-04",
            adjust="forward",
            data_root=asset_root,
        )
        assert connect_calls == 1
        assert adj_queries == 2

        # get_api returns the same singleton instance for identical resolved root
        assert get_api(asset_root) is get_api(str(asset_root))

    def test_use_data_root_isolates_load_bars_without_explicit_param(self, tmp_path):
        asset = tmp_path / "asset"
        (asset / "cn_stock_bars_daily").mkdir(parents=True)
        d = date(2024, 1, 2)
        write_parquet_atomic(
            pd.DataFrame([_bars_daily_row("688001.SH", d, close=99.5, adjfactor=1.0)]),
            asset / "cn_stock_bars_daily" / f"{d.isoformat()}.parquet",
            table="cn_stock_bars_daily",
        )

        before = get_data_root()
        with use_data_root(tmp_path):
            assert get_data_root() == tmp_path
            df = load_bars(start="2024-01-02", end="2024-01-02")
            assert df["ticker"].tolist() == ["688001.SH"]
            assert df["close"].iloc[0] == pytest.approx(99.5)

        assert get_data_root() == before
        assert get_data_root() == DATA_ROOT


# ---------------------------------------------------------------------------
# 5min / 60min aggregation
# ---------------------------------------------------------------------------
class TestAggregation:
    def test_5min_aggregation(self, minute_root):
        """4 minute bars at 09:31–09:34 → one 5min bucket at 09:30."""
        df = load_bars(
            start="2024-01-02",
            end="2024-01-02",
            freq="5min",
            data_root=minute_root,
        )
        assert len(df) == 1
        row = df.iloc[0]
        # first open = 10.0 (09:31 bar)
        assert abs(row["open"] - 10.0) < 1e-6
        # max high: all bars have high=11.0
        assert abs(row["high"] - 11.0) < 1e-6
        # min low: all bars have low=9.0
        assert abs(row["low"] - 9.0) < 1e-6
        # last close = 10.3 (09:34 bar)
        assert abs(row["close"] - 10.3) < 1e-6
        # sum volume = 100+101+102+103 = 406
        assert row["volume"] == 406

    def test_60min_aggregation(self, minute_root):
        df = load_bars(
            start="2024-01-02",
            end="2024-01-02",
            freq="60min",
            data_root=minute_root,
        )
        assert len(df) == 1
        # Same 4 bars all fall in the 09:00 bucket
        assert df.iloc[0]["volume"] == 406

    def test_minute_returns_datetime_as_date(self, minute_root):
        df = load_bars(
            start="2024-01-02",
            end="2024-01-02",
            freq="minute",
            data_root=minute_root,
        )
        assert len(df) == 4
        # date column carries the bar timestamp
        first = df.iloc[0]["date"]
        assert first == datetime(2024, 1, 2, 9, 31) or first == pd.Timestamp(
            "2024-01-02 09:31:00"
        )

    def test_aggregated_rejects_unsupported_field(self, minute_root):
        """Unsupported field in 5min/60min must fail loudly, not be silently dropped."""
        with pytest.raises(ValueError, match="not available"):
            load_bars(
                start="2024-01-02",
                end="2024-01-02",
                freq="5min",
                fields=["close", "settle"],
                data_root=minute_root,
            )


# ---------------------------------------------------------------------------
# alphalens adapters (DoD: to_alphalens_prices shape = date × ticker)
# ---------------------------------------------------------------------------
class TestAlphalensAdapters:
    def test_to_alphalens_prices_shape(self, asset_root):
        result = to_alphalens_prices(
            start="2024-01-02",
            end="2024-01-04",
            symbols=["600519.SH", "000001.SZ"],
            data_root=asset_root,
        )
        # Shape is date × ticker
        assert result.index.name == "date" or result.index.name is None
        assert set(result.columns) == {"600519.SH", "000001.SZ"}
        assert len(result) == 3  # 3 dates

    def test_to_alphalens_prices_is_forward_adjusted(self, asset_root):
        result = to_alphalens_prices(
            start="2024-01-02",
            end="2024-01-04",
            symbols=["600519.SH"],
            data_root=asset_root,
        )
        # day1 qfq = 5.0 (see TestAdjustment)
        val = result.iloc[0, 0]
        assert abs(val - 5.0) < 1e-6

    def test_to_alphalens_factor_multiindex(self):
        factor_df = pd.DataFrame(
            {
                "date": [date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 3)],
                "ticker": ["600519.SH", "000001.SZ", "600519.SH"],
                "momentum": [0.05, 0.03, 0.07],
            }
        )
        result = to_alphalens_factor(factor_df)
        assert isinstance(result, pd.Series)
        assert result.index.names == ["date", "ticker"]
        assert len(result) == 3

    def test_alphalens_constants(self):
        assert ALPHALENS_PERIODS == (1, 5, 10, 20)
        assert ALPHALENS_QUANTILES == 5

    def test_to_alphalens_factor_rejects_multiple_value_columns(self):
        """Multiple value columns must raise, not silently pick the first."""
        df = pd.DataFrame(
            {
                "date": [date(2024, 1, 2)],
                "ticker": ["600519.SH"],
                "momentum": [0.05],
                "reversal": [0.02],
            }
        )
        with pytest.raises(ValueError, match="exactly one value column"):
            to_alphalens_factor(df)

    def test_to_alphalens_factor_rejects_zero_value_columns(self):
        df = pd.DataFrame(
            {
                "date": [date(2024, 1, 2)],
                "ticker": ["600519.SH"],
            }
        )
        with pytest.raises(ValueError, match="exactly one value column"):
            to_alphalens_factor(df)


# ---------------------------------------------------------------------------
# load_bars_chunked
# ---------------------------------------------------------------------------
class TestLoadBarsChunked:
    def test_monthly_chunks(self, asset_root):
        chunks = list(
            load_bars_chunked(
                start="2024-01-02",
                end="2024-01-04",
                data_root=asset_root,
            )
        )
        assert len(chunks) == 1  # all in one month
        assert len(chunks[0]) == 6

    def test_multi_month_chunks(self, tmp_path):
        root = tmp_path / "asset"
        (root / "bars_daily").mkdir(parents=True)
        # Write one day per month for 2 months
        for d in [date(2024, 1, 15), date(2024, 2, 15)]:
            _write_daily(
                root,
                d,
                [
                    _bars_daily_row("600519.SH", d, adjfactor=1.0),
                ],
            )
        chunks = list(
            load_bars_chunked(
                start="2024-01-01",
                end="2024-02-28",
                data_root=root,
            )
        )
        assert len(chunks) == 2
        assert all(len(c) == 1 for c in chunks)

    def test_empty_months_skipped(self, tmp_path):
        root = tmp_path / "asset"
        (root / "bars_daily").mkdir(parents=True)
        # Only write in January
        _write_daily(
            root,
            date(2024, 1, 15),
            [
                _bars_daily_row("600519.SH", date(2024, 1, 15)),
            ],
        )
        chunks = list(
            load_bars_chunked(
                start="2024-01-01",
                end="2024-03-31",
                data_root=root,
            )
        )
        assert len(chunks) == 1  # only January has data


# ---------------------------------------------------------------------------
# Performance (DoD: 5 ETF daily load_bars < 1s)
# ---------------------------------------------------------------------------
class TestPerformance:
    def test_5_etf_daily_under_one_second(self, tmp_path):
        root = tmp_path / "asset"
        (root / "bars_daily").mkdir(parents=True)

        etfs = ["510300.SH", "510500.SH", "510050.SH", "588000.SH", "159915.SZ"]
        # 20 trading days — enough to exercise the path, small enough to be fast
        dates = [date(2024, 1, 2) + timedelta(days=i) for i in range(20)]

        for d in dates:
            rows = [
                _bars_daily_row(t, d, close=10.0 + i, adjfactor=1.0)
                for i, t in enumerate(etfs)
            ]
            _write_daily(root, d, rows)

        t0 = time.perf_counter()
        df = load_bars(
            symbol=etfs,
            start="2024-01-02",
            end="2024-01-21",
            data_root=root,
        )
        elapsed = time.perf_counter() - t0

        assert len(df) == 5 * 20  # 5 ETFs × 20 days
        assert elapsed < 1.0, f"load_bars took {elapsed:.3f}s (budget: 1s)"


# ---------------------------------------------------------------------------
# BarFreq / AdjustMode enums & BarsMeta / BarsResult (DataMeta)
# ---------------------------------------------------------------------------
class TestBarsMetaAndResult:
    def test_literal_enums_cover_contract_values(self):
        assert set(get_args(BarFreq)) == {"daily", "minute", "5min", "60min"}
        assert set(get_args(AdjustMode)) == {"none", "forward", "backward"}

    def test_load_bars_attaches_bars_meta_on_attrs(self, asset_root):
        df = load_bars(
            symbol=["600519.SH", "000001.SZ"],
            start="2024-01-02",
            end="2024-01-04",
            adjust="forward",
            data_root=asset_root,
        )
        meta = df.attrs["meta"]
        assert isinstance(meta, BarsMeta)
        assert meta == BarsMeta(
            table="cn_stock_bars_daily",
            table_family="equity",
            freq="daily",
            adjust="forward",
            start="2024-01-02",
            end="2024-01-04",
            as_of="2024-01-04",
            rows=6,
            tickers=2,
            source=None,
        )
        with pytest.raises(FrozenInstanceError):
            meta.adjust = "none"  # type: ignore[misc]

    def test_load_bars_empty_window_attaches_zero_meta(self, asset_root):
        df = load_bars(
            start="2025-01-01",
            end="2025-01-10",
            data_root=asset_root,
        )
        assert df.empty
        meta = df.attrs["meta"]
        assert meta.rows == 0
        assert meta.tickers == 0
        assert meta.as_of is None
        assert meta.table == "cn_stock_bars_daily"

    def test_load_bars_result_intraday_meta(self, minute_root):
        result = load_bars_result(
            start="2024-01-02",
            end="2024-01-02",
            freq="5min",
            data_root=minute_root,
        )
        assert isinstance(result, BarsResult)
        assert result.meta is result.data.attrs["meta"]
        assert result.meta.table == "cn_stock_bars_minute"
        assert result.meta.freq == "5min"
        assert result.meta.rows == 1
        assert result.meta.tickers == 1
        assert result.meta.as_of == "2024-01-02T09:30:00"

    def test_source_tag_read_single_multiple_and_aliased_dir(self, tmp_path):
        root = tmp_path / "asset"
        canonical_dir = root / "cn_stock_bars_daily"
        canonical_dir.mkdir(parents=True)

        d1, d2 = date(2024, 1, 2), date(2024, 1, 3)
        write_parquet_atomic(
            pd.DataFrame([_bars_daily_row("600519.SH", d1)]),
            canonical_dir / "2024-01-02.parquet",
            table="cn_stock_bars_daily",
            source_tag="straightflush",
        )
        single = load_bars_result(
            start="2024-01-02",
            end="2024-01-03",
            table_family="equity",
            data_root=root,
        )
        assert single.meta.table == "cn_stock_bars_daily"
        assert single.meta.source == "straightflush"

        write_parquet_atomic(
            pd.DataFrame([_bars_daily_row("600519.SH", d2)]),
            canonical_dir / "2024-01-03.parquet",
            table="cn_stock_bars_daily",
            source_tag="akshare:backfill",
        )
        multi = load_bars_result(
            start="2024-01-02",
            end="2024-01-03",
            table_family="equity",
            data_root=root,
        )
        assert multi.meta.source == "akshare:backfill,straightflush"
