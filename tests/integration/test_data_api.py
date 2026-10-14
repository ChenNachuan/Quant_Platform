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
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from data.data_api import (
    ALPHALENS_PERIODS,
    ALPHALENS_QUANTILES,
    TABLE_MAP,
    DataAPI,
    load_bars,
    load_bars_chunked,
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
@pytest.fixture
def asset_root(tmp_path: Path) -> Path:
    """Populated data root with bars_daily for two tickers over 3 days.

    Ticker A (600519.SH) has a 2:1 split on day 3 (adjfactor 1.0 → 2.0).
    Ticker B (000001.SZ) has no corporate action (adjfactor stays 1.0).
    """
    root = tmp_path / "asset"
    (root / "bars_daily").mkdir(parents=True)

    d1, d2, d3 = date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)

    _write_daily(root, d1, [
        _bars_daily_row("600519.SH", d1, close=10.0, adjfactor=1.0),
        _bars_daily_row("000001.SZ", d1, close=20.0, adjfactor=1.0),
    ])
    _write_daily(root, d2, [
        _bars_daily_row("600519.SH", d2, close=11.0, adjfactor=1.0),
        _bars_daily_row("000001.SZ", d2, close=21.0, adjfactor=1.0),
    ])
    _write_daily(root, d3, [
        # 2:1 split → adjfactor jumps to 2.0
        _bars_daily_row("600519.SH", d3, close=5.5, adjfactor=2.0),
        _bars_daily_row("000001.SZ", d3, close=22.0, adjfactor=1.0),
    ])
    return root


@pytest.fixture
def minute_root(tmp_path: Path) -> Path:
    """bars_minute data: one ticker, 4 minute bars on a single day."""
    root = tmp_path / "asset"
    (root / "bars_minute").mkdir(parents=True)
    d = date(2024, 1, 2)
    rows = []
    for i, (h, m, c) in enumerate([
        (9, 31, 10.0), (9, 32, 10.2), (9, 33, 10.1), (9, 34, 10.3),
    ]):
        rows.append(_bars_minute_row(
            "600519.SH", d,
            dt=datetime(2024, 1, 2, h, m),
            close=c,
            volume=100 + i,
            amount=(100 + i) * c,
        ))
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

    def test_query_returns_dataframe(self, asset_root):
        with DataAPI(asset_root) as api:
            df = api.query("SELECT ticker, close FROM bars_daily WHERE trade_date = '2024-01-02'")
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


# ---------------------------------------------------------------------------
# load_bars — return columns and sort order (DoD row)
# ---------------------------------------------------------------------------
class TestLoadBarsColumns:
    def test_default_columns_exact(self, asset_root):
        df = load_bars(start="2024-01-02", end="2024-01-04", data_root=asset_root)
        expected = ["date", "ticker", "open", "high", "low", "close", "volume", "amount", "adjfactor"]
        assert list(df.columns) == expected

    def test_sorted_by_date_ticker(self, asset_root):
        df = load_bars(start="2024-01-02", end="2024-01-04", data_root=asset_root)
        pairs = list(zip(df["date"], df["ticker"]))
        assert pairs == sorted(pairs)

    def test_no_adjfactor_for_minute(self, minute_root):
        df = load_bars(
            start="2024-01-02", end="2024-01-02",
            freq="minute", data_root=minute_root,
        )
        expected = ["date", "ticker", "open", "high", "low", "close", "volume", "amount"]
        assert list(df.columns) == expected

    def test_fields_subset(self, asset_root):
        df = load_bars(
            start="2024-01-02", end="2024-01-04",
            fields=["close"], data_root=asset_root,
        )
        assert "close" in df.columns
        assert "open" not in df.columns

    def test_symbol_filter(self, asset_root):
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02", end="2024-01-04",
            data_root=asset_root,
        )
        assert set(df["ticker"]) == {"600519.SH"}
        assert len(df) == 3

    def test_symbol_list_filter(self, asset_root):
        df = load_bars(
            symbol=["600519.SH", "000001.SZ"],
            start="2024-01-02", end="2024-01-04",
            data_root=asset_root,
        )
        assert set(df["ticker"]) == {"600519.SH", "000001.SZ"}


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
            start="2024-01-02", end="2024-01-04",
            adjust="forward", data_root=asset_root,
        )
        expected_closes = [5.0, 5.5, 5.5]
        for i, expected in enumerate(expected_closes):
            assert abs(df["close"].iloc[i] - expected) < 1e-6, (
                f"row {i}: expected {expected}, got {df['close'].iloc[i]}"
            )

    def test_forward_all_price_cols_adjusted(self, asset_root):
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02", end="2024-01-04",
            adjust="forward", data_root=asset_root,
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
            start="2024-01-02", end="2024-01-04",
            adjust="backward", data_root=asset_root,
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
            start="2024-01-02", end="2024-01-03",
            adjust="forward", data_root=asset_root,
        )
        # Full-history adj_latest = 2.0 (from day3), so:
        # day1: 10.0 × 1.0 / 2.0 = 5.0  (NOT 10.0 — window-limited would give 10.0)
        assert abs(df["close"].iloc[0] - 5.0) < 1e-6

    def test_no_adjustment_leaves_prices_raw(self, asset_root):
        df = load_bars(
            symbol="600519.SH",
            start="2024-01-02", end="2024-01-04",
            adjust="none", data_root=asset_root,
        )
        assert abs(df["close"].iloc[0] - 10.0) < 1e-6
        assert abs(df["close"].iloc[2] - 5.5) < 1e-6

    def test_adjust_without_adjfactor_raises(self, minute_root):
        with pytest.raises(ValueError, match="adjfactor"):
            load_bars(
                start="2024-01-02", end="2024-01-02",
                freq="minute", adjust="forward", data_root=minute_root,
            )


# ---------------------------------------------------------------------------
# Table-family mapping (asset_type)
# ---------------------------------------------------------------------------
class TestAssetTypeMapping:
    def test_equity_maps_to_bars_daily(self):
        assert TABLE_MAP[("equity", "daily")] == "bars_daily"
        assert TABLE_MAP[("equity", "minute")] == "bars_minute"

    def test_futures_maps_to_bars_daily_futures(self):
        assert TABLE_MAP[("futures", "daily")] == "bars_daily_futures"

    def test_continuous_maps_to_continuous(self):
        assert TABLE_MAP[("continuous", "daily")] == "continuous"

    def test_futures_load(self, futures_root):
        df = load_bars(
            start="2024-01-02", end="2024-01-02",
            asset_type="futures", data_root=futures_root,
        )
        assert len(df) == 1
        assert df["ticker"].iloc[0] == "IF2406.CFFEX"

    def test_continuous_load(self, continuous_root):
        df = load_bars(
            start="2024-01-02", end="2024-01-03",
            asset_type="continuous", data_root=continuous_root,
        )
        assert len(df) == 2
        # dominant_contract is mapped to ticker
        assert df["ticker"].iloc[0] == "IF2406.CFFEX"
        assert "adjfactor" in df.columns

    def test_invalid_asset_type_raises(self, asset_root):
        with pytest.raises(ValueError, match="no table"):
            load_bars(asset_type="bond", data_root=asset_root)

    def test_invalid_freq_raises(self, asset_root):
        with pytest.raises(ValueError, match="unknown freq"):
            load_bars(freq="weekly", data_root=asset_root)


# ---------------------------------------------------------------------------
# 5min / 60min aggregation
# ---------------------------------------------------------------------------
class TestAggregation:
    def test_5min_aggregation(self, minute_root):
        """4 minute bars at 09:31–09:34 → one 5min bucket at 09:30."""
        df = load_bars(
            start="2024-01-02", end="2024-01-02",
            freq="5min", data_root=minute_root,
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
            start="2024-01-02", end="2024-01-02",
            freq="60min", data_root=minute_root,
        )
        assert len(df) == 1
        # Same 4 bars all fall in the 09:00 bucket
        assert df.iloc[0]["volume"] == 406

    def test_minute_returns_datetime_as_date(self, minute_root):
        df = load_bars(
            start="2024-01-02", end="2024-01-02",
            freq="minute", data_root=minute_root,
        )
        assert len(df) == 4
        # date column carries the bar timestamp
        first = df.iloc[0]["date"]
        assert first == datetime(2024, 1, 2, 9, 31) or first == pd.Timestamp("2024-01-02 09:31:00")


# ---------------------------------------------------------------------------
# alphalens adapters (DoD: to_alphalens_prices shape = date × ticker)
# ---------------------------------------------------------------------------
class TestAlphalensAdapters:
    def test_to_alphalens_prices_shape(self, asset_root):
        result = to_alphalens_prices(
            start="2024-01-02", end="2024-01-04",
            symbols=["600519.SH", "000001.SZ"],
            data_root=asset_root,
        )
        # Shape is date × ticker
        assert result.index.name == "date" or result.index.name is None
        assert set(result.columns) == {"600519.SH", "000001.SZ"}
        assert len(result) == 3  # 3 dates

    def test_to_alphalens_prices_is_forward_adjusted(self, asset_root):
        result = to_alphalens_prices(
            start="2024-01-02", end="2024-01-04",
            symbols=["600519.SH"],
            data_root=asset_root,
        )
        # day1 qfq = 5.0 (see TestAdjustment)
        val = result.iloc[0, 0]
        assert abs(val - 5.0) < 1e-6

    def test_to_alphalens_factor_multiindex(self):
        factor_df = pd.DataFrame({
            "date": [date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 3)],
            "ticker": ["600519.SH", "000001.SZ", "600519.SH"],
            "momentum": [0.05, 0.03, 0.07],
        })
        result = to_alphalens_factor(factor_df)
        assert isinstance(result, pd.Series)
        assert result.index.names == ["date", "ticker"]
        assert len(result) == 3

    def test_alphalens_constants(self):
        assert ALPHALENS_PERIODS == (1, 5, 10, 20)
        assert ALPHALENS_QUANTILES == 5


# ---------------------------------------------------------------------------
# load_bars_chunked
# ---------------------------------------------------------------------------
class TestLoadBarsChunked:
    def test_monthly_chunks(self, asset_root):
        chunks = list(load_bars_chunked(
            start="2024-01-02", end="2024-01-04",
            data_root=asset_root,
        ))
        assert len(chunks) == 1  # all in one month
        assert len(chunks[0]) == 6

    def test_multi_month_chunks(self, tmp_path):
        root = tmp_path / "asset"
        (root / "bars_daily").mkdir(parents=True)
        # Write one day per month for 2 months
        for d in [date(2024, 1, 15), date(2024, 2, 15)]:
            _write_daily(root, d, [
                _bars_daily_row("600519.SH", d, adjfactor=1.0),
            ])
        chunks = list(load_bars_chunked(
            start="2024-01-01", end="2024-02-28",
            data_root=root,
        ))
        assert len(chunks) == 2
        assert all(len(c) == 1 for c in chunks)

    def test_empty_months_skipped(self, tmp_path):
        root = tmp_path / "asset"
        (root / "bars_daily").mkdir(parents=True)
        # Only write in January
        _write_daily(root, date(2024, 1, 15), [
            _bars_daily_row("600519.SH", date(2024, 1, 15)),
        ])
        chunks = list(load_bars_chunked(
            start="2024-01-01", end="2024-03-31",
            data_root=root,
        ))
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
            start="2024-01-02", end="2024-01-21",
            data_root=root,
        )
        elapsed = time.perf_counter() - t0

        assert len(df) == 5 * 20  # 5 ETFs × 20 days
        assert elapsed < 1.0, f"load_bars took {elapsed:.3f}s (budget: 1s)"
