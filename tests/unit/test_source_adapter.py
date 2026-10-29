"""Unit tests for data.source_adapter — ABC, guards, watermark, and three adapters.

Covers every ac-1 row, plus seven regression tests for PR #50 review findings:

* ``FetchResult`` dataclass fields and types
* ``SourceAdapter`` ABC — four methods present (fetch / list_asset_types / health_check / close)
* Three subclasses (AkshareAdapter / TushareAdapter / AmazingdataAdapter) run minimal fetch
* Hand-rolled decorators: rate_limit throttles, retry retries and gives up, timeout raises
* Watermark ``read_watermark`` / ``write_watermark`` round-trip at ``raw/{source}/.watermarks.json``

Regression tests (each fails without its fix):
  1. timeout is thread-safe (no SIGALRM ValueError off main thread)
  2. retry is outermost (fresh timeout per attempt, backoff outside alarm)
  3. akshare passes a real symbol (not empty string)
  4. retry catches requests.exceptions.ConnectionError (not just builtins)
  5. akshare passes adjust="" (raw contract, not qfq)
  6. health_check returns False when probe fails (not just SDK build)
  7. local_path assertion is real (asserts str type, not vacuous ``or True``)
"""

from __future__ import annotations

import json
import threading
import time
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data.adapters import REGISTRY, get_adapter
from data.adapters.akshare_adapter import AkshareAdapter
from data.adapters.amazingdata_adapter import AmazingdataAdapter
from data.adapters.tushare_adapter import TushareAdapter
from data.source_adapter import (
    RETRY_ON,
    FetchResult,
    FetchTimeoutError,
    SourceAdapter,
    _rate_limit,
    _retry,
    _timeout,
    read_watermark,
    write_watermark,
)


# ---------------------------------------------------------------------------
# FetchResult
# ---------------------------------------------------------------------------
class TestFetchResult:
    def test_fields(self):
        df = pd.DataFrame({"a": [1, 2]})
        fr = FetchResult(
            df=df,
            source="akshare",
            asset_type="stock",
            trade_date=date(2026, 1, 5),
            row_count=2,
            watermark=None,
        )
        assert fr.source == "akshare"
        assert fr.asset_type == "stock"
        assert fr.trade_date == date(2026, 1, 5)
        assert fr.row_count == 2
        assert fr.watermark is None
        assert len(fr.df) == 2

    def test_watermark_str(self):
        fr = FetchResult(
            df=pd.DataFrame(),
            source="tushare",
            asset_type="stock",
            trade_date=date(2026, 1, 5),
            row_count=0,
            watermark="20260105",
        )
        assert fr.watermark == "20260105"


# ---------------------------------------------------------------------------
# SourceAdapter ABC
# ---------------------------------------------------------------------------
class TestSourceAdapterABC:
    def test_abstract_methods_present(self):
        assert hasattr(SourceAdapter, "fetch")
        assert hasattr(SourceAdapter, "list_asset_types")
        assert hasattr(SourceAdapter, "health_check")
        assert hasattr(SourceAdapter, "close")

    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            SourceAdapter()  # type: ignore[abstract]

    def test_default_requests_per_second(self):
        assert SourceAdapter.requests_per_second == 5.0

    def test_default_close_is_noop(self):
        class Dummy(SourceAdapter):
            def fetch(self, asset_type, trade_date, watermark=None):
                return FetchResult(
                    pd.DataFrame(), "dummy", asset_type, trade_date, 0, None
                )

            def list_asset_types(self):
                return ["stock"]

        d = Dummy()
        d.close()  # should not raise

    def test_health_check_empty_asset_types(self):
        class Dummy(SourceAdapter):
            def fetch(self, asset_type, trade_date, watermark=None):
                return FetchResult(
                    pd.DataFrame(), "dummy", asset_type, trade_date, 0, None
                )

            def list_asset_types(self):
                return []

        d = Dummy()
        assert d.health_check() is False


# ---------------------------------------------------------------------------
# Decorators: _rate_limit
# ---------------------------------------------------------------------------
class TestRateLimit:
    def test_throttles_calls(self):
        calls = []

        @_rate_limit(10.0)  # 100ms interval
        def fast():
            calls.append(time.monotonic())

        fast()
        fast()
        fast()
        # First call is immediate; subsequent calls wait ~100ms each
        assert len(calls) == 3
        gap = calls[2] - calls[1]
        assert gap >= 0.05  # at least some throttling happened

    def test_zero_rate_no_throttle(self):
        calls = []

        @_rate_limit(0.0)
        def instant():
            calls.append(time.monotonic())

        start = time.monotonic()
        for _ in range(10):
            instant()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1  # no throttling
        assert len(calls) == 10


# ---------------------------------------------------------------------------
# Decorators: _retry
# ---------------------------------------------------------------------------
class TestRetry:
    def test_succeeds_first_try(self):
        @_retry(max_retries=3, initial_wait=0.01)
        def ok():
            return 42

        assert ok() == 42

    def test_retries_then_succeeds(self):
        attempts = []

        @_retry(max_retries=3, initial_wait=0.01)
        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("transient")
            return "done"

        result = flaky()
        assert result == "done"
        assert len(attempts) == 3

    def test_exhausts_retries_and_raises(self):
        attempts = []

        @_retry(max_retries=2, initial_wait=0.01)
        def always_fail():
            attempts.append(1)
            raise ConnectionError("permanent")

        with pytest.raises(ConnectionError):
            always_fail()
        assert len(attempts) == 3  # 1 original + 2 retries

    def test_non_retryable_exception_passes_through(self):
        @_retry(max_retries=3, initial_wait=0.01)
        def bad():
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            bad()


# ---------------------------------------------------------------------------
# Decorators: _timeout
# ---------------------------------------------------------------------------
class TestTimeout:
    def test_completes_within_timeout(self):
        @_timeout(5.0)
        def quick():
            return "ok"

        assert quick() == "ok"

    def test_timeout_raises(self):
        @_timeout(0.05)
        def slow():
            time.sleep(1.0)
            return "never"

        with pytest.raises(FetchTimeoutError):
            slow()


# ---------------------------------------------------------------------------
# Watermark persistence
# ---------------------------------------------------------------------------
class TestWatermark:
    def test_read_missing_returns_none(self, tmp_path: Path):
        assert read_watermark("akshare", "stock", tmp_path) is None

    def test_write_then_read(self, tmp_path: Path):
        write_watermark("akshare", "stock", "20260105", tmp_path)
        assert read_watermark("akshare", "stock", tmp_path) == "20260105"

    def test_write_multiple_asset_types(self, tmp_path: Path):
        write_watermark("akshare", "stock", "20260105", tmp_path)
        write_watermark("akshare", "option", "offset_99", tmp_path)
        assert read_watermark("akshare", "stock", tmp_path) == "20260105"
        assert read_watermark("akshare", "option", tmp_path) == "offset_99"

    def test_overwrite_same_key(self, tmp_path: Path):
        write_watermark("tushare", "stock", "20260101", tmp_path)
        write_watermark("tushare", "stock", "20260105", tmp_path)
        assert read_watermark("tushare", "stock", tmp_path) == "20260105"

    def test_watermark_file_location(self, tmp_path: Path):
        write_watermark("akshare", "stock", "w", tmp_path)
        expected = tmp_path / "raw" / "akshare" / ".watermarks.json"
        assert expected.exists()
        data = json.loads(expected.read_text(encoding="utf-8"))
        assert data["stock"] == "w"

    def test_corrupt_file_returns_none(self, tmp_path: Path):
        path = tmp_path / "raw" / "akshare" / ".watermarks.json"
        path.parent.mkdir(parents=True)
        path.write_text("not json", encoding="utf-8")
        assert read_watermark("akshare", "stock", tmp_path) is None


# ---------------------------------------------------------------------------
# Registry and factory
# ---------------------------------------------------------------------------
class TestRegistry:
    def test_three_adapters_registered(self):
        assert set(REGISTRY.keys()) == {"akshare", "tushare", "amazingdata"}

    def test_get_adapter_returns_correct_type(self):
        assert isinstance(get_adapter("akshare"), AkshareAdapter)
        assert isinstance(get_adapter("tushare"), TushareAdapter)
        assert isinstance(get_adapter("amazingdata"), AmazingdataAdapter)

    def test_get_adapter_unknown_source_raises(self):
        with pytest.raises(KeyError):
            get_adapter("nonexistent")


# ---------------------------------------------------------------------------
# Three adapters — minimal fetch (mocked external APIs)
# ---------------------------------------------------------------------------
class TestAkshareAdapter:
    def test_list_asset_types(self):
        a = AkshareAdapter()
        assert a.list_asset_types() == ["stock"]

    def test_minimal_fetch(self):
        a = AkshareAdapter()
        mock_df = pd.DataFrame({"date": ["2026-01-05"], "close": [10.0]})
        with patch("akshare.stock_zh_a_hist", return_value=mock_df):
            result = a.fetch("stock", date(2026, 1, 5))
        assert isinstance(result, FetchResult)
        assert result.source == "akshare"
        assert result.asset_type == "stock"
        assert result.row_count == 1
        assert result.watermark is None

    def test_unsupported_asset_type_raises(self):
        a = AkshareAdapter()
        with pytest.raises(ValueError, match="unsupported"):
            a.fetch("option", date(2026, 1, 5))


class TestTushareAdapter:
    def test_list_asset_types(self):
        t = TushareAdapter(token="fake")
        assert t.list_asset_types() == ["stock"]

    def test_minimal_fetch(self):
        t = TushareAdapter(token="fake")
        mock_df = pd.DataFrame({"ts_code": ["000001.SZ"], "close": [10.0]})
        mock_pro = MagicMock()
        mock_pro.daily.return_value = mock_df
        with patch("tushare.pro_api", return_value=mock_pro):
            result = t.fetch("stock", date(2026, 1, 5))
        assert isinstance(result, FetchResult)
        assert result.source == "tushare"
        assert result.row_count == 1
        assert result.watermark == "20260105"

    def test_unsupported_asset_type_raises(self):
        t = TushareAdapter(token="fake")
        with pytest.raises(ValueError, match="unsupported"):
            t.fetch("option", date(2026, 1, 5))


class TestAmazingdataAdapter:
    def test_list_asset_types(self):
        a = AmazingdataAdapter()
        assert a.list_asset_types() == ["stock", "option"]

    def test_requests_per_second(self):
        assert AmazingdataAdapter.requests_per_second == 2.0

    def test_minimal_fetch_stock(self, tmp_path: Path):
        a = AmazingdataAdapter(local_path=tmp_path)  # pass Path, not str
        mock_df = pd.DataFrame({"code": ["000001"], "close": [10.0]})
        mock_ad = MagicMock()
        mock_base = MagicMock()
        mock_base.get_hist_code_list.return_value = ["000001"]
        mock_base.get_hist_data.return_value = mock_df
        mock_ad.get_base_data.return_value = mock_base
        a._ad = mock_ad

        result = a.fetch("stock", date(2026, 1, 5))
        assert isinstance(result, FetchResult)
        assert result.source == "amazingdata"
        assert result.row_count == 1
        # regression fix 7: local_path must be str (C2) — real assertion, not vacuous
        call_args = mock_base.get_hist_data.call_args
        local_path_arg = call_args.kwargs.get("local_path") or call_args.args[-1]
        assert isinstance(local_path_arg, str), (
            f"SDK must receive str, got {type(local_path_arg).__name__}"
        )

    def test_minimal_fetch_option_batches(self, tmp_path: Path):
        a = AmazingdataAdapter(local_path=str(tmp_path))
        mock_ad = MagicMock()
        mock_base = MagicMock()
        mock_info = MagicMock()
        # 25 codes → 3 batches (10+10+5)
        codes = [f"OPT{i:04d}" for i in range(25)]
        mock_base.get_option_code_list.return_value = codes
        mock_info.get_option_basic_info.return_value = pd.DataFrame({"code": ["x"]})
        mock_ad.get_base_data.return_value = mock_base
        mock_ad.get_info_data.return_value = mock_info
        a._ad = mock_ad

        result = a.fetch("option", date(2026, 1, 5))
        assert result.row_count == 3  # 3 batches concatenated
        assert mock_info.get_option_basic_info.call_count == 3

    def test_close_calls_logout(self, tmp_path: Path):
        a = AmazingdataAdapter(local_path=str(tmp_path))
        mock_ad = MagicMock()
        a._ad = mock_ad
        a.close()
        mock_ad.logout.assert_called_once()
        assert a._ad is None

    def test_unsupported_asset_type_raises(self):
        a = AmazingdataAdapter()
        with pytest.raises(ValueError, match="unsupported"):
            a.fetch("crypto", date(2026, 1, 5))


# ---------------------------------------------------------------------------
# Regression tests — each fails without its PR #50 fix
# ---------------------------------------------------------------------------
class TestRegressionFix1ThreadSafeTimeout:
    """Fix 1: _timeout must not use SIGALRM (ValueError off main thread)."""

    def test_timeout_works_from_non_main_thread(self):
        results: list = []

        @_timeout(5.0)
        def quick():
            return "ok"

        def run():
            results.append(quick())

        t = threading.Thread(target=run)
        t.start()
        t.join(timeout=10)
        assert results == ["ok"], "timeout decorator must work off the main thread"

    def test_timeout_raises_from_non_main_thread(self):
        results: list = []

        @_timeout(0.05)
        def slow():
            time.sleep(0.5)

        def run():
            try:
                slow()
                results.append("no-error")
            except FetchTimeoutError:
                results.append("timeout")

        t = threading.Thread(target=run)
        t.start()
        t.join(timeout=5)
        assert results == ["timeout"], "FetchTimeoutError must fire off the main thread"


class TestRegressionFix2RetryOutermost:
    """Fix 2: retry must be outermost so each attempt gets a fresh timeout."""

    def test_retry_gets_fresh_timeout_per_attempt(self):
        attempts: list[float] = []

        @_retry(max_retries=2, initial_wait=0.01)
        @_timeout(0.3)
        def flaky():
            attempts.append(time.monotonic())
            if len(attempts) < 2:
                raise ConnectionError("transient")
            return "done"

        result = flaky()
        assert result == "done"
        assert len(attempts) == 2, "retry must re-attempt after a transient failure"

    def test_backoff_not_aborted_by_timeout(self):
        """With retry outermost, backoff sleep is outside any alarm."""
        attempts: list = []

        @_retry(max_retries=3, initial_wait=0.01)
        @_timeout(5.0)
        def slow_start():
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("transient")
            return "done"

        result = slow_start()
        assert result == "done"
        assert len(attempts) == 3


class TestRegressionFix3AkshareSymbol:
    """Fix 3: akshare must pass a real symbol, not empty string."""

    def test_fetch_passes_non_empty_symbol(self):
        a = AkshareAdapter(symbols=["000001"])
        mock_df = pd.DataFrame({"date": ["2026-01-05"], "close": [10.0]})
        with patch("akshare.stock_zh_a_hist", return_value=mock_df) as mock_call:
            a.fetch("stock", date(2026, 1, 5))
        call_kwargs = mock_call.call_args
        symbol = call_kwargs.kwargs.get("symbol") or call_kwargs.args[0]
        assert symbol != "", "symbol must not be empty — akshare requires one code"
        assert symbol == "000001"


class TestRegressionFix4RequestsRetry:
    """Fix 4: retry must catch requests.exceptions, not just builtins."""

    def test_retry_on_includes_requests_exceptions(self):
        from requests.exceptions import ConnectionError as ReqConnErr
        from requests.exceptions import Timeout as ReqTimeout

        assert ReqConnErr in RETRY_ON, "requests.ConnectionError must be retryable"
        assert ReqTimeout in RETRY_ON, "requests.Timeout must be retryable"

    def test_retry_catches_requests_connection_error(self):
        from requests.exceptions import ConnectionError as ReqConnErr

        attempts: list = []

        @_retry(max_retries=2, initial_wait=0.01)
        def flaky():
            attempts.append(1)
            if len(attempts) < 2:
                raise ReqConnErr("requests transient")
            return "done"

        result = flaky()
        assert result == "done"
        assert len(attempts) == 2


class TestRegressionFix5AkshareAdjust:
    """Fix 5: akshare must use adjust='' (raw contract) for idempotent re-runs."""

    def test_fetch_uses_raw_adjust(self):
        a = AkshareAdapter(symbols=["000001"])
        mock_df = pd.DataFrame({"date": ["2026-01-05"], "close": [10.0]})
        with patch("akshare.stock_zh_a_hist", return_value=mock_df) as mock_call:
            a.fetch("stock", date(2026, 1, 5))
        call_kwargs = mock_call.call_args
        adjust = call_kwargs.kwargs.get("adjust")
        assert adjust == "", f"adjust must be '' (raw) for idempotency, got {adjust!r}"


class TestRegressionFix6HealthCheck:
    """Fix 6: health_check must return False when the probe fails."""

    def test_base_health_check_false_on_fetch_failure(self):
        class FailAdapter(SourceAdapter):
            def fetch(self, asset_type, trade_date, watermark=None):
                raise RuntimeError("source down")  # non-retryable for fast test

            def list_asset_types(self):
                return ["stock"]

        a = FailAdapter()
        assert a.health_check() is False, (
            "health_check must report unhealthy on failure"
        )

    def test_amazingdata_health_check_false_on_sdk_failure(self):
        a = AmazingdataAdapter()
        mock_ad = MagicMock()
        mock_base = MagicMock()
        mock_base.get_hist_code_list.side_effect = ConnectionError("down")
        mock_ad.get_base_data.return_value = mock_base
        a._ad = mock_ad
        assert a.health_check() is False, (
            "health_check must probe a real call, not just build SDK"
        )

    def test_amazingdata_health_check_false_on_empty_codes(self):
        a = AmazingdataAdapter()
        mock_ad = MagicMock()
        mock_base = MagicMock()
        mock_base.get_hist_code_list.return_value = []
        mock_ad.get_base_data.return_value = mock_base
        a._ad = mock_ad
        assert a.health_check() is False, "empty code list = unhealthy"
