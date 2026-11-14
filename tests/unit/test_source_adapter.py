"""Unit tests for data.source_adapter — ABC, guards, watermark, and five adapters.

Covers every ac-1 row, plus seven regression tests for PR #50 review findings,
plus full mock-backed unit suites for QdcAdapter and StraightFlushAdapter:

* ``FetchResult`` dataclass fields and types
* ``SourceAdapter`` ABC — four methods present (fetch / list_asset_types / health_check / close)
* Five subclasses (Akshare / Tushare / Amazingdata / Qdc / StraightFlush) run fetch
* Hand-rolled decorators: rate_limit throttles, retry retries and gives up, timeout raises
* Watermark ``read_watermark`` / ``write_watermark`` round-trip at ``raw/{source}/.watermarks.json``

Regression tests (each fails without its fix):
  1. timeout is thread-safe (no SIGALRM ValueError off main thread)
  2. ``_apply_fetch_guards`` layers retry outermost (fresh timeout per
     attempt, backoff outside the timeout budget) — tested through
     ``_apply_fetch_guards`` itself, not hand-composed decorators
  3. akshare passes a real symbol (not empty string)
  4. retry catches requests.exceptions.ConnectionError (not just builtins)
  5. akshare passes adjust="" (raw contract, not qfq)
  6. health_check probes one real call at the asset-calendar trade date
     (calendar schema: single ``trade_date`` column, storage_schema.md)
  7. local_path assertion is real (asserts str type, not vacuous ``or True``)
"""

from __future__ import annotations

import io
import json
import subprocess
import threading
import time
from datetime import date, datetime
from functools import wraps
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
import requests

from data.adapters import REGISTRY, get_adapter
from data.adapters.akshare_adapter import AkshareAdapter
from data.adapters.amazingdata_adapter import AmazingdataAdapter
from data.adapters.qdc_adapter import QdcAdapter
from data.adapters.straightflush_adapter import StraightFlushAdapter
from data.adapters.tushare_adapter import TushareAdapter
from data.source_adapter import (
    FETCH_TIMEOUT,
    RETRY_ON,
    FetchResult,
    FetchTimeoutError,
    SourceAdapter,
    _apply_fetch_guards,
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

    def test_health_check_false_on_probe_failure(self):
        """Invariant (predates the PR #50 fixes): a raising probe = unhealthy."""

        class FailAdapter(SourceAdapter):
            def fetch(self, asset_type, trade_date, watermark=None):
                raise RuntimeError("source down")  # non-retryable for fast test

            def list_asset_types(self):
                return ["stock"]

        a = FailAdapter()
        assert a.health_check() is False


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
    def test_five_adapters_registered(self):
        assert set(REGISTRY.keys()) == {
            "akshare",
            "tushare",
            "amazingdata",
            "qdc",
            "straightflush",
        }

    def test_get_adapter_returns_correct_type(self):
        assert isinstance(get_adapter("akshare"), AkshareAdapter)
        assert isinstance(get_adapter("tushare"), TushareAdapter)
        assert isinstance(get_adapter("amazingdata"), AmazingdataAdapter)
        assert isinstance(get_adapter("qdc"), QdcAdapter)
        assert isinstance(get_adapter("straightflush"), StraightFlushAdapter)

    def test_get_adapter_unknown_source_raises(self):
        with pytest.raises(KeyError):
            get_adapter("nonexistent")


# ---------------------------------------------------------------------------
# Adapters — minimal fetch & protocol behavior (mocked external APIs)
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
# QdcAdapter tests
# ---------------------------------------------------------------------------
def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


class TestQdcAdapter:
    def test_defaults_to_cli_mode(self, tmp_path: Path):
        q = QdcAdapter(secrets_path=tmp_path / "missing.env")
        assert q._mode == "cli"
        assert q.list_asset_types() == [
            "stock",
            "etf",
            "index",
            "convertible",
            "futures",
            "option",
        ]

    def test_cli_mode_auto_syncs_missing_year_and_reads_parquet(
        self, tmp_path: Path, monkeypatch
    ):
        monkeypatch.delenv("QDC_DATA_DIR", raising=False)
        monkeypatch.setattr("shutil.which", lambda name: f"/opt/homebrew/bin/{name}")

        tz = ZoneInfo("Asia/Shanghai")
        ts_day1 = int(datetime(2026, 1, 5, 0, 0, tzinfo=tz).timestamp() * 1000)
        ts_day2 = int(datetime(2026, 1, 6, 0, 0, tzinfo=tz).timestamp() * 1000)
        df = pd.DataFrame(
            {
                "symbol": ["000001.XSHE", "000001.XSHE"],
                "ts": [ts_day1, ts_day2],
                "close": [11.5, 11.8],
            }
        )

        calls: list[list[str]] = []

        def fake_run(cmd, **_kwargs):
            calls.append(cmd)
            subcmd = cmd[1]
            if subcmd == "config":
                return subprocess.CompletedProcess(
                    cmd,
                    0,
                    stdout=json.dumps(
                        {
                            "download_root": str(tmp_path / "QDCData"),
                            "format": "parquet",
                        }
                    ),
                    stderr="",
                )
            if subcmd == "get":
                sku_dir = tmp_path / "QDCData" / "cn_daily_full" / "full"
                sku_dir.mkdir(parents=True, exist_ok=True)
                df.to_parquet(sku_dir / "2026.parquet", index=False)
                return subprocess.CompletedProcess(
                    cmd, 0, stdout='{"ok": true}', stderr=""
                )
            if subcmd == "whoami":
                return subprocess.CompletedProcess(
                    cmd,
                    0,
                    stdout=json.dumps(
                        {"email": "user@example.com", "entitlement": ["cn_daily_full"]}
                    ),
                    stderr="",
                )
            raise AssertionError(f"unexpected CLI subcommand: {cmd}")

        monkeypatch.setattr("subprocess.run", fake_run)

        q = QdcAdapter(secrets_path=tmp_path / "missing.env")
        res1 = q.fetch("stock", date(2026, 1, 5))
        assert res1.source == "qdc"
        assert res1.row_count == 1
        assert res1.df["close"].iloc[0] == pytest.approx(11.5)
        assert res1.watermark == "2026-01-05"

        # Second fetch in the same year reads the synced Parquet directly without re-running get
        res2 = q.fetch("stock", date(2026, 1, 6))
        assert res2.row_count == 1
        assert res2.df["close"].iloc[0] == pytest.approx(11.8)

        subcmds = [c[1] for c in calls]
        assert subcmds.count("config") == 2
        assert subcmds.count("get") == 1
        get_cmd = next(c for c in calls if c[1] == "get")
        assert get_cmd == [
            "/opt/homebrew/bin/qdc-cli",
            "get",
            "cn_daily_full",
            "--years",
            "2026",
            "--json",
            "--quiet",
        ]
        assert q.health_check() is True

    def test_cli_download_root_without_qdcdata_suffix_appends_subdir(
        self, tmp_path: Path, monkeypatch
    ):
        monkeypatch.delenv("QDC_DATA_DIR", raising=False)
        monkeypatch.setattr("shutil.which", lambda name: f"/opt/homebrew/bin/{name}")
        sku_dir = tmp_path / "QDCData" / "cn_daily_full" / "full"
        sku_dir.mkdir(parents=True)
        tz = ZoneInfo("Asia/Shanghai")
        ts_day1 = int(datetime(2026, 1, 5, 0, 0, tzinfo=tz).timestamp() * 1000)
        pd.DataFrame(
            {"symbol": ["000001.XSHE"], "ts": [ts_day1], "close": [11.5]}
        ).to_parquet(sku_dir / "2026.parquet", index=False)

        monkeypatch.setattr(
            "subprocess.run",
            lambda cmd, **_kw: subprocess.CompletedProcess(
                cmd,
                0,
                stdout=json.dumps({"download_root": str(tmp_path)}),
                stderr="",
            ),
        )
        q = QdcAdapter(secrets_path=tmp_path / "missing.env")
        res = q.fetch("stock", date(2026, 1, 5))
        assert res.row_count == 1
        assert res.df["close"].iloc[0] == pytest.approx(11.5)

    def test_cli_missing_download_root_raises_value_error(
        self, tmp_path: Path, monkeypatch
    ):
        monkeypatch.delenv("QDC_DATA_DIR", raising=False)
        monkeypatch.setattr("shutil.which", lambda name: f"/opt/homebrew/bin/{name}")
        monkeypatch.setattr(
            "subprocess.run",
            lambda cmd, **_kw: subprocess.CompletedProcess(
                cmd, 0, stdout='{"download_root": ""}', stderr=""
            ),
        )

        q = QdcAdapter(secrets_path=tmp_path / "missing.env")
        with pytest.raises(ValueError, match="download_root"):
            q.fetch("stock", date(2026, 1, 5))
        assert q.health_check() is False

    def test_cli_missing_binary_raises_file_not_found(
        self, tmp_path: Path, monkeypatch
    ):
        monkeypatch.delenv("QDC_DATA_DIR", raising=False)
        monkeypatch.setattr("shutil.which", lambda _name: None)

        q = QdcAdapter(
            cli_bin="nonexistent-qdc-cli", secrets_path=tmp_path / "missing.env"
        )
        with pytest.raises(FileNotFoundError, match="nonexistent-qdc-cli"):
            q.fetch("stock", date(2026, 1, 5))
        assert q.health_check() is False

    def test_cli_auth_error_raises_permission_error_without_retry(
        self, tmp_path: Path, monkeypatch
    ):
        monkeypatch.delenv("QDC_DATA_DIR", raising=False)
        monkeypatch.setattr("shutil.which", lambda name: f"/opt/homebrew/bin/{name}")

        calls: list[str] = []

        def fake_run(cmd, **_kwargs):
            calls.append(cmd[1])
            if cmd[1] == "config":
                return subprocess.CompletedProcess(
                    cmd,
                    0,
                    stdout=json.dumps({"download_root": str(tmp_path)}),
                    stderr="",
                )
            return subprocess.CompletedProcess(
                cmd, 1, stdout="", stderr="403 entitlement missing"
            )

        monkeypatch.setattr("subprocess.run", fake_run)

        q = QdcAdapter(secrets_path=tmp_path / "missing.env")
        with pytest.raises(PermissionError, match="403"):
            q.fetch("stock", date(2026, 1, 5))
        assert calls.count("get") == 1

    def test_local_parquet_mode_supports_qdcdata_subdir_and_beijing_day(
        self, tmp_path: Path
    ):
        # Support passing download_root directly (containing QDCData/)
        sku_dir = tmp_path / "QDCData" / "cn_daily_full" / "full"
        sku_dir.mkdir(parents=True)
        tz = ZoneInfo("Asia/Shanghai")
        ts_day1 = int(datetime(2026, 1, 5, 0, 0, tzinfo=tz).timestamp() * 1000)
        ts_day2 = int(datetime(2026, 1, 6, 0, 0, tzinfo=tz).timestamp() * 1000)
        df = pd.DataFrame(
            {
                "symbol": ["000001.XSHE", "000001.XSHE"],
                "ts": [ts_day1, ts_day2],
                "close": [11.5, 11.8],
            }
        )
        df.to_parquet(sku_dir / "2026.parquet", index=False)

        q = QdcAdapter(local_dir=tmp_path, secrets_path=tmp_path / "missing.env")
        result = q.fetch("stock", date(2026, 1, 5))
        assert result.row_count == 1
        assert result.df["close"].iloc[0] == pytest.approx(11.5)
        assert q.health_check() is True

    def test_http_parquet_pagination_and_headers(self, tmp_path: Path):
        page1_df = pd.DataFrame(
            {"symbol": ["000001.XSHE"], "ts": [1767542400000], "close": [11.5]}
        )
        page2_df = pd.DataFrame(
            {"symbol": ["600519.XSHG"], "ts": [1767542400000], "close": [1500.0]}
        )

        resp1 = MagicMock(spec=requests.Response)
        resp1.status_code = 200
        resp1.headers = {
            "Content-Type": "application/vnd.apache.parquet",
            "X-QDC-Truncated": "true",
            "X-QDC-Next-Cursor": "cur-2",
        }
        resp1.content = _parquet_bytes(page1_df)

        resp2 = MagicMock(spec=requests.Response)
        resp2.status_code = 200
        resp2.headers = {
            "Content-Type": "application/vnd.apache.parquet",
            "X-QDC-Truncated": "false",
        }
        resp2.content = _parquet_bytes(page2_df)

        mock_session = MagicMock()
        mock_session.get.side_effect = [resp1, resp2]

        q = QdcAdapter(
            api_key="test-key",
            device="dev-01",
            secrets_path=tmp_path / "missing.env",
        )
        assert q._mode == "http"
        q._session = mock_session

        result = q.fetch("stock", date(2026, 1, 5))
        assert result.source == "qdc"
        assert result.asset_type == "stock"
        assert result.row_count == 2
        assert result.watermark == "2026-01-05"
        assert list(result.df["symbol"]) == ["000001.XSHE", "600519.XSHG"]

        assert mock_session.get.call_count == 2
        first_call = mock_session.get.call_args_list[0]
        assert first_call.kwargs["headers"] == {
            "X-API-Key": "test-key",
            "X-Device": "dev-01",
        }
        assert first_call.kwargs["timeout"] == FETCH_TIMEOUT
        assert first_call.kwargs["params"]["sku"] == "cn_daily_full"
        assert first_call.kwargs["params"]["format"] == "parquet"
        second_call = mock_session.get.call_args_list[1]
        assert second_call.kwargs["params"]["cursor"] == "cur-2"
        assert second_call.kwargs["params"]["sku"] == "cn_daily_full"

    def test_http_futures_passes_table_and_symbols(self, tmp_path: Path):
        df = pd.DataFrame({"symbol": ["IF2603"], "close": [3900.0]})
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.headers = {"Content-Type": "application/vnd.apache.parquet"}
        resp.content = _parquet_bytes(df)

        mock_session = MagicMock()
        mock_session.get.return_value = resp

        q = QdcAdapter(
            api_key="k",
            symbols=["IF2603"],
            secrets_path=tmp_path / "missing.env",
        )
        q._session = mock_session

        result = q.fetch("futures", date(2026, 1, 5))
        assert result.row_count == 1
        params = mock_session.get.call_args.kwargs["params"]
        assert params["sku"] == "cn_futures_1d"
        assert params["table"] == "bars"
        assert params["code"] == "IF2603"

    def test_http_422_raises_value_error_without_retry(self, tmp_path: Path):
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 422
        resp.text = "date range exceeds 14 days"
        mock_session = MagicMock()
        mock_session.get.return_value = resp

        q = QdcAdapter(api_key="k", secrets_path=tmp_path / "missing.env")
        q._session = mock_session
        with pytest.raises(ValueError, match="422"):
            q.fetch("stock", date(2026, 1, 5))
        assert mock_session.get.call_count == 1

    def test_http_403_raises_permission_error(self, tmp_path: Path):
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 403
        resp.text = "not purchased"
        mock_session = MagicMock()
        mock_session.get.return_value = resp

        q = QdcAdapter(api_key="k", secrets_path=tmp_path / "missing.env")
        q._session = mock_session
        with pytest.raises(PermissionError, match="403"):
            q.fetch("stock", date(2026, 1, 5))

    def test_http_429_triggers_retry_and_recovers(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr("time.sleep", lambda _s: None)
        bad_resp = MagicMock(spec=requests.Response)
        bad_resp.status_code = 429
        bad_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("429")

        ok_resp = MagicMock(spec=requests.Response)
        ok_resp.status_code = 200
        ok_resp.headers = {"Content-Type": "application/vnd.apache.parquet"}
        ok_resp.content = _parquet_bytes(pd.DataFrame({"symbol": ["000001.XSHE"]}))

        mock_session = MagicMock()
        mock_session.get.side_effect = [bad_resp, ok_resp]

        q = QdcAdapter(api_key="k", secrets_path=tmp_path / "missing.env")
        q._session = mock_session
        res = q.fetch("stock", date(2026, 1, 5))
        assert res.row_count == 1
        assert mock_session.get.call_count == 2

    def test_http_missing_api_key_raises(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("QDC_API_KEY", raising=False)
        monkeypatch.delenv("QDC_DATA_DIR", raising=False)
        q = QdcAdapter(mode="http", secrets_path=tmp_path / "missing.env")
        with pytest.raises(ValueError, match="QDC_API_KEY"):
            q.fetch("stock", date(2026, 1, 5))

    def test_http_health_check_and_close(self, tmp_path: Path):
        resp_ok = MagicMock(spec=requests.Response)
        resp_ok.status_code = 200
        resp_ok.json.return_value = {"datasets": [{"sku": "cn_daily_full"}]}
        mock_session = MagicMock()
        mock_session.get.return_value = resp_ok

        q = QdcAdapter(mode="http", api_key="k", secrets_path=tmp_path / "missing.env")
        q._session = mock_session
        assert q.health_check() is True
        assert mock_session.get.call_args.kwargs["timeout"] == FETCH_TIMEOUT

        q.close()
        mock_session.close.assert_called_once()
        assert q._session is None

    def test_unsupported_asset_type_raises(self, tmp_path: Path):
        q = QdcAdapter(secrets_path=tmp_path / "missing.env")
        with pytest.raises(ValueError, match="unsupported"):
            q.fetch("crypto", date(2026, 1, 5))


# ---------------------------------------------------------------------------
# StraightFlushAdapter (Fuyao) tests
# ---------------------------------------------------------------------------
class TestStraightFlushAdapter:
    def test_list_asset_types(self, tmp_path: Path):
        s = StraightFlushAdapter(token="fake", secrets_path=tmp_path / "missing.env")
        assert s.list_asset_types() == [
            "stock",
            "etf",
            "index",
            "futures",
            "option",
            "calendar",
        ]

    def test_stock_batch_uses_adjust_none_and_chunks_by_10(self, tmp_path: Path):
        # 25 codes -> 3 batch calls (10 + 10 + 5)
        codes = [f"60{i:04d}.SH" for i in range(25)]

        def make_resp(params_codes: list[str]) -> MagicMock:
            resp = MagicMock(spec=requests.Response)
            resp.status_code = 200
            resp.json.return_value = {
                "code": 0,
                "message": "success",
                "request_id": "req-1",
                "data": {
                    "interval": "1d",
                    "adjust": "none",
                    "item": [
                        {
                            "thscode": code,
                            "timestamp": 1767542400000,
                            "item": [
                                {
                                    "date_ms": 1767542400000,
                                    "open_price": 10.0,
                                    "high_price": 10.5,
                                    "low_price": 9.8,
                                    "close_price": 10.2,
                                    "volume": 100000,
                                    "turnover": 1020000.0,
                                }
                            ],
                        }
                        for code in params_codes
                    ],
                },
            }
            return resp

        mock_session = MagicMock()
        mock_session.get.side_effect = [
            make_resp(codes[:10]),
            make_resp(codes[10:20]),
            make_resp(codes[20:25]),
        ]

        s = StraightFlushAdapter(
            token="test-fuyao-key",
            symbols=codes,
            secrets_path=tmp_path / "missing.env",
        )
        s._session = mock_session

        result = s.fetch("stock", date(2026, 1, 5))
        assert result.source == "straightflush"
        assert result.asset_type == "stock"
        assert result.row_count == 25
        assert result.watermark == "20260105"
        assert "thscode" in result.df.columns
        assert mock_session.get.call_count == 3

        for call in mock_session.get.call_args_list:
            assert call.kwargs["headers"] == {"X-api-key": "test-fuyao-key"}
            assert call.kwargs["timeout"] == FETCH_TIMEOUT
            params = call.kwargs["params"]
            assert params["interval"] == "1d"
            assert params["adjust"] == "none"
            assert len(params["thscodes"].split(",")) <= 10

    def test_auto_ticker_list_pagination_when_symbols_omitted(self, tmp_path: Path):
        list_resp = MagicMock(spec=requests.Response)
        list_resp.status_code = 200
        list_resp.json.return_value = {
            "code": 0,
            "data": {"item": [{"thscode": "600519.SH"}, {"thscode": "000001.SZ"}]},
        }

        bars_resp = MagicMock(spec=requests.Response)
        bars_resp.status_code = 200
        bars_resp.json.return_value = {
            "code": 0,
            "data": {
                "item": [
                    {
                        "thscode": "600519.SH",
                        "item": [{"date_ms": 1767542400000, "close_price": 1500.0}],
                    }
                ]
            },
        }

        mock_session = MagicMock()
        mock_session.get.side_effect = [list_resp, bars_resp]

        s = StraightFlushAdapter(
            token="test-key", secrets_path=tmp_path / "missing.env"
        )
        s._session = mock_session

        result = s.fetch("stock", date(2026, 1, 5))
        assert result.row_count == 1
        assert "/api/meta/tickers/list" in mock_session.get.call_args_list[0].args[0]
        assert (
            mock_session.get.call_args_list[0].kwargs["params"]["asset_type"]
            == "a-share"
        )
        assert (
            "/api/a-share/prices/historical-batch"
            in mock_session.get.call_args_list[1].args[0]
        )

    def test_etf_futures_and_calendar_endpoints(self, tmp_path: Path):
        mock_session = MagicMock()
        etf_resp = MagicMock(spec=requests.Response)
        etf_resp.status_code = 200
        etf_resp.json.return_value = {
            "code": 0,
            "data": {
                "item": [
                    {
                        "thscode": "510300.SH",
                        "item": [{"date_ms": 1767542400000, "close_price": 4.1}],
                    }
                ]
            },
        }
        fut_resp = MagicMock(spec=requests.Response)
        fut_resp.status_code = 200
        fut_resp.json.return_value = {
            "code": 0,
            "data": {
                "item": [
                    {
                        "date_ms": 1767542400000,
                        "close_price": 3900.0,
                        "open_interest": 10000,
                    }
                ]
            },
        }
        cal_resp = MagicMock(spec=requests.Response)
        cal_resp.status_code = 200
        cal_resp.json.return_value = {
            "code": 0,
            "data": {"item": [{"date_ms": 1767542400000, "date": "20260105"}]},
        }
        mock_session.get.side_effect = [etf_resp, fut_resp, cal_resp]

        s = StraightFlushAdapter(
            token="k",
            symbols=["510300.SH"],
            secrets_path=tmp_path / "missing.env",
        )
        s._session = mock_session

        r_etf = s.fetch("etf", date(2026, 1, 5))
        assert r_etf.row_count == 1
        assert (
            "/api/fund/market/historical-batch"
            in mock_session.get.call_args_list[0].args[0]
        )

        s._symbols = ["CU2603.SHF"]
        r_fut = s.fetch("futures", date(2026, 1, 5))
        assert r_fut.row_count == 1
        assert r_fut.df["thscode"].iloc[0] == "CU2603.SHF"
        assert (
            mock_session.get.call_args_list[1].kwargs["params"]["time_period"]
            == "day_1"
        )

        r_cal = s.fetch("calendar", date(2026, 1, 5))
        assert r_cal.row_count == 1
        assert r_cal.df["date"].iloc[0] == "20260105"

    def test_business_error_codes_and_retry(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr("time.sleep", lambda _s: None)
        s = StraightFlushAdapter(
            token="k",
            symbols=["600519.SH"],
            secrets_path=tmp_path / "missing.env",
        )

        # 4001 rate limit retries and succeeds
        rate_resp = MagicMock(spec=requests.Response)
        rate_resp.status_code = 200
        rate_resp.json.return_value = {
            "code": 4001,
            "message": "rate limited",
            "request_id": "r1",
        }
        ok_resp = MagicMock(spec=requests.Response)
        ok_resp.status_code = 200
        ok_resp.json.return_value = {
            "code": 0,
            "data": {"item": [{"date_ms": 1767542400000, "date": "20260105"}]},
        }
        mock_session = MagicMock()
        mock_session.get.side_effect = [rate_resp, ok_resp]
        s._session = mock_session
        res = s.fetch("calendar", date(2026, 1, 5))
        assert res.row_count == 1
        assert mock_session.get.call_count == 2

        # 2001 auth error -> PermissionError without retry
        auth_resp = MagicMock(spec=requests.Response)
        auth_resp.status_code = 200
        auth_resp.json.return_value = {
            "code": 2001,
            "message": "invalid api key",
            "request_id": "r2",
        }
        mock_session.get.side_effect = [auth_resp]
        with pytest.raises(PermissionError, match="2001"):
            s.fetch("calendar", date(2026, 1, 5))

        # 3002 data not ready -> empty DataFrame
        empty_resp = MagicMock(spec=requests.Response)
        empty_resp.status_code = 200
        empty_resp.json.return_value = {
            "code": 3002,
            "message": "not ready",
            "request_id": "r3",
        }
        mock_session.get.side_effect = [empty_resp]
        res_empty = s.fetch("calendar", date(2026, 1, 5))
        assert res_empty.row_count == 0

    def test_reads_stright_flush_token_from_secrets_file(
        self, tmp_path: Path, monkeypatch
    ):
        for key in (
            "STRIGHT_FLUSH_TOKEN",
            "STRAIGHT_FLUSH_TOKEN",
            "HITHINK_FINANCE_API_KEY",
        ):
            monkeypatch.delenv(key, raising=False)
        env_file = tmp_path / ".env"
        env_file.write_text(
            "STRIGHT_FLUSH_TOKEN=placeholder-only-token\n", encoding="utf-8"
        )
        s = StraightFlushAdapter(secrets_path=env_file)
        assert s._headers() == {"X-api-key": "placeholder-only-token"}

    def test_health_check_and_close(self, tmp_path: Path):
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.return_value = {
            "code": 0,
            "data": {"item": [{"thscode": "000001.SZ"}]},
        }
        mock_session = MagicMock()
        mock_session.get.return_value = resp

        s = StraightFlushAdapter(token="k", secrets_path=tmp_path / "missing.env")
        s._session = mock_session
        assert s.health_check() is True

        s.close()
        mock_session.close.assert_called_once()
        assert s._session is None

    def test_unsupported_asset_type_raises(self, tmp_path: Path):
        s = StraightFlushAdapter(token="k", secrets_path=tmp_path / "missing.env")
        with pytest.raises(ValueError, match="unsupported"):
            s.fetch("crypto", date(2026, 1, 5))


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
    """Fix 2: ``_apply_fetch_guards`` must layer retry outermost.

    Every test calls ``_apply_fetch_guards`` itself.  The pre-fix ordering
    ``rate_limit(timeout(retry(fn)))`` fails them: one timeout budget spans
    all attempts plus backoff, so a backoff wait eats the budget and a
    timeout aborts the retry loop.
    """

    def test_composes_retry_outermost(self, monkeypatch):
        order: list[str] = []

        def recorder(name: str):
            def factory(*_args, **_kwargs):
                def decorator(func):
                    @wraps(func)
                    def wrapper(*args, **kwargs):
                        order.append(name)
                        return func(*args, **kwargs)

                    return wrapper

                return decorator

            return factory

        monkeypatch.setattr("data.source_adapter._retry", recorder("retry"))
        monkeypatch.setattr("data.source_adapter._timeout", recorder("timeout"))
        monkeypatch.setattr("data.source_adapter._rate_limit", recorder("rate_limit"))

        def fetch_fn():
            order.append("fetch")
            return "ok"

        guarded = _apply_fetch_guards(fetch_fn, requests_per_second=100.0)
        assert guarded() == "ok"
        assert order == ["retry", "timeout", "rate_limit", "fetch"], (
            "wrapping order must be retry -> timeout -> rate_limit -> fetch "
            "(retry outermost, per source_adapter.md)"
        )

    def test_backoff_waits_outside_timeout_budget(self, monkeypatch):
        """The 1s retry backoff must sleep outside the 0.5s per-attempt budget."""
        monkeypatch.setattr("data.source_adapter.FETCH_TIMEOUT", 0.5)
        attempts: list[int] = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 2:
                raise ConnectionError("transient")
            return "done"

        guarded = _apply_fetch_guards(flaky, requests_per_second=1000.0)
        assert guarded() == "done", (
            "backoff (1s) inside a 0.5s timeout budget = pre-fix ordering"
        )
        assert len(attempts) == 2

    def test_each_attempt_gets_a_fresh_timeout(self, monkeypatch):
        """Two 0.25s attempts + 1s backoff pass per-attempt, fail on one budget."""
        monkeypatch.setattr("data.source_adapter.FETCH_TIMEOUT", 0.6)
        attempts: list[int] = []

        def slow_once():
            attempts.append(1)
            if len(attempts) < 2:
                time.sleep(0.25)
                raise ConnectionError("transient")
            return "done"

        guarded = _apply_fetch_guards(slow_once, requests_per_second=1000.0)
        assert guarded() == "done", (
            "each attempt must get its own timeout window (retry outermost)"
        )
        assert len(attempts) == 2


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


class TestRegressionSdkTimeout:
    """Review r2 optional 2: SDK call must pass timeout (default None pins thread)."""

    def test_fetch_passes_timeout_to_akshare(self):
        a = AkshareAdapter(symbols=["000001"])
        mock_df = pd.DataFrame({"date": ["2026-01-05"], "close": [10.0]})
        with patch("akshare.stock_zh_a_hist", return_value=mock_df) as mock_call:
            a.fetch("stock", date(2026, 1, 5))
        call_kwargs = mock_call.call_args
        timeout = call_kwargs.kwargs.get("timeout")
        assert timeout is not None, (
            "SDK timeout must be set (None = hangs on half-open)"
        )
        assert timeout == FETCH_TIMEOUT, (
            f"SDK timeout must be FETCH_TIMEOUT ({FETCH_TIMEOUT}), got {timeout!r}"
        )


class TestRegressionFix6HealthCheck:
    """Fix 6: health_check probes one real call at the asset-calendar trade date.

    The calendar is ``DATA_ROOT/asset/calendar/sse.parquet`` — single
    column ``trade_date`` (storage_schema.md).  Every test here fails
    without its fix: the pre-fix base probed ``date.today()``, and the
    pre-fix amazingdata check only built the SDK.
    """

    @staticmethod
    def _write_calendar(root: Path) -> date:
        """Write the real calendar layout (one ``trade_date`` DATE column)."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        cal_dir = root / "asset" / "calendar"
        cal_dir.mkdir(parents=True)
        max_date = date(2026, 10, 20)
        table = pa.table(
            {
                "trade_date": pa.array(
                    [date(2026, 10, 16), date(2026, 10, 20)], type=pa.date32()
                )
            }
        )
        pq.write_table(table, cal_dir / "sse.parquet")
        return max_date

    @staticmethod
    def _probe_adapter(probed: list):
        class Probe(SourceAdapter):
            def fetch(self, asset_type, trade_date, watermark=None):
                probed.append(trade_date)
                return FetchResult(
                    pd.DataFrame(), "probe", asset_type, trade_date, 0, None
                )

            def list_asset_types(self):
                return ["stock"]

        return Probe()

    def test_latest_trade_date_reads_asset_calendar(self, tmp_path, monkeypatch):
        monkeypatch.setattr("data.source_adapter.DATA_ROOT", tmp_path)
        max_date = self._write_calendar(tmp_path)
        assert self._probe_adapter([])._latest_trade_date() == max_date

    def test_latest_trade_date_falls_back_to_wall_clock(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # no calendar file here
        assert self._probe_adapter([])._latest_trade_date() == date.today()

    def test_base_health_check_probes_calendar_trade_date(self, tmp_path, monkeypatch):
        monkeypatch.setattr("data.source_adapter.DATA_ROOT", tmp_path)
        max_date = self._write_calendar(tmp_path)
        probed: list = []
        assert self._probe_adapter(probed).health_check() is True
        assert probed == [max_date], (
            "health_check must probe the calendar's max trade_date, "
            f"not date.today(); probed {probed!r}"
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

    def test_latest_trade_date_clamps_future_rows(self, tmp_path, monkeypatch):
        """Calendar has ~30 future trading days; MAX alone would return a
        date that has not happened yet.  Must clamp to the last date <= today."""
        from datetime import timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        monkeypatch.setattr("data.source_adapter.DATA_ROOT", tmp_path)
        cal_dir = tmp_path / "asset" / "calendar"
        cal_dir.mkdir(parents=True)

        today = date.today()
        past = today - timedelta(days=1)
        future = today + timedelta(days=30)  # trade_cal look-ahead
        table = pa.table(
            {"trade_date": pa.array([past, today, future], type=pa.date32())}
        )
        pq.write_table(table, cal_dir / "sse.parquet")

        result = self._probe_adapter([])._latest_trade_date()
        assert result == today, (
            f"must clamp to latest date <= today ({today}), not MAX ({future}); "
            f"got {result}"
        )
        assert result <= today, "must never return a future date"

    def test_latest_trade_date_all_future_falls_back_to_wall_clock(
        self, tmp_path, monkeypatch
    ):
        """If all calendar rows are future (e.g. fresh calendar, no history yet),
        the clamp yields nothing → fall back to today."""
        from datetime import timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        monkeypatch.setattr("data.source_adapter.DATA_ROOT", tmp_path)
        cal_dir = tmp_path / "asset" / "calendar"
        cal_dir.mkdir(parents=True)
        future = date.today() + timedelta(days=30)
        table = pa.table({"trade_date": pa.array([future], type=pa.date32())})
        pq.write_table(table, cal_dir / "sse.parquet")

        result = self._probe_adapter([])._latest_trade_date()
        assert result == date.today(), (
            f"all-future calendar must fall back to wall clock, got {result}"
        )
