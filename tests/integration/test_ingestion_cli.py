"""数据接入编排器与 CLI 脚本（fetch_daily / backfill / cross_check / amazing_pull）集成测试。"""

from __future__ import annotations

import ast
import json
import sys
import types
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pyarrow.parquet as pq
import pytest

from data.dqc.report import build_reference_overrides
from data.dqc.severity import DQCBatchReject
from data.ingestion import (
    cross_check_path,
    cross_source_check,
    ingest_table,
    raw_exists,
    respond,
    run_cross_check_for_date,
    run_ingestion,
)
from data.raw import raw_path, save_raw
from data.source_adapter import FetchResult, read_watermark
from data.storage_ops import partition_path
from scripts.ingestion import resolve_tables, resolve_trading_dates
from scripts.ingestion import amazing_pull
from scripts.ingestion import backfill as backfill_cli
from scripts.ingestion import cross_check as cross_check_cli
from scripts.ingestion import fetch_daily as fetch_daily_cli


TRADE_DATE = date(2025, 3, 3)


def _seed_reference_data(data_root: Path, dates: list[date]) -> None:
    """写入规范路径下的日历与前日行情，确保 DQC C01/C02/L02/L07 通过。"""
    cal_dates = sorted(set(dates + [date(2025, 2, 28)]))
    cal_df = pd.DataFrame({"trade_date": cal_dates, "is_open": [True] * len(cal_dates)})
    cal_path = data_root / "asset" / "cn_calendar" / "SSE.parquet"
    cal_path.parent.mkdir(parents=True, exist_ok=True)
    cal_df.to_parquet(cal_path, index=False)


def _tushare_raw_stock(day: date, n: int = 10, *, close: float = 10.0) -> pd.DataFrame:
    ymd = day.strftime("%Y%m%d")
    return pd.DataFrame(
        {
            "ts_code": [f"{i:06d}.SZ" for i in range(1, n + 1)],
            "trade_date": [ymd] * n,
            "open": [close] * n,
            "high": [close + 0.3] * n,
            "low": [close - 0.3] * n,
            "close": [close] * n,
            "vol": [10_000.0] * n,  # 手（×100 = 1_000_000 股）
            "amount": [10_000.0 * close] * n,  # 千元（×1000 = 10_000_000 * close 元）
            "adj_factor": [1.05] * n,
            "suspend_d": [" "] * n,
        }
    )


def _qdc_raw_stock(day: date, n: int = 10, *, close: float = 10.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": [f"{i:06d}.SZ" for i in range(1, n + 1)],
            "trade_date": [day.strftime("%Y-%m-%d")] * n,
            "open": [close] * n,
            "high": [close + 0.3] * n,
            "low": [close - 0.3] * n,
            "close": [close] * n,
            "volume": [1_000_000] * n,
            "amount": [10_000_000.0 * close] * n,
            "adjfactor": [1.05] * n,
            "is_suspended": [False] * n,
        }
    )


def _straightflush_raw_stock(
    day: date, n: int = 10, *, close: float = 10.0
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "thscode": [f"{i:06d}.SZ" for i in range(1, n + 1)],
            "trade_date": [day.strftime("%Y-%m-%d")] * n,
            "open_price": [close] * n,
            "high_price": [close + 0.3] * n,
            "low_price": [close - 0.3] * n,
            "close_price": [close] * n,
            "volume": [1_000_000] * n,
            "turnover": [10_000_000.0 * close] * n,
            "adjfactor": [1.05] * n,
            "suspend": [False] * n,
        }
    )


def _dummy_adapter(source: str, df_or_exc):
    adapter = MagicMock()
    adapter.list_datasets.return_value = ["stock", "calendar"]

    def _fetch(dataset: str, trade_date: date, watermark=None):
        if isinstance(df_or_exc, Exception):
            raise df_or_exc
        df = df_or_exc(trade_date) if callable(df_or_exc) else df_or_exc
        return FetchResult(
            df=df,
            source=source,
            dataset=dataset,
            trade_date=trade_date,
            row_count=len(df),
            watermark=trade_date.strftime("%Y-%m-%d"),
        )

    adapter.fetch.side_effect = _fetch
    return adapter


class TestIngestOrchestrator:
    def test_end_to_end_primary_writes_raw_asset_metadata_watermark_and_crosscheck(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _seed_reference_data(tmp_path, [TRADE_DATE])
        # 预先落地全部备源 raw，验证自动触发交叉校验并写入 cross_check JSON
        qdc_df = _qdc_raw_stock(
            TRADE_DATE, close=10.5
        )  # price diff 0.5 > 0.01 -> ERROR
        save_raw(qdc_df, "qdc", "cn_stock_bars_daily", TRADE_DATE, tmp_path)
        save_raw(
            _qdc_raw_stock(TRADE_DATE, close=10.0).rename(
                columns={"trade_date": "kline_time", "is_suspended": "suspend"}
            ),
            "amazingdata",
            "cn_stock_bars_daily",
            TRADE_DATE,
            tmp_path,
        )

        adapters = {
            "straightflush": _dummy_adapter("straightflush", _straightflush_raw_stock),
            "qdc": _dummy_adapter("qdc", qdc_df),
        }
        monkeypatch.setattr("data.adapters.get_adapter", lambda src: adapters[src])

        outcome = ingest_table("cn_stock_bars_daily", TRADE_DATE, data_root=tmp_path)
        assert outcome.status == "success"
        assert outcome.source == "straightflush"
        assert outcome.row_count == 10
        assert outcome.fallback_from is None

        # raw 在 DQC 之前先落盘
        assert raw_exists(
            "straightflush", "cn_stock_bars_daily", TRADE_DATE, data_root=tmp_path
        )
        # asset 写入且 Parquet 元数据包含主源血缘
        target = partition_path("cn_stock_bars_daily", TRADE_DATE, tmp_path)
        assert target.exists()
        meta = pq.read_schema(target).metadata
        assert meta[b"source_tag"] == b"straightflush"
        assert b"ingest_time" in meta

        # 水位线已写入
        assert read_watermark(
            "straightflush", "cn_stock_bars_daily", tmp_path
        ) == TRADE_DATE.strftime("%Y-%m-%d")

        # 自动交叉校验报告已落盘且发现 qdc 差异
        assert outcome.cross_check_levels is not None
        assert outcome.cross_check_levels["qdc"] == "ERROR"
        assert outcome.cross_check_levels["amazingdata"] == "silent"
        cc_file = cross_check_path("cn_stock_bars_daily", TRADE_DATE, tmp_path)
        assert cc_file.exists()
        cc_payload = json.loads(cc_file.read_text(encoding="utf-8"))
        assert cc_payload["comparisons"]["qdc"]["level"] == "ERROR"

        # 幂等跳过与 --force 覆盖
        skipped = ingest_table("cn_stock_bars_daily", TRADE_DATE, data_root=tmp_path)
        assert skipped.status == "skipped"
        forced = ingest_table(
            "cn_stock_bars_daily", TRADE_DATE, force=True, data_root=tmp_path
        )
        assert forced.status == "success"

    def test_primary_dqc_reject_falls_back_to_backup_with_backfill_tag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _seed_reference_data(tmp_path, [TRADE_DATE])
        # 主源 straightflush 全部 close <= 0 触发 DQC HARD 升级拒收
        bad_sf = _straightflush_raw_stock(TRADE_DATE, close=-1.0)
        good_qdc = _qdc_raw_stock(TRADE_DATE, close=10.0)
        adapters = {
            "straightflush": _dummy_adapter("straightflush", bad_sf),
            "qdc": _dummy_adapter("qdc", good_qdc),
        }
        monkeypatch.setattr("data.adapters.get_adapter", lambda src: adapters[src])

        outcome = ingest_table("cn_stock_bars_daily", TRADE_DATE, data_root=tmp_path)
        assert outcome.status == "success"
        assert outcome.source == "qdc"
        assert outcome.fallback_from == "straightflush"

        # 主源被 DQC 拒收时其 raw 证据仍应保留
        assert raw_path(
            "straightflush", "cn_stock_bars_daily", TRADE_DATE, tmp_path
        ).exists()
        target = partition_path("cn_stock_bars_daily", TRADE_DATE, tmp_path)
        meta = pq.read_schema(target).metadata
        assert meta[b"source_tag"] == b"qdc:backfill"

    def test_from_raw_and_field_override(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _seed_reference_data(tmp_path, [TRADE_DATE])
        sf_raw = _straightflush_raw_stock(TRADE_DATE, close=10.0)
        sf_raw["adjfactor"] = 1.0  # 将被 override 替换为 qdc 的 1.88
        qdc_raw = _qdc_raw_stock(TRADE_DATE, close=10.0)
        qdc_raw["adjfactor"] = 1.88
        save_raw(sf_raw, "straightflush", "cn_stock_bars_daily", TRADE_DATE, tmp_path)
        save_raw(qdc_raw, "qdc", "cn_stock_bars_daily", TRADE_DATE, tmp_path)

        monkeypatch.setattr(
            "data.ingestion.get_field_overrides",
            lambda _table: {"adjfactor": "qdc"},
        )
        # 确保未调用外部适配器
        monkeypatch.setattr(
            "data.adapters.get_adapter",
            lambda _src: (_ for _ in ()).throw(
                AssertionError("should not fetch online")
            ),
        )

        outcome = ingest_table(
            "cn_stock_bars_daily",
            TRADE_DATE,
            from_raw=True,
            cross_check=False,
            data_root=tmp_path,
        )
        assert outcome.status == "success"
        landed = pd.read_parquet(outcome.asset_path)
        assert landed["adjfactor"].iloc[0] == pytest.approx(1.88)

    def test_non_dqc_table_validates_schema_and_writes_source_tag(self, tmp_path: Path):
        cal_raw_1 = pd.DataFrame({"date": ["20250228"], "date_ms": [1740672000000]})
        save_raw(cal_raw_1, "straightflush", "cn_calendar", date(2025, 2, 28), tmp_path)
        out_1 = ingest_table(
            "cn_calendar",
            date(2025, 2, 28),
            from_raw=True,
            cross_check=False,
            data_root=tmp_path,
        )
        assert out_1.status == "success"
        assert out_1.asset_path == tmp_path / "asset" / "cn_calendar" / "SSE.parquet"
        assert (
            pq.read_schema(out_1.asset_path).metadata[b"source_tag"] == b"straightflush"
        )

        # 再次写入新日期时应增量合并保留历史交易日
        cal_raw_2 = pd.DataFrame({"date": ["20250303"], "date_ms": [1740931200000]})
        save_raw(cal_raw_2, "straightflush", "cn_calendar", TRADE_DATE, tmp_path)
        outcome = ingest_table(
            "cn_calendar",
            TRADE_DATE,
            from_raw=True,
            force=True,
            cross_check=False,
            data_root=tmp_path,
        )
        assert outcome.status == "success"
        cal_df = pd.read_parquet(outcome.asset_path)
        assert list(cal_df["trade_date"]) == [date(2025, 2, 28), TRADE_DATE]

    def test_index_table_fills_missing_nullable_columns(self, tmp_path: Path):
        idx_raw = pd.DataFrame(
            [
                {
                    "ts": 1740988800000,  # 2025-03-03 Asia/Shanghai
                    "symbol": "000300.XSHG",
                    "open": 3900.0,
                    "high": 3920.0,
                    "low": 3880.0,
                    "close": 3910.0,
                    "volume": 1000000,
                    "amount": 5000000000.0,
                }
            ]
        )
        save_raw(
            idx_raw,
            "qdc",
            "cn_stock_index_bars_daily",
            TRADE_DATE,
            tmp_path,
        )
        outcome = ingest_table(
            "cn_stock_index_bars_daily",
            TRADE_DATE,
            source="qdc",
            from_raw=True,
            cross_check=False,
            data_root=tmp_path,
        )
        assert outcome.status == "success"
        landed = pd.read_parquet(outcome.asset_path)
        assert set(["pe", "pb", "total_market_cap", "float_market_cap"]) <= set(
            landed.columns
        )

    def test_dqc_reject_propagates_when_explicit_source_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _seed_reference_data(tmp_path, [TRADE_DATE])
        save_raw(
            _tushare_raw_stock(TRADE_DATE, close=-1.0),
            "tushare",
            "cn_stock_bars_daily",
            TRADE_DATE,
            tmp_path,
        )
        with pytest.raises(DQCBatchReject):
            ingest_table(
                "cn_stock_bars_daily",
                TRADE_DATE,
                source="tushare",
                from_raw=True,
                data_root=tmp_path,
            )


class TestCLIScriptsAndHelpers:
    def test_resolve_tables_and_trading_dates(self, tmp_path: Path):
        tables = resolve_tables("bars_daily,cn_stock_bars_daily", group="core")
        assert tables[0] == "cn_calendar"
        assert "cn_stock_bars_daily" in tables
        assert len(tables) == len(set(tables))

        with pytest.raises(ValueError, match="未知"):
            resolve_tables("nonexistent_table_xyz")

        # 真实日历排除休市日（2025-03-04 设为休市）
        cal_df = pd.DataFrame(
            {
                "trade_date": [date(2025, 3, 3), date(2025, 3, 4), date(2025, 3, 5)],
                "is_open": [True, False, True],
            }
        )
        cal_path = tmp_path / "asset" / "cn_calendar" / "SSE.parquet"
        cal_path.parent.mkdir(parents=True, exist_ok=True)
        cal_df.to_parquet(cal_path, index=False)
        days = resolve_trading_dates(
            date(2025, 3, 3), date(2025, 3, 5), data_root=tmp_path
        )
        assert days == [date(2025, 3, 3), date(2025, 3, 5)]

    def test_fetch_daily_and_backfill_cli(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _seed_reference_data(tmp_path, [date(2025, 3, 3), date(2025, 3, 4)])
        for d in (date(2025, 3, 3), date(2025, 3, 4)):
            save_raw(
                _straightflush_raw_stock(d),
                "straightflush",
                "cn_stock_bars_daily",
                d,
                tmp_path,
            )
            save_raw(_qdc_raw_stock(d), "qdc", "cn_stock_bars_daily", d, tmp_path)

        rc_daily = fetch_daily_cli.main(
            [
                "--date",
                "2025-03-03",
                "--tables",
                "cn_stock_bars_daily",
                "--from-raw",
                "--no-cross-check",
                "--data-root",
                str(tmp_path),
            ]
        )
        assert rc_daily == 0
        assert partition_path(
            "cn_stock_bars_daily", date(2025, 3, 3), tmp_path
        ).exists()

        rc_backfill = backfill_cli.main(
            [
                "--start",
                "20250303",
                "--end",
                "20250304",
                "--tables",
                "cn_stock_bars_daily",
                "--from-raw",
                "--no-cross-check",
                "--data-root",
                str(tmp_path),
            ]
        )
        assert rc_backfill == 0
        assert partition_path(
            "cn_stock_bars_daily", date(2025, 3, 4), tmp_path
        ).exists()

        # 非法参数返回 2，缺失 raw 返回 1
        assert (
            fetch_daily_cli.main(["--date", "not-a-date", "--data-root", str(tmp_path)])
            == 2
        )
        assert (
            fetch_daily_cli.main(
                [
                    "--date",
                    "2025-03-05",
                    "--source",
                    "straightflush",
                    "--from-raw",
                    "--data-root",
                    str(tmp_path),
                ]
            )
            == 1
        )
        # 直接测试 run_ingestion(fail_fast=False) 汇总失败条目
        batch = run_ingestion(
            ["cn_stock_bars_daily"],
            [date(2025, 3, 5)],
            source="straightflush",
            from_raw=True,
            data_root=tmp_path,
        )
        assert len(batch) == 1
        assert batch[0].status == "failed"

    def test_cross_check_cli_returns_1_on_error(self, tmp_path: Path):
        save_raw(
            _straightflush_raw_stock(TRADE_DATE, close=10.0),
            "straightflush",
            "cn_stock_bars_daily",
            TRADE_DATE,
            tmp_path,
        )
        save_raw(
            _qdc_raw_stock(TRADE_DATE, close=10.0),
            "qdc",
            "cn_stock_bars_daily",
            TRADE_DATE,
            tmp_path,
        )
        rc_ok = cross_check_cli.main(
            [
                "--start",
                "2025-03-03",
                "--tables",
                "cn_stock_bars_daily",
                "--data-root",
                str(tmp_path),
            ]
        )
        assert rc_ok == 0

        # 备源价格偏差超限触发 ERROR -> 退出码 1
        save_raw(
            _qdc_raw_stock(TRADE_DATE, close=11.0),
            "qdc",
            "cn_stock_bars_daily",
            TRADE_DATE,
            tmp_path,
        )
        rc_err = cross_check_cli.main(
            ["--start", "2025-03-03", "--data-root", str(tmp_path)]
        )
        assert rc_err == 1
        levels = run_cross_check_for_date(
            "cn_stock_bars_daily", TRADE_DATE, data_root=tmp_path
        )
        assert levels["qdc"] == "ERROR"

    def test_cross_check_handles_nullable_int64_zero_volume(self):
        df_a = pd.DataFrame(
            {
                "ticker": ["510300.SH"],
                "trade_date": [TRADE_DATE],
                "close": [4.0],
                "volume": pd.Series([0], dtype="Int64"),
                "amount": [0.0],
            }
        )
        df_b = df_a.copy()
        res = cross_source_check(
            df_a,
            df_b,
            "cn_etf_bars_daily",
            TRADE_DATE,
            secondary_source="straightflush",
        )
        assert res.matched_rows == 1
        assert res.mismatch_rate == 0.0
        assert respond(res) == "silent"

    def test_cross_check_skips_unavailable_sources_when_fetching_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        save_raw(
            _straightflush_raw_stock(TRADE_DATE, close=10.0),
            "straightflush",
            "cn_stock_bars_daily",
            TRADE_DATE,
            tmp_path,
        )
        save_raw(
            _qdc_raw_stock(TRADE_DATE, close=10.0),
            "qdc",
            "cn_stock_bars_daily",
            TRADE_DATE,
            tmp_path,
        )

        def _fake_adapter(source: str):
            if source == "amazingdata":
                raise ModuleNotFoundError("No module named 'AmazingData'")
            raise AssertionError(f"unexpected online fetch for {source}")

        monkeypatch.setattr("data.adapters.get_adapter", _fake_adapter)
        levels = run_cross_check_for_date(
            "cn_stock_bars_daily",
            TRADE_DATE,
            data_root=tmp_path,
            fetch_missing_raw=True,
        )
        assert levels == {"qdc": "silent"}

        # 幂等跳过应兼容 YYYYMMDD 水位线
        from data.source_adapter import write_watermark

        write_watermark("straightflush", "cn_calendar", "20250303", tmp_path)
        (tmp_path / "asset" / "cn_calendar").mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"trade_date": [TRADE_DATE]}).to_parquet(
            tmp_path / "asset" / "cn_calendar" / "SSE.parquet", index=False
        )
        skip_out = ingest_table("cn_calendar", TRADE_DATE, data_root=tmp_path)
        assert skip_out.status == "skipped"


class TestAmazingPullAndAdapters:
    def test_amazing_pull_has_zero_internal_dependencies(self):
        tree = ast.parse(Path(amazing_pull.__file__).read_text(encoding="utf-8"))
        forbidden_roots = {"data", "core", "pandera", "duckdb", "ops", "research"}
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        assert not (imported & forbidden_roots)

    def test_amazing_pull_mock_sdk_writes_raw_and_always_logs_out(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("USER", "demo_user")
        monkeypatch.setenv("PASSWORD", "demo_secret")
        monkeypatch.setenv("HOST", "127.0.0.1")
        monkeypatch.setenv("PORT", "8000")

        logout_calls: list[str | None] = []

        class FakeBaseData:
            def get_calendar(self):
                return ["20250303", "20250304"]

            def get_hist_code_list(self, **_kwargs):
                return ["000001.SZ"]

            def get_option_code_list(self, **_kwargs):
                return [f"100000{i:02d}.SH" for i in range(15)]

        class FakeMarketData:
            def __init__(self, _cal):
                pass

            def query_kline(self, code_list, **_kwargs):
                return {
                    code_list[0]: pd.DataFrame(
                        {
                            "kline_time": ["2025-03-03", "2025-03-04"],
                            "open": [10.0, 10.1],
                            "high": [10.3, 10.4],
                            "low": [9.8, 9.9],
                            "close": [10.2, 10.3],
                            "volume": [100000, 120000],
                            "amount": [1020000.0, 1236000.0],
                        }
                    )
                }

        class FakeInfoData:
            def __init__(self):
                self.batches: list[int] = []

            def get_option_basic_info(self, batch, **_kwargs):
                self.batches.append(len(batch))
                return pd.DataFrame({"MARKET_CODE": list(batch)})

        fake_info = FakeInfoData()
        fake_ad = types.ModuleType("AmazingData")
        fake_ad.constant = SimpleNamespace(
            Period=SimpleNamespace(day=SimpleNamespace(value="day"))
        )
        fake_ad.login = MagicMock()
        fake_ad.logout = MagicMock(
            side_effect=lambda username=None: logout_calls.append(username)
        )
        fake_ad.BaseData = FakeBaseData
        fake_ad.MarketData = FakeMarketData
        fake_ad.InfoData = lambda: fake_info

        monkeypatch.setitem(sys.modules, "AmazingData", fake_ad)

        rc = amazing_pull.main(
            [
                "--tables",
                "cn_calendar,cn_stock_bars_daily,option_basic",
                "--start",
                "2025-03-03",
                "--end",
                "2025-03-04",
                "--data-root",
                str(tmp_path),
                "--local-path",
                str(tmp_path / "cache"),
            ]
        )
        assert rc == 0
        assert logout_calls == ["demo_user"]
        assert (
            tmp_path / "raw" / "amazingdata" / "cn_calendar" / "2025-03-04.parquet"
        ).exists()
        assert (
            tmp_path
            / "raw"
            / "amazingdata"
            / "cn_stock_bars_daily"
            / "2025-03-03.parquet"
        ).exists()
        assert (
            tmp_path
            / "raw"
            / "amazingdata"
            / "cn_stock_bars_daily"
            / "2025-03-04.parquet"
        ).exists()
        assert (
            tmp_path / "raw" / "amazingdata" / "option_basic" / "2025-03-04.parquet"
        ).exists()
        # 15 个期权代码必须分为 10 + 5 两批查询
        assert fake_info.batches == [10, 5]
        wm = json.loads(
            (tmp_path / "raw" / "amazingdata" / ".watermarks.json").read_text(
                encoding="utf-8"
            )
        )
        assert wm["cn_stock_bars_daily"] == "2025-03-04"

    def test_amazing_pull_calendar_integer_dates_and_default_cache_under_raw(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("USER", "demo_user")
        monkeypatch.setenv("PASSWORD", "demo_secret")
        monkeypatch.setenv("HOST", "127.0.0.1")
        monkeypatch.setenv("PORT", "8000")

        class IntCalendarBaseData:
            def get_calendar(self):
                return [19901219, 20261112, 20261113, "2026-11-16", 20261117.0]

        class DummyMarketData:
            def __init__(self, _cal):
                pass

        class DummyInfoData:
            pass

        fake_ad = types.ModuleType("AmazingData")
        fake_ad.constant = SimpleNamespace(
            Period=SimpleNamespace(day=SimpleNamespace(value="day"))
        )
        fake_ad.login = MagicMock()
        fake_ad.logout = MagicMock()
        fake_ad.BaseData = IntCalendarBaseData
        fake_ad.MarketData = DummyMarketData
        fake_ad.InfoData = DummyInfoData
        monkeypatch.setitem(sys.modules, "AmazingData", fake_ad)

        rc = amazing_pull.main(
            [
                "--tables",
                "cn_calendar",
                "--date",
                "2026-11-13",
                "--data-root",
                str(tmp_path),
            ]
        )
        assert rc == 0
        cal_out = (
            tmp_path / "raw" / "amazingdata" / "cn_calendar" / "2026-11-13.parquet"
        )
        assert cal_out.exists()
        df = pd.read_parquet(cal_out)
        assert list(pd.to_datetime(df["trade_date"]).dt.date) == [date(2026, 11, 13)]
        assert (tmp_path / "raw" / "amazingdata" / "_cache").is_dir()
        assert not (tmp_path / "amazingdata_cache").exists()

        # _pull_calendar 直接兼容整型 / 浮点 / ISO 字符串
        win = amazing_pull._pull_calendar(
            IntCalendarBaseData(), date(2026, 11, 12), date(2026, 11, 17)
        )
        assert list(win["trade_date"]) == [
            date(2026, 11, 12),
            date(2026, 11, 13),
            date(2026, 11, 16),
            date(2026, 11, 17),
        ]

    def test_amazingdata_normalizer_supports_etf_and_int_calendar(self):
        from data.normalizers.amazingdata_normalizer import normalize

        etf_raw = pd.DataFrame(
            {
                "code": ["510300.SH"],
                "kline_time": ["2026-11-13"],
                "open": [4.0],
                "high": [4.1],
                "low": [3.9],
                "close": [4.05],
                "volume": [1000],
                "amount": [4050.0],
            }
        )
        etf_norm = normalize("etf", etf_raw)
        assert len(etf_norm) == 1
        assert etf_norm["ticker"].iloc[0] == "510300.SH"

        cal_norm = normalize(
            "calendar", pd.DataFrame({"cal_date": [20261113, "20261116"]})
        )
        assert list(cal_norm["trade_date"]) == [date(2026, 11, 13), date(2026, 11, 16)]

    def test_tushare_enrichment_and_canonical_reference_overrides(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from data.adapters.tushare_adapter import TushareAdapter

        # 测试 TushareAdapter 自动合并 adj_factor 与 suspend_d
        fake_pro = MagicMock()
        fake_pro.daily.return_value = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "trade_date": ["20250303"], "close": [10.0]}
        )
        fake_pro.adj_factor.return_value = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "trade_date": ["20250303"], "adj_factor": [1.25]}
        )
        fake_pro.suspend_d.return_value = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "suspend_type": ["S"]}
        )
        adapter = TushareAdapter(secrets_path=tmp_path / "missing.env")
        fake_env = tmp_path / "tushare.env"
        fake_env.write_text("TUSHARE_TOKEN=fake_secret_from_file\n", encoding="utf-8")
        adapter_env = TushareAdapter(secrets_path=fake_env)
        assert adapter_env._resolve_token() == "fake_secret_from_file"

        monkeypatch.setattr("tushare.pro_api", lambda _tok: fake_pro)
        res = adapter.fetch("stock", TRADE_DATE)
        assert res.df["adj_factor"].iloc[0] == pytest.approx(1.25)
        assert res.df["suspend_d"].iloc[0] == "S"

        # 测试 build_reference_overrides 优先识别 51 表规范路径
        _seed_reference_data(tmp_path, [TRADE_DATE])
        ind_path = tmp_path / "asset" / "cn_stock_industry" / "industry.parquet"
        ind_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"ticker": ["000001.SZ"], "industry": ["银行"]}).to_parquet(
            ind_path, index=False
        )
        ov = build_reference_overrides(tmp_path, "cn_stock_bars_daily", TRADE_DATE)
        assert TRADE_DATE.isoformat() in ov["calendar_dates"]
        assert "000001.SZ" in ov["universe"]

    def test_scheduler_fetch_stock_daily_targets_new_cli(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        import ops.scheduler as scheduler

        captured: list[list[str]] = []

        def fake_run(cmd, **_kwargs):
            captured.append(list(cmd))
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(scheduler.subprocess, "run", fake_run)
        summary = scheduler.fetch_stock_daily()
        assert summary == "fetch_stock_daily exited 0"
        assert len(captured) == 1
        assert captured[0][0] == sys.executable
        assert captured[0][1].endswith("scripts/ingestion/fetch_daily.py")
        assert captured[0][2:] == ["--tables", "cn_stock_bars_daily"]

    def test_etf_sparse_null_ohlc_drops_without_fallback_and_high_null_falls_back(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        date_ms = 1794528000000  # 2026-11-13 Asia/Shanghai
        n = 10
        sf_sparse = pd.DataFrame(
            {
                "thscode": [f"5100{i:02d}.SH" for i in range(n)],
                "date_ms": [date_ms] * n,
                "open_price": [float("nan")] + [2.5] * (n - 1),
                "high_price": [float("nan")] + [2.6] * (n - 1),
                "low_price": [float("nan")] + [2.4] * (n - 1),
                "close_price": [2.44] + [2.55] * (n - 1),
                "volume": [0] + [100000] * (n - 1),
                "turnover": [0] + [255000] * (n - 1),  # 整型成交额同时验证转 float64
            }
        )
        qdc_adj = pd.DataFrame(
            {
                "symbol": [f"5100{i:02d}.XSHG" for i in range(n)],
                "ts": [date_ms] * n,
                "open": [2.44] + [2.5] * (n - 1),
                "high": [2.44] + [2.6] * (n - 1),
                "low": [2.44] + [2.4] * (n - 1),
                "close": [2.44] + [2.55] * (n - 1),
                "volume": [0] + [100000] * (n - 1),
                "amount": [0.0] + [255000.0] * (n - 1),
                "adjfactor": [1.0] * n,
            }
        )
        save_raw(
            sf_sparse,
            "straightflush",
            "cn_etf_bars_daily",
            date(2026, 11, 13),
            tmp_path,
        )
        save_raw(qdc_adj, "qdc", "cn_etf_bars_daily", date(2026, 11, 13), tmp_path)

        outcome = ingest_table(
            "cn_etf_bars_daily",
            date(2026, 11, 13),
            from_raw=True,
            cross_check=False,
            data_root=tmp_path,
        )
        assert outcome.status == "success"
        assert outcome.source == "straightflush"
        assert outcome.fallback_from is None
        assert outcome.row_count == 9
        meta = pq.read_schema(outcome.asset_path).metadata
        assert meta[b"source_tag"] == b"straightflush"

        # 超过 20%（3/10 = 30%）时应拒收并回退至备源 qdc:backfill
        sf_corrupt = sf_sparse.copy()
        sf_corrupt.loc[:2, "open_price"] = float("nan")
        save_raw(
            sf_corrupt,
            "straightflush",
            "cn_etf_bars_daily",
            date(2026, 11, 13),
            tmp_path,
        )
        fb = ingest_table(
            "cn_etf_bars_daily",
            date(2026, 11, 13),
            from_raw=True,
            force=True,
            cross_check=False,
            data_root=tmp_path,
        )
        assert fb.status == "success"
        assert fb.source == "qdc"
        assert fb.fallback_from == "straightflush"
        assert pq.read_schema(fb.asset_path).metadata[b"source_tag"] == b"qdc:backfill"

    def test_amazing_pull_supports_index_table(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("USER", "demo_user")
        monkeypatch.setenv("PASSWORD", "demo_secret")
        monkeypatch.setenv("HOST", "127.0.0.1")
        monkeypatch.setenv("PORT", "8000")

        seen_sec_types: list[str] = []

        class IndexBaseData:
            def get_calendar(self):
                return [20261113]

            def get_code_list(self, security_type=None):
                seen_sec_types.append(str(security_type))
                return ["000300.SH"]

        class IndexMarketData:
            def __init__(self, _cal):
                pass

            def query_kline(self, code_list, **_kwargs):
                return {
                    code_list[0]: pd.DataFrame(
                        {
                            "kline_time": ["2026-11-13"],
                            "open": [4000.0],
                            "high": [4020.0],
                            "low": [3990.0],
                            "close": [4016.29],
                            "volume": [179341998],
                            "amount": [460245657530.9],
                        }
                    )
                }

        fake_ad = types.ModuleType("AmazingData")
        fake_ad.constant = SimpleNamespace(
            Period=SimpleNamespace(day=SimpleNamespace(value="day"))
        )
        fake_ad.login = MagicMock()
        fake_ad.logout = MagicMock()
        fake_ad.BaseData = IndexBaseData
        fake_ad.MarketData = IndexMarketData
        fake_ad.InfoData = lambda: object()
        monkeypatch.setitem(sys.modules, "AmazingData", fake_ad)

        rc = amazing_pull.main(
            [
                "--tables",
                "cn_stock_index_bars_daily",
                "--date",
                "2026-11-13",
                "--data-root",
                str(tmp_path),
            ]
        )
        assert rc == 0
        assert seen_sec_types == ["EXTRA_INDEX_A"]
        assert (
            tmp_path
            / "raw"
            / "amazingdata"
            / "cn_stock_index_bars_daily"
            / "2026-11-13.parquet"
        ).exists()
