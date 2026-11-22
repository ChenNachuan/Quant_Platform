"""01-stock-core-and-status 剩余 5 张表的单元测试。

覆盖：
1. ``cn_stock_basic``（Tushare / StraightFlush / AmazingData 标准化 + ``old_code`` override）
2. ``cn_stock_corporate_actions``（多事件拆分 + 三键联合交叉校验与去重合并）
3. ``cn_stock_fundamental``（QDC / Tushare 单位换算与 schema 校验）
4. ``cn_stock_daily_status``（QDC / Tushare / AmazingData 标准化与限价/ST/XR 标志）
5. ``cn_stock_bars_minute``（C03 支持 [240, 241] 且保持标量兼容；``ingest_table`` 透传日线 ``cross_df``）
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from data.dqc.checks.custom_checks import check_minute_bar_count
from data.ingestion import (
    _apply_field_overrides,
    cross_source_check,
    ingest_table,
)
from data.normalizers import normalize
from data.raw import save_raw
from data.storage_schema import SCHEMA


TRADE_DAY = date(2026, 11, 13)


class TestStockBasicNormalizersAndOverride:
    def test_tushare_straightflush_amazingdata_normalize_and_validate(self):
        ts_raw = pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "920000.BJ", "T600018.SH", "900901.SH"],
                "name": ["平安银行", "安徽凤凰", "上港集团(退市)", "仪电B股"],
                "exchange": ["SZSE", "BSE", "SSE", "SSE"],
                "market": ["主板", None, "主板", "B股"],
                "list_status": ["L", "L", "D", "L"],
                "list_date": ["19910403", "20201223", "20000719", "19920327"],
                "delist_date": [None, None, "20061020", None],
                "is_hs": ["S", "N", None, None],
            }
        )
        ts_norm = normalize("tushare", ts_raw, dataset="stock_basic")
        assert list(ts_norm["ticker"]) == ["000001.SZ", "920000.BJ"]
        assert (
            ts_norm.loc[ts_norm["ticker"] == "920000.BJ", "market"].iloc[0] == "北交所"
        )
        assert ts_norm["old_code"].isna().all()
        SCHEMA["cn_stock_basic"].validate(ts_norm)

        sf_raw = pd.DataFrame(
            {
                "thscode": ["000001.SZ", "920000.BJ"],
                "name": ["平安银行", "安徽凤凰"],
                "exchange": ["SZ", "BJ"],
                "list_date": ["1991-04-03", "2020-12-23"],
                "end_date": [None, None],
            }
        )
        sf_norm = normalize("straightflush", sf_raw, dataset="stock_basic")
        assert list(sf_norm["ticker"]) == ["000001.SZ", "920000.BJ"]
        assert list(sf_norm["list_status"]) == ["L", "L"]
        SCHEMA["cn_stock_basic"].validate(sf_norm)

        ad_raw = pd.DataFrame(
            {
                "MARKET_CODE": ["000001.SZ", "920000.BJ", "600001.SH"],
                "SECURITY_NAME": ["平安银行", "安徽凤凰", "邯郸钢铁(退市)"],
                "LISTPLATE_NAME": ["主板", "北证", "主板"],
                "LISTDATE": ["19910403", "20201223", "19980122"],
                "DELISTDATE": [None, None, "20091229"],
                "IS_LISTED": [1, 1, 0],
                "OLD_CODE": [None, "832000", None],
            }
        )
        ad_norm = normalize("amazingdata", ad_raw, dataset="stock_basic")
        assert list(ad_norm["ticker"]) == ["000001.SZ", "600001.SH", "920000.BJ"]
        bj_row = ad_norm[ad_norm["ticker"] == "920000.BJ"].iloc[0]
        assert bj_row["old_code"] == "832000.BJ"
        assert bj_row["market"] == "北交所"
        assert ad_norm[ad_norm["ticker"] == "600001.SH"].iloc[0]["list_status"] == "D"
        SCHEMA["cn_stock_basic"].validate(ad_norm)

    def test_stock_basic_old_code_override_without_trade_date(self, tmp_path: Path):
        ts_norm = normalize(
            "tushare",
            pd.DataFrame(
                {
                    "ts_code": ["000001.SZ", "920000.BJ"],
                    "name": ["平安银行", "安徽凤凰"],
                    "exchange": ["SZSE", "BSE"],
                    "market": ["主板", None],
                    "list_status": ["L", "L"],
                    "list_date": ["19910403", "20201223"],
                    "delist_date": [None, None],
                    "is_hs": ["S", "N"],
                }
            ),
            dataset="stock_basic",
        )
        ad_raw = pd.DataFrame(
            {
                "MARKET_CODE": ["920000.BJ"],
                "SECURITY_NAME": ["安徽凤凰"],
                "LISTPLATE_NAME": ["北证"],
                "LISTDATE": ["20201223"],
                "DELISTDATE": [None],
                "IS_LISTED": [1],
                "OLD_CODE": ["832000"],
            }
        )
        save_raw(ad_raw, "amazingdata", "cn_stock_basic", TRADE_DAY, tmp_path)
        merged = _apply_field_overrides(
            ts_norm,
            "tushare",
            "cn_stock_basic",
            TRADE_DAY,
            data_root=tmp_path,
            from_raw=True,
        )
        bj_row = merged[merged["ticker"] == "920000.BJ"].iloc[0]
        assert bj_row["old_code"] == "832000.BJ"
        sz_row = merged[merged["ticker"] == "000001.SZ"].iloc[0]
        assert pd.isna(sz_row["old_code"]) or sz_row["old_code"] is None


class TestCorporateActionsSplitAndCrossCheck:
    def test_all_four_normalizers_split_and_aggregate(self):
        sf_raw = pd.DataFrame(
            {
                "thscode": ["000001.SZ", "600000.SH", "000002.SZ"],
                "ex_date_ms": [
                    int(datetime(2024, 6, 14, 0, 0).timestamp() * 1000),
                    int(datetime(2024, 6, 18, 0, 0).timestamp() * 1000),
                    int(datetime(2024, 6, 19, 0, 0).timestamp() * 1000),
                ],
                "dividend_per_share": [0.5, 0.0, 0.2],
                "per_share_bonus": [0.3, -0.1, 0.0],
                "allotment_ratio": [0.0, 0.2, 0.0],
                "allotment_price": [0.0, 8.5, 0.0],
            }
        )
        sf_norm = normalize("straightflush", sf_raw, dataset="corporate_actions")
        assert list(sf_norm["event_type"]) == [
            "bonus",
            "dividend",
            "dividend",
            "rights",
        ]
        SCHEMA["cn_stock_corporate_actions"].validate(sf_norm)

        qdc_raw = pd.DataFrame(
            {
                "symbol": ["000001.XSHE", "920000.XSHE"],
                "progress": ["实施分配", "实施分配"],
                "ex_date": ["2024-06-14", "2024-07-01"],
                "disclosure_date": ["2024-06-07", "2024-06-25"],
                "record_date": ["2024-06-13", "2024-06-30"],
                "cash": [5.0, 2.0],
                "bonus_share": [2.0, 0.0],
                "convert_share": [1.0, 0.0],
            }
        )
        qdc_norm = normalize("qdc", qdc_raw, dataset="corporate_actions")
        assert "920000.BJ" in set(qdc_norm["ticker"])
        p_div = qdc_norm[
            (qdc_norm["ticker"] == "000001.SZ") & (qdc_norm["event_type"] == "dividend")
        ].iloc[0]
        p_bon = qdc_norm[
            (qdc_norm["ticker"] == "000001.SZ") & (qdc_norm["event_type"] == "bonus")
        ].iloc[0]
        assert p_div["cash_dividend"] == pytest.approx(0.5)
        assert p_bon["stock_dividend"] == pytest.approx(0.3)
        SCHEMA["cn_stock_corporate_actions"].validate(qdc_norm)

        ts_raw = pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "div_proc": ["实施", "实施"],
                "ex_date": ["20240614", "20240614"],
                "ann_date": ["20240607", "20240607"],
                "record_date": ["20240613", "20240613"],
                "cash_div_tax": [0.2, 0.3],
                "stk_bo_rate": [0.1, 0.0],
                "stk_co_rate": [0.2, 0.0],
            }
        )
        ts_norm = normalize("tushare", ts_raw, dataset="corporate_actions")
        assert len(ts_norm) == 2
        assert ts_norm.loc[ts_norm["event_type"] == "dividend", "cash_dividend"].iloc[
            0
        ] == pytest.approx(0.5)
        SCHEMA["cn_stock_corporate_actions"].validate(ts_norm)

        ad_raw = pd.DataFrame(
            {
                "MARKET_CODE": ["000001.SZ", "600000.SH"],
                "_action_kind": ["dividend", "rights"],
                "EX_DATE": ["20240614", "20240618"],
                "ANN_DATE": ["20240607", "20240601"],
                "RECORD_DATE": ["20240613", "20240617"],
                "CASH_DIVIDEND": [0.5, float("nan")],
                "BONUS_RATE": [0.2, float("nan")],
                "CONVERSED_RATE": [0.1, float("nan")],
                "STOCK_DIVIDEND": [0.3, float("nan")],
                "RIGHTS_PRICE": [float("nan"), 8.5],
                "RIGHTS_RATIO": [float("nan"), 0.2],
                "REMARK": ["", "每10股配2股"],
            }
        )
        ad_norm = normalize("amazingdata", ad_raw, dataset="corporate_actions")
        assert list(ad_norm["event_type"]) == ["bonus", "dividend", "rights"]
        SCHEMA["cn_stock_corporate_actions"].validate(ad_norm)

        # 三键 [ticker, ex_date, event_type] 交叉校验完全一致
        sf_cmp = sf_norm[
            sf_norm["ticker"].isin(["000001.SZ", "600000.SH"])
        ].reset_index(drop=True)
        res = cross_source_check(
            sf_cmp,
            ad_norm,
            "cn_stock_corporate_actions",
            TRADE_DAY,
            secondary_source="amazingdata",
        )
        assert res.matched_rows == 3
        assert res.mismatched_fields == {}


class TestFundamentalAndDailyStatus:
    def test_fundamental_qdc_and_tushare_units_and_schema(self):
        qdc_raw = pd.DataFrame(
            {
                "symbol": ["000001.XSHE", "600519.XSHG"],
                "trade_date": ["2026-11-13", "2026-11-13"],
                "market_cap": [2.0e11, 1.8e12],
                "circulating_market_cap": [1.9e11, 1.7e12],
                "pe_ttm": [6.5, 22.0],
                "pe_latest_yr": [6.2, 21.5],
                "pb_excl_other_equity_mrq": [0.55, 7.2],
                "ps_ttm": [1.4, 9.8],
                "total_shares": [19_405_918_198, 1_256_197_800],
                "circulating_shares": [19_405_500_000, 1_256_197_800],
            }
        )
        qdc_norm = normalize("qdc", qdc_raw, dataset="fundamental")
        assert qdc_norm["pe_lyr"].tolist() == pytest.approx([6.2, 21.5])
        assert qdc_norm["pb"].tolist() == pytest.approx([0.55, 7.2])
        SCHEMA["cn_stock_fundamental"].validate(qdc_norm)

        ts_raw = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20261113"],
                "total_mv": [20_000_000.0],  # 万元 -> *10000
                "circ_mv": [19_000_000.0],
                "pe_ttm": [6.5],
                "pe": [6.2],
                "pb": [0.55],
                "ps_ttm": [1.4],
                "total_share": [1_940_591.8198],  # 万股 -> *10000
                "float_share": [1_940_550.0],
            }
        )
        ts_norm = normalize("tushare", ts_raw, dataset="fundamental")
        assert ts_norm["total_market_cap"].iloc[0] == pytest.approx(2.0e11)
        assert ts_norm["total_shares"].iloc[0] == 19_405_918_198
        SCHEMA["cn_stock_fundamental"].validate(ts_norm)

    def test_daily_status_qdc_and_amazingdata_validate(self):
        qdc_raw = pd.DataFrame(
            {
                "symbol": ["000001.XSHE", "920000.XSHE"],
                "date": [TRADE_DAY, TRADE_DAY],
                "pre_close": [11.79, 0.0],
                "up_limit": [12.97, float("nan")],
                "down_limit": [10.61, float("nan")],
                "limit_status": ["finite", "unverified_special"],
                "is_st": [False, False],
                "trading_state": ["traded", "suspended"],
                "close_limit_up": [False, False],
                "close_limit_down": [False, False],
                "touched_up": [False, False],
                "touched_down": [False, False],
                "is_xr": [False, True],
            }
        )
        qdc_norm = normalize("qdc", qdc_raw, dataset="daily_status")
        assert qdc_norm.loc[qdc_norm["ticker"] == "000001.SZ", "limit_pct"].iloc[
            0
        ] == pytest.approx(0.1001, abs=1e-4)
        row_bj = qdc_norm[qdc_norm["ticker"] == "920000.BJ"].iloc[0]
        assert bool(row_bj["is_suspend"]) is True
        assert bool(row_bj["is_xr"]) is True
        assert pd.isna(row_bj["pre_close"])
        assert pd.isna(row_bj["limit_pct"])
        SCHEMA["cn_stock_daily_status"].validate(qdc_norm)

        ad_raw = pd.DataFrame(
            {
                "MARKET_CODE": ["000001.SZ"],
                "TRADE_DATE": ["20261113"],
                "PRECLOSE": [11.79],
                "HIGH_LIMITED": [12.97],
                "LOW_LIMITED": [10.61],
                "PRICE_HIGH_LMT_RATE": [0.1],
                "IS_ST_SEC": ["0"],
                "IS_SUSP_SEC": ["0"],
                "IS_XR_SEC": ["0"],
                "high": [12.97],
                "low": [11.70],
                "close": [12.97],
            }
        )
        ad_norm = normalize("amazingdata", ad_raw, dataset="daily_status")
        assert bool(ad_norm["is_limit_up"].iloc[0]) is True
        assert bool(ad_norm["touched_limit_up"].iloc[0]) is True
        SCHEMA["cn_stock_daily_status"].validate(ad_norm)


class TestMinuteBarsC03AndIngestion:
    def test_c03_accepts_240_and_241_and_rejects_239(self):
        t0 = datetime(2026, 11, 13, 9, 30)
        df_240 = pd.DataFrame(
            {
                "ticker": ["000001.SZ"] * 240,
                "trade_date": [TRADE_DAY] * 240,
                "datetime": [t0 + timedelta(minutes=i) for i in range(240)],
            }
        )
        df_241 = pd.DataFrame(
            {
                "ticker": ["000001.SZ"] * 241,
                "trade_date": [TRADE_DAY] * 241,
                "datetime": [t0 + timedelta(minutes=i) for i in range(241)],
            }
        )
        df_239 = df_240.iloc[:239].copy()

        assert (
            bool(check_minute_bar_count(df_240, expected_bars=[240, 241]).iloc[0])
            is False
        )
        assert (
            bool(check_minute_bar_count(df_241, expected_bars=[240, 241]).iloc[0])
            is False
        )
        assert (
            bool(check_minute_bar_count(df_239, expected_bars=[240, 241]).iloc[0])
            is True
        )
        # 标量 int 保持向后兼容
        assert bool(check_minute_bar_count(df_240, expected_bars=240).iloc[0]) is False
        assert bool(check_minute_bar_count(df_241, expected_bars=240).iloc[0]) is True

    def test_ingest_table_passes_daily_cross_df_for_minute(
        self, tmp_path: Path, monkeypatch
    ):
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Asia/Shanghai")
        bars_t = [
            int(datetime(2026, 11, 13, 9, 30, tzinfo=tz).timestamp() * 1000)
            + i * 60_000
            for i in range(241)
        ]
        qdc_min = pd.DataFrame(
            {
                "symbol": ["000001.XSHE"] * 241,
                "ts": bars_t,
                "open": [10.0] * 241,
                "high": [10.1] * 241,
                "low": [9.9] * 241,
                "close": [10.05] * 241,
                "volume": [1000] * 241,
                "amount": [10050.0] * 241,
            }
        )
        save_raw(qdc_min, "qdc", "cn_stock_bars_minute", TRADE_DAY, tmp_path)

        daily_dir = tmp_path / "asset" / "cn_stock_bars_daily"
        daily_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "ticker": ["000001.SZ"],
                "trade_date": [TRADE_DAY],
                "open": [10.0],
                "high": [10.1],
                "low": [9.9],
                "close": [10.05],
                "volume": [241000],
                "amount": [2422050.0],
            }
        ).to_parquet(daily_dir / "2026-11-13.parquet", index=False)

        captured: dict[str, object] = {}

        def fake_ingest_with_dqc(df, td, dqc_group, **kwargs):
            captured["dqc_group"] = dqc_group
            captured["cross_df"] = kwargs.get("cross_df")
            return SCHEMA["cn_stock_bars_minute"].validate(df)

        monkeypatch.setattr("data.dqc.engine.ingest_with_dqc", fake_ingest_with_dqc)
        monkeypatch.setattr(
            "data.dqc.report.build_reference_overrides",
            lambda *_args, **_kwargs: {},
        )

        outcome = ingest_table(
            "cn_stock_bars_minute",
            TRADE_DAY,
            source="qdc",
            from_raw=True,
            cross_check=False,
            data_root=tmp_path,
        )
        assert outcome.status == "success"
        assert captured["dqc_group"] == "bars_minute"
        assert isinstance(captured["cross_df"], pd.DataFrame)
        assert len(captured["cross_df"]) == 1
