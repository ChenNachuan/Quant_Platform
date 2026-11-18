"""Integration tests for QDCData and StraightFlush normalizers & raw->unified flow."""

from __future__ import annotations

import io
from datetime import date, datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest
import requests

from data.adapters import get_adapter
from data.normalizers import (
    SOURCE_COLUMN_MAP,
    UNIFIED_COLUMNS,
    get_normalizer,
    normalize as normalize_unified,
)
from data.normalizers.qdc_normalizer import normalize as normalize_qdc
from data.normalizers.straightflush_normalizer import (
    normalize as normalize_straightflush,
)
from data.raw import load_raw, save_raw
from data.storage_ops import normalize_missing_floats
from data.storage_schema import (
    BARS_DAILY_SCHEMA,
    CN_STOCK_INDEX_BARS_DAILY_SCHEMA,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class TestQdcNormalizer:
    def test_dispatch_and_unified_columns(self):
        fn = get_normalizer("qdc")
        assert fn is normalize_qdc

        # 2026-01-05 00:00:00 Asia/Shanghai = 2026-01-04 16:00:00 UTC!
        # Must resolve to 2026-01-05 (Beijing date), NOT 2026-01-04 (UTC date).
        ts_ms = int(datetime(2026, 1, 5, 0, 0, tzinfo=_SHANGHAI).timestamp() * 1000)
        raw = pd.DataFrame(
            {
                "symbol": ["000001.XSHE", "600519.XSHG", "430047.XBSE"],
                "ts": [ts_ms, ts_ms, ts_ms],
                "open": [11.0, 1500.0, 20.0],
                "high": [11.5, 1520.0, 21.0],
                "low": [10.8, 1490.0, 19.5],
                "close": [11.2, 1510.0, 20.5],
                "volume": [100000.0, 50000.0, 12000.0],
                "amount": [1120000.0, 75500000.0, 246000.0],
                "source": ["qdc", "qdc", "qdc"],
            }
        )
        out = fn("stock", raw)
        assert list(out.columns) == UNIFIED_COLUMNS
        assert out["ticker"].tolist() == ["000001.SZ", "600519.SH", "430047.BJ"]
        assert all(d == date(2026, 1, 5) for d in out["trade_date"])
        assert (out["adjfactor"] == 1.0).all()
        assert (out["suspend"] == False).all()  # noqa: E712
        BARS_DAILY_SCHEMA.validate(out)

    def test_preserves_nan_and_hfq_factor_on_suspended_row(self):
        ts_ms = int(datetime(2026, 1, 5, 0, 0, tzinfo=_SHANGHAI).timestamp() * 1000)
        raw = pd.DataFrame(
            {
                "symbol": ["000001.XSHE", "600519.XSHG"],
                "ts": [ts_ms, ts_ms],
                "open": [11.0, np.nan],
                "high": [11.5, np.nan],
                "low": [10.8, np.nan],
                "close": [11.2, np.nan],
                "volume": [100000, np.nan],
                "amount": [1120000.0, np.nan],
                "hfq_factor": [112.3, 8.45],
                "suspend": [False, True],
            }
        )
        out = normalize_qdc("stock", raw)
        assert pd.isna(out.loc[1, "open"])
        assert pd.isna(out.loc[1, "volume"])
        assert out.loc[1, "suspend"] == True  # noqa: E712
        assert out.loc[1, "adjfactor"] == pytest.approx(8.45)

    def test_rejects_unsupported_dataset(self):
        with pytest.raises(ValueError, match="unsupported dataset"):
            normalize_qdc("futures", pd.DataFrame())


class TestStraightFlushNormalizer:
    def test_dispatch_and_maps_turnover_to_amount(self):
        fn = get_normalizer("straightflush")
        assert fn is normalize_straightflush

        date_ms = int(datetime(2026, 1, 5, 0, 0, tzinfo=_SHANGHAI).timestamp() * 1000)
        raw = pd.DataFrame(
            {
                "thscode": ["600519.SH", "000001.SZ"],
                "date_ms": [date_ms, date_ms],
                "open_price": [1500.0, 11.0],
                "high_price": [1520.0, 11.5],
                "low_price": [1490.0, 10.8],
                "close_price": [1510.0, 11.2],
                "volume": [50000, 100000],
                "turnover": [75500000.0, 1120000.0],  # 成交额 -> amount
                "turnover_ratio_pct": [0.35, 1.20],  # 换手率 -> turnover
            }
        )
        out = fn("stock", raw)
        assert list(out.columns) == UNIFIED_COLUMNS
        assert out["ticker"].tolist() == ["600519.SH", "000001.SZ"]
        assert all(d == date(2026, 1, 5) for d in out["trade_date"])
        assert out["amount"].tolist() == pytest.approx([75500000.0, 1120000.0])
        assert out["turnover"].tolist() == pytest.approx([0.35, 1.20])
        assert (out["adjfactor"] == 1.0).all()
        BARS_DAILY_SCHEMA.validate(out)

    def test_rejects_unsupported_dataset(self):
        with pytest.raises(ValueError, match="unsupported dataset"):
            normalize_straightflush("futures", pd.DataFrame())

    def test_casts_integer_amount_to_float64_for_index_schema(self):
        date_ms = int(datetime(2026, 1, 5, 0, 0, tzinfo=_SHANGHAI).timestamp() * 1000)
        raw = pd.DataFrame(
            {
                "thscode": ["000300.SH", "399001.SZ"],
                "date_ms": [date_ms, date_ms],
                "open_price": [4000, 10900],
                "high_price": [4020, 11000],
                "low_price": [3990, 10850],
                "close_price": [4016, 10968],
                "volume": [17934199800, 55925521000],
                "turnover": [460245657530, 885054040000],  # 整型成交额
            }
        )
        out = normalize_straightflush("index", raw)
        assert out["amount"].dtype == np.float64
        assert out["open"].dtype == np.float64
        schema = CN_STOCK_INDEX_BARS_DAILY_SCHEMA
        proj = out[[c for c in schema.columns if c in out.columns]].copy()
        for col_name, cs in schema.columns.items():
            if col_name not in proj.columns and cs.nullable:
                proj[col_name] = None
        schema.validate(normalize_missing_floats(proj, "cn_stock_index_bars_daily"))

    def test_normalizes_sse_csi_index_suffix_to_sh_while_preserving_pure_csi(self):
        date_ms = int(datetime(2026, 11, 13, 0, 0, tzinfo=_SHANGHAI).timestamp() * 1000)
        raw = pd.DataFrame(
            {
                "thscode": ["000510.CSI", "000860.CSI", "932000.CSI"],
                "date_ms": [date_ms, date_ms, date_ms],
                "open_price": [4907.23, 1142.15, 1000.0],
                "high_price": [4913.55, 1148.20, 1010.0],
                "low_price": [4827.96, 1134.12, 990.0],
                "close_price": [4842.51, 1137.45, 1005.0],
                "volume": [22980251000, 4899016300, 1000000],
                "turnover": [594562340000.0, 87961076000.0, 20000000.0],
            }
        )
        out = normalize_straightflush("index", raw)
        assert out["ticker"].tolist() == ["000510.SH", "000860.SH", "932000.CSI"]

        # 若输入已同时包含 000510.SH，不得把 000510.CSI 碰撞成重复键
        dup_raw = pd.concat([raw.iloc[[0]], raw.iloc[[0]].assign(thscode="000510.SH")])
        dup_out = normalize_straightflush("index", dup_raw)
        assert dup_out["ticker"].tolist() == ["000510.CSI", "000510.SH"]


class TestAmazingdataIndexNormalizer:
    def test_scales_sh_index_volume_lots_to_shares_and_passes_schema(self):
        from data.normalizers.amazingdata_normalizer import (
            normalize as normalize_amazingdata,
        )

        raw = pd.DataFrame(
            {
                "code": ["000300.SH", "399001.SZ", "899050.BJ"],
                "kline_time": ["2026-11-13", "2026-11-13", "2026-11-13"],
                "open": [4000.0, 10900.0, 870.0],
                "high": [4020.0, 11000.0, 880.0],
                "low": [3990.0, 10850.0, 865.0],
                "close": [4016.29, 10968.79, 876.33],
                "volume": [179341998, 17725061993, 611233743],
                "amount": [460245657530, 453925229230, 11557578700],
            }
        )
        out = normalize_amazingdata("index", raw)
        assert out["volume"].tolist() == [17934199800, 17725061993, 611233743]
        assert str(out["volume"].dtype) == "Int64"
        assert out["amount"].dtype == np.float64
        schema = CN_STOCK_INDEX_BARS_DAILY_SCHEMA
        proj = out[[c for c in schema.columns if c in out.columns]].copy()
        for col_name, cs in schema.columns.items():
            if col_name not in proj.columns and cs.nullable:
                proj[col_name] = None
        schema.validate(normalize_missing_floats(proj, "cn_stock_index_bars_daily"))


class TestUnifiedNormalizerRegistry:
    def test_source_column_map_covers_all_five_sources(self):
        assert set(SOURCE_COLUMN_MAP) == {
            "qdc",
            "amazingdata",
            "straightflush",
            "tushare",
            "akshare",
        }

    def test_unified_normalize_dispatches_and_validates_required_columns(self):
        raw_tushare = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20260105"],
                "open": [11.0],
                "high": [11.5],
                "low": [10.8],
                "close": [11.2],
                "vol": [1000.0],
                "amount": [1120.0],
            }
        )
        out = normalize_unified("tushare", raw_tushare)
        assert list(out.columns) == UNIFIED_COLUMNS
        assert out["volume"].iloc[0] == 100000
        assert out["amount"].iloc[0] == pytest.approx(1120000.0)

        with pytest.raises(ValueError, match="no normalizer for source"):
            normalize_unified("unknown_src", raw_tushare)

        with pytest.raises(ValueError, match="缺失必需列"):
            normalize_unified("qdc", pd.DataFrame({"symbol": ["000001.XSHE"]}))

    def test_adapter_preserves_raw_units_while_normalizer_converts(
        self, tmp_path, monkeypatch
    ):
        from data.adapters.amazingdata_adapter import AmazingdataAdapter

        raw_df = pd.DataFrame(
            {
                "code": ["000300.SH"],
                "kline_time": ["2026-11-13"],
                "open": [4000.0],
                "high": [4020.0],
                "low": [3990.0],
                "close": [4016.29],
                "volume": [1000],  # 原始手数，Adapter 不应提前乘以 100
                "amount": [4602456575.0],
            }
        )
        raw_out = (
            tmp_path
            / "raw"
            / "amazingdata"
            / "cn_stock_index_bars_daily"
            / "2026-11-13.parquet"
        )
        raw_out.parent.mkdir(parents=True, exist_ok=True)
        raw_df.to_parquet(raw_out, index=False)

        monkeypatch.setattr(
            "data.adapters.amazingdata_adapter.subprocess.run",
            lambda cmd, **_kw: MagicMock(returncode=0, stdout="", stderr=""),
        )
        adapter = AmazingdataAdapter(data_root=tmp_path, mode="docker")
        fetched = adapter.fetch("index", date(2026, 11, 13))
        assert fetched.df["volume"].iloc[0] == 1000

        norm = get_normalizer("amazingdata")("index", fetched.df)
        assert norm["volume"].iloc[0] == 100000


class TestAdapterRawNormalizeRoundTrip:
    def test_qdc_fetch_save_raw_normalize(self, tmp_path):
        ts_ms = int(datetime(2026, 1, 5, 0, 0, tzinfo=_SHANGHAI).timestamp() * 1000)
        raw_src = pd.DataFrame(
            {
                "symbol": ["000001.XSHE"],
                "ts": [ts_ms],
                "open": [11.0],
                "high": [11.5],
                "low": [10.8],
                "close": [11.2],
                "volume": [100000],
                "amount": [1120000.0],
            }
        )
        buf = io.BytesIO()
        raw_src.to_parquet(buf, index=False)

        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.headers = {"Content-Type": "application/vnd.apache.parquet"}
        resp.content = buf.getvalue()
        mock_session = MagicMock()
        mock_session.get.return_value = resp

        adapter = get_adapter("qdc", api_key="fake", secrets_path=tmp_path / "m.env")
        adapter._session = mock_session
        try:
            fetched = adapter.fetch("stock", date(2026, 1, 5))
            save_raw(fetched.df, "qdc", "stock", date(2026, 1, 5), tmp_path)
            loaded = load_raw("qdc", "stock", date(2026, 1, 5), tmp_path)
            assert loaded is not None
            norm = get_normalizer("qdc")("stock", loaded)
            BARS_DAILY_SCHEMA.validate(norm)
            assert norm["ticker"].iloc[0] == "000001.SZ"
        finally:
            adapter.close()

    def test_straightflush_fetch_save_raw_normalize(self, tmp_path):
        date_ms = int(datetime(2026, 1, 5, 0, 0, tzinfo=_SHANGHAI).timestamp() * 1000)
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.return_value = {
            "code": 0,
            "data": {
                "item": [
                    {
                        "thscode": "600519.SH",
                        "item": [
                            {
                                "date_ms": date_ms,
                                "open_price": 1500.0,
                                "high_price": 1520.0,
                                "low_price": 1490.0,
                                "close_price": 1510.0,
                                "volume": 50000,
                                "turnover": 75500000.0,
                            }
                        ],
                    }
                ]
            },
        }
        mock_session = MagicMock()
        mock_session.get.return_value = resp

        adapter = get_adapter(
            "straightflush",
            token="fake",
            symbols=["600519.SH"],
            secrets_path=tmp_path / "m.env",
        )
        adapter._session = mock_session
        try:
            fetched = adapter.fetch("stock", date(2026, 1, 5))
            save_raw(fetched.df, "straightflush", "stock", date(2026, 1, 5), tmp_path)
            loaded = load_raw("straightflush", "stock", date(2026, 1, 5), tmp_path)
            assert loaded is not None
            norm = get_normalizer("straightflush")("stock", loaded)
            BARS_DAILY_SCHEMA.validate(norm)
            assert norm["ticker"].iloc[0] == "600519.SH"
            assert norm["amount"].iloc[0] == pytest.approx(75500000.0)
        finally:
            adapter.close()
