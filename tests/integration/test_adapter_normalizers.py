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
from data.normalizers import UNIFIED_COLUMNS, get_normalizer
from data.normalizers.qdc_normalizer import normalize as normalize_qdc
from data.normalizers.straightflush_normalizer import (
    normalize as normalize_straightflush,
)
from data.raw import load_raw, save_raw
from data.storage_schema import BARS_DAILY_SCHEMA

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

    def test_rejects_unsupported_asset_type(self):
        with pytest.raises(ValueError, match="unsupported asset_type"):
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

    def test_rejects_unsupported_asset_type(self):
        with pytest.raises(ValueError, match="unsupported asset_type"):
            normalize_straightflush("futures", pd.DataFrame())


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
