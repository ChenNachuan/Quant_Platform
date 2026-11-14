"""Tushare 数据源适配器（A 股日线行情，支持按日期水位线增量拉取）。"""

from __future__ import annotations

import logging
import os
from datetime import date

import pandas as pd

from data.source_adapter import FetchResult, SourceAdapter

logger = logging.getLogger(__name__)

__all__ = ["TushareAdapter"]


class TushareAdapter(SourceAdapter):
    """通过 Tushare Pro API 拉取 A 股日线行情。"""

    requests_per_second = 5.0

    _ASSET_TYPES = ["stock"]

    def __init__(self, token: str | None = None) -> None:
        # 仅从入参或环境变量读取 token
        self._token = token or os.environ.get("TUSHARE_TOKEN", "")

    def list_asset_types(self) -> list[str]:
        return list(self._ASSET_TYPES)

    def fetch(
        self,
        asset_type: str,
        trade_date: date,
        watermark: str | None = None,
    ) -> FetchResult:
        if asset_type not in self._ASSET_TYPES:
            raise ValueError(f"unsupported asset_type: {asset_type!r}")

        import tushare as ts

        pro = ts.pro_api(self._token)
        date_str = trade_date.strftime("%Y%m%d")

        if asset_type == "stock":
            df = pro.daily(trade_date=date_str)
        else:  # pragma: no cover
            raise ValueError(f"unsupported asset_type: {asset_type!r}")

        if df is None:
            df = pd.DataFrame()

        return FetchResult(
            df=df,
            source="tushare",
            asset_type=asset_type,
            trade_date=trade_date,
            row_count=len(df),
            watermark=date_str,  # 以最近成功交易日作为水位线
        )
