"""Tushare source adapter — A-share daily market data.

Wraps ``tushare`` Pro API to pull raw daily bars.  Supports date-based
watermark for incremental fetch.  API constraints handled internally.
"""

from __future__ import annotations

import logging
import os
from datetime import date


from data.source_adapter import FetchResult, SourceAdapter

logger = logging.getLogger(__name__)

__all__ = ["TushareAdapter"]


class TushareAdapter(SourceAdapter):
    """Fetch A-share daily bars via Tushare Pro API."""

    requests_per_second = 5.0

    _ASSET_TYPES = ["stock"]

    def __init__(self, token: str | None = None) -> None:
        # Token from config key, never from .env directly (S6).
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
        else:  # pragma: no cover — guarded above
            raise ValueError(f"unsupported asset_type: {asset_type!r}")

        return FetchResult(
            df=df,
            source="tushare",
            asset_type=asset_type,
            trade_date=trade_date,
            row_count=len(df),
            watermark=date_str,  # watermark = last successful trade date
        )
