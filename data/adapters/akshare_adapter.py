"""AKShare source adapter — A-share daily market data.

Wraps ``akshare`` to pull raw daily OHLCV.  No watermark support (full pull
each call).  API constraints handled internally: callers see only
``fetch()`` → ``FetchResult``.
"""

from __future__ import annotations

import logging
from datetime import date


from data.source_adapter import FetchResult, SourceAdapter

logger = logging.getLogger(__name__)

__all__ = ["AkshareAdapter"]


class AkshareAdapter(SourceAdapter):
    """Fetch A-share daily bars via ``akshare``."""

    requests_per_second = 5.0

    #: asset types this adapter can fetch
    _ASSET_TYPES = ["stock"]

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

        import akshare as ak

        date_str = trade_date.strftime("%Y%m%d")

        if asset_type == "stock":
            # stock_zh_a_hist returns all A-share stocks for a given date range.
            # Minimal fetch: one-day window.
            df = ak.stock_zh_a_hist(
                symbol="",  # empty = all stocks
                period="daily",
                start_date=date_str,
                end_date=date_str,
                adjust="qfq",
            )
        else:  # pragma: no cover — guarded above
            raise ValueError(f"unsupported asset_type: {asset_type!r}")

        return FetchResult(
            df=df,
            source="akshare",
            asset_type=asset_type,
            trade_date=trade_date,
            row_count=len(df),
            watermark=None,  # akshare does not support incremental watermark
        )
