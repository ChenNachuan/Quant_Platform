"""AKShare source adapter — A-share daily market data.

Wraps ``akshare`` to pull raw daily OHLCV.  No watermark support (full pull
each call).  API constraints handled internally: callers see only
``fetch()`` → ``FetchResult``.

Raw contract: ``adjust=""`` (no forward/backward adjustment) so the values
are source-original and idempotent re-runs return identical prices.
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

    _ASSET_TYPES = ["stock"]

    def __init__(self, symbols: list[str] | None = None) -> None:
        # Minimal default: one liquid large-cap.  Full-market paging is
        # iteration 2+ scope; the ABC contract is what ac-1 verifies.
        self._symbols = symbols or ["000001"]

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
        frames = []

        if asset_type == "stock":
            for sym in self._symbols:
                df = ak.stock_zh_a_hist(
                    symbol=sym,
                    period="daily",
                    start_date=date_str,
                    end_date=date_str,
                    adjust="",  # raw contract: source-original values (fix 5)
                )
                if df is not None and len(df) > 0:
                    frames.append(df)
        else:  # pragma: no cover — guarded above
            raise ValueError(f"unsupported asset_type: {asset_type!r}")

        import pandas as pd

        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        return FetchResult(
            df=combined,
            source="akshare",
            asset_type=asset_type,
            trade_date=trade_date,
            row_count=len(combined),
            watermark=None,  # akshare does not support incremental watermark
        )
