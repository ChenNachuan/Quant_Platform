"""AKShare 数据源适配器（A 股日线行情）。

不支持增量水位线（每次全量拉取）；拉取时固定 ``adjust=""`` 保持原始未复权价格，确保重跑幂等。
"""

from __future__ import annotations

import logging
from datetime import date

from data.source_adapter import FETCH_TIMEOUT, FetchResult, SourceAdapter

logger = logging.getLogger(__name__)

__all__ = ["AkshareAdapter"]


class AkshareAdapter(SourceAdapter):
    """通过 ``akshare`` 拉取 A 股日线行情。"""

    requests_per_second = 5.0

    _ASSET_TYPES = ["stock"]

    def __init__(self, symbols: list[str] | None = None) -> None:
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
                    adjust="",  # 原始未复权价格
                    timeout=FETCH_TIMEOUT,  # SDK 默认为 None，超时不设限易挂起线程
                )
                if df is not None and len(df) > 0:
                    frames.append(df)
        else:  # pragma: no cover
            raise ValueError(f"unsupported asset_type: {asset_type!r}")

        import pandas as pd

        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        return FetchResult(
            df=combined,
            source="akshare",
            asset_type=asset_type,
            trade_date=trade_date,
            row_count=len(combined),
            watermark=None,  # AKShare 不支持增量水位线
        )
