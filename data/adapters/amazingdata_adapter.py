"""AmazingData source adapter — A-share / options market data.

Wraps the AmazingData SDK.  Key constraints handled internally (C2):

* ``get_option_basic_info`` caps ~100–104 rows/call → batch 10 codes per call.
* ``local_path`` must be ``str()`` (SDK does string concatenation internally).
* ``close()`` calls ``ad.logout()`` to release the connection slot.
* ``requests_per_second = 2.0`` (connection count is limited).
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from data.source_adapter import FetchResult, SourceAdapter

logger = logging.getLogger(__name__)

__all__ = ["AmazingdataAdapter"]

_BATCH_SIZE = 10  # codes per API call (C2)


class AmazingdataAdapter(SourceAdapter):
    """Fetch market data via AmazingData SDK."""

    requests_per_second = 2.0  # connection-limited (design doc)

    _ASSET_TYPES = ["stock", "option"]

    def __init__(self, local_path: str | Path = "/data") -> None:
        self._local_path = str(local_path)  # SDK requires str (C2)
        self._ad = None  # lazy init

    def _ensure_client(self):
        if self._ad is None:
            from AmazingData import AmazingData  # type: ignore[import-untyped]

            self._ad = AmazingData()
        return self._ad

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

        ad = self._ensure_client()
        date_str = trade_date.strftime("%Y-%m-%d")

        if asset_type == "stock":
            df = self._fetch_stock(ad, date_str)
        elif asset_type == "option":
            df = self._fetch_option(ad, date_str)
        else:  # pragma: no cover — guarded above
            raise ValueError(f"unsupported asset_type: {asset_type!r}")

        return FetchResult(
            df=df,
            source="amazingdata",
            asset_type=asset_type,
            trade_date=trade_date,
            row_count=len(df),
            watermark=date_str,
        )

    def _fetch_stock(self, ad, date_str: str) -> pd.DataFrame:
        """Fetch stock daily data.  Minimal: one call for all codes on *date_str*."""
        base_data = ad.get_base_data()
        code_list = base_data.get_hist_code_list(
            local_path=self._local_path,  # str() forced in __init__
        )
        df = base_data.get_hist_data(
            code_list=code_list,
            begin_date=date_str,
            end_date=date_str,
            local_path=self._local_path,
        )
        return df

    def _fetch_option(self, ad, date_str: str) -> pd.DataFrame:
        """Fetch option basic info in batches of 10 codes (C2: ~100-row cap)."""
        info_data = ad.get_info_data()
        base_data = ad.get_base_data()
        all_codes = base_data.get_option_code_list(local_path=self._local_path)

        frames: list[pd.DataFrame] = []
        for i in range(0, len(all_codes), _BATCH_SIZE):
            batch = all_codes[i : i + _BATCH_SIZE]
            result = info_data.get_option_basic_info(batch, is_local=False)
            if result is not None and len(result) > 0:
                frames.append(result)

        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()

    def health_check(self) -> bool:
        """Probe one real SDK call — building the client alone is not a health signal."""
        try:
            ad = self._ensure_client()
            base_data = ad.get_base_data()
            codes = base_data.get_hist_code_list(local_path=self._local_path)
            return codes is not None and len(codes) > 0
        except Exception:
            return False

    def close(self) -> None:
        """Release the AmazingData connection slot (C2: ad.logout())."""
        if self._ad is not None:
            try:
                self._ad.logout()
            except Exception:
                logger.warning("amazingdata logout failed", exc_info=True)
            finally:
                self._ad = None
