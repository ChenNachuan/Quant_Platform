"""AmazingData 数据源适配器（A 股与期权行情）。

SDK 约束：
* ``get_option_basic_info`` 单次返回上限约 100–104 行，按每批 10 个代码分批查询。
* ``local_path`` 必须为 ``str``（SDK 内部会做字符串拼接）。
* ``close()`` 调用 ``ad.logout()`` 释放有限的连接槽位，限速 ``requests_per_second = 2.0``。
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from data.source_adapter import FetchResult, SourceAdapter

logger = logging.getLogger(__name__)

__all__ = ["AmazingdataAdapter"]

_BATCH_SIZE = 10  # 单次 API 调用的代码批量上限


class AmazingdataAdapter(SourceAdapter):
    """通过 AmazingData SDK 拉取行情数据。"""

    requests_per_second = 2.0  # 受限于并发连接数

    _ASSET_TYPES = ["stock", "option"]

    def __init__(self, local_path: str | Path = "/data") -> None:
        self._local_path = str(local_path)  # SDK 要求 str 类型
        self._ad = None

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
        else:  # pragma: no cover
            raise ValueError(f"unsupported asset_type: {asset_type!r}")

        if df is None:
            df = pd.DataFrame()

        return FetchResult(
            df=df,
            source="amazingdata",
            asset_type=asset_type,
            trade_date=trade_date,
            row_count=len(df),
            watermark=date_str,
        )

    def _fetch_stock(self, ad, date_str: str) -> pd.DataFrame:
        """按日拉取全市场股票日线数据。"""
        base_data = ad.get_base_data()
        code_list = base_data.get_hist_code_list(
            local_path=self._local_path,
        )
        df = base_data.get_hist_data(
            code_list=code_list,
            begin_date=date_str,
            end_date=date_str,
            local_path=self._local_path,
        )
        return df if df is not None else pd.DataFrame()

    def _fetch_option(self, ad, date_str: str) -> pd.DataFrame:
        """按每批 10 个代码分批拉取期权基础信息。"""
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
        """执行一次真实 SDK 调用验证连通性。"""
        try:
            ad = self._ensure_client()
            base_data = ad.get_base_data()
            codes = base_data.get_hist_code_list(local_path=self._local_path)
            return codes is not None and len(codes) > 0
        except Exception:
            return False

    def close(self) -> None:
        """调用 ``ad.logout()`` 释放连接槽位。"""
        if self._ad is not None:
            try:
                self._ad.logout()
            except Exception:
                logger.warning("amazingdata logout failed", exc_info=True)
            finally:
                self._ad = None
