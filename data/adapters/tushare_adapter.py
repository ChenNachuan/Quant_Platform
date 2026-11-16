"""Tushare 数据源适配器（A 股日线行情，支持按日期水位线增量拉取）。"""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd

from core.config import load_secrets
from data.source_adapter import FetchResult, SourceAdapter, SourceDataset

logger = logging.getLogger(__name__)

__all__ = ["TushareAdapter"]


def _read_tushare_secrets(secrets_path: str | Path) -> dict[str, str]:
    path = Path(secrets_path)
    if not path.is_file():
        return {}
    try:
        return load_secrets(str(path))
    except OSError:
        logger.debug("could not read secrets file %s", path)
        return {}


class TushareAdapter(SourceAdapter):
    """通过 Tushare Pro API 拉取 A 股日线行情。"""

    requests_per_second = 5.0

    _SUPPORTED_DATASETS = ["stock"]

    def __init__(
        self, token: str | None = None, secrets_path: str | Path = "config/.env"
    ) -> None:
        self._token = (token or "").strip()
        self._secrets_path = secrets_path

    def _resolve_token(self) -> str:
        """按显式入参 -> 环境变量 -> secrets 文件顺序懒加载 TUSHARE_TOKEN。"""
        if self._token:
            return self._token
        env_tok = os.environ.get("TUSHARE_TOKEN", "").strip()
        if env_tok:
            return env_tok
        return (
            _read_tushare_secrets(self._secrets_path).get("TUSHARE_TOKEN", "").strip()
        )

    def list_datasets(self) -> list[str]:
        return list(self._SUPPORTED_DATASETS)

    def fetch(
        self,
        dataset: SourceDataset | str,
        trade_date: date,
        watermark: str | None = None,
    ) -> FetchResult:
        if dataset not in self._SUPPORTED_DATASETS:
            raise ValueError(f"unsupported dataset: {dataset!r}")

        import tushare as ts

        pro = ts.pro_api(self._resolve_token())
        date_str = trade_date.strftime("%Y%m%d")

        if dataset == "stock":
            df = pro.daily(trade_date=date_str)
            df = self._enrich_stock_daily(pro, df, date_str)
        else:  # pragma: no cover
            raise ValueError(f"unsupported dataset: {dataset!r}")

        if df is None:
            df = pd.DataFrame()

        return FetchResult(
            df=df,
            source="tushare",
            dataset=dataset,
            trade_date=trade_date,
            row_count=len(df),
            watermark=date_str,  # 以最近成功交易日作为水位线
        )

    @staticmethod
    def _enrich_stock_daily(
        pro, df: pd.DataFrame | None, date_str: str
    ) -> pd.DataFrame:
        """补充同日复权因子与停牌标记（接口不可用或无数据时安全跳过）。"""
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()
        if "ts_code" not in df.columns:
            return df
        out = df.copy()
        try:
            adj = pro.adj_factor(trade_date=date_str)
            if (
                isinstance(adj, pd.DataFrame)
                and not adj.empty
                and {"ts_code", "adj_factor"} <= set(adj.columns)
            ):
                adj_sub = adj[["ts_code", "adj_factor"]].drop_duplicates("ts_code")
                out = out.merge(adj_sub, on="ts_code", how="left")
        except Exception as exc:  # noqa: BLE001
            logger.debug("tushare adj_factor unavailable for %s: %s", date_str, exc)

        try:
            susp = pro.suspend_d(trade_date=date_str)
            if (
                isinstance(susp, pd.DataFrame)
                and not susp.empty
                and "ts_code" in susp.columns
            ):
                if "suspend_type" in susp.columns:
                    susp = susp[susp["suspend_type"].astype(str).str.upper() == "S"]
                halted = set(susp["ts_code"].astype(str))
                out["suspend_d"] = (
                    out["ts_code"].astype(str).isin(halted).map({True: "S", False: " "})
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("tushare suspend_d unavailable for %s: %s", date_str, exc)
        return out
