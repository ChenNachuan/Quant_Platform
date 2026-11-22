"""Tushare 数据源适配器（A 股基础信息、日线行情、ETF 日线、估值基本面、日频风控状态与公司行动）。

协议要点：
* 为 Pro API 请求内置瞬态网络/50x 指数退避重试（``1s / 2s / 4s``）。
* ``stock_basic`` 汇总 ``L`` / ``D`` / ``P`` 状态去重。
* ``daily_basic``（2000 积分无限频）支撑 ``cn_stock_fundamental`` 全历史校验。
* ``fund_daily`` + ``fund_adj`` 支撑场内 ETF 日线行情与历史 K02 修复补缺。
* 严格禁用限频极严的 ``adj_factor``（批量历史）与 ``stk_mins``。
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from core.config import load_secrets
from data.source_adapter import FetchResult, SourceAdapter, SourceDataset

logger = logging.getLogger(__name__)

__all__ = ["TushareAdapter"]

_TRANSIENT_TOKENS = (
    "timeout",
    "timed out",
    "connection",
    "remote end closed",
    "500",
    "502",
    "503",
    "504",
    "429",
    "temporarily unavailable",
)


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
    """通过 Tushare Pro API 拉取 A 股基础档案、行情、ETF 日线与估值数据。"""

    requests_per_second = 5.0

    _SUPPORTED_DATASETS = [
        "stock",
        "etf",
        "stock_basic",
        "fundamental",
        "daily_status",
        "corporate_actions",
    ]

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

    @staticmethod
    def _call_with_retry(fn, *args: Any, max_retries: int = 3, **kwargs: Any):
        """对瞬态网络抖动或 50x 错误执行指数退避重试。"""
        for attempt in range(max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                msg = str(exc).lower()
                is_transient = isinstance(
                    exc, (ConnectionError, TimeoutError, OSError)
                ) or any(tok in msg for tok in _TRANSIENT_TOKENS)
                if not is_transient or attempt == max_retries:
                    raise
                wait = float(2**attempt)
                logger.warning(
                    "tushare transient error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    max_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)
        raise RuntimeError("unreachable")  # pragma: no cover

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
            df = self._call_with_retry(pro.daily, trade_date=date_str)
            df = self._enrich_stock_daily(pro, df, date_str)
        elif dataset == "etf":
            df = self._call_with_retry(pro.fund_daily, trade_date=date_str)
            df = self._enrich_etf_daily(pro, df, date_str)
        elif dataset == "stock_basic":
            df = self._fetch_stock_basic(pro)
        elif dataset == "fundamental":
            df = self._call_with_retry(
                pro.daily_basic,
                trade_date=date_str,
                fields=(
                    "ts_code,trade_date,close,turnover_rate,volume_ratio,"
                    "pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,"
                    "total_share,float_share,free_share,total_mv,circ_mv"
                ),
            )
        elif dataset == "corporate_actions":
            df = self._call_with_retry(pro.dividend, ex_date=date_str)
        elif dataset == "daily_status":
            df = self._fetch_daily_status(pro, date_str)
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
            watermark=date_str,
        )

    def _fetch_stock_basic(self, pro) -> pd.DataFrame:
        fields = (
            "ts_code,symbol,name,area,industry,market,exchange,"
            "list_status,list_date,delist_date,is_hs"
        )
        frames: list[pd.DataFrame] = []
        for status in ("L", "D", "P"):
            part = self._call_with_retry(
                pro.stock_basic, exchange="", list_status=status, fields=fields
            )
            if isinstance(part, pd.DataFrame) and not part.empty:
                frames.append(part)
        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames, ignore_index=True)
        if "ts_code" in merged.columns:
            merged = merged.drop_duplicates(subset=["ts_code"], keep="first")
        return merged.reset_index(drop=True)

    def _fetch_daily_status(self, pro, date_str: str) -> pd.DataFrame:
        lim = self._call_with_retry(pro.stk_limit, trade_date=date_str)
        daily = self._call_with_retry(pro.daily, trade_date=date_str)
        lim_df = lim.copy() if isinstance(lim, pd.DataFrame) else pd.DataFrame()
        day_df = daily.copy() if isinstance(daily, pd.DataFrame) else pd.DataFrame()

        if lim_df.empty and day_df.empty:
            return pd.DataFrame()

        if not lim_df.empty and not day_df.empty:
            day_cols = [
                c
                for c in ("ts_code", "pre_close", "high", "low", "close")
                if c in day_df.columns
            ]
            out = lim_df.merge(
                day_df[day_cols].drop_duplicates("ts_code"),
                on="ts_code",
                how="outer",
            )
        elif not lim_df.empty:
            out = lim_df
        else:
            out = day_df

        if "trade_date" not in out.columns:
            out["trade_date"] = date_str
        else:
            out["trade_date"] = out["trade_date"].fillna(date_str)

        try:
            susp = self._call_with_retry(pro.suspend_d, trade_date=date_str)
            if (
                isinstance(susp, pd.DataFrame)
                and not susp.empty
                and "ts_code" in susp.columns
            ):
                if "suspend_type" in susp.columns:
                    susp = susp[susp["suspend_type"].astype(str).str.upper() == "S"]
                halted = set(susp["ts_code"].astype(str))
                out["is_suspend"] = out["ts_code"].astype(str).isin(halted)
        except Exception as exc:  # noqa: BLE001
            logger.debug("tushare suspend_d unavailable for %s: %s", date_str, exc)

        try:
            div = self._call_with_retry(pro.dividend, ex_date=date_str)
            if (
                isinstance(div, pd.DataFrame)
                and not div.empty
                and "ts_code" in div.columns
            ):
                if "div_proc" in div.columns:
                    div = div[div["div_proc"].astype(str) == "实施"]
                xr_codes = set(div["ts_code"].astype(str))
                out["is_xr"] = out["ts_code"].astype(str).isin(xr_codes)
        except Exception as exc:  # noqa: BLE001
            logger.debug("tushare dividend unavailable for %s: %s", date_str, exc)

        return out

    @classmethod
    def _enrich_etf_daily(
        cls, pro, df: pd.DataFrame | None, date_str: str
    ) -> pd.DataFrame:
        """补充同日 ETF 复权因子（接口不可用或无数据时安全跳过）。"""
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()
        if "ts_code" not in df.columns:
            return df
        out = df.copy()
        try:
            adj = cls._call_with_retry(pro.fund_adj, trade_date=date_str)
            if (
                isinstance(adj, pd.DataFrame)
                and not adj.empty
                and {"ts_code", "adj_factor"} <= set(adj.columns)
            ):
                adj_sub = adj[["ts_code", "adj_factor"]].drop_duplicates("ts_code")
                out = out.merge(adj_sub, on="ts_code", how="left")
        except Exception as exc:  # noqa: BLE001
            logger.debug("tushare fund_adj unavailable for %s: %s", date_str, exc)
        return out

    @classmethod
    def _enrich_stock_daily(
        cls, pro, df: pd.DataFrame | None, date_str: str
    ) -> pd.DataFrame:
        """补充同日复权因子与停牌标记（接口不可用或无数据时安全跳过）。"""
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()
        if "ts_code" not in df.columns:
            return df
        out = df.copy()
        try:
            adj = cls._call_with_retry(pro.adj_factor, trade_date=date_str)
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
            susp = cls._call_with_retry(pro.suspend_d, trade_date=date_str)
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
