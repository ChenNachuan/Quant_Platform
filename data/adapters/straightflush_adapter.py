"""同花顺（扶摇 / StraightFlush）数据源适配器。

协议与契约要点：
* 鉴权头 ``X-api-key`` 按显式 ``token`` → 环境变量 → ``config/.env`` 顺序解析。
* HTTP 429 / 5xx 及业务码 ``4001/5001/5002/5003`` 触发可重试异常。
* A 股日线显式传入 ``adjust="none"``（接口默认前复权），保证拉取未复权原始价。
* ``/api/*/historical-batch`` 单次最多传 10 个 ``thscode``，时间参数为 ``Asia/Shanghai`` 毫秒时间戳。
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from core.config import load_secrets
from data.source_adapter import FETCH_TIMEOUT, FetchResult, SourceAdapter

logger = logging.getLogger(__name__)

__all__ = ["StraightFlushAdapter"]

_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_DEFAULT_BASE_URL = "https://fuyao.aicubes.cn"
_BATCH_SIZE = 10  # historical-batch 接口单次最多 10 个代码
_TICKER_LIST_PAGE_LIMIT = 10_000

_TOKEN_ENV_KEYS = (
    "STRIGHT_FLUSH_TOKEN",
    "STRAIGHT_FLUSH_TOKEN",
    "HITHINK_FINANCE_API_KEY",
)

_RETRYABLE_BUSINESS_CODES = frozenset({4001, 5001, 5002, 5003})
_AUTH_BUSINESS_CODES = frozenset({2001, 2003})
_EMPTY_BUSINESS_CODES = frozenset({3001, 3002})

_ASSET_TYPES = ["stock", "etf", "index", "futures", "option", "calendar"]

_META_ASSET_TYPE_MAP: dict[str, str] = {
    "stock": "a-share",
    "etf": "fund-etf",
    "index": "a-share-index",
    "futures": "futures",
    "option": "options",
}


def _read_fuyao_secrets(secrets_path: str | Path) -> dict[str, str]:
    path = Path(secrets_path)
    if not path.is_file():
        return {}
    try:
        return load_secrets(str(path))
    except OSError:
        logger.debug("could not read secrets file %s", path)
        return {}


def _shanghai_day_bounds_ms(trade_date: date) -> tuple[int, int]:
    start_dt = datetime(
        trade_date.year, trade_date.month, trade_date.day, 0, 0, 0, tzinfo=_SHANGHAI_TZ
    )
    end_dt = datetime(
        trade_date.year,
        trade_date.month,
        trade_date.day,
        23,
        59,
        59,
        999_000,
        tzinfo=_SHANGHAI_TZ,
    )
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def _dedup_preserve_order(codes: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in codes:
        code = raw.strip()
        if code and code not in seen:
            seen.add(code)
            result.append(code)
    return result


class StraightFlushAdapter(SourceAdapter):
    """通过同花顺扶摇 HTTP API 拉取原始行情数据。"""

    requests_per_second = 5.0

    _ASSET_TYPES = _ASSET_TYPES

    def __init__(
        self,
        token: str | None = None,
        base_url: str = _DEFAULT_BASE_URL,
        secrets_path: str | Path = "config/.env",
        symbols: list[str] | None = None,
    ) -> None:
        self._explicit_token = token.strip() if token else ""
        self._secrets_path = secrets_path
        self._base_url = base_url.rstrip("/")
        self._symbols = _dedup_preserve_order(symbols) if symbols else None
        self._session: requests.Session | None = None

    def _ensure_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def _resolve_token(self) -> str:
        if self._explicit_token:
            return self._explicit_token
        for key in _TOKEN_ENV_KEYS:
            val = os.environ.get(key, "").strip()
            if val:
                return val
        secrets = _read_fuyao_secrets(self._secrets_path)
        for key in _TOKEN_ENV_KEYS:
            val = secrets.get(key, "").strip()
            if val:
                return val
        return ""

    def _headers(self) -> dict[str, str]:
        token = self._resolve_token()
        if not token:
            raise ValueError(
                "STRIGHT_FLUSH_TOKEN (or HITHINK_FINANCE_API_KEY) is required "
                "(set in env or config/.env, or pass token=)"
            )
        return {"X-api-key": token}

    def list_asset_types(self) -> list[str]:
        return list(self._ASSET_TYPES)

    def _request_json(
        self, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        session = self._ensure_session()
        resp = session.get(
            f"{self._base_url}{path}",
            params=params,
            headers=self._headers(),
            timeout=FETCH_TIMEOUT,
        )
        if resp.status_code in (401, 403):
            raise PermissionError(
                f"Fuyao API unauthorized/forbidden ({resp.status_code}): {resp.text}"
            )
        if resp.status_code == 429 or resp.status_code >= 500:
            resp.raise_for_status()
        if resp.status_code >= 400:
            raise ValueError(f"Fuyao API HTTP error ({resp.status_code}): {resp.text}")

        payload: dict[str, Any] = resp.json()
        code = payload.get("code")
        if code == 0:
            return payload

        msg = payload.get("message", "")
        req_id = payload.get("request_id", "")
        if code in _RETRYABLE_BUSINESS_CODES:
            raise requests.exceptions.HTTPError(
                f"Fuyao API transient error {code}: {msg} (request_id={req_id})",
                response=resp,
            )
        if code in _AUTH_BUSINESS_CODES:
            raise PermissionError(
                f"Fuyao API auth error {code}: {msg} (request_id={req_id})"
            )
        if code in _EMPTY_BUSINESS_CODES:
            logger.debug(
                "Fuyao API returned empty-data code %s for %s: %s", code, path, msg
            )
            return {
                "code": code,
                "message": msg,
                "request_id": req_id,
                "data": {"item": []},
            }
        raise ValueError(f"Fuyao API error {code}: {msg} (request_id={req_id})")

    def _resolve_thscodes(self, asset_type: str) -> list[str]:
        if self._symbols is not None:
            return list(self._symbols)

        fuyao_type = _META_ASSET_TYPE_MAP[asset_type]
        codes: list[str] = []
        offset = 0
        while True:
            payload = self._request_json(
                "/api/meta/tickers/list",
                params={
                    "asset_type": fuyao_type,
                    "limit": _TICKER_LIST_PAGE_LIMIT,
                    "offset": offset,
                },
            )
            items = (payload.get("data") or {}).get("item") or []
            for entry in items:
                thscode = entry.get("thscode")
                if thscode:
                    codes.append(str(thscode))
            if len(items) < _TICKER_LIST_PAGE_LIMIT:
                break
            offset += _TICKER_LIST_PAGE_LIMIT
        return _dedup_preserve_order(codes)

    def fetch(
        self,
        asset_type: str,
        trade_date: date,
        watermark: str | None = None,
    ) -> FetchResult:
        if asset_type not in self._ASSET_TYPES:
            raise ValueError(f"unsupported asset_type: {asset_type!r}")

        if asset_type == "calendar":
            df = self._fetch_calendar(trade_date)
        else:
            start_ms, end_ms = _shanghai_day_bounds_ms(trade_date)
            thscodes = self._resolve_thscodes(asset_type)
            if asset_type == "stock":
                df = self._fetch_batch_bars(
                    "/api/a-share/prices/historical-batch",
                    thscodes,
                    start_ms,
                    end_ms,
                    adjust="none",  # 原始未复权价格
                )
            elif asset_type == "etf":
                df = self._fetch_batch_bars(
                    "/api/fund/market/historical-batch",
                    thscodes,
                    start_ms,
                    end_ms,
                )
            elif asset_type == "index":
                df = self._fetch_batch_bars(
                    "/api/a-share-index/prices/historical-batch",
                    thscodes,
                    start_ms,
                    end_ms,
                )
            elif asset_type == "futures":
                df = self._fetch_single_daily(
                    "/api/futures/prices/daily",
                    thscodes,
                    start_ms,
                    end_ms,
                )
            elif asset_type == "option":
                df = self._fetch_single_daily(
                    "/api/options/prices/daily",
                    thscodes,
                    start_ms,
                    end_ms,
                )
            else:  # pragma: no cover
                raise ValueError(f"unsupported asset_type: {asset_type!r}")

        return FetchResult(
            df=df,
            source="straightflush",
            asset_type=asset_type,
            trade_date=trade_date,
            row_count=len(df),
            watermark=trade_date.strftime("%Y%m%d"),
        )

    def _fetch_batch_bars(
        self,
        endpoint: str,
        thscodes: list[str],
        start_ms: int,
        end_ms: int,
        adjust: str | None = None,
    ) -> pd.DataFrame:
        if not thscodes:
            return pd.DataFrame()

        rows: list[dict[str, Any]] = []
        for i in range(0, len(thscodes), _BATCH_SIZE):
            batch = thscodes[i : i + _BATCH_SIZE]
            params: dict[str, Any] = {
                "thscodes": ",".join(batch),
                "interval": "1d",
                "start": start_ms,
                "end": end_ms,
            }
            if adjust is not None:
                params["adjust"] = adjust
            payload = self._request_json(endpoint, params=params)
            data = payload.get("data") or {}
            for entry in data.get("item") or []:
                code = entry.get("thscode")
                for bar in entry.get("item") or []:
                    row = dict(bar)
                    if code and "thscode" not in row:
                        row["thscode"] = code
                    rows.append(row)

        return pd.DataFrame(rows)

    def _fetch_single_daily(
        self,
        endpoint: str,
        thscodes: list[str],
        start_ms: int,
        end_ms: int,
    ) -> pd.DataFrame:
        if not thscodes:
            return pd.DataFrame()

        rows: list[dict[str, Any]] = []
        for code in thscodes:
            payload = self._request_json(
                endpoint,
                params={
                    "thscode": code,
                    "start": start_ms,
                    "end": end_ms,
                    "time_period": "day_1",
                },
            )
            data = payload.get("data") or {}
            for bar in data.get("item") or []:
                row = dict(bar)
                if "thscode" not in row:
                    row["thscode"] = code
                rows.append(row)

        return pd.DataFrame(rows)

    def _fetch_calendar(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        payload = self._request_json(
            "/api/a-share/calendar/trading-days",
            params={"start_date": date_str, "end_date": date_str},
        )
        items = (payload.get("data") or {}).get("item") or []
        return pd.DataFrame(items)

    def health_check(self) -> bool:
        """通过轻量级代码列表接口探测鉴权与连通性。"""
        try:
            payload = self._request_json(
                "/api/meta/tickers/list",
                params={"asset_type": "a-share", "limit": 1, "offset": 0},
            )
            items = (payload.get("data") or {}).get("item") or []
            return len(items) > 0
        except Exception:
            return False

    def close(self) -> None:
        if self._session is not None:
            try:
                self._session.close()
            finally:
                self._session = None
