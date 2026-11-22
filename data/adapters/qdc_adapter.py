"""QDCData 数据源适配器（支持 ``qdc-cli`` 本地 Parquet 与在线 HTTP REST）。

默认 ``mode="cli"``，读取 ``<download_root>/QDCData/<sku>/<subtable>/*.parquet``，
不消耗在线 API 每日行数配额；传入 ``mode="http"`` 或仅提供 ``api_key=`` 时使用在线接口。

协议要点：
* CLI 模式通过 ``qdc-cli config`` 解析目录（或使用 ``local_dir=`` / ``QDC_DATA_DIR``），
  按需同步年度 Parquet 分片。
* 本地 Parquet 按 ``Asia/Shanghai`` 日界毫秒过滤 ``ts``（避免 ``TRY_CAST(ts AS DATE)`` 返回 NULL），
  对 ``string`` / ``date32`` 日期列启用 PyArrow ``filters`` 下推加速单日查询。
* HTTP 模式从环境变量或 ``config/.env`` 读取 ``QDC_API_KEY`` / ``QDC_DEVICE``，请求
  ``GET /v1/query``（``format=parquet`` + 游标分页）。
"""

from __future__ import annotations

import io
import json
import logging
import os
import shutil
import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from core.config import load_secrets
from data.source_adapter import FETCH_TIMEOUT, FetchResult, SourceAdapter, SourceDataset

logger = logging.getLogger(__name__)

__all__ = ["QdcAdapter"]

_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_DEFAULT_BASE_URL = "https://data.qdc-data.com"
_DEFAULT_CLI_BIN = "qdc-cli"
_DAY_MS = 86_400_000
_BSE_PREFIXES = ("43", "83", "87", "92")

# 平台 dataset -> (QDC sku, 默认子表路径或 None -> 'full')
_DATASET_TO_SKU: dict[str, tuple[str, str | None]] = {
    "stock": ("cn_daily_full", None),
    "stock_minute": ("stocks_1m", None),
    "fundamental": ("cn_valuation_1d", None),
    "daily_status": ("cn_limit_flag", "limits/full"),
    "corporate_actions": ("cn_dividend", None),
    "etf": ("cn_etf_1d", None),
    "index": ("cn_index_1d", None),
    "convertible": ("cn_cb_1d", None),
    "futures": ("cn_futures_1d", "bars"),
    "option": ("cn_opt_etf_1d", None),
}


def _read_qdc_secrets(secrets_path: str | Path) -> dict[str, str]:
    path = Path(secrets_path)
    if not path.is_file():
        return {}
    try:
        return load_secrets(str(path))
    except OSError:
        logger.debug("could not read secrets file %s", path)
        return {}


def _beijing_day_ms_bounds(trade_date: date) -> tuple[int, int]:
    start_dt = datetime(
        trade_date.year, trade_date.month, trade_date.day, tzinfo=_SHANGHAI_TZ
    )
    start_ms = int(start_dt.timestamp() * 1000)
    return start_ms, start_ms + _DAY_MS


def _normalize_qdc_root(root: Path) -> Path:
    """兼容传入 ``<download_root>`` 或 ``QDCData/`` 本身。"""
    if root.name == "QDCData":
        return root
    candidate = root / "QDCData"
    if candidate.is_dir():
        return candidate
    return root


def _canonicalize_qdc_code(value: str, *, fix_bse_xshe: bool = False) -> str:
    val = str(value).strip()
    if fix_bse_xshe and val.endswith(".XSHE") and val[:2] in _BSE_PREFIXES:
        return val[: -len(".XSHE")] + ".BJ"
    if val.endswith(".XSHG"):
        return val[: -len(".XSHG")] + ".SH"
    if val.endswith(".XSHE"):
        return val[: -len(".XSHE")] + ".SZ"
    if val.endswith(".XBSE"):
        return val[: -len(".XBSE")] + ".BJ"
    return val


class QdcAdapter(SourceAdapter):
    """通过 ``qdc-cli`` 或 HTTP API 拉取 QDCData 原始行情与基础状态。"""

    requests_per_second = 5.0

    _SUPPORTED_DATASETS = list(_DATASET_TO_SKU)

    def __init__(
        self,
        mode: Literal["cli", "http"] | None = None,
        cli_bin: str = _DEFAULT_CLI_BIN,
        auto_sync: bool = True,
        auto_update: bool = False,
        local_dir: str | Path | None = None,
        api_key: str | None = None,
        device: str | None = None,
        base_url: str = _DEFAULT_BASE_URL,
        secrets_path: str | Path = "config/.env",
        symbols: list[str] | None = None,
    ) -> None:
        self._explicit_api_key = api_key.strip() if api_key else ""
        self._explicit_device = device.strip() if device else ""
        self._secrets_path = secrets_path
        self._base_url = base_url.rstrip("/")

        env_local_dir = os.environ.get("QDC_DATA_DIR", "").strip()
        chosen_local = local_dir if local_dir is not None else (env_local_dir or None)
        self._local_dir = Path(chosen_local) if chosen_local is not None else None

        if mode is None:
            resolved_mode: Literal["cli", "http"] = (
                "http"
                if (self._explicit_api_key and self._local_dir is None)
                else "cli"
            )
        elif mode in ("cli", "http"):
            resolved_mode = mode
        else:
            raise ValueError(
                f"unsupported QDC mode: {mode!r}; expected 'cli' or 'http'"
            )

        self._mode: Literal["cli", "http"] = resolved_mode
        self._cli_bin = cli_bin
        self._auto_sync = auto_sync
        self._auto_update = auto_update
        self._symbols = list(symbols) if symbols else None
        self._session: requests.Session | None = None
        self._synced_keys: set[tuple[str, int]] = set()
        self._cli_updated = False

    # ── 公共接口 ──────────────────────────────────────────────────────────
    def list_datasets(self) -> list[str]:
        return list(self._SUPPORTED_DATASETS)

    def fetch(
        self,
        dataset: SourceDataset | str,
        trade_date: date,
        watermark: str | None = None,
    ) -> FetchResult:
        if dataset not in _DATASET_TO_SKU:
            raise ValueError(f"unsupported dataset: {dataset!r}")

        if self._mode == "cli":
            df = self._fetch_cli(dataset, trade_date)
        else:
            df = self._fetch_http(dataset, trade_date)

        date_str = trade_date.strftime("%Y-%m-%d")
        return FetchResult(
            df=df,
            source="qdc",
            dataset=dataset,
            trade_date=trade_date,
            row_count=len(df),
            watermark=date_str,
        )

    def health_check(self) -> bool:
        """CLI 模式检查本地目录或 ``qdc-cli whoami``，HTTP 模式检查 ``/v1/datasets``。"""
        if self._mode == "cli":
            return self._health_check_cli()
        return self._health_check_http()

    def close(self) -> None:
        if self._session is not None:
            try:
                self._session.close()
            finally:
                self._session = None

    # ── CLI 与本地 Parquet 模式 ───────────────────────────────────────────
    def _resolve_cli_binary(self) -> str:
        bin_path = Path(self._cli_bin)
        if bin_path.is_file():
            return str(bin_path)
        found = shutil.which(self._cli_bin)
        if found:
            return found
        raise FileNotFoundError(
            f"QDC CLI binary not found: {self._cli_bin!r}. "
            "Install qdc-cli (https://qdc-data.com/downloads.html) or pass local_dir= / mode='http'."
        )

    def _run_cli_json(self, args: list[str]) -> dict[str, Any]:
        cli_path = self._resolve_cli_binary()
        cmd = [cli_path, *args, "--json", "--quiet"]
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=FETCH_TIMEOUT,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"qdc-cli command timed out after {FETCH_TIMEOUT}s: {' '.join(cmd)}"
            ) from exc

        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        payload: dict[str, Any] = {}
        if stdout:
            try:
                parsed = json.loads(stdout)
                if isinstance(parsed, dict):
                    payload = parsed
            except json.JSONDecodeError:
                payload = {}

        if proc.returncode != 0:
            err_msg = str(
                payload.get("error")
                or payload.get("message")
                or stderr
                or stdout
                or f"exit code {proc.returncode}"
            )
            low = err_msg.lower()
            if any(
                tok in low
                for tok in (
                    "401",
                    "403",
                    "login",
                    "unauth",
                    "entitle",
                    "permission",
                    "未登录",
                    "未购买",
                    "权限",
                )
            ):
                raise PermissionError(
                    f"qdc-cli authentication/entitlement failed: {err_msg}"
                )
            if any(tok in low for tok in ("404", "unknown", "不存在", "缺少参数")):
                raise ValueError(
                    f"qdc-cli rejected command {' '.join(args)}: {err_msg}"
                )
            raise RuntimeError(f"qdc-cli failed ({proc.returncode}): {err_msg}")

        return payload

    def _resolve_cli_data_root(self) -> Path:
        if self._local_dir is not None:
            return _normalize_qdc_root(self._local_dir)

        cfg = self._run_cli_json(["config"])
        download_root = str(cfg.get("download_root", "")).strip()
        if not download_root:
            raise ValueError(
                "qdc-cli download_root is not configured. "
                "Run 'qdc-cli config --dir <path>' first, or set QDC_DATA_DIR / pass local_dir=."
            )
        root = Path(download_root)
        if root.name == "QDCData":
            return root
        return root / "QDCData"

    @staticmethod
    def _has_year_or_full_parquet(dataset_dir: Path, year: int) -> bool:
        if not dataset_dir.is_dir():
            return False
        if (dataset_dir / "all.parquet").is_file():
            return True
        return any(dataset_dir.glob(f"{year}*.parquet"))

    def _maybe_sync_cli(
        self, data_root: Path, sku: str, subtable: str, trade_date: date
    ) -> None:
        # 显式指定 local_dir / QDC_DATA_DIR 时视作纯本地归档目录，不触发 CLI 下载
        if self._local_dir is not None:
            return

        if self._auto_update and not self._cli_updated:
            self._run_cli_json(["update"])
            self._cli_updated = True

        if not self._auto_sync:
            return

        dataset_dir = data_root / sku / subtable
        key = (sku, trade_date.year)
        if self._has_year_or_full_parquet(dataset_dir, trade_date.year):
            return
        if key in self._synced_keys:
            return

        self._run_cli_json(["get", sku, "--years", str(trade_date.year)])
        self._synced_keys.add(key)

    @staticmethod
    def _select_cli_files(dataset_dir: Path, trade_date: date) -> list[Path]:
        """优先定位单日/单月/单年切片 Parquet，避免扫描多余分片文件。"""
        day_file = dataset_dir / f"{trade_date.strftime('%Y-%m-%d')}.parquet"
        if day_file.is_file():
            return [day_file]
        month_file = dataset_dir / f"{trade_date.strftime('%Y-%m')}.parquet"
        if month_file.is_file():
            return [month_file]
        year_file = dataset_dir / f"{trade_date.year}.parquet"
        if year_file.is_file():
            return [year_file]
        year_files = sorted(dataset_dir.glob(f"{trade_date.year}*.parquet"))
        if year_files:
            return year_files
        return sorted(dataset_dir.glob("*.parquet"))

    def _read_cli_date_slice(self, dataset_dir: Path, trade_date: date) -> pd.DataFrame:
        files = self._select_cli_files(dataset_dir, trade_date)
        if not files:
            return pd.DataFrame()

        start_ms, end_ms = _beijing_day_ms_bounds(trade_date)
        date_iso = trade_date.strftime("%Y-%m-%d")
        frames: list[pd.DataFrame] = []

        for path in files:
            schema = pq.read_schema(path)
            schema_names = set(schema.names)
            if "ts" in schema_names:
                filters: list[tuple[str, str, Any]] = [
                    ("ts", ">=", start_ms),
                    ("ts", "<", end_ms),
                ]
                df = pd.read_parquet(path, filters=filters)
            else:
                date_col = next(
                    (
                        c
                        for c in ("trade_date", "trade_day", "d", "date")
                        if c in schema_names
                    ),
                    None,
                )
                if date_col is not None:
                    field_type = schema.field(date_col).type
                    if pa.types.is_string(field_type) or pa.types.is_large_string(
                        field_type
                    ):
                        df = pd.read_parquet(path, filters=[(date_col, "==", date_iso)])
                    elif pa.types.is_date(field_type):
                        df = pd.read_parquet(
                            path, filters=[(date_col, "==", trade_date)]
                        )
                    else:
                        df = pd.read_parquet(path)
                        parsed = pd.to_datetime(
                            df[date_col], errors="coerce"
                        ).dt.strftime("%Y-%m-%d")
                        df = df[parsed == date_iso]
                else:
                    df = pd.read_parquet(path)

            if len(df) > 0:
                if self._symbols:
                    for sym_col in ("symbol", "ts_code", "code"):
                        if sym_col in df.columns:
                            df = df[df[sym_col].isin(self._symbols)]
                            break
                if len(df) > 0:
                    frames.append(df)

        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()

    def _fetch_cli_corporate_actions(
        self, data_root: Path, trade_date: date
    ) -> pd.DataFrame:
        self._maybe_sync_cli(data_root, "cn_dividend", "full", trade_date)
        dataset_dir = data_root / "cn_dividend" / "full"
        if not dataset_dir.is_dir():
            raise FileNotFoundError(
                f"QDC local dataset directory not found: {dataset_dir}"
            )
        all_file = dataset_dir / "all.parquet"
        files = (
            [all_file] if all_file.is_file() else sorted(dataset_dir.glob("*.parquet"))
        )
        if not files:
            return pd.DataFrame()
        df = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
        if df.empty:
            return df
        if "progress" in df.columns:
            df = df[df["progress"].astype(str) == "实施分配"].copy()
        if "ex_date" in df.columns:
            ex_parsed = pd.to_datetime(df["ex_date"], errors="coerce").dt.date
            df = df[ex_parsed.notna() & (ex_parsed <= trade_date)].copy()
        if self._symbols:
            for sym_col in ("symbol", "ts_code", "code"):
                if sym_col in df.columns:
                    df = df[df[sym_col].isin(self._symbols)].copy()
                    break
        return df.reset_index(drop=True)

    def _fetch_cli_daily_status(
        self, data_root: Path, trade_date: date
    ) -> pd.DataFrame:
        self._maybe_sync_cli(data_root, "cn_limit_flag", "limits/full", trade_date)
        limits_dir = data_root / "cn_limit_flag" / "limits" / "full"
        status_dir = data_root / "cn_limit_flag" / "status" / "full"
        if not limits_dir.is_dir() and not status_dir.is_dir():
            raise FileNotFoundError(
                f"QDC local dataset directory not found: {limits_dir}"
            )

        lim_df = (
            self._read_cli_date_slice(limits_dir, trade_date)
            if limits_dir.is_dir()
            else pd.DataFrame()
        )
        stat_df = (
            self._read_cli_date_slice(status_dir, trade_date)
            if status_dir.is_dir()
            else pd.DataFrame()
        )

        if lim_df.empty and stat_df.empty:
            return pd.DataFrame()

        if not lim_df.empty and not stat_df.empty:
            join_cols = [
                c
                for c in ("date", "symbol")
                if c in lim_df.columns and c in stat_df.columns
            ]
            if not join_cols:
                join_cols = ["symbol"]
            extra_stat_cols = [
                c for c in stat_df.columns if c in join_cols or c not in lim_df.columns
            ]
            out = lim_df.merge(
                stat_df[extra_stat_cols].drop_duplicates(join_cols),
                on=join_cols,
                how="outer",
            )
        elif not lim_df.empty:
            out = lim_df.copy()
        else:
            out = stat_df.copy()

        norm_syms = (
            out["symbol"]
            .astype(str)
            .map(lambda v: _canonicalize_qdc_code(v, fix_bse_xshe=True))
        )

        st_set: set[str] = set()
        st_path = data_root / "cn_st" / "full" / "all.parquet"
        if st_path.is_file():
            st_df = pd.read_parquet(st_path)
            if not st_df.empty and "st_start" in st_df.columns:
                st_start = pd.to_datetime(st_df["st_start"], errors="coerce").dt.date
                st_end = (
                    pd.to_datetime(st_df["st_end"], errors="coerce").dt.date
                    if "st_end" in st_df.columns
                    else pd.Series(pd.NaT, index=st_df.index)
                )
                active = (st_start <= trade_date) & (
                    st_end.isna() | (st_end >= trade_date)
                )
                code_col = "ts_code" if "ts_code" in st_df.columns else "symbol"
                if code_col in st_df.columns:
                    st_set = {
                        _canonicalize_qdc_code(str(c), fix_bse_xshe=True)
                        for c in st_df.loc[active, code_col].dropna()
                    }
        out["is_st"] = norm_syms.isin(st_set)

        xr_set: set[str] = set()
        div_path = data_root / "cn_dividend" / "full" / "all.parquet"
        if div_path.is_file():
            div_df = pd.read_parquet(div_path)
            if not div_df.empty and "ex_date" in div_df.columns:
                if "progress" in div_df.columns:
                    div_df = div_df[div_df["progress"].astype(str) == "实施分配"]
                ex_dt = pd.to_datetime(div_df["ex_date"], errors="coerce").dt.date
                sym_col = "symbol" if "symbol" in div_df.columns else "ts_code"
                if sym_col in div_df.columns:
                    xr_set = {
                        _canonicalize_qdc_code(str(c), fix_bse_xshe=True)
                        for c in div_df.loc[ex_dt == trade_date, sym_col].dropna()
                    }
        out["is_xr"] = norm_syms.isin(xr_set)
        return out

    def _fetch_cli(self, dataset: str, trade_date: date) -> pd.DataFrame:
        data_root = self._resolve_cli_data_root()
        if dataset == "corporate_actions":
            return self._fetch_cli_corporate_actions(data_root, trade_date)
        if dataset == "daily_status":
            return self._fetch_cli_daily_status(data_root, trade_date)

        sku, table = _DATASET_TO_SKU[dataset]
        subtable = table or "full"
        self._maybe_sync_cli(data_root, sku, subtable, trade_date)

        dataset_dir = data_root / sku / subtable
        if not dataset_dir.is_dir():
            raise FileNotFoundError(
                f"QDC local dataset directory not found: {dataset_dir}"
            )
        return self._read_cli_date_slice(dataset_dir, trade_date)

    def _health_check_cli(self) -> bool:
        if self._local_dir is not None:
            root = _normalize_qdc_root(self._local_dir)
            return root.is_dir() and any(root.glob("**/*.parquet"))
        try:
            cfg = self._run_cli_json(["config"])
            if not str(cfg.get("download_root", "")).strip():
                return False
            whoami = self._run_cli_json(["whoami"])
            return bool(whoami.get("email") or whoami.get("entitlement"))
        except Exception:
            return False

    # ── 在线 HTTP REST 模式 ───────────────────────────────────────────────
    def _ensure_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def _resolve_credentials(self) -> tuple[str, str]:
        api_key = self._explicit_api_key or os.environ.get("QDC_API_KEY", "").strip()
        device = self._explicit_device or os.environ.get("QDC_DEVICE", "").strip()
        if not api_key or not device:
            secrets = _read_qdc_secrets(self._secrets_path)
            if not api_key:
                api_key = secrets.get("QDC_API_KEY", "").strip()
            if not device:
                device = secrets.get("QDC_DEVICE", "").strip()
        return api_key, device

    def _headers(self) -> dict[str, str]:
        api_key, device = self._resolve_credentials()
        if not api_key:
            raise ValueError(
                "QDC_API_KEY is required for QDC HTTP API "
                "(set QDC_API_KEY in env or config/.env, or pass api_key=)"
            )
        headers = {"X-API-Key": api_key}
        if device:
            headers["X-Device"] = device
        return headers

    def _fetch_http(self, dataset: str, trade_date: date) -> pd.DataFrame:
        sku, table = _DATASET_TO_SKU[dataset]
        date_str = trade_date.strftime("%Y-%m-%d")
        params: dict[str, Any] = {
            "sku": sku,
            "start": date_str,
            "end": date_str,
            "format": "parquet",
        }
        if table:
            params["table"] = table.split("/")[0]
        if self._symbols:
            params["code"] = ",".join(self._symbols)

        session = self._ensure_session()
        headers = self._headers()
        frames: list[pd.DataFrame] = []
        seen_cursors: set[str] = set()

        while True:
            resp = session.get(
                f"{self._base_url}/v1/query",
                params=params,
                headers=headers,
                timeout=FETCH_TIMEOUT,
            )
            self._handle_error_status(resp)

            frame = self._parse_query_response(resp)
            if len(frame) > 0:
                frames.append(frame)

            truncated = resp.headers.get("X-QDC-Truncated", "").strip().lower()
            if truncated != "true":
                break
            next_cursor = resp.headers.get("X-QDC-Next-Cursor", "").strip()
            if not next_cursor:
                break
            if next_cursor in seen_cursors:
                raise RuntimeError(
                    f"QDC pagination loop detected on cursor {next_cursor!r}"
                )
            seen_cursors.add(next_cursor)
            params["cursor"] = next_cursor

        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()

    @staticmethod
    def _handle_error_status(resp: requests.Response) -> None:
        status = resp.status_code
        if status < 400:
            return
        if status in (401, 403):
            raise PermissionError(
                f"QDC API authentication/entitlement failed ({status}): {resp.text}"
            )
        if status in (400, 404, 422):
            raise ValueError(f"QDC API rejected request ({status}): {resp.text}")
        # 429 与 5xx 抛出 HTTPError 交由 SourceAdapter 重试
        resp.raise_for_status()

    @staticmethod
    def _parse_query_response(resp: requests.Response) -> pd.DataFrame:
        if not resp.content:
            return pd.DataFrame()
        content_type = resp.headers.get("Content-Type", "").lower()
        if "json" in content_type:
            payload = resp.json()
            if isinstance(payload, dict):
                rows = (
                    payload.get("data")
                    or payload.get("rows")
                    or payload.get("item")
                    or []
                )
                return pd.DataFrame(rows)
            if isinstance(payload, list):
                return pd.DataFrame(payload)
            return pd.DataFrame()
        return pd.read_parquet(io.BytesIO(resp.content))

    def _health_check_http(self) -> bool:
        try:
            headers = self._headers()
            session = self._ensure_session()
            resp = session.get(
                f"{self._base_url}/v1/datasets",
                headers=headers,
                timeout=FETCH_TIMEOUT,
            )
            if resp.status_code != 200:
                return False
            payload = resp.json()
            if isinstance(payload, dict):
                datasets = payload.get("datasets", payload.get("data", payload))
                return bool(datasets)
            return isinstance(payload, list) and len(payload) > 0
        except Exception:
            return False
