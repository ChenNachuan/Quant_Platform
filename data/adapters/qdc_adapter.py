"""QDCData source adapter — multi-asset market data via ``qdc-cli`` or HTTP REST.

By default ``QdcAdapter`` operates in **CLI mode** (``mode="cli"``), using the
local ``qdc-cli`` downloader and Parquet archives under
``<download_root>/QDCData/<sku>/<subtable>/*.parquet`` without consuming
online API daily row quotas.  Online HTTP REST (``https://data.qdc-data.com``)
remains available when ``mode="http"`` or when ``api_key=`` is passed directly
without ``local_dir=``.

Key protocol rules from ``docs/data_source/QDCData/``:

* CLI mode discovers ``download_root`` lazily via ``qdc-cli config --json --quiet``
  (or uses ``local_dir=`` / ``QDC_DATA_DIR``), optionally runs
  ``qdc-cli get <sku> --years <YYYY> --json --quiet`` when the target year's
  Parquet shards are not yet on disk, and probes ``qdc-cli whoami`` in
  :meth:`health_check`.
* Local Parquet reads filter ``ts`` (Unix ms) using ``Asia/Shanghai`` day
  bounds — never ``TRY_CAST(ts AS DATE)`` which silently yields NULL in DuckDB.
* HTTP mode authenticates via ``X-API-Key`` (``QDC_API_KEY``) and optional
  ``X-Device`` (``QDC_DEVICE``), resolved lazily from env or ``config/.env``,
  and queries ``GET /v1/query`` with ``format=parquet`` and cursor pagination.
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
import pyarrow.parquet as pq
import requests

from core.config import load_secrets
from data.source_adapter import FETCH_TIMEOUT, FetchResult, SourceAdapter

logger = logging.getLogger(__name__)

__all__ = ["QdcAdapter"]

_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_DEFAULT_BASE_URL = "https://data.qdc-data.com"
_DEFAULT_CLI_BIN = "qdc-cli"
_DAY_MS = 86_400_000

# Platform asset_type -> (QDC sku, default subtable or None)
_ASSET_TO_SKU: dict[str, tuple[str, str | None]] = {
    "stock": ("cn_daily_full", None),
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
    """Resolve either ``<download_root>`` (containing ``QDCData/``) or ``QDCData/`` itself."""
    if root.name == "QDCData":
        return root
    candidate = root / "QDCData"
    if candidate.is_dir():
        return candidate
    return root


class QdcAdapter(SourceAdapter):
    """Fetch source-raw market data from QDCData via ``qdc-cli`` or HTTP API."""

    requests_per_second = 5.0

    _ASSET_TYPES = list(_ASSET_TO_SKU)

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

    # ------------------------------------------------------------------
    # Public SourceAdapter API
    # ------------------------------------------------------------------
    def list_asset_types(self) -> list[str]:
        return list(self._ASSET_TYPES)

    def fetch(
        self,
        asset_type: str,
        trade_date: date,
        watermark: str | None = None,
    ) -> FetchResult:
        if asset_type not in _ASSET_TO_SKU:
            raise ValueError(f"unsupported asset_type: {asset_type!r}")

        if self._mode == "cli":
            df = self._fetch_cli(asset_type, trade_date)
        else:
            df = self._fetch_http(asset_type, trade_date)

        date_str = trade_date.strftime("%Y-%m-%d")
        return FetchResult(
            df=df,
            source="qdc",
            asset_type=asset_type,
            trade_date=trade_date,
            row_count=len(df),
            watermark=date_str,
        )

    def health_check(self) -> bool:
        """Probe ``qdc-cli`` (config + whoami) / local directory, or HTTP ``/v1/datasets``."""
        if self._mode == "cli":
            return self._health_check_cli()
        return self._health_check_http()

    def close(self) -> None:
        if self._session is not None:
            try:
                self._session.close()
            finally:
                self._session = None

    # ------------------------------------------------------------------
    # CLI + Local Parquet mode
    # ------------------------------------------------------------------
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
        # Explicit local_dir / QDC_DATA_DIR acts as a direct local archive path
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

    def _fetch_cli(self, asset_type: str, trade_date: date) -> pd.DataFrame:
        data_root = self._resolve_cli_data_root()
        sku, table = _ASSET_TO_SKU[asset_type]
        subtable = table or "full"

        self._maybe_sync_cli(data_root, sku, subtable, trade_date)

        dataset_dir = data_root / sku / subtable
        if not dataset_dir.is_dir():
            raise FileNotFoundError(
                f"QDC local dataset directory not found: {dataset_dir}"
            )

        year_files = sorted(dataset_dir.glob(f"{trade_date.year}*.parquet"))
        files = year_files if year_files else sorted(dataset_dir.glob("*.parquet"))
        if not files:
            return pd.DataFrame()

        start_ms, end_ms = _beijing_day_ms_bounds(trade_date)
        date_iso = trade_date.strftime("%Y-%m-%d")
        frames: list[pd.DataFrame] = []

        for path in files:
            schema_names = set(pq.read_schema(path).names)
            if "ts" in schema_names:
                filters: list[tuple[str, str, Any]] = [
                    ("ts", ">=", start_ms),
                    ("ts", "<", end_ms),
                ]
                df = pd.read_parquet(path, filters=filters)
            else:
                df = pd.read_parquet(path)
                for col in ("trade_date", "trade_day", "d", "date"):
                    if col in df.columns:
                        parsed = pd.to_datetime(df[col], errors="coerce").dt.strftime(
                            "%Y-%m-%d"
                        )
                        df = df[parsed == date_iso]
                        break
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

    # ------------------------------------------------------------------
    # Online HTTP REST mode (optional: mode="http" or explicit api_key=)
    # ------------------------------------------------------------------
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

    def _fetch_http(self, asset_type: str, trade_date: date) -> pd.DataFrame:
        sku, table = _ASSET_TO_SKU[asset_type]
        date_str = trade_date.strftime("%Y-%m-%d")
        params: dict[str, Any] = {
            "sku": sku,
            "start": date_str,
            "end": date_str,
            "format": "parquet",
        }
        if table:
            params["table"] = table
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
        # 429 and 5xx trigger SourceAdapter retry via HTTPError
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
