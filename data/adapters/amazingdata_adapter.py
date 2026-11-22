"""AmazingData 数据源适配器（A 股日线/分钟/基础/除权/状态、ETF、指数、交易日历与期权）。

支持两种执行模式：
1. 容器内 / Linux x64 原生 SDK 模式（本地已安装 ``AmazingData`` 与 ``tgw`` 时）。
2. 宿主机默认 Docker 桥接模式（macOS 等未安装原生 ``AmazingData`` 时）：通过
   ``subprocess`` 调用 ``docker compose run --rm amazingdata python3 /scripts/ingestion/amazing_pull.py ...``
   利用 ``docker-compose.yml`` 的目录挂载实时拉取并写回 ``raw/amazingdata/``，随后读取返回。

SDK 约束：
* ``get_option_basic_info`` 单次返回上限约 100–104 行，按每批 10 个代码分批查询。
* ``local_path`` 必须为 ``str``（SDK 内部会做字符串拼接）；默认缓存目录位于
  ``raw/amazingdata/_cache``。
* ``close()`` 调用 ``ad.logout()`` 释放有限的连接槽位，限速 ``requests_per_second = 2.0``。
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from datetime import date
from pathlib import Path
from typing import Any, Literal

import pandas as pd

import core.config as core_config
from data.raw import raw_path
from data.source_adapter import FetchResult, SourceAdapter, SourceDataset

logger = logging.getLogger(__name__)

__all__ = ["AmazingdataAdapter"]

_BATCH_SIZE = 10  # get_option_basic_info 单次代码批量上限
_STOCK_BATCH_SIZE = 200  # 股票扩展信息 / 分钟线批量上限
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_COMPOSE_FILE = _REPO_ROOT / "docker-compose.yml"
_DEFAULT_CONTAINER_SCRIPT = "/scripts/ingestion/amazing_pull.py"
_DEFAULT_CONTAINER_DATA_ROOT = "/data"
_DEFAULT_DOCKER_TIMEOUT = 600

_DATASET_TO_TABLE: dict[str, str] = {
    "calendar": "cn_calendar",
    "stock_basic": "cn_stock_basic",
    "stock": "cn_stock_bars_daily",
    "stock_minute": "cn_stock_bars_minute",
    "daily_status": "cn_stock_daily_status",
    "corporate_actions": "cn_stock_corporate_actions",
    "etf": "cn_etf_bars_daily",
    "index": "cn_stock_index_bars_daily",
    "option": "option_basic",
}


class AmazingdataAdapter(SourceAdapter):
    """通过 AmazingData SDK 或挂载容器脚本拉取行情与基础数据。"""

    requests_per_second = 2.0  # 受限于并发连接数

    _SUPPORTED_DATASETS = [
        "stock",
        "stock_minute",
        "stock_basic",
        "daily_status",
        "corporate_actions",
        "etf",
        "index",
        "calendar",
        "option",
    ]

    def __init__(
        self,
        local_path: str | Path | None = None,
        *,
        mode: Literal["auto", "sdk", "docker"] = "auto",
        data_root: str | Path | None = None,
        compose_file: str | Path | None = None,
        service: str = "amazingdata",
        docker_bin: str = "docker",
        timeout: int = _DEFAULT_DOCKER_TIMEOUT,
    ) -> None:
        if mode not in ("auto", "sdk", "docker"):
            raise ValueError(f"unsupported mode: {mode!r}")
        self._explicit_local_path = str(local_path) if local_path is not None else None
        self._data_root = Path(data_root) if data_root is not None else None
        self._compose_file = (
            Path(compose_file) if compose_file is not None else _DEFAULT_COMPOSE_FILE
        )
        self._service = service
        self._docker_bin = docker_bin
        self._timeout = int(timeout)
        self._mode: Literal["auto", "sdk", "docker"] = mode
        # 兼容直调 SDK 测试路径：SDK 要求 local_path 必须为 str
        self._local_path = (
            self._explicit_local_path
            if self._explicit_local_path is not None
            else f"{self._resolve_data_root() / 'raw' / 'amazingdata' / '_cache'}/"
        )
        self._ad = None

    def _resolve_data_root(self) -> Path:
        if self._data_root is not None:
            return self._data_root
        return core_config.get_data_root()

    def _can_import_sdk(self) -> bool:
        try:
            import AmazingData  # type: ignore[import-untyped]  # noqa: F401

            return True
        except ImportError:
            return False

    def _should_use_docker(self) -> bool:
        if self._ad is not None or self._mode == "sdk":
            return False
        if self._mode == "docker":
            return True
        return not self._can_import_sdk()

    def _ensure_client(self):
        if self._ad is None:
            from AmazingData import AmazingData  # type: ignore[import-untyped]

            self._ad = AmazingData()
        return self._ad

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

        if self._should_use_docker():
            return self._fetch_via_docker(dataset, trade_date)

        ad = self._ensure_client()
        date_str = trade_date.strftime("%Y-%m-%d")

        if dataset == "stock":
            df = self._fetch_stock(ad, date_str)
        elif dataset == "stock_basic":
            df = self._fetch_stock_basic(ad)
        elif dataset == "daily_status":
            df = self._fetch_daily_status(ad, trade_date)
        elif dataset == "corporate_actions":
            df = self._fetch_corporate_actions(ad, trade_date)
        elif dataset == "option":
            df = self._fetch_option(ad, date_str)
        elif dataset in ("stock_minute", "etf", "index", "calendar"):
            raise ValueError(
                f"direct SDK fetch for {dataset!r} requires docker bridge mode"
            )
        else:  # pragma: no cover
            raise ValueError(f"unsupported dataset: {dataset!r}")

        if df is None:
            df = pd.DataFrame()

        return FetchResult(
            df=df,
            source="amazingdata",
            dataset=dataset,
            trade_date=trade_date,
            row_count=len(df),
            watermark=date_str,
        )

    def _build_docker_command(
        self,
        table: str,
        date_str: str,
        data_root: Path,
    ) -> list[str]:
        cmd = [self._docker_bin, "compose"]
        if self._compose_file.is_file():
            cmd.extend(["-f", str(self._compose_file)])
        cmd.extend(["run", "--rm", "--remove-orphans"])
        default_root = Path(core_config.DATA_ROOT).resolve()
        if data_root.resolve() != default_root:
            cmd.extend(["-v", f"{data_root.resolve()}:{_DEFAULT_CONTAINER_DATA_ROOT}"])
        cmd.extend(
            [
                self._service,
                "python3",
                _DEFAULT_CONTAINER_SCRIPT,
                "--tables",
                table,
                "--date",
                date_str,
                "--data-root",
                _DEFAULT_CONTAINER_DATA_ROOT,
                "--force",
            ]
        )
        if self._explicit_local_path is not None:
            cmd.extend(["--local-path", self._explicit_local_path])
        return cmd

    def _fetch_via_docker(self, dataset: str, trade_date: date) -> FetchResult:
        table = _DATASET_TO_TABLE[dataset]
        date_str = trade_date.strftime("%Y-%m-%d")
        data_root = self._resolve_data_root()
        cmd = self._build_docker_command(table, date_str, data_root)
        cwd = str(
            self._compose_file.parent if self._compose_file.is_file() else _REPO_ROOT
        )
        logger.info("invoking amazingdata container bridge: %s", " ".join(cmd))
        try:
            proc = subprocess.run(
                cmd,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=self._timeout,
                check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"docker executable not found ({self._docker_bin!r}); "
                "cannot invoke amazingdata container"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"amazingdata container timed out after {self._timeout}s for "
                f"{table} {date_str}"
            ) from exc

        if proc.returncode != 0:
            err_detail = (proc.stderr or proc.stdout or "").strip()
            raise RuntimeError(
                f"amazingdata container pull failed (exit={proc.returncode}) for "
                f"{table} {date_str}: {err_detail}"
            )

        out_path = raw_path("amazingdata", table, trade_date, data_root)
        if not out_path.is_file():
            raise FileNotFoundError(
                f"amazingdata container succeeded but expected raw file is missing: "
                f"{out_path}"
            )
        df = pd.read_parquet(out_path)
        return FetchResult(
            df=df,
            source="amazingdata",
            dataset=dataset,
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

    def _fetch_stock_basic(self, ad) -> pd.DataFrame:
        """直调 SDK 拉取 A 股基础档案及北交所新老代码映射。"""
        base_data = ad.get_base_data()
        info_data = ad.get_info_data()
        all_codes = list(base_data.get_code_list(security_type="EXTRA_STOCK_A"))
        if not all_codes:
            return pd.DataFrame()
        frames: list[pd.DataFrame] = []
        for i in range(0, len(all_codes), _STOCK_BATCH_SIZE):
            part = info_data.get_stock_basic(all_codes[i : i + _STOCK_BATCH_SIZE])
            if isinstance(part, pd.DataFrame) and not part.empty:
                frames.append(part)
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True).drop_duplicates(
            subset=["MARKET_CODE"]
        )
        bj_df = info_data.get_bj_code_mapping(
            local_path=self._local_path, is_local=False
        )
        if isinstance(bj_df, pd.DataFrame) and not bj_df.empty:
            bj_work = bj_df.copy()
            bj_work["MARKET_CODE"] = (
                bj_work["NEW_CODE"]
                .astype(str)
                .str.strip()
                .map(lambda c: c if "." in c else f"{c}.BJ")
            )
            bj_work["OLD_CODE"] = (
                bj_work["OLD_CODE"]
                .astype(str)
                .str.strip()
                .map(lambda c: None if not c or c.lower() == "nan" else f"{c}.BJ")
            )
            df = df.merge(
                bj_work[["MARKET_CODE", "OLD_CODE"]].drop_duplicates(["MARKET_CODE"]),
                on="MARKET_CODE",
                how="left",
            )
        return df.reset_index(drop=True)

    def _fetch_daily_status(self, ad, trade_date: date) -> pd.DataFrame:
        """直调 SDK 拉取单日涨跌停与 ST/停牌/除权状态。"""
        base_data = ad.get_base_data()
        info_data = ad.get_info_data()
        all_codes = list(base_data.get_code_list(security_type="EXTRA_STOCK_A"))
        if not all_codes:
            return pd.DataFrame()
        ymd = int(trade_date.strftime("%Y%m%d"))
        frames: list[pd.DataFrame] = []
        for i in range(0, len(all_codes), _STOCK_BATCH_SIZE):
            res: Any = info_data.get_history_stock_status(
                all_codes[i : i + _STOCK_BATCH_SIZE],
                local_path=self._local_path,
                is_local=False,
                begin_date=ymd,
                end_date=ymd,
            )
            if isinstance(res, dict):
                for code, part in res.items():
                    if isinstance(part, pd.DataFrame) and not part.empty:
                        item = part.copy()
                        if "MARKET_CODE" not in item.columns:
                            item["MARKET_CODE"] = str(code)
                        frames.append(item)
            elif isinstance(res, pd.DataFrame) and not res.empty:
                frames.append(res)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _fetch_corporate_actions(self, ad, trade_date: date) -> pd.DataFrame:
        """直调 SDK 拉取全市场已实施分红与配股事件。"""
        base_data = ad.get_base_data()
        info_data = ad.get_info_data()
        all_codes = list(base_data.get_code_list(security_type="EXTRA_STOCK_A"))
        if not all_codes:
            return pd.DataFrame()
        end_ymd = trade_date.strftime("%Y%m%d")
        parts: list[pd.DataFrame] = []
        for i in range(0, len(all_codes), _STOCK_BATCH_SIZE):
            batch = all_codes[i : i + _STOCK_BATCH_SIZE]
            dv = info_data.get_dividend(
                batch, local_path=self._local_path, is_local=False
            )
            if isinstance(dv, pd.DataFrame) and not dv.empty:
                sub = dv.copy()
                if "DIV_PROGRESS" in sub.columns:
                    sub = sub[sub["DIV_PROGRESS"].astype(str).str.strip() == "3"]
                if "DATE_EX" in sub.columns:
                    ex = sub["DATE_EX"].astype(str).str.strip()
                    sub = sub[ex.str.match(r"^\d{8}$") & (ex <= end_ymd)]
                if not sub.empty:
                    sub["_action_kind"] = "dividend"
                    parts.append(sub)
            ri = info_data.get_right_issue(
                batch, local_path=self._local_path, is_local=False
            )
            if isinstance(ri, pd.DataFrame) and not ri.empty:
                sub = ri.copy()
                if "PROGRESS" in sub.columns:
                    sub = sub[sub["PROGRESS"].astype(str).str.strip() == "3"]
                if "EX_DIVIDEND_DATE" in sub.columns:
                    ex = sub["EX_DIVIDEND_DATE"].astype(str).str.strip()
                    sub = sub[ex.str.match(r"^\d{8}$") & (ex <= end_ymd)]
                if not sub.empty:
                    sub["_action_kind"] = "rights"
                    parts.append(sub)
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

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
        """执行真实 SDK 探测或验证 Docker Compose 桥接环境可用性。"""
        if self._should_use_docker():
            return (
                shutil.which(self._docker_bin) is not None
                and self._compose_file.is_file()
            )
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
