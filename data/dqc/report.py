"""DQC 报告双表读写与 ``python -m data.dqc.report`` CLI。

双表位置（均位于 ``{data_root}/dqc/``）：
- ``_dqc_results/month=YYYY-MM/data.parquet``：按业务日期 ``data_date`` 分月追加，每规则每批一行。
- ``_dqc_batch_summary.parquet``：单文件汇总表，每批一行（``total_rows = rows_dropped + rows_flagged + rows_clean``）。

CLI 退出码：0 正常完成；1 数据校验未通过（``reingest``/``revalidate`` 遇拒收批次）；2 用法或运行错误。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import pyarrow.parquet as pq

from core.config import DATA_ROOT
from data.dqc.severity import severity_config_for
from data.quarantine import QUARANTINE_ROOT
from data.storage_ops import (
    PARQUET_WRITE_CONFIG,
    FileLock,
    is_monthly_partition,
    partition_path,
)
from data.storage_schema import PARTITION_PATHS, resolve_table_name

__all__ = [
    "DQC_REPORT_ROOT",
    "DETAIL_TABLE",
    "SUMMARY_TABLE",
    "RUN_TYPES",
    "detail_path",
    "summary_path",
    "build_detail_rows",
    "build_summary_row",
    "compute_quality_score",
    "append_detail",
    "append_summary",
    "load_detail",
    "load_summary",
    "purge_debug_runs",
    "load_quarantine_batches",
    "load_prev_day_stats",
    "build_reference_overrides",
    "run_dashboard",
    "reingest",
    "revalidate",
    "main",
]

logger = logging.getLogger(__name__)

DQC_REPORT_ROOT = "dqc"
DETAIL_TABLE = "_dqc_results"
SUMMARY_TABLE = "_dqc_batch_summary"

DETAIL_COLUMNS = [
    "run_date",
    "run_time",
    "run_id",
    "run_type",
    "data_date",
    "table",
    "rule_id",
    "rule_name",
    "dimension",
    "severity",
    "action_taken",
    "total_rows",
    "failing_rows",
    "failing_rate",
    "sample_tickers",
    "sample_values",
    "rule_version",
]
SUMMARY_COLUMNS = [
    "run_time",
    "run_id",
    "run_type",
    "data_date",
    "table",
    "total_rows",
    "rows_dropped",
    "rows_flagged",
    "rows_clean",
    "drop_rate",
    "flag_rate",
    "batch_status",
    "hard_rules_failed",
    "soft_rules_failed",
    "elapsed_seconds",
    "quality_score",
]
QUARANTINE_COLUMNS = [
    "data_date",
    "table",
    "reject_time",
    "batch_status",
    "total_rows",
    "rows_would_drop",
    "drop_rate",
    "hard_rules_triggered",
    "reject_reason",
]
BATCH_STATUSES = ("accepted", "accepted_with_warnings", "rejected")
RUN_TYPES = ("daily", "backfill", "debug")
_DATETIME_COLUMNS = ("run_date", "run_time", "data_date")

_STOCK_UNIVERSE_TABLES = frozenset(
    {
        "cn_stock_bars_daily",
        "cn_stock_bars_minute",
        "cn_stock_fundamental",
        "cn_stock_daily_status",
        "cn_stock_industry",
        "factor",
        "reference",
    }
)
_CA_DATE_TABLES = frozenset({"cn_stock_bars_daily", "cn_etf_bars_daily"})
_PREV_STATS_COLS = (
    "ticker",
    "trade_date",
    "close",
    "open",
    "high",
    "low",
    "volume",
    "amount",
    "pre_close",
    "total_market_cap",
)


def _now() -> datetime:
    """返回朴素 UTC 时间戳。"""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ── 表路径 ───────────────────────────────────────────────────────────────


def detail_path(
    data_date: date, data_root: str | os.PathLike[str] = str(DATA_ROOT)
) -> Path:
    """返回 *data_date* 所属月份的明细表路径。"""
    month = data_date.strftime("%Y-%m")
    return (
        Path(data_root)
        / DQC_REPORT_ROOT
        / DETAIL_TABLE
        / f"month={month}"
        / "data.parquet"
    )


def summary_path(data_root: str | os.PathLike[str] = str(DATA_ROOT)) -> Path:
    """返回批次汇总表路径。"""
    return Path(data_root) / DQC_REPORT_ROOT / f"{SUMMARY_TABLE}.parquet"


# ── 行构建 ───────────────────────────────────────────────────────────────


def compute_quality_score(
    hard_results: Sequence[Any],
    soft_results: Sequence[Any],
    total_rows: int = 0,
) -> float:
    """百分制数据质量分：满分 100，每个触发的 HARD 规则扣 10 分，每个触发的 SOFT 规则扣 2 分。"""
    del total_rows  # 保留签名参数以兼容调用方
    score = 100.0
    for r in hard_results:
        if r.failing_count > 0:
            score -= 10.0
    for r in soft_results:
        if r.failing_count > 0:
            score -= 2.0
    return max(0.0, score)


def build_detail_rows(
    hard_results: Sequence[Any],
    soft_results: Sequence[Any],
    *,
    data_date: date,
    table: str,
    batch_rejected: bool = False,
    run_time: datetime | None = None,
    run_id: str = "",
    run_type: str = "daily",
) -> list[dict]:
    """构建单批每条规则对应的 ``_dqc_results`` 明细行。"""
    if run_type not in RUN_TYPES:
        raise ValueError(f"bad run_type {run_type!r}; known: {list(RUN_TYPES)}")
    when = run_time or _now()
    rows: list[dict] = []
    for result in list(hard_results) + list(soft_results):
        failing = result.failing_count
        if failing == 0:
            action = "pass"
        elif batch_rejected and severity_config_for(result.rule_id).row_action in (
            "drop",
            "pass",
        ):
            action = "reject_batch"
        else:
            action = result.action_taken
        rows.append(
            {
                "run_date": when.date(),
                "run_time": when,
                "run_id": run_id,
                "run_type": run_type,
                "data_date": data_date,
                "table": table,
                "rule_id": result.rule_id,
                "rule_name": result.rule_name,
                "dimension": result.dimension,
                "severity": result.severity,
                "action_taken": action,
                "total_rows": int(result.total_rows),
                "failing_rows": int(failing),
                "failing_rate": float(result.failing_rate),
                "sample_tickers": json.dumps(
                    list(result.sample_tickers), ensure_ascii=False, default=str
                ),
                "sample_values": json.dumps(
                    list(result.sample_values), ensure_ascii=False, default=str
                ),
                "rule_version": result.rule_version,
            }
        )
    return rows


def build_summary_row(
    data_date: date,
    table: str,
    *,
    total_rows: int,
    rows_dropped: int,
    rows_flagged: int,
    batch_status: str,
    hard_failed: Sequence[str],
    soft_failed: Sequence[str],
    elapsed_seconds: float,
    run_time: datetime | None = None,
    run_id: str = "",
    quality_score: float | None = None,
    hard_results: Sequence[Any] | None = None,
    soft_results: Sequence[Any] | None = None,
    run_type: str = "daily",
) -> dict:
    """构建单批对应的 ``_dqc_batch_summary`` 汇总行。"""
    if batch_status not in BATCH_STATUSES:
        raise ValueError(
            f"bad batch_status {batch_status!r}; known: {list(BATCH_STATUSES)}"
        )
    if run_type not in RUN_TYPES:
        raise ValueError(f"bad run_type {run_type!r}; known: {list(RUN_TYPES)}")
    total = int(total_rows)
    dropped = int(rows_dropped)
    flagged = int(rows_flagged)
    if quality_score is not None:
        score = float(quality_score)
    elif hard_results is not None or soft_results is not None:
        score = compute_quality_score(hard_results or (), soft_results or (), total)
    else:
        score = max(
            0.0, 100.0 - 10.0 * len(list(hard_failed)) - 2.0 * len(list(soft_failed))
        )
    return {
        "run_time": run_time or _now(),
        "run_id": run_id,
        "run_type": run_type,
        "data_date": data_date,
        "table": table,
        "total_rows": total,
        "rows_dropped": dropped,
        "rows_flagged": flagged,
        "rows_clean": total - dropped - flagged,
        "drop_rate": (dropped / total) if total else 0.0,
        "flag_rate": (flagged / total) if total else 0.0,
        "batch_status": batch_status,
        "hard_rules_failed": json.dumps(list(hard_failed), ensure_ascii=False),
        "soft_rules_failed": json.dumps(list(soft_failed), ensure_ascii=False),
        "elapsed_seconds": float(elapsed_seconds),
        "quality_score": score,
    }


# ── 追加与读取 ───────────────────────────────────────────────────────────


def _normalize_legacy_table_col(df: pd.DataFrame) -> pd.DataFrame:
    """兼容历史报告中的 ``asset_type`` 列，读取时统一规整为 ``table``。"""
    if "table" not in df.columns and "asset_type" in df.columns:
        return df.rename(columns={"asset_type": "table"})
    return df


def _align_report_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """补齐历史报告缺失的新增字段并按 *columns* 对齐列序。"""
    out = _normalize_legacy_table_col(df).copy()
    for col in columns:
        if col not in out.columns:
            if col == "run_id":
                out[col] = ""
            elif col == "run_type":
                out[col] = "daily"
            elif col == "quality_score":
                out[col] = float("nan")
            else:
                out[col] = pd.NA
    return out[columns]


def _append_parquet(path: Path, rows: list[dict], columns: list[str]) -> Path:
    """在 ``FileLock`` 保护下原子追加行到未注册的报告 Parquet 文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(rows, columns=columns)
    for col in _DATETIME_COLUMNS:
        if col in new_df.columns:
            new_df[col] = pd.to_datetime(new_df[col])
    tmp = path.with_name(path.name + ".tmp")
    with FileLock(path):
        if path.exists():
            existing = _align_report_columns(pd.read_parquet(path), columns)
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        try:
            combined.to_parquet(tmp, **PARQUET_WRITE_CONFIG)
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, path)
        except BaseException:
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            raise
        try:
            dir_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            logger.warning("wrote %s but could not fsync %s", path, path.parent)
    return path


def append_detail(
    rows: list[dict], data_root: str | os.PathLike[str] = str(DATA_ROOT)
) -> list[Path]:
    """按 ``data_date`` 所属月份追加明细行到各月分区文件。"""
    if not rows:
        return []
    by_month: dict[str, list[dict]] = {}
    for row in rows:
        month = pd.Timestamp(row["data_date"]).strftime("%Y-%m")
        by_month.setdefault(month, []).append(row)
    written: list[Path] = []
    for month_rows in by_month.values():
        first = pd.Timestamp(month_rows[0]["data_date"]).date()
        written.append(
            _append_parquet(detail_path(first, data_root), month_rows, DETAIL_COLUMNS)
        )
    return written


def append_summary(
    row: dict, data_root: str | os.PathLike[str] = str(DATA_ROOT)
) -> Path:
    """追加一条汇总记录到汇总表。"""
    return _append_parquet(summary_path(data_root), [row], SUMMARY_COLUMNS)


def _write_report_parquet_locked(
    path: Path, frame: pd.DataFrame, columns: list[str]
) -> None:
    for col in _DATETIME_COLUMNS:
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col])
    tmp = path.with_name(path.name + ".tmp")
    try:
        frame[columns].to_parquet(tmp, **PARQUET_WRITE_CONFIG)
        fd = os.open(tmp, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except FileNotFoundError:
            pass
        raise


def _drop_debug_rows_from_file(
    path: Path,
    columns: list[str],
    *,
    data_date: date | None = None,
    table: str | None = None,
) -> int:
    if not path.exists():
        return 0
    with FileLock(path):
        df = _align_report_columns(pd.read_parquet(path), columns)
        if df.empty:
            return 0
        mask = df["run_type"].fillna("daily") == "debug"
        if data_date is not None:
            dates = pd.to_datetime(df["data_date"], errors="coerce").dt.date
            mask = mask & (dates == data_date)
        if table is not None:
            mask = mask & (df["table"].astype(str) == str(table))
        removed = int(mask.sum())
        if not removed:
            return 0
        kept = df.loc[~mask].reset_index(drop=True)
        _write_report_parquet_locked(path, kept, columns)
        return removed


def purge_debug_runs(
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    *,
    data_date: date | None = None,
    table: str | None = None,
) -> int:
    """清除指定批次（或全部）的 ``run_type='debug'`` 历史记录，返回清理的汇总行数。"""
    removed = _drop_debug_rows_from_file(
        summary_path(data_root), SUMMARY_COLUMNS, data_date=data_date, table=table
    )
    if data_date is not None:
        detail_files = [detail_path(data_date, data_root)]
    else:
        detail_root = Path(data_root) / DQC_REPORT_ROOT / DETAIL_TABLE
        detail_files = sorted(detail_root.glob("month=*/data.parquet"))
    for f in detail_files:
        _drop_debug_rows_from_file(f, DETAIL_COLUMNS, data_date=data_date, table=table)
    return removed


def load_detail(data_root: str | os.PathLike[str] = str(DATA_ROOT)) -> pd.DataFrame:
    """读取全部明细分区（无数据时返回空表）。"""
    root = Path(data_root) / DQC_REPORT_ROOT / DETAIL_TABLE
    files = sorted(root.glob("month=*/data.parquet"))
    if not files:
        return pd.DataFrame(columns=DETAIL_COLUMNS)
    frames = [_align_report_columns(pd.read_parquet(f), DETAIL_COLUMNS) for f in files]
    return pd.concat(frames, ignore_index=True)[DETAIL_COLUMNS]


def load_summary(data_root: str | os.PathLike[str] = str(DATA_ROOT)) -> pd.DataFrame:
    """读取批次汇总表（无数据时返回空表）。"""
    path = summary_path(data_root)
    if not path.exists():
        return pd.DataFrame(columns=SUMMARY_COLUMNS)
    return _align_report_columns(pd.read_parquet(path), SUMMARY_COLUMNS)


def load_quarantine_batches(
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> list[dict]:
    """合并隔离区 ``rejection.json`` 与汇总表 ``rejected`` 记录（应对过期清理后的差异）。"""
    root = Path(data_root)
    batches: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for meta_file in sorted(root.glob(f"{QUARANTINE_ROOT}/*/*.rejection.json")):
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("unreadable rejection metadata %s: %s", meta_file, exc)
            continue
        row = {col: meta.get(col) for col in QUARANTINE_COLUMNS}
        row["hard_rules_triggered"] = list(row.get("hard_rules_triggered") or [])
        row["data_date"] = str(
            row.get("data_date") or meta_file.name[: -len(".rejection.json")]
        )
        row["table"] = str(
            row.get("table") or meta.get("asset_type") or meta_file.parent.name
        )
        batches.append(row)
        seen.add((row["data_date"], row["table"]))
    for summary in load_summary(data_root).to_dict("records"):
        if summary.get("batch_status") != "rejected":
            continue
        tbl = str(summary.get("table") or summary.get("asset_type") or "")
        key = (
            pd.Timestamp(summary["data_date"]).strftime("%Y-%m-%d"),
            tbl,
        )
        if key in seen:
            continue
        seen.add(key)
        batches.append(
            {
                "data_date": key[0],
                "table": key[1],
                "reject_time": None,
                "batch_status": "rejected",
                "total_rows": summary["total_rows"],
                "rows_would_drop": summary["rows_dropped"],
                "drop_rate": summary["drop_rate"],
                "hard_rules_triggered": json.loads(
                    summary["hard_rules_failed"] or "[]"
                ),
                "reject_reason": "(no quarantine record)",
            }
        )
    return sorted(batches, key=lambda r: (r["data_date"], r["table"]))


# ── 参考数据注入 ─────────────────────────────────────────────────────────


def load_prev_day_stats(
    data_root: str | os.PathLike[str],
    table: str,
    data_date: date,
) -> dict[str, Any]:
    """从前一个可用日/月分区 Parquet 提取行数、标的列表及均值统计（严格早于 *data_date*）。"""
    root = Path(data_root)
    canonical = resolve_table_name(table)
    template = PARTITION_PATHS.get(canonical)
    if not template or ("{date}" not in template and "{month}" not in template):
        return {}
    table_dir = partition_path(canonical, data_date, root).parent
    if not table_dir.exists() and canonical == "cn_stock_bars_daily":
        legacy_dir = root / "asset" / "bars_daily"
        if legacy_dir.exists():
            table_dir = legacy_dir
    if not table_dir.exists():
        return {}

    prev_date_str = ""
    prev_df = pd.DataFrame()
    if "{date}" in template:
        date_str = data_date.strftime("%Y-%m-%d")
        prior = [
            f
            for f in table_dir.glob("*.parquet")
            if len(f.stem) == 10 and f.stem < date_str
        ]
        if not prior:
            return {}
        prev_file = max(prior)
        prev_date_str = prev_file.stem
        avail = set(pq.read_schema(prev_file).names)
        cols = [c for c in _PREV_STATS_COLS if c in avail]
        prev_df = pd.read_parquet(prev_file, columns=cols or None)
    else:
        cur_month = data_date.strftime("%Y-%m")
        month_files = sorted(
            (
                f
                for f in table_dir.glob("*.parquet")
                if len(f.stem) == 7 and f.stem <= cur_month
            ),
            reverse=True,
        )
        for mf in month_files:
            try:
                avail = set(pq.read_schema(mf).names)
            except Exception:  # noqa: BLE001
                continue
            if "trade_date" not in avail:
                continue
            dates_only = pd.read_parquet(mf, columns=["trade_date"])
            if dates_only.empty:
                continue
            td_all = pd.to_datetime(dates_only["trade_date"], errors="coerce").dt.date
            earlier = td_all[td_all.notna() & (td_all < data_date)]
            if earlier.empty:
                continue
            target_prev_date = earlier.max()
            cols = [c for c in _PREV_STATS_COLS if c in avail]
            sub = pd.read_parquet(mf, columns=cols or None)
            sub_td = pd.to_datetime(sub["trade_date"], errors="coerce").dt.date
            prev_df = sub.loc[sub_td == target_prev_date].reset_index(drop=True)
            prev_date_str = target_prev_date.strftime("%Y-%m-%d")
            break
        if prev_df.empty or not prev_date_str:
            return {}

    stats: dict[str, Any] = {
        "prev_date": prev_date_str,
        "row_count": int(len(prev_df)),
    }
    if "ticker" in prev_df.columns:
        stats["tickers"] = sorted(set(prev_df["ticker"].dropna().astype(str)))
    for col in (
        "close",
        "open",
        "high",
        "low",
        "volume",
        "amount",
        "pre_close",
        "total_market_cap",
    ):
        if col in prev_df.columns:
            series = pd.to_numeric(prev_df[col], errors="coerce").dropna()
            if not series.empty:
                stats[f"{col}_mean"] = float(series.mean())
    return stats


def _active_tickers_from_basic(path: Path, data_date: date) -> list[str]:
    if not path.exists():
        return []
    basic_df = pd.read_parquet(path)
    if "ticker" not in basic_df.columns:
        return []
    active = basic_df
    if "list_date" in active.columns:
        ld = pd.to_datetime(active["list_date"], errors="coerce").dt.date
        active = active[ld.isna() | (ld <= data_date)]
    if "delist_date" in active.columns:
        dd = pd.to_datetime(active["delist_date"], errors="coerce").dt.date
        if "list_status" in active.columns:
            still_listed = dd.isna() & (active["list_status"] != "D")
            active = active[still_listed | (dd >= data_date)]
        else:
            active = active[dd.isna() | (dd >= data_date)]
    elif "list_status" in active.columns:
        active = active[active["list_status"] != "D"]
    if active.empty:
        return []
    return sorted(set(active["ticker"].dropna().astype(str)))


def build_reference_overrides(
    data_root: str | os.PathLike[str], table: str, data_date: date
) -> dict:
    """从本地存储装配检查所需的参考数据 ``overrides``；缺失表保持缺省以触发 fail-closed。"""
    root = Path(data_root)
    canonical = resolve_table_name(table)
    overrides: dict[str, Any] = {"today": data_date, "table": canonical}

    cal = next(
        (
            p
            for p in (
                root / "asset" / "cn_calendar" / "SSE.parquet",
                root / "asset" / "cn_calendar" / "sse.parquet",
                root / "asset" / "calendar" / "sse.parquet",
            )
            if p.exists()
        ),
        None,
    )
    if cal is not None:
        cal_df = pd.read_parquet(cal)
        if "trade_date" in cal_df.columns:
            overrides["calendar_dates"] = sorted(
                pd.to_datetime(cal_df["trade_date"]).dt.strftime("%Y-%m-%d").unique()
            )

    if canonical in _CA_DATE_TABLES:
        ca = next(
            (
                p
                for p in (
                    root
                    / "asset"
                    / "cn_stock_corporate_actions"
                    / "corporate_actions.parquet",
                    root / "asset" / "reference" / "corporate_actions.parquet",
                )
                if p.exists()
            ),
            None,
        )
        if ca is not None:
            ca_df = pd.read_parquet(ca, columns=["ex_date"])
            if "ex_date" in ca_df.columns:
                overrides["ca_dates"] = sorted(
                    pd.to_datetime(ca_df["ex_date"]).dt.strftime("%Y-%m-%d").unique()
                )

    if canonical in _STOCK_UNIVERSE_TABLES:
        ind = next(
            (
                p
                for p in (
                    root / "asset" / "cn_stock_industry" / "industry.parquet",
                    root / "asset" / "reference" / "industry.parquet",
                )
                if p.exists()
            ),
            None,
        )
        if ind is not None:
            ind_df = pd.read_parquet(ind)
            if "ticker" in ind_df.columns:
                overrides["universe"] = sorted(set(ind_df["ticker"].astype(str)))
        if "universe" not in overrides:
            basic = next(
                (
                    p
                    for p in (
                        root / "asset" / "cn_stock_basic" / "stock_basic.parquet",
                        root / "asset" / "reference" / "stock_basic.parquet",
                    )
                    if p.exists()
                ),
                None,
            )
            if basic is not None:
                tickers = _active_tickers_from_basic(basic, data_date)
                if tickers:
                    overrides["universe"] = tickers

        bars_dir = next(
            (
                d
                for d in (
                    root / "asset" / "cn_stock_bars_daily",
                    root / "asset" / "bars_daily",
                )
                if d.exists()
            ),
            None,
        )
        if bars_dir is not None:
            prior = [
                f
                for f in bars_dir.glob("*.parquet")
                if f.stem <= data_date.strftime("%Y-%m-%d")
            ]
            if prior:
                prev = pd.read_parquet(max(prior), columns=["ticker"])
                if "ticker" in prev.columns:
                    overrides["daily_tickers"] = sorted(set(prev["ticker"].astype(str)))
    elif canonical in ("cn_etf_bars_daily", "cn_etf_bars_minute"):
        etf_basic = root / "asset" / "cn_etf_basic" / "etf_basic.parquet"
        tickers = _active_tickers_from_basic(etf_basic, data_date)
        if tickers:
            overrides["universe"] = tickers
        if canonical == "cn_etf_bars_minute":
            etf_bars_dir = root / "asset" / "cn_etf_bars_daily"
            if etf_bars_dir.exists():
                prior = [
                    f
                    for f in etf_bars_dir.glob("*.parquet")
                    if f.stem <= data_date.strftime("%Y-%m-%d")
                ]
                if prior:
                    prev = pd.read_parquet(max(prior), columns=["ticker"])
                    if "ticker" in prev.columns:
                        overrides["daily_tickers"] = sorted(
                            set(prev["ticker"].astype(str))
                        )
    elif canonical == "cn_stock_index_bars_daily":
        idx_basic = root / "asset" / "cn_stock_index_basic" / "index_basic.parquet"
        tickers = _active_tickers_from_basic(idx_basic, data_date)
        if tickers:
            overrides["universe"] = tickers

    template = PARTITION_PATHS.get(canonical)
    if template and ("{date}" in template or "{month}" in template):
        target = partition_path(canonical, data_date, root)
        if target.exists():
            col_names = set(pq.read_schema(target).names)
            if {"ticker", "adjfactor"} <= col_names:
                stored = pd.read_parquet(target)
                if is_monthly_partition(canonical) and "trade_date" in stored.columns:
                    td = pd.to_datetime(stored["trade_date"], errors="coerce").dt.date
                    stored = stored[td == data_date].reset_index(drop=True)
                overrides["previous_adjfactor"] = stored

    prev_stats = load_prev_day_stats(root, canonical, data_date)
    if prev_stats:
        overrides["prev_row_count"] = prev_stats.get("row_count", 0)
        overrides["prev_tickers"] = prev_stats.get("tickers", [])
        overrides["prev_stats"] = prev_stats
        if "universe" not in overrides and prev_stats.get("tickers"):
            overrides["universe"] = list(prev_stats["tickers"])
    return overrides


# ── 看板与 CLI 操作 ──────────────────────────────────────────────────────


def run_dashboard(
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    last: int = 10,
    *,
    include_debug: bool = False,
) -> str:
    """使用 DuckDB 汇总批次状态计数、近期批次与高频失败规则。"""
    import duckdb

    summary = summary_path(data_root)
    if not summary.exists():
        return f"no DQC batches under {Path(data_root) / DQC_REPORT_ROOT}/"
    detail_glob = (
        Path(data_root) / DQC_REPORT_ROOT / DETAIL_TABLE / "month=*/data.parquet"
    )
    con = duckdb.connect()
    lines: list[str] = []
    cols = {
        r[0]
        for r in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(summary)]
        ).fetchall()
    }
    where_clause = (
        "WHERE coalesce(run_type, 'daily') != 'debug'"
        if ("run_type" in cols and not include_debug)
        else ""
    )
    by_status = con.execute(
        "SELECT batch_status, count(*) AS batches, "
        "round(avg(drop_rate), 4) AS avg_drop_rate, "
        "round(avg(flag_rate), 4) AS avg_flag_rate "
        f"FROM read_parquet(?) {where_clause} GROUP BY 1 ORDER BY 1",
        [str(summary)],
    ).fetchdf()
    lines.append("-- batch_status --")
    lines.append(by_status.to_string(index=False))
    tbl_expr = '"table"' if "table" in cols else 'asset_type AS "table"'
    run_type_expr = "run_type, " if "run_type" in cols else ""
    recent = con.execute(
        f"SELECT data_date, {tbl_expr}, {run_type_expr}batch_status, total_rows, "
        "rows_dropped, rows_flagged, drop_rate, flag_rate, elapsed_seconds "
        f"FROM read_parquet(?) {where_clause} ORDER BY data_date DESC, run_time DESC LIMIT ?",
        [str(summary), int(last)],
    ).fetchdf()
    lines.append(f"-- last {int(last)} batches --")
    lines.append(recent.to_string(index=False))
    if list(detail_glob.parent.glob("month=*/data.parquet")):
        dcols = {
            r[0]
            for r in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(detail_glob)]
            ).fetchall()
        }
        d_debug = (
            " AND coalesce(run_type, 'daily') != 'debug'"
            if ("run_type" in dcols and not include_debug)
            else ""
        )
        top = con.execute(
            "SELECT rule_id, rule_name, severity, count(*) AS failing_batches, "
            "round(max(failing_rate), 4) AS max_failing_rate "
            f"FROM read_parquet(?) WHERE failing_rows > 0{d_debug} "
            "GROUP BY 1, 2, 3 ORDER BY failing_batches DESC, rule_id LIMIT 10",
            [str(detail_glob)],
        ).fetchdf()
        lines.append("-- top failing rules --")
        lines.append(top.to_string(index=False))
    con.close()
    return "\n".join(lines)


def reingest(
    source: str,
    table: str,
    trade_date: date,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    overrides: dict | None = None,
) -> int:
    """重新拉取指定日期数据并完整执行 DQC 入库；无数据或拒收时返回 1。"""
    from data.adapters import get_adapter
    from data.dqc.engine import ingest_with_dqc
    from data.dqc.severity import DQCBatchReject
    from data.ingestion import TABLE_TO_DQC_GROUP
    from data.normalizers import get_normalizer
    from data.raw import save_raw

    adapter = get_adapter(source)
    try:
        result = adapter.fetch(table, trade_date)
    finally:
        adapter.close()
    raw_df = result.df
    if raw_df is None or raw_df.empty:
        print(
            f"source {source} returned no rows for {table} "
            f"{trade_date.strftime('%Y-%m-%d')}",
            file=sys.stderr,
        )
        return 1
    save_raw(raw_df, source, table, trade_date, data_root)
    df = get_normalizer(source)(table, raw_df)
    ref = build_reference_overrides(data_root, table, trade_date)
    ref.update(overrides or {})
    dqc_group = TABLE_TO_DQC_GROUP.get(table, table)
    try:
        ingest_with_dqc(
            df,
            trade_date,
            dqc_group,
            table=table,
            overrides=ref,
            data_root=data_root,
        )
    except DQCBatchReject as exc:
        print(f"batch rejected: {exc}", file=sys.stderr)
        return 1
    return 0


def revalidate(
    source: str,
    table: str,
    start: date,
    end: date,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    overrides: dict | None = None,
) -> int:
    """按当前规则复核本地 raw 数据并仅写入 ``_dqc_results`` 明细表；存在拒收批次时返回 1。"""
    from data.dqc.engine import evaluate_batch
    from data.ingestion import TABLE_TO_DQC_GROUP
    from data.normalizers import get_normalizer
    from data.raw import load_raw

    normalize = get_normalizer(source)
    dqc_group = TABLE_TO_DQC_GROUP.get(table, table)
    rows: list[dict] = []
    checked = rejected = skipped = 0
    day = start
    while day <= end:
        raw = load_raw(source, table, day, data_root)
        if raw is None:
            skipped += 1
        else:
            df = normalize(table, raw)
            ref = build_reference_overrides(data_root, table, day)
            ref.update(overrides or {})
            evaluation = evaluate_batch(df, day, dqc_group, overrides=ref)
            bad = evaluation.batch_status == "rejected"
            checked += 1
            rejected += int(bad)
            rows.extend(
                build_detail_rows(
                    evaluation.hard_results,
                    evaluation.soft_results,
                    data_date=day,
                    table=table,
                    batch_rejected=bad,
                )
            )
        day += timedelta(days=1)
    written = append_detail(rows, data_root) if rows else []
    print(
        f"revalidated {checked} batch(es): {rejected} would reject, "
        f"{skipped} missing raw, {len(written)} report file(s) written"
    )
    return 1 if rejected else 0


# ── CLI 入口 ─────────────────────────────────────────────────────────────


def _parse_date(text: str) -> date:
    return datetime.strptime(text, "%Y-%m-%d").date()


def _print_quarantine(batches: list[dict]) -> None:
    if not batches:
        print("no quarantined batches")
        return
    for row in batches:
        triggered = ",".join(row.get("hard_rules_triggered") or []) or "-"
        print(
            f"{row['data_date']} {row['table']}: {row['batch_status']} "
            f"total={row['total_rows']} would_drop={row['rows_would_drop']} "
            f"drop_rate={row['drop_rate']} rules=[{triggered}] "
            f"reason={row['reject_reason']}"
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m data.dqc.report",
        description=(
            "DQC dual-table report: quarantine / dashboard / reingest / revalidate."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser(
        "quarantine", help="list quarantined batches with reject reasons"
    )
    p.add_argument("--data-root", default=str(DATA_ROOT))
    p.add_argument("--json", action="store_true", help="print machine-readable JSON")

    p = sub.add_parser(
        "dashboard", help="status counts, recent batches, top failing rules"
    )
    p.add_argument("--data-root", default=str(DATA_ROOT))
    p.add_argument("--last", type=int, default=10)
    p.add_argument(
        "--include-debug",
        action="store_true",
        help="include run_type='debug' batches in summary",
    )

    p = sub.add_parser(
        "reingest", help="re-download one date and re-run the full ingest"
    )
    p.add_argument("--source", required=True, help="raw source name (akshare/...)")
    p.add_argument(
        "--table",
        "--asset",
        dest="table",
        required=True,
        help="table or DQC group (bars_daily/cn_stock_bars_daily/...)",
    )
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--data-root", default=str(DATA_ROOT))

    p = sub.add_parser(
        "revalidate", help="re-check local raw under current rules (report only)"
    )
    p.add_argument("--source", required=True, help="raw source name (akshare/...)")
    p.add_argument(
        "--table",
        "--asset",
        dest="table",
        required=True,
        help="table or DQC group (bars_daily/cn_stock_bars_daily/...)",
    )
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--data-root", default=str(DATA_ROOT))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """``python -m data.dqc.report`` CLI 入口，返回退出码。"""
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "quarantine":
            batches = load_quarantine_batches(args.data_root)
            if args.json:
                print(json.dumps(batches, ensure_ascii=False, indent=2, default=str))
            else:
                _print_quarantine(batches)
            return 0
        if args.command == "dashboard":
            print(
                run_dashboard(
                    args.data_root, last=args.last, include_debug=args.include_debug
                )
            )
            return 0
        if args.command == "reingest":
            return reingest(
                args.source,
                args.table,
                _parse_date(args.date),
                data_root=args.data_root,
            )
        if args.command == "revalidate":
            start, end = _parse_date(args.start), _parse_date(args.end)
            if end < start:
                raise ValueError(f"end {end} precedes start {start}")
            return revalidate(
                args.source,
                args.table,
                start,
                end,
                data_root=args.data_root,
            )
    except Exception as exc:  # noqa: BLE001 — 运行错误统一返回退出码 2
        print(f"error: {exc}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    sys.exit(main())
