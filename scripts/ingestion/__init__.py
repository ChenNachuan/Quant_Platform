"""数据接入命令行工具包（``fetch_daily`` / ``backfill`` / ``cross_check`` / ``amazing_pull``）。

封装表分组解析、日期区间展开与执行摘要格式化等共享辅助函数；
所有函数与参数命名均使用 51 张规范表名（兼容旧别名解析）。
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence

from core.config import DATA_ROOT
from data.ingestion import IngestOutcome
from data.storage_schema import SCHEMA, resolve_table_name

DEFAULT_TABLE = "cn_stock_bars_daily"

#: 常用数据表分组（覆盖日常核心、行情、股票全表、ETF、公募、基本面与期货）
TABLE_GROUPS: dict[str, tuple[str, ...]] = {
    "core": (
        "cn_calendar",
        "cn_stock_basic",
        "cn_stock_bars_daily",
        "cn_stock_fundamental",
        "cn_stock_daily_status",
    ),
    "daily_market": (
        "cn_stock_bars_daily",
        "cn_stock_daily_status",
        "cn_stock_index_bars_daily",
        "cn_etf_bars_daily",
        "cn_futures_bars_daily",
    ),
    "stock": tuple(k for k in SCHEMA if k.startswith("cn_stock_")),
    "etf": tuple(k for k in SCHEMA if k.startswith("cn_etf_")),
    "fund": tuple(k for k in SCHEMA if k.startswith("cn_fund_")),
    "fundamental": tuple(k for k in SCHEMA if k.startswith("cn_stock_fundamental")),
    "futures": tuple(k for k in SCHEMA if k.startswith("cn_futures_")),
    "all": tuple(k for k in SCHEMA if k != "factor"),
}

__all__ = [
    "DEFAULT_TABLE",
    "TABLE_GROUPS",
    "parse_date",
    "resolve_tables",
    "latest_trade_date",
    "resolve_trading_dates",
    "format_outcomes",
]


def parse_date(value: str) -> date:
    """解析 ``YYYY-MM-DD`` 或 ``YYYYMMDD`` 格式的日期字符串。"""
    raw = str(value).strip()
    if len(raw) == 8 and raw.isdigit():
        return datetime.strptime(raw, "%Y%m%d").date()
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(
            f"日期格式非法: {value!r}（仅支持 YYYY-MM-DD 或 YYYYMMDD）"
        ) from exc


def resolve_tables(
    tables: str | Sequence[str] | None = None,
    group: str | None = None,
    *,
    default: str = DEFAULT_TABLE,
) -> list[str]:
    """根据 ``--tables`` 与 ``--group`` 解析去重后的规范表名列表。"""
    resolved: list[str] = []
    seen: set[str] = set()

    def _add(name: str) -> None:
        canonical = resolve_table_name(name.strip())
        if canonical not in SCHEMA:
            raise ValueError(f"未知资产表名: {name!r}")
        if canonical not in seen:
            seen.add(canonical)
            resolved.append(canonical)

    if group:
        if group not in TABLE_GROUPS:
            raise ValueError(
                f"未知表分组 {group!r}，可选: {', '.join(sorted(TABLE_GROUPS))}"
            )
        for tbl in TABLE_GROUPS[group]:
            _add(tbl)

    if tables:
        items: Iterable[str]
        if isinstance(tables, str):
            items = (part for part in tables.split(",") if part.strip())
        else:
            items = (part for part in tables if str(part).strip())
        for item in items:
            _add(str(item))

    if not resolved:
        _add(default)
    return resolved


def _find_calendar_path(data_root: str | os.PathLike[str]) -> Path | None:
    root = Path(data_root)
    for rel in (
        "asset/cn_calendar/SSE.parquet",
        "asset/cn_calendar/sse.parquet",
        "asset/calendar/sse.parquet",
    ):
        candidate = root / rel
        if candidate.exists():
            return candidate
    return None


def _read_calendar_dates(cal_path: Path) -> list[date]:
    import pandas as pd

    df = pd.read_parquet(cal_path)
    date_col = "trade_date" if "trade_date" in df.columns else "cal_date"
    if date_col not in df.columns:
        return []
    if "is_open" in df.columns:
        df = df[df["is_open"].astype(bool)]
    parsed = pd.to_datetime(df[date_col], errors="coerce").dt.date.dropna()
    return sorted(set(parsed.tolist()))


def latest_trade_date(data_root: str | os.PathLike[str] = DATA_ROOT) -> date:
    """获取最近已发生的交易日；若本地日历缺失或不可读则回退至今日。"""
    cal_path = _find_calendar_path(data_root)
    today = date.today()
    if cal_path is not None:
        try:
            days = [d for d in _read_calendar_dates(cal_path) if d <= today]
            if days:
                return days[-1]
        except Exception:  # noqa: BLE001
            pass
    return today


def resolve_trading_dates(
    start: date,
    end: date,
    *,
    data_root: str | os.PathLike[str] = DATA_ROOT,
    all_calendar_days: bool = False,
) -> list[date]:
    """生成 ``[start, end]`` 区间内待处理的日期序列（默认按交易日或工作日）。"""
    if start > end:
        raise ValueError(f"起始日期 {start} 不能晚于结束日期 {end}")

    if all_calendar_days:
        total = (end - start).days + 1
        return [start + timedelta(days=offset) for offset in range(total)]

    cal_path = _find_calendar_path(data_root)
    if cal_path is not None:
        try:
            cal_dates = _read_calendar_dates(cal_path)
            if cal_dates:
                min_cal, max_cal = cal_dates[0], cal_dates[-1]
                # 若本地日历覆盖窗口或部分覆盖窗口，则在覆盖部分使用真实交易日历
                if min_cal <= end and max_cal >= start:
                    covered = [d for d in cal_dates if start <= d <= end]
                    if end > max_cal:
                        tail_start = max(start, max_cal + timedelta(days=1))
                        cur = tail_start
                        while cur <= end:
                            if cur.weekday() < 5:
                                covered.append(cur)
                            cur += timedelta(days=1)
                    return covered
        except Exception:  # noqa: BLE001
            pass

    days: list[date] = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def format_outcomes(outcomes: Sequence[IngestOutcome]) -> list[str]:
    """将 :class:`IngestOutcome` 列表格式化为可读摘要文本行。"""
    success = sum(1 for o in outcomes if o.status == "success")
    skipped = sum(1 for o in outcomes if o.status == "skipped")
    failed = sum(1 for o in outcomes if o.status == "failed")
    lines = [
        f"汇总: total={len(outcomes)} success={success} skipped={skipped} failed={failed}"
    ]
    for o in outcomes:
        parts = [f"[{o.status.upper()}]", o.trade_date.isoformat(), o.table]
        if o.source:
            parts.append(f"source={o.source}")
        if o.status == "success":
            parts.append(f"rows={o.row_count}")
            if o.fallback_from:
                parts.append(f"fallback_from={o.fallback_from}")
            if o.cross_check_levels:
                cc = ",".join(
                    f"{k}:{v}" for k, v in sorted(o.cross_check_levels.items())
                )
                parts.append(f"cross_check={cc}")
        elif o.status == "failed" and o.error:
            parts.append(f"error={o.error}")
        lines.append("  " + " ".join(parts))
    return lines
