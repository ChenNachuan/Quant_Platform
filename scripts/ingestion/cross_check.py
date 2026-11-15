"""独立多源交叉校验与报告生成入口。

对指定日期区间与表集合执行主备源截面对比，调用五级容差仲裁与分级响应，
并将明细结果持久化到 ``{data_root}/cross_check/{table}/{date}.json``。
任一字段触发 ``ERROR`` 级别差异时以状态码 1 退出。
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Sequence

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.config import DATA_ROOT  # noqa: E402
from data.ingestion import ResponseLevel, run_cross_check_for_date  # noqa: E402
from scripts.ingestion import (  # noqa: E402
    TABLE_GROUPS,
    parse_date,
    resolve_tables,
    resolve_trading_dates,
)

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="对已落地或在线拉取的主备源批次执行多源交叉校验"
    )
    parser.add_argument(
        "--start",
        required=True,
        help="起始日期（YYYY-MM-DD 或 YYYYMMDD）",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="结束日期（默认与 --start 相同）",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="逗号分隔的目标表名（默认 cn_stock_bars_daily）",
    )
    parser.add_argument(
        "--group",
        choices=sorted(TABLE_GROUPS),
        default=None,
        help="按预设分组批量校验表集合",
    )
    parser.add_argument(
        "--fetch-missing-raw",
        action="store_true",
        help="当 raw/ 下缺少主备源批次时自动调用外部适配器拉取并落地",
    )
    parser.add_argument(
        "--all-calendar-days",
        action="store_true",
        help="遍历区间内全部自然日，不跳过周末或休市日",
    )
    parser.add_argument(
        "--data-root",
        default=str(DATA_ROOT),
        help=f"数据根目录路径（默认 {DATA_ROOT}）",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="日志输出级别（默认 INFO）",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """命令行主入口：无 ERROR 级差异返回 0，出现 ERROR 或执行异常返回 1，参数错误返回 2。"""
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    data_root = Path(args.data_root)
    try:
        start = parse_date(args.start)
        end = parse_date(args.end) if args.end else start
        tables = resolve_tables(args.tables, args.group)
        dates = resolve_trading_dates(
            start,
            end,
            data_root=data_root,
            all_calendar_days=args.all_calendar_days,
        )
    except (ValueError, KeyError) as exc:
        print(f"参数错误: {exc}", file=sys.stderr)
        return 2

    if not dates:
        print(f"汇总: 区间 {start}..{end} 内无待校验日期")
        return 0

    has_error = False
    checked = 0
    missing_raw = 0
    lines: list[str] = []

    for trade_date in dates:
        for table in tables:
            try:
                levels: dict[str, ResponseLevel] = run_cross_check_for_date(
                    table,
                    trade_date,
                    data_root=data_root,
                    fetch_missing_raw=args.fetch_missing_raw,
                )
            except FileNotFoundError as exc:
                missing_raw += 1
                logger.warning("跳过 %s %s: %s", table, trade_date, exc)
                lines.append(
                    f"  [SKIP_MISSING_RAW] {trade_date.isoformat()} {table} ({exc})"
                )
                continue
            except Exception as exc:  # noqa: BLE001
                has_error = True
                logger.exception("交叉校验异常 %s %s", table, trade_date)
                lines.append(
                    f"  [ERROR] {trade_date.isoformat()} {table} ({type(exc).__name__}: {exc})"
                )
                continue

            if not levels:
                lines.append(
                    f"  [NO_BACKUP] {trade_date.isoformat()} {table} (无可用备源批次)"
                )
                continue

            checked += 1
            if any(lvl == "ERROR" for lvl in levels.values()):
                has_error = True
            detail = ",".join(f"{src}:{lvl}" for src, lvl in sorted(levels.items()))
            lines.append(f"  [CHECKED] {trade_date.isoformat()} {table} {detail}")

    print(
        f"交叉校验汇总: checked={checked} missing_raw={missing_raw} "
        f"has_error={has_error}"
    )
    for line in lines:
        print(line)

    return 1 if has_error else 0


if __name__ == "__main__":
    sys.exit(main())
