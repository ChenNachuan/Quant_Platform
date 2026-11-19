"""历史区间多表数据回补入口。

按 ``--start .. --end`` 展开交易日序列并逐日执行标准接入流程（``raw/ -> DQC -> asset/``），
支持断点续跑（默认跳过已落地分区）与限流间隔控制。
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
from data.ingestion import run_ingestion  # noqa: E402
from scripts.ingestion import (  # noqa: E402
    TABLE_GROUPS,
    format_outcomes,
    parse_date,
    resolve_tables,
    resolve_trading_dates,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="按日期区间批量回补历史数据（默认仅遍历交易日或工作日）"
    )
    parser.add_argument(
        "--start",
        required=True,
        help="起始日期（YYYY-MM-DD 或 YYYYMMDD）",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="结束日期（YYYY-MM-DD 或 YYYYMMDD）",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="逗号分隔的目标表名（支持 51 张规范表名或旧别名，默认 cn_stock_bars_daily）",
    )
    parser.add_argument(
        "--group",
        choices=sorted(TABLE_GROUPS),
        default=None,
        help="按预设分组批量回补表集合",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="显式指定单一数据源（禁用自动主备切换）",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="覆盖已存在的历史分区",
    )
    parser.add_argument(
        "--from-raw",
        action="store_true",
        help="直接读取已有 raw/ 原始文件重新标准化与入库",
    )
    parser.add_argument(
        "--no-cross-check",
        action="store_true",
        help="禁用入库后的自动交叉校验",
    )
    parser.add_argument(
        "--skip-dqc",
        action="store_true",
        help="跳过 DQC 规则校验（仍执行 Pandera Schema 校验）",
    )
    parser.add_argument(
        "--all-calendar-days",
        action="store_true",
        help="遍历区间内全部自然日，不跳过周末或休市日",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="相邻批次间额外等待秒数（默认 0）",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="遇到首个失败批次立即中断退出",
    )
    parser.add_argument(
        "--data-root",
        default=str(DATA_ROOT),
        help=f"数据根目录路径（默认 {DATA_ROOT}）",
    )
    parser.add_argument(
        "--run-type",
        choices=("daily", "backfill", "debug"),
        default="backfill",
        help="DQC 运行类型标记（默认 backfill）",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="日志输出级别（默认 INFO）",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """命令行主入口：返回 0 表示无失败批次，1 表示存在失败，2 表示参数错误。"""
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.sleep < 0:
        print("参数错误: --sleep 不能为负数", file=sys.stderr)
        return 2

    data_root = Path(args.data_root)
    try:
        start = parse_date(args.start)
        end = parse_date(args.end)
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
        print(f"汇总: 区间 {start}..{end} 内无待回补日期，已跳过")
        return 0

    try:
        outcomes = run_ingestion(
            tables,
            dates,
            source=args.source,
            force=args.force,
            from_raw=args.from_raw,
            cross_check=not args.no_cross_check,
            skip_dqc=args.skip_dqc,
            data_root=data_root,
            fail_fast=args.fail_fast,
            sleep_seconds=args.sleep,
            run_type=args.run_type,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"回补执行中断: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    for line in format_outcomes(outcomes):
        print(line)

    return 1 if any(o.status == "failed" for o in outcomes) else 0


if __name__ == "__main__":
    sys.exit(main())
