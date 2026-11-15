"""日常单日多表数据接入入口。

按 ``source_priority.yaml`` 主源优先级依次执行：
外部拉取（或 ``--from-raw`` 离线读取）-> ``raw/`` 落盘 -> Normalizer -> DQC ->
``asset/`` 原子写入（含 ``source_tag`` 血缘）-> 水位线更新 -> 交叉校验。
主源失败或 DQC 拒收时自动切换备源并标记 ``{backup}:backfill``；显式指定 ``--source`` 时仅尝试该源。
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
    latest_trade_date,
    parse_date,
    resolve_tables,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="按配置优先级执行单日数据接入、DQC 校验与交叉校验"
    )
    parser.add_argument(
        "--date",
        default=None,
        help="目标交易日（YYYY-MM-DD 或 YYYYMMDD，默认取本地日历最近交易日或今日）",
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
        help="按预设分组批量接入表集合",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="显式指定单一数据源（禁用主源失败时自动切换备源）",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="忽略已落盘结果强制重新覆盖写入",
    )
    parser.add_argument(
        "--from-raw",
        action="store_true",
        help="跳过外部拉取，直接读取已有 raw/ 原始文件完成标准化与入库",
    )
    parser.add_argument(
        "--no-cross-check",
        action="store_true",
        help="禁用入库后的自动多源交叉校验",
    )
    parser.add_argument(
        "--skip-dqc",
        action="store_true",
        help="跳过 DQC 规则校验（仍执行 Pandera Schema 契约校验）",
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
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="日志输出级别（默认 INFO）",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """命令行主入口：返回 0 表示全部成功或跳过，1 表示存在失败，2 表示参数错误。"""
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    data_root = Path(args.data_root)
    try:
        tables = resolve_tables(args.tables, args.group)
        trade_date = (
            parse_date(args.date) if args.date else latest_trade_date(data_root)
        )
    except (ValueError, KeyError) as exc:
        print(f"参数错误: {exc}", file=sys.stderr)
        return 2

    try:
        outcomes = run_ingestion(
            tables,
            [trade_date],
            source=args.source,
            force=args.force,
            from_raw=args.from_raw,
            cross_check=not args.no_cross_check,
            skip_dqc=args.skip_dqc,
            data_root=data_root,
            fail_fast=args.fail_fast,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"接入执行中断: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    for line in format_outcomes(outcomes):
        print(line)

    return 1 if any(o.status == "failed" for o in outcomes) else 0


if __name__ == "__main__":
    sys.exit(main())
