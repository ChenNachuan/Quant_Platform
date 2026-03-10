"""
Parquet 数据湖 Schema 迁移脚本
Version: 1.0.0
Date: 2026-03-10

功能：
  遍历 data_lake 下所有 Parquet 文件，将 timestamp 列统一转换为
  timestamp[ns] (Naive, UTC-free) 格式，消除 DuckDB union_by_name
  的类型不兼容报错。

运行方式：
  uv run python scripts/migrate_schema.py
  uv run python scripts/migrate_schema.py --dry-run   # 只检查，不写入
"""

import sys
import logging
import argparse
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# 统一目标类型：Naive Timestamp, ns 精度
TARGET_TYPE = pa.timestamp("ns", tz=None)

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_LAKE_DIR = PROJECT_ROOT / "data_lake"


def detect_timestamp_type(file_path: Path) -> Optional[str]:
    """
    返回文件中 timestamp 列的 Arrow 类型字符串，若无 timestamp 列返回 None。
    """
    try:
        schema = pq.read_schema(file_path)
        if "timestamp" in schema.names:
            return str(schema.field("timestamp").type)
    except Exception as e:
        logger.warning(f"无法读取 Schema: {file_path} -> {e}")
    return None


def migrate_file(file_path: Path, dry_run: bool = False) -> bool:
    """
    原地迁移单个 Parquet 文件的 timestamp 列类型。

    策略：
    - 读取原始 Table
    - 将 timestamp 列强制转换为 TARGET_TYPE (timestamp[ns], tz-naive)
    - 写回同路径，使用 lz4 压缩（速度快、压缩率适中）

    返回：True 表示文件被修改，False 表示无需修改
    """
    ts_type = detect_timestamp_type(file_path)
    if ts_type is None:
        logger.debug(f"跳过（无 timestamp 列）: {file_path.name}")
        return False

    if ts_type == str(TARGET_TYPE):
        logger.debug(f"已是目标格式，跳过: {file_path.name}")
        return False

    logger.info(f"类型不符: {file_path} | {ts_type} -> {TARGET_TYPE}")

    if dry_run:
        return True

    try:
        table = pq.read_table(file_path)

        # 强制转换：先移除时区（cast to ns naive）
        col_idx = table.schema.get_field_index("timestamp")
        old_col = table.column(col_idx)

        # 若是 timestamp with tz，先移除 tz 再降精度/升精度统一为 ns
        if pa.types.is_timestamp(old_col.type) and old_col.type.tz is not None:
            # 转换为 UTC naive：先 cast to utc，再移除 tz
            old_col = old_col.cast(pa.timestamp("ns", tz="UTC"))
            old_col = old_col.cast(TARGET_TYPE)
        else:
            old_col = old_col.cast(TARGET_TYPE)

        table = table.set_column(col_idx, "timestamp", old_col)

        # 先写临时文件，成功后再覆盖原文件，确保原子性
        tmp_path = file_path.with_suffix(".tmp.parquet")
        pq.write_table(table, tmp_path, compression="lz4")
        tmp_path.replace(file_path)

        logger.info(f"✅ 迁移完成: {file_path.name}")
        return True

    except Exception as e:
        logger.error(f"❌ 迁移失败: {file_path} -> {e}")
        # 清理可能残留的临时文件
        tmp_path = file_path.with_suffix(".tmp.parquet")
        if tmp_path.exists():
            tmp_path.unlink()
        return False


def run_migration(data_lake_dir: Path, dry_run: bool = False) -> None:
    """
    扫描并迁移 data_lake 下所有 Parquet 文件。
    """
    all_files = list(data_lake_dir.rglob("*.parquet"))
    total = len(all_files)
    logger.info(f"共发现 {total} 个 Parquet 文件，开始扫描...")

    stats = {"skipped": 0, "migrated": 0, "failed": 0}

    for i, file_path in enumerate(all_files, 1):
        # 过滤临时文件（防御性）
        if file_path.suffix == ".tmp.parquet":
            continue

        ts_type = detect_timestamp_type(file_path)
        if ts_type is None or ts_type == str(TARGET_TYPE):
            stats["skipped"] += 1
            continue

        changed = migrate_file(file_path, dry_run=dry_run)
        if changed:
            stats["migrated"] += 1
        else:
            stats["failed"] += 1

        if i % 10 == 0:
            logger.info(f"进度: {i}/{total}")

    logger.info("\n" + "=" * 50)
    logger.info(f"迁移结果{'（演习模式，未实际写入）' if dry_run else ''}")
    logger.info(f"  无需迁移: {stats['skipped']}")
    logger.info(f"  已迁移:   {stats['migrated']}")
    logger.info(f"  失败:     {stats['failed']}")
    logger.info("=" * 50)

    if stats["failed"] > 0:
        logger.error("存在迁移失败的文件，请检查日志")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="统一 data_lake Parquet 时间戳 Schema")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只检查类型不符的文件，不执行实际写入"
    )
    parser.add_argument(
        "--data-lake",
        type=str,
        default=str(DATA_LAKE_DIR),
        help=f"data_lake 根目录 (默认: {DATA_LAKE_DIR})"
    )
    args = parser.parse_args()

    target_dir = Path(args.data_lake)
    if not target_dir.exists():
        logger.error(f"目录不存在: {target_dir}")
        sys.exit(1)

    run_migration(target_dir, dry_run=args.dry_run)
