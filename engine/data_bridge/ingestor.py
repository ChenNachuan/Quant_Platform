"""
坚果云数据同步桥 — Mac 端数据入库器

职责:
1. watchdog 监听 trans/responses/ 目录
2. .parquet → 读取入库 → 移动到 archive/
3. _error.json → 记录日志/告警 → 移动到 failed/
4. 启动时扫描遗留文件 (housekeeping)
5. 清理 archive/ 中超过 7 天的文件
"""

import json
import logging
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from engine.data_bridge.protocol import safe_read_json

logger = logging.getLogger(__name__)


class ResponseHandler(FileSystemEventHandler):
    """
    Watchdog 事件处理器。

    关键: atomic_write 的 os.rename 触发的是 on_moved 而非 on_created。
    坚果云也会生成 .~nutstore_tmp... 隐藏文件，必须严格过滤后缀。
    """

    def __init__(self, ingestor: "DataIngestor"):
        self.ingestor = ingestor

    def _is_valid_file(self, path: str) -> bool:
        return path.endswith(".parquet") or path.endswith("_error.json")

    def on_created(self, event):
        if not event.is_directory and self._is_valid_file(event.src_path):
            logger.debug(f"on_created: {event.src_path}")
            self.ingestor._process_response(Path(event.src_path))

    def on_moved(self, event):
        # atomic_write 的 os.rename 触发此事件
        if not event.is_directory and self._is_valid_file(event.dest_path):
            logger.debug(f"on_moved: {event.dest_path}")
            self.ingestor._process_response(Path(event.dest_path))


class DataIngestor:
    def __init__(self, sync_dir: Path, storage):
        """
        Args:
            sync_dir: 坚果云同步根目录, e.g. ~/Nutstore/trans/
            storage: StorageManager 实例
        """
        self.sync_dir = sync_dir
        self.responses_dir = sync_dir / "responses"
        self.archive_dir = sync_dir / "archive"
        self.failed_dir = sync_dir / "failed"
        self.storage = storage

        # 确保目录存在
        for d in [self.responses_dir, self.archive_dir, self.failed_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def start(self) -> None:
        """
        启动入库器:
        1. housekeeping (扫描遗留文件 + 清理旧 archive)
        2. 启动 watchdog 监听
        """
        logger.info(f"DataIngestor 启动，监听目录: {self.responses_dir}")

        # 1. 处理历史遗留文件
        self._scan_pending_responses()

        # 2. 清理旧 archive
        self.housekeeping()

        # 3. 启动 watchdog
        handler = ResponseHandler(self)
        observer = Observer()
        observer.schedule(handler, str(self.responses_dir), recursive=False)
        observer.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("DataIngestor 停止")
            observer.stop()
        observer.join()

    def _scan_pending_responses(self) -> None:
        """启动时扫描 responses/ 中已有的文件"""
        pending = list(self.responses_dir.glob("*"))
        # 过滤掉坚果云临时文件
        pending = [f for f in pending if not f.name.startswith(".~")]
        if pending:
            logger.info(f"扫描到 {len(pending)} 个待处理的响应文件")
            for f in pending:
                if f.suffix in (".parquet", ".json"):
                    self._process_response(f)

    def _process_response(self, path: Path) -> None:
        """
        处理单个响应文件:
        - .parquet → 读取入库 → 移动到 archive/
        - _error.json → 记录日志 → 移动到 failed/
        """
        if path.name.endswith("_error.json"):
            self._handle_error(path)
        elif path.suffix == ".parquet":
            self._handle_parquet(path)
        else:
            logger.debug(f"忽略非目标文件: {path.name}")

    def _handle_parquet(self, path: Path) -> None:
        """读取 Parquet → 入库 → 归档"""
        task_id = path.stem
        try:
            df = pd.read_parquet(path)
            if df.empty:
                logger.warning(f"空 Parquet 文件: {path.name}")
                self._move_to_failed(path, "empty dataframe")
                return

            # 入库
            # 根据文件名推断 category 和 market
            # 约定: {task_id}.parquet 中 task_id 格式为 task_{timestamp}
            # 数据的 data_type 信息从 task JSON 获取，或者从 parquet 的 schema 推断
            logger.info(f"入库: {path.name}, {len(df)} 行, 列: {list(df.columns)}")

            # 调用 StorageManager 写入
            self.storage.write_data(
                df,
                category="market_data",
                market="cn_stock",
                frequency="1d",
            )

            # 移动到 archive
            dest = self.archive_dir / f"{task_id}_{datetime.now():%Y%m%d_%H%M%S}.parquet"
            shutil.move(str(path), str(dest))
            logger.info(f"已归档: {dest.name}")

        except Exception as e:
            logger.error(f"处理 Parquet 失败 {path.name}: {e}")
            self._move_to_failed(path, str(e))

    def _handle_error(self, path: Path) -> None:
        """处理 Windows 端返回的错误 JSON"""
        data = safe_read_json(path)
        task_id = path.stem.replace("_error", "")

        if data:
            error_msg = data.get("error", "unknown error")
            logger.error(f"Windows 端报告错误 [{task_id}]: {error_msg}")
        else:
            logger.error(f"无法读取错误文件: {path.name}")

        # 移动到 failed/
        dest = self.failed_dir / path.name
        shutil.move(str(path), str(dest))
        logger.info(f"已移入 failed/: {dest.name}")

    def _move_to_failed(self, path: Path, reason: str) -> None:
        """将处理失败的文件移到 failed/ 目录"""
        # 写一个 error JSON 旁路文件
        error_path = self.failed_dir / f"{path.stem}_error.json"
        error_data = {
            "task_id": path.stem,
            "error": reason,
            "timestamp": datetime.now().isoformat(),
        }
        with open(error_path, "w", encoding="utf-8") as f:
            json.dump(error_data, f, ensure_ascii=False, indent=2)

        # 移动原文件
        dest = self.failed_dir / path.name
        if path.exists():
            shutil.move(str(path), str(dest))
        logger.info(f"已移入 failed/: {path.name} (原因: {reason})")

    def housekeeping(self, max_age_days: int = 7) -> None:
        """清理 archive/ 中超过 max_age_days 天的文件"""
        cutoff = datetime.now() - timedelta(days=max_age_days)
        removed = 0

        for f in self.archive_dir.iterdir():
            if f.is_file():
                mtime = datetime.fromtimestamp(f.stat().st_mtime)
                if mtime < cutoff:
                    f.unlink()
                    removed += 1

        if removed:
            logger.info(f"Housekeeping: 清理了 {removed} 个过期归档文件 (>{max_age_days}天)")
