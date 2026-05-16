"""
坚果云数据同步桥 — Mac 端任务发布器

职责:
1. 对比本地数据湖，找出缺失的时间段
2. 排除已在 trans/requests/ 中的 in-flight 任务
3. 生成 TaskRequest JSON 写入 trans/requests/
"""

import logging
import time
from pathlib import Path
from engine.data_bridge.protocol import TaskRequest, atomic_write_json, safe_read_json

logger = logging.getLogger(__name__)


class TaskPublisher:
    def __init__(self, sync_dir: Path):
        """
        Args:
            sync_dir: 坚果云同步根目录, e.g. ~/Nutstore/trans/
        """
        self.sync_dir = sync_dir
        self.requests_dir = sync_dir / "requests"
        self.requests_dir.mkdir(parents=True, exist_ok=True)

    def publish_tasks(self, tasks: list[TaskRequest]) -> int:
        """
        将任务列表逐个写入 requests/ 目录。
        返回实际写入的任务数。
        """
        written = 0
        for task in tasks:
            path = self.requests_dir / f"{task.task_id}.json"
            if path.exists():
                logger.warning(f"任务文件已存在，跳过: {path}")
                continue
            atomic_write_json(path, task.to_dict())
            logger.info(f"发布任务: {task.task_id} ({task.data_type}, {len(task.entity_ids)} 个标的)")
            written += 1
        return written

    def get_in_flight_tasks(self) -> list[TaskRequest]:
        """扫描 requests/ 目录，获取所有正在飞行中的任务"""
        in_flight = []
        for f in self.requests_dir.glob("*.json"):
            data = safe_read_json(f)
            if data:
                try:
                    in_flight.append(TaskRequest(**data))
                except (TypeError, KeyError) as e:
                    logger.warning(f"解析 in-flight 任务失败 {f}: {e}")
        return in_flight

    def find_missing_data(
        self,
        entity_ids: list[str],
        start: str,
        end: str,
        data_type: str,
        provider: str = "amazingdata",
        storage=None,
    ) -> list[TaskRequest]:
        """
        查询本地 data_lake，找出缺失的时间段，生成任务。
        排除已在 trans/requests/ 中的 in-flight 任务，防止重复生成。

        Args:
            entity_ids: 标的列表 e.g. ["stock_sz_000001", ...]
            start: 起始日期 "2026-05-01"
            end: 结束日期 "2026-05-15"
            data_type: 数据类型 "stock_1d_kdata" / "stock_adj_factor"
            provider: 数据源 "amazingdata" / "akshare"
            storage: StorageManager 实例 (可选，用于查询本地数据)
        """
        # 1. 获取 in-flight 任务
        in_flight = self.get_in_flight_tasks()
        in_flight_keys = set()
        for t in in_flight:
            for eid in t.entity_ids:
                in_flight_keys.add((eid, t.start_timestamp, t.end_timestamp))

        # 2. 过滤已在飞行中的标的+时间段
        missing_ids = []
        for eid in entity_ids:
            if (eid, start, end) not in in_flight_keys:
                missing_ids.append(eid)

        if not missing_ids:
            logger.info("所有任务均已在飞行中，无需重复生成")
            return []

        # 3. 如果有 storage，进一步检查本地数据是否存在
        if storage is not None:
            actually_missing = self._check_db_missing(
                storage, missing_ids, start, end, data_type
            )
        else:
            actually_missing = missing_ids

        if not actually_missing:
            logger.info("本地数据已完整，无需生成任务")
            return []

        # 4. 生成 TaskRequest
        task = TaskRequest(
            task_id=f"task_{int(time.time())}",
            provider=provider,
            data_type=data_type,
            entity_ids=actually_missing,
            start_timestamp=start,
            end_timestamp=end,
            status="pending",
        )
        logger.info(f"生成任务: {task.task_id}, {len(actually_missing)} 个标的缺失数据")
        return [task]

    def _check_db_missing(
        self, storage, entity_ids: list[str], start: str, end: str, data_type: str
    ) -> list[str]:
        """查询 DuckDB，找出确实没有数据的标的"""
        try:
            # 根据 data_type 确定查询的表
            table_map = {
                "stock_1d_kdata": "cn_stock_1d",
                "stock_adj_factor": "cn_stock_adj_factor",
            }
            table = table_map.get(data_type)
            if not table:
                logger.warning(f"未知 data_type: {data_type}，跳过 DB 检查")
                return entity_ids

            # 批量查询哪些 entity_id 在指定时间段有数据
            placeholders = ", ".join(f"'{eid}'" for eid in entity_ids)
            result = storage.query(f"""
                SELECT DISTINCT entity_id
                FROM {table}
                WHERE entity_id IN ({placeholders})
                  AND timestamp >= '{start}'
                  AND timestamp <= '{end}'
            """)
            existing = set(result["entity_id"].tolist()) if not result.empty else set()
            missing = [eid for eid in entity_ids if eid not in existing]

            logger.info(f"DB 检查: {len(existing)} 有数据, {len(missing)} 缺失")
            return missing

        except Exception as e:
            logger.warning(f"DB 检查失败，返回全部标的: {e}")
            return entity_ids
