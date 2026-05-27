"""
DuckDB 连接生命周期管理 + 视图注册。
支持 data_lake/{asset_type}/{category}/{sub_type} 三级层次结构。
"""

import duckdb
import logging
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class ConnectionManager:
    """管理 DuckDB 连接及 Parquet 视图注册。"""

    def __init__(self, config: Dict[str, Any]):
        project_root = Path(config["project_root"])

        raw_data_lake_dir = config["data"]["data_lake_dir"]
        self.data_lake_dir = (project_root / raw_data_lake_dir).resolve()
        self.data_lake_dir.mkdir(parents=True, exist_ok=True)

        raw_db_path: str = config.get("duckdb", {}).get("db_path", ":memory:")
        if raw_db_path == ":memory:":
            db_path = ":memory:"
        else:
            db_path = str((project_root / raw_db_path).resolve())
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.conn = duckdb.connect(database=db_path)
        logger.info(f"DuckDB 连接已建立: {db_path}")

        self.refresh_views()

    def refresh_views(self) -> None:
        """扫描 data_lake 三级层次结构，注册 DuckDB 视图。

        视图命名规则: {asset_type}_{category}_{sub_type}
        例: stock_market_data_1d, option_reference_basic_info
        """
        logger.info("正在刷新 DuckDB 视图...")
        try:
            self._scan_and_register_views()
        except Exception as e:
            logger.warning(f"视图刷新部分失败: {e}")

    def _scan_and_register_views(self) -> None:
        """递归扫描 data_lake/{asset_type}/{category}/{sub_type} 并注册视图。"""
        registered = 0

        for asset_dir in sorted(self.data_lake_dir.iterdir()):
            if not asset_dir.is_dir() or asset_dir.name.startswith("."):
                continue
            asset_type = asset_dir.name

            for category_dir in sorted(asset_dir.iterdir()):
                if not category_dir.is_dir() or category_dir.name.startswith("."):
                    continue
                category = category_dir.name

                for sub_dir in sorted(category_dir.iterdir()):
                    if not sub_dir.is_dir() or sub_dir.name.startswith("."):
                        continue
                    sub_type = sub_dir.name

                    # 检查是否有 parquet 文件
                    if not list(sub_dir.glob("**/*.parquet")):
                        continue

                    view_name = f"{asset_type}_{category}_{sub_type}"
                    sql = f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT * FROM read_parquet('{sub_dir}/**/*.parquet', union_by_name=true, hive_partitioning=1)
                    """
                    self.conn.execute(sql)
                    registered += 1
                    logger.debug(f"注册视图: {view_name}")

        logger.info(f"共注册 {registered} 个视图")

    def list_views(self) -> List[str]:
        """列出所有已注册的视图名称。"""
        result = self.conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def close(self) -> None:
        try:
            self.conn.close()
            logger.info("DuckDB 连接已关闭")
        except Exception as e:
            logger.warning(f"关闭连接失败: {e}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
