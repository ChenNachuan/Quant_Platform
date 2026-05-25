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
        self.create_adjusted_views()

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

    def create_adjusted_views(self) -> None:
        """创建前复权/后复权视图（基于 stock_market_data_1d）。"""
        logger.info("正在创建复权视图...")
        base_view = "stock_market_data_1d"
        try:
            self.conn.execute(f"DESCRIBE {base_view}")
        except Exception:
            logger.warning(f"基础视图 {base_view} 不存在，跳过复权视图创建")
            return

        for view_name, factor_col in [
            ("stock_market_data_1d_qfq", "qfq_factor"),
            ("stock_market_data_1d_hfq", "hfq_factor"),
        ]:
            try:
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT
                        timestamp,
                        code as symbol,
                        entity_id,
                        open  * {factor_col} AS open,
                        high  * {factor_col} AS high,
                        low   * {factor_col} AS low,
                        close * {factor_col} AS close,
                        CASE WHEN {factor_col} > 0 THEN volume / {factor_col} ELSE volume END AS volume,
                        turnover,
                        {factor_col},
                        year,
                        month(timestamp) as month
                    FROM {base_view}
                    WHERE {factor_col} IS NOT NULL
                """)
                logger.debug(f"已创建视图: {view_name}")
            except Exception as e:
                logger.warning(f"创建视图 {view_name} 失败: {e}")

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
