"""
DuckDB 连接生命周期管理 + 视图注册。
"""
import duckdb
import logging
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)


class ConnectionManager:
    """管理 DuckDB 连接及 Parquet 视图注册。"""

    def __init__(self, config: Dict[str, Any]):
        project_root = Path(config['project_root'])

        raw_data_lake_dir = config['data']['data_lake_dir']
        self.data_lake_dir = (project_root / raw_data_lake_dir).resolve()
        self.data_lake_dir.mkdir(parents=True, exist_ok=True)

        raw_db_path: str = config.get('duckdb', {}).get('db_path', ':memory:')
        if raw_db_path == ':memory:':
            db_path = ':memory:'
        else:
            db_path = str((project_root / raw_db_path).resolve())
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.conn = duckdb.connect(database=db_path)
        logger.info(f"DuckDB 连接已建立: {db_path}")

        self.refresh_views()
        self.create_adjusted_views()

    def refresh_views(self) -> None:
        """递归扫描数据湖，注册视图。"""
        logger.info("正在刷新 DuckDB 视图...")
        try:
            self._register_views_for_root("market_data", prefix="market")
            self._register_views_for_root("factors", prefix="factors")
        except Exception as e:
            logger.warning(f"视图刷新部分失败: {e}")

    def _register_views_for_root(self, root_name: str, prefix: str) -> None:
        root_path = self.data_lake_dir / root_name
        if not root_path.exists():
            return

        for market_path in root_path.iterdir():
            if not market_path.is_dir():
                continue
            for freq_path in market_path.iterdir():
                if not freq_path.is_dir():
                    continue
                view_name = f"{prefix}_{market_path.name}_{freq_path.name}"
                if list(freq_path.glob("**/*.parquet")):
                    sql = f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT * FROM read_parquet('{freq_path}/**/*.parquet', union_by_name=true, hive_partitioning=1)
                    """
                    self.conn.execute(sql)
                    logger.debug(f"注册视图: {view_name}")

    def create_adjusted_views(self) -> None:
        """创建前复权/后复权视图。"""
        logger.info("正在创建复权视图...")
        base_view = "market_cn_stock_1d"
        try:
            self.conn.execute(f"DESCRIBE {base_view}")
        except Exception:
            logger.warning(f"基础视图 {base_view} 不存在，跳过复权视图创建")
            return

        for view_name, factor_col in [("cn_stock_1d_qfq", "qfq_factor"), ("cn_stock_1d_hfq", "hfq_factor")]:
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
