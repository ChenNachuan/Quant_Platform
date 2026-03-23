"""
DuckDB 查询引擎：视图查询、因子矩阵、通用 SQL。
"""
import logging
from pathlib import Path
from typing import Callable, List, Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class QueryEngine:
    """负责所有 DuckDB 查询操作。"""

    def __init__(self, conn: duckdb.DuckDBPyConnection, data_lake_dir: Path,
                 refresh_fn: Callable[[], None]):
        self.conn = conn
        self.data_lake_dir = data_lake_dir
        self._refresh_views = refresh_fn

    def get_factor_matrix(self,
                          factor_names: List[str],
                          start_date: str = '2020-01-01',
                          end_date: str = '2029-12-31',
                          frequency: str = '1d') -> pd.DataFrame:
        """获取因子矩阵 (Wide Format)，返回 MultiIndex(timestamp, entity_id)。"""
        self._refresh_views()

        path_list = [
            str(self.data_lake_dir / 'factors' / name / frequency / '**/*.parquet')
            for name in factor_names
        ]
        paths_str = ", ".join([f"'{p}'" for p in path_list])

        sql = f"""
            SELECT timestamp, entity_id, factor_name, value
            FROM read_parquet([{paths_str}], hive_partitioning=true, union_by_name=true)
            WHERE timestamp >= $1 AND timestamp <= $2
        """
        pivot_sql = f"""
            PIVOT ({sql}) ON factor_name USING first(value)
            GROUP BY timestamp, entity_id
            ORDER BY timestamp, entity_id
        """

        logger.info(f"执行因子矩阵查询 (Pivot): {factor_names}")
        df = self.conn.execute(pivot_sql, [start_date, end_date]).df()

        if not df.empty and 'timestamp' in df.columns and 'entity_id' in df.columns:
            df.set_index(['timestamp', 'entity_id'], inplace=True)
            df.sort_index(inplace=True)
        return df

    def query(self, sql: str, params: list | None = None) -> pd.DataFrame:
        """执行参数化 SQL 查询。"""
        logger.debug(f"执行 SQL: {sql}")
        return self.conn.execute(sql, params or []).df()

    def get_data(self,
                 market: str = 'cn_stock',
                 frequency: str = '1d',
                 codes: Optional[List[str]] = None,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None) -> pd.DataFrame:
        """快捷查询接口，自动路由到 market_{market}_{frequency} 视图。"""
        view_name = f"market_{market}_{frequency}"

        try:
            self.conn.execute(f"DESCRIBE {view_name}")
        except duckdb.CatalogException:
            self._refresh_views()
            try:
                self.conn.execute(f"DESCRIBE {view_name}")
            except Exception:
                logger.warning(f"视图 {view_name} 不存在 (可能是数据尚未写入)")
                return pd.DataFrame()

        query_parts = [f"SELECT * FROM {view_name} WHERE 1=1"]
        params = []

        if codes:
            query_parts.append("AND entity_id IN (SELECT unnest(?))")
            params.append(codes)
        if start_date:
            query_parts.append("AND timestamp >= ?")
            params.append(start_date)
        if end_date:
            query_parts.append("AND timestamp <= ?")
            params.append(end_date)

        full_query = " ".join(query_parts)
        logger.debug(f"执行 SQL: {full_query} | 参数: {params}")
        df = self.conn.execute(full_query, parameters=params).df()

        if not df.empty and 'timestamp' in df.columns and 'entity_id' in df.columns:
            df.set_index(['timestamp', 'entity_id'], inplace=True)
            df.sort_index(inplace=True)
        return df
