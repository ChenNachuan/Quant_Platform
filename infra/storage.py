"""
Version: 0.1.0
Date: 2026-02-15
修改内容: 初次生成后代码审计，将所有因子值存储在一起便于多因子
"""

import duckdb
import pandas as pd
import tomli
from pathlib import Path
from typing import List, Optional, Any, Dict, Union, Literal
import logging

# 配置日志记录器
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    配置加载器
    """

    # 存储已经加载的配置
    # 前面下划线表示是在类内部使用的受保护变量
    # Optional 说明可以是要求的对应格式的 Dict，也可以是None，并且初始化为 None
    _config: Optional[Dict[str, Any]] = None

    @classmethod
    def load(cls) -> Dict[str, Any]:
        """
        加载全局配置文件 (单例模式)

        密钥加载优先级:
        1. 环境变量 (最高优先级)
        2. 项目根目录 .env 文件 (本地开发)
        3. settings.toml 中的明文值 (已清空，仅作 fallback)
        """
        if cls._config is None:
            import os
            # 获取项目根目录（二级父目录）
            cls.PROJECT_ROOT = Path(__file__).parent.parent

            # 优先加载 .env 文件（不覆盖已存在的环境变量，override=False）
            try:
                from dotenv import load_dotenv
                env_path = cls.PROJECT_ROOT / ".env"
                load_dotenv(dotenv_path=env_path, override=False)
                if env_path.exists():
                    logger.info(f"已从 .env 加载环境变量: {env_path}")
            except ImportError:
                logger.warning("python-dotenv 未安装，跳过 .env 加载。请运行: uv sync")

            # 设定 config 的目录
            config_path = cls.PROJECT_ROOT / "config" / "settings.toml"
            if not config_path.exists():
                logger.error(f"配置文件未找到: {config_path}")
                raise FileNotFoundError(f"配置文件未找到: {config_path}")

            # 二进制读取，并解析成 dict，赋值给 cls._config
            with open(config_path, "rb") as f:
                cls._config = tomli.load(f)

            # 将 PROJECT_ROOT 注入到配置中，方便后续引用
            cls._config['project_root'] = str(cls.PROJECT_ROOT)

            # 用环境变量覆盖 toml 中的敏感字段（优先级：env > toml）
            tushare_token = os.environ.get('TUSHARE_TOKEN', '').strip()
            if tushare_token:
                cls._config.setdefault('tushare', {})['token'] = tushare_token
                logger.info("Tushare Token 已从环境变量 TUSHARE_TOKEN 加载")
            elif not cls._config.get('tushare', {}).get('token'):
                logger.warning(
                    "⚠️  未检测到 TUSHARE_TOKEN 环境变量，且 settings.toml 中 token 为空。\n"
                    "   请在项目根目录创建 .env 文件并添加：TUSHARE_TOKEN=<your_token>"
                )

            # --- 设置 ZVT_HOME 环境变量 ---
            zvt_home = cls._config.get('zvt', {}).get('zvt_home')
            if zvt_home:
                # 解析相对路径
                zvt_home_path = Path(zvt_home)

                # 将相对路径拼接为完整路径
                if not zvt_home_path.is_absolute():
                    zvt_home_path = (cls.PROJECT_ROOT / zvt_home_path).resolve()

                # 设置环境变量，确保 ZVT 初始化时能读取到
                os.environ['ZVT_HOME'] = str(zvt_home_path)
                logger.info(f"设置 ZVT_HOME: {zvt_home_path}")

        return cls._config


class StorageManager:
    """
    存储管理器
    负责基于 DuckDB 和 Parquet 的数据持久化与检索操作。
    使用 project_root 作为根目录，
    使用 data_lake_dir 作为数据湖目录，不存在情况下使用 ':memory:' 纯内存模式
    调用 refresh_views() 注册现有 parquet 视图
    """

    def __init__(self):

        # 使用类方法获取 config
        self.config: Dict[str, Any] = ConfigLoader.load()

        # 读取绝对路径
        self.project_root = Path(self.config['project_root'])

        # 解析数据湖路径 (支持相对路径)
        raw_data_lake_dir = self.config['data']['data_lake_dir']
        self.data_lake_dir = (self.project_root / raw_data_lake_dir).resolve()

        # 1. 确保根目录存在（不存在的情况下创建文件夹）
        self.data_lake_dir.mkdir(parents=True, exist_ok=True)

        # 2. 初始化 DuckDB 连接，默认使用 config 的路径，如果没有配置，使用':memory:'纯内存模式
        raw_db_path: str = self.config.get('duckdb', {}).get('db_path', ':memory:')
        if raw_db_path == ':memory:':
            db_path = ':memory:'
        else:
            # 转化为绝对路径，保证目录存在
            db_path = str((self.project_root / raw_db_path).resolve())
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.conn = duckdb.connect(database=db_path)
        logger.info(f"DuckDB 连接已建立: {db_path}")

        # 3. 注册所有现有的 Parquet 数据为视图
        self.refresh_views()

        # 4. 创建复权视图
        self.create_adjusted_views()

    def refresh_views(self) -> None:
        """
        递归扫描数据湖，注册视图。
        视图命名规则: {category}_{market}_{frequency} (e.g., market_data_cn_stock_1d)
        """
        logger.info("正在刷新 DuckDB 视图...")
        try:
            # 1. Market Data 视图 (标准结构: market_data/market/frequency)
            self._register_views_for_root("market_data", prefix="market")

            # 2. 因子视图 (标准结构: factors/factor_name/frequency)
            self._register_views_for_root("factors", prefix="factors")

            # 3. Financial/Macro (按需扩展)

        except Exception as e:
            logger.warning(f"视图刷新部分失败: {e}")

    def _register_views_for_root(self, root_name: str, prefix: str) -> None:
        """
        扫描指定根目录下的二级子目录 (Cat/Freq) 并注册视图。
        """
        root_path = self.data_lake_dir / root_name
        if not root_path.exists():
            return

        # 使用 glob 直接查找所有二级子目录 (category/freq)
        # 修改逻辑：支持 cn_stock/1d 这种结构
        # 现在的结构 data_lake/market_data/cn_stock/1d/year=2023/data.parquet
        for market_path in root_path.iterdir():
            if not market_path.is_dir():
                continue
            
            market_name = market_path.name
            
            for freq_path in market_path.iterdir():
                if not freq_path.is_dir():
                    continue
                
                freq_name = freq_path.name

                # 视图命名: {prefix}_{market}_{freq}
                # e.g.: market_cn_stock_1d
                view_name = f"{prefix}_{market_name}_{freq_name}"

                # 检查是否有数据
                if list(freq_path.glob("**/*.parquet")):
                    # 关键修改：启用 hive_partitioning=1 自动识别 year/month 分区
                    sql = f"""
                    CREATE OR REPLACE VIEW {view_name} AS 
                    SELECT * FROM read_parquet('{freq_path}/**/*.parquet', union_by_name=true, hive_partitioning=1)
                    """
                    self.conn.execute(sql)
                    logger.debug(f"注册视图: {view_name}")

    def create_adjusted_views(self) -> None:
        """
        创建复权价格视图 (QFQ/HFQ)。
        基于新的单表结构：data_lake/market_data/cn_stock/1d/year=YYYY/data.parquet
        该表包含 open, high, low, close, volume, qfq_factor, hfq_factor
        """
        logger.info("正在创建复权视图...")
        
        # 基础数据表视图名
        base_view = "market_cn_stock_1d"
        
        # 检查基础视图是否存在
        try:
            self.conn.execute(f"DESCRIBE {base_view}")
        except:
            logger.warning(f"基础视图 {base_view} 不存在，跳过复权视图创建")
            return

        # 视图 1: 前复权 (QFQ)
        try:
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW cn_stock_1d_qfq AS
                SELECT 
                    timestamp,
                    code as symbol,
                    entity_id,
                    open * qfq_factor AS open,
                    high * qfq_factor AS high,
                    low * qfq_factor AS low,
                    close * qfq_factor AS close,
                    CASE WHEN qfq_factor > 0 THEN volume / qfq_factor ELSE volume END AS volume,
                    turnover,
                    qfq_factor,
                    year,
                    month(timestamp) as month
                FROM {base_view}
                WHERE qfq_factor IS NOT NULL
            """)
            logger.debug("已创建视图: cn_stock_1d_qfq")
        except Exception as e:
            logger.warning(f"创建 QFQ 视图失败: {e}")
        
        # 视图 2: 后复权 (HFQ)
        try:
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW cn_stock_1d_hfq AS
                SELECT 
                    timestamp,
                    code as symbol,
                    entity_id,
                    open * hfq_factor AS open,
                    high * hfq_factor AS high,
                    low * hfq_factor AS low,
                    close * hfq_factor AS close,
                    CASE WHEN hfq_factor > 0 THEN volume / hfq_factor ELSE volume END AS volume,
                    turnover,
                    hfq_factor,
                    year,
                    month(timestamp) as month
                FROM {base_view}
                WHERE hfq_factor IS NOT NULL
            """)
            logger.debug("已创建视图: cn_stock_1d_hfq")
        except Exception as e:
            logger.warning(f"创建 HFQ 视图失败: {e}")

    def write_data(self,
                   df: pd.DataFrame,
                   category: str = 'market_data',
                   market: str = 'cn_stock',
                   frequency: str = '1d',
                   partition_cols=None) -> None:
        """
        通用数据写入接口。
        
        Path: data_lake/{category}/{market}/{frequency}/year=YYYY/data.parquet

        Args:
            df (pd.DataFrame): 数据框
            category (str): 数据类别 (market_data, financial, macro)
            market (str): 市场 (cn_stock, us_stock)
            frequency (str): 频率 (1d, 15m, 1m)
            partition_cols (List[str]): 分区键
        """
        if partition_cols is None:
            partition_cols = ['year']

        if df.empty:
            logger.warning("尝试写入空 DataFrame，操作已跳过")
            return

        # 构建存储路径: data_lake/market_data/cn_stock/1d
        target_dir = self.data_lake_dir / category / market / frequency
        target_dir.mkdir(parents=True, exist_ok=True)

        df_to_write = df.copy()

        # 预处理: 确保时间戳和分区列
        if 'timestamp' not in df_to_write.columns and isinstance(df_to_write.index, pd.DatetimeIndex):
            df_to_write['timestamp'] = df_to_write.index

        if 'timestamp' in df_to_write.columns:
            # Parquet 标准存储 UTC，读取时需注意
            df_to_write['timestamp'] = pd.to_datetime(df_to_write['timestamp'])
            
            # 统一转为 Naive Time (移除时区信息，避免DuckDB处理复杂化)
            if df_to_write['timestamp'].dt.tz is not None:
                df_to_write['timestamp'] = df_to_write['timestamp'].dt.tz_convert(None)

            if 'year' in partition_cols:
                # noinspection PyTypeChecker
                df_to_write['year'] = df_to_write['timestamp'].dt.year
            if 'month' in partition_cols:
                # noinspection PyTypeChecker
                df_to_write['month'] = df_to_write['timestamp'].dt.month

        try:
            # 使用 unique id 避免文件名冲突 (覆盖 vs 追加)
            import uuid
            unique_id = uuid.uuid4().hex[:8]
            
            df_to_write.to_parquet(
                target_dir,
                engine='pyarrow',
                partition_cols=partition_cols,
                index=False,
                existing_data_behavior='overwrite_or_ignore',
                basename_template=f'data_{unique_id}_{{i}}.parquet'
            )
            logger.info(f"成功写入 {len(df_to_write)} 条数据 -> {category}/{market}/{frequency}")

        except Exception as e:
            logger.error(f"数据写入失败: {e}")
            raise RuntimeError(f"数据写入失败: {e}")

    def write_factor(self,
                     df: pd.DataFrame,
                     factor_name: str,
                     frequency: str = '1d') -> None:
        """
        统一因子存储接口。
        
        Args:
            frequency:
            df: 必须包含 entity_id, timestamp, value 列。
            factor_name: 因子名称 (e.g., 'Momentum').
        """
        # 1. 检查必要列
        required = {'entity_id', 'timestamp', 'value'}
        if not required.issubset(df.columns):
            raise ValueError(f"因子数据必须包含 {required} 列")

        # 2. 注入因子名称 (如果不存在)
        df = df.copy()
        if 'factor_name' not in df.columns:
            df['factor_name'] = factor_name

        # 3. 写入 (存储路径: factors/{factor_name}/{freq})
        # 这样物理上隔离，逻辑上可以通过 glob 读取所有
        self.write_data(
            df,
            category='factors',
            market=factor_name,  # 借用 market 字段作为因子名分区
            frequency=frequency,
            partition_cols=['year', 'month']
        )

    def get_factor_matrix(self,
                          factor_names: List[str],
                          start_date: str = '2020-01-01',
                          end_date: str = '2029-12-31',
                          frequency: str = '1d') -> pd.DataFrame:
        """
        获取因子矩阵 (Wide Format)。
        返回: Index=timestamp, Columns=MultiIndex(factor, entity_id) or similar?
        通常我们需要 (timestamp, entity_id) -> [factor1, factor2...]
        """
        # 1. 动态注册所有因子视图
        self.refresh_views()

        # 2. 构建查询
        # 直接读取 Parquet 文件列表，利用 DuckDB 的 Ad-hoc 查询能力动态聚合
        path_list = [
            str(self.data_lake_dir / 'factors' / name / frequency / '**/*.parquet')
            for name in factor_names
        ]

        # DuckDB 0.10+ 支持 read_parquet list
        # 构造 SQL: SELECT * FROM read_parquet([...])
        paths_str = ", ".join([f"'{p}'" for p in path_list])

        sql = f"""
            SELECT 
                timestamp, 
                entity_id, 
                factor_name,
                value
            FROM read_parquet([{paths_str}], hive_partitioning=true, union_by_name=true)
            WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'
        """

        # 3. Pivot (Long -> Wide)
        # DuckDB Pivot 语法: PIVOT <table> ON <column> USING <agg>
        pivot_sql = f"""
            PIVOT (
                {sql}
            ) ON factor_name USING first(value) GROUP BY timestamp, entity_id
            ORDER BY timestamp, entity_id
        """

        logger.info(f"执行因子矩阵查询 (Pivot): {factor_names}")
        df = self.conn.execute(pivot_sql).df()

        # 转换为标准的 Panel Data 格式: MultiIndex(timestamp, entity_id)
        # 这样每一行由 (时间, 标的) 唯一确定，列为各因子值
        if not df.empty and 'timestamp' in df.columns and 'entity_id' in df.columns:
            df.set_index(['timestamp', 'entity_id'], inplace=True)
            df.sort_index(inplace=True)

        return df

    def query(self, sql: str) -> pd.DataFrame:
        """
        执行 SQL 查询。
        """
        logger.debug(f"执行 SQL: {sql}")
        return self.conn.execute(sql).df()

    def get_data(self,
                 market: str = 'cn_stock',
                 frequency: str = '1d',
                 codes: Optional[List[str]] = None,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None) -> pd.DataFrame:
        """
        快捷查询接口。
        自动路由到对应的视图: `market_{market}_{frequency}`
        """
        view_name = f"market_{market}_{frequency}"

        # 检查视图是否存在
        try:
            self.conn.execute(f"DESCRIBE {view_name}")
        except duckdb.CatalogException:
            # 尝试刷新视图
            self._refresh_views()
            try:
                self.conn.execute(f"DESCRIBE {view_name}")
            except:
                logger.warning(f"视图 {view_name} 不存在 (可能是数据尚未写入)")
                return pd.DataFrame()

        query_parts = [f"SELECT * FROM {view_name} WHERE 1=1"]
        params = []

        if codes:
            # DuckDB 绑定列表参数需要 list[str]
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

        # 使用 execute 自带的参数绑定
        df = self.conn.execute(full_query, parameters=params).df()

        # 统一返回 MultiIndex (timestamp, entity_id)
        if not df.empty and 'timestamp' in df.columns and 'entity_id' in df.columns:
            df.set_index(['timestamp', 'entity_id'], inplace=True)
            df.sort_index(inplace=True)

        return df

    def close(self) -> None:
        """关闭数据库连接"""
        try:
            self.conn.close()
            logger.info("DuckDB 连接已关闭")
        except Exception as e:
            logger.warning(f"关闭连接失败: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    # 冒烟测试 (Smoke Test)
    try:
        print("=== StorageManager 冒烟测试 ===")
        with StorageManager() as db:
            print(f"数据湖根目录: {db.data_lake_dir}")

            # 测试配置加载
            config = ConfigLoader.load()
            print(f"当前环境配置: {config.get('project', {}).get('name')}")

            # 测试视图刷新
            db.refresh_views()

            # 打印当前视图
            res = db.query("SHOW TABLES")
            print("\n当前 DuckDB 视图列表:")
            print(res)

        print("\n=== 测试成功完成 ===")
    except Exception as e:
        logger.error(f"测试失败: {e}")
