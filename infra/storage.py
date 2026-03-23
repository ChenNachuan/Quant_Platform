"""
Version: 0.1.0
Date: 2026-02-15
修改内容: 初次生成后代码审计，将所有因子值存储在一起便于多因子
"""

import tomli
from pathlib import Path
from typing import Any, Dict, Optional
import logging

# 配置日志记录器
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
    def reset(cls) -> None:
        """重置配置缓存，用于测试间隔离。"""
        cls._config = None

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
    存储管理器 Facade。
    委托给 ConnectionManager / DataWriter / QueryEngine 三个职责类。
    公开 API 与原实现完全兼容。
    """

    def __init__(self):
        from infra.connection import ConnectionManager
        from infra.writer import DataWriter
        from infra.query import QueryEngine

        self.config: Dict[str, Any] = ConfigLoader.load()
        self._cm = ConnectionManager(self.config)
        self._writer = DataWriter(self._cm.data_lake_dir)
        self._query = QueryEngine(self._cm.conn, self._cm.data_lake_dir,
                                  refresh_fn=self._cm.refresh_views)

        # 保留直接属性，兼容现有调用方
        self.conn = self._cm.conn
        self.data_lake_dir = self._cm.data_lake_dir
        self.project_root = self._cm.data_lake_dir.parent

    # ── Connection / View ──────────────────────────────────────
    def refresh_views(self) -> None:
        self._cm.refresh_views()

    def create_adjusted_views(self) -> None:
        self._cm.create_adjusted_views()

    # ── Write ──────────────────────────────────────────────────
    def write_data(self, df, category='market_data', market='cn_stock',
                   frequency='1d', partition_cols=None) -> None:
        self._writer.write_data(df, category, market, frequency, partition_cols)

    def write_factor(self, df, factor_name: str, frequency: str = '1d') -> None:
        self._writer.write_factor(df, factor_name, frequency)

    # ── Query ──────────────────────────────────────────────────
    def get_factor_matrix(self, factor_names, start_date='2020-01-01',
                          end_date='2029-12-31', frequency='1d'):
        return self._query.get_factor_matrix(factor_names, start_date, end_date, frequency)

    def query(self, sql: str, params: list | None = None):
        return self._query.query(sql, params)

    def get_data(self, market='cn_stock', frequency='1d', codes=None,
                 start_date=None, end_date=None):
        return self._query.get_data(market, frequency, codes, start_date, end_date)

    # ── Lifecycle ──────────────────────────────────────────────
    def close(self) -> None:
        self._cm.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


if __name__ == "__main__":
    try:
        print("=== StorageManager 冒烟测试 ===")
        with StorageManager() as db:
            print(f"数据湖根目录: {db.data_lake_dir}")
            config = ConfigLoader.load()
            print(f"当前环境配置: {config.get('project', {}).get('name')}")
            db.refresh_views()
            res = db.query("SHOW TABLES")
            print("\n当前 DuckDB 视图列表:")
            print(res)
        print("\n=== 测试成功完成 ===")
    except Exception as e:
        logger.error(f"测试失败: {e}")

