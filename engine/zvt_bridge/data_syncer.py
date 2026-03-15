"""
Version: 0.1.0
Date: 2026-02-17
修改内容: 初次生成后代码审计，数据来源问题未解决，影响数据库结构和数据更新未解决

TODO:  1. 修改数据源使用 Tushare 和 AKShare 两者加载后比对无误后保存
       2. 保留 raw 数据和 adj_factors 数据，实现 qfg, hfq, 以及动态前复权这三种复权方式（qfq 和 hfq 可以更新并保存）
       3. **[数据湖 Schema 兼容性]** 历史 Parquet 文件使用 TIMESTAMP_NS，新文件使用 TIMESTAMP WITH TIME ZONE，
          导致 DuckDB 视图创建失败。需要清空并重新抓取全历史数据，或编写迁移脚本统一 Schema。
       4. Stock 列表检查现在数据源是东方财富，考虑后续换成 Tushare 的 stock_basic
"""

import pandas as pd
from typing import List, Optional, Union
from datetime import datetime
import sys
import logging
from pathlib import Path

# 配置日志
logger = logging.getLogger(__name__)

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from infra.storage import StorageManager, ConfigLoader

# 必须在导入 ZVT 之前加载配置，以确保 ZVT_HOME 环境变量被正确设置
ConfigLoader.load()

# 导入 ZVT 模块
from zvt.domain import Stock1dKdata, Stock, Stock1dHfqKdata
from zvt.contract.api import get_data
from zvt.contract.api import get_data
# 使用相对引用，更简洁
from engine.zvt_bridge.recorders.akshare.stock_1d_kdata_recorder import register_akshare_recorders
from engine.zvt_bridge.recorders.akshare.stock_adj_factor_recorder import register_akshare_factor_recorder
from engine.zvt_bridge.domain.stock_adj_factor import StockAdjFactor


class ZvtDataSyncer:
    """
    ZVT 数据同步器
    负责桥接 ZVT 数据抓取与 Parquet 列式存储。
    将 ZVT 抓取的 Pandas DataFrame 清洗并标准化，写入数据湖。
    """

    def __init__(self):
        self.storage = StorageManager()
        # 注册 AKShare Recorder
        # 注册 AKShare Recorder
        register_akshare_recorders()
        register_akshare_factor_recorder()

    def fetch_and_store_kdata(self,
                              codes: Optional[List[str]] = None,
                              start_date: str = '1990-12-19',
                              end_date: Optional[str] = None) -> None:
        """
        从 ZVT 获取 K 线数据并存储至数据湖。
        
        策略:
        1. 抓取 Raw OHLCV 数据
        2. 抓取复权因子 (qfq_factor, hfq_factor)
        3. 合并为宽表 (Timestamp, Symbol, OHLCV, Factors)
        4. 写入单一 Parquet 文件 (market_data/cn_stock/1d)
        """
        logger.info(f"开始抓取 RAW 数据: codes={codes if codes else '全市场'}, start={start_date}")

        # 0. 检查 Stock 元数据
        if Stock.query_data(provider='em', codes=codes).empty:
            logger.info("请求的代码缺少元数据 (StockMetadata)。正在录入股票列表数据 (ZVT)...")
            Stock.record_data(provider='em', codes=codes, sleeping_time=0.1)

        # 1. 获取 Raw Data (不复权)
        try:
            logger.info("正在抓取 Raw Data (AKShare)...")
            Stock1dKdata.record_data(
                codes=codes,
                provider='akshare',
                start_timestamp=start_date,
                end_timestamp=end_date,
                sleeping_time=0.1
            )

            df_raw = Stock1dKdata.query_data(
                codes=codes,
                start_timestamp=start_date,
                end_timestamp=end_date,
                provider='akshare'
            )
        except Exception as e:
            logger.error(f"ZVT Raw Data 抓取失败: {e}")
            return

        if df_raw is None or df_raw.empty:
            logger.warning("未获取到 Raw Data")
            return

        logger.info(f"Raw Data 获取成功: {len(df_raw)} 条")

        # 2. 获取复权因子
        df_factors = None
        try:
            logger.info(f"正在抓取复权因子 (AKShare)...")
            StockAdjFactor.record_data(
                codes=codes,
                provider='akshare',
                start_timestamp=start_date,
                end_timestamp=end_date,
                sleeping_time=0.1
            )

            df_factors = StockAdjFactor.query_data(
                codes=codes,
                start_timestamp=start_date,
                end_timestamp=end_date,
                provider='akshare'
            )

            if df_factors is not None and not df_factors.empty:
                logger.info(f"复权因子获取成功: {len(df_factors)} 条")
            else:
                logger.warning("未获取到复权因子数据")

        except Exception as e:
            logger.error(f"复权因子获取失败: {e}")

        # 3. 数据合并与清洗
        try:
            # 准备 Raw Data
            raw_cols = ['entity_id', 'timestamp', 'code', 'name', 'open', 'high', 'low', 'close', 'volume', 'turnover']
            df_raw = df_raw.reset_index() if 'timestamp' not in df_raw.columns else df_raw
            df_merged = df_raw[[c for c in raw_cols if c in df_raw.columns]].copy()

            # 合并因子
            if df_factors is not None and not df_factors.empty:
                df_factors = df_factors.reset_index() if 'timestamp' not in df_factors.columns else df_factors
                factor_cols = ['entity_id', 'timestamp', 'qfq_factor', 'hfq_factor']

                # 确保时间类型一致
                df_merged['timestamp'] = pd.to_datetime(df_merged['timestamp'])
                df_factors['timestamp'] = pd.to_datetime(df_factors['timestamp'])

                df_merged = pd.merge(
                    df_merged,
                    df_factors[[c for c in factor_cols if c in df_factors.columns]],
                    on=['entity_id', 'timestamp'],
                    how='left'
                )

            logger.info(f"合并后数据: {len(df_merged)} 条")

            # 4. 写入存储 (Schema自动对齐)
            # category='market_data', market='cn_stock', frequency='1d'
            self.storage.write_data(
                df_merged,
                category='market_data',
                market='cn_stock',
                frequency='1d'
            )

        except Exception as e:
            logger.error(f"数据处理/写入失败: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    syncer = ZvtDataSyncer()
    # 示例: 抓取 000004 2023 年至今的数据
    syncer.fetch_and_store_kdata(codes=['000004'], start_date='2023-01-01')
