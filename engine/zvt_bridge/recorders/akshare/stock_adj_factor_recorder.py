"""
Version: 0.1.0
Date: 2026-02-17
修改内容: 初次生成后代码审计

TODO: 1. Entity 依赖问题：entity_provider='em'，必须先运行 Stock.record_data(provider='em')
         录入股票列表，否则本 Recorder 无法获取股票实体。
      2. QFQ 因子不再存储：前复权因子随分红配股变化，只存储 HFQ 因子。
"""


from typing import List
import akshare as ak
import pandas as pd
from zvt.contract import IntervalLevel, AdjustType
from zvt.contract.api import df_to_db, get_db_session_factory, get_db_engine, _get_db_name
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Stock
from zvt.utils.time_utils import to_pd_timestamp, to_date_time_str, TIME_FORMAT_DAY
from zvt.utils.pd_utils import pd_is_not_null

from engine.zvt_bridge.domain.stock_adj_factor import StockAdjFactor

class AKShareStockAdjFactorRecorder(FixedCycleDataRecorder):
    provider = 'akshare'
    data_schema = StockAdjFactor
    entity_provider = 'em' 
    entity_schema = Stock
    
    def __init__(
        self,
        force_update=False,
        sleeping_time=5,
        exchanges=None,
        entity_id=None,
        entity_ids=None,
        code=None,
        codes=None,
        day_data=False,
        entity_filters=None,
        ignore_failed=True,
        real_time=False,
        fix_duplicate_way="add",
        start_timestamp=None,
        end_timestamp=None,
        level=IntervalLevel.LEVEL_1DAY,
        kdata_use_begin_time=False,
        one_shot=False,
        options=None,
    ) -> None:
        super().__init__(
            force_update,
            sleeping_time,
            exchanges,
            entity_id,
            entity_ids,
            code,
            codes,
            day_data,
            entity_filters,
            ignore_failed,
            real_time,
            fix_duplicate_way,
            start_timestamp,
            end_timestamp,
            level,
            kdata_use_begin_time,
            one_shot,
            options,
        )

    def record(self, entity, start, end=None, size=None, timestamps=None):
        try:
            # 1. 抓取 Raw Data
            df_raw = ak.stock_zh_a_hist(
                symbol=entity.code,
                period="daily",
                start_date=to_date_time_str(start, fmt=TIME_FORMAT_DAY).replace("-", ""),
                end_date=to_date_time_str(end, fmt=TIME_FORMAT_DAY).replace("-", "") if end else "20300101",
                adjust=""
            )
            
            # 2. 抓取 HFQ Data
            df_hfq = ak.stock_zh_a_hist(
                symbol=entity.code,
                period="daily",
                start_date=to_date_time_str(start, fmt=TIME_FORMAT_DAY).replace("-", ""),
                end_date=to_date_time_str(end, fmt=TIME_FORMAT_DAY).replace("-", "") if end else "20300101",
                adjust="hfq"
            )

            # 检查是否为空
            if df_raw.empty or df_hfq.empty:
                return None
            
            # 处理 Raw Data
            df_raw = df_raw.rename(columns={"\u65e5\u671f": "timestamp", "\u6536\u76d8": "close_raw"})
            df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])
            df_raw = df_raw.set_index('timestamp')[['close_raw']]
            
            # 处理 HFQ Data
            df_hfq = df_hfq.rename(columns={"\u65e5\u671f": "timestamp", "\u6536\u76d8": "close_hfq"})
            df_hfq['timestamp'] = pd.to_datetime(df_hfq['timestamp'])
            df_hfq = df_hfq.set_index('timestamp')[['close_hfq']]
            
            # 合并
            df_merged = df_raw.join(df_hfq, how='left')
            
            # 计算 HFQ 因子（带除零保护）
            if not df_merged.empty:
                # 使用 np.where 避免除零
                df_merged['hfq_factor'] = pd.to_numeric(
                    df_merged['close_hfq'], errors='coerce'
                ) / pd.to_numeric(df_merged['close_raw'], errors='coerce')
                
                # 处理 inf 和 NaN
                df_merged['hfq_factor'] = df_merged['hfq_factor'].replace([float('inf'), -float('inf')], pd.NA)
                df_merged['hfq_factor'] = df_merged['hfq_factor'].round(6)
            
            # 重置索引
            df_merged = df_merged.reset_index()
            
            # 填充公共字段
            df_merged['entity_id'] = entity.entity_id
            df_merged['provider'] = self.provider
            df_merged['code'] = entity.code
            df_merged['name'] = entity.name
            df_merged['level'] = self.level.value
            
            # 向量化生成 ID
            df_merged['id'] = df_merged['entity_id'] + '_' + df_merged['timestamp'].dt.strftime('%Y-%m-%d')
            
            # 选择相关列（删除 qfq_factor）
            cols = ['id', 'entity_id', 'timestamp', 'provider', 'code', 'name', 'level', 'hfq_factor']
            final_cols = [c for c in cols if c in df_merged.columns]
            df_to_save = df_merged[final_cols]

            # Save
            df_to_db(df=df_to_save, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)
            
            return None
        except Exception as e:
            self.logger.error(f"Error recording factor for {entity.code}: {e}")
            return None

def register_akshare_factor_recorder():
    # Initialize DB engine for StockAdjFactor
    try:
        db_name = _get_db_name(StockAdjFactor)
        engine = get_db_engine(provider='akshare', db_name=db_name, data_schema=StockAdjFactor)
        
        # Create all tables
        StockAdjFactor.metadata.create_all(engine)
        
        session_factory = get_db_session_factory(provider='akshare', db_name=db_name, data_schema=StockAdjFactor)
        session_factory.configure(bind=engine)
        
        StockAdjFactor.register_recorder_cls('akshare', AKShareStockAdjFactorRecorder)
    except Exception as e:
        print(f"Error initializing AKShare Factor DB engine: {e}")

if __name__ == '__main__':
    from zvt.domain import Stock
    # For testing
    recorder = AKShareStockAdjFactorRecorder(codes=['000004'])
    recorder.run()
