"""
Version: 0.1.0
Date: 2026-02-17
修改内容: 初次生成后代码审计

TODO: 1. Entity 依赖问题：entity_provider='em'，必须先运行 Stock.record_data(provider='em') 
         录入股票列表，否则本 Recorder 无法获取股票实体。
      2. 复权逻辑注册问题：同一个类同时注册到 Stock1dKdata 和 Stock1dHfqKdata，
         ZVT 自动调用时可能不会传递 adjust_type='hfq' 参数。考虑拆分为两个子类。
"""


from typing import List
import akshare as ak
import pandas as pd
from zvt.api.kdata import get_kdata_schema
from zvt.contract import IntervalLevel, AdjustType
from zvt.contract.api import df_to_db, get_db_session_factory, get_db_engine, _get_db_name
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import (
    Stock,
    StockKdataCommon,
    Stock1dKdata,
    Stock1dHfqKdata,
)
from zvt.utils.time_utils import to_pd_timestamp, to_date_time_str, TIME_FORMAT_DAY1, TIME_FORMAT_DAY
from zvt.utils.pd_utils import pd_is_not_null

class AKShareStock1dKdataRecorder(FixedCycleDataRecorder):
    entity_provider = "em"  # 股票列表来源：东方财富
    entity_schema = Stock
    provider = "akshare"     # K线数据来源：AKShare

    def __init__(
        self,
        force_update=True,
        sleeping_time=2,
        exchanges=None,
        entity_id=None,
        entity_ids=None,
        code=None,
        codes=None,
        day_data=False,
        entity_filters=None,
        ignore_failed=True,
        real_time=False,
        fix_duplicate_way="ignore",
        start_timestamp=None,
        end_timestamp=None,
        level=IntervalLevel.LEVEL_1DAY,
        kdata_use_begin_time=False,
        one_day_trading_minutes=24 * 60,
        adjust_type=None,  # None=不复权, 'qfq'=前复权, 'hfq'=后复权
        return_unfinished=False,
    ) -> None:
        level = IntervalLevel(level)
        self.adjust_type = AdjustType(adjust_type) if adjust_type else None
        self.entity_type = self.entity_schema.__name__.lower()
        
        # 显式指定 Schema 类型
        if self.adjust_type == AdjustType.hfq:
            self.data_schema = Stock1dHfqKdata
        else:
            self.data_schema = Stock1dKdata

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
            one_day_trading_minutes,
            return_unfinished,
        )

    def record(self, entity, start, end, size, timestamps):
        # 映射 ZVT 复权类型到 AKShare 参数
        ak_adjust = ""
        if self.adjust_type == AdjustType.qfq:
            ak_adjust = "qfq"
        elif self.adjust_type == AdjustType.hfq:
            ak_adjust = "hfq"
        
        start_str = to_date_time_str(start, fmt=TIME_FORMAT_DAY1) if start else "20000101"
        end_str = to_date_time_str(end, fmt=TIME_FORMAT_DAY1) if end \
            else to_date_time_str(pd.Timestamp.now(), fmt=TIME_FORMAT_DAY1)

        try:
            df = ak.stock_zh_a_hist(
                symbol=entity.code,
                period="daily",
                start_date=start_str,
                end_date=end_str,
                adjust=ak_adjust
            )
        except Exception as e:
            self.logger.error(f"Error fetching data for {entity.code}: {e}")
            return None

        if pd_is_not_null(df):
            # 列名映射: AKShare -> ZVT Schema
            df.rename(columns={
                '日期': 'timestamp',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'turnover',
                '涨跌幅': 'change_pct',
                '换手率': 'turnover_rate'
            }, inplace=True)
            
            # 数据类型转换：AKShare 可能返回字符串
            numeric_cols = ['open', 'close', 'high', 'low', 'volume', 'turnover', 'change_pct', 'turnover_rate']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['name'] = entity.name
            df['entity_id'] = entity.id
            df['provider'] = self.provider
            df['code'] = entity.code
            df['level'] = self.level.value
            
            columns = ['timestamp', 'entity_id', 'provider', 'code', 'name', 'level', 'open',
                       'close', 'high', 'low', 'volume', 'turnover', 'change_pct', 'turnover_rate']
            df = df[columns]

            # 生成主键: entity_id_YYYY-MM-DD
            def generate_id(row):
                return "{}_{}".format(row['entity_id'], to_date_time_str(row['timestamp'], fmt=TIME_FORMAT_DAY))

            df['id'] = df.apply(generate_id, axis=1)

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)
            return None
        
        return None

def register_akshare_recorders():
    Stock1dKdata.register_recorder_cls('akshare', AKShareStock1dKdataRecorder)
    Stock1dKdata.register_provider('akshare')
    
    Stock1dHfqKdata.register_recorder_cls('akshare', AKShareStock1dKdataRecorder)
    Stock1dHfqKdata.register_provider('akshare')
    
    try:
        # 初始化 Raw Data 数据库引擎
        db_name = _get_db_name(Stock1dKdata)
        engine = get_db_engine(provider='akshare', db_name=db_name, data_schema=Stock1dKdata)
        Stock1dKdata.metadata.create_all(engine)
        session_factory = get_db_session_factory(provider='akshare', db_name=db_name, data_schema=Stock1dKdata)
        session_factory.configure(bind=engine)
        
        # 初始化 HFQ 数据库引擎
        db_name_hfq = _get_db_name(Stock1dHfqKdata)
        engine_hfq = get_db_engine(provider='akshare', db_name=db_name_hfq, data_schema=Stock1dHfqKdata)
        Stock1dHfqKdata.metadata.create_all(engine_hfq)
        session_factory_hfq = get_db_session_factory(provider='akshare', db_name=db_name_hfq, data_schema=Stock1dHfqKdata)
        session_factory_hfq.configure(bind=engine_hfq)
        
    except Exception as e:
        print(f"Error initializing AKShare DB engine: {e}")
