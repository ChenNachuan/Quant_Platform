'''
ZVT 适配器
将自定义 Factor 包装为 ZVT 兼容的 Factor
'''

from typing import List, Optional
import pandas as pd
import warnings

from zvt.contract.factor import Factor as ZVTFactor, TargetType
from zvt.contract import IntervalLevel
from zvt.domain import Stock1dHfqKdata

from factor_library.base import Factor as CustomFactor
from factor_library.universe import UniverseFilter, UniverseConfig


class FactorAdapter(ZVTFactor):
    """
    因子适配器
    
    将我们的 Factor 转换为 ZVT Factor，用于回测
    
    用法:
        >>> my_factor = MomentumReturn(timeframe='1d', para={'window': 20})
        >>> zvt_factor = FactorAdapter(
        ...     custom_factor=my_factor,
        ...     codes=['000001.SZ', '000002.SZ']
        ... )
        >>> trader = MyTrader(...)
        >>> trader.run()
    """
    
    def __init__(
        self,
        custom_factor: CustomFactor,
        entity_ids: List[str] = None,
        entity_schema = Stock1dHfqKdata,
        exchanges: List[str] = None,
        codes: List[str] = None,
        start_timestamp = None,
        end_timestamp = None,
        adjust_type = None,
        provider: str = 'akshare',
        level: IntervalLevel = IntervalLevel.LEVEL_1DAY,
        keep_all_timestamp: bool = False,
        fill_method: str = 'ffill',
        effective_number: int = None,
        need_persist: bool = False,
        check_lookahead: bool = True,            # 前视偏差检测开关
        universe_filter: Optional[UniverseFilter] = None  # Universe 过滤器
    ):
        """
        初始化适配器
        
        Args:
            custom_factor: 我们自己定义的因子实例
            其他参数同 ZVTFactor
        """
        self.custom_factor = custom_factor
        self.check_lookahead = check_lookahead
        self.universe_filter = universe_filter
        self.universe_mask: Optional[pd.DataFrame] = None   # 计算后缓存，供外部读取
        
        # 使用 DuckDB 而不是 ZVT 默认数据源
        self.use_duckdb = True
        
        super().__init__(
            data_schema=entity_schema,
            entity_ids=entity_ids,
            entity_schema=entity_schema,
            exchanges=exchanges,
            codes=codes,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            columns=['open', 'high', 'low', 'close', 'volume'],
            filters=None,
            provider=provider,
            level=level,
            category_field='entity_id',
            time_field='timestamp',
            keep_all_timestamp=keep_all_timestamp,
            fill_method=fill_method,
            effective_number=effective_number,
            need_persist=need_persist
        )
        
        # 主动触发计算，确保数据就绪
        self.compute_factor()

    def load_data(self):
        """
        覆盖 ZVT 的 load_data。
        我们使用 DuckDB 加载数据，不需要 ZVT 的默认加载逻辑 (SQLAlchemy)
        """
        pass
    
    def compute_factor(self):
        """
        覆写 ZVT 的计算逻辑
        调用我们自己的 Factor
        """
        try:
            # 从 DuckDB 获取数据
            df = self._load_data_from_duckdb()
            
            if df.empty:
                warnings.warn(f"因子 {self.custom_factor.name} 无数据")
                return
            
            # Cache kdata for backtest execution (AccountService can read it)
            self.kdata_df = df
            
            # 按股票分组计算因子
            results = []
            for entity_id, group in df.groupby('entity_id'):
                group = group.sort_values('timestamp')
                
                # 调用我们自己的因子计算逻辑
                try:
                    factor_values = self.custom_factor.compute(group)

                    # 前视偏差（Look-ahead Bias）启发式检测
                    # 原理：截断末尾 1 行重算，若历史部分值发生变化，
                    #      说明因子访问了未来数据（如 .shift(-1)）
                    if self.check_lookahead and len(group) > 2:
                        try:
                            truncated_vals = self.custom_factor.compute(group.iloc[:-1])
                            overlap = factor_values.iloc[:-1]
                            if len(truncated_vals) == len(overlap):
                                if not overlap.reset_index(drop=True).round(8).equals(
                                    truncated_vals.reset_index(drop=True).round(8)
                                ):
                                    warnings.warn(
                                        f"⚠️  因子 [{self.custom_factor.name}] 疑似存在前视偏差 "
                                        f"(Look-ahead Bias)：截断末行后历史值发生变化，"
                                        f"请检查是否使用了 .shift(-N) 或未对齐的 rolling。"
                                    )
                        except Exception:
                            pass  # 检测本身不影响主流程

                    # 转换为 DataFrame
                    factor_df = pd.DataFrame({
                        'timestamp': group['timestamp'],
                        'entity_id': entity_id,
                        'value': factor_values
                    })
                    results.append(factor_df)
                    
                except Exception as e:
                    warnings.warn(f"计算因子失败 {entity_id}: {e}")
                    continue
            
            # 合并所有结果
            if results:
                all_results = pd.concat(results, ignore_index=True)
                
                # ZVT Drawer expects OHLCV in data_df
                # df contains the original OHLCV data from DuckDB
                # Filter df to keep only what we have factor values for? 
                # Or just keep the full df used for computation.
                # df has columns: timestamp, entity_id, open, high, low, close, volume...
                self.data_df = df.set_index(['entity_id', 'timestamp']).sort_index()

                self.factor_df = all_results.pivot(
                    index='timestamp',
                    columns='entity_id',
                    values='value'
                )

                # Universe 过滤：将不在有效股票池内的因子值置为 NaN
                # 防止停牌/ST/退市/次新股的因子值参与截面排名
                if self.universe_filter is not None:
                    try:
                        self.factor_df = self.universe_filter.apply_to_factor(
                            factor_wide=self.factor_df,
                            kdata=df,
                        )
                        self.universe_mask = self.universe_filter.filter(df)
                        n_filtered = (~self.universe_mask).sum().sum()
                        if n_filtered > 0:
                            import logging
                            logging.getLogger(__name__).info(
                                f"Universe Filter: 共过滤 {n_filtered:.0f} 个因子值（停牌/ST/新股/退市）"
                            )
                    except Exception as ue:
                        warnings.warn(f"Universe 过滤失败（跳过）: {ue}")

        except Exception as e:
            warnings.warn(f"计算因子失败: {e}")
            import traceback
            traceback.print_exc()
    
    def _load_data_from_duckdb(self) -> pd.DataFrame:
        """
        从 DuckDB 加载数据
        
        Returns:
            包含 OHLCV 数据的 DataFrame
        """
        from infra.storage import StorageManager
        
        storage = StorageManager()
        
        # 优先使用 entity_ids，因为它是标准化的 (stock_sz_000001)
        if not self.entity_ids:
             if self.codes:
                 # 手动转换: 000001.SZ -> stock_sz_000001
                 self.entity_ids = []
                 for code_str in self.codes:
                     if '.' in code_str:
                         code, exchange = code_str.split('.')
                         self.entity_ids.append(f'stock_{exchange.lower()}_{code}')
                     else:
                         # 默认 sz? 或者报错
                         self.entity_ids.append(f'stock_sz_{code_str}')
             else:
                 raise ValueError("必须提供 codes 或 entity_ids")
        
        target_ids = self.entity_ids
        # Ensure it is a list
        if target_ids is None:
             target_ids = []

        # 根据时间框架选择表
        if self.level == IntervalLevel.LEVEL_1DAY:
            # 使用后复权数据作为默认源，避免因子计算受除权除息影响
            table_name = 'cn_stock_1d_hfq'
        elif self.level == IntervalLevel.LEVEL_1HOUR:
            table_name = 'cn_stock_1h'
        else:
            table_name = 'cn_stock_1d_hfq'  # 默认日线后复权

        placeholders = ", ".join(f"${i+1}" for i in range(len(target_ids)))
        params = list(target_ids)
        query = f"""
            SELECT
                timestamp,
                entity_id,
                open,
                high,
                low,
                close,
                volume
            FROM {table_name}
            WHERE entity_id IN ({placeholders})
        """

        if self.start_timestamp:
            params.append(self.start_timestamp)
            query += f" AND timestamp >= ${len(params)}"

        if self.end_timestamp:
            params.append(self.end_timestamp)
            query += f" AND timestamp <= ${len(params)}"

        query += " ORDER BY symbol, timestamp"

        try:
            df = storage.conn.execute(query, params).df()
            return df
        except Exception as e:
            import traceback
            traceback.print_exc()
            warnings.warn(f"从 DuckDB 加载数据失败: {e}")
            return pd.DataFrame()
    
    def compute_result(self):
        """
        生成买卖信号
        
        策略：因子值最高的前N只股票买入
        
        可以被子类覆写以实现自定义策略
        """
        if self.factor_df is None or self.factor_df.empty:
            return
        
        # 简单策略：选因子值最高的前10只
        # 1 表示买入，0 表示不持有
        top_n = min(10, len(self.factor_df.columns))
        self.result_df = self.factor_df.rank(axis=1, ascending=False) <= top_n
        
        # 将 True/False 转换为 1/0
        self.result_df = self.result_df.astype(int)


class CustomStrategyAdapter(FactorAdapter):
    """
    自定义策略适配器
    
    允许用户自定义选股逻辑
    
    用法:
        >>> def my_strategy(factor_df):
        ...     # 自定义选股逻辑
        ...     return factor_df > factor_df.quantile(0.8, axis=1)
        >>> 
        >>> adapter = CustomStrategyAdapter(
        ...     custom_factor=my_factor,
        ...     codes=[...],
        ...     strategy_func=my_strategy
        ... )
    """
    
    def __init__(self, *args, strategy_func=None, **kwargs):
        """
        Args:
            strategy_func: 自定义策略函数
                输入: factor_df (DataFrame, index=timestamp, columns=entity_id)
                输出: result_df (DataFrame, 同shape, 1=买入, 0=不持有)
        """
        self.strategy_func = strategy_func
        super().__init__(*args, **kwargs)
    
    def compute_result(self):
        """使用自定义策略生成信号"""
        if self.factor_df is None or self.factor_df.empty:
            return
        
        if self.strategy_func is None:
            # 回退到默认策略
            super().compute_result()
        else:
            # 使用自定义策略
            self.result_df = self.strategy_func(self.factor_df)
            
            # 确保结果为 0/1
            if not (self.result_df.dtypes == int).all():
                self.result_df = self.result_df.astype(int)


__all__ = ['FactorAdapter', 'CustomStrategyAdapter']
