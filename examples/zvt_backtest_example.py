# -*- coding: utf-8 -*-
"""
ZVT 回测示例
演示如何使用 FactorAdapter 进行因子回测
"""
import sys
sys.path.insert(0, '/Users/nachuanchen/Documents/Quant')

from zvt.trader.trader import StockTrader
from zvt.contract import IntervalLevel
from factor_library.technical.momentum import MomentumReturn
from engine.zvt_bridge.backtest import FactorAdapter
from zvt.contract.api import df_to_db
from zvt.domain import Stock1dHfqKdata
from zvt.trader.sim_account import SimAccountService
from zvt.trader import TradingSignal, OrderType, trading_signal_type_to_order_type
import pandas as pd

class FactorDataAccountService(SimAccountService):
    """
    使用 FactorAdapter 缓存的 kdata_df 进行回测的账户服务
    避免了 get_kdata 的 SQL 兼容性问题
    """
    def __init__(self, factor_adapter, **kwargs):
        self.factor_adapter = factor_adapter
        # 预处理 kdata_df 为 MultiIndex 以便快速查找
        if hasattr(self.factor_adapter, 'kdata_df') and self.factor_adapter.kdata_df is not None:
             # Ensure index is set
             if 'entity_id' in self.factor_adapter.kdata_df.columns:
                 self.kdata_cache = self.factor_adapter.kdata_df.set_index(['entity_id', 'timestamp']).sort_index()
             else:
                 self.kdata_cache = self.factor_adapter.kdata_df
        else:
             self.kdata_cache = None
             
        super().__init__(**kwargs)

    def handle_trading_signal(self, trading_signal: TradingSignal):
        entity_id = trading_signal.entity_id
        happen_timestamp = trading_signal.happen_timestamp
        order_type = trading_signal_type_to_order_type(trading_signal.trading_signal_type)
        
        if order_type:
            the_price = None
            if self.kdata_cache is not None:
                try:
                    # 从缓存获取价格
                    if (entity_id, happen_timestamp) in self.kdata_cache.index:
                        series = self.kdata_cache.loc[(entity_id, happen_timestamp)]
                        if isinstance(series, pd.DataFrame):
                            series = series.iloc[0]
                        the_price = series['close']  # Assuming 'close' column exists
                except Exception as e:
                    print(f"  [FactorAccount] Error looking up price: {e}")

            if the_price is not None:
                print(f"  [FactorAccount] Executing {order_type} for {entity_id} at {the_price}")
                if trading_signal.position_pct:
                    self.order_by_position_pct(
                        entity_id=entity_id,
                        order_price=the_price,
                        order_timestamp=happen_timestamp,
                        order_position_pct=trading_signal.position_pct,
                        order_type=order_type,
                    )
                elif trading_signal.order_money:
                    self.order_by_money(
                        entity_id=entity_id,
                        order_price=the_price,
                        order_timestamp=happen_timestamp,
                        order_money=trading_signal.order_money,
                        order_type=order_type,
                    )
                else:
                    # order_amount logic if needed
                    pass
            else:
                 print(f"  [FactorAccount] Ignored signal: No price for {entity_id} at {happen_timestamp}")

    def on_trading_close(self, timestamp):
        # self.logger.info("on_trading_close:{}".format(timestamp))
        # remove the empty position
        self.account.positions = [
            position for position in self.account.positions if position.long_amount > 0 or position.short_amount > 0
        ]

        # clear the data which need recomputing
        # Use simple string format for ID
        from zvt.utils.time_utils import to_pd_timestamp, to_date_time_str, TIME_FORMAT_ISO8601
        
        the_id = "{}_{}".format(self.trader_name, to_date_time_str(timestamp, TIME_FORMAT_ISO8601))

        self.account.value = 0
        self.account.all_value = 0
        for position in self.account.positions:
            # Get closing price from cache
            closing_price = None
            if self.kdata_cache is not None:
                try:
                    if (position.entity_id, timestamp) in self.kdata_cache.index:
                        series = self.kdata_cache.loc[(position.entity_id, timestamp)]
                        if isinstance(series, pd.DataFrame):
                            series = series.iloc[0]
                        closing_price = series['close']
                except:
                    pass
            
            position.available_long = position.long_amount
            position.available_short = position.short_amount

            if closing_price:
                if (position.long_amount is not None) and position.long_amount > 0:
                    position.value = position.long_amount * closing_price
                elif (position.short_amount is not None) and position.short_amount > 0:
                    position.value = 2 * (position.short_amount * position.average_short_price)
                    position.value -= position.short_amount * closing_price

                # refresh profit
                position.profit = (closing_price - position.average_long_price) * position.long_amount
                # Avoid division by zero
                if position.average_long_price * position.long_amount != 0:
                    position.profit_rate = position.profit / (position.average_long_price * position.long_amount)
                else:
                    position.profit_rate = 0
            else:
                # Keep previous value if no price found (e.g. suspension or missing data)
                pass

            # Always accumulate value to account
            if position.value is not None:
                self.account.value += position.value

            position.id = "{}_{}_{}".format(
                self.trader_name, position.entity_id, to_date_time_str(timestamp, TIME_FORMAT_ISO8601)
            )
            position.timestamp = to_pd_timestamp(timestamp)
            position.account_stats_id = the_id

        self.account.id = the_id
        self.account.all_value = self.account.value + self.account.cash
        self.account.closing = True
        self.account.timestamp = to_pd_timestamp(timestamp)
        self.account.profit = self.account.all_value - self.account.input_money
        self.account.profit_rate = self.account.profit / self.account.input_money

        self.session.add(self.account)
        self.session.commit()
        # account_info = (
        #    f"on_trading_close,holding size:{len(self.account.positions)} profit:{self.account.profit} input_money:{self.account.input_money} "
        #    f"cash:{self.account.cash} value:{self.account.value} all_value:{self.account.all_value}"
        # )
        # self.logger.info(account_info)


class MomentumTrader(StockTrader):
    """基于动量因子的交易策略"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # The default SimAccountService will be used, which fetches data from ZVT DB.
        # We will inject data into ZVT DB before running the backtest.
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Use FactorDataAccountService
        if self.factors:
             self.account_service = FactorDataAccountService(
                factor_adapter=self.factors[0],
                entity_schema=self.entity_schema,
                trader_name=self.trader_name,
                timestamp=self.start_timestamp,
                provider=self.provider,
                level=self.level,
                rich_mode=self.rich_mode,
                adjust_type=self.adjust_type,
                keep_history=self.keep_history
             )
             # Re-register listener to replace the default one created in super().__init__
             # super().__init__ creates an account_service and registers it.
             # We should overwrite it.
             self.trading_signal_listeners = [self.account_service]
    
    def init_factors(
        self,
        entity_ids,
        entity_schema,
        exchanges,
        codes,
        start_timestamp,
        end_timestamp,
        adjust_type=None
    ):
        """
        初始化因子
        
        ZVT 会自动调用此方法来获取因子列表
        """
        # 创建我们的动量因子
        momentum_factor = MomentumReturn(
            timeframe='1d',
            para={'window': 20}
        )
        
        # 包装为 ZVT Factor
        return [
            FactorAdapter(
                custom_factor=momentum_factor,
                entity_ids=entity_ids,
                entity_schema=entity_schema,
                exchanges=exchanges,
                codes=codes,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                level=IntervalLevel.LEVEL_1DAY,
                need_persist=False  # 不持久化到ZVT数据库
            )
        ]

    def on_time(self, timestamp):
        """
        每日交易逻辑
        买入动量最高的 Top N 股票
        """
        # 获取当天所有标的的因子值
        # 注意: ZVT 的数据结构比较复杂，这里使用简化的方式
        # self.readers 是一个列表，对应 init_factors 返回的 adapter
        # ERROR: Trader no longer has self.readers. It has self.factors (list of Factor objects/adapters)
        
        factor_reader = self.factors[0]
        
        # 获取当前时间切片的数据 (Standard DataFrame with index: entity_id)
        # column should be 'value' because FactorAdapter maps factor result to 'value' column?
        # Let's check FactorAdapter. It returns 'score' or similar?
        # In FactorAdapter.code: `col='score'`, `value` is named `score` in ZVT standard?
        # Actually FactorAdapter.compute_factor returns DataFrame. 
        # ZVT DataReader (adapter) should provide data.
        
        # 使用 reader.get_value(timestamp) ?
        # ZVT design: DataReader provides data. 
        # Simpler approach: query the dataframe directly from reader
        
        try:
            # 1. 获取当天的因子数据
            df = factor_reader.data_df
            if df is None or df.empty:
                print(f"[{timestamp}] Factor DF is empty")
                return
            
            # 尝试获取当天数据
            try:
                # Based on previous successful run, timestamp filtering works
                if 'timestamp' in df.index.names:
                     current_df = df.query(f"timestamp == '{timestamp}'")
                else:
                     current_df = df[df['timestamp'] == timestamp]         
            except Exception as e:
                print(f"[{timestamp}] Filtering error: {e}")
                return

            if current_df.empty:
                print(f"[{timestamp}] No factor data found. (Df size: {len(df)})")
                return
            
            print(f"[{timestamp}] Found {len(current_df)} factor records.")

            # 2. 排序选股 (买 Top 1)
            # FactorAdapter output columns may vary. Usually 'value' or 'score'
            # Let's assume 'value' based on our factor definition.
            # Wait, our `MomentumReturn` computes a Series. `FactorAdapter` puts it into ZVT structure.
            # ZVT usually uses 'score' or 'value'. Let's debug print if needed or assume 'value'.
            # Looking at previous logs, `FactorAdapter` seems to just pass DF. 
            # In `FactorAdapter.compute_factor`: returns DF with `entity_id`, `timestamp`, `value`?
            # It returns whatever `compute` returns. `ts_mean` returns Series.
            
            # Let's assume the column name is the factor name or 'value'.
            target_col = 'momentum_return' # or 'value'? 
            # The Series name from `ts_mean` is usually None or preserved from input.
            # In ZVT Adapter, we should probably rename it to 'score' for consistency.
            # For now, let's select the first numeric column.
            
            numeric_cols = current_df.select_dtypes(include=['float']).columns
            if len(numeric_cols) == 0:
                return
            target_col = numeric_cols[0]
            
            # 选出 Top 1
            current_df = current_df.sort_values(target_col, ascending=False)
            # data_df has MultiIndex (entity_id, timestamp), need to extract entity_id level
            target_codes = current_df.head(1).index.get_level_values('entity_id').tolist()
            
            # print(f"[{timestamp}] Target Codes: {target_codes}")

            # 3. 执行交易
            current_account = self.get_current_account()
            current_holdings = [pos.entity_id for pos in current_account.positions]
            
            # 卖出不在目标列表中的
            for entity_id in current_holdings:
                if entity_id not in target_codes:
                    print(f"[{timestamp}] Selling {entity_id}")
                    self.sell(entity_ids=[entity_id], timestamp=timestamp)
            
            # 买入目标股票
            for entity_id in target_codes:
                if entity_id not in current_holdings:
                    # 全仓买入 (简化资金管理)
                    # ZVT 的 buy 不需要 cash 参数，仓位由 long_position_control 控制
                    print(f"[{timestamp}] Buying {entity_id}")
                    self.buy(entity_ids=[entity_id], timestamp=timestamp)
                    
        except Exception as e:
            self.logger.error(f"Trading Logic Error: {e}")

    def long_position_control(self):
        """
        买入仓位控制
        因为我们只买 1 只，所以返回 1.0 (全仓)
        """
        return 1.0


if __name__ == "__main__":
    """
    运行回测
    """
    # 股票池
    codes = ['000001.SZ', '000002.SZ', '000004.SZ', '000005.SZ', '000006.SZ']
    
    start_date = '2024-01-01'
    end_date = '2026-01-01'
    
    print("="*60)
    print("ZVT 回测示例 - 动量因子")
    print("="*60)
    
    print(f"\n回测参数:")
    print(f"  股票池: {codes}")
    print(f"  开始日期: {start_date}")
    print(f"  结束日期: {end_date}")
    print(f"  因子: MomentumReturn(window=20)")
    
    # 临时 Provider 名称，用于注入数据到 ZVT DB
    TEMP_PROVIDER = 'duck_temp'
    
    trader = MomentumTrader(
        codes=codes,
        level=IntervalLevel.LEVEL_1DAY,
        start_timestamp=start_date,
        end_timestamp=end_date,
        trader_name='momentum_20d_backtest',
        provider=TEMP_PROVIDER # 让 AccountService 使用我们的临时 provider 读取数据
    )
    
    # 既然使用了 FactorDataAccountService，我们不需要注入数据到 ZVT DB 了
    # 但为了确保 FactorAdapter 加载数据，我们在 init_factors 中已经触发了 loading/computing
    # 所以直接运行即可
    
    print(f"\n开始回测...")
    
    try:
        # 运行回测
        trader.run()
        
        print(f"\n✅ 回测完成！")
        
        # 读取结果并生成报表
        from zvt.trader.trader_info_api import AccountStatsReader
        reader = AccountStatsReader(trader_names=['momentum_20d_backtest'])
        
        if not reader.data_df.empty:
            final_value = reader.data_df['all_value'].iloc[-1]
            print(f"  - 最终净值: {final_value:.2f}")
            
            # 使用 BatchBacktestRunner 的绘图功能 (如果有)
            # 或者直接在这里绘图
            try:
                import plotly.graph_objs as go
                from plotly.subplots import make_subplots
                from datetime import datetime
                
                df = reader.data_df
                
                fig = make_subplots(rows=1, cols=1)
                fig.add_trace(go.Scatter(x=df.index, y=df['all_value'], mode='lines', name='Net Value'))
                
                fig.update_layout(title="Momentum Factor Backtest Result", height=600)
                
                output_path = f'/Users/nachuanchen/.zvt/ui/momentum_backtest_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
                import os
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                fig.write_html(output_path)
                print(f"✅ 报表已生成: {output_path}")
                
            except ImportError:
                print("❌ Plotly 未安装，无法生成报表")
        
    except Exception as e:
        print(f"\n❌ 回测失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
