# -*- coding: utf-8 -*-
"""
批量回测完整示例
演示如何使用 BatchBacktestRunner 对比多个因子
"""
import sys
sys.path.insert(0, '/Users/nachuanchen/Documents/Quant')

from engine.factor.backtest_runner import BatchBacktestRunner
from zvt.contract import IntervalLevel


def run_batch_backtest():
    """运行批量回测"""
    
    print("\n" + "="*60)
    print("批量因子回测系统")
    print("="*60)
    
    # 配置参数
    codes = [
        '000001.SZ', '000002.SZ', '000004.SZ', '000005.SZ', '000006.SZ',
        '000007.SZ', '000008.SZ', '000009.SZ', '000010.SZ', '000011.SZ'
    ]
    start_date = '2023-01-01'
    end_date = '2024-01-01'
    
    print(f"\n回测配置:")
    print(f"  股票池: {len(codes)} 只股票")
    print(f"  时间范围: {start_date} 至 {end_date}")
    print(f"  频率: 日线 (1d)")
    
    # 创建批量回测器
    runner = BatchBacktestRunner(
        codes=codes,
        start_timestamp=start_date,
        end_timestamp=end_date,
        level=IntervalLevel.LEVEL_1DAY
    )
    
    # 方式1: 运行所有已注册的因子
    print("\n" + "="*60)
    print("方式1: 自动运行所有注册因子")
    print("="*60)
    runner.run_all_factors()
    
    # 方式2: 手动指定因子和参数
    print("\n" + "="*60)
    print("方式2: 手动指定因子参数")
    print("="*60)
    
    # 测试动量因子的不同参数
    for window in [10, 20, 60]:
        runner.run_single_factor(
            factor_name='momentum_return',
            timeframe='1d',
            para={'window': window},
            trader_name=f'momentum_win{window}'
        )
    
    # 导出汇总表
    print("\n" + "="*60)
    print("导出结果")
    print("="*60)
    
    summary_df = runner.export_summary_table()
    
    # 绘制对比图表
    runner.draw_comparison_dashboard(show=False)
    
    print("\n" + "="*60)
    print("批量回测完成！")
    print("="*60)
    print("\n结果文件:")
    print("  - CSV汇总表: /Users/nachuanchen/.zvt/ui/factor_summary_*.csv")
    print("  - HTML可视化: /Users/nachuanchen/.zvt/ui/factor_comparison_*.html")
    print("\n打开 HTML 文件即可查看交互式图表")
    print("="*60 + "\n")


def run_custom_strategy_example():
    """
    自定义策略示例
    
    演示如何使用自定义选股逻辑
    """
    from engine.zvt_bridge.backtest import CustomStrategyAdapter
    from factor_library.technical.momentum import MomentumReturn
    from zvt.trader.trader import StockTrader
    from zvt.contract import IntervalLevel
    import pandas as pd
    
    print("\n" + "="*60)
    print("自定义策略示例")
    print("="*60)
    
    # 自定义策略函数
    def top_bottom_strategy(factor_df: pd.DataFrame) -> pd.DataFrame:
        """
        多空策略：做多因子值最高的3只，做空最低的3只
        
        Args:
            factor_df: 因子值矩阵
        
        Returns:
            信号矩阵: 1=做多, -1=做空, 0=不持有
        """
        result = pd.DataFrame(0, index=factor_df.index, columns=factor_df.columns)
        
        for idx, row in factor_df.iterrows():
            # 排序
            sorted_stocks = row.sort_values(ascending=False)
            
            # 做多前3
            top_3 = sorted_stocks.head(3).index
            result.loc[idx, top_3] = 1
            
            # 做空后3
            bottom_3 = sorted_stocks.tail(3).index
            result.loc[idx, bottom_3] = -1
        
        return result
    
    # 创建 Trader
    class CustomTrader(StockTrader):
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
            factor = MomentumReturn(timeframe='1d', para={'window': 20})
            
            return [
                CustomStrategyAdapter(
                    custom_factor=factor,
                    codes=codes,
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    strategy_func=top_bottom_strategy,
                    need_persist=False
                )
            ]
    
    print(f"\n策略说明:")
    print(f"  - 做多因子值最高的3只股票")
    print(f"  - 做空因子值最低的3只股票")
    print(f"  - 实现多空对冲")
    
    try:
        trader = CustomTrader(
            codes=['000001.SZ', '000002.SZ', '000004.SZ', '000005.SZ', '000006.SZ'],
            level=IntervalLevel.LEVEL_1DAY,
            start_timestamp='2023-01-01',
            end_timestamp='2024-01-01',
            trader_name='custom_long_short_strategy'
        )
        
        print(f"\n开始回测...")
        trader.run()
        print(f"✅ 自定义策略回测完成")
        
    except Exception as e:
        print(f"❌ 回测失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='批量因子回测')
    parser.add_argument(
        '--mode',
        choices=['batch', 'custom', 'both'],
        default='batch',
        help='运行模式: batch=批量回测, custom=自定义策略, both=两者都运行'
    )
    
    args = parser.parse_args()
    
    if args.mode in ['batch', 'both']:
        run_batch_backtest()
    
    if args.mode in ['custom', 'both']:
        run_custom_strategy_example()
