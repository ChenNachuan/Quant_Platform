"""
批量回测运行器
用于批量测试多个因子并生成对比报告

修改时间 2026 年 3 月 2 日

TODO：

1. 在汇总阶段增加指标：IC、RollingIC、MeanIC、回撤时长、不同时间的回撤周期占比、周胜率、月胜率等等

2. 对于每一个因子，分别绘制出一幅 5/10 分位分组的净值曲线图

3. 对于每一个因子，分别绘制出一幅 rollingIC、累计 IC 的曲线图

"""

from typing import List, Dict, Optional, Union
import pandas as pd
import warnings
from pathlib import Path
from datetime import datetime

from zvt.trader.trader import StockTrader
from zvt.contract import IntervalLevel
from zvt.trader.trader_info_api import AccountStatsReader
from zvt.domain import Stock
from zvt.utils.utils import zvt_env

from factor_library.registry import FactorRegistry
from engine.zvt_bridge.backtest import FactorAdapter


class BatchBacktestRunner:
    """
    批量回测运行器
    
    功能:
    1. 批量运行多个因子的回测
    2. 生成性能对比报告
    3. 绘制可视化图表
    
    用法:
        >>> runner = BatchBacktestRunner(
        ...     start_timestamp = '2023-01-01',
        ...     end_timestamp = '2024-01-01',
        ...     codes = None,
        ...     level = IntervalLevel.LEVEL_1DAY
        ... )
        >>> runner.run_all_factors()
        >>> runner.export_summary_table()
    """
    
    def __init__(
        self,
        start_timestamp: str,
        end_timestamp: str,
        codes: Optional[List[str]] = None,
        level: IntervalLevel = IntervalLevel.LEVEL_1DAY
    ):
        """
        初始化批量回测器
        
        Args:
            start_timestamp: 开始时间
            end_timestamp: 结束时间
            codes: 股票代码列表，如果为 None 则使用全市场股票
            level: 时间级别
        """
        if codes is None:
            print(" 未指定股票代码，加载全市场股票列表")
            stock_df = Stock.query_data(return_type='df')
            if stock_df is not None and not stock_df.empty:
                self.codes = stock_df['entity_id'].tolist()
                print(f" 已加载 {len(self.codes)} 只股票")
            else:
                raise ValueError("无法获取全市场股票列表，请检查数据源")
        else:
            self.codes = codes

        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.level = level
        self.results: Dict[str, pd.DataFrame] = {}
    
    def run_single_factor(
        self,
        factor_name: str,
        timeframe: str,
        para: dict,
        trader_name: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        运行单个因子回测
        
        Args:
            factor_name: 因子名称
            timeframe: 时间框架
            para: 因子参数
            trader_name: 交易策略名称（可选）
        
        Returns:
            回测结果 DataFrame
        """
        if trader_name is None:
            # 提取参数值生成简洁的标识符
            para_str = "_".join([f"{k}{v}" for k, v in para.items()])
            trader_name = f"{factor_name}_{timeframe}_{para_str}"
        
        print(f"\n{'='*60}")
        print(f"回测: {trader_name}")
        print(f"{'='*60}")
        
        try:
            # 从注册表获取因子
            factor = FactorRegistry.create_instance(factor_name, timeframe, para)
            
            # 创建动态 Trader 类
            class SingleFactorTrader(StockTrader):
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
                    return [
                        FactorAdapter(
                            custom_factor=factor,
                            entity_ids=entity_ids,
                            entity_schema=entity_schema,
                            codes=codes,
                            start_timestamp=start_timestamp,
                            end_timestamp=end_timestamp,
                            level=self.level,
                            need_persist=False
                        )
                    ]
            
            # 运行回测
            trader = SingleFactorTrader(
                codes=self.codes,
                level=self.level,
                start_timestamp=self.start_timestamp,
                end_timestamp=self.end_timestamp,
                trader_name=trader_name,
                draw_result=False  # 批量测试时不绘图
            )
            
            trader.run()
            
            # 读取结果
            reader = AccountStatsReader(trader_names=[trader_name])
            if not reader.data_df.empty:
                self.results[trader_name] = reader.data_df
                return reader.data_df
            else:
                print(f" 无回测结果")
                return None
                
        except Exception as e:
            print(f" 回测失败: {e}")
            warnings.warn(f"因子 {trader_name} 回测失败: {e}")
            return None

    def run_all_factors(self, max_paras_per_factor: Optional[int] = 3):
        """运行所有注册因子的回测"""
        print("\n" + "="*60)
        print("批量回测所有因子")
        print("="*60)
        
        all_factors = FactorRegistry.get_all()
        print(f"\n发现 {len(all_factors)} 个已注册因子")
        
        for factor_name, factor_cls in all_factors.items():
            print(f"\n处理因子: {factor_name}")
            
            # 获取参数空间
            # 注意：需要临时实例化来调用 generate_para_space()
            timeframe = self.level.value
            temp_instance = factor_cls(timeframe=timeframe, para={})
            para_space = temp_instance.generate_para_space()
            
            print(f"  参数空间: {len(para_space)} 个组合")
            
            # 遍历参数
            if max_paras_per_factor:
                para_space_to_run = para_space[:max_paras_per_factor]
            else:
                para_space_to_run = para_space
            for para in para_space_to_run:
                self.run_single_factor(
                    factor_name=factor_name,
                    timeframe=timeframe,
                    para=para
                )
    
    def export_summary_table(self, output_path: Optional[str] = None) -> pd.DataFrame:
        """
        导出因子表现汇总表
        
        Args:
            output_path: 输出路径（可选）
        
        Returns:
            汇总 DataFrame
        """
        if not self.results:
            print("  无回测结果可导出")
            return pd.DataFrame()
        
        print("\n" + "="*60)
        print("因子表现汇总表")
        print("="*60)
        
        summary = []
        for trader_name, df in self.results.items():
            if df.empty:
                continue

            equity = df['all_value']
            returns = equity.pct_change().dropna()
            
            # 计算性能指标
            final_value = equity.iloc[-1]
            total_return = (final_value / equity.iloc[0] - 1) * 100
            
            # 年化收益率 (Annualized Return)
            days = (df.index[-1] - df.index[0]).days
            annual_return = (final_value / equity.iloc[0]) ** (365.25 / days) - 1
            annual_return = annual_return * 100
            
            # Sharpe Ratio
            sharpe = returns.mean() / returns.std() * (252 ** 0.5) if returns.std() > 0 else 0
            
            # 最大回撤 (Max Drawdown)
            cummax = equity.cummax()
            drawdown = (equity / cummax - 1)
            max_drawdown = drawdown.min() * 100
            
            # 胜率 (Win Rate)
            win_rate = (returns > 0).sum() / len(returns) * 100 if len(returns) > 0 else 0
            
            summary.append({
                '因子': trader_name,
                '最终净值': final_value,
                '总收益率(%)': total_return,
                '年化收益率(%)': annual_return,
                'Sharpe Ratio': sharpe,
                '最大回撤(%)': max_drawdown,
                '胜率(%)': win_rate
            })
        
        summary_df = pd.DataFrame(summary)
        if not summary_df.empty:
            # 按 Sharpe Ratio 排序
            summary_df = summary_df.sort_values('Sharpe Ratio', ascending=False).reset_index(drop=True)

        # 确定输出路径
        if output_path is None:
            # 使用 zvt 环境目录，确保可移植性
            output_dir = Path(zvt_env["zvt_home"]) / "ui"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f'factor_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

        summary_df.to_csv(str(output_path), index=False, encoding='utf-8-sig')
        print(f"\n✅ 汇总表已保存: {str(output_path)}")
        print(f"\n{summary_df.to_string(index=False)}")
        
        return summary_df
    
    def draw_comparison_dashboard(self, show: bool = True):
        """
        绘制多因子对比面板
        
        Args:
            show: 是否显示图表
        """
        # 先生成汇总表，以复用计算结果
        summary_df = self.export_summary_table()
        
        if summary_df.empty:
            print("  无回测结果可绘制")
            return None
        
        try:
            import plotly.graph_objs as go
            from plotly.subplots import make_subplots
            
            print("\n" + "="*60)
            print("生成对比面板")
            print("="*60)
            
            # 创建子图
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('因子净值曲线对比', 'Sharpe Ratio 排名'),
                row_heights=[0.7, 0.3],
                vertical_spacing=0.1
            )
            
            # 1. 绘制净值曲线
            for trader_name in summary_df['因子']:
                df = self.results.get(trader_name)
                if df.empty:
                    continue
                
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['all_value'],
                        mode='lines',
                        name=trader_name
                    ),
                    row=1, col=1
                )
            
            # 2. 绘制 Sharpe Ratio 排名 (复用 summary_df)
            # 绘图时，通常将表现最好的放在最上面，所以按升序排列
            sharpe_df_sorted = summary_df.sort_values('Sharpe Ratio', ascending=True)
            
            fig.add_trace(
                go.Bar(
                    x=sharpe_df_sorted['Sharpe Ratio'],
                    y=sharpe_df_sorted['因子'],
                    orientation='h',
                    marker=dict(color='steelblue')
                ),
                row=2, col=1
            )
            
            # 更新布局
            fig.update_layout(
                height=800,
                showlegend=True,
                title_text="因子回测对比面板"
            )
            
            fig.update_xaxes(title_text="日期", row=1, col=1)
            fig.update_yaxes(title_text="净值", row=1, col=1)
            fig.update_xaxes(title_text="Sharpe Ratio", row=2, col=1)
            fig.update_yaxes(title_text="因子", row=2, col=1)
            
            # 保存HTML
            output_dir = Path(zvt_env["zvt_home"]) / "ui"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f'factor_comparison_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
            fig.write_html(str(output_path))
            
            print(f"\n 对比面板已保存: {str(output_path)}")
            
            if show:
                fig.show()
            
            return fig
            
        except ImportError:
            print(" Plotly 未安装，无法绘制图表")
            return None
        except Exception as e:
            print(f" 绘制失败: {e}")
            import traceback
            traceback.print_exc()
            return None


__all__ = ['BatchBacktestRunner']
