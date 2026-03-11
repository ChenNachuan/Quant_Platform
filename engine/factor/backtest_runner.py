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
from engine.simulation.models import (
    BaseFeeModel, AShareFeeModel,
    BaseSlippageModel, ConstantSlippageModel,
)


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
        level: IntervalLevel = IntervalLevel.LEVEL_1DAY,
        apply_cost: bool = False,
        fee_model: Optional[BaseFeeModel] = None,
        slippage_model: Optional[BaseSlippageModel] = None,
    ):
        """
        Args:
            start_timestamp: 开始时间
            end_timestamp: 结束时间
            codes: 股票代码列表，None 则全市场
            level: 时间级别
            apply_cost: 是否开启交易成本扣除
            fee_model: 费率模型，默认 AShareFeeModel（从 settings.toml 初始化）
            slippage_model: 滑点模型，默认 ConstantSlippageModel
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
        self.apply_cost = apply_cost
        self.results: Dict[str, pd.DataFrame] = {}
        self._adapters: Dict[str, object] = {}

        # 初始化费率模型：优先用传入的实例，否则从 settings.toml 自动构建 AShareFeeModel
        if fee_model is not None:
            self._fee_model: Optional[BaseFeeModel] = fee_model
        elif apply_cost:
            import sys
            sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
            from infra.storage import ConfigLoader
            cfg = ConfigLoader.load()
            cost_cfg = cfg.get('backtest', {}).get('cost', {})
            self._fee_model = AShareFeeModel.from_config(cost_cfg)
            print(f"交易成本已开启: {self._fee_model.__class__.__name__} | "
                  f"佣金 {self._fee_model.commission_rate:.4%} | "
                  f"印花税 {self._fee_model.stamp_duty:.4%} | "
                  f"最低 {self._fee_model.min_commission:.0f} 元")
        else:
            self._fee_model = None

        self._slippage_model: Optional[BaseSlippageModel] = slippage_model

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

            # 存储 adapter 供 analyze_factor_ic 复用（避免重复计算）
            if hasattr(trader, 'factors') and trader.factors:
                self._adapters[trader_name] = trader.factors[0]

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

            # ── 交易成本估算（调用可插拔 fee_model）──
            # 换手次数粗估：以信号变向次数近似（每次换仓等额买卖）
            # AShareFeeModel 区分买卖方向：卖出多收印花税 + 最低 5 元限制
            cost_drag = 0.0
            if self._fee_model is not None and len(returns) > 0:
                # 假设每次换仓的成交金额约等于账户初始净值（1 个单位）
                unit_value = equity.iloc[0]
                turnover_count = int((returns > 0).astype(int).diff().abs().sum())
                # 买入成本 + 卖出成本（各 turnover_count 次）
                single_buy  = self._fee_model.calc_fee(unit_value, 'BUY')
                single_sell = self._fee_model.calc_fee(unit_value, 'SELL')
                total_cost  = (single_buy + single_sell) * turnover_count
                # 将成本折算为收益率百分比
                cost_drag = total_cost / unit_value * 100

            summary.append({
                '因子': trader_name,
                '最终净值': final_value,
                '总收益率(%)': total_return - cost_drag,
                '年化收益率(%)': annual_return,
                'Sharpe Ratio': sharpe,
                '最大回撤(%)': max_drawdown,
                '胜率(%)': win_rate,
                '成本拖拽(%)': round(cost_drag, 4),
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


    def analyze_factor_ic(
        self,
        trader_name: str,
        forward_periods: Optional[List[int]] = None,
        quantiles: int = 5,
        output_dir: Optional[str] = None
    ) -> Optional['pd.DataFrame']:
        """
        基于 Alphalens 生成因子 IC 分析报告。

        输出内容：
        1. IC / RankIC 均值、IR（信息比率）汇总表
        2. Rolling IC 曲线图
        3. N 分组净值曲线图（因子单调性验证）
        4. 因子收益率分解图（Tearsheet）

        Args:
            trader_name: 回测时使用的 trader_name（与 run_single_factor 一致）
            forward_periods: 预测周期列表，默认 [1, 5, 10, 20]（交易日）
            quantiles: 分组数量，默认 5（五分组）
            output_dir: 报告保存目录，默认使用 zvt_home/ui

        Returns:
            IC 分析汇总 DataFrame，若失败返回 None
        """
        if forward_periods is None:
            forward_periods = [1, 5, 10, 20]

        # 从存储的 adapter 中取数据
        adapter = self._adapters.get(trader_name)
        if adapter is None:
            print(f"  未找到 {trader_name} 的 Adapter，请先运行 run_single_factor()。")
            return None

        factor_df = getattr(adapter, 'factor_df', None)
        kdata_df  = getattr(adapter, 'kdata_df',  None)

        if factor_df is None or factor_df.empty:
            print(f"  {trader_name} 的因子数据为空，无法进行 IC 分析。")
            return None

        try:
            import alphalens

            # ── 数据格式转换 ──
            # factor_df: index=timestamp, columns=entity_id -> Series(MultiIndex)
            factor_series = (
                factor_df
                .stack()
                .reset_index()
                .rename(columns={'level_0': 'date', 'level_1': 'asset', 0: 'factor'})
                .set_index(['date', 'asset'])['factor']
            )

            # 收盘价宽表：index=date, columns=entity_id
            if kdata_df is not None and not kdata_df.empty:
                prices = kdata_df.reset_index().pivot(
                    index='timestamp', columns='entity_id', values='close'
                )
            else:
                print("  kdata_df 为空，无法构建价格矩阵。")
                return None

            # ── 调用 Alphalens ──
            print(f"\n{'='*60}")
            print(f"IC 分析: {trader_name}")
            print(f"{'='*60}")

            factor_data = alphalens.utils.get_clean_factor_and_forward_returns(
                factor=factor_series,
                prices=prices,
                periods=tuple(forward_periods),
                quantiles=quantiles,
                max_loss=0.35   # 允许最多 35% 的数据因前视期不足而丢失
            )

            # IC 均值汇总表
            ic_summary = alphalens.performance.factor_information_coefficient(factor_data)
            mean_ic = ic_summary.mean().rename('Mean IC')
            std_ic  = ic_summary.std().rename('IC Std')
            ir      = (mean_ic / std_ic).rename('IR')
            ic_table = pd.concat([mean_ic, std_ic, ir], axis=1)

            print("\n── IC 统计 ──")
            print(ic_table.to_string())

            # 确定输出目录
            if output_dir is None:
                save_dir = Path(zvt_env["zvt_home"]) / "ui" / "alphalens"
            else:
                save_dir = Path(output_dir)
            save_dir.mkdir(parents=True, exist_ok=True)

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = trader_name.replace("/", "_")

            # 绘图 1：Rolling IC 曲线
            import matplotlib
            matplotlib.use('Agg')   # 非交互模式，防止 macOS 弹窗
            import matplotlib.pyplot as plt

            alphalens.plotting.plot_ic_ts(ic_summary)
            plt.tight_layout()
            rolling_ic_path = save_dir / f"{safe_name}_rolling_ic_{ts}.png"
            plt.savefig(rolling_ic_path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"  Rolling IC 曲线 -> {rolling_ic_path}")

            # 绘图 2：分组净值曲线
            mean_quant_ret, _ = alphalens.performance.mean_period_weighted_return_by_quantile(
                factor_data
            )
            alphalens.plotting.plot_quantile_returns_bar(mean_quant_ret)
            plt.tight_layout()
            quant_path = save_dir / f"{safe_name}_quantile_return_{ts}.png"
            plt.savefig(quant_path, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"  分组平均收益 -> {quant_path}")

            # 汇总 IC 表保存为 CSV
            ic_csv_path = save_dir / f"{safe_name}_ic_summary_{ts}.csv"
            ic_table.to_csv(str(ic_csv_path), encoding='utf-8-sig')
            print(f"  IC 统计表    -> {ic_csv_path}")

            return ic_table

        except ImportError:
            print("  alphalens-reloaded 未安装，请运行: uv pip install alphalens-reloaded")
            return None
        except Exception as e:
            print(f"  IC 分析失败: {e}")
            import traceback
            traceback.print_exc()
            return None


__all__ = ['BatchBacktestRunner']
