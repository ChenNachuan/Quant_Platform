"""
Backtest module for ML Factor Strategy.

包含分组测试、收益率计算、统计指标等。
"""
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats

from .config import BacktestConfig

warnings.filterwarnings('ignore')


class PortfolioConstructor:
    """投资组合构建器."""

    def __init__(self, config: BacktestConfig):
        self.config = config

    def assign_groups(self, all_predictions: pd.DataFrame) -> pd.DataFrame:
        """将股票按预测值分组."""
        all_predictions = all_predictions.copy()
        all_predictions['group'] = all_predictions.groupby('date')['pred_ret'].transform(
            self._assign_group
        )
        return all_predictions

    def _assign_group(self, x):
        """分组函数."""
        if len(x) < self.config.min_stocks_per_group:
            return np.nan

        try:
            return pd.qcut(x.rank(method='first'), q=self.config.n_groups, labels=False)
        except Exception as e:
            print(f"Groupby Error: {e}")
            return np.nan


class PerformanceCalculator:
    """业绩计算器."""

    def __init__(self, config: BacktestConfig):
        self.config = config

    def calculate_returns(self, all_predictions: pd.DataFrame) -> pd.DataFrame:
        """计算等权和市值加权收益."""
        print("=" * 50)
        print("Step 3: 分组收益率测算")
        print("=" * 50)

        # 等权收益
        port_returns_ew = all_predictions.groupby(['date', 'group'])['next_ret'].mean().unstack()
        strategy_ew = port_returns_ew[4.0] - port_returns_ew[0.0]

        # 市值加权收益
        port_returns_vw = all_predictions.groupby(['date', 'group']).apply(
            self._weighted_avg_ret
        ).unstack()
        strategy_vw = port_returns_vw[4.0] - port_returns_vw[0.0]

        # 合并结果
        performance_df = pd.DataFrame({
            'EW_Long': port_returns_ew[4.0],
            'EW_Short': port_returns_ew[0.0],
            'EW_Strategy': strategy_ew,
            'VW_Long': port_returns_vw[4.0],
            'VW_Short': port_returns_vw[0.0],
            'VW_Strategy': strategy_vw
        })

        print("第三步完成：双重加权逻辑计算完毕。")
        print(performance_df.head())
        return performance_df

    def _weighted_avg_ret(self, group_df):
        """市值加权平均收益."""
        if group_df['raw_size'].sum() == 0:
            return group_df['next_ret'].mean()
        return np.average(group_df['next_ret'], weights=group_df['raw_size'])


class StatisticsCalculator:
    """统计指标计算器."""

    @staticmethod
    def calculate_summary_stats(performance_df: pd.DataFrame) -> pd.DataFrame:
        """计算策略表现统计表."""
        print("=" * 50)
        print("Step 4: 策略表现统计表")
        print("=" * 50)

        summary_table = performance_df.apply(StatisticsCalculator._get_summary_stats).T
        print(summary_table.to_string())
        return summary_table

    @staticmethod
    def _get_summary_stats(returns):
        """计算单个策略的统计指标."""
        num_months = len(returns)
        avg_ret = returns.mean()
        std_dev = returns.std()

        # 年化指标
        ann_ret = avg_ret * 12
        ann_std = std_dev * np.sqrt(12)

        # 夏普比率
        sharpe = ann_ret / ann_std if ann_std != 0 else 0

        # 最大回撤
        cum_ret = (1 + returns).cumprod()
        max_dd = ((cum_ret.cummax() - cum_ret) / cum_ret.cummax()).max()

        # t 统计量
        t_stat, p_val = stats.ttest_1samp(returns, 0)

        # 胜率
        win_rate = len(returns[returns > 0]) / num_months

        return pd.Series({
            'Avg Return (Monthly)': f"{avg_ret:.2%}",
            'Ann Return': f"{ann_ret:.2%}",
            'Ann Std Dev': f"{ann_std:.2%}",
            'Sharpe Ratio': f"{sharpe:.4f}",
            't-statistic': f"{t_stat:.4f}",
            'Max Drawdown': f"{max_dd:.2%}",
            'Win Rate': f"{win_rate:.2%}",
            'Obs (Months)': num_months
        })


class BacktestEngine:
    """回测引擎."""

    def __init__(self, config: BacktestConfig):
        self.config = config
        self.portfolio_constructor = PortfolioConstructor(config)
        self.performance_calculator = PerformanceCalculator(config)
        self.statistics_calculator = StatisticsCalculator()

    def run(self, all_predictions: pd.DataFrame) -> Dict:
        """运行完整回测."""
        # 1. 分组
        all_predictions = self.portfolio_constructor.assign_groups(all_predictions)

        # 2. 计算收益
        performance_df = self.performance_calculator.calculate_returns(all_predictions)

        # 3. 计算统计指标
        summary_stats = self.statistics_calculator.calculate_summary_stats(performance_df)

        # 4. 保存分组收益供 GRS 使用
        port_returns_vw_full = all_predictions.groupby(['date', 'group']).apply(
            self.performance_calculator._weighted_avg_ret
        ).unstack()
        port_returns_vw_full.columns = [0, 1, 2, 3, 4]
        port_returns_vw_full.index = pd.to_datetime(port_returns_vw_full.index) + pd.offsets.MonthEnd(0)

        return {
            'all_predictions': all_predictions,
            'performance_df': performance_df,
            'summary_stats': summary_stats,
            'port_returns_vw_full': port_returns_vw_full
        }
