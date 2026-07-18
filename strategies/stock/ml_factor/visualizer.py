"""
Visualization module for ML Factor Strategy.
"""
import warnings
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from strategies.stock.ml_factor.config import ARTIFACT_ROOT

warnings.filterwarnings('ignore')


class StrategyVisualizer:
    """策略可视化器."""

    def __init__(self, output_dir: str | Path = ARTIFACT_ROOT):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 设置绘图风格
        sns.set_style('whitegrid')
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
        plt.rcParams['axes.unicode_minus'] = False

    def plot_cumulative_returns(
        self,
        performance_df: pd.DataFrame,
        strategy_name: str = "ML Factor",
        save_path: Optional[str] = None
    ):
        """绘制累计净值图."""
        cum_performance = (1 + performance_df).cumprod()

        plt.figure(figsize=(14, 8))

        # VW 策略 - 实线
        plt.plot(cum_performance.index, cum_performance['VW_Strategy'],
                 label='VW 多空策略 (市值加权)', color='#d62728', linewidth=2.5)
        plt.plot(cum_performance.index, cum_performance['VW_Long'],
                 label='VW 多头组', color='#ff7f0e', linewidth=1.5)
        plt.plot(cum_performance.index, cum_performance['VW_Short'],
                 label='VW 空头组', color='#2ca02c', linewidth=1.5)

        # EW 策略 - 虚线
        plt.plot(cum_performance.index, cum_performance['EW_Strategy'],
                 label='EW 多空策略 (等权)', color='#ff9896', linestyle='--', linewidth=2)
        plt.plot(cum_performance.index, cum_performance['EW_Long'],
                 label='EW 多头组', color='#ffbb78', linestyle='--', linewidth=1)
        plt.plot(cum_performance.index, cum_performance['EW_Short'],
                 label='EW 空头组', color='#98df8a', linestyle='--', linewidth=1)

        plt.axhline(y=1, color='grey', linestyle='-', linewidth=1)
        plt.title(f'{strategy_name} 策略表现', fontsize=18, fontweight='bold')
        plt.xlabel('年份', fontsize=12)
        plt.ylabel('累计净值', fontsize=12)
        plt.legend(fontsize=11, loc='upper left', ncol=3)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.show()

    def plot_rolling_ic(
        self,
        ic_series: pd.Series,
        strategy_name: str = "ML Factor",
        window_size: int = 12,
        save_path: Optional[str] = None
    ):
        """绘制滚动 IC 图."""
        plt.figure(figsize=(14, 6))

        rolling_ic = ic_series.rolling(window=window_size).mean()
        rolling_ic.plot(label=f'Rolling IC ({window_size}-month MA)', color='orange', linewidth=2)

        plt.axhline(y=0, color='grey', linestyle='--', alpha=0.6)
        plt.axhline(y=ic_series.mean(), color='blue', linestyle=':', label='Mean IC', alpha=0.8)

        plt.title(f'{strategy_name} Rolling IC', fontsize=15)
        plt.ylabel('IC Value', fontsize=12)
        plt.xlabel('Date', fontsize=12)
        plt.legend(loc='best')
        plt.grid(True, linestyle='--', alpha=0.3)
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.show()

    def plot_barra_attribution(
        self,
        attribution_summary: pd.DataFrame,
        strategy_name: str = "ML Factor",
        save_path: Optional[str] = None
    ):
        """绘制 Barra 归因堆叠图."""
        cum_attrib = attribution_summary.cumsum()

        plt.figure(figsize=(14, 7))

        plt.stackplot(
            cum_attrib.index,
            cum_attrib['Market'],
            cum_attrib['Industry'],
            cum_attrib['Style'],
            cum_attrib['Alpha'],
            labels=['Market (市场)', 'Industry (行业)', 'Style (风格)', 'Alpha (选股)'],
            alpha=0.7,
            colors=['#f1c40f', '#95a5a6', '#3498db', '#e74c3c']
        )

        cum_actual = attribution_summary.sum(axis=1).cumsum()
        plt.plot(cum_actual.index, cum_actual, color='black', linewidth=2.5, linestyle='--', label='Total Return')

        plt.title(f'{strategy_name} 多头组合收益归因分析', fontsize=16, fontweight='bold')
        plt.ylabel('Cumulative Return', fontsize=12)
        plt.legend(loc='upper left', fontsize=10)
        plt.grid(True, linestyle='--', alpha=0.3)
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.show()

    def plot_feature_importance(
        self,
        feature_importances: List[Dict],
        feature_cols: List[str],
        strategy_name: str = "ML Factor",
        top_n: int = 20,
        save_path: Optional[str] = None
    ):
        """绘制特征重要性图."""
        if not feature_importances:
            print("没有特征重要性数据")
            return

        # 使用最后一次的特征重要性
        last_imp = feature_importances[-1]
        imp_df = pd.DataFrame({
            'feature': feature_cols,
            'importance': last_imp['importance']
        }).sort_values('importance', ascending=False).head(top_n)

        plt.figure(figsize=(12, 8))
        plt.barh(range(len(imp_df)), imp_df['importance'].values, color='steelblue', alpha=0.8)
        plt.yticks(range(len(imp_df)), imp_df['feature'].values)
        plt.xlabel('Importance', fontsize=12)
        plt.title(f'{strategy_name} Top {top_n} Feature Importance', fontsize=15)
        plt.gca().invert_yaxis()
        plt.grid(axis='x', linestyle='--', alpha=0.5)
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.show()

    def plot_group_returns_heatmap(
        self,
        all_predictions: pd.DataFrame,
        strategy_name: str = "ML Factor",
        save_path: Optional[str] = None
    ):
        """绘制分组收益热力图."""
        # 计算每月各组平均收益
        group_returns = all_predictions.groupby(['date', 'group'])['next_ret'].mean().unstack()

        # 转换为年月格式
        group_returns.index = pd.to_datetime(group_returns.index)
        group_returns['year'] = group_returns.index.year
        group_returns['month'] = group_returns.index.month

        # 按年计算平均收益
        yearly_returns = group_returns.groupby('year')[[0.0, 1.0, 2.0, 3.0, 4.0]].mean()

        plt.figure(figsize=(12, 8))
        sns.heatmap(
            yearly_returns,
            annot=True,
            fmt='.2%',
            cmap='RdYlGn',
            center=0,
            xticklabels=['Group 1 (Short)', 'Group 2', 'Group 3', 'Group 4', 'Group 5 (Long)'],
            yticklabels=yearly_returns.index
        )
        plt.title(f'{strategy_name} 分组年度收益热力图', fontsize=15)
        plt.xlabel('Group', fontsize=12)
        plt.ylabel('Year', fontsize=12)
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.show()

    def plot_comparison(
        self,
        rf_performance: pd.DataFrame,
        xgb_performance: pd.DataFrame,
        save_path: Optional[str] = None
    ):
        """对比 RF 和 XGBoost 策略."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. 累计净值对比
        ax1 = axes[0, 0]
        rf_cum = (1 + rf_performance['VW_Strategy']).cumprod()
        xgb_cum = (1 + xgb_performance['VW_Strategy']).cumprod()
        ax1.plot(rf_cum.index, rf_cum.values, label='Random Forest', color='blue', linewidth=2)
        ax1.plot(xgb_cum.index, xgb_cum.values, label='XGBoost', color='red', linewidth=2)
        ax1.set_title('VW Strategy Cumulative Returns', fontsize=12)
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # 2. 滚动夏普对比
        ax2 = axes[0, 1]
        rf_sharpe = rf_performance['VW_Strategy'].rolling(12).mean() / rf_performance['VW_Strategy'].rolling(12).std() * np.sqrt(12)
        xgb_sharpe = xgb_performance['VW_Strategy'].rolling(12).mean() / xgb_performance['VW_Strategy'].rolling(12).std() * np.sqrt(12)
        ax2.plot(rf_sharpe.index, rf_sharpe.values, label='Random Forest', color='blue', linewidth=2)
        ax2.plot(xgb_sharpe.index, xgb_sharpe.values, label='XGBoost', color='red', linewidth=2)
        ax2.set_title('Rolling 12-Month Sharpe Ratio', fontsize=12)
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        # 3. 月度收益分布对比
        ax3 = axes[1, 0]
        ax3.hist(rf_performance['VW_Strategy'].dropna(), bins=50, alpha=0.5, label='Random Forest', color='blue')
        ax3.hist(xgb_performance['VW_Strategy'].dropna(), bins=50, alpha=0.5, label='XGBoost', color='red')
        ax3.set_title('Monthly Return Distribution', fontsize=12)
        ax3.legend()
        ax3.grid(True, alpha=0.3)

        # 4. 年度收益对比
        ax4 = axes[1, 1]
        rf_perf = rf_performance.copy()
        xgb_perf = xgb_performance.copy()
        rf_perf.index = pd.to_datetime(rf_perf.index)
        xgb_perf.index = pd.to_datetime(xgb_perf.index)
        rf_annual = rf_perf.groupby(rf_perf.index.year)['VW_Strategy'].sum()
        xgb_annual = xgb_perf.groupby(xgb_perf.index.year)['VW_Strategy'].sum()
        x = np.arange(len(rf_annual))
        width = 0.35
        ax4.bar(x - width/2, rf_annual.values, width, label='Random Forest', color='blue', alpha=0.7)
        ax4.bar(x + width/2, xgb_annual.values, width, label='XGBoost', color='red', alpha=0.7)
        ax4.set_title('Annual Returns Comparison', fontsize=12)
        ax4.set_xticks(x)
        ax4.set_xticklabels(rf_annual.index, rotation=45)
        ax4.legend()
        ax4.grid(True, alpha=0.3)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.show()
