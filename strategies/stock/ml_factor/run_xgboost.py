"""
XGBoost Strategy Runner.

运行 XGBoost ML 因子策略的完整流程。
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from strategies.stock.ml_factor.config import StrategyConfig, XGBoostConfig
from strategies.stock.ml_factor.data_preprocessor import DataPreprocessor, BenchmarkDataLoader
from strategies.stock.ml_factor.model_trainer import create_trainer
from strategies.stock.ml_factor.backtest import BacktestEngine
from strategies.stock.ml_factor.evaluator import (
    ICEvaluator, BarraEvaluator, FamaMacBethEvaluator,
    FactorModelEvaluator, GRSEvaluator
)


def main():
    """运行 XGBoost 策略."""
    print("=" * 60)
    print("XGBoost ML 因子策略")
    print("=" * 60)

    # 1. 初始化配置
    config = StrategyConfig()

    # 2. 数据预处理
    preprocessor = DataPreprocessor(config.data, config.preprocess)
    df = preprocessor.load_and_prepare()
    feature_cols = preprocessor.get_feature_cols()

    # 3. 模型训练与预测
    # 注意：如果没有 GPU，将 device 改为 "cpu"
    xgb_config = XGBoostConfig(device="cpu")  # 使用 CPU 模式，兼容性更好
    trainer = create_trainer("xgboost", xgb_config=xgb_config)
    all_predictions = trainer.train_and_predict(df, feature_cols)

    # 保存预测结果
    output_dir = Path(config.data.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    all_predictions.to_parquet(output_dir / 'xgb_predictions.parquet', index=False)

    # 4. IC 检验
    ic_series = ICEvaluator.calculate_ic(all_predictions)
    ic_series.to_csv(output_dir / 'xgb_ic_series.csv')

    # 5. 回测
    backtest_engine = BacktestEngine(config.backtest)
    results = backtest_engine.run(all_predictions)
    performance_df = results['performance_df']
    summary_stats = results['summary_stats']

    # 保存回测结果
    performance_df.to_parquet(output_dir / 'xgb_performance.parquet')

    # 6. Barra 检验
    barra_evaluator = BarraEvaluator(config.barra, config.data)
    monthly_gammas = barra_evaluator.run_barra_regression(all_predictions, industry_col='industry3')

    # 7. Fama-MacBeth 检验
    fmb_results = FamaMacBethEvaluator.run_regression(all_predictions, df)

    # 8. 因子模型检验
    factor_evaluator = FactorModelEvaluator(config.data)
    ch3_results = factor_evaluator.run_ch3_test(performance_df)
    ch4_results = factor_evaluator.run_ch4_test(performance_df)
    carhart_results = factor_evaluator.run_carhart_test(performance_df)

    # 9. GRS 检验
    factors_df = pd.read_excel(config.data.ch3_factors_path)
    factors_df = factors_df.rename(columns={
        'mnthdt': 'date',
        'mktrf': 'MKT',
        'SMB': 'SMB',
        'VMG': 'VMG'
    })
    factors_df['date'] = pd.to_datetime(factors_df['date'].astype(str))
    factors_df = factors_df.set_index('date')
    factors_df.index = factors_df.index + pd.offsets.MonthEnd(0)

    grs_stat, grs_p, grs_alphas = GRSEvaluator.run_test(
        factors_df, results['port_returns_vw_full']
    )

    # 10. 绘图
    plot_results(performance_df, ic_series, config.data)

    print("\n" + "=" * 60)
    print("XGBoost 策略运行完成！")
    print("=" * 60)


def plot_results(performance_df, ic_series, data_config):
    """绘制结果图表."""
    sns.set_style('whitegrid')
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
    plt.rcParams['axes.unicode_minus'] = False

    # 1. 累计净值图
    cum_performance = (1 + performance_df).cumprod()

    plt.figure(figsize=(14, 8))

    plt.plot(cum_performance.index, cum_performance['VW_Strategy'],
             label='VW 多空策略 (市值加权)', color='#d62728', linewidth=2.5)
    plt.plot(cum_performance.index, cum_performance['VW_Long'],
             label='VW 多头组', color='#ff7f0e', linewidth=1.5)
    plt.plot(cum_performance.index, cum_performance['VW_Short'],
             label='VW 空头组', color='#2ca02c', linewidth=1.5)

    plt.plot(cum_performance.index, cum_performance['EW_Strategy'],
             label='EW 多空策略 (等权)', color='#ff9896', linestyle='--', linewidth=2)
    plt.plot(cum_performance.index, cum_performance['EW_Long'],
             label='EW 多头组', color='#ffbb78', linestyle='--', linewidth=1)
    plt.plot(cum_performance.index, cum_performance['EW_Short'],
             label='EW 空头组', color='#98df8a', linestyle='--', linewidth=1)

    plt.axhline(y=1, color='grey', linestyle='-', linewidth=1)
    plt.title('XGBoost 策略表现', fontsize=18, fontweight='bold')
    plt.xlabel('年份', fontsize=12)
    plt.ylabel('累计净值', fontsize=12)
    plt.legend(fontsize=11, loc='upper left', ncol=3)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    output_dir = Path(data_config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_dir / 'xgb_cumulative_returns.png', dpi=150)
    plt.show()

    # 2. IC 图
    plt.figure(figsize=(14, 6))
    rolling_ic = ic_series.rolling(window=12).mean()
    rolling_ic.plot(label='Rolling IC (12-month MA)', color='orange', linewidth=2)
    plt.axhline(y=0, color='grey', linestyle='--', alpha=0.6)
    plt.axhline(y=ic_series.mean(), color='blue', linestyle=':', label='Mean IC', alpha=0.8)
    plt.title('XGBoost Rolling IC', fontsize=15)
    plt.ylabel('IC Value', fontsize=12)
    plt.xlabel('Date', fontsize=12)
    plt.legend(loc='best')
    plt.grid(True, linestyle='--', alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / 'xgb_rolling_ic.png', dpi=150)
    plt.show()


if __name__ == "__main__":
    main()
