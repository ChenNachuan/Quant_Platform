# -*- coding: utf-8 -*-
"""
绘制不同策略方案的净值曲线对比图

参考 if_ALSTM_rolling_3state_intra_executed.ipynb 中的绘图方式
"""

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from strategies.futures.alstm.config import ARTIFACT_ROOT, PLOT_ROOT

# 设置中文字体
plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

# 输出目录
OUTPUT_DIR = PLOT_ROOT
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_account_curves(csv_path: Path) -> pd.DataFrame:
    """加载账户曲线CSV文件"""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.set_index("datetime")
    return df


def calculate_metrics(curve: pd.Series, initial_account: float = 1_500_000) -> dict:
    """计算策略指标"""
    curve = (
        pd.to_numeric(curve, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    if curve.empty:
        return {
            "total_return": np.nan,
            "annualized_sharpe": np.nan,
            "max_drawdown": np.nan,
        }

    ret = curve.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    ret_mean = ret.mean() if len(ret) else np.nan
    ret_std = ret.std(ddof=1) if len(ret) > 1 else np.nan

    # 年化夏普比率（假设1分钟K线，每天240分钟，每年252天）
    periods_per_year = 240 * 252
    sharpe = ret_mean / ret_std if np.isfinite(ret_std) and ret_std > 0 else np.nan
    annualized_sharpe = (
        sharpe * np.sqrt(periods_per_year) if np.isfinite(sharpe) else np.nan
    )

    drawdown = curve / curve.cummax() - 1

    return {
        "total_return": float(curve.iloc[-1] / initial_account - 1),
        "annualized_sharpe": float(annualized_sharpe),
        "max_drawdown": float(drawdown.min()),
    }


def plot_curves_comparison(
    curves_dict: dict[str, pd.DataFrame],
    title: str = "策略净值曲线对比",
    output_path: Path | None = None,
    figsize: tuple = (14, 6),
) -> None:
    """
    绘制多个方案的净值曲线对比图

    Args:
        curves_dict: {方案名称: DataFrame} 字典
        title: 图表标题
        output_path: 输出文件路径
        figsize: 图表大小
    """
    fig, axes = plt.subplots(
        2, 1, figsize=figsize, gridspec_kw={"height_ratios": [3, 1]}
    )

    # 上图：净值曲线
    ax1 = axes[0]
    colors = plt.cm.Set2(np.linspace(0, 1, len(curves_dict)))

    for (name, df), color in zip(curves_dict.items(), colors):
        # 选择第一列（策略列）绘制
        col = df.columns[0]
        curve = df[col]
        ax1.plot(curve.index, curve.values, label=name, color=color, linewidth=1.2)

    ax1.set_title(title, fontsize=14, fontweight="bold")
    ax1.set_ylabel("账户净值", fontsize=12)
    ax1.legend(loc="upper left", fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis="x", rotation=45)

    # 下图：回撤曲线
    ax2 = axes[1]
    for (name, df), color in zip(curves_dict.items(), colors):
        col = df.columns[0]
        curve = df[col]
        drawdown = curve / curve.cummax() - 1
        ax2.fill_between(
            drawdown.index, drawdown.values, alpha=0.3, color=color, label=name
        )

    ax2.set_ylabel("回撤", fontsize=12)
    ax2.set_xlabel("时间", fontsize=12)
    ax2.legend(loc="lower left", fontsize=10)
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis="x", rotation=45)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"图表已保存: {output_path}")

    plt.show()


def plot_multi_strategy_curves(
    csv_paths: dict[str, Path],
    title: str = "多策略净值曲线对比",
    output_path: Path | None = None,
) -> pd.DataFrame:
    """
    绘制多个策略的净值曲线并返回指标汇总

    Args:
        csv_paths: {策略名称: CSV路径} 字典
        title: 图表标题
        output_path: 输出文件路径

    Returns:
        指标汇总DataFrame
    """
    curves_dict = {}
    metrics_list = []

    for name, path in csv_paths.items():
        if not path.exists():
            print(f"警告: 文件不存在 - {path}")
            continue

        df = load_account_curves(path)
        curves_dict[name] = df

        # 计算指标
        col = df.columns[0]
        metrics = calculate_metrics(df[col])
        metrics["strategy"] = name
        metrics_list.append(metrics)

    if not curves_dict:
        print("错误: 没有找到有效的曲线数据")
        return pd.DataFrame()

    # 绘制曲线
    plot_curves_comparison(curves_dict, title=title, output_path=output_path)

    # 返回指标汇总
    metrics_df = pd.DataFrame(metrics_list)
    metrics_df = metrics_df[
        ["strategy", "total_return", "annualized_sharpe", "max_drawdown"]
    ]
    metrics_df = metrics_df.sort_values("annualized_sharpe", ascending=False)

    return metrics_df


def main():
    """主函数：绘制不同方案的净值曲线对比"""

    # 定义要对比的方案
    base_path = ARTIFACT_ROOT / "if_alstm_rolling_3state_intra_2025"

    # 方案1: 状态过滤方案对比
    state_filter_paths = {
        "基准 (baseline)": base_path
        / "state_future_vol_predicted_high_filter"
        / "continuous_once_run_backtest"
        / "continuous_account_curves.csv",
        "高波动过滤": base_path
        / "state_highvol_highvolume_only_filter"
        / "continuous_once_run_backtest"
        / "continuous_account_curves.csv",
        "未来波动率预测": base_path
        / "state_future_vol_predicted_high_filter"
        / "continuous_once_run_backtest"
        / "continuous_account_curves.csv",
    }

    # 方案2: Pv3 两阶段模型对比
    pv3_paths = {
        "Pv3 Model B Compare": base_path
        / "pv3_two_stage_trade_direction_model_b_compare"
        / "alstm3_retrain_ab_split_raw_634d"
        / "continuous_once_run_backtest"
        / "alstm3_saved_634b"
        / "continuous_account_curves.csv",
        "Enhanced Full": base_path
        / "pv3_two_stage_trade_direction_model_b_compare"
        / "alstm3_retrain_ab_split_raw_634d"
        / "enhanced_full_vs_nonflat_binary_635"
        / "continuous_once_run_backtest"
        / "full_enhanced_fee_0p0002"
        / "continuous_account_curves.csv",
        "Nonflat Binary Enhanced": base_path
        / "pv3_two_stage_trade_direction_model_b_compare"
        / "alstm3_retrain_ab_split_raw_634d"
        / "enhanced_full_vs_nonflat_binary_635"
        / "continuous_once_run_backtest"
        / "nonflat_binary_enhanced_fee_0p0002"
        / "continuous_account_curves.csv",
    }

    # 方案3: 10手方案对比
    lot10_paths = {
        "Full Raw 10lot": base_path
        / "pv3_two_stage_trade_direction_model_b_compare"
        / "alstm3_retrain_ab_split_raw_634d"
        / "pv3_10lot_from_634d_models"
        / "continuous_once_run_backtest"
        / "full_raw_10lot"
        / "continuous_account_curves.csv",
        "Full Model Led 10lot": base_path
        / "pv3_two_stage_trade_direction_model_b_compare"
        / "alstm3_retrain_ab_split_raw_634d"
        / "pv3_10lot_from_634d_models"
        / "continuous_once_run_backtest"
        / "full_model_led_10lot"
        / "continuous_account_curves.csv",
        "Nonflat Binary Raw 10lot": base_path
        / "pv3_two_stage_trade_direction_model_b_compare"
        / "alstm3_retrain_ab_split_raw_634d"
        / "pv3_10lot_from_634d_models"
        / "continuous_once_run_backtest"
        / "nonflat_binary_raw_10lot"
        / "continuous_account_curves.csv",
        "Nonflat Binary Model Led 10lot": base_path
        / "pv3_two_stage_trade_direction_model_b_compare"
        / "alstm3_retrain_ab_split_raw_634d"
        / "pv3_10lot_from_634d_models"
        / "continuous_once_run_backtest"
        / "nonflat_binary_model_led_10lot"
        / "continuous_account_curves.csv",
    }

    # 绘制各方案对比图
    print("=" * 60)
    print("绘制状态过滤方案对比图")
    print("=" * 60)
    metrics1 = plot_multi_strategy_curves(
        state_filter_paths,
        title="状态过滤方案净值曲线对比",
        output_path=OUTPUT_DIR / "state_filter_comparison.png",
    )
    print("\n指标汇总:")
    print(metrics1.to_string(index=False))

    print("\n" + "=" * 60)
    print("绘制Pv3两阶段模型对比图")
    print("=" * 60)
    metrics2 = plot_multi_strategy_curves(
        pv3_paths,
        title="Pv3两阶段模型净值曲线对比",
        output_path=OUTPUT_DIR / "pv3_model_comparison.png",
    )
    print("\n指标汇总:")
    print(metrics2.to_string(index=False))

    print("\n" + "=" * 60)
    print("绘制10手方案对比图")
    print("=" * 60)
    metrics3 = plot_multi_strategy_curves(
        lot10_paths,
        title="10手方案净值曲线对比",
        output_path=OUTPUT_DIR / "10lot_comparison.png",
    )
    print("\n指标汇总:")
    print(metrics3.to_string(index=False))

    # 合并所有指标
    all_metrics = pd.concat([metrics1, metrics2, metrics3], ignore_index=True)
    all_metrics.to_csv(
        OUTPUT_DIR / "all_metrics_summary.csv", index=False, encoding="utf-8-sig"
    )
    print(f"\n所有指标已保存: {OUTPUT_DIR / 'all_metrics_summary.csv'}")


if __name__ == "__main__":
    main()
