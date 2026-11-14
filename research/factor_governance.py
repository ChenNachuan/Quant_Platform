"""因子治理层：准入/退出线、生命周期状态机与质量评估。

消费 ``factor_evaluation`` 的 IC 序列及 ``FactorRegistry`` 的元数据。
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pandas as pd

from research.factor_evaluation import (
    EXIT_IC_THRESHOLD,
    GATE_IC_MIN,
    GATE_IR_MIN,
    GATE_T_BASE,
)

__all__ = [
    "CORR_MAX",
    "CS_STD_CV_ABNORMAL",
    "EXIT_IC_THRESHOLD",
    "EXIT_RATIO",
    "EXIT_T_THRESHOLD",
    "GATE_IC_MIN",
    "GATE_IR_MIN",
    "GATE_T_BASE",
    "HEALTHY_RATIO",
    "META_FIELDS",
    "N_QUANTILE_GROUPS",
    "WARNING_RATIO",
    "WINDOW_12M",
    "WINDOW_36M",
    "WINDOW_6M",
    "check_admission",
    "check_factor_health",
    "detect_redundant_factors",
    "diagnose_factor",
    "evaluate_factor",
    "list_factor_meta",
    "plot_factor_report",
    "redundancy_tier",
    "transition_lifecycle",
]

# ---------------------------------------------------------------------------
# 常量定义
# ---------------------------------------------------------------------------

#: 从 Factor 子类提取的元数据类属性。
META_FIELDS: list[str] = [
    "name",
    "version",
    "description",
    "category",
    "frequency",
    "asset_types",
    "depends_on",
    "status",
    "created_at",
]

#: 准入条件允许的与现有因子最大平均截面 Spearman 相关系数绝对值。
CORR_MAX: float = 0.5

#: 滚动 36 个月 t 统计量退出阈值（严格小于）。
EXIT_T_THRESHOLD: float = 1.5

#: 健康度级联比例阈值：recent_6m > HEALTHY_RATIO * mean_ic → healthy。
HEALTHY_RATIO: float = 0.7
#: 健康度级联比例阈值：recent_6m > WARNING_RATIO * mean_ic → warning。
WARNING_RATIO: float = 0.5
#: 健康度级联比例阈值：recent_12m < EXIT_RATIO * mean_ic → exit（相对衰减）。
EXIT_RATIO: float = 0.3

# 日频 IC 窗口长度（交易日）
WINDOW_6M: int = 126
WINDOW_12M: int = 252
WINDOW_36M: int = 756

# 生命周期合法状态转移
_LIFECYCLE_LEGAL: set[tuple[str, str]] = {
    ("draft", "production"),
    ("production", "deprecated"),
}

#: 截面标准差变异系数异常阈值。
CS_STD_CV_ABNORMAL: float = 0.5
#: plot_factor_report 默认分层组数。
N_QUANTILE_GROUPS: int = 5


# ---------------------------------------------------------------------------
# list_factor_meta
# ---------------------------------------------------------------------------


def list_factor_meta() -> list[dict[str, Any]]:
    """遍历 FactorRegistry 提取所有已注册因子的类级元数据（键为 META_FIELDS，缺失则为 None）。"""
    from core.registry import FactorRegistry

    result: list[dict[str, Any]] = []
    for name in FactorRegistry.list_all():
        cls = FactorRegistry.get_class(name)
        result.append({f: getattr(cls, f, None) for f in META_FIELDS})
    return result


# ---------------------------------------------------------------------------
# evaluate_factor
# ---------------------------------------------------------------------------


def evaluate_factor(
    factor_values: pd.DataFrame,
    returns: pd.DataFrame,
    factor_name: str,
) -> dict[str, Any]:
    """按日期计算截面 Spearman IC 评估指标。

    参数 ``factor_values`` 为 ``[date, ticker, factor_name]``，``returns`` 为对齐后的
    ``[date, ticker, fwd_return]``。返回 ``{ic_mean, ic_ir, ic_t, n_periods, coverage}``，
    其中 ``ic_std == 0`` 时 ``ic_ir = 0.0``，``t = IR * sqrt(n)``。
    """
    merged = factor_values.merge(returns, on=["date", "ticker"])
    ic_series = merged.groupby("date").apply(
        lambda g: g[factor_name].corr(g["fwd_return"], method="spearman"),
        include_groups=False,
    )
    n_periods = int(ic_series.notna().sum())
    ic_mean = float(ic_series.mean())
    ic_std = float(ic_series.std())
    ic_ir = ic_mean / ic_std if ic_std > 0 else 0.0
    ic_t = ic_ir * math.sqrt(n_periods) if n_periods > 0 else 0.0
    coverage = float(factor_values[factor_name].notna().mean())
    return {
        "ic_mean": round(ic_mean, 4),
        "ic_ir": round(ic_ir, 3),
        "ic_t": round(ic_t, 2),
        "n_periods": n_periods,
        "coverage": round(coverage, 3),
    }


# ---------------------------------------------------------------------------
# check_factor_health
# ---------------------------------------------------------------------------


def check_factor_health(ic_history: pd.Series) -> str:
    """因子健康度级联判定：先判绝对退出线，再判相对衰减。

    判定顺序：
    1. ``ic_mean < 0.03`` 或近 756 期 ``t < 1.5`` → ``exit``（零标准差窗口不触发 t 退出，不足 756 期则使用全部样本）
    2. ``recent_6m > 0.7 * mean_ic`` → ``healthy``
    3. ``recent_6m > 0.5 * mean_ic`` → ``warning``
    4. ``recent_12m < 0.3 * mean_ic`` → ``exit``
    5. 其余 → ``warning``
    """
    mean_ic = float(ic_history.mean())

    if mean_ic < EXIT_IC_THRESHOLD:
        return "exit"

    window = ic_history.tail(WINDOW_36M)
    if len(window) > 0:
        w_mean = float(window.mean())
        w_std = float(window.std())
        if w_std > 0:
            t = w_mean / w_std * math.sqrt(len(window))
            if t < EXIT_T_THRESHOLD:
                return "exit"

    recent_6m = float(ic_history.tail(WINDOW_6M).mean())
    recent_12m = float(ic_history.tail(WINDOW_12M).mean())

    if recent_6m > HEALTHY_RATIO * mean_ic:
        return "healthy"
    if recent_6m > WARNING_RATIO * mean_ic:
        return "warning"
    if recent_12m < EXIT_RATIO * mean_ic:
        return "exit"
    return "warning"


# ---------------------------------------------------------------------------
# check_admission
# ---------------------------------------------------------------------------


def check_admission(metrics: dict, max_corr: float) -> tuple[bool, dict]:
    """因子准入检验（严格不等式，边界值不通过）。

    四项条件：``ic_mean > 0.04``、``ic_ir > 0.5``、``ic_t > 2``、``max_corr < 0.5``。
    返回 ``(all_passed, detail)``，``detail`` 按条件名记录 ``{value, threshold, passed}``。
    """
    ic = float(metrics["ic_mean"])
    ir = float(metrics["ic_ir"])
    t = float(metrics["ic_t"])
    corr = float(max_corr)

    detail = {
        "ic": {
            "value": ic,
            "threshold": GATE_IC_MIN,
            "passed": ic > GATE_IC_MIN,
        },
        "ir": {
            "value": ir,
            "threshold": GATE_IR_MIN,
            "passed": ir > GATE_IR_MIN,
        },
        "t": {
            "value": t,
            "threshold": GATE_T_BASE,
            "passed": t > GATE_T_BASE,
        },
        "corr": {
            "value": corr,
            "threshold": CORR_MAX,
            "passed": corr < CORR_MAX,
        },
    }
    passed = all(d["passed"] for d in detail.values())
    return passed, detail


# ---------------------------------------------------------------------------
# transition_lifecycle
# ---------------------------------------------------------------------------


def transition_lifecycle(current: str, target: str) -> str:
    """生命周期状态机：仅允许 ``draft → production`` 与 ``production → deprecated``，其余均抛出 ``ValueError``。"""
    if (current, target) not in _LIFECYCLE_LEGAL:
        raise ValueError(
            f"illegal lifecycle transition: {current!r} -> {target!r}; "
            f"legal: draft->production, production->deprecated"
        )
    return target


# ---------------------------------------------------------------------------
# detect_redundant_factors & redundancy_tier
# ---------------------------------------------------------------------------


def detect_redundant_factors(
    factor_matrix: pd.DataFrame,
    threshold: float = 0.7,
) -> list[tuple[str, str, float]]:
    """按日截面 Spearman 相关系数检测冗余因子对。

    接收宽表 ``[date, ticker, f1, f2, ...]``，先剔除全 NaN 因子行，保留平均相关系数绝对值
    严格大于 ``threshold`` 的因子对，按 ``-|corr|`` 降序返回 ``[(f1, f2, round(avg, 3))]``。
    """
    factor_cols = [c for c in factor_matrix.columns if c not in ("date", "ticker")]
    cleaned = factor_matrix.dropna(subset=factor_cols, how="all")
    pairs: list[tuple[str, str, float]] = []
    for i, f1 in enumerate(factor_cols):
        for f2 in factor_cols[i + 1 :]:
            daily_corr = cleaned.groupby("date").apply(
                lambda g: g[f1].corr(g[f2], method="spearman"),
                include_groups=False,
            )
            avg_corr = float(daily_corr.mean())
            if abs(avg_corr) > threshold:
                pairs.append((f1, f2, round(avg_corr, 3)))
    return sorted(pairs, key=lambda x: -abs(x[2]))


def redundancy_tier(abs_corr: float) -> str:
    """将平均截面 Spearman 相关系数绝对值映射为冗余分档：``high`` (>0.9)、``possible`` ((0.7, 0.9])、``warn`` ([0.5, 0.7])、``independent`` (<0.5)。"""
    if abs_corr > 0.9:
        return "high"
    if abs_corr > 0.7:
        return "possible"
    if abs_corr >= 0.5:
        return "warn"
    return "independent"


# ---------------------------------------------------------------------------
# diagnose_factor
# ---------------------------------------------------------------------------


def diagnose_factor(df: pd.DataFrame, factor_name: str) -> dict[str, Any]:
    """因子异常数据质量诊断，返回 12 项统计指标（``cs_std_cv > 0.5`` 标识截面异常）。"""
    s = df[factor_name]
    by_date_nan = df.groupby("date")[factor_name].apply(lambda x: x.isna().mean())
    cs_std = df.groupby("date")[factor_name].std()
    cs_mean = float(cs_std.mean()) if len(cs_std) else 0.0
    cs_std_cv = round(float(cs_std.std()) / cs_mean, 3) if cs_mean > 0 else 0.0
    return {
        "total_rows": int(len(df)),
        "nan_ratio": round(float(s.isna().mean()), 4),
        "inf_count": int(s.isin([float("inf"), float("-inf")]).sum()),
        "zero_ratio": round(float((s == 0).mean()), 4),
        "coverage_mean": round(float(1 - by_date_nan.mean()), 3),
        "coverage_min": round(float(1 - by_date_nan.max()), 3),
        "min_date": by_date_nan.idxmax(),
        "value_min": float(s.min()),
        "value_max": float(s.max()),
        "value_mean": float(s.mean()),
        "value_std": float(s.std()),
        "cs_std_cv": cs_std_cv,
    }


# ---------------------------------------------------------------------------
# plot_factor_report
# ---------------------------------------------------------------------------


def plot_factor_report(
    factor_values: pd.DataFrame,
    returns: pd.DataFrame,
    factor_name: str,
    n_groups: int = N_QUANTILE_GROUPS,
) -> None:
    """生成 2×2 因子评估报告图（IC 序列、累计 IC、截面分布直方图、分位累计收益），保存至 ``research/reports/{factor_name}_tearsheet.png``。"""
    import matplotlib.pyplot as plt

    merged = factor_values.merge(returns, on=["date", "ticker"])
    ic_series = merged.groupby("date").apply(
        lambda g: g[factor_name].corr(g["fwd_return"], method="spearman"),
        include_groups=False,
    )

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    axes[0, 0].bar(range(len(ic_series)), ic_series.values, width=1.0)
    axes[0, 0].axhline(ic_series.mean(), color="red", ls="--")
    axes[0, 0].set_title(f"IC Series (mean={ic_series.mean():.4f})")

    axes[0, 1].plot(ic_series.cumsum().values)
    axes[0, 1].set_title("Cumulative IC")

    axes[1, 0].hist(factor_values[factor_name].dropna(), bins=50)
    axes[1, 0].set_title("Cross-Section Distribution")

    _plot_quantile_returns(axes[1, 1], merged, factor_name, n_groups)
    axes[1, 1].set_title(f"Quantile Returns (n={n_groups})")

    plt.tight_layout()
    out = Path("research/reports") / f"{factor_name}_tearsheet.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150)
    plt.close(fig)


def _plot_quantile_returns(ax, merged: pd.DataFrame, factor_name: str, n: int) -> None:
    """绘制各分位数组合的累计平均前瞻收益。"""
    try:
        q = merged.groupby("date")[factor_name].transform(
            lambda s: pd.qcut(s.rank(method="first"), n, labels=False) + 1
        )
    except ValueError:
        q = merged.groupby("date")[factor_name].transform(
            lambda s: pd.cut(s.rank(method="first"), n, labels=False) + 1
        )
    grp = merged.assign(_q=q)
    q_ret = grp.groupby(["date", "_q"])["fwd_return"].mean().unstack()
    cum = q_ret.cumsum()
    for col in cum.columns:
        ax.plot(range(len(cum)), cum[col], label=f"Q{int(col)}")
    ax.legend()
