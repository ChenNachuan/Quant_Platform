"""日报与 Alpha-Beta 归因报告生成模块。

在 :data:`REPORTS_DIR` 下生成两类报告：
- ``daily_{date}.md``：策略/系统 12 项指标表、当日告警列表与净值曲线图链接。
- ``attribution_{alpha_type_id}_{quarter}.md``：Alpha-Beta 回归、三线解读与因子贡献拆解。

``alpha_beta_attribution`` 基于月度收益率拟合 ``r_p = α + β·r_b + ε``，
年化按 ``alpha_monthly × 12`` 计算，返回全精度浮点数（舍入仅在渲染层处理）。
因子贡献拆解直接复用 :func:`research.factor_combiner.factor_contribution`。
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd
from scipy.stats import linregress

from research.factor_combiner import factor_contribution

__all__ = [
    "ANNUALIZATION",
    "REPORTS_DIR",
    "alpha_beta_attribution",
    "interpret_attribution",
    "write_attribution_report",
    "write_daily_report",
]

#: 日报与归因报告输出目录
REPORTS_DIR = Path("logs/ops/reports")

#: 年化系数（月频 × 12）
ANNUALIZATION = 12

#: 报告表格展示小数位数
_ALPHA_DP = 4
_BETA_DP = 3
_T_ALPHA_DP = 2

_NO_VALUE = "—"


# --- Alpha-Beta 单因子回归 ---


def _as_returns(values: Any, name: str) -> pd.Series:
    """转换为浮点序列；两输入序列按位置而非按索引对齐。"""
    try:
        series = pd.Series(values, dtype=float)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} is not a numeric series: {exc}") from exc
    return series


def alpha_beta_attribution(
    portfolio_returns: Any,
    benchmark_returns: Any,
) -> dict[str, float | None]:
    """拟合单因子回归 ``r_p = α + β·r_b + ε`` 并返回 5 项全精度归因指标。

    输入需为等长月度收益率序列（任一侧含 NaN 的行自动剔除）。
    长度不等、有效样本不足 2 个或基准方差为 0 时抛出 ``ValueError``。
    """
    r_p = _as_returns(portfolio_returns, "portfolio_returns")
    r_b = _as_returns(benchmark_returns, "benchmark_returns")
    if len(r_p) != len(r_b):
        raise ValueError(
            f"series length mismatch: portfolio_returns={len(r_p)} "
            f"benchmark_returns={len(r_b)}"
        )
    frame = pd.DataFrame({"p": r_p.to_numpy(), "b": r_b.to_numpy()}).dropna()
    if len(frame) < 2:
        raise ValueError(
            f"need >= 2 paired observations, got {len(frame)} after dropping NaN"
        )
    if not frame["b"].std(ddof=0) > 0:
        raise ValueError("benchmark_returns have no variance — beta is undefined")

    fit = linregress(frame["b"].to_numpy(), frame["p"].to_numpy())
    alpha_monthly = float(fit.intercept)
    stderr = float(fit.intercept_stderr)
    t_alpha = (
        alpha_monthly / stderr if math.isfinite(stderr) and stderr != 0.0 else None
    )
    return {
        "alpha_monthly": alpha_monthly,
        "alpha_annual": alpha_monthly * ANNUALIZATION,
        "beta": float(fit.slope),
        "r_squared": float(fit.rvalue**2),
        "t_alpha": t_alpha,
    }


# --- 归因报告三线解读 ---


def _finite(value: Any) -> float | None:
    """返回有限浮点数；``None``、布尔或非有限值返回 ``None``。"""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def interpret_attribution(attribution: Mapping[str, Any]) -> list[str]:
    """生成适用的 Alpha-Beta 解读条目（Alpha 与 R² 条件独立判定，缺失字段跳过）。"""
    if not isinstance(attribution, Mapping):
        raise TypeError(
            f"attribution must be a mapping, got {type(attribution).__name__}"
        )
    alpha = _finite(attribution.get("alpha_annual"))
    r_squared = _finite(attribution.get("r_squared"))

    out: list[str] = []
    if alpha is not None:
        if alpha > 0.08:
            out.append("Alpha 年化 > 8%：查回测问题（未来函数/费用遗漏）——太高不真实")
        elif 0.03 <= alpha <= 0.05:
            out.append("Alpha 年化 3-5%：优秀散户水平")
    if r_squared is not None:
        if r_squared > 0.7:
            out.append("R² > 0.7：beta 解释力强，alpha 可信")
        elif r_squared < 0.5:
            out.append("R² < 0.5：超额波动大，样本不足")
    return out


# --- 因子贡献表渲染 ---


def _contribution_table(shares: pd.Series) -> list[str]:
    """按因子贡献绝对值降序渲染 Markdown 表格行。"""
    ordered = shares.reindex(shares.abs().sort_values(ascending=False).index)
    return [
        f"| {name} | {float(share):.{_ALPHA_DP}f} |" for name, share in ordered.items()
    ]


# --- 报告渲染与落盘 ---


def _day(date: Any) -> str:
    """格式化报告日期为 ``YYYY-MM-DD``。"""
    if isinstance(date, str):
        return date
    if hasattr(date, "isoformat"):
        return date.isoformat()[:10]
    return str(date)


def _fmt(value: Any) -> str:
    if value is None:
        return _NO_VALUE
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return _NO_VALUE
        return f"{value:.{_ALPHA_DP}f}"
    return str(value)


def _metric_rows(section: Mapping[str, Any]) -> list[str]:
    rows: list[str] = []
    for name, entry in section.items():
        if isinstance(entry, Mapping):
            value, status = entry.get("value"), entry.get("status")
        else:
            value, status = entry, ""
        rows.append(f"| {name} | {_fmt(value)} | {status} |")
    return rows


def _alert_line(alert: Any) -> str:
    if not isinstance(alert, Mapping):
        return f"- {alert}"
    level = alert.get("level")
    text = getattr(level, "value", level)
    return f"- [{text}] {alert.get('msg', '')}"


def _nav_x_axis(nav: pd.Series) -> Any:
    """优先使用时间索引作为横轴，否则退化为位置序号。"""
    if isinstance(nav.index, pd.DatetimeIndex):
        return nav.index
    if len(nav.index) and isinstance(nav.index[0], str):
        return pd.to_datetime(list(nav.index))
    return range(len(nav))


def _render_nav_png(nav: pd.Series, path: Path) -> None:
    """在报告同目录生成净值曲线 PNG 图片。"""
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 3))
    ax.plot(_nav_x_axis(nav), nav.to_numpy(), linewidth=1.2, color="#1f4e79")
    ax.set_ylabel("nav")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)


def write_daily_report(
    date: str,
    metrics: Mapping[str, Any],
    alerts: Sequence[Any],
    nav: pd.Series | None = None,
) -> str:
    """生成 ``logs/ops/reports/daily_{date}.md`` 并返回文件路径。

    直接渲染已采集的指标表与告警列表；传入非空 ``nav`` 时同步绘制 ``nav_{date}.png``。
    """
    if not isinstance(metrics, Mapping):
        raise TypeError(f"metrics must be a mapping, got {type(metrics).__name__}")
    if alerts is None:
        raise TypeError("alerts must be a sequence, not None")
    if nav is not None and not isinstance(nav, pd.Series):
        nav = pd.Series(nav, dtype=float)

    day = _day(date)
    lines = [
        f"# Ops Daily Report {day}",
        "",
        "## 策略指标",
        "| 指标 | 值 | 状态 |",
        "|---|---|---|",
        *_metric_rows(metrics.get("strategy") or {}),
        "",
        "## 系统指标",
        "| 指标 | 值 | 状态 |",
        "|---|---|---|",
        *_metric_rows(metrics.get("system") or {}),
        "",
        "## 告警",
    ]
    alert_list = list(alerts)
    lines += [_alert_line(a) for a in alert_list] if alert_list else ["- 无告警"]
    png_name = f"nav_{day}.png"
    lines += ["", "## Nav 曲线", "", f"![nav curve]({png_name})"]

    reports = Path(REPORTS_DIR)
    reports.mkdir(parents=True, exist_ok=True)
    if nav is not None and len(nav):
        _render_nav_png(nav, reports / png_name)
    path = reports / f"daily_{day}.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def write_attribution_report(
    alpha_type_id: str,
    quarter: str,
    portfolio_returns: Any,
    benchmark_returns: Any,
    *,
    weights: pd.DataFrame,
    factor_returns: pd.DataFrame,
    factor_exposures: pd.DataFrame,
) -> str:
    """生成 ``attribution_{alpha_type_id}_{quarter}.md`` 并返回文件路径。

    包含 Alpha-Beta 回归表、复用 ``factor_contribution`` 的因子贡献拆解表与解读结论。
    """
    attribution = alpha_beta_attribution(portfolio_returns, benchmark_returns)
    shares = factor_contribution(weights, factor_returns, factor_exposures)

    t_alpha = attribution["t_alpha"]
    rows = [
        "| 字段 | 值 |",
        "|---|---|",
        f"| alpha_monthly | {attribution['alpha_monthly']:.{_ALPHA_DP}f} |",
        f"| alpha_annual | {attribution['alpha_annual']:.{_ALPHA_DP}f} |",
        f"| beta | {attribution['beta']:.{_BETA_DP}f} |",
        f"| r_squared | {attribution['r_squared']:.{_BETA_DP}f} |",
        f"| t_alpha | {_NO_VALUE if t_alpha is None else f'{t_alpha:.{_T_ALPHA_DP}f}'} |",
    ]
    read = interpret_attribution(attribution)
    lines = [
        f"# Attribution Report {alpha_type_id} {quarter}",
        "",
        "## Alpha-Beta 回归",
        *rows,
        "",
        "## 因子贡献拆解",
        "| 因子 | 贡献占比 |",
        "|---|---|",
        *_contribution_table(shares),
        "",
        "## 解读",
        *([f"- {line}" for line in read] if read else ["- 无适用解读"]),
    ]

    reports = Path(REPORTS_DIR)
    reports.mkdir(parents=True, exist_ok=True)
    path = reports / f"attribution_{alpha_type_id}_{quarter}.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)
