"""Daily report + Alpha-Beta attribution (``docs/design/ops/reporting.md``).

Two report families land in :data:`REPORTS_DIR`:

* ``daily_{date}.md`` — the metrics table (strategy then system, monitoring's
  12-metric snapshot), the day's alert list, and a link to the nav curve png.
* ``attribution_{alpha_type_id}_{quarter}.md`` — the Alpha-Beta fit, the
  design's three-line read of it, and the per-factor contribution breakdown.

:func:`alpha_beta_attribution` is the design's single-factor regression
(``r_p = α + β·r_b + ε``, §18.6) fitted with :func:`scipy.stats.linregress`.
``alpha_annual`` is ``alpha_monthly × 12`` — the design's 月频×12 note is a
simple multiplication, not compounding, so the return series is read as
monthly.  The five returned fields are full-precision floats; the design
snippet rounds them, but that is report formatting and lives in the renderers
here rather than in the numeric result.

The factor-contribution breakdown **reuses**
:func:`research.factor_combiner.factor_contribution` (§63.6).  Reporting never
re-implements it — that is the whole point of the seam.

Nothing here collects metrics or evaluates alerts: those are
:mod:`ops.monitoring` and :mod:`ops.alerting`.  This module renders their
output and stops there.  ``scipy`` and ``matplotlib`` arrive with the declared
``alphalens-reloaded`` dependency (which requires both) and are imported where
they are used.
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

#: Landing spot for both report families (``data/`` is runtime data).
REPORTS_DIR = Path("data/ops/reports")

#: ``alpha_annual = alpha_monthly * ANNUALIZATION`` (design: 月频×12).
ANNUALIZATION = 12

#: The design's display precision, used by the report tables only.
_ALPHA_DP = 4
_BETA_DP = 3
_T_ALPHA_DP = 2

_NO_VALUE = "—"


# ---------------------------------------------------------------------------
# Alpha-Beta single-factor regression
# ---------------------------------------------------------------------------


def _as_returns(values: Any, name: str) -> pd.Series:
    """One float series, positional. The design regresses ``.values`` on
    ``.values``, so the two inputs are paired by position, not by index."""
    try:
        series = pd.Series(values, dtype=float)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} is not a numeric series: {exc}") from exc
    return series


def alpha_beta_attribution(
    portfolio_returns: Any,
    benchmark_returns: Any,
) -> dict[str, float | None]:
    """Fit ``r_p = α + β·r_b + ε`` and return the design's five fields.

    Parameters
    ----------
    portfolio_returns, benchmark_returns
        Return series of equal length, read monthly (see the module docstring
        for the ×12 convention).  Rows where either side is NaN are dropped.

    Returns
    -------
    dict
        ``alpha_monthly`` (the regression intercept), ``alpha_annual``
        (``alpha_monthly × 12``), ``beta``, ``r_squared`` and ``t_alpha``
        (``intercept / intercept_stderr``, ``None`` when the standard error is
        zero or not finite).  Full precision — the report renderers round.

    Raises
    ------
    ValueError
        Unequal lengths, fewer than two usable observations, or a benchmark
        with no variance (the regression is undefined against a flat line).
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


# ---------------------------------------------------------------------------
# The design's three-line read of a fit (reporting.md §归因报告解读三线)
# ---------------------------------------------------------------------------


def _finite(value: Any) -> float | None:
    """A real number, or ``None`` for None / bool / non-numeric / non-finite."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def interpret_attribution(attribution: Mapping[str, Any]) -> list[str]:
    """The applicable interpretation lines for one Alpha-Beta fit.

    The design's three lines are conditional, so this returns the ones whose
    condition holds — a fit can raise the alpha line and the R² line at once,
    and the two R² branches are mutually exclusive.  A field that is ``None``
    (no computable source) triggers nothing.

    ``alpha_annual > 8%`` is the too-good-to-be-true check and outranks the
    优秀散户 band; R² splits at 0.7 / 0.5.
    """
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


# ---------------------------------------------------------------------------
# Factor contribution — the reuse seam (§63.6)
# ---------------------------------------------------------------------------


def _contribution_table(shares: pd.Series) -> list[str]:
    """``| factor | share |`` rows, biggest absolute share first."""
    ordered = shares.reindex(shares.abs().sort_values(ascending=False).index)
    return [
        f"| {name} | {float(share):.{_ALPHA_DP}f} |" for name, share in ordered.items()
    ]


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


def _day(date: Any) -> str:
    """``YYYY-MM-DD`` for the report filename."""
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
    """Dates when the index carries them; position otherwise."""
    if isinstance(nav.index, pd.DatetimeIndex):
        return nav.index
    if len(nav.index) and isinstance(nav.index[0], str):
        return pd.to_datetime(list(nav.index))
    return range(len(nav))


def _render_nav_png(nav: pd.Series, path: Path) -> None:
    """Write the nav curve png next to the report that links it."""
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
    """Write ``data/ops/reports/daily_{date}.md`` and return its path.

    The report carries the design's four parts: the 策略指标 table, the 系统指标
    table, the 告警 list, and the nav curve link.  Both tables read monitoring's
    ``{"value", "status"}`` entries; nothing is recomputed here.

    ``nav`` is the optional series behind the curve.  When it is given the png
    is rendered beside the report so the link resolves; when it is not, the
    section still carries the link to ``nav_{date}.png`` — the caller that owns
    the series supplies it.
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
    """Write ``attribution_{alpha_type_id}_{quarter}.md`` and return its path.

    Three sections: the Alpha-Beta fit (five fields), the per-factor
    contribution breakdown, and the design's three-line read.  The breakdown is
    :func:`research.factor_combiner.factor_contribution` called verbatim —
    reporting does not own that arithmetic.
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
