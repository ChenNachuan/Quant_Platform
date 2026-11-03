"""Factor governance layer (S11-T2).

Admission / exit lines, lifecycle state machine, and quality judgement for
registered factors.  Consumes IC series from ``factor_evaluation`` and metadata
from ``FactorRegistry`` (code-is-registry).  See
``docs/design/research/factor_governance.md`` and §62 of the archive.

Iteration 1 scope (ac-1..ac-3): ``META_FIELDS``, ``list_factor_meta``,
``evaluate_factor``, ``check_factor_health``, ``check_admission``,
``transition_lifecycle``.

Iteration 2 scope (ac-4..ac-7): ``detect_redundant_factors``,
``redundancy_tier``, ``diagnose_factor``, ``plot_factor_report``.
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
# Constants
# ---------------------------------------------------------------------------

#: Metadata class attributes extracted from every Factor subclass (§62.2).
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

# Admission gate thresholds (D2) — same IC / IR / t as ``check_gate``.
#: Max allowed |avg cross-sectional Spearman| vs existing factors.
CORR_MAX: float = 0.5

# Exit-line thresholds (D1).
#: Rolling-36m t-statistic exit threshold (strict <).
EXIT_T_THRESHOLD: float = 1.5

# Health-cascade ratios (D1).
#: recent_6m > HEALTHY_RATIO * mean_ic → healthy.
HEALTHY_RATIO: float = 0.7
#: recent_6m > WARNING_RATIO * mean_ic → warning.
WARNING_RATIO: float = 0.5
#: recent_12m < EXIT_RATIO * mean_ic → exit (relative decay).
EXIT_RATIO: float = 0.3

# Window lengths in trading days (daily IC).
WINDOW_6M: int = 126
WINDOW_12M: int = 252
WINDOW_36M: int = 756

# Lifecycle legal transitions (D3).
_LIFECYCLE_LEGAL: set[tuple[str, str]] = {
    ("draft", "production"),
    ("production", "deprecated"),
}

# Redundancy tier / diagnostic thresholds (D8, D4, settled item 2 & 3).
#: cs_std_cv above this marks an abnormal cross-section.
CS_STD_CV_ABNORMAL: float = 0.5
#: Default number of quantile groups for plot_factor_report.
N_QUANTILE_GROUPS: int = 5


# ---------------------------------------------------------------------------
# list_factor_meta
# ---------------------------------------------------------------------------


def list_factor_meta() -> list[dict[str, Any]]:
    """Walk FactorRegistry and extract class-level metadata (§62.2).

    Returns one dict per registered factor with keys from :data:`META_FIELDS`.
    A missing attribute becomes ``None``.
    """
    from core.registry import FactorRegistry

    result: list[dict[str, Any]] = []
    for name in FactorRegistry.list_all():
        cls = FactorRegistry.get_class(name)
        result.append({f: getattr(cls, f, None) for f in META_FIELDS})
    return result


# ---------------------------------------------------------------------------
# evaluate_factor (D5)
# ---------------------------------------------------------------------------


def evaluate_factor(
    factor_values: pd.DataFrame,
    returns: pd.DataFrame,
    factor_name: str,
) -> dict[str, Any]:
    """Per-date cross-sectional Spearman IC evaluation (§62.3, D5).

    Parameters
    ----------
    factor_values
        ``[date, ticker, factor_name]`` — raw or processed factor values.
    returns
        ``[date, ticker, fwd_return]`` — forward returns aligned to the same
        date / ticker grid.
    factor_name
        Column name of the factor in *factor_values*.

    Returns
    -------
    dict
        ``{ic_mean, ic_ir, ic_t, n_periods, coverage}`` — ``ic_mean`` at 4dp,
        ``ic_ir`` at 3dp, ``ic_t`` at 2dp, ``coverage`` at 3dp.
        ``ic_ir = 0.0`` when ``std == 0``.  ``t = IR * sqrt(n)``.
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
# check_factor_health (D1 cascade)
# ---------------------------------------------------------------------------


def check_factor_health(ic_history: pd.Series) -> str:
    """Health cascade: absolute exit line first, then relative decay (D1).

    Cascade order::

        1. exit  if ic_mean < 0.03  or  trailing-756-obs t < 1.5
        2. healthy  if recent_6m > 0.7 * mean_ic
        3. warning  if recent_6m > 0.5 * mean_ic
        4. exit  if recent_12m < 0.3 * mean_ic
        5. warning

    A zero-std window does **not** fire the t trigger.  Shorter-than-756
    series use all available observations for the trailing t.
    """
    mean_ic = float(ic_history.mean())

    # Step 1 — absolute exit line.
    if mean_ic < EXIT_IC_THRESHOLD:
        return "exit"

    # Step 1 (cont) — trailing-36m t exit line.
    window = ic_history.tail(WINDOW_36M)
    if len(window) > 0:
        w_mean = float(window.mean())
        w_std = float(window.std())
        if w_std > 0:
            t = w_mean / w_std * math.sqrt(len(window))
            if t < EXIT_T_THRESHOLD:
                return "exit"

    # Steps 2-5 — relative decay cascade.
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
# check_admission (D2)
# ---------------------------------------------------------------------------


def check_admission(metrics: dict, max_corr: float) -> tuple[bool, dict]:
    """Admission gate: per-condition verdict (D2).

    Four conditions, all strict (boundary values fail):

    - ``ic_mean > 0.04``
    - ``ic_ir > 0.5``
    - ``ic_t > 2``
    - ``max_corr < 0.5``

    Parameters
    ----------
    metrics
        Output of :func:`evaluate_factor` — needs ``ic_mean``, ``ic_ir``,
        ``ic_t`` keys.
    max_corr
        Candidate's max ``|avg cross-sectional Spearman|`` against existing
        factors.

    Returns
    -------
    tuple[bool, dict]
        ``(all_passed, detail)`` where *detail* maps condition name to
        ``{value, threshold, passed}``.
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
# transition_lifecycle (D3)
# ---------------------------------------------------------------------------


def transition_lifecycle(current: str, target: str) -> str:
    """Lifecycle state machine (D3).

    Legal transitions are exactly ``draft → production`` and
    ``production → deprecated``.  Every other pair — including self,
    reverse, and ``draft → deprecated`` — raises :class:`ValueError`.
    """
    if (current, target) not in _LIFECYCLE_LEGAL:
        raise ValueError(
            f"illegal lifecycle transition: {current!r} -> {target!r}; "
            f"legal: draft->production, production->deprecated"
        )
    return target


# ---------------------------------------------------------------------------
# detect_redundant_factors (D9) + redundancy_tier (D4)
# ---------------------------------------------------------------------------


def detect_redundant_factors(
    factor_matrix: pd.DataFrame,
    threshold: float = 0.7,
) -> list[tuple[str, str, float]]:
    """Detect redundant factor pairs by per-date cross-sectional Spearman (§62.7).

    Input *factor_matrix* is the wide table from
    :func:`research.factor_storage.load_factor_matrix` — ``[date, ticker, f1,
    f2, ...]``.  Per-date all-NaN rows are dropped before pairwise computation
    (settled item 1).  For each pair of factor columns the per-date
    cross-sectional Spearman correlation is averaged; pairs with
    ``|mean| > threshold`` (strict) are kept.

    Returns ``[(f1, f2, round(avg, 3))]`` sorted by ``-|corr|`` (D4).
    """
    factor_cols = [c for c in factor_matrix.columns if c not in ("date", "ticker")]
    # Settled item 1: drop rows where all factor columns are NaN.
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
    """Map ``|avg cross-sectional Spearman|`` to a tier label (D4).

    ===============  ==========
    |corr| range    tier
    ===============  ==========
    > 0.9            high
    (0.7, 0.9]       possible
    [0.5, 0.7]       warn
    < 0.5            independent
    ===============  ==========
    """
    if abs_corr > 0.9:
        return "high"
    if abs_corr > 0.7:
        return "possible"
    if abs_corr >= 0.5:
        return "warn"
    return "independent"


# ---------------------------------------------------------------------------
# diagnose_factor (D8)
# ---------------------------------------------------------------------------


def diagnose_factor(df: pd.DataFrame, factor_name: str) -> dict[str, Any]:
    """Five-branch anomaly diagnosis first step (§62.9, D8).

    Returns a 12-key dict describing data quality.  ``cs_std_cv > 0.5``
    (settled item 2) marks an abnormal cross-section.

    Routing table:

    ==========================  ==================  =================================
    symptom                     direction           check
    ==========================  ==================  =================================
    all tickers abnormal        data problem        load_bars rows / NaN ratio / dates
    specific ticker abnormal    suspension/ST/exit  listing status
    specific date abnormal      data source issue   depends_hash changed?
    value range abnormal        div-zero/overflow   manual single-date cross-section
    IC sudden change            parameter or decay  §18.3 four indicators
    ==========================  ==================  =================================
    """
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
# plot_factor_report (D7)
# ---------------------------------------------------------------------------


def plot_factor_report(
    factor_values: pd.DataFrame,
    returns: pd.DataFrame,
    factor_name: str,
    n_groups: int = N_QUANTILE_GROUPS,
) -> None:
    """2×2 factor report tearsheet (§62.10, D7).

    Four panels: IC bars with mean red dashed line / cumulative IC /
    cross-section distribution histogram (bins=50) / quantile cumulative
    returns (``n_groups=5``, settled item 3).  Saves to
    ``research/reports/{factor_name}_tearsheet.png`` at ``dpi=150``.
    Lazy matplotlib import; never calls ``plt.show``.
    """
    import matplotlib.pyplot as plt

    merged = factor_values.merge(returns, on=["date", "ticker"])
    ic_series = merged.groupby("date").apply(
        lambda g: g[factor_name].corr(g["fwd_return"], method="spearman"),
        include_groups=False,
    )

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # [1] IC bars + mean red dashed line
    axes[0, 0].bar(range(len(ic_series)), ic_series.values, width=1.0)
    axes[0, 0].axhline(ic_series.mean(), color="red", ls="--")
    axes[0, 0].set_title(f"IC Series (mean={ic_series.mean():.4f})")

    # [2] Cumulative IC
    axes[0, 1].plot(ic_series.cumsum().values)
    axes[0, 1].set_title("Cumulative IC")

    # [3] Cross-section distribution histogram
    axes[1, 0].hist(factor_values[factor_name].dropna(), bins=50)
    axes[1, 0].set_title("Cross-Section Distribution")

    # [4] Quantile cumulative returns (n_groups=5)
    _plot_quantile_returns(axes[1, 1], merged, factor_name, n_groups)
    axes[1, 1].set_title(f"Quantile Returns (n={n_groups})")

    plt.tight_layout()
    out = Path("research/reports") / f"{factor_name}_tearsheet.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150)
    plt.close(fig)


def _plot_quantile_returns(ax, merged: pd.DataFrame, factor_name: str, n: int) -> None:
    """Panel 4 helper: cumulative mean fwd_return per quantile group."""
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
