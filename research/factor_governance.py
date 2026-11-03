"""Factor governance layer (S11-T2).

Admission / exit lines, lifecycle state machine, and quality judgement for
registered factors.  Consumes IC series from ``factor_evaluation`` and metadata
from ``FactorRegistry`` (code-is-registry).  See
``docs/design/research/factor_governance.md`` and §62 of the archive.

Iteration 1 scope (ac-1..ac-3): ``META_FIELDS``, ``list_factor_meta``,
``evaluate_factor``, ``check_factor_health``, ``check_admission``,
``transition_lifecycle``.
"""

from __future__ import annotations

import math
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
    "EXIT_IC_THRESHOLD",
    "EXIT_RATIO",
    "EXIT_T_THRESHOLD",
    "GATE_IC_MIN",
    "GATE_IR_MIN",
    "GATE_T_BASE",
    "HEALTHY_RATIO",
    "META_FIELDS",
    "WARNING_RATIO",
    "WINDOW_12M",
    "WINDOW_36M",
    "WINDOW_6M",
    "check_admission",
    "check_factor_health",
    "evaluate_factor",
    "list_factor_meta",
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
    n_periods = len(ic_series)
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
