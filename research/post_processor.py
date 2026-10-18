"""Factor post-processing — winsorize → fillna → standardize → neutralize.

Consumption-time clean-up of the **raw** values ``Factor.compute()`` produces
and :mod:`research.factor_storage` stores (post_processor.md §60).  The four
stages run in exactly this order:

1. **winsorize** — MAD clip, bounds ``median ± n × 1.4826 × MAD``
2. **fillna** — cross-section median
3. **standardize** — z-score, or rank for badly skewed factors
4. **neutralize** *(optional, separate call)* — per-date OLS residual on
   industry dummies + log market cap

The order is not negotiable: extreme values distort the mean/σ a z-score
needs, and standardizing before filling would compute those statistics from
the un-filled rows only.

Every step is **cross-sectional** — :meth:`PostProcessor.transform` runs each
one through ``groupby(date)``, and :func:`neutralize` fits one regression per
date.  A full-sample mean/σ/MAD/regression is look-ahead bias: tomorrow's
cross-section would decide today's cleaned value.  The tests pin this with
counter-examples, not with prose.

Store raw, clean on read — swapping ``n`` or the standardize method never
touches the parquet tree.

Not this module: factor computation (:mod:`research.factor_interface`),
persistence (:mod:`research.factor_storage`), evaluation / IC (S11), and a
standalone orthogonalisation step (neutralising against extra factors is
just more regressors inside :func:`neutralize`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd
from numpy.linalg import lstsq

__all__ = [
    "MAD_SCALE",
    "PIPELINE_ETF",
    "PIPELINE_RANK",
    "PIPELINE_RESEARCH",
    "PIPELINE_STOCK_EVAL",
    "PostProcessStep",
    "PostProcessor",
    "fillna_cross_section",
    "fillna_median",
    "neutralize",
    "standardize_rank",
    "standardize_zscore",
    "winsorize_mad",
]

#: Normal-consistency constant: ``MAD × 1.4826`` estimates σ for Gaussian data,
#: so the ``n = 3.0`` default is a 3σ-equivalent clip that survives outliers.
MAD_SCALE: float = 1.4826

#: A cross-section needs this many non-NaN values before neutralize will fit.
#: Below it the regression has no degrees of freedom worth trusting, and the
#: factor is passed through unchanged.
MIN_NEUTRALIZE_ROWS: int = 10


# ---------------------------------------------------------------------------
# Series steps — one cross-section in, one cross-section out
# ---------------------------------------------------------------------------


def winsorize_mad(s: pd.Series, n: float = 3.0) -> pd.Series:
    """Clip *s* to ``median ± n × 1.4826 × MAD``.

    The median/MAD pair is computed from the same section it clips, and both
    are robust to the very outliers being clipped — unlike a 3σ rule, whose
    mean/σ come from the contaminated sample and therefore move *towards* the
    outlier it is trying to tame.

    Operates on **one cross-section**; :meth:`PostProcessor.transform`
    supplies that by ``groupby(date)``.  An all-NaN section has no median and
    is returned unchanged.
    """
    values = s.astype("float64")
    med = values.median()
    mad = (values - med).abs().median()
    if pd.isna(med) or pd.isna(mad):
        return values
    half = n * MAD_SCALE * mad
    return values.clip(med - half, med + half)


def fillna_median(s: pd.Series) -> pd.Series:
    """Fill NaN with the median of *s*.

    The Series-level primitive behind :func:`fillna_cross_section` and the
    ``fillna`` pipeline step — :class:`PostProcessStep.func` is
    ``Series → Series``, so the step cannot take the DataFrame form.  The
    median is the robust centre: a filled value is meant to be "typical for
    this date", not "pulled by the tail".  A section that is entirely NaN has
    no median and is returned unchanged.
    """
    values = s.astype("float64")
    med = values.median()
    if pd.isna(med):
        return values
    return values.fillna(med)


def standardize_zscore(s: pd.Series) -> pd.Series:
    """Return ``(x - mean) / std`` over one cross-section.

    ``std`` is the sample standard deviation (pandas ``ddof=1``), so a
    non-constant section comes back with mean 0 and std 1 exactly.  A constant
    or single-row section has no dispersion and becomes zeros — dividing by
    a zero σ would turn a flat factor into ``inf`` rather than "no signal".
    """
    values = s.astype("float64")
    mu = values.mean()
    sigma = values.std()
    if pd.isna(sigma) or sigma == 0:
        return values - mu
    return (values - mu) / sigma


def standardize_rank(s: pd.Series) -> pd.Series:
    """Rank one cross-section onto ``(0, 1]`` (``pct=True``).

    The skewed-distribution alternative to :func:`standardize_zscore`: ranks
    are bounded and monotone, so a heavy tail cannot dominate the scale.  NaN
    stays NaN — a missing value has no rank.
    """
    return s.rank(pct=True)


# ---------------------------------------------------------------------------
# DataFrame steps — need columns beyond the factor itself
# ---------------------------------------------------------------------------


def fillna_cross_section(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Fill NaN in *col* with **each date's own** median.

    A convenience wrapper for callers not going through
    :class:`PostProcessor`.  Returns a copy; *df* is never mutated.  The
    column is hard-wired to ``date`` — the same one
    :func:`research.factor_storage` persists and the rest of the chain joins
    on.
    """
    out = df.copy()
    out[col] = out.groupby("date")[col].transform(fillna_median)
    return out


def neutralize(
    df: pd.DataFrame,
    factor_col: str,
    industry_col: str = "industry",
    size_col: str = "log_market_cap",
    method: str = "industry_size",
) -> pd.DataFrame:
    """Replace *factor_col* with the residual of a per-date cross-sectional OLS.

    ``factor ~ 1 + industry dummies + log market cap`` (``"industry_size"``,
    the default) or ``factor ~ 1 + industry dummies`` (``"industry"``).  One
    regression **per date**: a pooled fit would let other dates' cross-sections
    set today's betas.  Rows with a NaN factor are excluded from the fit and
    stay NaN in the residual.  A date with fewer than
    :data:`MIN_NEUTRALIZE_ROWS` valid values is passed through unchanged.

    Returns a copy; *df* is never mutated.  The column is hard-wired to
    ``date``, matching :func:`fillna_cross_section`.
    """
    if method not in {"industry", "industry_size"}:
        raise ValueError(
            f"method must be 'industry' or 'industry_size', got {method!r}"
        )

    result = df.copy()
    for _, group in df.groupby("date"):
        y = group[factor_col].astype("float64").to_numpy()
        valid = ~np.isnan(y)
        if int(valid.sum()) < MIN_NEUTRALIZE_ROWS:
            continue

        parts: list[np.ndarray] = []
        dummies = pd.get_dummies(group[industry_col], drop_first=True)
        if dummies.shape[1] > 0:
            parts.append(dummies.to_numpy(dtype="float64"))
        if method == "industry_size":
            parts.append(group[size_col].to_numpy(dtype="float64").reshape(-1, 1))
        rhs = np.hstack(parts) if parts else np.empty((len(y), 0))
        x = np.column_stack([np.ones(len(y)), rhs])

        beta, _, _, _ = lstsq(x[valid], y[valid], rcond=None)
        result.loc[group.index, factor_col] = y - x @ beta
    return result


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


@dataclass
class PostProcessStep:
    """One pipeline stage: a ``Series → Series`` op applied per date group."""

    name: str
    func: Callable[[pd.Series], pd.Series]
    params: dict = field(default_factory=dict)


class PostProcessor:
    """Configurable clean-up pipeline, applied cross-section by cross-section.

    Each step runs through ``groupby(date_col).transform``, so a step can only
    ever see its own date's rows.  That is the whole look-ahead defence: the
    statistics a step computes are today's cross-section, never the sample's.
    """

    def __init__(self, steps: list[PostProcessStep] | None = None):
        self.steps = list(PIPELINE_RESEARCH if steps is None else steps)

    def transform(
        self, df: pd.DataFrame, factor_col: str, date_col: str = "date"
    ) -> pd.DataFrame:
        """Return a copy of *df* with *factor_col* cleaned, date by date."""
        result = df.copy()
        for step in self.steps:
            result[factor_col] = result.groupby(date_col)[factor_col].transform(
                step.func, **step.params
            )
        return result


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------

#: Research iteration — the three Series steps, no neutralise.  Quick screening
#: of a raw factor before anyone claims an IC.
PIPELINE_RESEARCH: tuple[PostProcessStep, ...] = (
    PostProcessStep("winsorize", winsorize_mad),
    PostProcessStep("fillna", fillna_median),
    PostProcessStep("standardize", standardize_zscore),
)

#: Stock evaluation — the same three steps, then **call :func:`neutralize`
#: yourself**.  Neutralising needs the ``industry`` / ``log_market_cap``
#: columns and so cannot be a Series step; on the 个股池 it is mandatory,
#: because an un-neutralised IC is mostly sector and size in disguise.
PIPELINE_STOCK_EVAL: tuple[PostProcessStep, ...] = (
    PostProcessStep("winsorize", winsorize_mad),
    PostProcessStep("fillna", fillna_median),
    PostProcessStep("standardize", standardize_zscore),
)

#: ETF pool — the same three steps, and deliberately **no** neutralise: an ETF
#: strategy earns sector/style rotation, which is exactly what neutralising
#: would regress out.
PIPELINE_ETF: tuple[PostProcessStep, ...] = (
    PostProcessStep("winsorize", winsorize_mad),
    PostProcessStep("fillna", fillna_median),
    PostProcessStep("standardize", standardize_zscore),
)

#: Skewed factors — rank replaces z-score as the standardize step; winsorize
#: and fillna are unchanged.
PIPELINE_RANK: tuple[PostProcessStep, ...] = (
    PostProcessStep("winsorize", winsorize_mad),
    PostProcessStep("fillna", fillna_median),
    PostProcessStep("standardize", standardize_rank),
)
