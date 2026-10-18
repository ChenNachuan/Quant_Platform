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
standalone orthogonalisation step — :func:`neutralize` takes only the industry
and size columns its signature names.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd
from numpy.linalg import lstsq, matrix_rank

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
    supplies that by ``groupby(date)``.  Degenerate sections are returned
    unchanged: an all-NaN section has no median, and a section whose MAD is
    zero (half or more of the names tied at the median — a sparse factor, a
    limit-move tape) has a band that is a single point, which ``clip`` would
    use to collapse every name onto the median.  There is no dispersion to
    winsorise, so there is nothing to clip.
    """
    values = s.astype("float64")
    med = values.median()
    mad = (values - med).abs().median()
    if pd.isna(med) or pd.isna(mad) or mad == 0:
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
    set today's betas.  A date with fewer than
    :data:`MIN_NEUTRALIZE_ROWS` usable rows is passed through unchanged, and so
    is one whose design matrix is saturated — see the rank guard below.

    A row joins the fit only when everything the regression reads is sound:
    a finite factor, a known industry, and — when the method uses it — a
    finite size.  This is one mask, not one per column, because each hole has
    its own way of ruining the date: a non-finite size reaches ``lstsq`` and
    aborts the whole call (a clean cross-section dies with the dirty one), and
    a missing industry is what ``get_dummies`` renders as an all-zero row, i.e.
    the dropped reference industry — the name is then neutralised against the
    wrong beta.  Rows the fit did not use keep their input value, so neither
    hole can smuggle in a residual from a beta that never saw that name.

    Returns a copy; *df* is never mutated.  The column is hard-wired to
    ``date``, matching :func:`fillna_cross_section`.  A non-unique index is
    fine — writes are positional within the group.
    """
    if method not in {"industry", "industry_size"}:
        raise ValueError(
            f"method must be 'industry' or 'industry_size', got {method!r}"
        )

    result = df.copy()
    # Writes below are addressed by group labels; a caller's duplicate index
    # would make `loc` raise before a single date is processed.  Run the loop
    # on a positional index and hand the original labels back at the end.
    original_index = result.index
    result = result.reset_index(drop=True)

    for _, group in result.groupby("date"):
        y = group[factor_col].astype("float64").to_numpy()
        size = (
            group[size_col].astype("float64").to_numpy()
            if method == "industry_size"
            else None
        )
        valid = np.isfinite(y) & group[industry_col].notna().to_numpy()
        if size is not None:
            valid &= np.isfinite(size)
        if int(valid.sum()) < MIN_NEUTRALIZE_ROWS:
            continue

        parts: list[np.ndarray] = []
        dummies = pd.get_dummies(group[industry_col], drop_first=True)
        if dummies.shape[1] > 0:
            parts.append(dummies.to_numpy(dtype="float64"))
        if size is not None:
            parts.append(size.reshape(-1, 1))
        rhs = np.hstack(parts) if parts else np.empty((len(y), 0))
        x = np.column_stack([np.ones(len(y)), rhs])

        # A saturated design interpolates the cross-section: rank == n means
        # `lstsq` fits every name exactly, the residuals come out ~0 and the
        # date's factor is silently wiped to nothing — the same shape of
        # failure as a zero MAD.  Skip that date; the OLS is unchanged.
        #
        # The boundary is rank == n, not n-1.  At rank n-1 there is still one
        # residual degree of freedom and the OLS removes the industry effect
        # it should; refusing that date would pass the factor through still
        # carrying it — the same silent failure, in the other direction.
        n_fit = int(valid.sum())
        if int(matrix_rank(x[valid])) >= n_fit:
            continue

        beta, _, _, _ = lstsq(x[valid], y[valid], rcond=None)
        fitted = y - x @ beta
        y[valid] = fitted[valid]
        result.loc[group.index, factor_col] = y

    result.index = original_index
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
        # Copy each step along with its `params`: the presets are shared module
        # constants, so a caller who writes `steps[0].params["n"]` must not be
        # editing `PIPELINE_RESEARCH` under the next reader.
        self.steps = [
            PostProcessStep(name=step.name, func=step.func, params=dict(step.params))
            for step in (PIPELINE_RESEARCH if steps is None else steps)
        ]

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
