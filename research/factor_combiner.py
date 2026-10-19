"""Multi-factor combination and combination quality evaluation.

Turns the N z-scored factor columns that survived single-factor screening into
one ``combined_score`` per ``(date, ticker)``, then judges whether that score
deserves to become an :class:`~research` AlphaSignal (factor_combiner.md §16,
§63).  Three responsibilities and nothing else:

1. :func:`preprocess` — the pre-combination clean-up every factor column goes
   through first: **winsorize (3 MAD) → cross-sectional z-score → neutralize**.
   Each step is ``groupby(date)``; a full-sample statistic is look-ahead.
2. :class:`FactorCombiner` — ``equal_weight`` (the default and the primary
   path) or ``ic_ir_weight`` / ``ic_weight`` from an exogenous IC history.
3. :func:`FactorCombiner.evaluate` / :func:`write_eval_report` — the 8-metric
   table, a :func:`factor_contribution` breakdown, and
   ``data/signals/{name}/eval_report.json`` carrying ``config_snapshot`` as
   ``path@hash``.

Not this module: single-factor evaluation gating (S11 ``factor_evaluation``),
``score → position weights`` (S6-T2 ``alpha_signal.signal_to_weights``), ML
combination (that is ``MLFactor``, S11), Gram-Schmidt / PCA orthogonalisation,
portfolio-level Barra neutralisation, and correlation-clustering de-duplication
(which lives at factor-ingestion time in the governance layer).  Combination
here means *scores*, never *bets*.

The weight window is **expanding** (fixed start, end rolls at ``t-1``), updated
**monthly**, and every weight uses **only IC strictly before** the date it
scores.  Using the current period's IC to set the current period's weight is
look-ahead, and :meth:`FactorCombiner.combine` is built so that it cannot
happen: see ``TestLookahead``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from core.config import config_snapshot
from research.post_processor import neutralize as neutralize_residual
from research.post_processor import standardize_zscore, winsorize_mad

__all__ = [
    "DEFAULT_LOOKBACK",
    "EVAL_METRIC_KEYS",
    "FactorCombiner",
    "WEIGHT_METHODS",
    "ZERO_SIGMA_IR",
    "factor_contribution",
    "preprocess",
    "write_eval_report",
]

#: Minimum IC observations a weighted method needs before its weights are
#: trusted.  Below this :class:`FactorCombiner` falls back to equal weights —
#: an ICIR estimated on a handful of dates is noise, and acting on noise is
#: how a "smart" combiner under-performs the baseline it was meant to beat.
#:
#: The window itself is **expanding** (fixed start, end at ``t-1``), not
#: rolling: the design rejects rolling windows for a 3–5 factor book with
#: short history (factor_combiner.md §63.4).  ``lookback`` is therefore the
#: warm-up gate, not a window length.
DEFAULT_LOOKBACK: int = 60

#: The three methods the design allows.  ``ic_weight`` is kept in the enum
#: because the interface names it, but the primary path is ``equal_weight``
#: and the recommended optional path is ``ic_ir_weight`` (ICIR punishes an
#: unstable factor; a raw IC mean does not).
WEIGHT_METHODS: tuple[str, ...] = ("equal_weight", "ic_weight", "ic_ir_weight")

#: The 8-metric table (§63.6), in report order.  ``factor_contribution`` is
#: the breakdown itself, not a scalar.
EVAL_METRIC_KEYS: tuple[str, ...] = (
    "ic_mean",
    "ic_ir",
    "ann_return",
    "ann_sharpe",
    "max_drawdown",
    "calmar",
    "turnover_monthly",
    "factor_contribution",
)

#: Stand-in for a ``+∞`` ICIR.  A factor whose IC never moves has ``std == 0``;
#: its ``mean / std`` is undefined in floats but its meaning is not — a steady
#: positive IC is the *best* ICIR there is.  Dividing would be the only way to
#: score it, and the sketch's ``else 0.0`` would hand the whole book to the
#: unstable factor instead.  A large finite value wins the normalisation
#: without needing an infinity special case.
ZERO_SIGMA_IR: float = 1e12


# ---------------------------------------------------------------------------
# Pre-combination clean-up
# ---------------------------------------------------------------------------


def preprocess(
    df: pd.DataFrame,
    factor_col: str,
    *,
    date_col: str = "date",
    industry_col: str = "industry",
    size_col: str = "log_market_cap",
    n_mad: float = 3.0,
    neutralize: bool = True,
) -> pd.DataFrame:
    """Return a copy of *df* with *factor_col* ready to combine.

    The pre-combination contract (factor_combiner.md §16.2, §63.4) is three
    steps in this order, each **cross-sectional**:

    1. **winsorize** — MAD clip at ``3 × 1.4826 × MAD``
    2. **z-score** — per-date ``(x - mean) / std``
    3. **neutralize** — per-date OLS residual on industry dummies + log market
       cap.  Mandatory on the stock pool; the ETF pool earns sector/style
       rotation and callers pass ``neutralize=False`` to skip it.

    Every step runs through ``groupby(date_col)`` (step 3 fits one regression
    per date), so one cross-section's statistics never touch another's.  A
    full-sample mean/σ/MAD/regression would let tomorrow's names decide
    today's cleaned value — :func:`research.post_processor` pins the
    counter-example and this module re-asserts it.

    Note the omission relative to :class:`research.post_processor.PostProcessor`:
    there is no ``fillna`` step.  The combine contract is winsorize → z-score
    → neutralize; :meth:`FactorCombiner.combine` renormalises weights over the
    factors a row actually has rather than inventing a filled value.

    Returns a copy; *df* is never mutated.  Rows with a null date belong to no
    group and keep their input value.
    """
    if factor_col not in df.columns:
        raise ValueError(f"factor_col {factor_col!r} not in columns {list(df.columns)}")
    if date_col not in df.columns:
        raise ValueError(f"date_col {date_col!r} not in columns {list(df.columns)}")

    result = df.copy()

    cleaned = result.groupby(date_col)[factor_col].transform(winsorize_mad, n=n_mad)
    result[factor_col] = cleaned.where(result[date_col].notna(), result[factor_col])

    cleaned = result.groupby(date_col)[factor_col].transform(standardize_zscore)
    result[factor_col] = cleaned.where(result[date_col].notna(), result[factor_col])

    if neutralize:
        result = neutralize_residual(
            result,
            factor_col,
            industry_col=industry_col,
            size_col=size_col,
        )
    return result


# ---------------------------------------------------------------------------
# factor_contribution
# ---------------------------------------------------------------------------


def factor_contribution(
    weights: pd.DataFrame,
    factor_returns: pd.DataFrame,
    factor_exposures: pd.DataFrame,
) -> pd.Series:
    """Decompose combined performance into per-factor shares.

    The design's formula (§63.6) is

        ``contribution_i = Σ_t Σ_j w_{j,t} · exposure_{j,i,t} · factor_return_{i,t}``

    and its simplification note is the one that governs here: **use the factor
    value as the exposure and the grouped (quantile) return as the factor
    return.**  That is an approximation — a true exposure is the portfolio's
    holding, not its z-score — and its error is the price of not running a
    full Brinson attribution.  The breakdown is for spotting a single factor
    carrying the book (``max |share| > 0.7`` is fake diversification), which
    the approximation is good enough for.

    Parameters
    ----------
    weights
        ``[date, ticker, weight]`` — the book.  ``signal_to_weights`` (S6-T2)
        produces these; this function never invents them.
    factor_returns
        ``[date, factor_name, factor_return]`` — one return per factor per date.
    factor_exposures
        ``[date, ticker, {factor_name: z}]`` — the preprocessed z-scores that
        went into the combination.  Wide, one column per factor.

    Returns
    -------
    pd.Series
        One share per factor, normalised so the **absolute** shares sum to 1.
        Signs are kept: a factor whose contribution ran against the book shows
        up negative.  ``max |share|`` is the concentration the metric table
        warns about.
    """
    for frame, name, cols in (
        (weights, "weights", ("date", "ticker", "weight")),
        (factor_returns, "factor_returns", ("date", "factor_name", "factor_return")),
    ):
        missing = [c for c in cols if c not in frame.columns]
        if missing:
            raise ValueError(f"{name} is missing columns {missing}")

    exposure_cols = [c for c in factor_exposures.columns if c not in ("date", "ticker")]
    if not exposure_cols:
        raise ValueError("factor_exposures has no factor columns")

    names = factor_returns["factor_name"].astype(str)
    unknown = sorted(set(names.unique()) - set(exposure_cols))
    if unknown:
        raise ValueError(
            f"factor_returns names {unknown} not in factor_exposures columns"
        )
    orphan = sorted(set(exposure_cols) - set(names.unique()))
    if orphan:
        # An exposure with no return is a data gap, not a zero contribution.
        # Dropping it here would inflate the remaining shares and make the
        # "strongest factor > 70%" fake-diversification check read wrong numbers.
        raise ValueError(
            f"factor_exposures columns {orphan} have no factor_returns rows"
        )

    # Long form: one row per (date, ticker, factor).
    wide = factor_exposures.melt(
        id_vars=["date", "ticker"],
        value_vars=exposure_cols,
        var_name="factor_name",
        value_name="exposure",
    )
    merged = wide.merge(
        weights[["date", "ticker", "weight"]],
        on=["date", "ticker"],
        how="inner",
    ).merge(
        factor_returns[["date", "factor_name", "factor_return"]],
        on=["date", "factor_name"],
        how="inner",
    )
    if merged.empty:
        raise ValueError("no overlapping (date, ticker) between weights and exposures")

    raw = (
        (merged["weight"] * merged["exposure"] * merged["factor_return"])
        .groupby(merged["factor_name"])
        .sum()
    )
    total = float(raw.abs().sum())
    if total == 0.0:
        # Nothing moved.  Equal shares beat an arbitrary one and keep the
        # "shares sum to 1" contract observable.
        return pd.Series(
            {c: 1.0 / len(exposure_cols) for c in exposure_cols}, dtype="float64"
        )
    return raw / total


# ---------------------------------------------------------------------------
# eval_report.json
# ---------------------------------------------------------------------------


def write_eval_report(
    name: str,
    metrics: dict,
    config_path: str,
    *,
    out_dir: str = "data/signals",
    period: tuple | None = None,
    walk_forward_folds: list | None = None,
    combiner: dict | None = None,
) -> Path:
    """Write ``{out_dir}/{name}/eval_report.json`` and return its path.

    The report is the run record the validation discipline points at
    (§63.6): walk-forward, test set looked at once, everything on disk.
    ``config_snapshot`` is the platform's ``path@hash`` form produced by
    :func:`core.config.config_snapshot` — a string, not a parameter dict —
    so the exact config that produced these numbers can be re-read from the
    path and re-hashed.  Secrets never enter it (``.env`` is never read).

    ``walk_forward_folds`` is passed through untouched.  Its field structure
    is still an open design item (factor_combiner.md "实施时定稿项"), so the
    writer does not invent one; callers that ran folds put theirs in, callers
    that did not leave it as ``[]``.
    """
    if not name:
        raise ValueError("name must be non-empty")
    missing = [k for k in EVAL_METRIC_KEYS if k not in metrics]
    if missing:
        raise ValueError(f"metrics is missing {missing}")

    out = Path(out_dir) / name
    out.mkdir(parents=True, exist_ok=True)

    if period is not None and len(period) != 2:
        raise ValueError(f"period must be a (start, end) pair, got {period!r}")

    report = {
        "alpha_name": name,
        "config_snapshot": config_snapshot(config_path),
        "period": list(period) if period is not None else None,
        "metrics": {k: _jsonable(metrics[k]) for k in EVAL_METRIC_KEYS},
        "walk_forward_folds": list(walk_forward_folds or []),
        "combiner": dict(combiner or {}),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    path = out / "eval_report.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path


def _jsonable(value):
    """JSON cannot carry numpy scalars or NaN; put them in their Python form."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


# ---------------------------------------------------------------------------
# FactorCombiner
# ---------------------------------------------------------------------------


class FactorCombiner:
    """Combine preprocessed factor z-scores into one ``combined_score``.

    ``equal_weight`` is the primary path and always runs first: any more
    complicated scheme has to beat it out of sample, and if it cannot, the
    complexity is overfitting and gets dropped (§63.2).  The default is
    therefore ``equal_weight`` and a caller who picks a weighted method is
    taking on the burden of proof.

    Weighted methods need ``ic_history`` in **wide** form — ``[date, f1, f2, …]``
    of Rank IC, one column per factor name, sourced from
    ``factor_eval/{name}/ic_series.parquet`` and pivoted by the caller.  The
    combiner never computes IC itself (single source, one definition).
    """

    def __init__(self, method: str = "equal_weight", lookback: int = 60):
        if method not in WEIGHT_METHODS:
            raise ValueError(f"method must be one of {WEIGHT_METHODS}, got {method!r}")
        if not lookback > 0:
            raise ValueError(f"lookback must be positive, got {lookback!r}")
        self.method = method
        self.lookback = int(lookback)

    # -- combination ------------------------------------------------------

    def combine(
        self,
        factor_scores: pd.DataFrame,
        ic_history: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Return ``[date, ticker, combined_score]``.

        ``factor_scores`` is ``[date, ticker] × N factor z-score columns`` —
        already through :func:`preprocess`.  For a weighted method the score
        at date *t* is built from weights computed on IC **strictly before**
        *t*, refreshed only at month boundaries (month-end IC sets next
        month's weights).  A row whose factors are partly missing gets the
        weighted mean of the factors it does have; a row with none keeps NaN.

        Raises ``ValueError`` for a weighted method with no ``ic_history``:
        silently falling back to equal weights there would let a caller think
        they ran ICIR when they did not.
        """
        id_cols = [c for c in ("date", "ticker") if c in factor_scores.columns]
        if "date" not in id_cols:
            raise ValueError("factor_scores must carry a 'date' column")
        factor_cols = [c for c in factor_scores.columns if c not in id_cols]
        if not factor_cols:
            raise ValueError("factor_scores has no factor columns")

        if self.method == "equal_weight":
            w = self._equal_weights(factor_cols)
            score = _weighted_score(factor_scores, factor_cols, w)
            return factor_scores[id_cols].assign(combined_score=score)

        if ic_history is None:
            raise ValueError(
                f"method {self.method!r} requires ic_history; "
                "equal_weight is the method that needs none"
            )
        if "date" not in ic_history.columns:
            raise ValueError("ic_history must carry a 'date' column")
        if not set(factor_cols) & set(ic_history.columns):
            # Every factor column is absent → the estimators would assign 0
            # to all of them and _normalise would fall back to equal weights,
            # silently degrading to the baseline the caller did not ask for.
            raise ValueError(
                f"ic_history shares no factor columns with {factor_cols}; "
                f"its columns are {list(ic_history.columns)}"
            )

        schedule = self._weight_schedule(factor_scores, ic_history, factor_cols)
        score = pd.Series(np.nan, index=factor_scores.index, dtype="float64")
        for idx, w in schedule:
            score.loc[idx] = _weighted_score(factor_scores.loc[idx], factor_cols, w)
        return factor_scores[id_cols].assign(combined_score=score)

    def _weight_schedule(
        self,
        factor_scores: pd.DataFrame,
        ic_history: pd.DataFrame,
        factor_cols: list[str],
    ) -> list[tuple[pd.Index, dict]]:
        """Month-by-month weights: one dict per calendar month of *factor_scores*.

        Month *M* uses IC with ``date < first_score_date_of_M`` — i.e. the
        month-end of *M-1* at the earliest, and never anything at or after the
        first date being scored.  That single ``<`` is the anti-look-ahead
        boundary; widening it to ``<=`` makes ``TestLookahead`` fail.
        """
        dates = pd.to_datetime(factor_scores["date"])
        periods = dates.dt.to_period("M")
        ic_dates = pd.to_datetime(ic_history["date"])

        schedule: list[tuple[pd.Index, dict]] = []
        for _, idx in factor_scores.groupby(periods).groups.items():
            as_of = dates.loc[idx].min()
            hist = ic_history.loc[ic_dates < as_of]
            if len(hist) < self.lookback:
                # Fast-path warm-up.  The per-factor :attr:`lookback` gate in
                # the estimators is what actually enforces the contract — it
                # also covers a ragged history where this row count is high
                # but one factor's IC column is mostly NaN.
                w = self._equal_weights(factor_cols)
            elif self.method == "ic_weight":
                w = self._ic_weights(hist, factor_cols)
            else:
                w = self._ic_ir_weights(hist, factor_cols)
            schedule.append((idx, w))
        return schedule

    # -- weight estimators -------------------------------------------------

    def _equal_weights(self, factor_cols) -> dict:
        """``w_i = 1/n``.  The baseline every weighted method has to beat."""
        n = len(factor_cols)
        if n == 0:
            raise ValueError("factor_cols is empty")
        return {c: 1.0 / n for c in factor_cols}

    def _ic_weights(self, ic_history, factor_cols) -> dict:
        """``w_i = mean(IC_i)`` over the supplied window, negative → 0, sum 1.

        *ic_history* is already cut to the legal window (see
        :meth:`_weight_schedule`); this method does no slicing of its own, so
        it cannot accidentally widen it.  A factor with no IC column, with
        fewer than :attr:`lookback` finite ICs, or with a non-positive mean
        gets 0.  If every factor's IC is non-positive the book would sit at
        zero weight, which is not a portfolio — fall back to equal weights so
        the returned dict always sums to 1.
        """
        w = {}
        for c in factor_cols:
            if c not in ic_history.columns:
                w[c] = 0.0
                continue
            s = ic_history[c].astype("float64").dropna()
            if s.size < self.lookback:
                w[c] = 0.0
                continue
            w[c] = max(float(s.mean()), 0.0)
        return self._normalise(w, factor_cols)

    def _ic_ir_weights(self, ic_history, factor_cols) -> dict:
        """``w_i = mean(IC_i)/std(IC_i)`` over the window, negative → 0, sum 1.

        ICIR, not IC, because a factor whose IC is high but swings wildly is a
        worse bet than a slightly weaker steady one.  Negative ICIR is
        truncated to 0 — A-share cannot short, and a negative weight here
        would be a directional bet the combiner is not licensed to make.

        A series with two or more observations and zero dispersion is the one
        case ``mean/std`` cannot express: its ICIR is ``+∞`` when the mean is
        positive (the best possible) and meaningless when it is not.  It is
        scored as :data:`ZERO_SIGMA_IR` or 0 respectively — see that constant
        for why 0 is wrong for the first case.  Fewer than two observations
        has no dispersion estimate at all and gets 0.

        Both the :attr:`lookback` warm-up and the two-observation floor apply
        **per factor**, not to the wide frame's row count: a ragged IC history
        (a 3–5 factor book with different launch dates) would otherwise let a
        factor with a handful of ICs through on the strength of its siblings'
        history.
        """
        w = {}
        for c in factor_cols:
            if c not in ic_history.columns:
                w[c] = 0.0
                continue
            s = ic_history[c].astype("float64").dropna()
            if s.size < self.lookback:
                w[c] = 0.0
                continue
            if len(s) < 2:
                w[c] = 0.0
                continue
            mu = float(s.mean())
            sigma = float(s.std())
            if sigma > 0:
                ir = mu / sigma
            else:
                ir = ZERO_SIGMA_IR if mu > 0 else 0.0
            w[c] = max(ir, 0.0)
        return self._normalise(w, factor_cols)

    def _normalise(self, w: dict, factor_cols) -> dict:
        """Scale *w* to sum 1, or fall back to equal weights if all are 0."""
        total = sum(w.values())
        if total <= 0:
            return self._equal_weights(factor_cols)
        return {c: w[c] / total for c in factor_cols}

    # -- evaluation --------------------------------------------------------

    def evaluate(
        self,
        combined: pd.DataFrame,
        forward_returns: pd.DataFrame,
        portfolio_returns: pd.DataFrame,
        *,
        weights: pd.DataFrame | None = None,
        factor_exposures: pd.DataFrame | None = None,
        factor_returns: pd.DataFrame | None = None,
    ) -> dict:
        """Return the 8-metric table (§63.6) as a plain dict.

        * ``ic_mean`` / ``ic_ir`` — cross-sectional Spearman of
          ``combined_score`` vs next-period return, per date.
        * ``ann_return`` / ``ann_sharpe`` / ``max_drawdown`` / ``calmar`` —
          from *portfolio_returns* ``[date, ret]``.  This module scores
          factors; turning a score into a book is ``signal_to_weights``
          (S6-T2), which is why the returns come in rather than get built here.
        * ``turnover_monthly`` — ``Σ|Δw|/2`` at month frequency, from *weights*.
        * ``factor_contribution`` — :func:`factor_contribution` shares.

        ``weights`` + ``factor_exposures`` + ``factor_returns`` are all needed
        for the last two; without them the function raises rather than
        returning a table with holes in it.
        """
        for frame, name, cols in (
            (combined, "combined", ("date", "ticker", "combined_score")),
            (forward_returns, "forward_returns", ("date", "ticker", "ret")),
            (portfolio_returns, "portfolio_returns", ("date", "ret")),
        ):
            missing = [c for c in cols if c not in frame.columns]
            if missing:
                raise ValueError(f"{name} is missing columns {missing}")
        if weights is None or factor_exposures is None or factor_returns is None:
            raise ValueError(
                "weights, factor_exposures and factor_returns are all required "
                "for turnover_monthly and factor_contribution"
            )

        ic = _rank_ic_series(combined, forward_returns)
        ic_mean = float(ic.mean()) if not ic.empty else float("nan")
        ic_std = float(ic.std()) if len(ic) >= 2 else float("nan")
        ic_ir = ic_mean / ic_std if np.isfinite(ic_std) and ic_std > 0 else float("nan")

        rets = portfolio_returns.sort_values("date")["ret"].astype("float64")
        ann_return = _ann_return(rets)
        ann_sharpe = _ann_sharpe(rets)
        max_drawdown = _max_drawdown(rets)
        calmar = (
            ann_return / abs(max_drawdown)
            if np.isfinite(max_drawdown) and max_drawdown != 0
            else float("nan")
        )

        contribution = factor_contribution(weights, factor_returns, factor_exposures)

        return {
            "ic_mean": ic_mean,
            "ic_ir": ic_ir,
            "ann_return": ann_return,
            "ann_sharpe": ann_sharpe,
            "max_drawdown": max_drawdown,
            "calmar": calmar,
            "turnover_monthly": _turnover_monthly(weights),
            "factor_contribution": {str(k): float(v) for k, v in contribution.items()},
        }


# ---------------------------------------------------------------------------
# metric primitives
# ---------------------------------------------------------------------------


def _weighted_score(scores: pd.DataFrame, factor_cols: list[str], w: dict) -> pd.Series:
    """``Σ w_i z_i`` over the factors a row actually has, renormalised.

    Renormalising means a name missing one factor gets the weighted mean of
    the rest rather than NaN — the same choice ``Series.mean`` makes, kept
    here because the weights are not uniform under a weighted method.
    """
    values = scores[factor_cols].astype("float64").to_numpy()
    weight_vec = np.array([w[c] for c in factor_cols], dtype="float64")
    finite = np.isfinite(values)
    num = np.where(finite, values * weight_vec, 0.0).sum(axis=1)
    den = np.where(finite, weight_vec, 0.0).sum(axis=1)
    out = np.divide(
        num, den, out=np.full(len(num), np.nan, dtype="float64"), where=den > 0
    )
    return pd.Series(out, index=scores.index, dtype="float64")


def _rank_ic_series(combined: pd.DataFrame, forward_returns: pd.DataFrame) -> pd.Series:
    """Per-date cross-sectional Spearman of score vs next-period return."""
    merged = combined.merge(
        forward_returns[["date", "ticker", "ret"]], on=["date", "ticker"]
    )

    def _one(group: pd.DataFrame) -> float:
        if len(group) < 2:
            return float("nan")
        if group["combined_score"].nunique() < 2 or group["ret"].nunique() < 2:
            return float("nan")
        return float(group["combined_score"].corr(group["ret"], method="spearman"))

    return (
        merged.groupby("date", sort=True)[["combined_score", "ret"]]
        .apply(_one)
        .dropna()
    )


def _ann_return(rets: pd.Series) -> float:
    """``(1 + total) ** (252 / n) - 1`` over the period's daily returns."""
    r = rets.dropna()
    if r.empty:
        return float("nan")
    total = float((1.0 + r).prod() - 1.0)
    return float((1.0 + total) ** (252.0 / len(r)) - 1.0)


def _ann_sharpe(rets: pd.Series) -> float:
    """``mean / std × √252`` over the period's daily returns."""
    r = rets.dropna()
    if len(r) < 2:
        return float("nan")
    sigma = float(r.std())
    if sigma <= 0:
        return float("nan")
    return float(r.mean() / sigma * np.sqrt(252.0))


def _max_drawdown(rets: pd.Series) -> float:
    """Deepest peak-to-trough of the cumulative NAV; negative or 0."""
    r = rets.dropna()
    if r.empty:
        return float("nan")
    nav = (1.0 + r).cumprod()
    return float((nav / nav.cummax() - 1.0).min())


def _turnover_monthly(weights: pd.DataFrame) -> float:
    """Mean over months of ``Σ|Δw|/2`` between consecutive month-end books."""
    if not {"date", "ticker", "weight"} <= set(weights.columns):
        raise ValueError("weights must carry columns ['date', 'ticker', 'weight']")
    frame = weights.copy()
    frame["_t"] = pd.to_datetime(frame["date"])
    frame["_p"] = frame["_t"].dt.to_period("M")

    # Month-end book per month: the last date's weight vector.
    month_ends = []
    for _, group in frame.groupby("_p", sort=True):
        last = group["_t"].max()
        book = (
            group.loc[group["_t"] == last, ["ticker", "weight"]]
            .set_index("ticker")["weight"]
            .astype("float64")
        )
        month_ends.append(book)

    if len(month_ends) < 2:
        return float("nan")

    turns = []
    for prev, cur in zip(month_ends, month_ends[1:]):
        aligned = pd.concat([prev, cur], axis=1).fillna(0.0)
        aligned.columns = ["prev", "cur"]
        turns.append(float((aligned["cur"] - aligned["prev"]).abs().sum() / 2.0))
    return float(np.mean(turns))
