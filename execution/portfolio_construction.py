"""Constraint-satisfaction layer: raw weights -> clipped -> lot-rounded book.

Design authority: ``docs/design/execution/portfolio_construction.md`` (§17, §34).
Weight allocation (equal weight / score weighting / risk parity) happens in the
strategy; this module never optimizes — it clips and rounds.  The dataclass
bodies live in :mod:`core.types` (gap #1 ruling A); the solver stays in the
execution domain.

Pipeline order (pinned — restated in the PR description): constraint clip ->
total-weight normalize -> turnover clip -> pre_opt snapshot -> floor_to_lot ->
min_trade_value filter.  Clip first, round last: lot rounding floors against
the budget, so it can only reduce a clipped weight and never push one back
through a bound.  Round-then-clip would clip in weight space and leave
non-lot quantities — violating the execution unit.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from core.types import Account, Position, TargetPortfolio, TargetPosition


@dataclass
class PortfolioConstraints:
    """Constraint set — the sole authority on portfolio bounds (§34).

    Signals carry scores, never constraints (gap #1 ruling A).  Mutable config
    object per design.  ``min_cash`` bounds the total weight together with
    ``max_total_weight`` — the tighter of the two caps is what binds.
    """

    max_weight: float = 0.10
    min_weight: float = 0.0
    max_turnover: float | None = None
    max_industry_weight: float | None = None
    max_total_weight: float = 0.95
    min_cash: float = 0.0
    lot_size: int = 100
    min_trade_value: float = 5000.0


def floor_to_lot(raw_quantity: float, lot_size: int = 100) -> int:
    """Floor *raw_quantity* to a multiple of *lot_size* — the budget-safe direction.

    Below one lot the result is 0: an order smaller than one lot is not
    tradeable.  NaN and non-positive quantities are refused (``not (x > 0)``,
    NaN-safe like ``FeeModel.compute``) — a NaN would floor into a garbage lot
    count and a negative quantity is not a long-only target.  The result is a
    non-negative multiple of *lot_size* and never exceeds *raw_quantity*.
    """
    if not isinstance(lot_size, int) or isinstance(lot_size, bool) or lot_size <= 0:
        raise ValueError(f"lot_size must be a positive int, got {lot_size!r}")
    if not (raw_quantity > 0) or not math.isfinite(raw_quantity):
        raise ValueError(
            f"raw_quantity must be strictly positive and finite (NaN refused), "
            f"got {raw_quantity!r}"
        )
    lots = math.floor(raw_quantity / lot_size)
    qty = lots * lot_size
    if qty > raw_quantity:  # float division can round a hair above the true quotient
        qty -= lot_size
    return int(qty)


class PortfolioOptimizer:
    """Constraint-satisfaction solver — clips and rounds, never optimizes.

    ``solve`` is the one entry point: it takes unconstrained raw weights and
    returns a :class:`~core.types.TargetPortfolio` whose quantities are whole
    lots.  The identity fields (``strategy_id`` / ``trade_date`` / ...) come
    from the signal's meta and are keyword-only — the design sketch elides
    them at ``TargetPortfolio(...)``.
    """

    def solve(
        self,
        raw_weights: dict[str, float],
        current_positions: dict[str, Position],
        account: Account,
        constraints: PortfolioConstraints,
        prices: dict[str, float],
        industry_map: dict[str, str] | None = None,
        *,
        strategy_id: str,
        trade_date: date,
        exec_date: date | None = None,
        create_time: datetime | None = None,
    ) -> TargetPortfolio:
        """Clip *raw_weights* to *constraints*, round to lots, build the book.

        Order (pinned): box/industry clip -> total-weight normalize -> turnover
        clip -> ``pre_opt_weights`` snapshot -> ``floor_to_lot`` ->
        ``min_trade_value`` filter.  The returned ``positions`` hold only
        positive holdings — absence of a ticker is the exit signal for the
        order diff downstream.

        Raises ``ValueError`` for NaN/inf raw weights, missing or non-positive
        prices, negative position quantities, an incoherent current book
        (weight sum > 1), or invalid constraint fields.  Refusing is what
        makes the output invariants (no negative weight, total <= 1) true of
        every book this function can return.
        """
        _validate_constraints(constraints)
        if not math.isfinite(account.total_asset) or not (account.total_asset > 0):
            raise ValueError(
                f"account.total_asset must be strictly positive and finite "
                f"(NaN refused), got {account.total_asset!r}"
            )
        needed = set(raw_weights)
        for ticker, pos in current_positions.items():
            if pos.quantity != 0:
                needed.add(ticker)
        for ticker in needed:
            _require_price(prices, ticker)

        applied: list[str] = []

        def note(name: str) -> None:
            if name not in applied:
                applied.append(name)

        # Step 1 — per-ticker box clip (raw candidates only; exits stay at 0).
        w: dict[str, float] = {}
        for ticker, raw in raw_weights.items():
            if not math.isfinite(raw):
                raise ValueError(
                    f"raw weight for {ticker!r} must be finite (NaN refused), "
                    f"got {raw!r}"
                )
            if raw < constraints.min_weight:
                note("min_weight")
            if raw > constraints.max_weight:
                note("max_weight")
            w[ticker] = min(max(raw, constraints.min_weight), constraints.max_weight)

        # Step 1b — industry clip (proportional scale-down per industry).
        if constraints.max_industry_weight is not None and industry_map:
            if self._clip_industry(w, industry_map, constraints.max_industry_weight):
                note("max_industry_weight")

        # Step 2 — total-weight normalize.  Cap honours min_cash too: the
        # tighter of max_total_weight and the cash floor is what binds.
        total_cap = min(constraints.max_total_weight, 1.0 - constraints.min_cash)
        total = sum(w.values())
        if total > total_cap:
            scale = total_cap / total if total > 0 else 0.0
            w = {t: v * scale for t, v in w.items()}
            note("max_total_weight")
            if 1.0 - constraints.min_cash < constraints.max_total_weight:
                note("min_cash")

        # Step 3 — turnover clip (convex interpolation toward the current book).
        w_cur = self._current_weights(current_positions, account, prices)
        if constraints.max_turnover is not None:
            if self._clip_turnover(w, w_cur, constraints.max_turnover):
                note("max_turnover")

        # Step 4 — pre-rounding snapshot.
        pre_opt = dict(w)

        # Step 5-6 — lot rounding, then the min_trade_value filter.
        positions: dict[str, TargetPosition] = {}
        targeted = 0
        for ticker, weight in w.items():
            if not (weight > 0):
                continue
            targeted += 1
            px = prices[ticker]
            raw_qty = weight * account.total_asset / px
            qty = floor_to_lot(raw_qty, constraints.lot_size)
            if qty < raw_qty:
                note("lot_size")
            if qty > 0 and qty * px < constraints.min_trade_value:
                qty = 0
                note("min_trade_value")
            if qty > 0:
                positions[ticker] = TargetPosition(
                    ticker=ticker,
                    target_weight=qty * px / account.total_asset,
                    target_quantity=qty,
                    target_notional=qty * px,
                    ref_price=px,
                    side="LONG",
                )

        if targeted == 0 or len(positions) == targeted:
            solver_status = "optimal"
        elif not positions:
            solver_status = "infeasible"
        else:
            solver_status = "suboptimal"

        total_weight = sum(p.target_weight for p in positions.values())
        estimated_turnover = sum(
            abs((positions[t].target_weight if t in positions else 0.0) - w_cur.get(t, 0.0))
            for t in set(positions) | set(w_cur)
        ) / 2.0

        return TargetPortfolio(
            strategy_id=strategy_id,
            trade_date=trade_date,
            exec_date=exec_date if exec_date is not None else trade_date + timedelta(days=1),
            create_time=create_time if create_time is not None else datetime.now(),
            positions=positions,
            total_weight=total_weight,
            num_holdings=len(positions),
            cash_weight=1.0 - total_weight,
            estimated_turnover=estimated_turnover,
            solver_status=solver_status,
            constraints_applied=applied,
            pre_opt_weights=pre_opt,
        )

    @staticmethod
    def _clip_industry(
        w: dict[str, float], industry_map: dict[str, str], cap: float
    ) -> bool:
        """Scale each over-weight industry's members proportionally to *cap*.

        Tickers absent from *industry_map* are left alone — the map is
        advisory metadata, not a partition requirement.
        """
        by_industry: dict[str, list[str]] = {}
        for ticker in w:
            industry = industry_map.get(ticker)
            if industry is not None:
                by_industry.setdefault(industry, []).append(ticker)
        changed = False
        for members in by_industry.values():
            total = sum(w[t] for t in members)
            if total > cap and total > 0:
                scale = cap / total
                for t in members:
                    w[t] *= scale
                changed = True
        return changed

    @staticmethod
    def _clip_turnover(
        w: dict[str, float], w_cur: dict[str, float], max_turnover: float
    ) -> bool:
        """Interpolate the target toward the current book until turnover fits.

        ``turnover = sum(|Δw|)/2`` (design §34).  Scaled deltas are convex
        combinations of the two books, so bounds on both books carry over.
        Partial exits land in *w* when the budget cannot exit in one step.
        """
        tickers = set(w) | set(w_cur)
        delta = {t: w.get(t, 0.0) - w_cur.get(t, 0.0) for t in tickers}
        turnover = sum(abs(d) for d in delta.values()) / 2.0
        if not (turnover > max_turnover):
            return False
        scale = max_turnover / turnover
        for t in tickers:
            w[t] = w_cur.get(t, 0.0) + delta[t] * scale
        return True

    @staticmethod
    def _current_weights(
        current_positions: dict[str, Position],
        account: Account,
        prices: dict[str, float],
    ) -> dict[str, float]:
        """Value the current book at *prices* as fractions of ``total_asset``."""
        w_cur: dict[str, float] = {}
        for ticker, pos in current_positions.items():
            if not isinstance(pos.quantity, int) or pos.quantity < 0:
                raise ValueError(
                    f"position quantity for {ticker!r} must be a non-negative int, "
                    f"got {pos.quantity!r}"
                )
            if pos.quantity == 0:
                continue
            px = _require_price(prices, ticker)
            w_cur[ticker] = pos.quantity * px / account.total_asset
        total = sum(w_cur.values())
        if total > 1.0 + 1e-9:
            raise ValueError(
                f"current book weights sum to {total!r} > 1 — incoherent account "
                f"state for a long-only solver"
            )
        return w_cur


def _require_price(prices: dict[str, float], ticker: str) -> float:
    px = prices.get(ticker)
    if not isinstance(px, (int, float)) or not (px > 0) or not math.isfinite(px):
        raise ValueError(
            f"price for {ticker!r} must be strictly positive and finite "
            f"(NaN refused), got {px!r}"
        )
    return float(px)


def _validate_constraints(constraints: PortfolioConstraints) -> None:
    c = constraints
    if not math.isfinite(c.min_weight) or c.min_weight < 0:
        raise ValueError(f"min_weight must be finite and >= 0, got {c.min_weight!r}")
    if not math.isfinite(c.max_weight) or not (c.min_weight <= c.max_weight <= 1.0):
        raise ValueError(
            f"max_weight must satisfy min_weight <= max_weight <= 1, "
            f"got {c.max_weight!r}"
        )
    if not math.isfinite(c.max_total_weight) or not (0 < c.max_total_weight <= 1.0):
        raise ValueError(
            f"max_total_weight must be in (0, 1] — V1 is long-only, no leverage, "
            f"got {c.max_total_weight!r}"
        )
    if not math.isfinite(c.min_cash) or not (0 <= c.min_cash < 1.0):
        raise ValueError(f"min_cash must be in [0, 1), got {c.min_cash!r}")
    if not isinstance(c.lot_size, int) or isinstance(c.lot_size, bool) or c.lot_size <= 0:
        raise ValueError(f"lot_size must be a positive int, got {c.lot_size!r}")
    if not math.isfinite(c.min_trade_value) or c.min_trade_value < 0:
        raise ValueError(
            f"min_trade_value must be finite and >= 0, got {c.min_trade_value!r}"
        )
    if c.max_turnover is not None and (
        not math.isfinite(c.max_turnover) or not (c.max_turnover > 0)
    ):
        raise ValueError(
            f"max_turnover must be None or strictly positive (NaN refused), "
            f"got {c.max_turnover!r}"
        )
    if c.max_industry_weight is not None and (
        not math.isfinite(c.max_industry_weight) or not (c.max_industry_weight > 0)
    ):
        raise ValueError(
            f"max_industry_weight must be None or strictly positive (NaN refused), "
            f"got {c.max_industry_weight!r}"
        )
