"""Tests for execution.portfolio_construction — constraint clipping + lot rounding.

Acceptance criteria (QUANT-27, iteration 1):
1. solve() output weights are non-negative and sum <= 1 + 1e-9 (hypothesis).
2. floor_to_lot is an integer multiple of lot_size and never exceeds budget
   (exact asserts; below one lot -> 0; NaN / non-positive refused).
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
from hypothesis import given, settings, strategies as st

from core.types import Account, Position, TargetPortfolio
from execution.portfolio_construction import (
    PortfolioConstraints,
    PortfolioOptimizer,
    floor_to_lot,
)

TRADE_DATE = date(2026, 1, 5)
CREATE_TIME = datetime(2026, 1, 5, 15, 0, 0)


def make_account(total_asset: float, cash: float | None = None) -> Account:
    return Account(
        account_id="acc",
        total_asset=total_asset,
        cash=total_asset if cash is None else cash,
    )


def make_position(ticker: str, quantity: int) -> Position:
    return Position(portfolio_id="p", ticker=ticker, quantity=quantity, avg_cost=1.0)


def solve(
    raw: dict[str, float],
    prices: dict[str, float],
    *,
    current: dict[str, Position] | None = None,
    account: Account | None = None,
    constraints: PortfolioConstraints | None = None,
    industry_map: dict[str, str] | None = None,
) -> TargetPortfolio:
    return PortfolioOptimizer().solve(
        raw,
        current or {},
        account or make_account(1_000_000.0),
        constraints or PortfolioConstraints(lot_size=100, min_trade_value=0.0),
        prices,
        industry_map,
        strategy_id="strat-1",
        trade_date=TRADE_DATE,
        create_time=CREATE_TIME,
    )


class TestFloorToLotExact:
    """AC-2 exact asserts: lot multiple, budget, below-one-lot, refusal."""

    def test_floors_to_lot_multiple(self):
        assert floor_to_lot(250, 100) == 200
        assert floor_to_lot(250.0, 100) == 200
        assert floor_to_lot(12_345, 100) == 12_300
        assert floor_to_lot(25, 10) == 20

    def test_exact_multiple_passthrough(self):
        assert floor_to_lot(100.0, 100) == 100
        assert floor_to_lot(1000, 100) == 1000
        assert floor_to_lot(20.0, 10) == 20

    def test_never_exceeds_budget(self):
        for raw_quantity in (1.0, 99.99, 150, 250.7, 9999.2, 100_000):
            assert floor_to_lot(raw_quantity, 100) <= raw_quantity

    def test_below_one_lot_is_zero(self):
        assert floor_to_lot(99.99, 100) == 0
        assert floor_to_lot(0.5, 100) == 0
        assert floor_to_lot(9, 10) == 0

    def test_result_is_non_negative_lot_multiple(self):
        for raw_quantity in (10.0, 250, 1001.5, 777):
            qty = floor_to_lot(raw_quantity, 100)
            assert qty >= 0
            assert qty % 100 == 0

    @pytest.mark.parametrize("bad", [0.0, -1.0, -250])
    def test_non_positive_refused(self, bad):
        with pytest.raises(ValueError):
            floor_to_lot(bad, 100)

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_nan_and_inf_refused(self, bad):
        with pytest.raises(ValueError):
            floor_to_lot(bad, 100)

    @pytest.mark.parametrize("bad_lot", [0, -100, 100.0, True, "100"])
    def test_invalid_lot_size_refused(self, bad_lot):
        with pytest.raises(ValueError):
            floor_to_lot(250, bad_lot)


class TestFloorToLotProperties:
    @settings(deadline=None, max_examples=100)
    @given(
        raw_quantity=st.floats(
            allow_nan=False,
            allow_infinity=False,
            min_value=0.01,
            max_value=1e9,
        ),
        lot_size=st.integers(min_value=1, max_value=10_000),
    )
    def test_lot_multiple_and_budget(self, raw_quantity, lot_size):
        qty = floor_to_lot(raw_quantity, lot_size)
        assert qty % lot_size == 0
        assert 0 <= qty <= raw_quantity
        if raw_quantity < lot_size:
            assert qty == 0


FLOAT = st.floats(allow_nan=False, allow_infinity=False, min_value=-2.0, max_value=2.0)
POS_FLOAT = st.floats(
    allow_nan=False, allow_infinity=False, min_value=0.01, max_value=1e4
)
TICKERS = [f"t{i}" for i in range(8)]


class TestSolveInvariants:
    """AC-1 hypothesis: no negative weights, total weight <= 1 + 1e-9."""

    @settings(deadline=None, max_examples=80)
    @given(
        raw_vals=st.lists(FLOAT, min_size=0, max_size=8),
        price_vals=st.lists(POS_FLOAT, min_size=1, max_size=8),
        held_qtys=st.lists(
            st.integers(min_value=0, max_value=5000), min_size=0, max_size=4
        ),
        max_weight=st.floats(
            allow_nan=False, allow_infinity=False, min_value=0.01, max_value=1.0
        ),
        min_weight=st.floats(
            allow_nan=False, allow_infinity=False, min_value=0.0, max_value=1.0
        ),
        max_total_weight=st.floats(
            allow_nan=False, allow_infinity=False, min_value=0.01, max_value=1.0
        ),
        min_cash=st.floats(
            allow_nan=False, allow_infinity=False, min_value=0.0, max_value=0.9
        ),
        lot_size=st.integers(min_value=1, max_value=1000),
        min_trade_value=st.floats(
            allow_nan=False, allow_infinity=False, min_value=0.0, max_value=1e6
        ),
        max_turnover=st.one_of(
            st.none(),
            st.floats(
                allow_nan=False, allow_infinity=False, min_value=0.01, max_value=1.0
            ),
        ),
        cash=st.floats(
            allow_nan=False, allow_infinity=False, min_value=1.0, max_value=1e6
        ),
    )
    def test_weights_non_negative_and_sum_le_one(
        self,
        raw_vals,
        price_vals,
        held_qtys,
        max_weight,
        min_weight,
        max_total_weight,
        min_cash,
        lot_size,
        min_trade_value,
        max_turnover,
        cash,
    ):
        min_weight = min(min_weight, max_weight)
        raw = {TICKERS[i]: v for i, v in enumerate(raw_vals)}
        prices = {TICKERS[i]: p for i, p in enumerate(price_vals)}
        for i, ticker in enumerate(raw):
            prices.setdefault(ticker, 1.0 + i)
        held = {TICKERS[i]: q for i, q in enumerate(held_qtys) if q > 0}
        for ticker in held:
            prices.setdefault(ticker, 3.0)
        position_value = sum(q * prices[t] for t, q in held.items())
        account = make_account(position_value + cash, cash=cash)
        current = {t: make_position(t, q) for t, q in held.items()}
        constraints = PortfolioConstraints(
            max_weight=max_weight,
            min_weight=min_weight,
            max_turnover=max_turnover,
            max_total_weight=max_total_weight,
            min_cash=min_cash,
            lot_size=lot_size,
            min_trade_value=min_trade_value,
        )

        book = solve(
            raw, prices, current=current, account=account, constraints=constraints
        )

        delivered = sum(p.target_weight for p in book.positions.values())
        assert delivered <= 1 + 1e-9
        assert all(p.target_weight >= 0 for p in book.positions.values())
        assert all(p.target_quantity > 0 for p in book.positions.values())
        pre_opt = book.pre_opt_weights
        assert pre_opt is not None
        assert sum(pre_opt.values()) <= 1 + 1e-9
        # The delivered book honours the total cap too — not just <= 1. The
        # turnover clip is a convex combination with w_cur, which can carry
        # the sum back through max_total_weight / min_cash; step 3b's
        # re-normalize is what keeps this true.
        total_cap = min(max_total_weight, 1.0 - min_cash)
        assert sum(pre_opt.values()) <= total_cap + 1e-9
        assert all(v >= 0 for v in pre_opt.values())
        assert book.cash_weight >= -1e-9
        assert book.solver_status in ("optimal", "suboptimal", "infeasible")


class TestSolvePipeline:
    def test_box_clip_low_and_high(self):
        book = solve(
            {"A": 0.5, "B": -0.2},
            {"A": 10.0, "B": 10.0},
            constraints=PortfolioConstraints(
                max_weight=0.1, min_weight=0.0, lot_size=100, min_trade_value=0.0
            ),
        )
        assert book.pre_opt_weights == {"A": 0.1, "B": 0.0}
        assert "max_weight" in book.constraints_applied
        assert "min_weight" in book.constraints_applied
        assert set(book.positions) == {"A"}

    def test_total_weight_normalize_scales_down(self):
        book = solve(
            {"A": 0.5, "B": 0.5},
            {"A": 10.0, "B": 10.0},
            constraints=PortfolioConstraints(
                max_weight=1.0, max_total_weight=0.6, lot_size=100, min_trade_value=0.0
            ),
        )
        assert book.pre_opt_weights == {
            "A": pytest.approx(0.3),
            "B": pytest.approx(0.3),
        }
        assert "max_total_weight" in book.constraints_applied

    def test_min_cash_tightens_the_total_cap(self):
        book = solve(
            {"A": 1.0},
            {"A": 10.0},
            constraints=PortfolioConstraints(
                max_weight=1.0,
                max_total_weight=0.95,
                min_cash=0.2,
                lot_size=100,
                min_trade_value=0.0,
            ),
        )
        assert book.pre_opt_weights["A"] == pytest.approx(0.8)
        assert "min_cash" in book.constraints_applied

    def test_clip_then_round_quantity_is_lot_multiple_within_bound(self):
        # raw_qty = 0.1 * 1e6 / 33 = 3030.30... -> 30 lots; weight back-figured.
        book = solve(
            {"A": 0.1},
            {"A": 33.0},
            constraints=PortfolioConstraints(
                max_weight=0.2, lot_size=100, min_trade_value=0.0
            ),
        )
        pos = book.positions["A"]
        assert pos.target_quantity == 3000
        assert pos.target_quantity % 100 == 0
        assert pos.target_notional == pytest.approx(3000 * 33.0)
        assert pos.target_weight == pytest.approx(3000 * 33.0 / 1_000_000.0)
        assert pos.target_weight <= 0.2 + 1e-9
        assert book.pre_opt_weights["A"] == pytest.approx(0.1)
        assert pos.ref_price == 33.0
        assert pos.side == "LONG"

    def test_rounding_never_exceeds_the_clipped_weight(self):
        book = solve(
            {"A": 0.1},
            {"A": 33.0},
            constraints=PortfolioConstraints(
                max_weight=0.1, lot_size=100, min_trade_value=0.0
            ),
        )
        assert book.positions["A"].target_weight <= 0.1 + 1e-12
        assert "lot_size" in book.constraints_applied

    def test_min_weight_is_advisory_after_lot_rounding(self):
        # 0.05 * 1e6 / 33 = 1515.15... shares -> 1500 -> weight 0.0495.
        # The lot is the tradeable unit, so min_weight cannot be guaranteed
        # post-rounding; solve() must deliver without raising. Pinned so the
        # "advisory" wording stays an executable contract.
        book = solve(
            {"A": 0.05},
            {"A": 33.0},
            constraints=PortfolioConstraints(
                min_weight=0.05, max_weight=0.1, lot_size=100, min_trade_value=0.0
            ),
        )
        pos = book.positions["A"]
        assert pos.target_quantity == 1500
        assert pos.target_weight == pytest.approx(1500 * 33.0 / 1_000_000.0)
        assert pos.target_weight < 0.05

    def test_min_trade_value_filter_zeroes_small_positions(self):
        book = solve(
            {"A": 0.1, "B": 0.001},
            {"A": 10.0, "B": 10.0},
            constraints=PortfolioConstraints(
                max_weight=0.5, lot_size=100, min_trade_value=5000.0
            ),
        )
        # A: 10_000 shares @ 10 = 100_000 survives; B: 100 shares @ 10 = 1_000 filtered.
        assert set(book.positions) == {"A"}
        assert "min_trade_value" in book.constraints_applied
        assert book.solver_status == "suboptimal"

    def test_identity_fields_from_keyword_args(self):
        book = solve({"A": 0.1}, {"A": 10.0})
        assert book.strategy_id == "strat-1"
        assert book.trade_date == TRADE_DATE
        assert book.create_time == CREATE_TIME
        # exec_date defaults to trade_date + 1 day when the caller omits it.
        assert book.exec_date == date(2026, 1, 6)
        assert book.num_holdings == len(book.positions)


class TestSolveClipHelpers:
    def test_turnover_clip_interpolates_toward_current_book(self):
        # current A: 1000 @ 10 on nav 20_000 -> w_cur A = 0.5; target flips to B.
        account = make_account(20_000.0, cash=10_000.0)
        current = {"A": make_position("A", 1000)}
        book = solve(
            {"A": 0.0, "B": 0.5},
            {"A": 10.0, "B": 10.0},
            current=current,
            account=account,
            constraints=PortfolioConstraints(
                max_weight=1.0, max_turnover=0.1, lot_size=100, min_trade_value=0.0
            ),
        )
        assert book.pre_opt_weights["A"] == pytest.approx(0.4)
        assert book.pre_opt_weights["B"] == pytest.approx(0.1)
        assert "max_turnover" in book.constraints_applied

    def test_turnover_clip_within_budget_is_noop(self):
        account = make_account(20_000.0, cash=10_000.0)
        current = {"A": make_position("A", 1000)}
        book = solve(
            {"A": 0.45},
            {"A": 10.0},
            current=current,
            account=account,
            constraints=PortfolioConstraints(
                max_weight=1.0, max_turnover=0.5, lot_size=100, min_trade_value=0.0
            ),
        )
        # |0.45 - 0.5| / 2 = 0.025 <= 0.5 — target stands unchanged.
        assert book.pre_opt_weights["A"] == pytest.approx(0.45)
        assert "max_turnover" not in book.constraints_applied

    def test_industry_clip_scales_members_proportionally(self):
        book = solve(
            {"A": 0.3, "B": 0.3, "C": 0.1},
            {"A": 10.0, "B": 10.0, "C": 10.0},
            industry_map={"A": "bank", "B": "bank", "C": "tech"},
            constraints=PortfolioConstraints(
                max_weight=0.5,
                max_industry_weight=0.2,
                lot_size=100,
                min_trade_value=0.0,
            ),
        )
        assert book.pre_opt_weights["A"] == pytest.approx(0.1)
        assert book.pre_opt_weights["B"] == pytest.approx(0.1)
        assert book.pre_opt_weights["C"] == pytest.approx(0.1)
        assert "max_industry_weight" in book.constraints_applied

    def test_drifted_book_cannot_exceed_max_weight_after_turnover(self):
        # current A: 15_000 @ 10 on nav 1e6 -> w_cur A = 0.15 > max_weight=0.1.
        # The turnover clip alone lands A at 0.1367 (convex combo with the
        # drifted book); the upper-bound re-clip must clamp it back to 0.1.
        account = make_account(1_000_000.0, cash=850_000.0)
        current = {"A": make_position("A", 15_000)}
        book = solve(
            {"A": 0.1, "C": 0.1},
            {"A": 10.0, "C": 10.0},
            current=current,
            account=account,
            constraints=PortfolioConstraints(
                max_weight=0.1, max_turnover=0.02, lot_size=100, min_trade_value=0.0
            ),
        )
        assert book.pre_opt_weights["A"] <= 0.1 + 1e-12
        assert book.positions["A"].target_weight <= 0.1 + 1e-12
        assert "max_turnover" in book.constraints_applied
        assert "max_weight" in book.constraints_applied

    def test_drifted_book_cannot_exceed_industry_cap_after_turnover(self):
        # current A: 25_000 @ 10 on nav 1e6 -> w_cur A = 0.25 > industry cap 0.2.
        # The industry clip runs before the turnover clip and would be undone
        # without the re-clip (bank book lands at 0.242 pre-rounding).
        account = make_account(1_000_000.0, cash=750_000.0)
        current = {"A": make_position("A", 25_000)}
        book = solve(
            {"A": 0.15, "B": 0.15},
            {"A": 10.0, "B": 10.0},
            current=current,
            account=account,
            industry_map={"A": "bank", "B": "bank"},
            constraints=PortfolioConstraints(
                max_weight=0.5,
                max_industry_weight=0.2,
                max_turnover=0.02,
                lot_size=100,
                min_trade_value=0.0,
            ),
        )
        pre_total = sum(book.pre_opt_weights.get(t, 0.0) for t in ("A", "B"))
        delivered_total = sum(
            book.positions[t].target_weight for t in ("A", "B") if t in book.positions
        )
        assert pre_total <= 0.2 + 1e-12
        assert delivered_total <= 0.2 + 1e-12
        assert "max_industry_weight" in book.constraints_applied

    def test_drifted_book_cannot_exceed_total_cap_after_turnover(self):
        # current A: 100_000 @ 10 on nav 1e6 -> w_cur = {A: 1.0}. Raw sums to
        # 0.55 < max_total_weight 0.6, so step 2 is a no-op; the turnover clip
        # mixes the target toward the fully invested book and lands the total
        # at ~0.883. The step-3b total normalize must pull it back to 0.6.
        account = make_account(1_000_000.0, cash=0.0)
        current = {"A": make_position("A", 100_000)}
        book = solve(
            {"A": 0.2, "B": 0.2, "C": 0.15},
            {"A": 10.0, "B": 10.0, "C": 10.0},
            current=current,
            account=account,
            constraints=PortfolioConstraints(
                max_weight=1.0,
                max_total_weight=0.6,
                max_turnover=0.15,
                lot_size=100,
                min_trade_value=0.0,
            ),
        )
        pre_total = sum(book.pre_opt_weights.values())
        delivered_total = sum(p.target_weight for p in book.positions.values())
        assert pre_total <= 0.6 + 1e-12
        assert delivered_total <= 0.6 + 1e-12
        assert "max_turnover" in book.constraints_applied
        assert "max_total_weight" in book.constraints_applied

    def test_drifted_book_cannot_exceed_min_cash_cap_after_turnover(self):
        # Same drift, tighter bound: min_cash 0.2 caps the total at 1-0.2=0.8
        # (max_total_weight 0.95 is looser). Raw sums to exactly 0.8, so step
        # 2 stays quiet; the convex combo with w_cur total 1.0 lands at 0.95.
        # min_cash is only ever noted by the normalize, so its presence in
        # constraints_applied pins that step 3b ran it.
        account = make_account(1_000_000.0, cash=0.0)
        current = {"A": make_position("A", 100_000)}
        book = solve(
            {"A": 0.3, "B": 0.3, "C": 0.2},
            {"A": 10.0, "B": 10.0, "C": 10.0},
            current=current,
            account=account,
            constraints=PortfolioConstraints(
                max_weight=1.0,
                max_total_weight=0.95,
                min_cash=0.2,
                max_turnover=0.15,
                lot_size=100,
                min_trade_value=0.0,
            ),
        )
        pre_total = sum(book.pre_opt_weights.values())
        delivered_total = sum(p.target_weight for p in book.positions.values())
        assert pre_total <= 0.8 + 1e-12
        assert delivered_total <= 0.8 + 1e-12
        assert "min_cash" in book.constraints_applied
        assert "max_total_weight" in book.constraints_applied


class TestSolveStatus:
    def test_optimal_when_everything_survives(self):
        book = solve(
            {"A": 0.1},
            {"A": 10.0},
            constraints=PortfolioConstraints(
                max_weight=0.5, lot_size=100, min_trade_value=0.0
            ),
        )
        assert book.solver_status == "optimal"
        assert book.total_weight == pytest.approx(0.1)
        assert book.cash_weight == pytest.approx(0.9)

    def test_suboptimal_when_partially_filtered(self):
        book = solve(
            {"A": 0.1, "B": 0.001},
            {"A": 10.0, "B": 10.0},
            constraints=PortfolioConstraints(
                max_weight=0.5, lot_size=100, min_trade_value=5000.0
            ),
        )
        assert book.solver_status == "suboptimal"

    def test_infeasible_when_nothing_survives(self):
        book = solve(
            {"A": 0.001},
            {"A": 10.0},
            constraints=PortfolioConstraints(
                max_weight=0.5, lot_size=100, min_trade_value=5000.0
            ),
        )
        assert book.solver_status == "infeasible"
        assert book.positions == {}
        assert book.total_weight == 0.0


class TestSolveValidation:
    def test_nan_raw_weight_refused(self):
        with pytest.raises(ValueError):
            solve({"A": float("nan")}, {"A": 10.0})

    def test_missing_price_refused(self):
        with pytest.raises(ValueError):
            solve({"A": 0.1}, {})

    def test_non_positive_price_refused(self):
        with pytest.raises(ValueError):
            solve({"A": 0.1}, {"A": 0.0})
        with pytest.raises(ValueError):
            solve({"A": 0.1}, {"A": float("nan")})

    def test_negative_position_quantity_refused(self):
        with pytest.raises(ValueError):
            solve(
                {"A": 0.1},
                {"A": 10.0},
                current={"B": make_position("B", -100)},
                account=make_account(100_000.0),
            )

    def test_incoherent_current_book_refused(self):
        with pytest.raises(ValueError):
            solve(
                {"A": 0.1},
                {"A": 10.0, "B": 10.0},
                current={"B": make_position("B", 100_000)},  # 1_000_000 on nav 100_000
                account=make_account(100_000.0),
            )

    @pytest.mark.parametrize(
        "field,value",
        [
            ("max_weight", 1.5),
            ("min_weight", -0.1),
            ("max_total_weight", 1.5),
            ("max_total_weight", 0.0),
            ("min_cash", -0.1),
            ("lot_size", 0),
            ("min_trade_value", -1.0),
            ("max_turnover", 0.0),
            ("max_industry_weight", -0.1),
        ],
    )
    def test_invalid_constraints_refused(self, field, value):
        constraints = PortfolioConstraints(lot_size=100, min_trade_value=0.0)
        setattr(constraints, field, value)
        with pytest.raises(ValueError):
            solve({"A": 0.1}, {"A": 10.0}, constraints=constraints)

    @pytest.mark.parametrize("bad_nav", [0.0, -1.0, float("nan")])
    def test_non_positive_nav_refused(self, bad_nav):
        with pytest.raises(ValueError):
            solve(
                {"A": 0.1},
                {"A": 10.0},
                account=make_account(bad_nav),
            )
