"""Golden-sample step 4: fee reconciliation against the FeeModel hand-calc (S8-T2).

Runs the S6->S7 main chain twice over the golden fixture — a test-side
zero-fee ``FeeModel`` double and the landed :class:`execution.fee_model.
FeeModel` — and checks the fee the chain actually charges against a
first-principles hand-calc of the published A-share schedule (ac-2).  The
0.1 % slippage is billed once, in FeeModel, on a raw fill price (ac-5,
QUANT-26).

Seams (kb_snapshot, plan 2026-10-25) — implement exactly:

* P5 same chain twice: the FeeModel is injected on the SimBroker instance.
  ``execution/fee_model.py`` takes no switch and is untouched (P10).
* P7 QUANT-26 no double count: the recorded ``fill_price`` is the raw
  lookup price; the fee carries exactly one slippage term.
* P8 the per-fill hand-calc covers the cases the golden ETF universe cannot
  reach — ``COMMISSION_MIN`` and a non-exempt SELL — since all five golden
  tickers are listed funds and waive stamp tax.
* P10 this file and ``test_step3_simbroker.py`` only; ``execution/`` and
  ``research/`` are untouched.

The step-4 twins target **fixed share counts**, not 1/5 of the running NAV.
``generate_orders`` sizes as ``w * nav / price``, so a nav-derived target
forks the two order paths the moment fees move the nav — measured at a 3.7 %
gap between the NAV delta and the fee sum.  Expressing the target as
``(shares + 1) * price / nav`` cancels the nav inside that product, both
twins raise identical orders, and the before/after delta is the fee charge
exactly.  The step-3 metrics gate keeps its own 1/5-of-nav clock; this file
is about fees.
"""

from __future__ import annotations

import inspect
from datetime import date

import numpy as np
import pandas as pd
import pytest

from core.asset_rules import is_stamp_tax_exempt_prefix
from execution.broker import FILL_BASIS_T1_OPEN, SimBroker
from execution.fee_model import FeeModel
from execution.market_data import get_latest_price
from execution.portfolio_construction import floor_to_lot
from research.cross_sectional_backtest import CrossSectionalBacktest
from tests.golden.test_step3_simbroker import (
    DECISION_DATES,
    INITIAL_CASH,
    RISK_CONFIG,
    UNIVERSE,
    NavRecordingSimBroker,
    OracleWeightClock,
    ZeroFeeModel,
    chain_metrics,
    load_prices,
    long_bars,
    run_dates,
)

pytestmark = pytest.mark.golden

# A 100 %-invested book cannot pay its own fees: the fee on the buys exceeds
# the lot-dust cash left after lot rounding and BUY settlements fail (probed:
# need 2.0025e9, have 1.9899e9).  Both twins target the same scaled notional,
# so the before/after delta stays attributable to fees alone.
SLEEVE = 0.95
NOTIONAL_PER_NAME = SLEEVE * INITIAL_CASH / len(UNIVERSE)

# First-principles fee schedule (docs/design/execution/fee_model.md, 教程
# §3.2).  Literals rather than ``FeeModel`` attributes: the hand-calc is the
# authority and ``test_constants_match_the_fee_model`` is what links the two.
COMMISSION_RATE = 0.00025
COMMISSION_MIN = 5.0
STAMP_TAX_RATE = 0.0005
TRANSFER_FEE_RATE = 0.00001
SLIPPAGE_RATE = 0.001

# P8 — the cases the golden ETF universe cannot reach.  Every golden ticker is
# a listed fund and waives stamp tax, so a non-exempt SELL has to be probed on
# a stock prefix, and ``COMMISSION_MIN`` needs a value small enough that the
# rate falls under the floor.
SYNTHETIC_CASES = (
    # (side, price, shares, ticker) — what it exercises
    ("BUY", 10.0, 100, "510300.SH"),  # commission floor binds; no stamp
    ("SELL", 10.0, 100, "510300.SH"),  # floor binds; stamp waived (listed fund)
    ("SELL", 10.0, 100, "600519.SH"),  # floor binds; stamp charged (stock)
    ("BUY", 141.5, 100_000, "600519.SH"),  # floor does not bind
    ("SELL", 141.5, 100_000, "600519.SH"),  # full schedule, stamp charged
    ("SELL", 141.5, 100_000, "510300.SH"),  # full schedule, stamp waived
)


# ---------------------------------------------------------------------------
# first-principles hand-calc
# ---------------------------------------------------------------------------


def hand_calc_fee(
    side: str, price: float, shares: int, exempt: bool
) -> dict[str, float]:
    """One fill's fee from the published schedule.

    Four additive parts: commission (rate with a per-fill floor), stamp tax
    (SELL only, waived for exempt instruments), transfer fee (both sides) and
    slippage (always against us, one term).  *exempt* is an input, not a
    lookup — the exemption rule lives in ``core.asset_rules`` and is checked
    separately, so this stays a derivation someone could redo on paper.
    """
    value = price * shares
    commission = max(value * COMMISSION_RATE, COMMISSION_MIN)
    stamp = value * STAMP_TAX_RATE if side == "SELL" and not exempt else 0.0
    transfer = value * TRANSFER_FEE_RATE
    slippage = value * SLIPPAGE_RATE
    return {
        "commission": commission,
        "stamp": stamp,
        "transfer": transfer,
        "slippage": slippage,
        "total": commission + stamp + transfer + slippage,
    }


# ---------------------------------------------------------------------------
# step-4 doubles — the fee seam is the instance attribute (P5)
# ---------------------------------------------------------------------------


class RecordingFeeModel(FeeModel):
    """The landed FeeModel, recording the arguments of every ``compute`` call."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def compute(self, side: str, price: float, shares: int, ticker: str) -> float:
        fee = super().compute(side, price, shares, ticker)
        self.calls.append(
            {
                "side": side,
                "price": price,
                "shares": shares,
                "ticker": ticker,
                "fee": fee,
            }
        )
        return fee


class FeeTwinBroker(NavRecordingSimBroker):
    """SimBroker with the fee model injected on the instance, as P5 requires."""

    def __init__(self, config: dict, fee_model: FeeModel) -> None:
        super().__init__(config)
        self._fee_model = fee_model


class FixedShareClock(OracleWeightClock):
    """Step-4 weight clock: fixed share targets, so both twins trade one book.

    Hold days come from the base clock and produce no orders.  On a decision
    date the target is a share count derived from a fixed notional, expressed
    as ``(shares + 1) * price / nav`` — the same +1 share bias the hold
    weights use to keep ``floor_to_lot`` idempotent through the float
    round-trip.  The nav cancels inside ``w * nav / price``, so the fee model
    cannot fork the order path.
    """

    def __init__(
        self,
        universe: list[str],
        decision_dates: tuple[date, ...],
        notional_per_name: float,
    ):
        super().__init__(universe, decision_dates)
        self._notional = notional_per_name
        self.target_shares: dict[str, int] = {}

    def on_rebalance(self, date_, bars, positions, account) -> dict[str, float]:
        if date_ not in self._decision_dates:
            return super().on_rebalance(date_, bars, positions, account)
        out: dict[str, float] = {}
        self.target_shares = {}
        for ticker in self._universe:
            price = get_latest_price(bars, ticker, date_, field="close")
            shares = floor_to_lot(self._notional / price, 100)
            self.target_shares[ticker] = shares
            out[ticker] = (shares + 1.0) * price / account.nav
        return out


# ---------------------------------------------------------------------------
# module-scoped twin run
# ---------------------------------------------------------------------------


def _run_chain(bars: pd.DataFrame, dates: list[date], fee_model: FeeModel) -> dict:
    config = {
        "initial_cash": INITIAL_CASH,
        "bars": bars,
        "fill_basis": FILL_BASIS_T1_OPEN,
        "risk": dict(RISK_CONFIG),
    }
    broker = FeeTwinBroker(config, fee_model)
    strategy = FixedShareClock(UNIVERSE, DECISION_DATES, NOTIONAL_PER_NAME)
    engine = CrossSectionalBacktest(broker)
    results = engine.run(strategy, bars, dates)
    nav = np.array(broker.nav_path)
    return {
        "broker": broker,
        "strategy": strategy,
        "results": results,
        "nav": nav,
        "metrics": chain_metrics(nav),
    }


def _fills(run: dict) -> list[dict]:
    return [
        f for r in run["results"] for f in r["fills"] if f.get("status") == "FILLED"
    ]


def _rejected(run: dict) -> list[dict]:
    return [
        f for r in run["results"] for f in r["fills"] if f.get("status") == "REJECTED"
    ]


@pytest.fixture(scope="module")
def prices() -> pd.DataFrame:
    return load_prices()


@pytest.fixture(scope="module")
def bars(prices) -> pd.DataFrame:
    return long_bars(prices)


@pytest.fixture(scope="module")
def dates(prices) -> list[date]:
    return run_dates(prices)


@pytest.fixture(scope="module")
def twins(bars, dates) -> dict:
    """The S6->S7 chain twice: zero-fee double, then the landed FeeModel.

    The order in which the two run is immaterial — each is a fresh broker,
    strategy and engine over the same bars and dates.
    """
    zero = _run_chain(bars, dates, ZeroFeeModel())
    recorder = RecordingFeeModel()
    fee = _run_chain(bars, dates, recorder)
    return {"zero": zero, "fee": fee, "fee_calls": recorder.calls}


# ---------------------------------------------------------------------------
# P8: the four-part schedule at the constants
# ---------------------------------------------------------------------------


class TestHandCalcSchedule:
    """P8 — the hand-calc covers COMMISSION_MIN and a non-exempt SELL."""

    def test_constants_match_the_fee_model(self):
        """The literals are the schedule FeeModel implements — one drift check."""
        assert COMMISSION_RATE == FeeModel.COMMISSION_RATE
        assert COMMISSION_MIN == FeeModel.COMMISSION_MIN
        assert STAMP_TAX_RATE == FeeModel.STAMP_TAX_RATE
        assert TRANSFER_FEE_RATE == FeeModel.TRANSFER_FEE_RATE
        assert SLIPPAGE_RATE == FeeModel.SLIPPAGE_RATE

    def test_case_table_exemption_matches_asset_rules(self):
        """The synthetic tickers are chosen for their prefix, and say so."""
        expected_exempt = {
            "510300.SH": True,  # listed fund
            "600519.SH": False,  # stock
        }
        for _side, _price, _shares, ticker in SYNTHETIC_CASES:
            assert is_stamp_tax_exempt_prefix(ticker) is expected_exempt[ticker]

    def test_commission_floor_binds_on_a_small_order(self):
        """P8 — the 5-yuan floor is what a small order actually pays."""
        fee = hand_calc_fee("BUY", 10.0, 100, exempt=True)
        assert fee["commission"] == COMMISSION_MIN
        assert 10.0 * 100 * COMMISSION_RATE < COMMISSION_MIN, (
            "the control needs the floor"
        )

    def test_commission_floor_does_not_bind_on_a_large_order(self):
        fee = hand_calc_fee("BUY", 141.5, 100_000, exempt=False)
        assert fee["commission"] == 141.5 * 100_000 * COMMISSION_RATE
        assert fee["commission"] > COMMISSION_MIN

    def test_stamp_tax_on_a_non_exempt_sell(self):
        """P8 — the golden universe waives stamp tax, so probe a stock prefix."""
        fee = hand_calc_fee("SELL", 141.5, 100_000, exempt=False)
        assert fee["stamp"] == 141.5 * 100_000 * STAMP_TAX_RATE
        assert fee["stamp"] > 0.0

    def test_no_stamp_tax_on_an_exempt_sell(self):
        fee = hand_calc_fee("SELL", 141.5, 100_000, exempt=True)
        assert fee["stamp"] == 0.0

    def test_no_stamp_tax_on_a_buy_even_when_taxable(self):
        """Stamp tax is SELL-only — the instrument's class never makes a BUY pay it."""
        fee = hand_calc_fee("BUY", 141.5, 100_000, exempt=False)
        assert fee["stamp"] == 0.0

    def test_slippage_is_one_term_on_both_sides(self):
        for side in ("BUY", "SELL"):
            fee = hand_calc_fee(side, 141.5, 100_000, exempt=False)
            assert fee["slippage"] == 141.5 * 100_000 * SLIPPAGE_RATE

    @pytest.mark.parametrize("side,price,shares,ticker", SYNTHETIC_CASES)
    def test_landed_fee_model_matches_the_hand_calc(self, side, price, shares, ticker):
        """The landed compute() agrees with the paper schedule on every case."""
        exempt = is_stamp_tax_exempt_prefix(ticker)
        got = FeeModel().compute(side, price, shares, ticker)
        exp = hand_calc_fee(side, price, shares, exempt)
        assert got == exp["total"], (
            f"{side} {shares} {ticker} @ {price}: FeeModel {got} != hand-calc {exp['total']}"
        )


# ---------------------------------------------------------------------------
# ac-2: the chain's fees match the hand-calc
# ---------------------------------------------------------------------------


class TestChainFeesMatchHandCalc:
    """ac-2 — every fill the chain settles is billed the hand-calc amount."""

    def test_the_golden_universe_is_stamp_tax_exempt(self):
        """All five golden tickers are listed funds — stamp tax never binds here.

        This is why the synthetic cases above own the non-exempt SELL.
        """
        for ticker in UNIVERSE:
            assert is_stamp_tax_exempt_prefix(ticker), (
                f"{ticker}: expected a listed-fund prefix, got {ticker[:2]}"
            )

    def test_the_fee_twin_charges_and_the_zero_twin_does_not(self, twins):
        assert twins["fee_calls"], "the landed FeeModel charged nothing"
        zero_fee = ZeroFeeModel().compute("BUY", 10.0, 100, UNIVERSE[0])
        assert zero_fee == 0.0
        assert all(c["fee"] > 0.0 for c in twins["fee_calls"])

    def test_every_fill_fee_matches_the_hand_calc(self, twins):
        for call in twins["fee_calls"]:
            exp = hand_calc_fee(
                call["side"], call["price"], call["shares"], exempt=True
            )["total"]
            assert call["fee"] == exp, (
                f"{call['side']} {call['shares']} {call['ticker']} @ {call['price']}: "
                f"charged {call['fee']} != hand-calc {exp}"
            )

    def test_sum_of_charged_fees_matches_the_hand_calc_sum(self, twins):
        charged = sum(c["fee"] for c in twins["fee_calls"])
        hand = sum(
            hand_calc_fee(c["side"], c["price"], c["shares"], exempt=True)["total"]
            for c in twins["fee_calls"]
        )
        assert charged == hand

    def test_one_fee_computation_per_settled_fill(self, twins):
        """A fill is billed once — not once at lookup and once at settlement."""
        filled = _fills(twins["fee"])
        assert not _rejected(twins["fee"]), (
            f"fee twin rejected fills: {_rejected(twins['fee'])}"
        )
        assert len(twins["fee_calls"]) == len(filled) > 0


# ---------------------------------------------------------------------------
# ac-2: the before/after delta is the fee charge
# ---------------------------------------------------------------------------


class TestFeeBeforeAfter:
    """ac-2 — zero-fee NAV minus fee-charged NAV == sum of the per-fill fees."""

    def test_twins_raise_identical_orders_and_fills(self, twins):
        """The precondition for the delta being the fee and nothing else.

        If the order paths forked, the delta would also carry a second-order
        sizing difference (probed at 3.7 % of the fee with nav-derived weights).
        """
        zero_fills, fee_fills = _fills(twins["zero"]), _fills(twins["fee"])
        assert len(zero_fills) == len(fee_fills) > 0
        for a, b in zip(zero_fills, fee_fills, strict=True):
            assert (a["ticker"], a["side"], a["shares"]) == (
                b["ticker"],
                b["side"],
                b["shares"],
            ), f"order path forked: {a} vs {b}"
            assert a["fill_price"] == b["fill_price"]

    def test_before_after_delta_equals_the_sum_of_per_fill_fees(self, twins):
        """ac-2 — the headline equality.

        The residual is float accumulation across 58 NAV points, not a sizing
        effect: the twins raise identical fills (test above), and with
        flat-overnight bars the settlement moves NAV by exactly the fee.
        """
        delta = float(twins["zero"]["nav"][-1] - twins["fee"]["nav"][-1])
        total_fee = sum(c["fee"] for c in twins["fee_calls"])
        assert delta > 0.0
        assert delta == pytest.approx(total_fee, rel=1e-9, abs=1e-4), (
            f"delta {delta} != sum of fees {total_fee}"
        )

    def test_before_after_delta_equals_the_hand_calc_sum(self, twins):
        delta = float(twins["zero"]["nav"][-1] - twins["fee"]["nav"][-1])
        hand = sum(
            hand_calc_fee(c["side"], c["price"], c["shares"], exempt=True)["total"]
            for c in twins["fee_calls"]
        )
        assert delta == pytest.approx(hand, rel=1e-9, abs=1e-4)

    def test_fee_drag_is_the_only_difference_in_the_final_nav(self, twins):
        zero_nav, fee_nav = twins["zero"]["nav"], twins["fee"]["nav"]
        assert len(zero_nav) == len(fee_nav)
        assert fee_nav[-1] < zero_nav[-1], (
            "fees are a cost; the fee twin must end lower"
        )

    def test_no_rejected_fills_in_either_twin(self, twins):
        """The fee sleeve is sized so every settlement clears — no silent gaps."""
        for name, run in (("zero", twins["zero"]), ("fee", twins["fee"])):
            rejected = _rejected(run)
            assert not rejected, f"{name} twin rejected fills: {rejected}"

    def test_zero_orders_and_fills_off_decision_dates_in_both_twins(self, twins):
        decision = set(DECISION_DATES)
        for name, run in (("zero", twins["zero"]), ("fee", twins["fee"])):
            off = [r for r in run["results"] if r["date"] not in decision]
            for result in off:
                assert result["orders"] == [], (
                    f"{name} {result['date']}: {result['orders']}"
                )
                assert result["fills"] == [], (
                    f"{name} {result['date']}: {result['fills']}"
                )


# ---------------------------------------------------------------------------
# ac-5: slippage is charged once, in FeeModel, never on the fill price
# ---------------------------------------------------------------------------


class TestNoDoubleCount:
    """ac-5 / P7 / QUANT-26 — the 0.1 % is billed exactly once."""

    def test_fill_price_is_the_raw_lookup_price(self, twins):
        """The recorded fill price is the open at the fill date, unadjusted.

        A fill price pre-skewed by the slippage rate would charge it twice:
        once in the price and again as one of FeeModel's four parts.
        """
        run = twins["fee"]
        broker = run["broker"]
        bars = broker._bars
        seen = 0
        for day_index, result in enumerate(run["results"]):
            fill_date = broker.fill_dates[day_index]
            for fill in result["fills"]:
                if fill.get("status") != "FILLED":
                    continue
                raw = get_latest_price(bars, fill["ticker"], fill_date, "open")
                assert raw is not None
                assert fill["fill_price"] == raw, (
                    f"{fill['ticker']} @ {fill_date}: fill_price "
                    f"{fill['fill_price']} != raw open {raw}"
                )
                seen += 1
        assert seen > 0

    def test_fee_twin_and_zero_twin_strike_the_same_raw_price(self, twins):
        """Charging the fee does not move the fill price — it is booked on top."""
        for a, b in zip(_fills(twins["zero"]), _fills(twins["fee"]), strict=True):
            assert a["fill_price"] == b["fill_price"]

    def test_the_landed_fee_carries_exactly_one_slippage_term(self, twins):
        """Decompose the charged fee: the 0.1 % appears once, not twice.

        The non-slippage parts come from the same schedule, so a fee that
        billed slippage on an already-skewed value would land above one term
        and a fee that omitted it would land below.  The split itself is a
        binary64 subtraction of three summed parts and is not bit-exact; the
        schedule is, which is why the one-term/two-term separation is the
        exact claim and the slice value is the tolerant one.
        """
        for call in twins["fee_calls"]:
            value = call["price"] * call["shares"]
            parts = hand_calc_fee(
                call["side"], call["price"], call["shares"], exempt=True
            )
            non_slippage = parts["commission"] + parts["stamp"] + parts["transfer"]
            charged = call["fee"] - non_slippage
            one_term = value * SLIPPAGE_RATE
            assert charged == pytest.approx(one_term, rel=1e-12, abs=1e-9), (
                f"{call['ticker']}: slippage slice {charged} != one term {one_term}"
            )
            # A fill price pre-skewed by 0.1 % would bill this at 2x.
            assert abs(charged - one_term) < abs(charged - 2.0 * one_term), (
                f"{call['ticker']}: charged slippage is closer to two terms than one"
            )

    def test_fee_model_is_called_with_the_recorded_fill_price(self, twins):
        """The call site passes the raw lookup number the fill also records.

        This is the QUANT-26 contract observed from outside: FeeModel never
        sees a price the fill-price path already adjusted.
        """
        calls = twins["fee_calls"]
        filled = _fills(twins["fee"])
        assert len(calls) == len(filled) > 0
        for call, fill in zip(calls, filled, strict=True):
            assert call["price"] == fill["fill_price"]
            assert call["shares"] == fill["shares"]
            assert call["side"] == fill["side"]
            assert call["ticker"] == fill["ticker"]

    def test_settle_bills_the_injected_fee_model_not_a_module_default(self):
        """P5/P10 — the seam is ``self._fee_model``, not a switch in fee_model.py."""
        source = inspect.getsource(SimBroker._settle)
        assert "self._fee_model.compute" in source
        assert "FeeModel(" not in source, (
            "_settle constructs a FeeModel — the seam must be the instance attribute"
        )
