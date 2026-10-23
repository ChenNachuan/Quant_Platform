"""Tests for execution.market_data + execution.order_generation — QUANT-27 ACs 3-5.

Acceptance criteria (QUANT-27, iteration 2):
3. compute_order_diff / generate_orders semantics: target_shares =
   floor_to_lot(w * nav / price); invalid price skipped; diff == 0 no order;
   sells before buys.
4. check_split_needed on > 1% daily volume only warns — never raises, never
   drops or blocks the order.
5. Gap #10 contract over all three entry points: strict asof equality (no
   last-known fallback), raw unadjusted prices, "open" | "close" only,
   volume in shares, duplicate (ticker, date) rows raise — and order_generation
   consumes the contract + floor_to_lot rather than re-deriving them.

(AC-6 is `uv run ruff check` on the ticket's files — a command, not a case.)
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd
import pytest

import execution.order_generation as order_generation
import execution.portfolio_construction as portfolio_construction
from execution.market_data import get_daily_volume, get_latest_price, get_prices
from execution.order_generation import (
    check_split_needed,
    compute_order_diff,
    generate_orders,
)

TRADE_DATE = date(2026, 1, 5)


class FakeAccount:
    """Duck-typed on the SimAccount surface order_generation sees (S7-T4)."""

    def __init__(self, holdings: dict[str, int]):
        self.holdings = holdings


def make_bars(rows: list[dict]) -> pd.DataFrame:
    columns = ["date", "ticker", "open", "high", "low", "close", "volume"]
    return pd.DataFrame(rows, columns=columns)


def bar(
    ticker: str,
    close: float,
    *,
    day: date = TRADE_DATE,
    open_: float | None = None,
    volume: float = 10_000_000.0,
) -> dict:
    return {
        "date": day,
        "ticker": ticker,
        "open": close if open_ is None else open_,
        "high": close,
        "low": close,
        "close": close,
        "volume": volume,
    }


# ---------------------------------------------------------------------------
# AC-5 — gap #10 contract
# ---------------------------------------------------------------------------


class TestMarketDataContract:
    def test_strict_asof_no_last_known_fallback(self):
        bars = make_bars([bar("A", 50.0, day=date(2026, 1, 5))])
        assert get_latest_price(bars, "A", date(2026, 1, 5)) == 50.0
        # 2026-01-06 has no row — suspended day reports no price, never 50.0.
        assert get_latest_price(bars, "A", date(2026, 1, 6)) is None
        assert get_prices(bars, ["A"], date(2026, 1, 6)) == {}

    def test_prices_are_raw_unadjusted(self):
        row = bar("A", 50.0)
        row["adjfactor"] = 2.0  # adjustment lives upstream; never applied here
        bars = pd.DataFrame([row])
        assert get_latest_price(bars, "A", TRADE_DATE, field="close") == 50.0

    def test_field_close_and_open(self):
        bars = make_bars([bar("A", 50.0, open_=48.5)])
        assert get_latest_price(bars, "A", TRADE_DATE, field="close") == 50.0
        assert get_latest_price(bars, "A", TRADE_DATE, field="open") == 48.5
        assert get_latest_price(bars, "A", TRADE_DATE) == 50.0  # default is close
        assert get_prices(bars, ["A"], TRADE_DATE, field="open") == {"A": 48.5}

    def test_invalid_field_refused(self):
        bars = make_bars([bar("A", 50.0)])
        for field in ("vwap", "Close", "adjclose", ""):
            with pytest.raises(ValueError):
                get_latest_price(bars, "A", TRADE_DATE, field=field)
            with pytest.raises(ValueError):
                get_prices(bars, ["A"], TRADE_DATE, field=field)

    def test_volume_is_that_days_shares(self):
        bars = make_bars(
            [
                bar("A", 50.0, day=date(2026, 1, 5), volume=1_234_567),
                bar("A", 51.0, day=date(2026, 1, 6), volume=2_000_000),
            ]
        )
        assert get_daily_volume(bars, "A", date(2026, 1, 5)) == 1_234_567.0
        assert get_daily_volume(bars, "A", date(2026, 1, 6)) == 2_000_000.0
        assert get_daily_volume(bars, "A", date(2026, 1, 7)) is None

    def test_duplicate_ticker_date_rows_raise(self):
        bars = make_bars([bar("A", 50.0), bar("A", 51.0)])
        with pytest.raises(ValueError):
            get_latest_price(bars, "A", TRADE_DATE)
        with pytest.raises(ValueError):
            get_prices(bars, ["A"], TRADE_DATE)
        with pytest.raises(ValueError):
            get_daily_volume(bars, "A", TRADE_DATE)

    @pytest.mark.parametrize("close", [float("nan"), 0.0, -3.0])
    def test_nan_and_non_positive_price_is_none(self, close):
        bars = make_bars([bar("A", close)])
        assert get_latest_price(bars, "A", TRADE_DATE) is None
        assert get_prices(bars, ["A"], TRADE_DATE) == {}

    def test_missing_ticker_is_none_and_omitted(self):
        bars = make_bars([bar("A", 50.0)])
        assert get_latest_price(bars, "ZZZ", TRADE_DATE) is None
        assert get_daily_volume(bars, "ZZZ", TRADE_DATE) is None
        assert get_prices(bars, ["A", "ZZZ"], TRADE_DATE) == {"A": 50.0}

    def test_nan_volume_is_none_zero_volume_is_data(self):
        bars = make_bars(
            [
                bar("A", 50.0, volume=float("nan")),
                bar("B", 50.0, volume=0),
                bar("C", 50.0, volume=-1),
            ]
        )
        assert get_daily_volume(bars, "A", TRADE_DATE) is None
        assert get_daily_volume(bars, "B", TRADE_DATE) == 0.0  # a real zero
        assert get_daily_volume(bars, "C", TRADE_DATE) is None

    def test_asof_accepts_date_datetime_and_string(self):
        bars = make_bars([bar("A", 50.0)])
        for asof in (TRADE_DATE, datetime(2026, 1, 5, 14, 30), "2026-01-05"):
            assert get_latest_price(bars, "A", asof) == 50.0
        assert get_latest_price(bars, "A", "2026-01-06") is None

    def test_datetime64_date_column_matches(self):
        bars = make_bars([bar("A", 50.0)])
        bars["date"] = pd.to_datetime(bars["date"])
        assert get_latest_price(bars, "A", TRADE_DATE) == 50.0

    def test_missing_contract_columns_refused(self):
        no_close = make_bars([bar("A", 50.0)]).drop(columns=["close"])
        with pytest.raises(ValueError):
            get_latest_price(no_close, "A", TRADE_DATE, field="close")
        no_volume = make_bars([bar("A", 50.0)]).drop(columns=["volume"])
        with pytest.raises(ValueError):
            get_daily_volume(no_volume, "A", TRADE_DATE)
        with pytest.raises(TypeError):
            get_latest_price([("A", 50.0)], "A", TRADE_DATE)

    def test_get_prices_refuses_bare_string(self):
        bars = make_bars([bar("A", 50.0)])
        with pytest.raises(TypeError):
            get_prices(bars, "A", TRADE_DATE)


# ---------------------------------------------------------------------------
# AC-3 — compute_order_diff semantics
# ---------------------------------------------------------------------------


class TestComputeOrderDiff:
    def test_target_shares_floors_to_lot_of_weight_nav_price(self):
        # w=0.10, nav=1e6, px=73 -> 1369.86 raw -> 1300 (floor to 100-lots).
        bars = make_bars([bar("A", 73.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.10]})
        orders = compute_order_diff(
            weights, FakeAccount({}), 1_000_000.0, bars, TRADE_DATE
        )
        assert orders == [
            {"ticker": "A", "side": "BUY", "shares": 1300, "limit_price": 73.0}
        ]

    def test_exact_lot_math_matches_hand_calculation(self):
        # w=0.25, nav=80_000, px=40 -> 500 raw -> 500 shares.
        bars = make_bars([bar("A", 40.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.25]})
        orders = compute_order_diff(
            weights, FakeAccount({}), 80_000.0, bars, TRADE_DATE
        )
        assert orders[0]["shares"] == 500
        assert isinstance(orders[0]["shares"], int)

    def test_shares_below_one_lot_produce_no_order(self):
        # w=0.0001, nav=500_000, px=50 -> 1 raw share -> floored to 0 lots.
        bars = make_bars([bar("A", 50.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.0001]})
        assert (
            compute_order_diff(weights, FakeAccount({}), 500_000.0, bars, TRADE_DATE)
            == []
        )

    def test_invalid_price_skipped_never_priced_off_stale_close(self):
        bars = make_bars(
            [
                bar("A", 50.0, day=date(2026, 1, 5)),
                bar("B", 50.0, day=date(2026, 1, 6)),  # suspended on T
                bar("C", float("nan")),
                bar("D", 0.0),
            ]
        )
        weights = pd.DataFrame(
            {"ticker": ["A", "B", "C", "D"], "weight": [0.10, 0.10, 0.10, 0.10]}
        )
        orders = compute_order_diff(
            weights, FakeAccount({}), 1_000_000.0, bars, TRADE_DATE
        )
        assert [o["ticker"] for o in orders] == ["A"]

    def test_zero_diff_produces_no_order(self):
        # target 200 shares vs held 200 shares -> nothing to trade.
        bars = make_bars([bar("A", 50.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.01]})  # 10_000 / 50 = 200
        assert (
            compute_order_diff(
                weights, FakeAccount({"A": 200}), 1_000_000.0, bars, TRADE_DATE
            )
            == []
        )

    def test_held_ticker_absent_from_weights_is_a_sell(self):
        bars = make_bars([bar("A", 50.0)])
        weights = pd.DataFrame({"ticker": [], "weight": []})
        orders = compute_order_diff(
            weights, FakeAccount({"A": 300}), 1_000_000.0, bars, TRADE_DATE
        )
        assert orders == [
            {"ticker": "A", "side": "SELL", "shares": 300, "limit_price": 50.0}
        ]

    def test_zero_weight_exits_a_holding_and_ignores_a_free_ticker(self):
        bars = make_bars([bar("A", 50.0), bar("B", 50.0)])
        weights = pd.DataFrame({"ticker": ["A", "B"], "weight": [0.0, 0.0]})
        orders = compute_order_diff(
            weights, FakeAccount({"A": 200}), 1_000_000.0, bars, TRADE_DATE
        )
        assert orders == [
            {"ticker": "A", "side": "SELL", "shares": 200, "limit_price": 50.0}
        ]

    def test_order_dict_matches_risk_control_shape(self):
        bars = make_bars([bar("A", 50.0), bar("B", 50.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.10]})
        orders = compute_order_diff(
            weights, FakeAccount({"B": 100}), 1_000_000.0, bars, TRADE_DATE
        )
        for order in orders:
            assert set(order) == {"ticker", "side", "shares", "limit_price"}
            assert order["side"] in ("BUY", "SELL")
            assert isinstance(order["shares"], int) and order["shares"] > 0
            assert order["limit_price"] > 0
        assert {o["ticker"]: o["side"] for o in orders} == {"A": "BUY", "B": "SELL"}

    def test_limit_price_is_the_decision_day_close(self):
        bars = make_bars([bar("A", 73.0, open_=71.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.10]})
        orders = compute_order_diff(
            weights, FakeAccount({}), 1_000_000.0, bars, TRADE_DATE
        )
        assert orders[0]["limit_price"] == 73.0

    @pytest.mark.parametrize("weight", [float("nan"), float("inf"), -0.1])
    def test_non_tradeable_weight_refused(self, weight):
        bars = make_bars([bar("A", 50.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [weight]})
        with pytest.raises(ValueError):
            compute_order_diff(weights, FakeAccount({}), 1_000_000.0, bars, TRADE_DATE)

    @pytest.mark.parametrize("nav", [float("nan"), 0.0, -1.0, float("inf")])
    def test_non_positive_nav_refused(self, nav):
        bars = make_bars([bar("A", 50.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.1]})
        with pytest.raises(ValueError):
            compute_order_diff(weights, FakeAccount({}), nav, bars, TRADE_DATE)

    def test_duplicate_ticker_row_refused(self):
        bars = make_bars([bar("A", 50.0)])
        weights = pd.DataFrame({"ticker": ["A", "A"], "weight": [0.1, 0.2]})
        with pytest.raises(ValueError):
            compute_order_diff(weights, FakeAccount({}), 1_000_000.0, bars, TRADE_DATE)

    def test_bad_weights_schema_refused(self):
        bars = make_bars([bar("A", 50.0)])
        with pytest.raises(ValueError):
            compute_order_diff(
                pd.DataFrame({"symbol": ["A"], "w": [0.1]}),
                FakeAccount({}),
                1_000_000.0,
                bars,
                TRADE_DATE,
            )
        with pytest.raises(TypeError):
            compute_order_diff(
                {"A": 0.1}, FakeAccount({}), 1_000_000.0, bars, TRADE_DATE
            )

    @pytest.mark.parametrize("quantity", [-1, float("nan"), 1.5, True])
    def test_bad_holdings_refused(self, quantity):
        bars = make_bars([bar("A", 50.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.1]})
        with pytest.raises(ValueError):
            compute_order_diff(
                weights, FakeAccount({"A": quantity}), 1_000_000.0, bars, TRADE_DATE
            )

    def test_account_without_holdings_refused(self):
        bars = make_bars([bar("A", 50.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.1]})

        class NoHoldings:
            pass

        with pytest.raises(TypeError):
            compute_order_diff(weights, NoHoldings(), 1_000_000.0, bars, TRADE_DATE)

    def test_lot_size_kwarg_is_honoured(self):
        bars = make_bars([bar("A", 50.0)])
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.1]})
        orders = compute_order_diff(
            weights, FakeAccount({}), 1_000_000.0, bars, TRADE_DATE, lot_size=10
        )
        # 0.1 * 1e6 / 50 = 2000 raw -> 2000 at lot 10 (2000 at lot 100 too);
        # a lot of 13 keeps the tail that lot 100 would floor away.
        orders_13 = compute_order_diff(
            weights, FakeAccount({}), 1_000_000.0, bars, TRADE_DATE, lot_size=13
        )
        assert orders[0]["shares"] == 2000
        assert orders_13[0]["shares"] == 1989  # floor(2000 / 13) * 13

    def test_consumes_floor_to_lot_and_market_data_contract(self):
        """Re-implementing either here is a finding — the names must be shared."""
        assert order_generation.floor_to_lot is portfolio_construction.floor_to_lot
        assert order_generation.get_latest_price is get_latest_price
        assert order_generation.get_daily_volume is get_daily_volume


# ---------------------------------------------------------------------------
# AC-3 — generate_orders semantics
# ---------------------------------------------------------------------------


class TestGenerateOrders:
    @staticmethod
    def _bars():
        return make_bars(
            [
                bar("A", 10.0, volume=10_000_000),
                bar("B", 10.0, volume=10_000_000),
                bar("C", 10.0, volume=10_000_000),
                bar("D", 10.0, volume=10_000_000),
            ]
        )

    def test_sells_come_before_buys(self):
        weights = pd.DataFrame({"ticker": ["C", "A"], "weight": [0.1, 0.1]})
        orders = generate_orders(
            weights, FakeAccount({"B": 500}), 1_000_000.0, self._bars(), TRADE_DATE
        )
        sides = [o["side"] for o in orders]
        assert sides == sorted(sides, key=lambda s: 0 if s == "SELL" else 1)
        assert sides.count("SELL") == 1 and sides.count("BUY") == 2

    def test_sells_sorted_by_notional_descending(self):
        weights = pd.DataFrame({"ticker": [], "weight": []})
        orders = generate_orders(
            weights,
            FakeAccount({"A": 300, "B": 100, "C": 200}),  # notionals 3000/1000/2000
            1_000_000.0,
            self._bars(),
            TRADE_DATE,
        )
        assert [(o["ticker"], o["side"]) for o in orders] == [
            ("A", "SELL"),
            ("C", "SELL"),
            ("B", "SELL"),
        ]

    def test_buys_sorted_by_scores_descending(self):
        weights = pd.DataFrame({"ticker": ["C", "D", "A"], "weight": [0.1, 0.1, 0.1]})
        orders = generate_orders(
            weights,
            FakeAccount({}),
            1_000_000.0,
            self._bars(),
            TRADE_DATE,
            scores={"A": 0.2, "C": 0.9, "D": 0.5},
        )
        assert [o["ticker"] for o in orders] == ["C", "D", "A"]

    def test_missing_score_sorts_below_every_present_score(self):
        weights = pd.DataFrame({"ticker": ["C", "D"], "weight": [0.1, 0.1]})
        orders = generate_orders(
            weights,
            FakeAccount({}),
            1_000_000.0,
            self._bars(),
            TRADE_DATE,
            scores={"C": -0.5},  # even a losing score outranks an absent one
        )
        assert [o["ticker"] for o in orders] == ["C", "D"]

    def test_buys_sorted_by_target_weight_without_scores(self):
        weights = pd.DataFrame(
            {"ticker": ["C", "D", "A"], "weight": [0.05, 0.15, 0.10]}
        )
        orders = generate_orders(
            weights, FakeAccount({}), 1_000_000.0, self._bars(), TRADE_DATE
        )
        assert [o["ticker"] for o in orders] == ["D", "A", "C"]

    def test_nan_score_refused(self):
        weights = pd.DataFrame({"ticker": ["A"], "weight": [0.1]})
        with pytest.raises(ValueError):
            generate_orders(
                weights,
                FakeAccount({}),
                1_000_000.0,
                self._bars(),
                TRADE_DATE,
                scores={"A": float("nan")},
            )

    def test_liquidity_warning_never_drops_or_reorders(self, caplog):
        # 1% of 10m shares is 100k. nav 10m at px 10: A buys 200k shares (over),
        # C buys 50k (under), B sells 50k held (under).
        weights = pd.DataFrame({"ticker": ["A", "C"], "weight": [0.2, 0.05]})
        account = FakeAccount({"B": 50_000})
        bars = make_bars(
            [
                bar("A", 10.0, volume=10_000_000),
                bar("B", 10.0, volume=10_000_000),
                bar("C", 10.0, volume=10_000_000),
            ]
        )
        orders = generate_orders(weights, account, 10_000_000.0, bars, TRADE_DATE)
        with caplog.at_level(logging.WARNING, logger="execution.order_generation"):
            warned = generate_orders(weights, account, 10_000_000.0, bars, TRADE_DATE)
        # AC-4: identical list — the warning neither drops, blocks nor reorders.
        assert warned == orders
        assert [o["ticker"] for o in orders] == ["B", "A", "C"]
        assert orders[1]["shares"] == 200_000  # the over-the-line order survives
        assert any("liquidity:A" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# AC-4 — check_split_needed is warn-only
# ---------------------------------------------------------------------------


class TestCheckSplitNeeded:
    def test_warns_above_one_percent_of_daily_volume(self):
        bars = make_bars([bar("A", 10.0, volume=1_000_000)])
        order = {"ticker": "A", "side": "BUY", "shares": 10_001, "limit_price": 10.0}
        assert check_split_needed(order, bars, TRADE_DATE) == ["liquidity:A"]

    def test_silent_at_or_below_one_percent(self):
        bars = make_bars([bar("A", 10.0, volume=1_000_000)])
        for shares in (1, 9_999, 10_000):
            order = {
                "ticker": "A",
                "side": "BUY",
                "shares": shares,
                "limit_price": 10.0,
            }
            assert check_split_needed(order, bars, TRADE_DATE) == []

    def test_max_volume_share_kwarg_scales_the_threshold(self):
        bars = make_bars([bar("A", 10.0, volume=1_000_000)])
        order = {"ticker": "A", "side": "BUY", "shares": 20_000, "limit_price": 10.0}
        assert check_split_needed(order, bars, TRADE_DATE, max_volume_share=0.01) == [
            "liquidity:A"
        ]
        assert check_split_needed(order, bars, TRADE_DATE, max_volume_share=0.05) == []

    def test_never_raises_on_duplicate_rows(self):
        bars = make_bars([bar("A", 10.0), bar("A", 11.0)])  # ValueError upstream
        order = {"ticker": "A", "side": "BUY", "shares": 10**6, "limit_price": 10.0}
        assert check_split_needed(order, bars, TRADE_DATE) == []

    def test_never_raises_on_broken_input(self):
        bars = make_bars([bar("A", 10.0)])
        for order in (
            None,
            {},
            {"ticker": None, "shares": 10},
            {"ticker": "A", "shares": float("nan")},
            {"ticker": "A", "shares": -5},
            {"ticker": "A", "shares": "lots"},
        ):
            assert check_split_needed(order, bars, TRADE_DATE) == []

    def test_missing_or_unusable_volume_yields_no_tag(self):
        bars = make_bars([bar("A", 10.0, volume=float("nan"))])
        order = {"ticker": "A", "side": "BUY", "shares": 10**6, "limit_price": 10.0}
        assert check_split_needed(order, bars, TRADE_DATE) == []
        assert (
            check_split_needed(
                {"ticker": "ZZZ", "side": "BUY", "shares": 10**6, "limit_price": 1.0},
                bars,
                TRADE_DATE,
            )
            == []
        )

    def test_never_mutates_the_order(self):
        bars = make_bars([bar("A", 10.0, volume=1_000)])
        order = {"ticker": "A", "side": "BUY", "shares": 10**6, "limit_price": 10.0}
        snapshot = dict(order)
        assert check_split_needed(order, bars, TRADE_DATE) == ["liquidity:A"]
        assert order == snapshot

    def test_suspended_day_yields_no_tag(self):
        bars = make_bars([bar("A", 10.0, day=date(2026, 1, 6))])
        order = {"ticker": "A", "side": "BUY", "shares": 10**6, "limit_price": 10.0}
        assert check_split_needed(order, bars, TRADE_DATE) == []
