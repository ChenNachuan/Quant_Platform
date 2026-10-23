"""Tests for execution.fee_model — the four-part A-share fee suite.

Acceptance criteria (QUANT-26):
1. Hand-calc agreement per component: commission 万 2.5 with ¥5 floor,
   stamp tax 万 5 on SELL only, transfer fee 万 0.1, slippage 0.1%.
2. Hypothesis: fee ≥ 0; sell fee ≥ buy fee.
3. Stamp-tax exemption follows core.asset_rules.STAMP_TAX_EXEMPT_PREFIXES —
   two-way agreement, and fee_model keeps no second table (gap #17).
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from core.asset_rules import (
    ASSET_PREFIXES,
    STAMP_TAX_EXEMPT_PREFIXES,
    is_stamp_tax_exempt_prefix,
)
from execution.fee_model import FeeModel

# Hand-calc fixture: value 100_000 exercises the commission-rate branch.
PRICE = 10.0
SHARES = 10_000
VALUE = PRICE * SHARES  # 100_000


class TestHandCalc:
    def test_rates_match_rule_table(self):
        assert FeeModel.COMMISSION_RATE == 0.00025
        assert FeeModel.COMMISSION_MIN == 5.0
        assert FeeModel.STAMP_TAX_RATE == 0.0005
        assert FeeModel.TRANSFER_FEE_RATE == 0.00001
        assert FeeModel.SLIPPAGE_RATE == 0.001

    def test_buy_stock_total(self):
        # commission 25.0 + stamp 0 + transfer 1.0 + slippage 100.0
        assert FeeModel().compute("BUY", PRICE, SHARES, "600519.SH") == pytest.approx(
            126.0
        )

    def test_sell_stock_total(self):
        # commission 25.0 + stamp 50.0 + transfer 1.0 + slippage 100.0
        assert FeeModel().compute("SELL", PRICE, SHARES, "600519.SH") == pytest.approx(
            176.0
        )

    def test_sell_exempt_total_matches_buy(self):
        # no stamp tax: ETF 51, LOF 50 / 16 all land on the buy total
        for ticker in ("510300.SH", "501050.SH", "160119.SZ"):
            sell = FeeModel().compute("SELL", PRICE, SHARES, ticker)
            buy = FeeModel().compute("BUY", PRICE, SHARES, ticker)
            assert sell == pytest.approx(126.0)
            assert sell == pytest.approx(buy)

    def test_commission_floor_on_small_order(self):
        # value 1_000 -> rate commission 0.25 floored to 5.0
        # buy: 5.0 + 0 + 0.01 + 1.0 = 6.01
        assert FeeModel().compute("BUY", 10.0, 100, "600519.SH") == pytest.approx(6.01)
        # sell stock: 5.0 + 0.5 + 0.01 + 1.0 = 6.51
        assert FeeModel().compute("SELL", 10.0, 100, "600519.SH") == pytest.approx(6.51)

    def test_commission_floor_boundary(self):
        # value 20_000 -> rate commission exactly 5.0 (no floored excess)
        assert FeeModel().compute("BUY", 20.0, 1000, "600519.SH") == pytest.approx(
            5.0 + 0.2 + 20.0
        )

    def test_stamp_is_sell_only_and_value_proportional(self):
        model = FeeModel()
        delta = model.sell_fee(PRICE, SHARES, "600519.SH") - model.buy_fee(
            PRICE, SHARES, "600519.SH"
        )
        assert delta == pytest.approx(VALUE * FeeModel.STAMP_TAX_RATE)

    def test_unknown_side_raises(self):
        with pytest.raises(ValueError):
            FeeModel().compute("buy", PRICE, SHARES, "600519.SH")


class TestHypothesis:
    @settings(max_examples=50)
    @given(
        price=st.floats(min_value=1, max_value=1000),
        shares=st.integers(min_value=100, max_value=10000),
    )
    def test_fee_non_negative_and_sell_ge_buy(self, price, shares):
        model = FeeModel()
        buy = model.buy_fee(price, shares)
        sell = model.sell_fee(price, shares)
        assert buy >= 0
        assert sell >= buy

    @settings(max_examples=50)
    @given(
        price=st.floats(min_value=1, max_value=1000),
        shares=st.integers(min_value=100, max_value=10000),
        ticker=st.sampled_from(["600519.SH", "000001.SZ", "510300.SH", "501050.SH"]),
    )
    def test_sell_buy_gap_tracks_exemption(self, price, shares, ticker):
        model = FeeModel()
        gap = model.sell_fee(price, shares, ticker) - model.buy_fee(
            price, shares, ticker
        )
        assert gap >= 0
        if is_stamp_tax_exempt_prefix(ticker):
            assert gap == pytest.approx(0.0)
        else:
            assert gap == pytest.approx(price * shares * FeeModel.STAMP_TAX_RATE)


class TestExemptSetConsistency:
    def test_no_second_table_in_fee_model(self):
        # Gap #17: the derived set lives in core.asset_rules only.
        import execution.fee_model as fee_model_module

        assert fee_model_module.is_stamp_tax_exempt_prefix is is_stamp_tax_exempt_prefix
        assert not hasattr(fee_model_module, "_is_etf")  # old wording, banned
        assert not hasattr(FeeModel, "_is_etf")
        assert not hasattr(fee_model_module, "EXEMPT_PREFIXES")
        assert not hasattr(fee_model_module, "STAMP_TAX_EXEMPT_PREFIXES")

    def test_two_way_agreement_with_derived_set(self):
        # Membership of the derived set decides the sell stamp exactly:
        # exempt -> gap 0; taxable -> gap = value * stamp rate.  Both ways.
        model = FeeModel()
        for prefix in ASSET_PREFIXES:
            ticker = f"{prefix}00000.SH"
            gap = model.sell_fee(PRICE, SHARES, ticker) - model.buy_fee(
                PRICE, SHARES, ticker
            )
            if prefix in STAMP_TAX_EXEMPT_PREFIXES:
                assert gap == pytest.approx(0.0), f"{ticker} should be exempt"
            else:
                assert gap == pytest.approx(VALUE * FeeModel.STAMP_TAX_RATE), (
                    f"{ticker} should pay stamp tax"
                )

    def test_compute_consults_core_predicate(self, monkeypatch):
        # A local table would not go through this name — the spy must fire.
        import execution.fee_model as fee_model_module

        seen: list[str] = []
        real = fee_model_module.is_stamp_tax_exempt_prefix

        def spy(ticker: str) -> bool:
            seen.append(ticker)
            return real(ticker)

        monkeypatch.setattr(fee_model_module, "is_stamp_tax_exempt_prefix", spy)
        fee_model_module.FeeModel().compute("SELL", PRICE, SHARES, "600519.SH")
        assert seen == ["600519.SH"]

    def test_exempt_predicate_drives_stamp(self, monkeypatch):
        # Forcing the predicate must flip the fee: no hidden branch on prefix.
        import execution.fee_model as fee_model_module

        model = FeeModel()
        monkeypatch.setattr(
            fee_model_module, "is_stamp_tax_exempt_prefix", lambda ticker: True
        )
        assert model.sell_fee(PRICE, SHARES, "600519.SH") == pytest.approx(
            model.buy_fee(PRICE, SHARES, "600519.SH")
        )
        monkeypatch.setattr(
            fee_model_module, "is_stamp_tax_exempt_prefix", lambda ticker: False
        )
        gap = model.sell_fee(PRICE, SHARES, "510300.SH") - model.buy_fee(
            PRICE, SHARES, "510300.SH"
        )
        assert gap == pytest.approx(VALUE * FeeModel.STAMP_TAX_RATE)
