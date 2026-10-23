"""Tests for execution.risk_control — seven pre-trade rules + append-only audit.

Acceptance criteria (QUANT-26):
1. Each of the seven rules (§66.3) triggers in isolation.
2. Any BLOCK refuses the whole batch; a too-small order is SKIPped without
   refusing the batch.
3. risk_audit.jsonl is append-only and carries records.
Plus: gap #3 limit-price rejection stays an interface (NotImplementedError),
and the short-sale flag is read from core.asset_rules (no second table).
"""

from __future__ import annotations

import json

import pytest

from core.asset_rules import ASSET_RULES
from core.types import AssetType
from execution.risk_control import (
    RiskCheckResult,
    check_price_limit,
    log_risk_audit,
    pre_trade_risk_check,
)


class _Account:
    """Stands in for broker.md SimAccount: cash + holdings (ticker -> shares)."""

    def __init__(self, cash: float, holdings: dict[str, int] | None = None):
        self.cash = cash
        self.holdings = dict(holdings or {})


def _order(ticker: str, side: str, shares: int, limit_price: float) -> dict:
    return {
        "ticker": ticker,
        "side": side,
        "shares": shares,
        "limit_price": limit_price,
    }


NAV = 100_000.0


class TestSevenRules:
    def test_rule1_single_weight_blocks(self):
        # 30% post-trade weight > default 25% — and nothing else fires.
        account = _Account(cash=NAV)
        orders = [_order("600519.SH", "BUY", 3000, 10.0)]
        result = pre_trade_risk_check(orders, account, NAV, {})
        assert result.passed is False
        assert result.blocked_by == ["single_weight:600519.SH"]
        assert result.is_blocked("600519.SH")

    def test_rule1_post_trade_weight_includes_holdings(self):
        # held 20% + this buy 10% = 30% > 25%; checking the order alone
        # (10%) would pass — the rule is on the post-trade weight.
        account = _Account(cash=80_000.0, holdings={"600519.SH": 2000})
        orders = [_order("600519.SH", "BUY", 1000, 10.0)]
        result = pre_trade_risk_check(orders, account, NAV, {})
        assert result.passed is False
        assert result.blocked_by == ["single_weight:600519.SH"]

    def test_rule2_total_weight_blocks_whole_batch(self):
        # five names at 19.2% each pass rule 1; together 96% > 95% fails rule 2
        account = _Account(cash=NAV)
        tickers = ["600519.SH", "000001.SZ", "300750.SZ", "688001.SH", "601318.SH"]
        orders = [_order(t, "BUY", 1920, 10.0) for t in tickers]
        result = pre_trade_risk_check(orders, account, NAV, {})
        assert result.passed is False
        assert set(result.blocked_by) == {f"total_weight:{t}" for t in tickers}
        # batch-level block is batch-wide per ticker
        for t in tickers:
            assert result.is_blocked(t)

    def test_rule3_turnover_warns_without_blocking(self):
        # gross 310k / nav 200k = 155% > 150% — warn only, batch still passes
        account = _Account(
            cash=10_000.0, holdings={"000001.SZ": 16_000, "600519.SH": 3_000}
        )
        orders = [
            _order("000001.SZ", "SELL", 16_000, 10.0),
            _order("300750.SZ", "BUY", 5_000, 10.0),
            _order("688001.SH", "BUY", 5_000, 10.0),
            _order("601318.SH", "BUY", 5_000, 10.0),
        ]
        result = pre_trade_risk_check(orders, account, 200_000.0, {})
        assert result.passed is True
        assert result.blocked_by == []
        assert set(result.warnings) == {
            "turnover:000001.SZ",
            "turnover:300750.SZ",
            "turnover:688001.SH",
            "turnover:601318.SH",
        }

    def test_rule4_small_buy_skipped_without_refusing_batch(self):
        account = _Account(cash=NAV)
        small = _order("600519.SH", "BUY", 100, 10.0)  # 1_000 < 5_000
        normal = _order("000001.SZ", "BUY", 1000, 10.0)  # 10_000
        result = pre_trade_risk_check([small, normal], account, NAV, {})
        assert small.get("skip") is True
        assert not normal.get("skip")
        assert result.passed is True
        assert result.blocked_by == []
        assert result.warnings == []

    def test_rule4_skipped_order_excluded_from_aggregates(self):
        # each 2_900-yuan buy alone is filtered; jointly they would be 58% of
        # this 10k nav and trip rule 1 — a skip must not move any figure.
        account = _Account(cash=NAV)
        a = _order("600519.SH", "BUY", 290, 10.0)
        b = _order("000001.SZ", "BUY", 290, 10.0)
        result = pre_trade_risk_check([a, b], account, 10_000.0, {})
        assert a.get("skip") is True and b.get("skip") is True
        assert result.passed is True
        assert result.blocked_by == []

    def test_rule5_short_forbidden_blocks(self):
        account = _Account(cash=50_000.0, holdings={"600519.SH": 1000})
        orders = [_order("600519.SH", "SELL", 2000, 10.0)]  # 2_000 > 1_000 held
        result = pre_trade_risk_check(orders, account, NAV, {})
        assert result.passed is False
        assert result.blocked_by == ["short_forbidden:600519.SH"]

    def test_rule5_short_flag_reads_asset_rules(self, monkeypatch):
        # §69.5: the prohibition parameter is ASSET_RULES[*]["short"].
        monkeypatch.setitem(ASSET_RULES[AssetType.STOCK.value], "short", True)
        account = _Account(cash=50_000.0, holdings={"600519.SH": 1000})
        orders = [_order("600519.SH", "SELL", 2000, 10.0)]
        result = pre_trade_risk_check(orders, account, NAV, {})
        assert "short_forbidden:600519.SH" not in result.blocked_by
        assert result.passed is True

    def test_rule6_st_blocks(self):
        account = _Account(cash=NAV)
        orders = [_order("600519.SH", "BUY", 1000, 10.0)]
        result = pre_trade_risk_check(
            orders, account, NAV, {"st_tickers": {"600519.SH"}}
        )
        assert result.passed is False
        assert result.blocked_by == ["st_forbidden:600519.SH"]

    def test_rule7_liquidity_warns(self):
        # 1_000 shares > 1% of 50_000 daily volume
        account = _Account(cash=NAV)
        orders = [_order("600519.SH", "BUY", 1000, 10.0)]
        result = pre_trade_risk_check(
            orders, account, NAV, {"daily_volumes": {"600519.SH": 50_000}}
        )
        assert result.passed is True
        assert result.blocked_by == []
        assert result.warnings == ["liquidity:600519.SH"]

    def test_rule7_liquidity_boundary_no_warn(self):
        # exactly 1% of the daily volume is not over it
        account = _Account(cash=NAV)
        orders = [_order("600519.SH", "BUY", 500, 10.0)]
        result = pre_trade_risk_check(
            orders, account, NAV, {"daily_volumes": {"600519.SH": 50_000}}
        )
        assert result.warnings == []

    def test_rule7_zero_volume_warns(self):
        # Review finding 1: volume 0 is a halted name, not "absent" — 1% of 0
        # is 0, so any order is over the cap and must warn.
        account = _Account(cash=NAV)
        orders = [_order("600519.SH", "BUY", 1000, 10.0)]
        result = pre_trade_risk_check(
            orders, account, NAV, {"daily_volumes": {"600519.SH": 0}}
        )
        assert result.warnings == ["liquidity:600519.SH"]
        assert result.passed is True

    def test_rule7_absent_volume_stays_silent(self):
        # counterpart: a ticker with no volume entry is unevaluated, not over
        account = _Account(cash=NAV)
        orders = [_order("600519.SH", "BUY", 1000, 10.0)]
        result = pre_trade_risk_check(orders, account, NAV, {"daily_volumes": {}})
        assert result.warnings == []
        assert result.blocked_by == []


class TestBatchSemantics:
    def test_block_refuses_whole_batch(self):
        # one violator + one innocent order — the batch verdict is BLOCK
        account = _Account(cash=NAV)
        bad = _order("600519.SH", "BUY", 3000, 10.0)  # 30% > 25%
        good = _order("000001.SZ", "BUY", 1000, 10.0)  # 10%
        result = pre_trade_risk_check([bad, good], account, NAV, {})
        assert result.passed is False
        assert result.is_blocked("600519.SH")

    def test_is_blocked_matches_ticker_suffix(self):
        result = RiskCheckResult(False, ["single_weight:600519.SH"], [])
        assert result.is_blocked("600519.SH")
        assert not result.is_blocked("000001.SZ")

    def test_unknown_prefix_fails_loudly(self):
        account = _Account(cash=NAV, holdings={"999999.SH": 100})
        orders = [_order("999999.SH", "SELL", 200, 10.0)]
        with pytest.raises(ValueError):
            pre_trade_risk_check(orders, account, NAV, {})

    def test_nav_must_be_positive(self):
        with pytest.raises(ValueError):
            pre_trade_risk_check([], _Account(cash=1.0), 0.0, {})

    def test_bad_side_raises(self):
        orders = [_order("600519.SH", "HOLD", 1000, 10.0)]
        with pytest.raises(ValueError):
            pre_trade_risk_check(orders, _Account(cash=NAV), NAV, {})

    def test_non_positive_shares_or_price_rejected(self):
        # Review finding 3: a SELL of -2000 against 30% holdings used to pass
        # rule 5 (-2000 > 3000 is False) while rule 1 scored a 50% post-trade
        # weight — the guard inverted the verdict on a malformed order.
        account = _Account(cash=NAV, holdings={"600519.SH": 3000})
        for bad in (
            _order("600519.SH", "SELL", -2000, 10.0),
            _order("600519.SH", "SELL", 0, 10.0),
            _order("600519.SH", "BUY", -100, 10.0),
            _order("600519.SH", "BUY", 100, 0.0),
            _order("600519.SH", "BUY", 100, -1.0),
        ):
            with pytest.raises(ValueError):
                pre_trade_risk_check([bad], account, NAV, {})

    def test_malformed_order_not_skipped_by_rule4(self):
        # validation runs before the min-value filter — a zero-share BUY must
        # not be quietly filtered instead of refused
        account = _Account(cash=NAV)
        tiny_bad = _order("600519.SH", "BUY", 0, 10.0)  # 0 < 5000, would skip
        with pytest.raises(ValueError):
            pre_trade_risk_check([tiny_bad], account, NAV, {})
        assert not tiny_bad.get("skip")


class TestRiskAudit:
    def test_audit_writes_block_and_warn_records(self, tmp_path):
        path = tmp_path / "risk_audit.jsonl"
        check = RiskCheckResult(
            False, ["single_weight:600519.SH"], ["liquidity:000001.SZ"]
        )
        log_risk_audit(check, path)
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 2
        block = json.loads(lines[0])
        assert block["rule"] == "single_weight"
        assert block["ticker"] == "600519.SH"
        assert block["action"] == "BLOCK"
        assert block["reason"]
        assert block["timestamp"]
        warn = json.loads(lines[1])
        assert warn["action"] == "WARN"
        assert warn["rule"] == "liquidity"
        assert warn["ticker"] == "000001.SZ"

    def test_audit_append_only(self, tmp_path):
        path = tmp_path / "risk_audit.jsonl"
        log_risk_audit(RiskCheckResult(False, ["short_forbidden:600519.SH"], []), path)
        first = path.read_text(encoding="utf-8")
        log_risk_audit(RiskCheckResult(True, [], ["turnover:000001.SZ"]), path)
        second = path.read_text(encoding="utf-8")
        assert second.startswith(first)  # nothing rewritten in place
        assert second.count("\n") == 2
        assert json.loads(second.splitlines()[0])["rule"] == "short_forbidden"
        assert json.loads(second.splitlines()[1])["rule"] == "turnover"

    def test_audit_clean_check_writes_nothing(self, tmp_path):
        path = tmp_path / "risk_audit.jsonl"
        log_risk_audit(RiskCheckResult(True, [], []), path)
        assert not path.exists()

    def test_audit_records_come_from_pre_trade_check(self, tmp_path):
        path = tmp_path / "risk_audit.jsonl"
        account = _Account(cash=NAV)
        orders = [_order("600519.SH", "BUY", 3000, 10.0)]
        result = pre_trade_risk_check(orders, account, NAV, {})
        log_risk_audit(result, path)
        record = json.loads(path.read_text(encoding="utf-8").strip())
        assert record["action"] == "BLOCK"
        assert record["rule"] == "single_weight"
        assert record["ticker"] == "600519.SH"


class TestPriceLimitHook:
    def test_gap3_hook_is_interface_only(self):
        # Gap #3 undecided: the check position exists, the body does not.
        order = _order("600519.SH", "BUY", 100, 10.0)
        with pytest.raises(NotImplementedError):
            check_price_limit(order, prev_close=9.9, config={})
