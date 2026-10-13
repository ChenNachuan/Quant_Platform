"""Tests for core.types — enum closure, bitmask semantics, dataclass construction."""

from datetime import datetime

import pytest

from core.types import (
    Account,
    AssetType,
    Direction,
    EventType,
    JournalEntry,
    Order,
    OrderStatus,
    Position,
    PriceType,
    QualityFlag,
    QualitySeverity,
    Trade,
)


class TestAssetType:
    def test_closed_enum_five_members(self):
        assert len(AssetType) == 5
        assert set(AssetType) == {
            AssetType.STOCK,
            AssetType.ETF,
            AssetType.BOND,
            AssetType.FUTURES,
            AssetType.OPTION,
        }

    def test_no_equity(self):
        values = [m.value for m in AssetType]
        assert "equity" not in values
        assert not hasattr(AssetType, "EQUITY")

    def test_rejects_unknown_value(self):
        with pytest.raises(ValueError):
            AssetType("equity")

    def test_values(self):
        assert AssetType.STOCK.value == "stock"
        assert AssetType.ETF.value == "etf"
        assert AssetType.BOND.value == "bond"
        assert AssetType.FUTURES.value == "futures"
        assert AssetType.OPTION.value == "option"


class TestQualityFlag:
    def test_clean_is_zero(self):
        assert QualityFlag.CLEAN == 0
        assert int(QualityFlag.CLEAN) == 0

    def test_five_bits_defined(self):
        bits = [
            QualityFlag.C02_COVERAGE,
            QualityFlag.C05_NULL_RATE,
            QualityFlag.L04_BEYOND_LIMIT,
            QualityFlag.L07_ADJFACTOR_RANGE,
            QualityFlag.K01_OVERNIGHT_GAP,
        ]
        assert len(bits) == 5
        for b in bits:
            assert b != 0

    def test_bits_non_overlapping(self):
        bits = [
            QualityFlag.C02_COVERAGE,
            QualityFlag.C05_NULL_RATE,
            QualityFlag.L04_BEYOND_LIMIT,
            QualityFlag.L07_ADJFACTOR_RANGE,
            QualityFlag.K01_OVERNIGHT_GAP,
        ]
        for i, a in enumerate(bits):
            for j, b in enumerate(bits):
                if i != j:
                    assert (a & b) == 0

    def test_bitmask_semantics(self):
        flag = QualityFlag.C02_COVERAGE | QualityFlag.L04_BEYOND_LIMIT
        assert flag & QualityFlag.C02_COVERAGE == QualityFlag.C02_COVERAGE
        assert flag & QualityFlag.L04_BEYOND_LIMIT == QualityFlag.L04_BEYOND_LIMIT
        assert flag & QualityFlag.C05_NULL_RATE == 0
        assert flag & QualityFlag.K01_OVERNIGHT_GAP == 0

    def test_combined_flag_value(self):
        flag = QualityFlag.C02_COVERAGE | QualityFlag.K01_OVERNIGHT_GAP
        assert int(flag) == 1 | 16 == 17


class TestQualitySeverity:
    def test_scale(self):
        assert QualitySeverity.LOW == 0
        assert QualitySeverity.MEDIUM == 1
        assert QualitySeverity.HIGH == 2
        assert len(QualitySeverity) == 3


class TestLedgerEnums:
    def test_direction(self):
        assert Direction.BUY.value == "BUY"
        assert Direction.SELL.value == "SELL"
        assert len(Direction) == 2

    def test_order_status(self):
        assert len(OrderStatus) == 6
        assert OrderStatus.CREATED.value == "CREATED"
        assert OrderStatus.SUBMITTED.value == "SUBMITTED"
        assert OrderStatus.PARTIAL.value == "PARTIAL"
        assert OrderStatus.FILLED.value == "FILLED"
        assert OrderStatus.CANCELLED.value == "CANCELLED"
        assert OrderStatus.REJECTED.value == "REJECTED"

    def test_price_type(self):
        assert PriceType.MARKET.value == "MARKET"
        assert PriceType.LIMIT.value == "LIMIT"
        assert len(PriceType) == 2

    def test_event_type(self):
        assert len(EventType) == 14


class TestLedgerDataclasses:
    def test_order_constructible(self):
        now = datetime(2026, 1, 15, 9, 30)
        order = Order(
            order_id="ord-001",
            portfolio_id="pf-001",
            datetime=now,
            ticker="510300",
            direction=Direction.BUY,
            quantity=100,
            price_type=PriceType.LIMIT,
            limit_price=4.5,
            ref_price=4.52,
            status=OrderStatus.CREATED,
        )
        assert order.order_id == "ord-001"
        assert order.direction is Direction.BUY
        assert order.status is OrderStatus.CREATED
        assert order.limit_price == 4.5

    def test_trade_constructible(self):
        now = datetime(2026, 1, 15, 9, 31)
        trade = Trade(
            trade_id="trd-001",
            order_id="ord-001",
            portfolio_id="pf-001",
            datetime=now,
            ticker="510300",
            direction=Direction.BUY,
            filled_qty=100,
            fill_price=4.52,
            commission=5.0,
            slippage=0.02,
        )
        assert trade.filled_qty == 100
        assert trade.commission == 5.0

    def test_position_constructible(self):
        pos = Position(
            portfolio_id="pf-001",
            ticker="510300",
            quantity=100,
            avg_cost=4.52,
            market_value=452.0,
        )
        assert pos.quantity == 100
        assert pos.realized_pnl == 0.0

    def test_account_constructible(self):
        acct = Account(
            account_id="acct-001",
            total_asset=100_000.0,
            cash=99_548.0,
        )
        assert acct.cash == 99_548.0
        assert acct.frozen_cash == 0.0

    def test_journal_entry_constructible(self):
        now = datetime(2026, 1, 15, 9, 30)
        entry = JournalEntry(
            timestamp=now,
            event_type=EventType.ORDER_CREATED,
            portfolio_id="pf-001",
        )
        assert entry.event_type is EventType.ORDER_CREATED
        assert entry.payload == {}
