"""Tests for core.asset_rules — lookup table, prefix table, predicate."""

import pytest

from core.asset_rules import (
    ASSET_RULES,
    STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES,
    get_asset_rules,
    is_stamp_tax_exempt_listed_fund,
)
from core.types import AssetType


class TestGetAssetRules:
    def test_unknown_raises_key_error(self):
        with pytest.raises(KeyError):
            get_asset_rules("unknown")

    def test_etf_stamp_tax_false(self):
        assert get_asset_rules("etf")["stamp_tax"] is False

    def test_stock_stamp_tax_true(self):
        assert get_asset_rules("stock")["stamp_tax"] is True

    def test_stock_price_limits(self):
        rules = get_asset_rules("stock")
        assert rules["price_limits"]["main"] == 0.10  # 60/00
        assert rules["price_limits"]["gem"] == 0.20  # 30
        assert rules["price_limits"]["star"] == 0.20  # 68
        assert rules["price_limits"]["bse"] == 0.30  # 8

    def test_stock_st_limit(self):
        assert get_asset_rules("stock")["st_limit"] == 0.05

    def test_etf_st_limit_none(self):
        assert get_asset_rules("etf")["st_limit"] is None

    def test_t_plus_lot_size_short(self):
        for asset in ("stock", "etf"):
            rules = get_asset_rules(asset)
            assert rules["t_plus"] == 1
            assert rules["lot_size"] == 100
            assert rules["short"] is False


class TestStampTaxExemptPrefixes:
    def test_exact_five(self):
        assert STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES == ("51", "56", "58", "15", "16")

    def test_five_members(self):
        assert len(STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES) == 5

    def test_shanghai_and_shenzhen(self):
        assert "51" in STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES  # Shanghai ETF
        assert "56" in STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES  # Shanghai ETF new segment
        assert "58" in STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES  # Shanghai STAR ETF
        assert "15" in STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES  # Shenzhen ETF
        assert "16" in STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES  # Shenzhen LOF/ETF


class TestIsStampTaxExemptListedFund:
    def test_etf_prefixes(self):
        assert is_stamp_tax_exempt_listed_fund("510300") is True
        assert is_stamp_tax_exempt_listed_fund("560000") is True
        assert is_stamp_tax_exempt_listed_fund("588000") is True
        assert is_stamp_tax_exempt_listed_fund("159915") is True
        assert is_stamp_tax_exempt_listed_fund("160119") is True  # Shenzhen LOF

    def test_stock_not_exempt(self):
        assert is_stamp_tax_exempt_listed_fund("600000") is False
        assert is_stamp_tax_exempt_listed_fund("000001") is False
        assert is_stamp_tax_exempt_listed_fund("300750") is False
        assert is_stamp_tax_exempt_listed_fund("688001") is False


class TestAssetRulesKeys:
    def test_keys_match_asset_type_values(self):
        expected = {AssetType.STOCK.value, AssetType.ETF.value}
        assert set(ASSET_RULES.keys()) == expected
