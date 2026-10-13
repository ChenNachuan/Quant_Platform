"""Tests for core.asset_rules — lookup table, prefix table, validators."""

import pytest

from core.asset_rules import (
    ASSET_PREFIXES,
    ASSET_RULES,
    CALENDAR_EXCHANGES,
    EXCHANGE_MAP,
    STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES,
    get_asset_rules,
    is_stamp_tax_exempt_listed_fund,
    validate_equity_ticker,
    validate_futures_ticker,
)
from core.types import AssetType


class TestGetAssetRules:
    def test_unknown_raises_key_error(self):
        with pytest.raises(KeyError):
            get_asset_rules("unknown")

    def test_etf_stamp_tax_false(self):
        assert get_asset_rules("etf")["stamp_tax"] is False

    def test_fund_stamp_tax_false(self):
        assert get_asset_rules("fund")["stamp_tax"] is False

    def test_stock_stamp_tax_true(self):
        assert get_asset_rules("stock")["stamp_tax"] is True

    def test_stock_price_limits(self):
        rules = get_asset_rules("stock")
        assert rules["price_limits"]["main"] == 0.10  # 60/00
        assert rules["price_limits"]["gem"] == 0.20  # 30
        assert rules["price_limits"]["star"] == 0.20  # 68
        assert rules["price_limits"]["bse"] == 0.30  # 8

    def test_fund_price_limits(self):
        rules = get_asset_rules("fund")
        assert rules["price_limits"]["main"] == 0.10  # LOF like stock, +/-10%

    def test_stock_st_limit(self):
        assert get_asset_rules("stock")["st_limit"] == 0.05

    def test_etf_st_limit_none(self):
        assert get_asset_rules("etf")["st_limit"] is None

    def test_fund_st_limit_none(self):
        assert get_asset_rules("fund")["st_limit"] is None

    def test_t_plus_lot_size_short(self):
        for asset in ("stock", "etf", "fund"):
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
        expected = {AssetType.STOCK.value, AssetType.ETF.value, AssetType.FUND.value}
        assert set(ASSET_RULES.keys()) == expected


class TestValidateEquityTicker:
    # Golden-sample ETFs (AGENTS.md §6) + representative stocks
    def test_golden_sample_tickers_pass(self):
        for t in ("510300.SH", "510500.SH", "588000.SH", "511010.SH", "159915.SZ"):
            assert validate_equity_ticker(t), f"{t} should pass"

    def test_shanghai_lof_passes_as_fund(self):
        assert validate_equity_ticker("501050.SH") is True
        assert ASSET_PREFIXES["50"] is AssetType.FUND

    def test_invalid_tickers_fail(self):
        assert validate_equity_ticker("999999.SH") is False  # unknown prefix
        assert validate_equity_ticker("510300.XX") is False  # unknown exchange
        assert validate_equity_ticker("510300") is False  # missing exchange

    def test_various_stocks_pass(self):
        for t in ("600000.SH", "000001.SZ", "300750.SZ", "688001.SH", "830799.BJ"):
            assert validate_equity_ticker(t), f"{t} should pass"


class TestValidateFuturesTicker:
    def test_futures_shapes_pass(self):
        for t in ("IF2406.CFFEX", "SC2412.INE", "AU2412.SHFE", "T2406.CFFEX"):
            assert validate_futures_ticker(t), f"{t} should pass"

    def test_futures_not_in_equity_prefixes(self):
        # Futures product codes are NOT two-digit numeric prefixes
        assert "IF" not in ASSET_PREFIXES
        assert "SC" not in ASSET_PREFIXES
        assert "AU" not in ASSET_PREFIXES

    def test_invalid_futures_fail(self):
        assert validate_futures_ticker("510300.SH") is False
        assert validate_futures_ticker("IF2406.XX") is False


class TestExchangeMap:
    def test_ine_maps_to_ine(self):
        assert EXCHANGE_MAP["INE"] == "ine"

    def test_nine_keys(self):
        assert len(EXCHANGE_MAP) == 9

    def test_calendar_exchanges(self):
        assert CALENDAR_EXCHANGES == set(EXCHANGE_MAP.values())

    def test_all_values_are_lowercase(self):
        for v in EXCHANGE_MAP.values():
            assert v == v.lower()


class TestAssetPrefixes:
    def test_b_share_prefixes_absent(self):
        # 90/20 deliberately excluded (V1 scope is stock + ETF)
        assert "90" not in ASSET_PREFIXES
        assert "20" not in ASSET_PREFIXES

    def test_fund_prefixes(self):
        assert ASSET_PREFIXES["50"] is AssetType.FUND
        assert ASSET_PREFIXES["16"] is AssetType.FUND

    def test_etf_prefixes(self):
        for p in ("51", "56", "58", "15"):
            assert ASSET_PREFIXES[p] is AssetType.ETF
