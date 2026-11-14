"""Tests for data.storage_schema — 52-table pandera contract.

Validates that every canonical schema (47 A-share + 4 futures + 1 factor)
accepts well-formed data and rejects wrong column names, wrong types, wrong
nullability, and cross-domain tickers.  Also checks legacy short alias
resolution, directory isolation (no shared ``cn_reference/``), quality_flag
bit definitions, and calendar exchanges.
"""

from datetime import date, datetime

import pandas as pd
import pandera.pandas as pa
import pytest

from core.asset_rules import CALENDAR_EXCHANGES, EXCHANGE_MAP
from core.types import QualityFlag
from data.storage_schema import (
    A_SHARE_TABLES,
    CLEAN,
    LEGACY_TO_CANONICAL,
    PARTITION_PATHS,
    QUALITY_FLAG_BITS,
    QUALITY_FLAG_MAX,
    SCHEMA,
    TABLE_ALIASES,
    TABLE_FAMILY,
    FactorViewRegistry,
    factor_schema,
    factor_view_name,
    resolve_table_name,
)

EXPECTED_TABLES = {
    # Calendar (1)
    "cn_calendar",
    # Stock Core & Status (6)
    "cn_stock_basic",
    "cn_stock_bars_daily",
    "cn_stock_bars_minute",
    "cn_stock_fundamental",
    "cn_stock_daily_status",
    "cn_stock_corporate_actions",
    # Stock Index / Industry / Concept (4)
    "cn_stock_index_basic",
    "cn_stock_index_bars_daily",
    "cn_stock_industry",
    "cn_stock_index_weights",
    # Stock Fundamental PIT (7)
    "cn_stock_fundamental_income",
    "cn_stock_fundamental_balance",
    "cn_stock_fundamental_cashflow",
    "cn_stock_fundamental_indicator",
    "cn_stock_fundamental_segment",
    "cn_stock_fundamental_earnings",
    "cn_stock_fundamental_consensus",
    # Stock Special Flows / Sentiment / Chips (9)
    "cn_stock_auction",
    "cn_stock_moneyflow",
    "cn_stock_hsgt_market_flow",
    "cn_stock_hsgt_hold",
    "cn_stock_margin",
    "cn_stock_lhb",
    "cn_stock_lhb_seats",
    "cn_stock_limit_pool",
    "cn_stock_chips_perf",
    # Stock Holders & Capital Events (8)
    "cn_stock_holder_num",
    "cn_stock_top_holders",
    "cn_stock_restricted_release",
    "cn_stock_block_trade",
    "cn_stock_repurchase",
    "cn_stock_holder_trade",
    "cn_stock_pledge",
    "cn_stock_research_visits",
    # Stock Tick & L2 (2)
    "cn_stock_tick",
    "cn_stock_tick_l2",
    # Independent ETF Domain (5)
    "cn_etf_basic",
    "cn_etf_bars_daily",
    "cn_etf_bars_minute",
    "cn_etf_fundamental",
    "cn_etf_tick",
    # OTC & Listed Mutual Funds (5)
    "cn_fund_basic",
    "cn_fund_managers",
    "cn_fund_nav",
    "cn_fund_dividend",
    "cn_fund_portfolio",
    # Futures (4)
    "cn_futures_bars_daily",
    "cn_futures_tick_l2",
    "cn_futures_contract_specs",
    "cn_futures_continuous",
    # Factors (1)
    "factor",
}

D = date(2024, 1, 15)
D_PREV = date(2024, 1, 10)
D_END = date(2023, 12, 31)
DT = datetime(2024, 1, 15, 9, 31, 0)


# ---------------------------------------------------------------------------
# Helpers — one valid sample per canonical table
# ---------------------------------------------------------------------------
def _valid_row(table: str) -> dict:
    """Return a single valid row as a dict for *table* (canonical or legacy)."""
    canonical = resolve_table_name(table)
    base = {
        "cn_calendar": {
            "trade_date": D,
        },
        "cn_stock_basic": {
            "ticker": "600519.SH",
            "name": "贵州茅台",
            "exchange": "SH",
            "market": "主板",
            "list_status": "L",
            "list_date": date(2001, 8, 27),
            "delist_date": None,
            "is_hs": "H",
            "old_code": None,
        },
        "cn_stock_bars_daily": {
            "ticker": "600519.SH",
            "trade_date": D,
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 103.0,
            "volume": 1_000_000,
            "amount": 1.03e8,
            "adjfactor": 1.0,
            "turnover": 0.5,
            "suspend": False,
            "is_limit_up": False,
            "is_limit_down": False,
            "quality_flag": 0,
        },
        "cn_stock_bars_minute": {
            "ticker": "600519.SH",
            "trade_date": D,
            "datetime": DT,
            "open": 100.0,
            "high": 101.0,
            "low": 99.5,
            "close": 100.5,
            "volume": 50_000,
            "amount": 5.025e6,
            "quality_flag": 0,
        },
        "cn_stock_fundamental": {
            "ticker": "600519.SH",
            "trade_date": D,
            "total_market_cap": 2.0e12,
            "float_market_cap": 1.5e12,
            "pe_ttm": 25.0,
            "pe_lyr": 24.0,
            "pb": 8.0,
            "ps_ttm": 12.0,
            "total_shares": 1_256_000_000,
            "float_shares": 1_256_000_000,
            "quality_flag": 0,
        },
        "cn_stock_daily_status": {
            "ticker": "600519.SH",
            "trade_date": D,
            "pre_close": 100.0,
            "up_limit": 110.0,
            "down_limit": 90.0,
            "limit_pct": 0.10,
            "is_st": False,
            "is_suspend": False,
            "is_limit_up": False,
            "is_limit_down": False,
            "touched_limit_up": False,
            "touched_limit_down": False,
            "is_xr": False,
            "quality_flag": 0,
        },
        "cn_stock_corporate_actions": {
            "ticker": "600519.SH",
            "ex_date": D,
            "event_type": "dividend",
            "announce_date": D_PREV,
            "record_date": date(2024, 1, 14),
            "detail": "10派2.00",
            "cash_dividend": 0.2,
            "stock_dividend": float("nan"),
            "rights_price": float("nan"),
            "rights_ratio": float("nan"),
        },
        "cn_stock_index_basic": {
            "ticker": "000300.SH",
            "name": "沪深300",
            "category": "综合指数",
            "publisher": "CSI",
            "base_date": date(2004, 12, 31),
            "base_point": 1000.0,
            "list_date": date(2005, 4, 8),
        },
        "cn_stock_index_bars_daily": {
            "ticker": "000300.SH",
            "trade_date": D,
            "open": 3300.0,
            "high": 3320.0,
            "low": 3280.0,
            "close": 3310.0,
            "volume": 100_000_000,
            "amount": 2.0e11,
            "pe": 11.5,
            "pb": 1.25,
            "turnover": 0.8,
            "total_market_cap": 5.0e13,
            "float_market_cap": 2.5e13,
            "quality_flag": 0,
        },
        "cn_stock_industry": {
            "ticker": "600519.SH",
            "industry_l1": "食品饮料",
            "industry_l2": "白酒",
            "industry_l3": "高端白酒",
            "classification": "citics",
            "effective_date": D,
        },
        "cn_stock_index_weights": {
            "index_code": "000300.SH",
            "ticker": "600519.SH",
            "trade_date": D,
            "weight": 5.2,
            "in_date": date(2005, 4, 8),
            "out_date": None,
        },
        "cn_stock_fundamental_income": {
            "ticker": "600519.SH",
            "report_date": D_END,
            "ann_date": D,
            "f_ann_date": D,
            "statement_type": "1",
            "comp_type": "1",
            "basic_eps": 59.49,
            "diluted_eps": 59.49,
            "total_revenue": 1.5e11,
            "revenue": 1.48e11,
            "total_cogs": 1.2e10,
            "operate_profit": 1.0e11,
            "total_profit": 1.0e11,
            "net_profit": 7.5e10,
            "net_profit_attr_p": 7.4e10,
            "net_profit_dedt": 7.3e10,
            "rd_exp": 1.5e8,
            "ebit": 1.0e11,
            "ebitda": 1.02e11,
        },
        "cn_stock_fundamental_balance": {
            "ticker": "600519.SH",
            "report_date": D_END,
            "ann_date": D,
            "f_ann_date": D,
            "statement_type": "1",
            "comp_type": "1",
            "total_assets": 2.7e11,
            "total_cur_assets": 2.2e11,
            "total_nca": 5.0e10,
            "money_cap": 1.6e11,
            "accounts_receiv": 1.0e8,
            "inventories": 4.6e10,
            "total_liab": 5.0e10,
            "total_cur_liab": 4.9e10,
            "total_ncl": 1.0e9,
            "total_hldr_eqy_exc_min_int": 2.1e11,
            "total_hldr_eqy_inc_min_int": 2.2e11,
            "total_share": 1.256e9,
        },
        "cn_stock_fundamental_cashflow": {
            "ticker": "600519.SH",
            "report_date": D_END,
            "ann_date": D,
            "f_ann_date": D,
            "statement_type": "1",
            "comp_type": "1",
            "n_cashflow_act": 6.6e10,
            "n_cashflow_inv_act": -8.0e9,
            "n_cash_flows_fnc_act": -5.0e10,
            "c_fr_sale_sg": 1.6e11,
            "c_paid_to_for_empl": 1.3e10,
            "c_pay_acq_const_fiolta": 4.0e9,
            "free_cashflow": 6.2e10,
            "n_incr_cash_cash_equ": 8.0e9,
        },
        "cn_stock_fundamental_indicator": {
            "ticker": "600519.SH",
            "report_date": D_END,
            "ann_date": D,
            "roe": 34.0,
            "roe_waa": 35.0,
            "roe_dt": 33.8,
            "roa": 27.5,
            "roic": 34.2,
            "grossprofit_margin": 91.9,
            "netprofit_margin": 52.0,
            "debt_to_assets": 18.5,
            "current_ratio": 4.5,
            "quick_ratio": 3.5,
            "assets_turn": 0.56,
            "netprofit_yoy": 19.1,
            "or_yoy": 18.0,
            "audit_result": "标准无保留意见",
        },
        "cn_stock_fundamental_segment": {
            "ticker": "600519.SH",
            "report_date": D_END,
            "ann_date": D,
            "segment_type": "product",
            "segment_name": "茅台酒",
            "bz_sales": 1.26e11,
            "bz_profit": 1.18e11,
            "bz_cost": 8.0e9,
            "sales_ratio": 85.0,
            "gross_margin": 94.1,
        },
        "cn_stock_fundamental_earnings": {
            "ticker": "600519.SH",
            "report_date": D_END,
            "ann_date": D,
            "event_kind": "express",
            "forecast_type": "预增",
            "p_change_min": 15.0,
            "p_change_max": 20.0,
            "net_profit_min": 7.2e10,
            "net_profit_max": 7.6e10,
            "revenue": 1.5e11,
            "net_profit": 7.47e10,
            "eps": 59.49,
            "roe": 34.0,
            "pre_date": date(2024, 4, 3),
            "actual_date": date(2024, 4, 3),
        },
        "cn_stock_fundamental_consensus": {
            "ticker": "600519.SH",
            "trade_date": D,
            "visible_from": D,
            "eps_fy0": 59.5,
            "eps_fy1": 68.0,
            "eps_fy2": 78.0,
            "eps_fwd12m": 69.0,
            "eps_fy1_n": 32.0,
            "eps_fy1_std": 1.2,
            "eps_fy1_up_3m": 5.0,
            "eps_fy1_down_3m": 1.0,
            "rating_score": 4.8,
            "tp_median": 2100.0,
            "quality_flag": 0,
        },
        "cn_stock_auction": {
            "ticker": "600519.SH",
            "trade_date": D,
            "auction_session": "open",
            "price": 1650.0,
            "pre_close": 1648.0,
            "pct_chg": 0.12,
            "volume": 50_000,
            "amount": 8.25e7,
            "turnover_rate": 0.004,
            "volume_ratio": 1.2,
            "quality_flag": 0,
        },
        "cn_stock_moneyflow": {
            "ticker": "600519.SH",
            "trade_date": D,
            "buy_elg_amount": 5.0e8,
            "sell_elg_amount": 3.0e8,
            "buy_lg_amount": 4.0e8,
            "sell_lg_amount": 3.5e8,
            "buy_md_amount": 2.0e8,
            "sell_md_amount": 2.5e8,
            "buy_sm_amount": 1.0e8,
            "sell_sm_amount": 1.5e8,
            "net_mf_amount": 1.5e8,
            "net_mf_pct": 5.2,
            "quality_flag": 0,
        },
        "cn_stock_hsgt_market_flow": {
            "trade_date": D,
            "ggt_ss": 12.5,
            "ggt_sz": 10.2,
            "hgt": 15.0,
            "sgt": 12.0,
            "north_money": 27.0,
            "south_money": 22.7,
            "is_turnover_regime": False,
        },
        "cn_stock_hsgt_hold": {
            "ticker": "600519.SH",
            "trade_date": D,
            "vol": 85_000_000,
            "ratio": 6.76,
            "exchange": "SH",
            "quality_flag": 0,
        },
        "cn_stock_margin": {
            "ticker": "600519.SH",
            "trade_date": D,
            "rzye": 1.8e10,
            "rzmre": 5.0e8,
            "rzche": 4.8e8,
            "rqyl": 100_000.0,
            "rqmcl": 5_000.0,
            "rqchl": 4_000.0,
            "rqye": 1.6e8,
            "rzrqye": 1.816e10,
            "quality_flag": 0,
        },
        "cn_stock_lhb": {
            "ticker": "000001.SZ",
            "trade_date": D,
            "reason": "日涨幅偏离值达7%",
            "close": 12.5,
            "pct_chg": 10.02,
            "turnover_rate": 8.5,
            "l_buy": 2.5e8,
            "l_sell": 1.2e8,
            "net_amount": 1.3e8,
            "net_rate": 12.5,
            "quality_flag": 0,
        },
        "cn_stock_lhb_seats": {
            "ticker": "000001.SZ",
            "trade_date": D,
            "side": "buy",
            "rank": 1,
            "seat_name": "机构专用",
            "buy": 8.0e7,
            "sell": 0.0,
            "net_buy": 8.0e7,
            "hm_name": None,
            "is_institution": True,
        },
        "cn_stock_limit_pool": {
            "ticker": "000001.SZ",
            "trade_date": D,
            "pool_type": "up",
            "first_time": "09:35:00",
            "last_time": "14:50:00",
            "open_times": 1.0,
            "streak_days": 2.0,
            "seal_amount": 1.2e8,
            "theme": "银行",
            "quality_flag": 0,
        },
        "cn_stock_chips_perf": {
            "ticker": "600519.SH",
            "trade_date": D,
            "his_low": 1500.0,
            "his_high": 1800.0,
            "cost_5pct": 1550.0,
            "cost_15pct": 1580.0,
            "cost_50pct": 1640.0,
            "cost_85pct": 1700.0,
            "cost_95pct": 1740.0,
            "weight_avg": 1645.0,
            "winner_rate": 62.5,
            "quality_flag": 0,
        },
        "cn_stock_holder_num": {
            "ticker": "600519.SH",
            "ann_date": D,
            "end_date": D_END,
            "holder_num": 160_000,
            "holder_num_chg_pct": -1.5,
            "avg_hold_shares": 7850.0,
        },
        "cn_stock_top_holders": {
            "ticker": "600519.SH",
            "ann_date": D,
            "end_date": D_END,
            "holder_scope": "top10",
            "rank": 1,
            "holder_name": "中国贵州茅台酒厂(集团)有限责任公司",
            "holder_type": "国有法人",
            "hold_amount": 6.78e8,
            "hold_ratio": 54.0,
            "hold_change": 0.0,
        },
        "cn_stock_restricted_release": {
            "ticker": "688001.SH",
            "ann_date": D_PREV,
            "float_date": D,
            "holder_name": "战略配售投资者",
            "float_share": 1.0e7,
            "float_ratio": 2.5,
            "share_type": "首发限售股",
            "is_announced": True,
        },
        "cn_stock_block_trade": {
            "ticker": "600519.SH",
            "trade_date": D,
            "seq": 1,
            "price": 1600.0,
            "vol": 10_000,
            "amount": 1.6e7,
            "discount_rate": -0.03,
            "buyer": "机构专用",
            "seller": "中信证券北京总部",
        },
        "cn_stock_repurchase": {
            "ticker": "600519.SH",
            "ann_date": D,
            "end_date": D_END,
            "proc": "实施",
            "exp_date": date(2024, 12, 31),
            "vol": 1.0e6,
            "amount": 1.6e9,
            "high_limit": 1700.0,
            "low_limit": 1400.0,
        },
        "cn_stock_holder_trade": {
            "ticker": "000001.SZ",
            "ann_date": D,
            "holder_name": "某控股股东",
            "holder_type": "C",
            "in_de": "IN",
            "change_vol": 1.0e6,
            "change_ratio": 0.05,
            "after_share": 5.0e8,
            "avg_price": 11.2,
            "begin_date": D_PREV,
            "close_date": D,
        },
        "cn_stock_pledge": {
            "ticker": "000001.SZ",
            "ann_date": D,
            "holder_name": "某股东",
            "pledge_amount": 5.0e6,
            "start_date": D_PREV,
            "end_date": None,
            "is_release": False,
            "pledge_ratio": 10.0,
            "freeze_type": "质押",
        },
        "cn_stock_research_visits": {
            "ticker": "300750.SZ",
            "surv_date": D,
            "rece_place": "公司会议室",
            "rece_mode": "现场参观",
            "org_name": "易方达基金",
            "org_type": "基金公司",
            "fund_visitors": "张三",
        },
        "cn_stock_tick": {
            "ticker": "600519.SH",
            "trade_date": D,
            "datetime": DT,
            "seq": 1,
            "price": 1650.0,
            "volume": 200,
            "amount": 330000.0,
            "bs_flag": "B",
            "quality_flag": 0,
        },
        "cn_stock_tick_l2": {
            "ticker": "600519.SH",
            "trade_date": D,
            "datetime": DT,
            "seq": 1,
            "last_price": 1650.0,
            "volume": 200,
            "amount": 330000.0,
            "num_trades": 5.0,
            "quality_flag": 0,
        },
        "cn_etf_basic": {
            "ticker": "510300.SH",
            "name": "沪深300ETF",
            "exchange": "SH",
            "management": "华泰柏瑞基金",
            "index_code": "000300.SH",
            "index_name": "沪深300",
            "etf_type": "股票型",
            "setup_date": date(2012, 5, 4),
            "list_date": date(2012, 5, 28),
            "delist_date": None,
            "m_fee": 0.50,
            "c_fee": 0.10,
        },
        "cn_etf_bars_daily": {
            "ticker": "510300.SH",
            "trade_date": D,
            "open": 3.45,
            "high": 3.48,
            "low": 3.42,
            "close": 3.46,
            "volume": 500_000_000,
            "amount": 1.73e9,
            "adjfactor": 1.0,
            "turnover": 1.2,
            "suspend": False,
            "is_limit_up": False,
            "is_limit_down": False,
            "quality_flag": 0,
        },
        "cn_etf_bars_minute": {
            "ticker": "510300.SH",
            "trade_date": D,
            "datetime": DT,
            "open": 3.45,
            "high": 3.46,
            "low": 3.44,
            "close": 3.455,
            "volume": 2_000_000,
            "amount": 6.91e6,
            "quality_flag": 0,
        },
        "cn_etf_fundamental": {
            "ticker": "510300.SH",
            "trade_date": D,
            "unit_nav": 3.461,
            "accum_nav": 3.890,
            "nav_adj_factor": 1.124,
            "iopv": 3.460,
            "total_shares": 4.0e10,
            "total_nav": 1.38e11,
            "quality_flag": 0,
        },
        "cn_etf_tick": {
            "ticker": "510300.SH",
            "trade_date": D,
            "datetime": DT,
            "seq": 1,
            "price": 3.455,
            "volume": 10_000,
            "amount": 34550.0,
            "bs_flag": "B",
            "quality_flag": 0,
        },
        "cn_fund_basic": {
            "ticker": "110011.OF",
            "name": "易方达中小盘混合",
            "market": "O",
            "management": "易方达基金",
            "custodian": "中国银行",
            "fund_type": "契约型开放式",
            "invest_type": "混合型",
            "found_date": date(2008, 6, 19),
            "list_date": None,
            "delist_date": None,
            "m_fee": 1.20,
            "c_fee": 0.20,
            "benchmark": "天相中盘指数*45%+天相小盘指数*35%+中债总指数*20%",
        },
        "cn_fund_managers": {
            "ticker": "110011.OF",
            "ann_date": date(2012, 9, 28),
            "name": "张坤",
            "gender": "M",
            "edu": "硕士",
            "begin_date": date(2012, 9, 28),
            "end_date": None,
        },
        "cn_fund_nav": {
            "ticker": "110011.OF",
            "nav_date": D,
            "ann_date": D,
            "unit_nav": 5.20,
            "accum_nav": 7.10,
            "adj_nav": 8.40,
            "nav_adj_factor": 1.615,
            "total_shares": 5.0e9,
            "total_netasset": 2.6e10,
            "quality_flag": 0,
        },
        "cn_fund_dividend": {
            "ticker": "110011.OF",
            "ann_date": D_PREV,
            "record_date": date(2024, 1, 14),
            "ex_date": D,
            "pay_date": date(2024, 1, 18),
            "div_cash": 0.25,
            "div_proc": "实施",
        },
        "cn_fund_portfolio": {
            "ticker": "110011.OF",
            "ann_date": D,
            "end_date": D_END,
            "symbol": "600519.SH",
            "asset_class": "stock",
            "mkv": 2.5e9,
            "amount": 1.5e6,
            "stk_mkv_ratio": 9.6,
            "stk_float_ratio": 0.12,
            "rank": 1.0,
        },
        "cn_futures_bars_daily": {
            "ticker": "IF2601.CFFEX",
            "trade_date": D,
            "open": 3500.0,
            "high": 3550.0,
            "low": 3480.0,
            "close": 3520.0,
            "settle": 3515.0,
            "volume": 120_000,
            "amount": 1.26e11,
            "open_interest": 80_000,
            "quality_flag": 0,
        },
        "cn_futures_tick_l2": {
            "ticker": "IF2601.CFFEX",
            "trade_date": D,
            "datetime": DT,
            "last_price": 3520.0,
            "volume": 120_000,
            "amount": 1.26e11,
            "open_interest": 80_000,
            "quality_flag": 0,
        },
        "cn_futures_contract_specs": {
            "ticker": "IF2601.CFFEX",
            "symbol": "IF",
            "exchange": "CFFEX",
            "contract_multiplier": 300.0,
            "price_tick": 0.2,
            "list_date": D,
            "expire_date": date(2026, 1, 16),
            "delivery_month": "202601",
            "price_limit_pct": 0.10,
            "margin_rate": 0.12,
            "trading_hours": "09:30-11:30,13:00-15:00",
            "is_active": True,
        },
        "cn_futures_continuous": {
            "trade_date": D,
            "dominant_contract": "IF2601.CFFEX",
            "open": 3500.0,
            "high": 3550.0,
            "low": 3480.0,
            "close": 3520.0,
            "volume": 120_000,
            "amount": 1.26e11,
            "open_interest": 80_000,
            "settle": 3515.0,
            "adjfactor": 1.0,
        },
        "factor": {
            "ticker": "600519.SH",
            "date": D,
            "value": 0.05,
        },
    }
    row = base[canonical].copy()
    if canonical == "cn_futures_tick_l2":
        for i in range(1, 6):
            row[f"bid_price_{i}"] = 3520.0 - i * 0.2
            row[f"bid_volume_{i}"] = 10 * i
            row[f"ask_price_{i}"] = 3520.0 + i * 0.2
            row[f"ask_volume_{i}"] = 10 * i
    elif canonical == "cn_stock_tick_l2":
        for i in range(1, 11):
            row[f"bid_price_{i}"] = 1650.0 - i * 0.01
            row[f"bid_volume_{i}"] = 100 * i
            row[f"ask_price_{i}"] = 1650.0 + i * 0.01
            row[f"ask_volume_{i}"] = 100 * i
    return row


def _valid_df(table: str) -> pd.DataFrame:
    return pd.DataFrame([_valid_row(table)])


# ---------------------------------------------------------------------------
# Table set completeness (52 canonical tables = 47 A-share + 4 futures + 1 factor)
# ---------------------------------------------------------------------------
class TestSchemaRegistry:
    def test_exactly_fifty_two_canonical_tables(self):
        assert set(SCHEMA) == EXPECTED_TABLES
        assert len(SCHEMA) == 52

    def test_exactly_forty_seven_a_share_tables(self):
        assert len(A_SHARE_TABLES) == 47
        assert all(
            not t.startswith("cn_futures_") and t != "factor" for t in A_SHARE_TABLES
        )

    def test_all_values_are_dataframe_schemas(self):
        for name, schema in SCHEMA.items():
            assert isinstance(schema, pa.DataFrameSchema), name

    def test_partition_paths_cover_all_tables(self):
        assert set(PARTITION_PATHS) == EXPECTED_TABLES
        assert len(PARTITION_PATHS) == 52

    def test_table_family_covers_all_tables(self):
        assert set(TABLE_FAMILY) == EXPECTED_TABLES
        assert len(TABLE_FAMILY) == 52

    def test_no_ext_or_shared_cn_reference_dirs(self):
        for table, path in PARTITION_PATHS.items():
            assert not path.startswith("ext/"), (table, path)
            assert "cn_reference/" not in path, (table, path)
            if table != "factor":
                assert path.startswith(f"asset/{table}/"), (table, path)

    def test_legacy_aliases_transparently_resolve(self):
        assert len(LEGACY_TO_CANONICAL) == 10
        for legacy, canonical in LEGACY_TO_CANONICAL.items():
            assert legacy in SCHEMA
            assert legacy in PARTITION_PATHS
            assert legacy in TABLE_FAMILY
            assert SCHEMA[legacy] is SCHEMA[canonical]
            assert PARTITION_PATHS[legacy] == PARTITION_PATHS[canonical]
            assert TABLE_FAMILY[legacy] == TABLE_FAMILY[canonical]
            assert resolve_table_name(legacy) == canonical
            assert TABLE_ALIASES[legacy] == canonical
            assert TABLE_ALIASES[canonical] == legacy

    def test_factor_has_no_quality_flag(self):
        assert "quality_flag" not in SCHEMA["factor"].columns

    def test_calendar_is_single_column(self):
        assert set(SCHEMA["calendar"].columns) == {"trade_date"}
        assert set(SCHEMA["cn_calendar"].columns) == {"trade_date"}


# ---------------------------------------------------------------------------
# All 52 schemas accept well-formed data
# ---------------------------------------------------------------------------
class TestValidDataAccepted:
    @pytest.mark.parametrize("table", sorted(EXPECTED_TABLES))
    def test_valid_row_accepted(self, table):
        df = _valid_df(table)
        result = SCHEMA[table].validate(df)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Domain-specific ticker isolation
# ---------------------------------------------------------------------------
class TestDomainTickerIsolation:
    def test_stock_table_rejects_etf_ticker(self):
        df = _valid_df("cn_stock_daily_status")
        df["ticker"] = "510300.SH"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["cn_stock_daily_status"].validate(df)

    def test_etf_table_rejects_stock_and_lof_tickers(self):
        for bad_ticker in ("600519.SH", "501050.SH", "160119.SZ"):
            df = _valid_df("cn_etf_bars_daily")
            df["ticker"] = bad_ticker
            with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
                SCHEMA["cn_etf_bars_daily"].validate(df)

    def test_etf_table_accepts_new_sh_etf_prefixes(self):
        for etf_ticker in ("520100.SH", "530100.SH", "550100.SH", "159915.SZ"):
            df = _valid_df("cn_etf_bars_daily")
            df["ticker"] = etf_ticker
            assert len(SCHEMA["cn_etf_bars_daily"].validate(df)) == 1

    def test_fund_table_accepts_otc_and_lof_rejects_etf_and_stock(self):
        for good in ("110011.OF", "501050.SH", "160119.SZ"):
            df = _valid_df("cn_fund_nav")
            df["ticker"] = good
            assert len(SCHEMA["cn_fund_nav"].validate(df)) == 1
        for bad in ("510300.SH", "600519.SH"):
            df = _valid_df("cn_fund_nav")
            df["ticker"] = bad
            with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
                SCHEMA["cn_fund_nav"].validate(df)

    def test_stock_bse_92_ticker_accepted(self):
        df = _valid_df("cn_stock_basic")
        df["ticker"] = "920001.BJ"
        df["exchange"] = "BJ"
        assert len(SCHEMA["cn_stock_basic"].validate(df)) == 1


# ---------------------------------------------------------------------------
# Rejection: wrong column name
# ---------------------------------------------------------------------------
class TestWrongColumnNameRejected:
    @pytest.mark.parametrize("table", sorted(EXPECTED_TABLES))
    def test_wrong_column_name_rejected(self, table):
        df = _valid_df(table)
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "bogus_column"})
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA[table].validate(df)

    def test_missing_column_rejected(self):
        df = _valid_df("bars_daily").drop(columns=["adjfactor"])
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_extra_column_rejected_strict(self):
        df = _valid_df("bars_daily")
        df["extra"] = 1
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)


# ---------------------------------------------------------------------------
# Rejection: wrong type
# ---------------------------------------------------------------------------
class TestWrongTypeRejected:
    def test_string_for_float_rejected(self):
        df = _valid_df("bars_daily")
        df["open"] = "not_a_number"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_string_for_int_rejected(self):
        df = _valid_df("bars_daily")
        df["volume"] = "not_an_int"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_unparseable_string_for_date_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = "garbage_not_a_date"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError, ValueError)):
            SCHEMA["calendar"].validate(df)

    def test_list_for_scalar_rejected(self):
        df = _valid_df("bars_daily")
        df["amount"] = [[1.0, 2.0]]
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_non_integral_float_for_int_rejected(self):
        """Regression: coerce=True used to silently truncate 10.5 -> 10."""
        df = _valid_df("bars_daily")
        df["volume"] = 10.5
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_non_integral_float_quality_flag_rejected(self):
        """Regression: coerce=True used to silently truncate 31.9 -> 31."""
        df = _valid_df("bars_daily")
        df["quality_flag"] = 31.9
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_integral_float_for_int_rejected(self):
        """Float dtype in int column is rejected even when value is integral."""
        df = _valid_df("bars_daily")
        df["volume"] = 10.0
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)


# ---------------------------------------------------------------------------
# Rejection: wrong nullability
# ---------------------------------------------------------------------------
class TestWrongNullabilityRejected:
    def test_null_in_non_nullable_rejected(self):
        df = _valid_df("bars_daily")
        df["close"] = None
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_null_in_non_nullable_date_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = None
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["calendar"].validate(df)

    def test_null_in_nullable_accepted(self):
        df = _valid_df("bars_daily")
        df["turnover"] = float("nan")
        result = SCHEMA["bars_daily"].validate(df)
        assert pd.isna(result["turnover"].iloc[0])

    def test_null_in_nullable_pe_accepted(self):
        df = _valid_df("fundamental")
        df["pe_ttm"] = float("nan")
        result = SCHEMA["fundamental"].validate(df)
        assert pd.isna(result["pe_ttm"].iloc[0])


# ---------------------------------------------------------------------------
# quality_flag contract
# ---------------------------------------------------------------------------
class TestQualityFlag:
    def test_bit_definitions_match_quality_flag_enum(self):
        expected = {
            "C02_COVERAGE": QualityFlag.C02_COVERAGE,
            "C05_NULL_RATE": QualityFlag.C05_NULL_RATE,
            "L04_BEYOND_LIMIT": QualityFlag.L04_BEYOND_LIMIT,
            "L07_ADJFACTOR_RANGE": QualityFlag.L07_ADJFACTOR_RANGE,
            "K01_OVERNIGHT_GAP": QualityFlag.K01_OVERNIGHT_GAP,
        }
        assert set(QUALITY_FLAG_BITS) == set(expected)
        for name, bit in expected.items():
            assert QUALITY_FLAG_BITS[name] == int(bit), name

    def test_clean_is_zero(self):
        assert CLEAN == 0
        assert CLEAN == int(QualityFlag.CLEAN)

    def test_five_bits_max_value(self):
        assert QUALITY_FLAG_MAX == 31

    def test_quality_flag_is_int64(self):
        df = _valid_df("bars_daily")
        result = SCHEMA["bars_daily"].validate(df)
        assert result["quality_flag"].dtype.kind in "iu"

    def test_quality_flag_non_negative_enforced(self):
        df = _valid_df("bars_daily")
        df["quality_flag"] = -1
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_quality_flag_within_five_bits_enforced(self):
        df = _valid_df("bars_daily")
        df["quality_flag"] = 32
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_quality_flag_boundary_values_accepted(self):
        for value in (0, 1, 15, 31):
            df = _valid_df("bars_daily")
            df["quality_flag"] = value
            result = SCHEMA["bars_daily"].validate(df)
            assert result["quality_flag"].iloc[0] == value

    def test_quality_flag_on_market_tables(self):
        tables_with = {
            "cn_stock_bars_daily",
            "cn_stock_bars_minute",
            "cn_stock_fundamental",
            "cn_stock_daily_status",
            "cn_stock_tick",
            "cn_stock_tick_l2",
            "cn_etf_bars_daily",
            "cn_etf_bars_minute",
            "cn_etf_fundamental",
            "cn_etf_tick",
            "cn_fund_nav",
            "cn_futures_bars_daily",
            "cn_futures_tick_l2",
        }
        for table in tables_with:
            assert "quality_flag" in SCHEMA[table].columns, table

    def test_quality_flag_absent_on_non_market_tables(self):
        tables_without = {
            "factor",
            "cn_calendar",
            "cn_stock_basic",
            "cn_futures_contract_specs",
            "cn_stock_industry",
            "cn_stock_corporate_actions",
            "cn_futures_continuous",
            "cn_fund_basic",
        }
        for table in tables_without:
            assert "quality_flag" not in SCHEMA[table].columns, table


# ---------------------------------------------------------------------------
# Calendar exchanges
# ---------------------------------------------------------------------------
class TestCalendarExchanges:
    def test_nine_exchanges(self):
        assert len(CALENDAR_EXCHANGES) == 9

    def test_equals_exchange_map_values(self):
        assert CALENDAR_EXCHANGES == set(EXCHANGE_MAP.values())

    def test_includes_ine_and_gfex(self):
        assert "ine" in CALENDAR_EXCHANGES
        assert "gfex" in CALENDAR_EXCHANGES


# ---------------------------------------------------------------------------
# Factor schema factory
# ---------------------------------------------------------------------------
class TestFactorSchema:
    def test_default_uses_value_column(self):
        df = _valid_df("factor")
        result = SCHEMA["factor"].validate(df)
        assert "value" in result.columns

    def test_named_factor_schema(self):
        schema = factor_schema("momentum_20d")
        df = pd.DataFrame(
            {
                "ticker": ["600519.SH"],
                "date": [D],
                "momentum_20d": [0.05],
            }
        )
        result = schema.validate(df)
        assert "momentum_20d" in result.columns

    def test_named_factor_rejects_wrong_column(self):
        schema = factor_schema("momentum_20d")
        df = pd.DataFrame(
            {
                "ticker": ["600519.SH"],
                "date": [D],
                "value": [0.05],  # wrong column name for this factor
            }
        )
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            schema.validate(df)

    def test_factor_value_nullable(self):
        schema = factor_schema("momentum_20d")
        df = pd.DataFrame(
            {
                "ticker": ["600519.SH"],
                "date": [D],
                "momentum_20d": [float("nan")],
            }
        )
        result = schema.validate(df)
        assert pd.isna(result["momentum_20d"].iloc[0])


# ---------------------------------------------------------------------------
# Factor view naming
# ---------------------------------------------------------------------------
class TestFactorViewName:
    def test_simple_name(self):
        assert factor_view_name("momentum_20d") == "factor_momentum_20d"

    def test_dot_replaced(self):
        assert factor_view_name("alpha.1") == "factor_alpha_1"

    def test_dash_replaced(self):
        assert factor_view_name("mom-20d") == "factor_mom_20d"

    def test_leading_digit_gets_extra_prefix(self):
        assert factor_view_name("20d_mom") == "factor__20d_mom"

    def test_underscore_preserved(self):
        assert factor_view_name("a_b_c") == "factor_a_b_c"


# ---------------------------------------------------------------------------
# Column-level spot checks (types and checks per §47)
# ---------------------------------------------------------------------------
class TestColumnContracts:
    def test_bars_daily_column_count(self):
        assert len(SCHEMA["bars_daily"].columns) == 14

    def test_bars_minute_no_adjfactor(self):
        assert "adjfactor" not in SCHEMA["bars_minute"].columns
        assert len(SCHEMA["bars_minute"].columns) == 10

    def test_bars_daily_no_prev_close_pct_change_close_adj(self):
        cols = SCHEMA["bars_daily"].columns
        for forbidden in ("prev_close", "pct_change", "close_adj"):
            assert forbidden not in cols

    def test_bars_daily_futures_no_adjfactor(self):
        assert "adjfactor" not in SCHEMA["bars_daily_futures"].columns
        assert "settle" in SCHEMA["bars_daily_futures"].columns
        assert "open_interest" in SCHEMA["bars_daily_futures"].columns

    def test_continuous_no_close_adj(self):
        assert "close_adj" not in SCHEMA["continuous"].columns
        assert "adjfactor" in SCHEMA["continuous"].columns

    def test_contract_specs_column_count(self):
        assert len(SCHEMA["contract_specs"].columns) == 12

    def test_corporate_actions_column_count(self):
        assert len(SCHEMA["corporate_actions"].columns) == 10

    def test_corporate_actions_event_type_rejects_fifth_type(self):
        df = _valid_df("corporate_actions")
        df["event_type"] = "merger"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["corporate_actions"].validate(df)

    def test_industry_classification_rejects_unknown(self):
        df = _valid_df("industry")
        df["classification"] = "unknown_system"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["industry"].validate(df)

    def test_industry_classification_accepts_ths_and_tdx(self):
        for cls in ("citics", "sw", "ths", "tdx"):
            df = _valid_df("cn_stock_industry")
            df["classification"] = cls
            assert len(SCHEMA["cn_stock_industry"].validate(df)) == 1

    def test_equity_ticker_rejected_on_futures_table(self):
        df = _valid_df("bars_daily_futures")
        df["ticker"] = "600519.SH"  # equity ticker on futures table
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily_futures"].validate(df)

    def test_futures_ticker_rejected_on_equity_table(self):
        df = _valid_df("bars_daily")
        df["ticker"] = "IF2601.CFFEX"  # futures ticker on equity table
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_ohlcv_positive_enforced(self):
        df = _valid_df("bars_daily")
        df["open"] = 0.0
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_volume_non_negative_enforced(self):
        df = _valid_df("bars_daily")
        df["volume"] = -1
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_datetime_coerced_to_date(self):
        """trade_date datetime64 input is coerced to date before validation."""
        df = _valid_df("bars_daily")
        df["trade_date"] = pd.to_datetime(["2024-01-15"])
        result = SCHEMA["bars_daily"].validate(df)
        assert result["trade_date"].iloc[0] == D


# ---------------------------------------------------------------------------
# Regression: code-review findings 2026-10-14
# ---------------------------------------------------------------------------
class TestDateColumnRejectsInt:
    """Finding 1: YYYYMMDD ints must not silently coerce to epoch dates."""

    def test_int_yyyymmdd_rejected_on_date_column(self):
        df = _valid_df("calendar")
        df["trade_date"] = 20240115
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)

    def test_int_yyyymmdd_rejected_on_bars_daily(self):
        df = _valid_df("bars_daily")
        df["trade_date"] = 20240115
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["bars_daily"].validate(df)

    def test_string_date_still_accepted(self):
        df = _valid_df("calendar")
        df["trade_date"] = "2024-01-15"
        result = SCHEMA["calendar"].validate(df)
        assert result["trade_date"].iloc[0] == D

    def test_date_object_still_accepted(self):
        df = _valid_df("calendar")
        df["trade_date"] = D
        result = SCHEMA["calendar"].validate(df)
        assert result["trade_date"].iloc[0] == D


class TestDatetimeColumnRejectsTzAware:
    """Finding 2: tz-aware timestamps must not silently shift."""

    def test_tz_aware_datetime_rejected(self):
        df = _valid_df("bars_minute")
        df["datetime"] = pd.to_datetime(["2024-01-15 09:31:00+08:00"])
        with pytest.raises(ValueError, match="tz-aware"):
            SCHEMA["bars_minute"].validate(df)

    def test_naive_datetime_accepted(self):
        df = _valid_df("bars_minute")
        result = SCHEMA["bars_minute"].validate(df)
        assert result["datetime"].iloc[0] == DT


class TestNullableDateColumns:
    """Finding A regression: nullable date columns must accept missing values."""

    def test_announce_date_none_accepted(self):
        row = _valid_row("corporate_actions")
        row["announce_date"] = None
        df = pd.DataFrame([row])
        result = SCHEMA["corporate_actions"].validate(df)
        assert pd.isna(result["announce_date"].iloc[0])

    def test_record_date_nan_accepted(self):
        row = _valid_row("corporate_actions")
        row["record_date"] = float("nan")
        df = pd.DataFrame([row])
        result = SCHEMA["corporate_actions"].validate(df)
        assert pd.isna(result["record_date"].iloc[0])

    def test_mixed_present_missing_batch_accepted(self):
        row1 = _valid_row("corporate_actions")
        row1["announce_date"] = date(2024, 1, 10)
        row1["record_date"] = None
        row2 = {
            **row1,
            "ticker": "000001.SZ",
            "announce_date": None,
            "record_date": date(2024, 1, 14),
        }
        df = pd.DataFrame([row1, row2])
        result = SCHEMA["corporate_actions"].validate(df)
        assert result["announce_date"].iloc[0] == date(2024, 1, 10)
        assert pd.isna(result["record_date"].iloc[0])
        assert pd.isna(result["announce_date"].iloc[1])
        assert result["record_date"].iloc[1] == date(2024, 1, 14)

    def test_both_dates_none_accepted(self):
        row = _valid_row("corporate_actions")
        row["announce_date"] = None
        row["record_date"] = None
        df = pd.DataFrame([row])
        result = SCHEMA["corporate_actions"].validate(df)
        assert pd.isna(result["announce_date"].iloc[0])
        assert pd.isna(result["record_date"].iloc[0])


class TestDateColumnRejectsFloatAndObject:
    """Finding B regression: float/object dtype integers must not silently coerce."""

    def test_float64_yyyymmdd_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = pd.Series([20240115.0])
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)

    def test_object_dtype_int_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = pd.Series([20240115], dtype="object")
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)

    def test_mixed_int_string_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = pd.Series([20240115, "2024-01-16"], dtype="object")
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)

    def test_float64_epoch_datetime_rejected(self):
        row = _valid_row("bars_minute")
        row["datetime"] = 1705282260.0
        df = pd.DataFrame([row])
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["bars_minute"].validate(df)

    def test_object_dtype_int_datetime_rejected(self):
        row = _valid_row("bars_minute")
        row["datetime"] = pd.Series([1705282260], dtype="object").iloc[0]
        df = pd.DataFrame([row])
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["bars_minute"].validate(df)

    def test_object_tz_aware_timestamp_rejected(self):
        ts = pd.Timestamp("2024-01-15 09:31:00+08:00")
        row = _valid_row("bars_minute")
        row["datetime"] = ts
        df = pd.DataFrame([row])
        with pytest.raises((ValueError, pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_minute"].validate(df)

    def test_numpy_int64_in_object_column_rejected(self):
        import numpy as np

        df = _valid_df("calendar")
        df["trade_date"] = pd.Series([np.int64(20240115), "2024-01-16"], dtype=object)
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)

    def test_numpy_float32_in_object_column_rejected(self):
        import numpy as np

        df = _valid_df("calendar")
        df["trade_date"] = pd.Series([np.float32(20240115.0)], dtype=object)
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)

    def test_numpy_bool_in_object_column_rejected(self):
        import numpy as np

        df = _valid_df("calendar")
        df["trade_date"] = pd.Series([np.bool_(True)], dtype=object)
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)


class TestPrimaryKeyUniqueness:
    """Finding 3: duplicate PK rows must be rejected (§47.14)."""

    def test_duplicate_bars_daily_pk_rejected(self):
        row = _valid_row("bars_daily")
        df = pd.DataFrame([row, row])
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_distinct_bars_daily_pk_accepted(self):
        row1 = _valid_row("bars_daily")
        row2 = {**row1, "ticker": "000001.SZ"}
        df = pd.DataFrame([row1, row2])
        result = SCHEMA["bars_daily"].validate(df)
        assert len(result) == 2

    def test_duplicate_corporate_actions_pk_rejected(self):
        row = _valid_row("corporate_actions")
        df = pd.DataFrame([row, row])
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["corporate_actions"].validate(df)

    def test_duplicate_calendar_pk_rejected(self):
        row = _valid_row("calendar")
        df = pd.DataFrame([row, row])
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["calendar"].validate(df)


class TestFactorViewRegistry:
    """Finding 4: factor_view_name collisions must be detected."""

    def test_collision_detected(self):
        registry = FactorViewRegistry()
        registry.register("alpha.1")
        with pytest.raises(ValueError, match="collision"):
            registry.register("alpha_1")

    def test_same_name_reregister_ok(self):
        registry = FactorViewRegistry()
        v1 = registry.register("momentum_20d")
        v2 = registry.register("momentum_20d")
        assert v1 == v2

    def test_distinct_names_ok(self):
        registry = FactorViewRegistry()
        assert registry.register("momentum_20d") == "factor_momentum_20d"
        assert registry.register("reversal_5d") == "factor_reversal_5d"

    def test_views_property(self):
        registry = FactorViewRegistry()
        registry.register("alpha.1")
        assert registry.views == {"factor_alpha_1": "alpha.1"}


class TestFactorSchemaReservedNames:
    """Finding 5: factor_schema must reject reserved column names."""

    def test_ticker_rejected(self):
        with pytest.raises(ValueError, match="reserved"):
            factor_schema("ticker")

    def test_date_rejected(self):
        with pytest.raises(ValueError, match="reserved"):
            factor_schema("date")

    def test_normal_name_accepted(self):
        schema = factor_schema("momentum_20d")
        assert "momentum_20d" in schema.columns


class TestContractSpecsExchange:
    """Finding 7: exchange column must be validated against EXCHANGE_MAP."""

    def test_valid_exchange_accepted(self):
        df = _valid_df("contract_specs")
        df["exchange"] = "CFFEX"
        result = SCHEMA["contract_specs"].validate(df)
        assert result["exchange"].iloc[0] == "CFFEX"

    def test_invalid_exchange_rejected(self):
        df = _valid_df("contract_specs")
        df["exchange"] = "FAKE_EXCHANGE"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["contract_specs"].validate(df)


# ---------------------------------------------------------------------------
# QUANT-15 regressions — second-review residuals
# ---------------------------------------------------------------------------
class TestTzAnnotatedStringDateRejected:
    """#1: tz-annotated strings must not silently shift the date."""

    def test_utc_offset_string_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = "2024-01-15 23:00:00+00:00"
        with pytest.raises(ValueError, match="tz"):
            SCHEMA["calendar"].validate(df)

    def test_beijing_offset_string_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = "2024-01-15 09:31:00+08:00"
        with pytest.raises(ValueError, match="tz"):
            SCHEMA["calendar"].validate(df)

    def test_naive_string_date_still_accepted(self):
        """No-tz ISO strings must keep their current behaviour."""
        df = _valid_df("calendar")
        df["trade_date"] = "2024-01-15"
        result = SCHEMA["calendar"].validate(df)
        assert result["trade_date"].iloc[0] == date(2024, 1, 15)

    def test_tz_annotated_string_on_bars_daily_rejected(self):
        df = _valid_df("bars_daily")
        df["trade_date"] = "2024-01-15 23:00:00+00:00"
        with pytest.raises(ValueError, match="tz"):
            SCHEMA["bars_daily"].validate(df)


class TestIndustryUniqueIncludesClassification:
    """#2: unique=["ticker"] rejects a ticker carrying both citics and sw."""

    def _industry_row(self, classification: str, **overrides) -> dict:
        row = _valid_row("industry")
        row["classification"] = classification
        row.update(overrides)
        return row

    def test_same_ticker_two_classifications_accepted(self):
        df = pd.DataFrame(
            [
                self._industry_row("citics"),
                self._industry_row("sw"),
            ]
        )
        result = SCHEMA["industry"].validate(df)
        assert len(result) == 2

    def test_same_ticker_same_classification_rejected(self):
        df = pd.DataFrame(
            [
                self._industry_row("citics"),
                self._industry_row("citics"),
            ]
        )
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["industry"].validate(df)

    def test_three_classifications_same_ticker_rejected_on_duplicate(self):
        """Two citics + one sw: the duplicate citics pair must still fail."""
        df = pd.DataFrame(
            [
                self._industry_row("citics"),
                self._industry_row("sw"),
                self._industry_row("citics"),
            ]
        )
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["industry"].validate(df)


class TestMissingLookingStringsRejected:
    """#3: garbage strings that look like missing must be rejected uniformly."""

    GARBAGE = ["", "NaN", "nan", "NaT", "null", "  "]

    @pytest.mark.parametrize("garbage", GARBAGE)
    def test_garbage_string_rejected_on_date(self, garbage):
        df = _valid_df("calendar")
        df["trade_date"] = garbage
        with pytest.raises(ValueError):
            SCHEMA["calendar"].validate(df)

    @pytest.mark.parametrize("garbage", GARBAGE)
    def test_garbage_string_rejected_on_datetime(self, garbage):
        row = _valid_row("bars_minute")
        row["datetime"] = garbage
        df = pd.DataFrame([row])
        with pytest.raises(ValueError):
            SCHEMA["bars_minute"].validate(df)

    @pytest.mark.parametrize("garbage", GARBAGE)
    def test_garbage_string_rejected_on_nullable_date(self, garbage):
        row = _valid_row("corporate_actions")
        row["announce_date"] = garbage
        df = pd.DataFrame([row])
        with pytest.raises(ValueError):
            SCHEMA["corporate_actions"].validate(df)

    def test_none_accepted_on_nullable_date(self):
        row = _valid_row("corporate_actions")
        row["announce_date"] = None
        df = pd.DataFrame([row])
        result = SCHEMA["corporate_actions"].validate(df)
        assert pd.isna(result["announce_date"].iloc[0])

    def test_nan_accepted_on_nullable_date(self):
        row = _valid_row("corporate_actions")
        row["record_date"] = float("nan")
        df = pd.DataFrame([row])
        result = SCHEMA["corporate_actions"].validate(df)
        assert pd.isna(result["record_date"].iloc[0])
