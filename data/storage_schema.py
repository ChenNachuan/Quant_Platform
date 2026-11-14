"""存储层 Schema 契约 —— 全部 52 张规范表的 Pandera DataFrameSchema 定义。

纯结构定义，无任何 IO 操作；写入与读取路径统一引用本模块 Schema。

目录布局（``$QUANT_DATA_ROOT/`` 下）：
- ``asset/``：47 张 A 股相关表（日历、股票核心与状态、指数/行业/权重、7 张 PIT 基本面表
  ``cn_stock_fundamental_*``、特色资金流/情绪/筹码、股东与资本事件、逐笔/L2、5 张独立 ``cn_etf_*``、
  5 张公募基金 ``cn_fund_*``）及 4 张期货表（``cn_futures_*``）。
- ``factors/``：因子表（1 张，按因子与年份分区的长表）。

结构与类型约定：
- Schema 仅允许向后兼容增列（新列须同时设置 ``nullable=True`` 与 ``required=False``），``strict=True`` 拒绝未声明列。
- PIT 约定：基本面与事件表以 ``ann_date`` / ``report_date``（或 ``visible_from``）标识可见时点，防止未来函数。
- 日期/时间列通过 ``_safe_date_parser`` / ``_safe_datetime_parser`` 转为无时区北京时间，拒绝整型与带时区输入。
- 数值列启用 ``coerce=False``，整型列遇非整数浮点直接报错不截断；可空整型/数值列统一使用 ``Column(float, ..., nullable=True)``。
- 因子 DuckDB 视图名通过 ``FactorViewRegistry`` 规范化为 ``factor_{sanitized}``。
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema, Parser

from core.asset_rules import (
    EXCHANGE_MAP,
    validate_equity_ticker,
    validate_etf_ticker,
    validate_fund_ticker,
    validate_futures_ticker,
    validate_index_ticker,
    validate_stock_ticker,
)
from core.types import QualityFlag

# --- quality_flag 位掩码（int64，5 位，CLEAN = 0） ---
QUALITY_FLAG_BITS: dict[str, int] = {
    "C02_COVERAGE": int(QualityFlag.C02_COVERAGE),
    "C05_NULL_RATE": int(QualityFlag.C05_NULL_RATE),
    "L04_BEYOND_LIMIT": int(QualityFlag.L04_BEYOND_LIMIT),
    "L07_ADJFACTOR_RANGE": int(QualityFlag.L07_ADJFACTOR_RANGE),
    "K01_OVERNIGHT_GAP": int(QualityFlag.K01_OVERNIGHT_GAP),
}
CLEAN: int = int(QualityFlag.CLEAN)
QUALITY_FLAG_MAX: int = 31  # 5 个标志位 -> 取值 < 32


def _quality_flag_column() -> Column:
    """公共质量标志列：5 位 int64 位掩码，非空。"""
    return Column(
        int,
        checks=[
            Check.ge(0, name="quality_flag_non_negative"),
            Check.lt(QUALITY_FLAG_MAX + 1, name="quality_flag_within_5_bits"),
        ],
        nullable=False,
    )


# --- 日期 / 时间安全转换 ---
_MISSING_LOOKING_TOKENS = frozenset(
    {
        "",
        "nan",
        "nat",
        "none",
        "null",
        "na",
        "n/a",
        "n.a.",
        "<na>",
        "<nat>",
    }
)


def _reject_numeric_and_bad_objects(series: pd.Series, kind: str) -> None:
    """拒绝数值类型、带时区元素及伪缺失字符串。"""
    if series.dtype.kind in "iufb":
        if series.isna().all():
            return
        raise ValueError(
            f"numeric input to {kind} column not allowed (got dtype {series.dtype}); "
            "pass datetime64, date, or ISO string"
        )
    if getattr(series.dtype, "tz", None) is not None:
        raise ValueError(f"tz-aware input to {kind} column not allowed")
    if series.dtype.kind == "O":
        for val in series.dropna():
            if isinstance(val, (bool, np.bool_, int, float, np.integer, np.floating)):
                raise ValueError(
                    f"numeric value {val!r} in {kind} column not allowed; "
                    "pass datetime64, date, or ISO string"
                )
            if getattr(val, "tzinfo", None) is not None:
                raise ValueError(f"tz-aware value {val!r} in {kind} column not allowed")
            if isinstance(val, str):
                token = val.strip().lower()
                if token in _MISSING_LOOKING_TOKENS:
                    raise ValueError(
                        f"missing-looking string {val!r} in {kind} column not allowed; "
                        "pass None/NaN/NaT for missing values"
                    )


def _safe_date_parser(series: pd.Series) -> pd.Series:
    """转为 ``datetime.date``，拒绝数值、带时区及非法字符串。"""
    _reject_numeric_and_bad_objects(series, "date")
    mask = series.isna()
    result = pd.Series(index=series.index, dtype="object")
    if (~mask).any():
        try:
            converted = pd.to_datetime(series[~mask], errors="raise")
        except (ValueError, TypeError) as exc:
            raise ValueError(f"cannot parse date column: {exc}") from exc
        if getattr(converted.dtype, "tz", None) is not None:
            raise ValueError(
                "tz-aware input to date column not allowed "
                "(design uses naive Beijing time); "
                "pass a naive ISO date string or datetime.date"
            )
        result[~mask] = converted.dt.date
    result[mask] = pd.NaT
    return result


def _safe_datetime_parser(series: pd.Series) -> pd.Series:
    """转为无时区 ``datetime64``，拒绝数值与带时区输入。"""
    _reject_numeric_and_bad_objects(series, "datetime")
    mask = series.isna()
    result = pd.Series(index=series.index, dtype="datetime64[ns]")
    if (~mask).any():
        try:
            converted = pd.to_datetime(series[~mask], errors="raise")
        except (ValueError, TypeError) as exc:
            raise ValueError(f"cannot parse datetime column: {exc}") from exc
        if getattr(converted.dtype, "tz", None) is not None:
            raise ValueError(
                "tz-aware timestamps not allowed (design uses naive Beijing time)"
            )
        result[~mask] = converted
    return result


def _date_column(nullable: bool = False) -> Column:
    """带安全转换器的日期列（拒绝整型与带时区输入）。"""
    return Column(
        pa.Date,
        coerce=False,
        parsers=Parser(_safe_date_parser),
        nullable=nullable,
    )


def _datetime_column(nullable: bool = False) -> Column:
    """带安全转换器的时间戳列（拒绝整型与带时区输入）。"""
    return Column(
        pa.DateTime,
        coerce=False,
        parsers=Parser(_safe_datetime_parser),
        nullable=nullable,
    )


# --- 公共校验规则 ---
_equity_ticker_check = Check(
    validate_equity_ticker,
    element_wise=True,
    name="equity_ticker",
)
_stock_ticker_check = Check(
    validate_stock_ticker,
    element_wise=True,
    name="stock_ticker",
)
_etf_ticker_check = Check(
    validate_etf_ticker,
    element_wise=True,
    name="etf_ticker",
)
_fund_ticker_check = Check(
    validate_fund_ticker,
    element_wise=True,
    name="fund_ticker",
)
_index_ticker_check = Check(
    validate_index_ticker,
    element_wise=True,
    name="index_ticker",
)
_futures_ticker_check = Check(
    validate_futures_ticker,
    element_wise=True,
    name="futures_ticker",
)

_ohlcv_checks = [Check.gt(0, name="positive")]
_non_negative_checks = [Check.ge(0, name="non_negative")]
_event_type_check = Check.isin(
    ["dividend", "split", "rights", "bonus"],
    name="event_type",
)
_classification_check = Check.isin(
    ["citics", "sw", "ths", "tdx"],
    name="classification",
)
_exchange_check = Check.isin(
    sorted(EXCHANGE_MAP),
    name="exchange_code",
)
_bs_flag_check = Check.isin(["B", "S", "N", "A"], name="bs_flag")


def _table(columns: dict, unique: list | None = None, **kwargs) -> DataFrameSchema:
    """构建统一配置的 DataFrameSchema（coerce=False / strict=True / unique）。"""
    return DataFrameSchema(
        columns,
        coerce=False,
        strict=True,
        unique=unique or [],
        **kwargs,
    )


# --- 旧表名与规范表名别名映射 ---
LEGACY_TO_CANONICAL: dict[str, str] = {
    "bars_daily": "cn_stock_bars_daily",
    "bars_minute": "cn_stock_bars_minute",
    "fundamental": "cn_stock_fundamental",
    "calendar": "cn_calendar",
    "industry": "cn_stock_industry",
    "corporate_actions": "cn_stock_corporate_actions",
    "bars_daily_futures": "cn_futures_bars_daily",
    "tick_futures_l2": "cn_futures_tick_l2",
    "contract_specs": "cn_futures_contract_specs",
    "continuous": "cn_futures_continuous",
}

CANONICAL_TO_LEGACY: dict[str, str] = {v: k for k, v in LEGACY_TO_CANONICAL.items()}

TABLE_ALIASES: dict[str, str] = {
    **LEGACY_TO_CANONICAL,
    **CANONICAL_TO_LEGACY,
}


def resolve_table_name(name: str) -> str:
    """将旧版短表名（如 ``bars_daily``）解析为规范表名。"""
    return LEGACY_TO_CANONICAL.get(name, name)


class _AliasedDict(dict):
    """仅存储规范表名键、同时透明兼容旧短表名查询（``in`` / ``[]`` / ``get``）的字典。"""

    def __contains__(self, key: object) -> bool:
        if super().__contains__(key):
            return True
        if isinstance(key, str) and key in LEGACY_TO_CANONICAL:
            return super().__contains__(LEGACY_TO_CANONICAL[key])
        return False

    def __getitem__(self, key: str):
        if super().__contains__(key):
            return super().__getitem__(key)
        if key in LEGACY_TO_CANONICAL:
            return super().__getitem__(LEGACY_TO_CANONICAL[key])
        raise KeyError(key)

    def get(self, key: str, default=None):
        if key in self:
            return self[key]
        return default


# --- 1. 交易日历 (1 张) ---
CALENDAR_SCHEMA = _table(
    {"trade_date": _date_column()},
    unique=["trade_date"],
)

# --- 2. A 股核心行情与状态 (6 张) ---
CN_STOCK_BASIC_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "name": Column(str, nullable=False),
        "exchange": Column(str, checks=_exchange_check, nullable=False),
        "market": Column(str, nullable=False),
        "list_status": Column(str, nullable=False),
        "list_date": _date_column(),
        "delist_date": _date_column(nullable=True),
        "is_hs": Column(str, nullable=True),
        "old_code": Column(str, nullable=True),
    },
    unique=["ticker"],
)

BARS_DAILY_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "open": Column(float, checks=_ohlcv_checks, nullable=False),
        "high": Column(float, checks=_ohlcv_checks, nullable=False),
        "low": Column(float, checks=_ohlcv_checks, nullable=False),
        "close": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "adjfactor": Column(float, checks=_ohlcv_checks, nullable=False),
        "turnover": Column(float, checks=_non_negative_checks, nullable=True),
        "suspend": Column(bool, nullable=False),
        "is_limit_up": Column(bool, nullable=False),
        "is_limit_down": Column(bool, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

BARS_MINUTE_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "datetime": _datetime_column(),
        "open": Column(float, checks=_ohlcv_checks, nullable=False),
        "high": Column(float, checks=_ohlcv_checks, nullable=False),
        "low": Column(float, checks=_ohlcv_checks, nullable=False),
        "close": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "datetime"],
)

FUNDAMENTAL_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "total_market_cap": Column(float, checks=_non_negative_checks, nullable=False),
        "float_market_cap": Column(float, checks=_non_negative_checks, nullable=False),
        "pe_ttm": Column(float, nullable=True),
        "pe_lyr": Column(float, nullable=True),
        "pb": Column(float, nullable=True),
        "ps_ttm": Column(float, nullable=True),
        "total_shares": Column(int, checks=_non_negative_checks, nullable=False),
        "float_shares": Column(int, checks=_non_negative_checks, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

CN_STOCK_DAILY_STATUS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "pre_close": Column(float, checks=_ohlcv_checks, nullable=True),
        "up_limit": Column(float, checks=_ohlcv_checks, nullable=True),
        "down_limit": Column(float, checks=_ohlcv_checks, nullable=True),
        "limit_pct": Column(float, checks=_non_negative_checks, nullable=True),
        "is_st": Column(bool, nullable=False),
        "is_suspend": Column(bool, nullable=False),
        "is_limit_up": Column(bool, nullable=False),
        "is_limit_down": Column(bool, nullable=False),
        "touched_limit_up": Column(bool, nullable=False),
        "touched_limit_down": Column(bool, nullable=False),
        "is_xr": Column(bool, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

CORPORATE_ACTIONS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "ex_date": _date_column(),
        "event_type": Column(str, checks=_event_type_check, nullable=False),
        "announce_date": _date_column(nullable=True),
        "record_date": _date_column(nullable=True),
        "detail": Column(str, nullable=False),
        "cash_dividend": Column(float, checks=_non_negative_checks, nullable=True),
        "stock_dividend": Column(float, checks=_non_negative_checks, nullable=True),
        "rights_price": Column(float, checks=_non_negative_checks, nullable=True),
        "rights_ratio": Column(float, checks=_non_negative_checks, nullable=True),
    },
    unique=["ticker", "ex_date", "event_type"],
)

# --- 3. 指数 / 行业 / 成分权重 (4 张) ---
CN_STOCK_INDEX_BASIC_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_index_ticker_check, nullable=False),
        "name": Column(str, nullable=False),
        "category": Column(str, nullable=False),
        "publisher": Column(str, nullable=True),
        "base_date": _date_column(nullable=True),
        "base_point": Column(float, checks=_ohlcv_checks, nullable=True),
        "list_date": _date_column(nullable=True),
    },
    unique=["ticker"],
)

CN_STOCK_INDEX_BARS_DAILY_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_index_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "open": Column(float, checks=_ohlcv_checks, nullable=False),
        "high": Column(float, checks=_ohlcv_checks, nullable=False),
        "low": Column(float, checks=_ohlcv_checks, nullable=False),
        "close": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "pe": Column(float, nullable=True),
        "pb": Column(float, nullable=True),
        "turnover": Column(float, checks=_non_negative_checks, nullable=True),
        "total_market_cap": Column(float, checks=_non_negative_checks, nullable=True),
        "float_market_cap": Column(float, checks=_non_negative_checks, nullable=True),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

INDUSTRY_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
        "industry_l1": Column(str, nullable=False),
        "industry_l2": Column(str, nullable=False),
        "industry_l3": Column(str, nullable=True),
        "classification": Column(str, checks=_classification_check, nullable=False),
        "effective_date": _date_column(),
    },
    unique=["ticker", "classification"],
)

CN_STOCK_INDEX_WEIGHTS_SCHEMA = _table(
    {
        "index_code": Column(str, checks=_index_ticker_check, nullable=False),
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "weight": Column(float, checks=_non_negative_checks, nullable=True),
        "in_date": _date_column(nullable=True),
        "out_date": _date_column(nullable=True),
    },
    unique=["index_code", "ticker", "trade_date"],
)

# --- 4. 股票基本面 PIT 表 (7 张，统一前缀 cn_stock_fundamental_*) ---
CN_STOCK_FUNDAMENTAL_INCOME_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "report_date": _date_column(),
        "ann_date": _date_column(),
        "f_ann_date": _date_column(nullable=True),
        "statement_type": Column(str, nullable=False),
        "comp_type": Column(str, nullable=True),
        "basic_eps": Column(float, nullable=True),
        "diluted_eps": Column(float, nullable=True),
        "total_revenue": Column(float, nullable=True),
        "revenue": Column(float, nullable=True),
        "total_cogs": Column(float, nullable=True),
        "operate_profit": Column(float, nullable=True),
        "total_profit": Column(float, nullable=True),
        "net_profit": Column(float, nullable=True),
        "net_profit_attr_p": Column(float, nullable=True),
        "net_profit_dedt": Column(float, nullable=True),
        "rd_exp": Column(float, nullable=True),
        "ebit": Column(float, nullable=True),
        "ebitda": Column(float, nullable=True),
    },
    unique=["ticker", "report_date", "ann_date", "statement_type"],
)

CN_STOCK_FUNDAMENTAL_BALANCE_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "report_date": _date_column(),
        "ann_date": _date_column(),
        "f_ann_date": _date_column(nullable=True),
        "statement_type": Column(str, nullable=False),
        "comp_type": Column(str, nullable=True),
        "total_assets": Column(float, nullable=True),
        "total_cur_assets": Column(float, nullable=True),
        "total_nca": Column(float, nullable=True),
        "money_cap": Column(float, nullable=True),
        "accounts_receiv": Column(float, nullable=True),
        "inventories": Column(float, nullable=True),
        "total_liab": Column(float, nullable=True),
        "total_cur_liab": Column(float, nullable=True),
        "total_ncl": Column(float, nullable=True),
        "total_hldr_eqy_exc_min_int": Column(float, nullable=True),
        "total_hldr_eqy_inc_min_int": Column(float, nullable=True),
        "total_share": Column(float, nullable=True),
    },
    unique=["ticker", "report_date", "ann_date", "statement_type"],
)

CN_STOCK_FUNDAMENTAL_CASHFLOW_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "report_date": _date_column(),
        "ann_date": _date_column(),
        "f_ann_date": _date_column(nullable=True),
        "statement_type": Column(str, nullable=False),
        "comp_type": Column(str, nullable=True),
        "n_cashflow_act": Column(float, nullable=True),
        "n_cashflow_inv_act": Column(float, nullable=True),
        "n_cash_flows_fnc_act": Column(float, nullable=True),
        "c_fr_sale_sg": Column(float, nullable=True),
        "c_paid_to_for_empl": Column(float, nullable=True),
        "c_pay_acq_const_fiolta": Column(float, nullable=True),
        "free_cashflow": Column(float, nullable=True),
        "n_incr_cash_cash_equ": Column(float, nullable=True),
    },
    unique=["ticker", "report_date", "ann_date", "statement_type"],
)

CN_STOCK_FUNDAMENTAL_INDICATOR_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "report_date": _date_column(),
        "ann_date": _date_column(),
        "roe": Column(float, nullable=True),
        "roe_waa": Column(float, nullable=True),
        "roe_dt": Column(float, nullable=True),
        "roa": Column(float, nullable=True),
        "roic": Column(float, nullable=True),
        "grossprofit_margin": Column(float, nullable=True),
        "netprofit_margin": Column(float, nullable=True),
        "debt_to_assets": Column(float, nullable=True),
        "current_ratio": Column(float, nullable=True),
        "quick_ratio": Column(float, nullable=True),
        "assets_turn": Column(float, nullable=True),
        "netprofit_yoy": Column(float, nullable=True),
        "or_yoy": Column(float, nullable=True),
        "audit_result": Column(str, nullable=True),
    },
    unique=["ticker", "report_date", "ann_date"],
)

CN_STOCK_FUNDAMENTAL_SEGMENT_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "report_date": _date_column(),
        "ann_date": _date_column(nullable=True),
        "segment_type": Column(str, nullable=False),
        "segment_name": Column(str, nullable=False),
        "bz_sales": Column(float, nullable=True),
        "bz_profit": Column(float, nullable=True),
        "bz_cost": Column(float, nullable=True),
        "sales_ratio": Column(float, nullable=True),
        "gross_margin": Column(float, nullable=True),
    },
    unique=["ticker", "report_date", "segment_type", "segment_name"],
)

CN_STOCK_FUNDAMENTAL_EARNINGS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "report_date": _date_column(),
        "ann_date": _date_column(),
        "event_kind": Column(
            str,
            checks=Check.isin(["forecast", "express", "schedule"], name="event_kind"),
            nullable=False,
        ),
        "forecast_type": Column(str, nullable=True),
        "p_change_min": Column(float, nullable=True),
        "p_change_max": Column(float, nullable=True),
        "net_profit_min": Column(float, nullable=True),
        "net_profit_max": Column(float, nullable=True),
        "revenue": Column(float, nullable=True),
        "net_profit": Column(float, nullable=True),
        "eps": Column(float, nullable=True),
        "roe": Column(float, nullable=True),
        "pre_date": _date_column(nullable=True),
        "actual_date": _date_column(nullable=True),
    },
    unique=["ticker", "report_date", "ann_date", "event_kind"],
)

CN_STOCK_FUNDAMENTAL_CONSENSUS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "visible_from": _date_column(nullable=True),
        "eps_fy0": Column(float, nullable=True),
        "eps_fy1": Column(float, nullable=True),
        "eps_fy2": Column(float, nullable=True),
        "eps_fwd12m": Column(float, nullable=True),
        "eps_fy1_n": Column(float, nullable=True),
        "eps_fy1_std": Column(float, nullable=True),
        "eps_fy1_up_3m": Column(float, nullable=True),
        "eps_fy1_down_3m": Column(float, nullable=True),
        "rating_score": Column(float, nullable=True),
        "tp_median": Column(float, nullable=True),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

# --- 5. 资金流 / 情绪 / 筹码 (9 张) ---
CN_STOCK_AUCTION_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "auction_session": Column(
            str,
            checks=Check.isin(["open", "close"], name="auction_session"),
            nullable=False,
        ),
        "price": Column(float, checks=_ohlcv_checks, nullable=False),
        "pre_close": Column(float, checks=_ohlcv_checks, nullable=True),
        "pct_chg": Column(float, nullable=True),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "turnover_rate": Column(float, checks=_non_negative_checks, nullable=True),
        "volume_ratio": Column(float, checks=_non_negative_checks, nullable=True),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "auction_session"],
)

CN_STOCK_MONEYFLOW_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "buy_elg_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "sell_elg_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "buy_lg_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "sell_lg_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "buy_md_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "sell_md_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "buy_sm_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "sell_sm_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "net_mf_amount": Column(float, nullable=True),
        "net_mf_pct": Column(float, nullable=True),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

CN_STOCK_HSGT_MARKET_FLOW_SCHEMA = _table(
    {
        "trade_date": _date_column(),
        "ggt_ss": Column(float, nullable=True),
        "ggt_sz": Column(float, nullable=True),
        "hgt": Column(float, nullable=True),
        "sgt": Column(float, nullable=True),
        "north_money": Column(float, nullable=True),
        "south_money": Column(float, nullable=True),
        "is_turnover_regime": Column(bool, nullable=False),
    },
    unique=["trade_date"],
)

CN_STOCK_HSGT_HOLD_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "vol": Column(int, checks=_non_negative_checks, nullable=False),
        "ratio": Column(float, checks=_non_negative_checks, nullable=True),
        "exchange": Column(str, checks=_exchange_check, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

CN_STOCK_MARGIN_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "rzye": Column(float, checks=_non_negative_checks, nullable=True),
        "rzmre": Column(float, checks=_non_negative_checks, nullable=True),
        "rzche": Column(float, checks=_non_negative_checks, nullable=True),
        "rqyl": Column(float, checks=_non_negative_checks, nullable=True),
        "rqmcl": Column(float, checks=_non_negative_checks, nullable=True),
        "rqchl": Column(float, checks=_non_negative_checks, nullable=True),
        "rqye": Column(float, checks=_non_negative_checks, nullable=True),
        "rzrqye": Column(float, checks=_non_negative_checks, nullable=True),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

CN_STOCK_LHB_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "reason": Column(str, nullable=False),
        "close": Column(float, checks=_ohlcv_checks, nullable=True),
        "pct_chg": Column(float, nullable=True),
        "turnover_rate": Column(float, checks=_non_negative_checks, nullable=True),
        "l_buy": Column(float, checks=_non_negative_checks, nullable=True),
        "l_sell": Column(float, checks=_non_negative_checks, nullable=True),
        "net_amount": Column(float, nullable=True),
        "net_rate": Column(float, nullable=True),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "reason"],
)

CN_STOCK_LHB_SEATS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "side": Column(
            str, checks=Check.isin(["buy", "sell"], name="side"), nullable=False
        ),
        "rank": Column(int, checks=Check.ge(1), nullable=False),
        "seat_name": Column(str, nullable=False),
        "buy": Column(float, checks=_non_negative_checks, nullable=True),
        "sell": Column(float, checks=_non_negative_checks, nullable=True),
        "net_buy": Column(float, nullable=True),
        "hm_name": Column(str, nullable=True),
        "is_institution": Column(bool, nullable=False),
    },
    unique=["ticker", "trade_date", "side", "rank", "seat_name"],
)

CN_STOCK_LIMIT_POOL_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "pool_type": Column(
            str,
            checks=Check.isin(["up", "down", "break"], name="pool_type"),
            nullable=False,
        ),
        "first_time": Column(str, nullable=True),
        "last_time": Column(str, nullable=True),
        "open_times": Column(float, checks=_non_negative_checks, nullable=True),
        "streak_days": Column(float, checks=_non_negative_checks, nullable=True),
        "seal_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "theme": Column(str, nullable=True),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "pool_type"],
)

CN_STOCK_CHIPS_PERF_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "his_low": Column(float, checks=_ohlcv_checks, nullable=True),
        "his_high": Column(float, checks=_ohlcv_checks, nullable=True),
        "cost_5pct": Column(float, checks=_ohlcv_checks, nullable=True),
        "cost_15pct": Column(float, checks=_ohlcv_checks, nullable=True),
        "cost_50pct": Column(float, checks=_ohlcv_checks, nullable=True),
        "cost_85pct": Column(float, checks=_ohlcv_checks, nullable=True),
        "cost_95pct": Column(float, checks=_ohlcv_checks, nullable=True),
        "weight_avg": Column(float, checks=_ohlcv_checks, nullable=True),
        "winner_rate": Column(float, checks=_non_negative_checks, nullable=True),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

# --- 6. 股东与资本运作事件 (8 张) ---
CN_STOCK_HOLDER_NUM_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "ann_date": _date_column(),
        "end_date": _date_column(),
        "holder_num": Column(int, checks=Check.ge(1), nullable=False),
        "holder_num_chg_pct": Column(float, nullable=True),
        "avg_hold_shares": Column(float, checks=_non_negative_checks, nullable=True),
    },
    unique=["ticker", "end_date", "ann_date"],
)

CN_STOCK_TOP_HOLDERS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "ann_date": _date_column(),
        "end_date": _date_column(),
        "holder_scope": Column(
            str,
            checks=Check.isin(["top10", "free_top10"], name="holder_scope"),
            nullable=False,
        ),
        "rank": Column(int, checks=Check.ge(1), nullable=False),
        "holder_name": Column(str, nullable=False),
        "holder_type": Column(str, nullable=True),
        "hold_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "hold_ratio": Column(float, checks=_non_negative_checks, nullable=True),
        "hold_change": Column(float, nullable=True),
    },
    unique=["ticker", "end_date", "holder_scope", "rank", "holder_name"],
)

CN_STOCK_RESTRICTED_RELEASE_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "ann_date": _date_column(nullable=True),
        "float_date": _date_column(),
        "holder_name": Column(str, nullable=False),
        "float_share": Column(float, checks=_non_negative_checks, nullable=False),
        "float_ratio": Column(float, checks=_non_negative_checks, nullable=True),
        "share_type": Column(str, nullable=False),
        "is_announced": Column(bool, nullable=False),
    },
    unique=["ticker", "float_date", "holder_name", "share_type"],
)

CN_STOCK_BLOCK_TRADE_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "seq": Column(int, checks=Check.ge(0), nullable=False),
        "price": Column(float, checks=_ohlcv_checks, nullable=False),
        "vol": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "discount_rate": Column(float, nullable=True),
        "buyer": Column(str, nullable=True),
        "seller": Column(str, nullable=True),
    },
    unique=["ticker", "trade_date", "seq"],
)

CN_STOCK_REPURCHASE_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "ann_date": _date_column(),
        "end_date": _date_column(nullable=True),
        "proc": Column(str, nullable=False),
        "exp_date": _date_column(nullable=True),
        "vol": Column(float, checks=_non_negative_checks, nullable=True),
        "amount": Column(float, checks=_non_negative_checks, nullable=True),
        "high_limit": Column(float, checks=_non_negative_checks, nullable=True),
        "low_limit": Column(float, checks=_non_negative_checks, nullable=True),
    },
    unique=["ticker", "ann_date", "proc"],
)

CN_STOCK_HOLDER_TRADE_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "ann_date": _date_column(),
        "holder_name": Column(str, nullable=False),
        "holder_type": Column(str, nullable=True),
        "in_de": Column(
            str,
            checks=Check.isin(["IN", "DE"], name="in_de"),
            nullable=False,
        ),
        "change_vol": Column(float, checks=_non_negative_checks, nullable=True),
        "change_ratio": Column(float, checks=_non_negative_checks, nullable=True),
        "after_share": Column(float, checks=_non_negative_checks, nullable=True),
        "avg_price": Column(float, checks=_non_negative_checks, nullable=True),
        "begin_date": _date_column(nullable=True),
        "close_date": _date_column(nullable=True),
    },
    unique=["ticker", "ann_date", "holder_name", "in_de"],
)

CN_STOCK_PLEDGE_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "ann_date": _date_column(),
        "holder_name": Column(str, nullable=False),
        "pledge_amount": Column(float, checks=_non_negative_checks, nullable=True),
        "start_date": _date_column(),
        "end_date": _date_column(nullable=True),
        "is_release": Column(bool, nullable=False),
        "pledge_ratio": Column(float, checks=_non_negative_checks, nullable=True),
        "freeze_type": Column(str, nullable=True),
    },
    unique=["ticker", "ann_date", "holder_name", "start_date"],
)

CN_STOCK_RESEARCH_VISITS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "surv_date": _date_column(),
        "rece_place": Column(str, nullable=True),
        "rece_mode": Column(str, nullable=True),
        "org_name": Column(str, nullable=False),
        "org_type": Column(str, nullable=True),
        "fund_visitors": Column(str, nullable=True),
    },
    unique=["ticker", "surv_date", "org_name"],
)

# --- 7. 股票逐笔与 L2 盘口 (2 张) ---
CN_STOCK_TICK_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "datetime": _datetime_column(),
        "seq": Column(int, checks=Check.ge(0), nullable=False),
        "price": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "bs_flag": Column(str, checks=_bs_flag_check, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "seq"],
)

_stock_l2_book: dict[str, Column] = {}
for _lvl in range(1, 11):
    _stock_l2_book[f"bid_price_{_lvl}"] = Column(
        float, checks=_non_negative_checks, nullable=False
    )
    _stock_l2_book[f"bid_volume_{_lvl}"] = Column(
        int, checks=_non_negative_checks, nullable=False
    )
    _stock_l2_book[f"ask_price_{_lvl}"] = Column(
        float, checks=_non_negative_checks, nullable=False
    )
    _stock_l2_book[f"ask_volume_{_lvl}"] = Column(
        int, checks=_non_negative_checks, nullable=False
    )

CN_STOCK_TICK_L2_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_stock_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "datetime": _datetime_column(),
        "seq": Column(int, checks=Check.ge(0), nullable=False),
        "last_price": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "num_trades": Column(float, checks=_non_negative_checks, nullable=True),
        **_stock_l2_book,
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "seq"],
)

# --- 8. 独立 ETF 域 (5 张，不含 cn_etf_pcf) ---
CN_ETF_BASIC_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_etf_ticker_check, nullable=False),
        "name": Column(str, nullable=False),
        "exchange": Column(str, checks=_exchange_check, nullable=False),
        "management": Column(str, nullable=True),
        "index_code": Column(str, nullable=True),
        "index_name": Column(str, nullable=True),
        "etf_type": Column(str, nullable=True),
        "setup_date": _date_column(nullable=True),
        "list_date": _date_column(nullable=True),
        "delist_date": _date_column(nullable=True),
        "m_fee": Column(float, checks=_non_negative_checks, nullable=True),
        "c_fee": Column(float, checks=_non_negative_checks, nullable=True),
    },
    unique=["ticker"],
)

CN_ETF_BARS_DAILY_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_etf_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "open": Column(float, checks=_ohlcv_checks, nullable=False),
        "high": Column(float, checks=_ohlcv_checks, nullable=False),
        "low": Column(float, checks=_ohlcv_checks, nullable=False),
        "close": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "adjfactor": Column(float, checks=_ohlcv_checks, nullable=False),
        "turnover": Column(float, checks=_non_negative_checks, nullable=True),
        "suspend": Column(bool, nullable=False),
        "is_limit_up": Column(bool, nullable=False),
        "is_limit_down": Column(bool, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

CN_ETF_BARS_MINUTE_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_etf_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "datetime": _datetime_column(),
        "open": Column(float, checks=_ohlcv_checks, nullable=False),
        "high": Column(float, checks=_ohlcv_checks, nullable=False),
        "low": Column(float, checks=_ohlcv_checks, nullable=False),
        "close": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "datetime"],
)

CN_ETF_FUNDAMENTAL_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_etf_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "unit_nav": Column(float, checks=_ohlcv_checks, nullable=True),
        "accum_nav": Column(float, checks=_ohlcv_checks, nullable=True),
        "nav_adj_factor": Column(float, checks=_ohlcv_checks, nullable=True),
        "iopv": Column(float, checks=_ohlcv_checks, nullable=True),
        "total_shares": Column(float, checks=_non_negative_checks, nullable=True),
        "total_nav": Column(float, checks=_non_negative_checks, nullable=True),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

CN_ETF_TICK_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_etf_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "datetime": _datetime_column(),
        "seq": Column(int, checks=Check.ge(0), nullable=False),
        "price": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "bs_flag": Column(str, checks=_bs_flag_check, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "seq"],
)

# --- 9. 场外与上市公募基金 (5 张) ---
CN_FUND_BASIC_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_fund_ticker_check, nullable=False),
        "name": Column(str, nullable=False),
        "market": Column(
            str, checks=Check.isin(["O", "E"], name="fund_market"), nullable=False
        ),
        "management": Column(str, nullable=True),
        "custodian": Column(str, nullable=True),
        "fund_type": Column(str, nullable=True),
        "invest_type": Column(str, nullable=True),
        "found_date": _date_column(nullable=True),
        "list_date": _date_column(nullable=True),
        "delist_date": _date_column(nullable=True),
        "m_fee": Column(float, checks=_non_negative_checks, nullable=True),
        "c_fee": Column(float, checks=_non_negative_checks, nullable=True),
        "benchmark": Column(str, nullable=True),
    },
    unique=["ticker"],
)

CN_FUND_MANAGERS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_fund_ticker_check, nullable=False),
        "ann_date": _date_column(nullable=True),
        "name": Column(str, nullable=False),
        "gender": Column(str, nullable=True),
        "edu": Column(str, nullable=True),
        "begin_date": _date_column(),
        "end_date": _date_column(nullable=True),
    },
    unique=["ticker", "name", "begin_date"],
)

CN_FUND_NAV_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_fund_ticker_check, nullable=False),
        "nav_date": _date_column(),
        "ann_date": _date_column(nullable=True),
        "unit_nav": Column(float, checks=_ohlcv_checks, nullable=False),
        "accum_nav": Column(float, checks=_ohlcv_checks, nullable=True),
        "adj_nav": Column(float, checks=_ohlcv_checks, nullable=True),
        "nav_adj_factor": Column(float, checks=_ohlcv_checks, nullable=True),
        "total_shares": Column(float, checks=_non_negative_checks, nullable=True),
        "total_netasset": Column(float, checks=_non_negative_checks, nullable=True),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "nav_date"],
)

CN_FUND_DIVIDEND_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_fund_ticker_check, nullable=False),
        "ann_date": _date_column(),
        "record_date": _date_column(nullable=True),
        "ex_date": _date_column(),
        "pay_date": _date_column(nullable=True),
        "div_cash": Column(float, checks=_non_negative_checks, nullable=True),
        "div_proc": Column(str, nullable=True),
    },
    unique=["ticker", "ex_date", "ann_date"],
)

CN_FUND_PORTFOLIO_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_fund_ticker_check, nullable=False),
        "ann_date": _date_column(),
        "end_date": _date_column(),
        "symbol": Column(str, nullable=False),
        "asset_class": Column(
            str,
            checks=Check.isin(["stock", "bond", "fund", "other"], name="asset_class"),
            nullable=False,
        ),
        "mkv": Column(float, checks=_non_negative_checks, nullable=True),
        "amount": Column(float, checks=_non_negative_checks, nullable=True),
        "stk_mkv_ratio": Column(float, checks=_non_negative_checks, nullable=True),
        "stk_float_ratio": Column(float, checks=_non_negative_checks, nullable=True),
        "rank": Column(float, checks=Check.ge(1), nullable=True),
    },
    unique=["ticker", "end_date", "symbol"],
)

# --- 10. 期货 (4 张) 与因子 (1 张) ---
BARS_DAILY_FUTURES_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_futures_ticker_check, nullable=False),
        "trade_date": _date_column(),
        "open": Column(float, checks=_ohlcv_checks, nullable=False),
        "high": Column(float, checks=_ohlcv_checks, nullable=False),
        "low": Column(float, checks=_ohlcv_checks, nullable=False),
        "close": Column(float, checks=_ohlcv_checks, nullable=False),
        "settle": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "open_interest": Column(int, checks=_non_negative_checks, nullable=False),
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date"],
)

_tick_base: dict = {
    "ticker": Column(str, checks=_futures_ticker_check, nullable=False),
    "trade_date": _date_column(),
    "datetime": _datetime_column(),
    "last_price": Column(float, checks=_ohlcv_checks, nullable=False),
    "volume": Column(int, checks=_non_negative_checks, nullable=False),
    "amount": Column(float, checks=_non_negative_checks, nullable=False),
    "open_interest": Column(int, checks=_non_negative_checks, nullable=False),
}
_tick_book: dict = {}
for _i in range(1, 6):
    _tick_book[f"bid_price_{_i}"] = Column(
        float, checks=_non_negative_checks, nullable=False
    )
    _tick_book[f"bid_volume_{_i}"] = Column(
        int, checks=_non_negative_checks, nullable=False
    )
    _tick_book[f"ask_price_{_i}"] = Column(
        float, checks=_non_negative_checks, nullable=False
    )
    _tick_book[f"ask_volume_{_i}"] = Column(
        int, checks=_non_negative_checks, nullable=False
    )

TICK_FUTURES_L2_SCHEMA = _table(
    {
        **_tick_base,
        **_tick_book,
        "quality_flag": _quality_flag_column(),
    },
    unique=["ticker", "trade_date", "datetime"],
)

CONTRACT_SPECS_SCHEMA = _table(
    {
        "ticker": Column(str, checks=_futures_ticker_check, nullable=False),
        "symbol": Column(str, nullable=False),
        "exchange": Column(str, checks=_exchange_check, nullable=False),
        "contract_multiplier": Column(float, checks=_ohlcv_checks, nullable=False),
        "price_tick": Column(float, checks=_ohlcv_checks, nullable=False),
        "list_date": _date_column(),
        "expire_date": _date_column(),
        "delivery_month": Column(str, nullable=False),
        "price_limit_pct": Column(float, checks=_ohlcv_checks, nullable=False),
        "margin_rate": Column(float, checks=_ohlcv_checks, nullable=True),
        "trading_hours": Column(str, nullable=False),
        "is_active": Column(bool, nullable=False),
    },
    unique=["ticker"],
)

CONTINUOUS_SCHEMA = _table(
    {
        "trade_date": _date_column(),
        "dominant_contract": Column(str, checks=_futures_ticker_check, nullable=False),
        "open": Column(float, checks=_ohlcv_checks, nullable=False),
        "high": Column(float, checks=_ohlcv_checks, nullable=False),
        "low": Column(float, checks=_ohlcv_checks, nullable=False),
        "close": Column(float, checks=_ohlcv_checks, nullable=False),
        "volume": Column(int, checks=_non_negative_checks, nullable=False),
        "amount": Column(float, checks=_non_negative_checks, nullable=False),
        "open_interest": Column(int, checks=_non_negative_checks, nullable=False),
        "settle": Column(float, checks=_ohlcv_checks, nullable=False),
        "adjfactor": Column(float, checks=_ohlcv_checks, nullable=False),
    },
    unique=["trade_date"],
)

_FACTOR_RESERVED_COLUMNS = frozenset({"ticker", "date"})


def factor_schema(value_column: str = "value") -> DataFrameSchema:
    """单因子长表 Schema（``ticker, date, {name}``）。"""
    if value_column in _FACTOR_RESERVED_COLUMNS:
        raise ValueError(
            f"value_column {value_column!r} collides with a fixed column name; "
            f"reserved names are {sorted(_FACTOR_RESERVED_COLUMNS)}"
        )
    return _table(
        {
            "ticker": Column(str, checks=_equity_ticker_check, nullable=False),
            "date": _date_column(),
            value_column: Column(float, nullable=True),
        },
        unique=["ticker", "date"],
    )


FACTOR_SCHEMA = factor_schema()

# --- 规范表注册表 —— 共 52 张表 (47 张 A 股相关 + 4 张期货 + 1 张因子) ---
PARTITION_PATHS: dict[str, str] = _AliasedDict(
    {
        # 交易日历 (1)
        "cn_calendar": "asset/cn_calendar/{exchange}.parquet",
        # 股票核心行情与状态 (6)
        "cn_stock_basic": "asset/cn_stock_basic/stock_basic.parquet",
        "cn_stock_bars_daily": "asset/cn_stock_bars_daily/{date}.parquet",
        "cn_stock_bars_minute": "asset/cn_stock_bars_minute/{date}.parquet",
        "cn_stock_fundamental": "asset/cn_stock_fundamental/{month}.parquet",
        "cn_stock_daily_status": "asset/cn_stock_daily_status/{month}.parquet",
        "cn_stock_corporate_actions": "asset/cn_stock_corporate_actions/corporate_actions.parquet",
        # 指数 / 行业 / 成分权重 (4)
        "cn_stock_index_basic": "asset/cn_stock_index_basic/index_basic.parquet",
        "cn_stock_index_bars_daily": "asset/cn_stock_index_bars_daily/{month}.parquet",
        "cn_stock_industry": "asset/cn_stock_industry/industry.parquet",
        "cn_stock_index_weights": "asset/cn_stock_index_weights/{year}.parquet",
        # 股票基本面 PIT (7)
        "cn_stock_fundamental_income": "asset/cn_stock_fundamental_income/{year}.parquet",
        "cn_stock_fundamental_balance": "asset/cn_stock_fundamental_balance/{year}.parquet",
        "cn_stock_fundamental_cashflow": "asset/cn_stock_fundamental_cashflow/{year}.parquet",
        "cn_stock_fundamental_indicator": "asset/cn_stock_fundamental_indicator/{year}.parquet",
        "cn_stock_fundamental_segment": "asset/cn_stock_fundamental_segment/{year}.parquet",
        "cn_stock_fundamental_earnings": "asset/cn_stock_fundamental_earnings/{year}.parquet",
        "cn_stock_fundamental_consensus": "asset/cn_stock_fundamental_consensus/{month}.parquet",
        # 资金流 / 情绪 / 筹码 (9)
        "cn_stock_auction": "asset/cn_stock_auction/{month}.parquet",
        "cn_stock_moneyflow": "asset/cn_stock_moneyflow/{month}.parquet",
        "cn_stock_hsgt_market_flow": "asset/cn_stock_hsgt_market_flow/market_flow.parquet",
        "cn_stock_hsgt_hold": "asset/cn_stock_hsgt_hold/{month}.parquet",
        "cn_stock_margin": "asset/cn_stock_margin/{month}.parquet",
        "cn_stock_lhb": "asset/cn_stock_lhb/{month}.parquet",
        "cn_stock_lhb_seats": "asset/cn_stock_lhb_seats/{month}.parquet",
        "cn_stock_limit_pool": "asset/cn_stock_limit_pool/{month}.parquet",
        "cn_stock_chips_perf": "asset/cn_stock_chips_perf/{month}.parquet",
        # 股东与资本运作事件 (8)
        "cn_stock_holder_num": "asset/cn_stock_holder_num/holder_num.parquet",
        "cn_stock_top_holders": "asset/cn_stock_top_holders/{year}.parquet",
        "cn_stock_restricted_release": "asset/cn_stock_restricted_release/{year}.parquet",
        "cn_stock_block_trade": "asset/cn_stock_block_trade/block_trade.parquet",
        "cn_stock_repurchase": "asset/cn_stock_repurchase/repurchase.parquet",
        "cn_stock_holder_trade": "asset/cn_stock_holder_trade/{year}.parquet",
        "cn_stock_pledge": "asset/cn_stock_pledge/{year}.parquet",
        "cn_stock_research_visits": "asset/cn_stock_research_visits/{year}.parquet",
        # 股票逐笔与 L2 盘口 (2)
        "cn_stock_tick": "asset/cn_stock_tick/{date}.parquet",
        "cn_stock_tick_l2": "asset/cn_stock_tick_l2/{date}.parquet",
        # 独立 ETF 域 (5)
        "cn_etf_basic": "asset/cn_etf_basic/etf_basic.parquet",
        "cn_etf_bars_daily": "asset/cn_etf_bars_daily/{date}.parquet",
        "cn_etf_bars_minute": "asset/cn_etf_bars_minute/{date}.parquet",
        "cn_etf_fundamental": "asset/cn_etf_fundamental/{month}.parquet",
        "cn_etf_tick": "asset/cn_etf_tick/{date}.parquet",
        # 场外与上市公募基金 (5)
        "cn_fund_basic": "asset/cn_fund_basic/fund_basic.parquet",
        "cn_fund_managers": "asset/cn_fund_managers/managers.parquet",
        "cn_fund_nav": "asset/cn_fund_nav/{month}.parquet",
        "cn_fund_dividend": "asset/cn_fund_dividend/dividend.parquet",
        "cn_fund_portfolio": "asset/cn_fund_portfolio/{year}.parquet",
        # 期货 (4) 与因子 (1)
        "cn_futures_bars_daily": "asset/cn_futures_bars_daily/{date}.parquet",
        "cn_futures_tick_l2": "asset/cn_futures_tick_l2/{date}.parquet",
        "cn_futures_contract_specs": "asset/cn_futures_contract_specs/contract_specs.parquet",
        "cn_futures_continuous": "asset/cn_futures_continuous/{symbol}.parquet",
        "factor": "factors/cn_stock_factors/{name}_v{version}/{year}.parquet",
    }
)

TABLE_FAMILY: dict[str, str] = _AliasedDict(
    {
        "cn_calendar": "reference",
        "cn_stock_basic": "stock",
        "cn_stock_bars_daily": "stock",
        "cn_stock_bars_minute": "stock",
        "cn_stock_fundamental": "stock",
        "cn_stock_daily_status": "stock",
        "cn_stock_corporate_actions": "stock",
        "cn_stock_index_basic": "stock",
        "cn_stock_index_bars_daily": "stock",
        "cn_stock_industry": "stock",
        "cn_stock_index_weights": "stock",
        "cn_stock_fundamental_income": "stock",
        "cn_stock_fundamental_balance": "stock",
        "cn_stock_fundamental_cashflow": "stock",
        "cn_stock_fundamental_indicator": "stock",
        "cn_stock_fundamental_segment": "stock",
        "cn_stock_fundamental_earnings": "stock",
        "cn_stock_fundamental_consensus": "stock",
        "cn_stock_auction": "stock",
        "cn_stock_moneyflow": "stock",
        "cn_stock_hsgt_market_flow": "stock",
        "cn_stock_hsgt_hold": "stock",
        "cn_stock_margin": "stock",
        "cn_stock_lhb": "stock",
        "cn_stock_lhb_seats": "stock",
        "cn_stock_limit_pool": "stock",
        "cn_stock_chips_perf": "stock",
        "cn_stock_holder_num": "stock",
        "cn_stock_top_holders": "stock",
        "cn_stock_restricted_release": "stock",
        "cn_stock_block_trade": "stock",
        "cn_stock_repurchase": "stock",
        "cn_stock_holder_trade": "stock",
        "cn_stock_pledge": "stock",
        "cn_stock_research_visits": "stock",
        "cn_stock_tick": "stock",
        "cn_stock_tick_l2": "stock",
        "cn_etf_basic": "etf",
        "cn_etf_bars_daily": "etf",
        "cn_etf_bars_minute": "etf",
        "cn_etf_fundamental": "etf",
        "cn_etf_tick": "etf",
        "cn_fund_basic": "fund",
        "cn_fund_managers": "fund",
        "cn_fund_nav": "fund",
        "cn_fund_dividend": "fund",
        "cn_fund_portfolio": "fund",
        "cn_futures_bars_daily": "futures",
        "cn_futures_tick_l2": "futures",
        "cn_futures_contract_specs": "futures",
        "cn_futures_continuous": "continuous",
        "factor": "stock",
    }
)

SCHEMA: dict[str, DataFrameSchema] = _AliasedDict(
    {
        "cn_calendar": CALENDAR_SCHEMA,
        "cn_stock_basic": CN_STOCK_BASIC_SCHEMA,
        "cn_stock_bars_daily": BARS_DAILY_SCHEMA,
        "cn_stock_bars_minute": BARS_MINUTE_SCHEMA,
        "cn_stock_fundamental": FUNDAMENTAL_SCHEMA,
        "cn_stock_daily_status": CN_STOCK_DAILY_STATUS_SCHEMA,
        "cn_stock_corporate_actions": CORPORATE_ACTIONS_SCHEMA,
        "cn_stock_index_basic": CN_STOCK_INDEX_BASIC_SCHEMA,
        "cn_stock_index_bars_daily": CN_STOCK_INDEX_BARS_DAILY_SCHEMA,
        "cn_stock_industry": INDUSTRY_SCHEMA,
        "cn_stock_index_weights": CN_STOCK_INDEX_WEIGHTS_SCHEMA,
        "cn_stock_fundamental_income": CN_STOCK_FUNDAMENTAL_INCOME_SCHEMA,
        "cn_stock_fundamental_balance": CN_STOCK_FUNDAMENTAL_BALANCE_SCHEMA,
        "cn_stock_fundamental_cashflow": CN_STOCK_FUNDAMENTAL_CASHFLOW_SCHEMA,
        "cn_stock_fundamental_indicator": CN_STOCK_FUNDAMENTAL_INDICATOR_SCHEMA,
        "cn_stock_fundamental_segment": CN_STOCK_FUNDAMENTAL_SEGMENT_SCHEMA,
        "cn_stock_fundamental_earnings": CN_STOCK_FUNDAMENTAL_EARNINGS_SCHEMA,
        "cn_stock_fundamental_consensus": CN_STOCK_FUNDAMENTAL_CONSENSUS_SCHEMA,
        "cn_stock_auction": CN_STOCK_AUCTION_SCHEMA,
        "cn_stock_moneyflow": CN_STOCK_MONEYFLOW_SCHEMA,
        "cn_stock_hsgt_market_flow": CN_STOCK_HSGT_MARKET_FLOW_SCHEMA,
        "cn_stock_hsgt_hold": CN_STOCK_HSGT_HOLD_SCHEMA,
        "cn_stock_margin": CN_STOCK_MARGIN_SCHEMA,
        "cn_stock_lhb": CN_STOCK_LHB_SCHEMA,
        "cn_stock_lhb_seats": CN_STOCK_LHB_SEATS_SCHEMA,
        "cn_stock_limit_pool": CN_STOCK_LIMIT_POOL_SCHEMA,
        "cn_stock_chips_perf": CN_STOCK_CHIPS_PERF_SCHEMA,
        "cn_stock_holder_num": CN_STOCK_HOLDER_NUM_SCHEMA,
        "cn_stock_top_holders": CN_STOCK_TOP_HOLDERS_SCHEMA,
        "cn_stock_restricted_release": CN_STOCK_RESTRICTED_RELEASE_SCHEMA,
        "cn_stock_block_trade": CN_STOCK_BLOCK_TRADE_SCHEMA,
        "cn_stock_repurchase": CN_STOCK_REPURCHASE_SCHEMA,
        "cn_stock_holder_trade": CN_STOCK_HOLDER_TRADE_SCHEMA,
        "cn_stock_pledge": CN_STOCK_PLEDGE_SCHEMA,
        "cn_stock_research_visits": CN_STOCK_RESEARCH_VISITS_SCHEMA,
        "cn_stock_tick": CN_STOCK_TICK_SCHEMA,
        "cn_stock_tick_l2": CN_STOCK_TICK_L2_SCHEMA,
        "cn_etf_basic": CN_ETF_BASIC_SCHEMA,
        "cn_etf_bars_daily": CN_ETF_BARS_DAILY_SCHEMA,
        "cn_etf_bars_minute": CN_ETF_BARS_MINUTE_SCHEMA,
        "cn_etf_fundamental": CN_ETF_FUNDAMENTAL_SCHEMA,
        "cn_etf_tick": CN_ETF_TICK_SCHEMA,
        "cn_fund_basic": CN_FUND_BASIC_SCHEMA,
        "cn_fund_managers": CN_FUND_MANAGERS_SCHEMA,
        "cn_fund_nav": CN_FUND_NAV_SCHEMA,
        "cn_fund_dividend": CN_FUND_DIVIDEND_SCHEMA,
        "cn_fund_portfolio": CN_FUND_PORTFOLIO_SCHEMA,
        "cn_futures_bars_daily": BARS_DAILY_FUTURES_SCHEMA,
        "cn_futures_tick_l2": TICK_FUTURES_L2_SCHEMA,
        "cn_futures_contract_specs": CONTRACT_SPECS_SCHEMA,
        "cn_futures_continuous": CONTINUOUS_SCHEMA,
        "factor": FACTOR_SCHEMA,
    }
)

A_SHARE_TABLES: frozenset[str] = frozenset(
    k for k in SCHEMA if not k.startswith("cn_futures_") and k != "factor"
)

# --- 因子 DuckDB 视图命名 ---
_non_alnum = re.compile(r"[^0-9a-zA-Z_]")


def factor_view_name(factor_name: str) -> str:
    """返回因子对应的 DuckDB 视图名称。"""
    sanitized = _non_alnum.sub("_", factor_name)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return f"factor_{sanitized}"


class FactorViewRegistry:
    """防冲突的因子名与 DuckDB 视图名注册表。"""

    def __init__(self) -> None:
        self._views: dict[str, str] = {}

    def register(self, factor_name: str) -> str:
        """注册因子名称并返回对应的视图名称。"""
        view = factor_view_name(factor_name)
        existing = self._views.get(view)
        if existing is not None and existing != factor_name:
            raise ValueError(
                f"factor view name collision: {existing!r} and {factor_name!r} "
                f"both map to {view!r}"
            )
        self._views[view] = factor_name
        return view

    @property
    def views(self) -> dict[str, str]:
        return dict(self._views)
