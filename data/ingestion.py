"""多源数据接入仲裁与交叉校验。

核心原则：资产层仅由主源写入，备源仅用于交叉校验与缺口回补，严禁混合写入以保持血缘单一可追溯。
交叉校验在 Normalizer 标准化后的 DataFrame 上执行：
- ``should_cross_check``：仅当主源入库完成且全部备源原始批次均已落地时触发；无备源表不触发。
- ``raw_exists``：按原始落地文件路径检查是否到达，不解析 watermark。
- ``NUMERIC_FIELDS``：按表定义参与交叉对比的数值列，单源或静态元数据表映射为 ``()``。
- ``pe_pb`` 容差类覆盖 ``pe_ttm`` / ``pe_lyr`` / ``pb`` / ``ps_ttm`` 等估值比率。
- ``field_override`` 指定的源必须包含在同一条目的 ``priority`` 列表中。
"""

from __future__ import annotations

import copy
import fnmatch
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from core.config import DATA_ROOT
from data.raw import raw_path
from data.storage_ops import (
    DiskFullError,
    FileLock,
    check_capacity,
    is_monthly_partition,
    normalize_missing_floats,
    partition_path,
    upsert_parquet_atomic,
    write_parquet_atomic,
)
from data.storage_schema import (
    CANONICAL_TO_LEGACY,
    PARTITION_PATHS,
    SCHEMA,
    _AliasedDict,
    resolve_table_name,
)

__all__ = [
    "SOURCE_PRIORITY_PATH",
    "NUMERIC_FIELDS",
    "TOLERANCE_CLASS",
    "TOLERANCE_CLASSES",
    "MismatchDetail",
    "CrossCheckResult",
    "ResponseLevel",
    "load_source_priority",
    "load_tolerances",
    "load_tolerance_overrides",
    "load_exemptions",
    "get_source_priority",
    "get_primary_source",
    "get_field_overrides",
    "tolerance_for",
    "_ticker_in_group",
    "cross_source_check",
    "cross_source_check_aggregate",
    "response_level",
    "respond",
    "backfill_source_tag",
    "write_asset",
    "raw_exists",
    "should_cross_check",
    "TABLE_TO_ADAPTER_DATASET",
    "TABLE_TO_DQC_GROUP",
    "IngestOutcome",
    "cross_check_path",
    "save_cross_check_result",
    "run_cross_check_for_date",
    "ingest_table",
    "run_ingestion",
]

logger = logging.getLogger(__name__)

_REPO_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "source_priority.yaml"
)
SOURCE_PRIORITY_PATH = DATA_ROOT / "config" / "source_priority.yaml"
_SOURCE_PRIORITY_CACHE: dict[Path, tuple[int, dict[str, Any]]] = {}


def _resolve_source_priority_path(path: str | os.PathLike[str] | None = None) -> Path:
    if path:
        return Path(path)
    if SOURCE_PRIORITY_PATH.is_file():
        return SOURCE_PRIORITY_PATH
    return _REPO_CONFIG_PATH


# --- 参与交叉校验的数值字段（状态标志列不参与；通过 _AliasedDict 兼容旧表名） ---
NUMERIC_FIELDS: dict[str, tuple[str, ...]] = _AliasedDict(
    {
        # 交易日历 (1)
        "cn_calendar": (),
        # 股票核心行情与状态 (6)
        "cn_stock_basic": (),
        "cn_stock_bars_daily": (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "adjfactor",
            "turnover",
        ),
        "cn_stock_bars_minute": ("open", "high", "low", "close", "volume", "amount"),
        "cn_stock_fundamental": (
            "total_market_cap",
            "float_market_cap",
            "pe_ttm",
            "pe_lyr",
            "pb",
            "ps_ttm",
            "total_shares",
            "float_shares",
        ),
        "cn_stock_daily_status": ("pre_close", "up_limit", "down_limit", "limit_pct"),
        "cn_stock_corporate_actions": (
            "cash_dividend",
            "stock_dividend",
            "rights_price",
            "rights_ratio",
        ),
        # 指数 / 行业 / 成分权重 (4)
        "cn_stock_index_basic": (),
        "cn_stock_index_bars_daily": (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "pe",
            "pb",
            "turnover",
            "total_market_cap",
            "float_market_cap",
        ),
        "cn_stock_industry": (),
        "cn_stock_index_weights": ("weight",),
        # 股票基本面 PIT 表 (7)
        "cn_stock_fundamental_income": (
            "basic_eps",
            "diluted_eps",
            "total_revenue",
            "revenue",
            "total_cogs",
            "operate_profit",
            "total_profit",
            "net_profit",
            "net_profit_attr_p",
            "net_profit_dedt",
            "rd_exp",
            "ebit",
            "ebitda",
        ),
        "cn_stock_fundamental_balance": (
            "total_assets",
            "total_cur_assets",
            "total_nca",
            "money_cap",
            "accounts_receiv",
            "inventories",
            "total_liab",
            "total_cur_liab",
            "total_ncl",
            "total_hldr_eqy_exc_min_int",
            "total_hldr_eqy_inc_min_int",
            "total_share",
        ),
        "cn_stock_fundamental_cashflow": (
            "n_cashflow_act",
            "n_cashflow_inv_act",
            "n_cash_flows_fnc_act",
            "c_fr_sale_sg",
            "c_paid_to_for_empl",
            "c_pay_acq_const_fiolta",
            "free_cashflow",
            "n_incr_cash_cash_equ",
        ),
        "cn_stock_fundamental_indicator": (
            "roe",
            "roe_waa",
            "roe_dt",
            "roa",
            "roic",
            "grossprofit_margin",
            "netprofit_margin",
            "debt_to_assets",
            "current_ratio",
            "quick_ratio",
            "assets_turn",
            "netprofit_yoy",
            "or_yoy",
        ),
        "cn_stock_fundamental_segment": (
            "bz_sales",
            "bz_profit",
            "bz_cost",
            "sales_ratio",
            "gross_margin",
        ),
        "cn_stock_fundamental_earnings": (
            "p_change_min",
            "p_change_max",
            "net_profit_min",
            "net_profit_max",
            "revenue",
            "net_profit",
            "eps",
            "roe",
        ),
        "cn_stock_fundamental_consensus": (
            "eps_fy0",
            "eps_fy1",
            "eps_fy2",
            "eps_fwd12m",
            "eps_fy1_n",
            "eps_fy1_std",
            "rating_score",
            "tp_median",
        ),
        # 资金流 / 情绪 / 筹码 (9)
        "cn_stock_auction": (
            "price",
            "pre_close",
            "pct_chg",
            "volume",
            "amount",
            "turnover_rate",
            "volume_ratio",
        ),
        "cn_stock_moneyflow": (
            "buy_elg_amount",
            "sell_elg_amount",
            "buy_lg_amount",
            "sell_lg_amount",
            "buy_md_amount",
            "sell_md_amount",
            "buy_sm_amount",
            "sell_sm_amount",
            "net_mf_amount",
            "net_mf_pct",
        ),
        "cn_stock_hsgt_market_flow": (
            "ggt_ss",
            "ggt_sz",
            "hgt",
            "sgt",
            "north_money",
            "south_money",
        ),
        "cn_stock_hsgt_hold": ("vol", "ratio"),
        "cn_stock_margin": (
            "rzye",
            "rzmre",
            "rzche",
            "rqyl",
            "rqmcl",
            "rqchl",
            "rqye",
            "rzrqye",
        ),
        "cn_stock_lhb": (
            "close",
            "pct_chg",
            "turnover_rate",
            "l_buy",
            "l_sell",
            "net_amount",
            "net_rate",
        ),
        "cn_stock_lhb_seats": ("buy", "sell", "net_buy"),
        "cn_stock_limit_pool": ("open_times", "streak_days", "seal_amount"),
        "cn_stock_chips_perf": (
            "his_low",
            "his_high",
            "cost_5pct",
            "cost_15pct",
            "cost_50pct",
            "cost_85pct",
            "cost_95pct",
            "weight_avg",
            "winner_rate",
        ),
        # 股东与资本运作事件 (8)
        "cn_stock_holder_num": ("holder_num", "holder_num_chg_pct", "avg_hold_shares"),
        "cn_stock_top_holders": ("hold_amount", "hold_ratio", "hold_change"),
        "cn_stock_restricted_release": ("float_share", "float_ratio"),
        "cn_stock_block_trade": ("price", "vol", "amount", "discount_rate"),
        "cn_stock_repurchase": ("vol", "amount", "high_limit", "low_limit"),
        "cn_stock_holder_trade": (
            "change_vol",
            "change_ratio",
            "after_share",
            "avg_price",
        ),
        "cn_stock_pledge": ("pledge_amount", "pledge_ratio"),
        "cn_stock_research_visits": (),
        # 股票逐笔与 L2 盘口 (2)
        "cn_stock_tick": ("price", "volume", "amount"),
        "cn_stock_tick_l2": ("last_price", "volume", "amount"),
        # 独立 ETF 域 (5)
        "cn_etf_basic": ("m_fee", "c_fee"),
        "cn_etf_bars_daily": (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "adjfactor",
            "turnover",
        ),
        "cn_etf_bars_minute": ("open", "high", "low", "close", "volume", "amount"),
        "cn_etf_fundamental": (
            "unit_nav",
            "accum_nav",
            "nav_adj_factor",
            "iopv",
            "total_shares",
            "total_nav",
        ),
        "cn_etf_tick": ("price", "volume", "amount"),
        # 场外与上市公募基金 (5)
        "cn_fund_basic": ("m_fee", "c_fee"),
        "cn_fund_managers": (),
        "cn_fund_nav": (
            "unit_nav",
            "accum_nav",
            "adj_nav",
            "nav_adj_factor",
            "total_shares",
            "total_netasset",
        ),
        "cn_fund_dividend": ("div_cash",),
        "cn_fund_portfolio": ("mkv", "amount", "stk_mkv_ratio", "stk_float_ratio"),
        # 期货 (4)
        "cn_futures_bars_daily": (
            "open",
            "high",
            "low",
            "close",
            "settle",
            "volume",
            "amount",
            "open_interest",
        ),
        "cn_futures_tick_l2": ("last_price", "volume", "amount", "open_interest"),
        "cn_futures_contract_specs": (),
        "cn_futures_continuous": (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "open_interest",
            "settle",
            "adjfactor",
        ),
        # 旧版兼容条目
        "option_basic": (),
        "reference": (),
    }
)

# 字段名 -> 容差类别（未列出字段回落到 "default"）
TOLERANCE_CLASS: dict[str, str] = {
    "open": "price",
    "high": "price",
    "low": "price",
    "close": "price",
    "pre_close": "price",
    "settle": "price",
    "price": "price",
    "last_price": "price",
    "up_limit": "price",
    "down_limit": "price",
    "unit_nav": "price",
    "accum_nav": "price",
    "adj_nav": "price",
    "iopv": "price",
    "volume": "volume",
    "vol": "volume",
    "amount": "amount",
    "adjfactor": "adjfactor",
    "nav_adj_factor": "adjfactor",
    "pe": "pe_pb",
    "pe_ttm": "pe_pb",
    "pe_lyr": "pe_pb",
    "pb": "pe_pb",
    "ps_ttm": "pe_pb",
}

# 五级容差类别：阈值配置于 source_priority.yaml（cross_check.tolerances）；price 与 adjfactor 采用绝对误差，其余采用相对主源误差
TOLERANCE_CLASSES: tuple[str, ...] = (
    "price",
    "volume",
    "amount",
    "adjfactor",
    "pe_pb",
    "default",
)
_ABSOLUTE_CLASSES = frozenset({"price", "adjfactor"})

# 分级响应阈值：<0.1% 静默记录，0.1%–1% WARNING，>1% ERROR；adjfactor 不一致至少触发 WARNING
ResponseLevel = Literal["silent", "WARNING", "ERROR"]
SILENT_BAND_MAX = 0.001
ERROR_BAND_MIN = 0.01


# --- source_priority.yaml 加载与校验 ---
def load_source_priority(
    path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """加载并校验数据源仲裁配置。

    要求每项包含 ``priority`` 列表（支持嵌套子表），且 ``field_override`` 指定的源必须在对应 ``priority`` 中，否则抛出 ``ValueError``。
    """
    resolved = _resolve_source_priority_path(path).resolve()
    mtime = resolved.stat().st_mtime_ns
    cached = _SOURCE_PRIORITY_CACHE.get(resolved)
    if cached is not None and cached[0] == mtime:
        return copy.deepcopy(cached[1])
    cfg = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise ValueError(f"{SOURCE_PRIORITY_PATH}: top level must be a mapping")
    for table, entry in cfg.items():
        if table == "cross_check":
            continue
        _validate_entry(str(table), entry)
    _SOURCE_PRIORITY_CACHE[resolved] = (mtime, cfg)
    return copy.deepcopy(cfg)


def _validate_entry(table: str, entry: Any) -> None:
    if not isinstance(entry, dict):
        raise ValueError(f"{table}: entry must be a mapping")
    if isinstance(entry.get("priority"), list):
        _validate_table(table, entry)
        return
    for sub, sub_entry in entry.items():
        if not isinstance(sub_entry, dict) or not isinstance(
            sub_entry.get("priority"), list
        ):
            raise ValueError(f"{table}.{sub}: needs a priority list")
        _validate_table(f"{table}.{sub}", sub_entry)


def _validate_table(name: str, entry: dict) -> None:
    priority = entry["priority"]
    if not all(isinstance(s, str) and s for s in priority):
        raise ValueError(f"{name}: priority must be a list of source names")
    overrides = entry.get("field_override") or {}
    if not isinstance(overrides, dict):
        raise ValueError(f"{name}: field_override must be a mapping")
    for field, source in overrides.items():
        if source not in priority:
            raise ValueError(
                f"{name}: field_override {field!r} names {source!r}, "
                f"which is not in priority {priority}"
            )


def _lookup_key(cfg: dict, name: str) -> str | None:
    if name in cfg:
        return name
    canonical = resolve_table_name(name)
    if canonical in cfg:
        return canonical
    legacy = CANONICAL_TO_LEGACY.get(name)
    if legacy is not None and legacy in cfg:
        return legacy
    return None


def _entry_for(cfg: dict, table: str, sub_type: str | None) -> dict:
    key = _lookup_key(cfg, table)
    if key is not None:
        entry = cfg[key]
        if sub_type is not None:
            sub_key = _lookup_key(entry, sub_type)
            if sub_key is None:
                raise KeyError(sub_type)
            entry = entry[sub_key]
    elif table == "reference" and sub_type is not None:
        sub_key = _lookup_key(cfg, sub_type)
        if sub_key is None:
            raise KeyError(sub_type)
        entry = cfg[sub_key]
    else:
        raise KeyError(table)
    if not isinstance(entry, dict) or not isinstance(entry.get("priority"), list):
        raise ValueError(f"{table}: per-table entry — pass sub_type")
    return entry


def load_tolerances(
    path: str | os.PathLike[str] | None = None,
) -> dict[str, float]:
    """从 ``cross_check.tolerances`` 读取五级容差阈值表。"""
    cfg = load_source_priority(path)
    section = cfg.get("cross_check") or {}
    tolerances = section.get("tolerances") or {}
    missing = [c for c in TOLERANCE_CLASSES if c not in tolerances]
    if missing:
        raise ValueError(f"cross_check.tolerances missing classes: {missing}")
    return {c: float(tolerances[c]) for c in TOLERANCE_CLASSES}


_BOND_INDEX_TICKERS = frozenset(
    {
        "000012.SH",
        "000013.SH",
        "000022.SH",
        "000061.SH",
        "000101.SH",
        "000116.SH",
        "399481.SZ",
    }
)
_CROSS_BORDER_TICKERS = frozenset({"000170.SH", "000867.SH", "000869.SH"})
_CROSS_BORDER_PREFIXES = ("HSI", "HSCEI", "HSTECH", "SPX", "NDX")
_EQUITY_INDEX_PREFIXES = ("000", "399", "899", "93", "97", "98", "47", "48")

_AGGREGATE_CROSS_CHECK_TABLES = frozenset(
    {
        "cn_stock_bars_minute",
        "cn_etf_bars_minute",
        "cn_stock_tick",
        "cn_stock_tick_l2",
        "cn_etf_tick",
        "cn_futures_tick_l2",
    }
)


def _cross_check_section(
    path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    cfg = load_source_priority(path)
    section = cfg.get("cross_check")
    return dict(section) if isinstance(section, dict) else {}


def load_tolerance_overrides(
    asset_type: str,
    *,
    path: str | os.PathLike[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """读取 ``cross_check.tolerance_overrides`` 中指定表的按子类分级容差配置。"""
    section = _cross_check_section(path)
    all_overrides = section.get("tolerance_overrides") or {}
    if not isinstance(all_overrides, dict):
        return {}
    key = _lookup_key(all_overrides, asset_type)
    if key is None:
        return {}
    table_overrides = all_overrides.get(key)
    if not isinstance(table_overrides, dict):
        return {}
    return {
        str(group): dict(cfg)
        for group, cfg in table_overrides.items()
        if isinstance(cfg, dict)
    }


def _date_matches_spec(target_date: str, spec: str) -> bool:
    """判断 ``YYYY-MM-DD`` 日期字符串是否命中单日或 ``start..end`` 闭区间规则。"""
    s = spec.strip()
    if ".." in s:
        start_s, end_s = s.split("..", 1)
        start_s, end_s = start_s.strip(), end_s.strip()
        if start_s and target_date < start_s:
            return False
        if end_s and target_date > end_s:
            return False
        return bool(start_s or end_s)
    return target_date == s


def load_exemptions(
    asset_type: str,
    *,
    trade_date: str | date | None = None,
    path: str | os.PathLike[str] | None = None,
) -> dict[str, set[str]]:
    """读取 ``cross_check.exemptions`` 中指定表的 ``{ticker: set(fields)}`` 口径豁免映射。

    同时支持按 ``"YYYY-MM-DD"`` 或 ``"YYYY-MM-DD..YYYY-MM-DD"`` 嵌套定义仅针对特定历史日期段的豁免。
    """
    section = _cross_check_section(path)
    all_exemptions = section.get("exemptions") or {}
    if not isinstance(all_exemptions, dict):
        return {}
    key = _lookup_key(all_exemptions, asset_type)
    if key is None:
        return {}
    table_exemptions = all_exemptions.get(key) or {}
    if not isinstance(table_exemptions, dict):
        return {}
    date_str = _as_date_str(trade_date)[:10] if trade_date is not None else ""
    result: dict[str, set[str]] = {}

    def _merge(ticker: str, fields: Any) -> None:
        t = str(ticker)
        result.setdefault(t, set()).update(str(f) for f in (fields or ()))

    for entry_key, entry_val in table_exemptions.items():
        if isinstance(entry_val, dict):
            if date_str and _date_matches_spec(date_str, str(entry_key)):
                for ticker, fields in entry_val.items():
                    _merge(str(ticker), fields)
        else:
            _merge(str(entry_key), entry_val)
    return result


def _load_skip_rules(
    asset_type: str,
    *,
    path: str | os.PathLike[str] | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    section = _cross_check_section(path)
    skip_tickers_map = section.get("skip_tickers") or {}
    skip_suffixes_map = section.get("skip_suffixes") or {}
    tickers: list[str] = []
    suffixes: list[str] = []
    if isinstance(skip_tickers_map, dict):
        key = _lookup_key(skip_tickers_map, asset_type)
        if key is not None:
            tickers = [str(t) for t in (skip_tickers_map.get(key) or ())]
    if isinstance(skip_suffixes_map, dict):
        key = _lookup_key(skip_suffixes_map, asset_type)
        if key is not None:
            suffixes = [str(s) for s in (skip_suffixes_map.get(key) or ())]
    return tuple(tickers), tuple(suffixes)


def _match_patterns(series: pd.Series, patterns: Sequence[str]) -> pd.Series:
    """按精确代码或 ``fnmatch`` 通配符（如 ``*.TI``、``899*.BJ``）匹配标的代码。"""
    if not patterns or series.empty:
        return pd.Series(False, index=series.index, dtype=bool)
    s = series.astype(str)
    exact = {p for p in patterns if not any(ch in p for ch in "*?[")}
    globs = [p for p in patterns if any(ch in p for ch in "*?[")]
    mask = s.isin(exact) if exact else pd.Series(False, index=series.index, dtype=bool)
    if globs:
        matched_unique = {
            val
            for val in s.unique()
            if any(fnmatch.fnmatchcase(val, pat) for pat in globs)
        }
        if matched_unique:
            mask = mask | s.isin(matched_unique)
    return mask


def _apply_skip_rules(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    canonical: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    skip_tickers, skip_suffixes = _load_skip_rules(canonical)
    if not skip_tickers and not skip_suffixes:
        return df_a, df_b

    def _keep_mask(series: pd.Series) -> pd.Series:
        s = series.astype(str)
        mask = pd.Series(True, index=series.index, dtype=bool)
        if skip_tickers:
            mask = mask & ~_match_patterns(s, skip_tickers)
        if skip_suffixes:
            mask = mask & ~s.str.endswith(skip_suffixes)
        return mask

    return df_a.loc[_keep_mask(df_a["ticker"])], df_b.loc[_keep_mask(df_b["ticker"])]


def _is_exempt(
    tickers: pd.Series,
    field: str,
    exemptions: Mapping[str, set[str]],
) -> pd.Series:
    """判断每个标的在指定字段上是否命中口径豁免（支持精确匹配与通配模式）。"""
    if not exemptions or tickers.empty:
        return pd.Series(False, index=tickers.index, dtype=bool)
    patterns = [
        pat for pat, fields in exemptions.items() if field in fields or "*" in fields
    ]
    return _match_patterns(tickers, patterns)


def _ticker_in_group(
    ticker: str,
    group_name: str,
    asset_type: str,
    *,
    group_cfg: Mapping[str, Any] | None = None,
) -> bool:
    """判定 *ticker* 是否属于 *asset_type* 下的指定子类 *group_name*。"""
    del asset_type
    t = str(ticker)
    if group_cfg is not None:
        explicit_tickers = group_cfg.get("tickers")
        explicit_prefixes = group_cfg.get("prefixes")
        explicit_suffixes = group_cfg.get("suffixes")
        has_explicit = any(
            x is not None
            for x in (explicit_tickers, explicit_prefixes, explicit_suffixes)
        )
        if has_explicit:
            if explicit_tickers and t in {str(x) for x in explicit_tickers}:
                return True
            if explicit_prefixes and t.startswith(
                tuple(str(p) for p in explicit_prefixes)
            ):
                return True
            if explicit_suffixes and t.endswith(
                tuple(str(s) for s in explicit_suffixes)
            ):
                return True
            return False
    if group_name == "bond":
        return t in _BOND_INDEX_TICKERS
    if group_name == "cross_border":
        return t in _CROSS_BORDER_TICKERS or t.startswith(_CROSS_BORDER_PREFIXES)
    if group_name == "equity":
        if (
            t in _BOND_INDEX_TICKERS
            or t in _CROSS_BORDER_TICKERS
            or t.startswith(_CROSS_BORDER_PREFIXES)
        ):
            return False
        return t.startswith(_EQUITY_INDEX_PREFIXES) or t.endswith(
            (".SH", ".SZ", ".BJ", ".CSI")
        )
    return False


def get_source_priority(
    table: str,
    sub_type: str | None = None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> list[str]:
    """返回 *table* 按优先级排序的数据源列表（首位为主源）。"""
    cfg = load_source_priority(path)
    return list(_entry_for(cfg, table, sub_type)["priority"])


def get_primary_source(
    table: str,
    sub_type: str | None = None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> str:
    """返回允许写入 *table* 资产层的唯一主源。"""
    priority = get_source_priority(table, sub_type, path=path)
    if not priority:
        raise ValueError(f"no source assigned for {table}/{sub_type or '-'}")
    return priority[0]


def get_field_overrides(
    table: str,
    sub_type: str | None = None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> dict[str, str]:
    """返回 *table* 的字段级数据源覆盖映射（无则返回 ``{}``）。"""
    cfg = load_source_priority(path)
    entry = _entry_for(cfg, table, sub_type)
    return dict(entry.get("field_override") or {})


# --- 五级容差查询 ---
def _resolve_tolerance_from_parts(
    field: str,
    base_table: Mapping[str, float],
    overrides: Mapping[str, Mapping[str, Any]],
    asset_type: str | None = None,
    ticker: str | None = None,
) -> tuple[float, str]:
    cls = TOLERANCE_CLASS.get(field, "default")
    value = float(base_table[cls] if cls in base_table else base_table["default"])
    mode = "absolute" if cls in _ABSOLUTE_CLASSES else "relative"

    if asset_type and ticker and overrides:
        for group_name, group_tols in overrides.items():
            if _ticker_in_group(ticker, group_name, asset_type, group_cfg=group_tols):
                override_val = group_tols.get(field)
                if override_val is None:
                    override_val = group_tols.get(cls)
                if isinstance(override_val, (int, float)) and not isinstance(
                    override_val, bool
                ):
                    return float(override_val), mode
    return value, mode


def tolerance_for(
    field: str,
    tolerances: Mapping[str, float] | None = None,
    *,
    asset_type: str | None = None,
    ticker: str | None = None,
    path: str | os.PathLike[str] | None = None,
) -> tuple[float, str]:
    """返回 *field* 的 ``(容差阈值, 比较模式)``（``absolute`` 或 ``relative``）；支持按资产子类覆盖容差。"""
    table = dict(tolerances) if tolerances is not None else load_tolerances(path)
    overrides = (
        load_tolerance_overrides(asset_type, path=path) if asset_type and ticker else {}
    )
    return _resolve_tolerance_from_parts(
        field, table, overrides, asset_type=asset_type, ticker=ticker
    )


# --- 交叉校验数据结构 ---
@dataclass
class MismatchDetail:
    """单字段不一致聚合统计。"""

    field: str
    mismatch_count: int
    mismatch_rate: float  # 不一致行数 / 匹配总行数
    max_abs_diff: float
    sample_tickers: list[str]  # 前 5 个不一致标的样本


@dataclass
class CrossCheckResult:
    """单次 ``(trade_date, table)`` 截面交叉校验结果。

    ``total_rows`` 为外连接总行数，``matched_rows`` 为双源共有行数；
    ``missing_in_primary`` 为仅备源存在的行数，``missing_in_secondary`` 为仅主源存在的行数。
    """

    table: str
    trade_date: str
    primary_source: str
    secondary_source: str
    total_rows: int
    matched_rows: int
    mismatched_fields: dict[str, MismatchDetail]
    missing_in_primary: int
    missing_in_secondary: int

    @property
    def mismatch_rate(self) -> float:
        """各字段中最高的不一致率（全部匹配时为 ``0.0``）。"""
        if not self.mismatched_fields:
            return 0.0
        return max(d.mismatch_rate for d in self.mismatched_fields.values())


# --- 截面交叉校验 ---
def cross_source_check(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    table: str,
    trade_date: str | date | None = None,
    tolerances: Mapping[str, float] | None = None,
    *,
    secondary_source: str | None = None,
) -> CrossCheckResult:
    """对比同一存储表经 Normalizer 标准化后的主备源截面。

    ``df_a`` 为主源数据，``df_b`` 为备源数据；按 ``[ticker, trade_date]``（无日期列时按 ``[ticker]``）外连接。
    依 :data:`NUMERIC_FIELDS` 与五级容差表逐列比较，超出容差或单边缺失判为不一致（双边均空视为一致）。
    """
    if resolve_table_name(table) == "cn_calendar":
        return _cross_check_calendar(
            df_a,
            df_b,
            table=table,
            trade_date=trade_date,
            secondary_source=secondary_source,
        )
    if "ticker" not in df_a.columns or "ticker" not in df_b.columns:
        raise ValueError("both frames need a 'ticker' join column")
    fields = NUMERIC_FIELDS.get(table)
    if fields is None:
        raise KeyError(
            f"no NUMERIC_FIELDS entry for {table!r} — define it in "
            "data/ingestion.py before cross-checking it"
        )
    if {"ticker", "ex_date", "event_type"} <= set(df_a.columns) and {
        "ticker",
        "ex_date",
        "event_type",
    } <= set(df_b.columns):
        join_keys = ["ticker", "ex_date", "event_type"]
    elif {"ticker", "ex_date"} <= set(df_a.columns) and {
        "ticker",
        "ex_date",
    } <= set(df_b.columns):
        join_keys = ["ticker", "ex_date"]
    elif "trade_date" in df_a.columns and "trade_date" in df_b.columns:
        join_keys = ["ticker", "trade_date"]
    else:
        join_keys = ["ticker"]
    canonical = resolve_table_name(table)
    df_a, df_b = _apply_skip_rules(df_a, df_b, canonical)

    if df_a.duplicated(join_keys).any() or df_b.duplicated(join_keys).any():
        raise ValueError(f"duplicate join keys {join_keys} in input frames")

    merged = df_a.merge(
        df_b, on=join_keys, how="outer", suffixes=("_a", "_b"), indicator=True
    )
    both = merged[merged["_merge"] == "both"]
    matched_rows = int(len(both))
    resolved_trade_date = _as_date_str(
        trade_date if trade_date is not None else _frame_trade_date(df_a, df_b)
    )
    exemptions = load_exemptions(canonical, trade_date=resolved_trade_date or None)
    base_tols = dict(tolerances) if tolerances is not None else load_tolerances()
    table_overrides = load_tolerance_overrides(canonical)

    def _field_tol(field_name: str) -> tuple[float | pd.Series, str]:
        base_tol, mode = _resolve_tolerance_from_parts(field_name, base_tols, {})
        if not table_overrides or both.empty:
            return base_tol, mode
        unique_tickers = both["ticker"].astype(str).unique()
        tol_map = {
            t: _resolve_tolerance_from_parts(
                field_name, base_tols, table_overrides, asset_type=canonical, ticker=t
            )[0]
            for t in unique_tickers
        }
        return both["ticker"].astype(str).map(tol_map).astype("float64"), mode

    _ohlc_nullable_when_zero_vol = frozenset({"open", "high", "low"})
    zero_vol_close_ok: pd.Series | None = None
    cls_a: pd.Series | None = None
    cls_b: pd.Series | None = None
    if {"volume_a", "volume_b", "close_a", "close_b"} <= set(both.columns):
        vol_a = pd.to_numeric(both["volume_a"], errors="coerce")
        vol_b = pd.to_numeric(both["volume_b"], errors="coerce")
        cls_a = pd.to_numeric(both["close_a"], errors="coerce").astype("float64")
        cls_b = pd.to_numeric(both["close_b"], errors="coerce").astype("float64")
        close_tol, close_mode = _field_tol("close")
        close_diff = (cls_a - cls_b).abs()
        if close_mode == "absolute":
            close_ok = close_diff <= close_tol
        else:
            close_ok = (close_diff / cls_a.abs().clip(lower=1e-9)) <= close_tol
        zero_vol_close_ok = (
            (vol_a == 0) & (vol_b == 0) & cls_a.notna() & cls_b.notna() & close_ok
        )

    mismatched: dict[str, MismatchDetail] = {}
    for field in fields:
        col_a, col_b = f"{field}_a", f"{field}_b"
        if col_a not in both.columns or col_b not in both.columns:
            continue  # 仅比较双源均提供的字段
        a = pd.to_numeric(both[col_a], errors="coerce").astype("float64")
        b = pd.to_numeric(both[col_b], errors="coerce").astype("float64")
        na, nb = a.isna(), b.isna()
        valid = ~(na | nb)
        one_missing = na ^ nb
        diff = (a - b).abs()
        tol, mode = _field_tol(field)
        if mode == "absolute":
            bad = valid & (diff > tol)
        else:
            base = a.abs().clip(lower=1e-9)
            bad = valid & ((diff / base) > tol)
        if (
            field in _ohlc_nullable_when_zero_vol
            and zero_vol_close_ok is not None
            and cls_a is not None
            and cls_b is not None
        ):
            if mode == "absolute":
                a_is_close = a.notna() & ((a - cls_a).abs() <= tol)
                b_is_close = b.notna() & ((b - cls_b).abs() <= tol)
            else:
                a_is_close = a.notna() & (
                    ((a - cls_a).abs() / cls_a.abs().clip(lower=1e-9)) <= tol
                )
                b_is_close = b.notna() & (
                    ((b - cls_b).abs() / cls_b.abs().clip(lower=1e-9)) <= tol
                )
            exempt_zero_vol = (
                one_missing & zero_vol_close_ok & (a_is_close | b_is_close)
            )
            bad = bad | (one_missing & ~exempt_zero_vol)
        else:
            bad = bad | one_missing
        is_exempt = pd.Series(False, index=both.index)
        if exemptions and not both.empty:
            is_exempt = _is_exempt(both["ticker"], field, exemptions)
            bad = bad & ~is_exempt
        count = int(bad.sum())
        if not count:
            continue
        valid_non_exempt = valid & ~is_exempt
        mismatched[field] = MismatchDetail(
            field=field,
            mismatch_count=count,
            mismatch_rate=(count / matched_rows) if matched_rows else 0.0,
            max_abs_diff=float(diff[valid_non_exempt].max())
            if bool(valid_non_exempt.any())
            else 0.0,
            sample_tickers=list(both.loc[bad, "ticker"].head(5)),
        )

    return CrossCheckResult(
        table=table,
        trade_date=resolved_trade_date,
        primary_source=_primary_source_or(df_a, table),
        secondary_source=secondary_source or str(df_b.attrs.get("source", "secondary")),
        total_rows=int(len(merged)),
        matched_rows=matched_rows,
        mismatched_fields=mismatched,
        missing_in_primary=int((merged["_merge"] == "right_only").sum()),
        missing_in_secondary=int((merged["_merge"] == "left_only").sum()),
    )


def cross_source_check_aggregate(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    asset_type: str,
    trade_date: str | date | None = None,
    agg_fields: Sequence[str] = ("volume", "amount"),
    group_by: str | Sequence[str] = "ticker",
    tolerances: Mapping[str, float] | None = None,
    *,
    secondary_source: str | None = None,
) -> CrossCheckResult:
    """对分钟线/Tick 等大表按 *group_by* 聚合汇总后比对总量，避免逐行 Join 开销。"""
    group_cols = [group_by] if isinstance(group_by, str) else list(group_by)
    if not group_cols:
        raise ValueError("group_by must not be empty")
    for col in group_cols:
        if col not in df_a.columns or col not in df_b.columns:
            raise ValueError(f"both frames need group_by column {col!r}")

    canonical = resolve_table_name(asset_type)
    if "ticker" in df_a.columns and "ticker" in df_b.columns:
        df_a, df_b = _apply_skip_rules(df_a, df_b, canonical)

    fields = [f for f in agg_fields if f in df_a.columns and f in df_b.columns]

    def _aggregate(frame: pd.DataFrame) -> pd.DataFrame:
        if not fields:
            return frame[group_cols].drop_duplicates().reset_index(drop=True)
        sub = frame[group_cols + fields].copy()
        for f in fields:
            sub[f] = pd.to_numeric(sub[f], errors="coerce")
        if sub.empty:
            return sub.drop_duplicates(subset=group_cols).reset_index(drop=True)
        return (
            sub.groupby(group_cols, sort=False, dropna=False)[fields]
            .sum(min_count=1)
            .reset_index()
        )

    agg_a = _aggregate(df_a)
    agg_b = _aggregate(df_b)
    merged = agg_a.merge(
        agg_b, on=group_cols, how="outer", suffixes=("_a", "_b"), indicator=True
    )
    both = merged[merged["_merge"] == "both"]
    matched_rows = int(len(both))

    resolved_trade_date = _as_date_str(
        trade_date if trade_date is not None else _frame_trade_date(df_a, df_b)
    )
    exemptions = load_exemptions(canonical, trade_date=resolved_trade_date or None)
    base_tols = dict(tolerances) if tolerances is not None else load_tolerances()
    table_overrides = (
        load_tolerance_overrides(canonical) if "ticker" in both.columns else {}
    )

    def _field_tol(field_name: str) -> tuple[float | pd.Series, str]:
        base_tol, mode = _resolve_tolerance_from_parts(field_name, base_tols, {})
        if not table_overrides or both.empty:
            return base_tol, mode
        unique_tickers = both["ticker"].astype(str).unique()
        tol_map = {
            t: _resolve_tolerance_from_parts(
                field_name, base_tols, table_overrides, asset_type=canonical, ticker=t
            )[0]
            for t in unique_tickers
        }
        return both["ticker"].astype(str).map(tol_map).astype("float64"), mode

    sample_col = "ticker" if "ticker" in both.columns else group_cols[0]
    mismatched: dict[str, MismatchDetail] = {}
    for field in fields:
        a = pd.to_numeric(both[f"{field}_a"], errors="coerce").astype("float64")
        b = pd.to_numeric(both[f"{field}_b"], errors="coerce").astype("float64")
        na, nb = a.isna(), b.isna()
        valid = ~(na | nb)
        one_missing = na ^ nb
        diff = (a - b).abs()
        tol, mode = _field_tol(field)
        if mode == "absolute":
            bad = valid & (diff > tol)
        else:
            base = a.abs().clip(lower=1e-9)
            bad = valid & ((diff / base) > tol)
        bad = bad | one_missing
        is_exempt = pd.Series(False, index=both.index, dtype=bool)
        if exemptions and not both.empty and "ticker" in both.columns:
            is_exempt = _is_exempt(both["ticker"], field, exemptions)
            bad = bad & ~is_exempt
        count = int(bad.sum())
        if not count:
            continue
        valid_non_exempt = valid & ~is_exempt
        mismatched[field] = MismatchDetail(
            field=field,
            mismatch_count=count,
            mismatch_rate=(count / matched_rows) if matched_rows else 0.0,
            max_abs_diff=float(diff[valid_non_exempt].max())
            if bool(valid_non_exempt.any())
            else 0.0,
            sample_tickers=list(both.loc[bad, sample_col].astype(str).head(5)),
        )

    return CrossCheckResult(
        table=asset_type,
        trade_date=resolved_trade_date,
        primary_source=_primary_source_or(df_a, asset_type),
        secondary_source=secondary_source or str(df_b.attrs.get("source", "secondary")),
        total_rows=int(len(merged)),
        matched_rows=matched_rows,
        mismatched_fields=mismatched,
        missing_in_primary=int((merged["_merge"] == "right_only").sum()),
        missing_in_secondary=int((merged["_merge"] == "left_only").sum()),
    )


def _normalize_calendar_dates(df: pd.DataFrame) -> set[date]:
    if "trade_date" not in df.columns:
        raise ValueError("both frames need a 'trade_date' join column")
    if df.empty:
        return set()
    parsed = pd.to_datetime(df["trade_date"], errors="coerce").dt.date.dropna()
    return set(parsed.tolist())


def _cross_check_calendar(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    *,
    table: str = "cn_calendar",
    trade_date: str | date | None = None,
    secondary_source: str | None = None,
) -> CrossCheckResult:
    """在双源交易日历的日期重叠窗口内逐日比对 ``trade_date`` 集合。"""
    dates_a = _normalize_calendar_dates(df_a)
    dates_b = _normalize_calendar_dates(df_b)

    mismatched: dict[str, MismatchDetail] = {}
    if not dates_a or not dates_b:
        union_all = dates_a | dates_b
        matched_rows = 0
        total_rows = len(union_all)
        missing_in_primary = len(dates_b)
        missing_in_secondary = len(dates_a)
        fallback_date: Any = max(union_all) if union_all else ""
    else:
        win_start = max(min(dates_a), min(dates_b))
        win_end = min(max(dates_a), max(dates_b))
        if win_start > win_end:
            matched_rows = 0
            total_rows = len(dates_a | dates_b)
            missing_in_primary = len(dates_b)
            missing_in_secondary = len(dates_a)
            fallback_date = win_end
        else:
            win_a = {d for d in dates_a if win_start <= d <= win_end}
            win_b = {d for d in dates_b if win_start <= d <= win_end}
            both_win = win_a & win_b
            union_win = win_a | win_b
            only_a = sorted(win_a - win_b)
            only_b = sorted(win_b - win_a)
            disagreed = sorted(only_a + only_b)
            matched_rows = len(both_win)
            total_rows = len(union_win)
            missing_in_primary = len(only_b)
            missing_in_secondary = len(only_a)
            if disagreed:
                mismatched["trade_date"] = MismatchDetail(
                    field="trade_date",
                    mismatch_count=len(disagreed),
                    mismatch_rate=(len(disagreed) / total_rows) if total_rows else 0.0,
                    max_abs_diff=float(len(disagreed)),
                    sample_tickers=[d.isoformat() for d in disagreed[:5]],
                )
            fallback_date = max(union_win) if union_win else win_end

    return CrossCheckResult(
        table=table,
        trade_date=_as_date_str(
            trade_date if trade_date is not None else fallback_date
        ),
        primary_source=_primary_source_or(df_a, table),
        secondary_source=secondary_source or str(df_b.attrs.get("source", "secondary")),
        total_rows=total_rows,
        matched_rows=matched_rows,
        mismatched_fields=mismatched,
        missing_in_primary=missing_in_primary,
        missing_in_secondary=missing_in_secondary,
    )


def _frame_trade_date(df_a: pd.DataFrame, df_b: pd.DataFrame) -> Any:
    for df in (df_a, df_b):
        if "trade_date" in df.columns and len(df):
            return df["trade_date"].iloc[0]
    return ""


def _as_date_str(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value) if value is not None else ""


def _primary_source_or(df_a: pd.DataFrame, table: str) -> str:
    explicit = df_a.attrs.get("source")
    if explicit:
        return str(explicit)
    try:
        return get_primary_source(table)
    except (KeyError, ValueError):
        return "primary"


# --- 分级响应 ---
def response_level(mismatch_rate: float) -> ResponseLevel:
    """按不一致率划分响应级别：<0.1% 为 silent，0.1%–1% 为 WARNING，>1% 为 ERROR。"""
    if mismatch_rate > ERROR_BAND_MIN:
        return "ERROR"
    if mismatch_rate >= SILENT_BAND_MAX:
        return "WARNING"
    return "silent"


def respond(result: CrossCheckResult) -> ResponseLevel:
    """判定并记录单次交叉校验的响应级别。

    取各字段最严重级别；``adjfactor`` 出现任何不一致至少为 ``WARNING``；
    若存在数据但无重叠标的，则判定为结论不明确并返回 ``WARNING``。
    """
    if result.matched_rows == 0 and result.total_rows:
        logger.warning(
            "cross-check %s %s: no overlapping tickers between %s and %s "
            "(%d rows unmatched) — comparison inconclusive, not clean",
            result.table,
            result.trade_date,
            result.primary_source,
            result.secondary_source,
            result.total_rows,
        )
        return "WARNING"
    level = response_level(result.mismatch_rate)
    adj = result.mismatched_fields.get("adjfactor")
    if adj is not None and adj.mismatch_count > 0 and level == "silent":
        level = "WARNING"
    detail = "; ".join(
        f"{f} {d.mismatch_rate:.2%} (n={d.mismatch_count}, "
        f"max_diff={d.max_abs_diff:.6f}, e.g. {d.sample_tickers})"
        for f, d in sorted(result.mismatched_fields.items())
    )
    msg = (
        f"cross-check {result.table} {result.trade_date}: "
        f"{result.primary_source} vs {result.secondary_source} — "
        f"worst mismatch {result.mismatch_rate:.2%}"
        + (f"; {detail}" if detail else "; all fields matched")
    )
    if level == "ERROR":
        logger.error(msg)
    elif level == "WARNING":
        logger.warning(msg)
    else:
        logger.info(msg)
    return level


# --- 备源回补标记与资产层写入 ---
def backfill_source_tag(backup: str) -> str:
    """生成缺口回补批次的血缘标签：``"{backup}:backfill"``。"""
    return f"{backup}:backfill"


def write_asset(
    df: pd.DataFrame,
    table: str,
    trade_date: str | date,
    source_tag: str,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    target_path: str | os.PathLike[str] | None = None,
) -> Path:
    """将单批次数据原子写入资产层，并在 Parquet 元数据中记录血缘标签。

    ``source_tag`` 与 ``ingest_time`` 写入文件键值元数据（不新增数据列）。
    写入前检查磁盘容量（CRITICAL 级别拒写），月度分区自动按 ``trade_date`` 合并去重排序，
    采用 ``FileLock`` + tmp -> fsync -> ``os.replace`` -> 父目录 fsync 保证原子性与持久性。
    """
    path = (
        Path(target_path)
        if target_path is not None
        else _landing_path(table, trade_date, data_root)
    )
    merge_monthly = target_path is None and is_monthly_partition(table)

    if check_capacity(path.parent if path.parent.exists() else path) == "CRITICAL":
        raise DiskFullError(
            f"free space is at the CRITICAL tier; refusing to write {path}"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with FileLock(path):
        frame_to_write = df
        if merge_monthly and path.exists() and "trade_date" in df.columns:
            existing = pd.read_parquet(path)
            if len(existing) and "trade_date" in existing.columns:
                replace_dates = {date.fromisoformat(_as_date_str(trade_date)[:10])}
                inc_dates = pd.to_datetime(
                    df["trade_date"], errors="coerce"
                ).dt.date.dropna()
                replace_dates.update(inc_dates.tolist())
                ext_dates = pd.to_datetime(
                    existing["trade_date"], errors="coerce"
                ).dt.date
                kept = existing[~ext_dates.isin(replace_dates)]
                if not kept.empty:
                    combined = (
                        pd.concat([kept, df], ignore_index=True)
                        if not df.empty
                        else kept.reset_index(drop=True)
                    )
                    sort_cols = [
                        c for c in ("trade_date", "ticker") if c in combined.columns
                    ]
                    frame_to_write = (
                        combined.sort_values(sort_cols, kind="mergesort").reset_index(
                            drop=True
                        )
                        if sort_cols
                        else combined
                    )
        pa_table = pa.Table.from_pandas(frame_to_write, preserve_index=False)
        meta = dict(pa_table.schema.metadata or {})
        meta[b"source_tag"] = str(source_tag).encode("utf-8")
        meta[b"ingest_time"] = (
            datetime.now().isoformat(timespec="seconds").encode("utf-8")
        )
        pa_table = pa_table.replace_schema_metadata(meta)
        try:
            pq.write_table(pa_table, tmp, compression="zstd")
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, path)
        except BaseException:
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            raise

        # 刷盘父目录确保 rename 持久性
        try:
            dir_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            logger.warning("wrote %s but could not fsync %s", path, path.parent)
    return path


def _landing_path(
    table: str, trade_date: str | date, data_root: str | os.PathLike[str]
) -> Path:
    return partition_path(table, trade_date, data_root)


# --- 交叉校验触发判断 ---
def raw_exists(
    source: str,
    table: str,
    trade_date: str | date,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> bool:
    """检查 *source* 在 ``(table, trade_date)`` 的原始落地文件是否存在（不解析 watermark）。"""
    if isinstance(trade_date, str):
        trade_date = date.fromisoformat(trade_date)
    return raw_path(source, table, trade_date, data_root).exists()


def should_cross_check(
    source: str,
    table: str,
    trade_date: str | date,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> bool:
    """判断是否应触发交叉校验：仅当主源刚入库完成且全部备源原始批次均已落地时返回 ``True``。"""
    try:
        priority = get_source_priority(table)
    except (KeyError, ValueError):
        # 未在仲裁配置中定义的资产类型（如 factor、裸 reference）
        return False
    if not priority or source != priority[0]:
        return False
    backups = priority[1:]
    if not backups:
        return False
    return all(
        raw_exists(backup, table, trade_date, data_root=data_root) for backup in backups
    )


# --- 规范表映射与单表/批量入库编排 ---
TABLE_TO_ADAPTER_DATASET: dict[str, str] = _AliasedDict(
    {
        "cn_calendar": "calendar",
        "cn_stock_basic": "stock_basic",
        "cn_stock_bars_daily": "stock",
        "cn_stock_bars_minute": "stock_minute",
        "cn_stock_fundamental": "fundamental",
        "cn_stock_daily_status": "daily_status",
        "cn_stock_corporate_actions": "corporate_actions",
        "cn_etf_bars_daily": "etf",
        "cn_stock_index_bars_daily": "index",
        "cn_futures_bars_daily": "futures",
        "option_basic": "option",
    }
)

TABLE_TO_DQC_GROUP: dict[str, str] = _AliasedDict(
    {
        # 日历与股票核心表 (7)
        "cn_calendar": "calendar",
        "cn_stock_basic": "stock_basic",
        "cn_stock_bars_daily": "bars_daily",
        "cn_stock_bars_minute": "bars_minute",
        "cn_stock_fundamental": "fundamental",
        "cn_stock_daily_status": "daily_status",
        "cn_stock_corporate_actions": "corporate_actions",
        # 指数 / 行业 / 成分权重 (4)
        "cn_stock_index_basic": "tabular",
        "cn_stock_index_bars_daily": "index_bars_daily",
        "cn_stock_industry": "tabular",
        "cn_stock_index_weights": "tabular",
        # 股票基本面 PIT (7)
        "cn_stock_fundamental_income": "tabular",
        "cn_stock_fundamental_balance": "tabular",
        "cn_stock_fundamental_cashflow": "tabular",
        "cn_stock_fundamental_indicator": "tabular",
        "cn_stock_fundamental_segment": "tabular",
        "cn_stock_fundamental_earnings": "tabular",
        "cn_stock_fundamental_consensus": "tabular",
        # 资金流 / 情绪 / 筹码 (9)
        "cn_stock_auction": "tabular",
        "cn_stock_moneyflow": "tabular",
        "cn_stock_hsgt_market_flow": "tabular",
        "cn_stock_hsgt_hold": "tabular",
        "cn_stock_margin": "tabular",
        "cn_stock_lhb": "tabular",
        "cn_stock_lhb_seats": "tabular",
        "cn_stock_limit_pool": "tabular",
        "cn_stock_chips_perf": "tabular",
        # 股东与资本运作事件 (8)
        "cn_stock_holder_num": "tabular",
        "cn_stock_top_holders": "tabular",
        "cn_stock_restricted_release": "tabular",
        "cn_stock_block_trade": "tabular",
        "cn_stock_repurchase": "tabular",
        "cn_stock_holder_trade": "tabular",
        "cn_stock_pledge": "tabular",
        "cn_stock_research_visits": "tabular",
        # 股票逐笔与 L2 盘口 (2)
        "cn_stock_tick": "tabular",
        "cn_stock_tick_l2": "tabular",
        # 独立 ETF 域 (5)
        "cn_etf_basic": "tabular",
        "cn_etf_bars_daily": "etf_bars_daily",
        "cn_etf_bars_minute": "etf_bars_minute",
        "cn_etf_fundamental": "tabular",
        "cn_etf_tick": "tabular",
        # 场外与上市公募基金 (5)
        "cn_fund_basic": "tabular",
        "cn_fund_managers": "tabular",
        "cn_fund_nav": "tabular",
        "cn_fund_dividend": "tabular",
        "cn_fund_portfolio": "tabular",
        # 期货 (4) 与因子 (1)
        "cn_futures_bars_daily": "futures_bars_daily",
        "cn_futures_tick_l2": "tabular",
        "cn_futures_contract_specs": "tabular",
        "cn_futures_continuous": "tabular",
        "factor": "factor",
    }
)


@dataclass
class IngestOutcome:
    """单表单日入库执行结果。"""

    table: str
    trade_date: date
    source: str | None
    status: Literal["success", "skipped", "failed"]
    row_count: int = 0
    asset_path: Path | None = None
    fallback_from: str | None = None
    cross_check_levels: dict[str, ResponseLevel] | None = None
    error: str | None = None


def _resolve_asset_target_path(
    canonical: str,
    trade_date: date,
    data_root: str | os.PathLike[str],
) -> Path:
    template = PARTITION_PATHS.get(canonical)
    if template is None:
        raise KeyError(f"unknown storage table {canonical!r}")
    if "{date}" in template or "{month}" in template:
        return partition_path(canonical, trade_date, data_root)
    if "{year}" in template:
        return Path(data_root) / template.format(year=trade_date.strftime("%Y"))
    if "{exchange}" in template:
        return Path(data_root) / template.format(exchange="SSE")
    if "{symbol}" in template:
        raise ValueError(
            f"table {canonical!r} uses symbol partition; pass target_path explicitly"
        )
    return Path(data_root) / template


def _already_ingested(
    canonical: str,
    trade_date: date,
    primary_source: str,
    data_root: str | os.PathLike[str],
) -> Path | None:
    """若该日数据已落地则返回目标路径，否则返回 ``None``。"""
    template = PARTITION_PATHS.get(canonical, "")
    target = _resolve_asset_target_path(canonical, trade_date, data_root)
    if "{date}" in template:
        return target if target.exists() else None
    if "{month}" in template:
        if not target.exists():
            return None
        try:
            existing = pd.read_parquet(target, columns=["trade_date"])
        except Exception:  # noqa: BLE001
            return None
        if existing.empty or "trade_date" not in existing.columns:
            return None
        dates = set(
            pd.to_datetime(existing["trade_date"], errors="coerce").dt.date.dropna()
        )
        return target if trade_date in dates else None
    from data.source_adapter import read_watermark

    wm = read_watermark(primary_source, canonical, Path(data_root))
    if target.exists() and wm in (
        trade_date.strftime("%Y-%m-%d"),
        trade_date.strftime("%Y%m%d"),
    ):
        return target
    return None


def _resolve_adapter_dataset(adapter: Any, canonical: str) -> str:
    supported = list(adapter.list_datasets())
    if canonical in supported:
        return canonical
    legacy = CANONICAL_TO_LEGACY.get(canonical)
    if legacy and legacy in supported:
        return legacy
    mapped = TABLE_TO_ADAPTER_DATASET.get(canonical)
    if mapped and mapped in supported:
        return mapped
    raise ValueError(
        f"{type(adapter).__name__} does not support table {canonical!r} "
        f"(supported: {supported})"
    )


def _resolve_normalizer_dataset(canonical: str) -> str:
    return TABLE_TO_ADAPTER_DATASET.get(canonical, canonical)


def _load_raw_any(
    source: str,
    canonical: str,
    trade_date: date,
    data_root: str | os.PathLike[str],
) -> pd.DataFrame | None:
    from data.raw import load_raw

    candidates: list[str] = [canonical]
    legacy = CANONICAL_TO_LEGACY.get(canonical)
    if legacy and legacy not in candidates:
        candidates.append(legacy)
    mapped = TABLE_TO_ADAPTER_DATASET.get(canonical)
    if mapped and mapped not in candidates:
        candidates.append(mapped)
    for key in candidates:
        df = load_raw(source, key, trade_date, data_root)
        if df is not None:
            return df
    return None


def _obtain_raw(
    source: str,
    canonical: str,
    trade_date: date,
    *,
    data_root: str | os.PathLike[str],
    from_raw: bool,
) -> tuple[pd.DataFrame, str | None]:
    """获取原始批次，并在外部拉取场景下先于 DQC 落地 raw/。"""
    if from_raw:
        raw_df = _load_raw_any(source, canonical, trade_date, data_root)
        if raw_df is None or raw_df.empty:
            raise FileNotFoundError(
                f"missing raw batch for source={source!r}, table={canonical!r}, "
                f"date={trade_date.isoformat()}"
            )
        return raw_df, trade_date.strftime("%Y-%m-%d")

    from data.adapters import get_adapter
    from data.raw import save_raw
    from data.source_adapter import read_watermark

    adapter = get_adapter(source)
    try:
        dataset_arg = _resolve_adapter_dataset(adapter, canonical)
        wm = read_watermark(source, canonical, Path(data_root))
        fetched = adapter.fetch(dataset_arg, trade_date, watermark=wm)
    finally:
        adapter.close()

    if fetched.df is None or fetched.df.empty:
        raise RuntimeError(
            f"source {source!r} returned empty dataframe for {canonical} {trade_date}"
        )
    # 证据链不变量：必须在进入 DQC 前落地 raw 层
    save_raw(fetched.df, source, canonical, trade_date, data_root)
    return fetched.df, fetched.watermark


def _apply_field_overrides(
    df: pd.DataFrame,
    primary_src: str,
    canonical: str,
    trade_date: date,
    *,
    data_root: str | os.PathLike[str],
    from_raw: bool,
) -> pd.DataFrame:
    """按 ``field_override`` 配置从指定源替换特定列。"""
    overrides_map = get_field_overrides(canonical)
    by_source: dict[str, list[str]] = {}
    for field, ov_src in overrides_map.items():
        if ov_src != primary_src:
            by_source.setdefault(ov_src, []).append(field)
    if not by_source:
        return df

    from data.normalizers import get_normalizer

    out = df.copy()
    norm_dataset = _resolve_normalizer_dataset(canonical)
    for ov_src, fields in by_source.items():
        try:
            ov_raw, _ = _obtain_raw(
                ov_src, canonical, trade_date, data_root=data_root, from_raw=from_raw
            )
        except FileNotFoundError:
            logger.warning(
                "field_override source %r raw missing for %s %s; keeping primary %r fields %s",
                ov_src,
                canonical,
                trade_date,
                primary_src,
                fields,
            )
            continue
        ov_norm = get_normalizer(ov_src)(norm_dataset, ov_raw)
        join_keys = [
            c
            for c in ("ticker", "ex_date", "event_type", "trade_date")
            if c in out.columns and c in ov_norm.columns
        ]
        valid_fields = [f for f in fields if f in ov_norm.columns]
        if not join_keys or not valid_fields:
            continue
        ov_sub = ov_norm[join_keys + valid_fields].drop_duplicates(join_keys)
        merged = out.merge(ov_sub, on=join_keys, how="left", suffixes=("", "_ov"))
        for field in valid_fields:
            ov_col = f"{field}_ov"
            if ov_col in merged.columns:
                combined = merged[ov_col].combine_first(merged[field])
                if str(out[field].dtype) == "Int64":
                    merged[field] = combined.astype("Int64")
                else:
                    merged[field] = combined
                merged = merged.drop(columns=[ov_col])
        out = merged
    return out


def _repair_etf_k02_from_tushare(
    df: pd.DataFrame,
    trade_date: date,
    *,
    data_root: str | os.PathLike[str],
    from_raw: bool,
) -> pd.DataFrame:
    """用 Tushare ``fund_daily`` 补齐 ETF 日线中违反 K02（``volume > 0`` 与 ``amount > 0`` 不一致）的量额。

    仅在主源（含 ``field_override`` 后）存在 K02 冲突行时触发；仅覆写冲突行上的 ``volume`` 与 ``amount``，
    保持 OHLC 价格、复权因子与其余标的完全不变。无 Tushare raw 或拉取失败时安全保留原值。
    """
    if df.empty or not {"ticker", "volume", "amount"} <= set(df.columns):
        return df

    vol = pd.to_numeric(df["volume"], errors="coerce")
    amt = pd.to_numeric(df["amount"], errors="coerce")
    bad_mask = vol.notna() & amt.notna() & ((vol > 0) != (amt > 0))
    if not bool(bad_mask.any()):
        return df

    from data.normalizers import get_normalizer

    canonical = "cn_etf_bars_daily"
    ts_raw = _load_raw_any("tushare", canonical, trade_date, data_root)
    if ts_raw is None or ts_raw.empty:
        if from_raw:
            logger.warning(
                "cn_etf_bars_daily %s has %d K02 mismatch row(s); tushare raw missing",
                trade_date,
                int(bad_mask.sum()),
            )
            return df
        try:
            ts_raw, _ = _obtain_raw(
                "tushare", canonical, trade_date, data_root=data_root, from_raw=False
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "cn_etf_bars_daily %s K02 repair via tushare failed (%s); keeping original values",
                trade_date,
                exc,
            )
            return df

    ts_norm = get_normalizer("tushare")("etf", ts_raw)
    if ts_norm.empty or not {"ticker", "volume", "amount"} <= set(ts_norm.columns):
        return df

    ts_vol = pd.to_numeric(ts_norm["volume"], errors="coerce")
    ts_amt = pd.to_numeric(ts_norm["amount"], errors="coerce")
    ts_valid = ts_vol.notna() & ts_amt.notna() & ((ts_vol > 0) == (ts_amt > 0))
    ts_clean = ts_norm.loc[ts_valid]
    if ts_clean.empty:
        return df

    bad_lookup = pd.DataFrame({"_ticker": df.loc[bad_mask, "ticker"].astype(str)})
    ts_lookup = pd.DataFrame(
        {
            "_ticker": ts_clean["ticker"].astype(str),
            "_vol_ts": ts_clean["volume"],
            "_amt_ts": ts_clean["amount"],
        }
    )
    if "trade_date" in df.columns and "trade_date" in ts_clean.columns:
        bad_lookup["_td"] = pd.to_datetime(
            df.loc[bad_mask, "trade_date"], errors="coerce"
        ).dt.date
        ts_lookup["_td"] = pd.to_datetime(
            ts_clean["trade_date"], errors="coerce"
        ).dt.date
        keys = ["_ticker", "_td"]
    else:
        keys = ["_ticker"]
    ts_lookup = ts_lookup.drop_duplicates(keys)
    merged = bad_lookup.merge(ts_lookup, on=keys, how="left", sort=False)

    matched_idx = df.index[bad_mask]
    rep_vol = merged["_vol_ts"].to_numpy()
    rep_amt = merged["_amt_ts"].to_numpy()
    has_fix = pd.notna(rep_vol) & pd.notna(rep_amt)
    if not bool(has_fix.any()):
        return df

    fix_idx = matched_idx[has_fix]
    out = df.copy()
    if str(out["volume"].dtype) == "Int64":
        out.loc[fix_idx, "volume"] = (
            pd.to_numeric(pd.Series(rep_vol[has_fix], index=fix_idx), errors="coerce")
            .round()
            .astype("Int64")
        )
    else:
        out.loc[fix_idx, "volume"] = pd.Series(
            rep_vol[has_fix], index=fix_idx, dtype=out["volume"].dtype
        )
    out.loc[fix_idx, "amount"] = pd.to_numeric(
        pd.Series(rep_amt[has_fix], index=fix_idx), errors="coerce"
    ).astype("float64")
    logger.info(
        "repaired %d cn_etf_bars_daily K02 row(s) on %s via tushare: %s",
        len(fix_idx),
        trade_date,
        list(out.loc[fix_idx, "ticker"].astype(str)),
    )
    return out


def cross_check_path(
    table: str,
    trade_date: str | date,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> Path:
    """返回交叉校验结果 JSON 路径：``{data_root}/cross_check/{table}/{date}.json``。"""
    canonical = resolve_table_name(table)
    return (
        Path(data_root)
        / "cross_check"
        / canonical
        / f"{_as_date_str(trade_date)[:10]}.json"
    )


def save_cross_check_result(
    result: CrossCheckResult,
    level: ResponseLevel,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> Path:
    """将交叉校验结果原子落盘至 ``cross_check/{table}/{date}.json``（同日多备源追加更新）。"""
    path = cross_check_path(result.table, result.trade_date, data_root)
    path.parent.mkdir(parents=True, exist_ok=True)

    existing: dict[str, Any] = {}
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                existing = loaded
        except (json.JSONDecodeError, OSError):
            existing = {}

    comparisons = (
        dict(existing.get("comparisons") or {})
        if existing.get("primary_source") == result.primary_source
        else {}
    )
    comparisons[result.secondary_source] = {
        "primary_source": result.primary_source,
        "secondary_source": result.secondary_source,
        "level": level,
        "total_rows": result.total_rows,
        "matched_rows": result.matched_rows,
        "missing_in_primary": result.missing_in_primary,
        "missing_in_secondary": result.missing_in_secondary,
        "mismatch_rate": result.mismatch_rate,
        "mismatched_fields": {
            field: {
                "mismatch_count": d.mismatch_count,
                "mismatch_rate": d.mismatch_rate,
                "max_abs_diff": d.max_abs_diff,
                "sample_tickers": list(d.sample_tickers),
            }
            for field, d in result.mismatched_fields.items()
        },
    }
    payload = {
        "table": resolve_table_name(result.table),
        "trade_date": result.trade_date,
        "primary_source": result.primary_source,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "comparisons": comparisons,
    }
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    return path


def run_cross_check_for_date(
    table: str,
    trade_date: date,
    *,
    primary_df: pd.DataFrame | None = None,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    fetch_missing_raw: bool = False,
) -> dict[str, ResponseLevel]:
    """对指定 ``(table, trade_date)`` 执行主备源交叉校验并落盘结果。"""
    from data.normalizers import get_normalizer

    canonical = resolve_table_name(table)
    priority = get_source_priority(canonical)
    if len(priority) < 2:
        return {}
    primary_src, backups = priority[0], priority[1:]
    norm_dataset = _resolve_normalizer_dataset(canonical)

    def _obtain_for_cross_check(src: str) -> pd.DataFrame:
        try:
            raw, _ = _obtain_raw(
                src, canonical, trade_date, data_root=data_root, from_raw=True
            )
        except FileNotFoundError:
            if not fetch_missing_raw:
                raise
            raw, _ = _obtain_raw(
                src, canonical, trade_date, data_root=data_root, from_raw=False
            )
        return get_normalizer(src)(norm_dataset, raw)

    unavailable_errors: tuple[type[Exception], ...] = (
        (FileNotFoundError, ValueError, ModuleNotFoundError, RuntimeError, OSError)
        if fetch_missing_raw
        else (FileNotFoundError,)
    )

    effective_primary = primary_src
    if primary_df is None:
        try:
            primary_df = _obtain_for_cross_check(primary_src)
        except unavailable_errors as exc:
            # 若主源无 raw 或不支持该表（回补场景），以优先列表中首个可用源作比对基准
            fallback_df = None
            for candidate in backups:
                try:
                    fallback_df = _obtain_for_cross_check(candidate)
                    effective_primary = candidate
                    backups = [b for b in backups if b != candidate]
                    break
                except unavailable_errors:
                    continue
            if fallback_df is None:
                raise
            logger.warning(
                "primary source %r unavailable for cross-check %s %s (%s); using %s",
                primary_src,
                canonical,
                trade_date,
                exc,
                effective_primary,
            )
            primary_df = fallback_df
        if effective_primary == primary_src:
            primary_df = _apply_field_overrides(
                primary_df,
                effective_primary,
                canonical,
                trade_date,
                data_root=data_root,
                from_raw=True,
            )
            if canonical == "cn_etf_bars_daily":
                primary_df = _repair_etf_k02_from_tushare(
                    primary_df, trade_date, data_root=data_root, from_raw=True
                )
    primary_df = primary_df.copy()
    primary_df.attrs["source"] = effective_primary

    levels: dict[str, ResponseLevel] = {}
    for backup in backups:
        try:
            backup_df = _obtain_for_cross_check(backup)
        except unavailable_errors as exc:
            logger.warning(
                "backup source %r skipped in cross-check %s %s: %s",
                backup,
                canonical,
                trade_date,
                exc,
            )
            continue
        backup_df.attrs["source"] = backup
        if (
            canonical in _AGGREGATE_CROSS_CHECK_TABLES
            and "ticker" in primary_df.columns
            and "ticker" in backup_df.columns
        ):
            res = cross_source_check_aggregate(
                primary_df,
                backup_df,
                canonical,
                trade_date,
                secondary_source=backup,
            )
        else:
            res = cross_source_check(
                primary_df,
                backup_df,
                canonical,
                trade_date,
                secondary_source=backup,
            )
        level = respond(res)
        save_cross_check_result(res, level, data_root=data_root)
        levels[backup] = level
    return levels


def ingest_table(
    table: str,
    trade_date: date,
    *,
    source: str | None = None,
    force: bool = False,
    from_raw: bool = False,
    cross_check: bool = True,
    overrides: dict[str, Any] | None = None,
    skip_dqc: bool = False,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    run_type: str = "daily",
) -> IngestOutcome:
    """执行单表、单交易日的完整接入流水线（拉取 -> 落地 raw -> 标准化 -> DQC -> 写 asset -> 水位线 -> 交叉校验）。

    默认按 ``source_priority.yaml`` 主源拉取并在主源失败或 DQC 拒收时自动切换备源（写 ``{backup}:backfill`` 血缘）；
    显式指定 *source* 时仅尝试该源。
    """
    from data.dqc.engine import ingest_with_dqc
    from data.dqc.report import build_reference_overrides
    from data.dqc.severity import SEVERITY_CONFIGS, DQCBatchReject
    from data.normalizers import get_normalizer
    from data.source_adapter import write_watermark

    canonical = resolve_table_name(table)
    if canonical not in SCHEMA:
        raise KeyError(f"unknown table {table!r}")

    priority = get_source_priority(canonical)
    primary_src = priority[0]
    candidates = [source] if source is not None else list(priority)

    if not force:
        existing = _already_ingested(canonical, trade_date, candidates[0], data_root)
        if existing is not None:
            return IngestOutcome(
                table=canonical,
                trade_date=trade_date,
                source=candidates[0],
                status="skipped",
                asset_path=existing,
            )

    last_error: Exception | None = None
    fallback_from: str | None = None
    norm_dataset = _resolve_normalizer_dataset(canonical)

    for idx, src in enumerate(candidates):
        is_primary = src == primary_src
        source_tag = src if is_primary else backfill_source_tag(src)
        try:
            raw_df, wm = _obtain_raw(
                src, canonical, trade_date, data_root=data_root, from_raw=from_raw
            )
            norm_df = get_normalizer(src)(norm_dataset, raw_df)
            if is_primary:
                norm_df = _apply_field_overrides(
                    norm_df,
                    src,
                    canonical,
                    trade_date,
                    data_root=data_root,
                    from_raw=from_raw,
                )
                if canonical == "cn_etf_bars_daily":
                    norm_df = _repair_etf_k02_from_tushare(
                        norm_df, trade_date, data_root=data_root, from_raw=from_raw
                    )

            dqc_group = TABLE_TO_DQC_GROUP.get(canonical)
            target = _resolve_asset_target_path(canonical, trade_date, data_root)
            if not skip_dqc and dqc_group is not None:
                ref = build_reference_overrides(data_root, canonical, trade_date)
                if overrides:
                    ref.update(overrides)
                cross_df: pd.DataFrame | None = None
                if canonical in ("cn_stock_bars_minute", "cn_etf_bars_minute"):
                    daily_table = (
                        "cn_stock_bars_daily"
                        if canonical == "cn_stock_bars_minute"
                        else "cn_etf_bars_daily"
                    )
                    daily_path = partition_path(daily_table, trade_date, data_root)
                    if daily_path.exists():
                        cross_df = pd.read_parquet(daily_path)
                df_clean = ingest_with_dqc(
                    norm_df,
                    trade_date,
                    dqc_group,
                    table=canonical,
                    overrides=ref,
                    cross_df=cross_df,
                    data_root=data_root,
                    source_tag=source_tag,
                    run_type=run_type,
                )
            else:
                schema = SCHEMA[canonical]
                projected = norm_df[
                    [c for c in schema.columns if c in norm_df.columns]
                ].copy()
                for col_name, col_schema in schema.columns.items():
                    if col_name not in projected.columns and col_schema.nullable:
                        projected[col_name] = None
                normalized_proj = normalize_missing_floats(projected, canonical)
                if canonical.endswith(("_bars_daily", "_bars_minute")) and len(
                    normalized_proj
                ):
                    critical_cols = [
                        c
                        for c in ("open", "high", "low", "close", "volume", "amount")
                        if c in normalized_proj.columns
                        and not schema.columns[c].nullable
                    ]
                    if critical_cols:
                        null_mask = normalized_proj[critical_cols].isna().any(axis=1)
                        if null_mask.any():
                            drop_count = int(null_mask.sum())
                            drop_rate = drop_count / len(normalized_proj)
                            max_drop_rate = (
                                SEVERITY_CONFIGS["C04"].escalation_threshold or 0.20
                            )
                            if drop_rate > max_drop_rate:
                                raise DQCBatchReject(
                                    f"batch {trade_date.isoformat()} {canonical} "
                                    f"rejected: null OHLCV rate {drop_rate:.4f} > "
                                    f"{max_drop_rate}",
                                    detail={
                                        "table": canonical,
                                        "trade_date": trade_date.isoformat(),
                                        "drop_count": drop_count,
                                        "total_rows": int(len(normalized_proj)),
                                        "drop_rate": round(drop_rate, 6),
                                    },
                                )
                            logger.warning(
                                "dropping %d/%d rows with null OHLCV in %s %s "
                                "(rate=%.4f <= %.2f)",
                                drop_count,
                                len(normalized_proj),
                                canonical,
                                trade_date,
                                drop_rate,
                                max_drop_rate,
                            )
                            normalized_proj = normalized_proj.loc[~null_mask].copy()
                validated = schema.validate(normalized_proj)
                if is_monthly_partition(canonical):
                    upsert_parquet_atomic(
                        validated,
                        target,
                        table=canonical,
                        partition_dates=[trade_date],
                        source_tag=source_tag,
                    )
                else:
                    if canonical == "cn_calendar" and target.exists():
                        existing_cal = pd.read_parquet(target, columns=["trade_date"])
                        merged_cal = (
                            pd.concat([existing_cal, validated], ignore_index=True)
                            .drop_duplicates(subset=["trade_date"])
                            .sort_values("trade_date")
                            .reset_index(drop=True)
                        )
                        validated = schema.validate(merged_cal)
                    write_parquet_atomic(
                        validated,
                        target,
                        table=canonical,
                        source_tag=source_tag,
                    )
                df_clean = validated

            write_watermark(
                src,
                canonical,
                wm or trade_date.strftime("%Y-%m-%d"),
                Path(data_root),
            )

            levels: dict[str, ResponseLevel] | None = None
            if cross_check and should_cross_check(
                src, canonical, trade_date, data_root=data_root
            ):
                levels = run_cross_check_for_date(
                    canonical,
                    trade_date,
                    primary_df=df_clean,
                    data_root=data_root,
                )

            if not is_primary:
                logger.warning(
                    "fallback backfill landed %s %s via %s (source_tag=%s)",
                    canonical,
                    trade_date,
                    src,
                    source_tag,
                )
            return IngestOutcome(
                table=canonical,
                trade_date=trade_date,
                source=src,
                status="success",
                row_count=len(df_clean),
                asset_path=target,
                fallback_from=fallback_from,
                cross_check_levels=levels,
            )
        except (DQCBatchReject, Exception) as exc:  # noqa: BLE001
            last_error = exc
            if idx == 0 and len(candidates) > 1:
                fallback_from = src
                logger.warning(
                    "primary source %r failed for %s %s (%s); trying backup sources",
                    src,
                    canonical,
                    trade_date,
                    exc,
                )
            else:
                logger.warning(
                    "source %r failed for %s %s: %s",
                    src,
                    canonical,
                    trade_date,
                    exc,
                )

    assert last_error is not None
    raise last_error


def run_ingestion(
    tables: Sequence[str],
    dates: Sequence[date],
    *,
    source: str | None = None,
    force: bool = False,
    from_raw: bool = False,
    cross_check: bool = True,
    overrides: dict[str, Any] | None = None,
    skip_dqc: bool = False,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    fail_fast: bool = False,
    sleep_seconds: float = 0.0,
    run_type: str = "daily",
) -> list[IngestOutcome]:
    """按 ``(dates, tables)`` 顺序批量执行入库并汇总每个批次的结果。"""
    outcomes: list[IngestOutcome] = []
    first = True
    for trade_date in dates:
        for table in tables:
            if not first and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            first = False
            canonical = resolve_table_name(table)
            try:
                outcome = ingest_table(
                    canonical,
                    trade_date,
                    source=source,
                    force=force,
                    from_raw=from_raw,
                    cross_check=cross_check,
                    overrides=overrides,
                    skip_dqc=skip_dqc,
                    data_root=data_root,
                    run_type=run_type,
                )
                outcomes.append(outcome)
            except Exception as exc:  # noqa: BLE001
                if fail_fast:
                    raise
                outcomes.append(
                    IngestOutcome(
                        table=canonical,
                        trade_date=trade_date,
                        source=source,
                        status="failed",
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
    return outcomes
