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

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Mapping

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
    partition_path,
)
from data.storage_schema import (
    CANONICAL_TO_LEGACY,
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
    "get_source_priority",
    "get_primary_source",
    "get_field_overrides",
    "tolerance_for",
    "cross_source_check",
    "response_level",
    "respond",
    "backfill_source_tag",
    "write_asset",
    "raw_exists",
    "should_cross_check",
]

logger = logging.getLogger(__name__)

_REPO_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "source_priority.yaml"
)
SOURCE_PRIORITY_PATH = DATA_ROOT / "config" / "source_priority.yaml"


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
    resolved = _resolve_source_priority_path(path)
    cfg = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise ValueError(f"{SOURCE_PRIORITY_PATH}: top level must be a mapping")
    for asset_type, entry in cfg.items():
        if asset_type == "cross_check":
            continue
        _validate_entry(str(asset_type), entry)
    return cfg


def _validate_entry(asset_type: str, entry: Any) -> None:
    if not isinstance(entry, dict):
        raise ValueError(f"{asset_type}: entry must be a mapping")
    if isinstance(entry.get("priority"), list):
        _validate_table(asset_type, entry)
        return
    for sub, sub_entry in entry.items():
        if not isinstance(sub_entry, dict) or not isinstance(
            sub_entry.get("priority"), list
        ):
            raise ValueError(f"{asset_type}.{sub}: needs a priority list")
        _validate_table(f"{asset_type}.{sub}", sub_entry)


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


def _entry_for(cfg: dict, asset_type: str, sub_type: str | None) -> dict:
    key = _lookup_key(cfg, asset_type)
    if key is not None:
        entry = cfg[key]
        if sub_type is not None:
            sub_key = _lookup_key(entry, sub_type)
            if sub_key is None:
                raise KeyError(sub_type)
            entry = entry[sub_key]
    elif asset_type == "reference" and sub_type is not None:
        sub_key = _lookup_key(cfg, sub_type)
        if sub_key is None:
            raise KeyError(sub_type)
        entry = cfg[sub_key]
    else:
        raise KeyError(asset_type)
    if not isinstance(entry, dict) or not isinstance(entry.get("priority"), list):
        raise ValueError(f"{asset_type}: per-table entry — pass sub_type")
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


def get_source_priority(
    asset_type: str,
    sub_type: str | None = None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> list[str]:
    """返回 *asset_type* 按优先级排序的数据源列表（首位为主源）。"""
    cfg = load_source_priority(path)
    return list(_entry_for(cfg, asset_type, sub_type)["priority"])


def get_primary_source(
    asset_type: str,
    sub_type: str | None = None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> str:
    """返回允许写入 *asset_type* 资产层的唯一主源。"""
    priority = get_source_priority(asset_type, sub_type, path=path)
    if not priority:
        raise ValueError(f"no source assigned for {asset_type}/{sub_type or '-'}")
    return priority[0]


def get_field_overrides(
    asset_type: str,
    sub_type: str | None = None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> dict[str, str]:
    """返回 *asset_type* 的字段级数据源覆盖映射（无则返回 ``{}``）。"""
    cfg = load_source_priority(path)
    entry = _entry_for(cfg, asset_type, sub_type)
    return dict(entry.get("field_override") or {})


# --- 五级容差查询 ---
def tolerance_for(
    field: str, tolerances: Mapping[str, float] | None = None
) -> tuple[float, str]:
    """返回 *field* 的 ``(容差阈值, 比较模式)``（``absolute`` 或 ``relative``）；未列出字段使用 ``default``。"""
    cls = TOLERANCE_CLASS.get(field, "default")
    table = dict(tolerances) if tolerances is not None else load_tolerances()
    value = float(table[cls] if cls in table else table["default"])
    mode = "absolute" if cls in _ABSOLUTE_CLASSES else "relative"
    return value, mode


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
    """单次 ``(trade_date, asset_type)`` 截面交叉校验结果。

    ``total_rows`` 为外连接总行数，``matched_rows`` 为双源共有行数；
    ``missing_in_primary`` 为仅备源存在的行数，``missing_in_secondary`` 为仅主源存在的行数。
    """

    asset_type: str
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
    asset_type: str,
    trade_date: str | date | None = None,
    tolerances: Mapping[str, float] | None = None,
    *,
    secondary_source: str | None = None,
) -> CrossCheckResult:
    """对比同一资产类型经 Normalizer 标准化后的主备源截面。

    ``df_a`` 为主源数据，``df_b`` 为备源数据；按 ``[ticker, trade_date]``（无日期列时按 ``[ticker]``）外连接。
    依 :data:`NUMERIC_FIELDS` 与五级容差表逐列比较，超出容差或单边缺失判为不一致（双边均空视为一致）。
    """
    if "ticker" not in df_a.columns or "ticker" not in df_b.columns:
        raise ValueError("both frames need a 'ticker' join column")
    fields = NUMERIC_FIELDS.get(asset_type)
    if fields is None:
        raise KeyError(
            f"no NUMERIC_FIELDS entry for {asset_type!r} — define it in "
            "data/ingestion.py before cross-checking it"
        )
    join_keys = (
        ["ticker", "trade_date"]
        if "trade_date" in df_a.columns and "trade_date" in df_b.columns
        else ["ticker"]
    )
    if df_a.duplicated(join_keys).any() or df_b.duplicated(join_keys).any():
        raise ValueError(f"duplicate join keys {join_keys} in input frames")

    merged = df_a.merge(
        df_b, on=join_keys, how="outer", suffixes=("_a", "_b"), indicator=True
    )
    both = merged[merged["_merge"] == "both"]
    matched_rows = int(len(both))

    mismatched: dict[str, MismatchDetail] = {}
    for field in fields:
        col_a, col_b = f"{field}_a", f"{field}_b"
        if col_a not in both.columns or col_b not in both.columns:
            continue  # 仅比较双源均提供的字段
        a, b = both[col_a], both[col_b]
        na, nb = a.isna(), b.isna()
        valid = ~(na | nb)
        one_missing = na ^ nb
        diff = (a - b).abs()
        tol, mode = tolerance_for(field, tolerances)
        if mode == "absolute":
            bad = valid & (diff > tol)
        else:
            base = a.abs().clip(lower=1e-9)
            bad = valid & ((diff / base) > tol)
        bad = bad | one_missing
        count = int(bad.sum())
        if not count:
            continue
        mismatched[field] = MismatchDetail(
            field=field,
            mismatch_count=count,
            mismatch_rate=(count / matched_rows) if matched_rows else 0.0,
            max_abs_diff=float(diff[valid].max()) if bool(valid.any()) else 0.0,
            sample_tickers=list(both.loc[bad, "ticker"].head(5)),
        )

    return CrossCheckResult(
        asset_type=asset_type,
        trade_date=_as_date_str(
            trade_date if trade_date is not None else _frame_trade_date(df_a, df_b)
        ),
        primary_source=_primary_source_or(df_a, asset_type),
        secondary_source=secondary_source or str(df_b.attrs.get("source", "secondary")),
        total_rows=int(len(merged)),
        matched_rows=matched_rows,
        mismatched_fields=mismatched,
        missing_in_primary=int((merged["_merge"] == "right_only").sum()),
        missing_in_secondary=int((merged["_merge"] == "left_only").sum()),
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


def _primary_source_or(df_a: pd.DataFrame, asset_type: str) -> str:
    try:
        return get_primary_source(asset_type)
    except (KeyError, ValueError):
        return str(df_a.attrs.get("source", "primary"))


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
            result.asset_type,
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
        f"cross-check {result.asset_type} {result.trade_date}: "
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
    asset_type: str,
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
        else _landing_path(asset_type, trade_date, data_root)
    )
    merge_monthly = target_path is None and is_monthly_partition(asset_type)

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
        table = pa.Table.from_pandas(frame_to_write, preserve_index=False)
        meta = dict(table.schema.metadata or {})
        meta[b"source_tag"] = str(source_tag).encode("utf-8")
        meta[b"ingest_time"] = (
            datetime.now().isoformat(timespec="seconds").encode("utf-8")
        )
        table = table.replace_schema_metadata(meta)
        try:
            pq.write_table(table, tmp, compression="zstd")
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
    asset_type: str, trade_date: str | date, data_root: str | os.PathLike[str]
) -> Path:
    return partition_path(asset_type, trade_date, data_root)


# --- 交叉校验触发判断 ---
def raw_exists(
    source: str,
    asset_type: str,
    trade_date: str | date,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> bool:
    """检查 *source* 在 ``(asset_type, trade_date)`` 的原始落地文件是否存在（不解析 watermark）。"""
    if isinstance(trade_date, str):
        trade_date = date.fromisoformat(trade_date)
    return raw_path(source, asset_type, trade_date, data_root).exists()


def should_cross_check(
    source: str,
    asset_type: str,
    trade_date: str | date,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> bool:
    """判断是否应触发交叉校验：仅当主源刚入库完成且全部备源原始批次均已落地时返回 ``True``。"""
    try:
        priority = get_source_priority(asset_type)
    except (KeyError, ValueError):
        # 未在仲裁配置中定义的资产类型（如 factor、裸 reference）
        return False
    if not priority or source != priority[0]:
        return False
    backups = priority[1:]
    if not backups:
        return False
    return all(
        raw_exists(backup, asset_type, trade_date, data_root=data_root)
        for backup in backups
    )
