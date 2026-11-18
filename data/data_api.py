"""数据读取层 — 基于 DuckDB 的只读 SQL 查询接口（DataAPI）、``load_bars`` 及 alphalens 适配器。

底层读取 Parquet 视图并按消费方约定整形；``table_family`` 在 :func:`load_bars` 中表示表族（equity / futures / continuous），与 :class:`core.types.AssetType` 严格区分。
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Literal

import duckdb
import pandas as pd
import pyarrow as pa
from core.config import DATA_ROOT, get_data_root
from data.storage_schema import TABLE_ALIASES

logger = logging.getLogger(__name__)


# --- 表族与参数枚举 ---
class TableFamily(Enum):
    """标准存储表族枚举（内部统一使用规范表名，入口别名经 ``_ALIASES`` 归一化）。"""

    CN_STOCK_DAILY = "cn_stock_bars_daily"
    CN_STOCK_MINUTE = "cn_stock_bars_minute"
    CN_ETF_DAILY = "cn_etf_bars_daily"
    CN_ETF_MINUTE = "cn_etf_bars_minute"
    CN_FUTURES_DAILY = "cn_futures_bars_daily"
    CN_CONTINUOUS_DAILY = "cn_futures_continuous"


#: 入口表族别名归一化映射（历史/简写 -> 规范表族前缀）。
_ALIASES: dict[str, str] = {
    "equity": "cn_stock",
    "stock": "cn_stock",
    "cn_equity": "cn_stock",
    "etf": "cn_etf",
    "futures": "cn_futures",
    "continuous": "cn_continuous",
}

#: 集中声明各规范行情表的列与属性，替代分散在各查询构造器中的硬编码分支。
TABLE_SCHEMA: dict[str, dict[str, Any]] = {
    "cn_stock_bars_daily": {
        "date_col": "trade_date",
        "filter_col": "trade_date",
        "ticker_col": "ticker",
        "has_adjfactor": True,
    },
    "cn_stock_bars_minute": {
        "date_col": "datetime",
        "filter_col": "trade_date",
        "ticker_col": "ticker",
        "has_adjfactor": False,
    },
    "cn_etf_bars_daily": {
        "date_col": "trade_date",
        "filter_col": "trade_date",
        "ticker_col": "ticker",
        "has_adjfactor": True,
    },
    "cn_etf_bars_minute": {
        "date_col": "datetime",
        "filter_col": "trade_date",
        "ticker_col": "ticker",
        "has_adjfactor": False,
    },
    "cn_futures_bars_daily": {
        "date_col": "trade_date",
        "filter_col": "trade_date",
        "ticker_col": "ticker",
        "has_adjfactor": False,
    },
    "cn_futures_continuous": {
        "date_col": "trade_date",
        "filter_col": "trade_date",
        "ticker_col": "dominant_contract",
        "has_adjfactor": True,
    },
}

#: 标准化行情频率字面量类型。
BarFreq = Literal["daily", "minute", "5min", "60min"]

#: 复权口径字面量类型。
AdjustMode = Literal["none", "forward", "backward"]


@dataclass(frozen=True)
class BarsMeta:
    """``load_bars`` / ``load_bars_result`` 返回的行情口径与数据血缘元数据。"""

    table: str
    table_family: str
    freq: str
    adjust: str
    start: str
    end: str
    as_of: str | None
    rows: int
    tickers: int
    source: str | None = None


@dataclass(frozen=True)
class BarsResult:
    """携带结构化元数据 :class:`BarsMeta` 的 ``load_bars_result`` 返回容器。"""

    data: pd.DataFrame
    meta: BarsMeta


# 目标频率 -> 底层表基础频率（5min/60min 由 minute 聚合）
_FREQ_BASE: dict[str, str] = {
    "daily": "daily",
    "minute": "minute",
    "5min": "minute",
    "60min": "minute",
}

# DuckDB time_bucket 聚合间隔
_AGG_INTERVAL: dict[str, str] = {
    "5min": "5 minutes",
    "60min": "60 minutes",
}

# 标准输出列顺序；源表含 adjfactor 时追加在末尾
_STD_COLS: list[str] = [
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
]

# 参与复权缩放的价格列
_PRICE_COLS: list[str] = ["open", "high", "low", "close"]

# alphalens 默认参数
ALPHALENS_PERIODS: tuple[int, ...] = (1, 5, 10, 20)
ALPHALENS_QUANTILES: int = 5

_DEFAULT_DATA_ROOT = str(DATA_ROOT / "asset")
_DEFAULT_START = "2020-01-01"
_DEFAULT_END = "2024-12-31"

# --- 模块级连接单例与复权基准缓存 ---
_API_CACHE: dict[Path, DataAPI] = {}
_API_LOCK = threading.Lock()
_ADJ_CACHE: dict[tuple[Path | int, str], pd.DataFrame] = {}
_ADJ_LOCK = threading.Lock()


def _base_freq(freq: str) -> str:
    """将 *freq* 映射到底层基础表频率（5min/60min -> minute）。"""
    try:
        return _FREQ_BASE[freq]
    except KeyError:
        raise ValueError(
            f"unknown freq {freq!r}; expected one of {sorted(_FREQ_BASE)}"
        ) from None


def _resolve_table(table_family: TableFamily | str, freq: BarFreq | str) -> str:
    """解析 (*table_family*, *freq*) 对应的规范存储表名。"""
    base = _base_freq(freq)
    if isinstance(table_family, TableFamily):
        if table_family.name.endswith(f"_{base.upper()}"):
            return table_family.value
    else:
        canonical = _ALIASES.get(table_family, table_family)
        enum_key = f"{canonical}_{base}".upper()
        member = TableFamily.__members__.get(enum_key)
        if member is not None:
            return member.value
        # 兼容直接传入完整规范表名或旧表名（如 cn_stock_bars_daily / bars_daily）
        for m in TableFamily:
            if m.name.endswith(f"_{base.upper()}"):
                alias_name = TABLE_ALIASES.get(m.value, "")
                if table_family in (m.value, alias_name):
                    return m.value
    valid = [m.name.lower() for m in TableFamily]
    raise ValueError(
        f"no table for table_family={table_family!r} freq={freq!r}; "
        f"valid table families: {valid}"
    )


def _resolve_asset_root(data_root: str | Path | None = None) -> Path:
    """解析资产层根目录；未显式传入时读取当前上下文 ``get_data_root() / 'asset'``。"""
    if data_root is None:
        return (get_data_root() / "asset").resolve()
    return Path(data_root).resolve()


def get_api(data_root: str | Path | None = None) -> DataAPI:
    """按解析后的 ``data_root`` 获取模块级复用的 :class:`DataAPI` 实例并完成一次视图注册。"""
    resolved = _resolve_asset_root(data_root)
    with _API_LOCK:
        api = _API_CACHE.get(resolved)
        if api is None:
            api = DataAPI(resolved)
            api.register_views()
            _API_CACHE[resolved] = api
        return api


def clear_api_cache() -> None:
    """关闭并清空模块级缓存的 :class:`DataAPI` 实例。"""
    with _API_LOCK:
        for api in _API_CACHE.values():
            try:
                api.close()
            except Exception:
                logger.debug("error closing cached DataAPI", exc_info=True)
        _API_CACHE.clear()


def clear_adj_cache() -> None:
    """清空复权基准因子缓存（供日终数据更新或测试重置后调用）。"""
    with _ADJ_LOCK:
        _ADJ_CACHE.clear()


# --- DataAPI（持久化 DuckDB 目录，仅支持 SQL） ---
class DataAPI:
    """基于 DuckDB ``read_parquet`` 视图的纯 SQL 查询层（持久化 ``catalog.duckdb``）。

    每进程建议单实例（DuckDB 连接跨进程不安全）；进程内多线程共享，``reconnect`` 与 ``close`` 通过 ``_conn_lock`` 串行化。
    """

    def __init__(
        self,
        data_root: str | Path | None = None,
        *,
        catalog_path: str | Path | None = None,
        statement_timeout: float = 60.0,
        read_only: bool = False,
    ) -> None:
        self._root = (
            Path(data_root) if data_root is not None else get_data_root() / "asset"
        )
        self._statement_timeout = statement_timeout
        self._read_only = read_only

        if catalog_path is not None:
            self._catalog_path = Path(catalog_path)
        elif self._root.name == "asset":
            self._catalog_path = self._root.parent / "catalog.duckdb"
        elif self._root.parent.name == "asset":
            self._catalog_path = self._root.parent.parent / "catalog.duckdb"
        else:
            self._catalog_path = self._root / "catalog.duckdb"

        self._catalog_path.parent.mkdir(parents=True, exist_ok=True)
        # 使用 RLock：_run 重试分支持锁后调用 reconnect() 需重入
        self._conn_lock = threading.RLock()
        self._conn: duckdb.DuckDBPyConnection = duckdb.connect(
            database=str(self._catalog_path),
            read_only=self._read_only,
        )
        self._apply_timeout()
        self._views_registered = False

    @property
    def catalog_path(self) -> Path:
        """返回底层 ``catalog.duckdb`` 文件路径。"""
        return self._catalog_path

    def _apply_timeout(self) -> None:
        """若当前 DuckDB 版本支持，设置语句超时时间。"""
        ms = int(self._statement_timeout * 1000)
        try:
            self._conn.execute(f"SET statement_timeout = '{ms}ms'")
        except duckdb.CatalogException:
            # 部分 DuckDB 版本不支持 statement_timeout，仍保留 [COST] 耗时日志
            logger.debug("statement_timeout not supported in this DuckDB build")

    # --- 视图注册与表血缘 ---
    def register_views(self) -> None:
        """为 ``data_root`` 下每个含 ``*.parquet`` 的直接子目录创建或替换视图（启用 ``union_by_name``，关闭 Hive 分区）。"""
        if not self._root.exists():
            logger.debug("data_root %s does not exist; no views registered", self._root)
            return
        # 与查询共用连接锁，避免并发执行语句打断未消费游标
        with self._conn_lock:
            self._register_views_locked()

    def _register_views_locked(self) -> None:
        for entry in sorted(self._root.iterdir()):
            if not entry.is_dir():
                continue
            glob = entry / "*.parquet"
            # 跳过尚无 parquet 文件的空表目录，避免 CREATE VIEW 报错阻塞其余表查询
            if not any(entry.glob("*.parquet")):
                logger.debug("no parquet files under %s; no view registered", entry)
                continue
            # 校验目录名标识符并转义路径字面量，防止异常目录名导致 SQL 语法错误
            view_name = _validate_identifier(entry.name)
            self._conn.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS "
                f"SELECT * FROM read_parquet({_sql_literal(str(glob))}, "
                f"union_by_name=true, hive_partitioning=false)"
            )
            alias = TABLE_ALIASES.get(entry.name)
            if alias:
                alias_name = _validate_identifier(alias)
                self._conn.execute(
                    f"CREATE OR REPLACE VIEW {alias_name} AS SELECT * FROM {view_name}"
                )
        self._views_registered = True

    def _resolve_table_dir(self, table: str) -> Path | None:
        """定位 *table*（或其别名）下含 Parquet 文件的实际目录。"""
        if not self._root.exists():
            return None
        for candidate_name in (table, TABLE_ALIASES.get(table)):
            if not candidate_name:
                continue
            candidate = self._root / candidate_name
            if candidate.is_dir() and any(candidate.glob("*.parquet")):
                return candidate
        return None

    def read_source_tags(self, table: str) -> str | None:
        """读取 *table* 底层 Parquet 文件元数据中去重后的 ``source_tag``（无记录返回 ``None``）。"""
        table_dir = self._resolve_table_dir(table)
        if table_dir is None:
            return None
        glob = _sql_literal(str(table_dir / "*.parquet"))
        sql = (
            f"SELECT DISTINCT value FROM parquet_kv_metadata({glob}) "
            "WHERE CAST(key AS VARCHAR) = 'source_tag'"
        )
        rows, _ = self._run(sql, lambda c: c.fetchall())
        tags = sorted(
            {
                (
                    bytes(val).decode("utf-8", errors="replace")
                    if isinstance(val, (bytes, bytearray, memoryview))
                    else str(val)
                ).strip()
                for (val,) in rows
                if val is not None
            }
            - {""}
        )
        return ",".join(tags) if tags else None

    # --- 查询执行 ---
    def _run(self, sql: str, fetch, profile: bool = False):
        """加锁执行 *sql* 并通过 *fetch* 在锁内物化游标，连接断开时重连并重试一次；返回 ``(result, elapsed_seconds)``。"""
        with self._conn_lock:
            try:
                if profile:
                    self._conn.execute("PRAGMA enable_profiling")
                t0 = time.perf_counter()
                result = fetch(self._conn.execute(sql))
                return result, time.perf_counter() - t0
            except duckdb.ConnectionException:
                # 长会话断连时重连并重试一次
                logger.warning("DuckDB connection lost; reconnecting once")
                self.reconnect()
                if profile:
                    self._conn.execute("PRAGMA enable_profiling")
                t0 = time.perf_counter()
                result = fetch(self._conn.execute(sql))
                return result, time.perf_counter() - t0
            finally:
                if profile:
                    try:
                        self._conn.execute("PRAGMA disable_profiling")
                    except Exception:
                        # 清理剖析状态时忽略失败，避免覆盖原始异常
                        logger.debug("disable_profiling failed", exc_info=True)

    def query(self, sql: str, profile: bool = False) -> pd.DataFrame:
        """执行 *sql* 返回 DataFrame，并记录 ``[COST]`` 耗时日志。"""
        result, elapsed = self._run(sql, lambda c: c.fetchdf(), profile)
        level = logging.INFO
        if elapsed > 30:
            level = logging.CRITICAL
        elif elapsed > 5:
            level = logging.WARNING
        logger.log(
            level,
            "[COST] time=%.3fs rows=%d sql=%s",
            elapsed,
            len(result),
            sql[:200],
        )
        return result

    def query_arrow(self, sql: str) -> pa.Table:
        """执行 *sql* 返回 PyArrow Table（复用 ``query`` 重连策略）。"""
        result, elapsed = self._run(sql, lambda c: c.to_arrow_table())
        logger.info(
            "[COST] time=%.3fs rows=%d sql=%s", elapsed, result.num_rows, sql[:200]
        )
        return result

    # --- 生命周期管理 ---
    def reconnect(self) -> None:
        """重建 DuckDB 连接并重新注册视图（通过连接锁串行化）。"""
        with self._conn_lock:
            try:
                self._conn.close()
            except Exception:
                logger.debug("error closing stale DuckDB connection", exc_info=True)
            self._conn = duckdb.connect(
                database=str(self._catalog_path),
                read_only=self._read_only,
            )
            self._apply_timeout()
            if self._views_registered:
                self.register_views()

    def close(self) -> None:
        with self._conn_lock:
            self._conn.close()

    def __enter__(self) -> DataAPI:
        self.register_views()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# --- load_bars（表映射、SQL 组装、复权与聚合） ---
def _sql_literal(value: str) -> str:
    """转义 Python 字符串为 SQL 单引号字面量。"""
    return "'" + value.replace("'", "''") + "'"


def _validate_identifier(name: str) -> str:
    """校验列名/表名为合法标识符，防止 SQL 注入。"""
    if not name.isidentifier():
        raise ValueError(
            f"invalid column name {name!r}: must be a valid Python identifier "
            "(letters, digits, underscores; no SQL fragments)"
        )
    return name


def _symbol_list(symbol: str | list[str] | None) -> list[str]:
    """规范化标的过滤条件：``None`` 表示全量，空列表 ``[]`` 直接抛错以防静默放大为全市场拉取（分钟线易 OOM）。"""
    if symbol is None:
        return []
    symbols = [symbol] if isinstance(symbol, str) else list(symbol)
    if not symbols:
        raise ValueError("symbol list is empty; pass None to load all")
    return [str(s) for s in symbols]


def _build_where(
    start: str,
    end: str,
    symbol: str | list[str] | None,
    schema: dict[str, Any],
) -> str:
    """构建统一的日期区间与标的列表 ``WHERE`` 子句。"""
    filter_col = schema.get("filter_col", schema["date_col"])
    where = f"WHERE {filter_col} BETWEEN {_sql_literal(start)} AND {_sql_literal(end)}"
    symbols = _symbol_list(symbol)
    if symbols:
        joined = ", ".join(_sql_literal(s) for s in symbols)
        where += f" AND {schema['ticker_col']} IN ({joined})"
    return where


def _build_daily_sql(
    table: str,
    start: str,
    end: str,
    symbol: str | list[str] | None,
    fields: list[str] | None,
) -> str:
    """构建非聚合表（日线或原始分钟线）的 SELECT 语句。"""
    schema = TABLE_SCHEMA[table]
    has_adj = schema["has_adjfactor"]
    ticker_col = schema["ticker_col"]
    date_col = schema["date_col"]

    select_parts = [f"{date_col} AS date", f"{ticker_col} AS ticker"]
    value_cols = fields if fields is not None else list(_STD_COLS[2:])
    for col in value_cols:
        if col == "adjfactor":
            if has_adj:
                select_parts.append("adjfactor")
            elif fields is not None:
                # 显式请求了无 adjfactor 表的该字段时直接报错，防止静默退化为空列
                raise ValueError(
                    f"field 'adjfactor' is not available in table {table!r}"
                )
            continue
        select_parts.append(_validate_identifier(col))
    if fields is None and has_adj:
        select_parts.append("adjfactor")

    where = _build_where(start, end, symbol, schema)
    cols_sql = ", ".join(select_parts)
    return f"SELECT {cols_sql} FROM {table} {where} ORDER BY date, ticker"


def _build_aggregated_sql(
    table: str,
    start: str,
    end: str,
    symbol: str | list[str] | None,
    fields: list[str] | None,
    freq: str,
) -> str:
    """基于分钟表构建 5min/60min ``time_bucket`` 聚合 SQL（按 ``trade_date`` 分组防跨日，午休空桶不填充）。"""
    schema = TABLE_SCHEMA[table]
    interval = _AGG_INTERVAL[freq]
    value_cols = (
        fields
        if fields is not None
        else ["open", "high", "low", "close", "volume", "amount"]
    )

    agg_exprs = {
        "open": "first(open ORDER BY datetime)",
        "high": "max(high)",
        "low": "min(low)",
        "close": "last(close ORDER BY datetime)",
        "volume": "sum(volume)",
        "amount": "sum(amount)",
    }
    select_parts = [
        f"time_bucket(INTERVAL '{interval}', {schema['date_col']}) AS date",
        f"{schema['ticker_col']} AS ticker",
    ]
    for col in value_cols:
        _validate_identifier(col)
        if col not in agg_exprs:
            raise ValueError(
                f"field {col!r} is not available in {freq!r} aggregation; "
                f"supported fields: {sorted(agg_exprs)}"
            )
        select_parts.append(f"{agg_exprs[col]} AS {col}")

    where = _build_where(start, end, symbol, schema)
    filter_col = schema.get("filter_col", schema["date_col"])
    cols_sql = ", ".join(select_parts)
    return (
        f"SELECT {cols_sql} FROM {table} {where} "
        f"GROUP BY date, ticker, {filter_col} "
        f"ORDER BY date, ticker"
    )


def _get_adj_base(api: DataAPI, table: str, schema: dict[str, Any]) -> pd.DataFrame:
    """查询并缓存全表历史复权极值基准（``arg_max`` / ``arg_min``），按数据根目录与表名隔离。"""
    root_key: Path | int = api._root.resolve() if hasattr(api, "_root") else id(api)
    cache_key = (root_key, table)
    with _ADJ_LOCK:
        cached = _ADJ_CACHE.get(cache_key)
    if cached is not None:
        return cached

    ticker_col = schema["ticker_col"]
    date_col = schema.get("filter_col", schema["date_col"])
    base_sql = (
        f"SELECT {ticker_col} AS ticker, "
        f"arg_max(adjfactor, {date_col}) AS adj_latest, "
        f"arg_min(adjfactor, {date_col}) AS adj_first "
        f"FROM {table} GROUP BY {ticker_col}"
    )
    base = api.query(base_sql)
    with _ADJ_LOCK:
        _ADJ_CACHE[cache_key] = base
    return base


def _apply_adjustment(
    df: pd.DataFrame,
    api: DataAPI,
    table: str,
    adjust: str,
    schema: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """执行前复权 / 后复权缩放。

    分母使用全表历史极值（``arg_max``/``arg_min``）而非查询窗口极值，保证窗口截断时比例准确；
    仅缩放价格列，成交量与成交额保持原始值。
    """
    resolved_schema = schema if schema is not None else TABLE_SCHEMA[table]
    base = _get_adj_base(api, table, resolved_schema)
    m = df.merge(base, on="ticker", how="left")

    if adjust == "forward":
        ratio = m["adjfactor"] / m["adj_latest"]
    elif adjust == "backward":
        ratio = m["adjfactor"] / m["adj_first"]
    else:
        raise ValueError(
            f"unknown adjust {adjust!r}; expected 'none', 'forward', or 'backward'"
        )

    for col in _PRICE_COLS:
        if col in m.columns:
            m[col] = m[col] * ratio

    drop_cols = [c for c in ("adj_latest", "adj_first") if c in m.columns]
    return m.drop(columns=drop_cols)


def _format_as_of(df: pd.DataFrame, freq: str) -> str | None:
    """提取结果集最大时间截断点并格式化为 ISO 字符串；空表返回 ``None``。"""
    if df.empty or "date" not in df.columns:
        return None
    max_val = df["date"].max()
    if pd.isna(max_val):
        return None
    if isinstance(max_val, (pd.Timestamp, datetime)):
        return (
            max_val.date().isoformat()
            if freq == "daily"
            else max_val.isoformat(timespec="seconds")
        )
    if isinstance(max_val, date):
        return max_val.isoformat()
    return str(max_val)


def _build_bars_meta(
    df: pd.DataFrame,
    *,
    table: str,
    table_family: str,
    freq: str,
    adjust: str,
    start: str,
    end: str,
    source: str | None,
) -> BarsMeta:
    """根据查询参数、结果集与 Parquet 元数据构建不可变 :class:`BarsMeta`。"""
    tickers = (
        int(df["ticker"].nunique()) if not df.empty and "ticker" in df.columns else 0
    )
    return BarsMeta(
        table=table,
        table_family=str(table_family),
        freq=str(freq),
        adjust=str(adjust),
        start=start,
        end=end,
        as_of=_format_as_of(df, str(freq)),
        rows=len(df),
        tickers=tickers,
        source=source,
    )


def load_bars(
    symbol: str | list[str] | None = None,
    start: str = _DEFAULT_START,
    end: str = _DEFAULT_END,
    freq: BarFreq = "daily",
    fields: list[str] | None = None,
    adjust: AdjustMode = "none",
    table_family: TableFamily | str = "equity",
    *,
    data_root: str | Path | None = None,
    api: DataAPI | None = None,
) -> pd.DataFrame:
    """从存储层加载行情 Bar，按 ``(date, ticker)`` 升序返回标准列及可选 ``adjfactor``。

    同时将 :class:`BarsMeta` 挂载到 ``df.attrs["meta"]``，以便在不破坏历史调用方的情况下核验复权口径与数据血缘。

    Parameters
    ----------
    symbol : 单个标的代码、列表或 ``None``（全量）。
    start, end : 起止日期（含边界，ISO 格式）。
    freq : ``daily`` | ``minute`` | ``5min`` | ``60min``。
    fields : 指定输出数值列；``None`` 返回全部标准列。
    adjust : ``none`` | ``forward`` | ``backward``。
    table_family : 表族名称（``equity`` / ``futures`` / ``continuous`` 等）。
    api : 可选外部传入的 :class:`DataAPI` 实例；默认通过 :func:`get_api` 复用模块级连接。
    """
    table = _resolve_table(table_family, freq)
    schema = TABLE_SCHEMA[table]
    has_adj = schema["has_adjfactor"]
    is_agg = freq in _AGG_INTERVAL

    # 查询前校验 adjust 取值，避免空区间时将非法参数掩盖为无数据
    if adjust not in ("none", "forward", "backward"):
        raise ValueError(
            f"unknown adjust {adjust!r}; expected 'none', 'forward', or 'backward'"
        )

    if adjust != "none" and not has_adj:
        raise ValueError(
            f"adjust={adjust!r} requires adjfactor but table {table!r} has none"
        )

    # 复权计算依赖 adjfactor：若未显式请求则临时补选，计算后剔除
    effective_fields = fields
    drop_adj_after = False
    if (
        adjust != "none"
        and has_adj
        and fields is not None
        and "adjfactor" not in fields
    ):
        effective_fields = [*fields, "adjfactor"]
        drop_adj_after = True

    if api is None:
        api = get_api(data_root)
    elif not api._views_registered:
        api.register_views()

    if is_agg:
        sql = _build_aggregated_sql(table, start, end, symbol, effective_fields, freq)
    else:
        sql = _build_daily_sql(table, start, end, symbol, effective_fields)
    df = api.query(sql)

    if adjust != "none" and not df.empty:
        df = _apply_adjustment(df, api, table, adjust, schema)

    source = api.read_source_tags(table)

    if drop_adj_after and "adjfactor" in df.columns:
        df = df.drop(columns=["adjfactor"])

    # 按契约列序重排并排序
    if not df.empty:
        ordered = [c for c in _STD_COLS if c in df.columns]
        if "adjfactor" in df.columns:
            ordered.append("adjfactor")
        extras = [c for c in df.columns if c not in ordered]
        df = df[[*ordered, *extras]]
        df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    # 所有 DataFrame 变形完成后挂载元数据，防止中间切片/排序丢失 attrs
    family_str = (
        table_family.value
        if isinstance(table_family, TableFamily)
        else str(table_family)
    )
    df.attrs["meta"] = _build_bars_meta(
        df,
        table=table,
        table_family=family_str,
        freq=freq,
        adjust=adjust,
        start=start,
        end=end,
        source=source,
    )
    return df


def load_bars_result(
    symbol: str | list[str] | None = None,
    start: str = _DEFAULT_START,
    end: str = _DEFAULT_END,
    freq: BarFreq = "daily",
    fields: list[str] | None = None,
    adjust: AdjustMode = "none",
    table_family: TableFamily | str = "equity",
    *,
    data_root: str | Path | None = None,
    api: DataAPI | None = None,
) -> BarsResult:
    """加载行情 Bar 并返回携带结构化 :class:`BarsMeta` 的 :class:`BarsResult`。"""
    df = load_bars(
        symbol=symbol,
        start=start,
        end=end,
        freq=freq,
        fields=fields,
        adjust=adjust,
        table_family=table_family,
        data_root=data_root,
        api=api,
    )
    return BarsResult(data=df, meta=df.attrs["meta"])


def load_bars_chunked(
    symbol: str | list[str] | None = None,
    start: str = _DEFAULT_START,
    end: str = _DEFAULT_END,
    freq: BarFreq = "daily",
    fields: list[str] | None = None,
    adjust: AdjustMode = "none",
    table_family: TableFamily | str = "equity",
    chunk_by: str = "month",
    *,
    data_root: str | Path | None = None,
    api: DataAPI | None = None,
) -> Iterator[pd.DataFrame]:
    """按自然月分块流式调用 :func:`load_bars`（跳过空块），避免长跨度分钟线查询 OOM。"""
    if chunk_by != "month":
        raise ValueError(
            f"unsupported chunk_by {chunk_by!r}; only 'month' is implemented"
        )
    shared_api = api if api is not None else get_api(data_root)
    for chunk_start, chunk_end in _date_chunks(start, end):
        df = load_bars(
            symbol,
            chunk_start,
            chunk_end,
            freq=freq,
            fields=fields,
            adjust=adjust,
            table_family=table_family,
            data_root=data_root,
            api=shared_api,
        )
        if not df.empty:
            yield df


def _date_chunks(start: str, end: str) -> list[tuple[str, str]]:
    """将 ``[start, end]`` 切分为自然月区间列表。"""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    chunks: list[tuple[str, str]] = []
    current = s
    while current <= e:
        # 计算当前自然月月末
        if current.month == 12:
            month_end = date(current.year, 12, 31)
        else:
            month_end = date(current.year, current.month + 1, 1) - timedelta(days=1)
        chunk_end = min(month_end, e)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return chunks


# --- alphalens 适配器 ---
def to_alphalens_prices(
    start: str = _DEFAULT_START,
    end: str = _DEFAULT_END,
    symbols: list[str] | None = None,
    *,
    data_root: str | Path | None = None,
    api: DataAPI | None = None,
) -> pd.DataFrame:
    """返回适用于 alphalens 的前复权日线收盘价透视表（``date × ticker``）。"""
    df = load_bars(
        symbols,
        start,
        end,
        freq="daily",
        adjust="forward",
        table_family="equity",
        data_root=data_root,
        api=api,
    )
    if df.empty:
        return pd.DataFrame()
    return df.pivot(index="date", columns="ticker", values="close")


def to_alphalens_factor(factor_df: pd.DataFrame) -> pd.Series:
    """将含 ``date``、``ticker`` 与单一因子列的 DataFrame 转为 MultiIndex ``(date, ticker)`` Series。"""
    value_cols = [c for c in factor_df.columns if c not in ("date", "ticker")]
    if len(value_cols) != 1:
        raise ValueError(
            f"factor_df must have exactly one value column besides date/ticker; "
            f"got {len(value_cols)}: {value_cols}"
        )
    value_col = value_cols[0]
    indexed = factor_df.set_index(["date", "ticker"])[value_col]
    indexed.name = value_col
    return indexed.sort_index()
