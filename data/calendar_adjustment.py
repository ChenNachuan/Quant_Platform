"""时间与价格坐标层：交易日历、期货夜盘归属、连续合约与复权计算。

核心约定：
- 日历文件仅含 ``trade_date: date`` 列（缺失非交易日）；文件不存在直接抛 ``FileNotFoundError``，不做静默回退。
- 时间戳一律为交易所本地 naive 时间（Asia/Shanghai），严禁使用 UTC。
- 期货夜盘归属：``hour >= 21`` 归入下一交易日；``hour < 3`` 延续至当前交易日；数据源自带 ``trade_date`` 优先。
- 复权公式：``hfq = raw * adj``，``qfq = raw * adj / adj_latest``（首日基准 1.0）；收益率分子分母消去基准，保证回测与实盘一致。
- 连续合约：按日 **成交量** 取主力合约，换月日按双边收盘价计算比值并累积 ``adjfactor``，每日全量重建。
- 复权价格仅在读取时即时计算，永不落盘（零回填）。
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from data.storage_schema import PARTITION_PATHS
from core.config import DATA_ROOT

_DEFAULT_DATA_ROOT = str(DATA_ROOT)


def _as_date(d: date) -> date:
    """将 *d* 规范为纯 ``date``（把 ``datetime``/``pd.Timestamp`` 窄化，避免 ``bisect`` 比较失配）。"""
    return d.date() if isinstance(d, datetime) else d


class TradingCalendar:
    """单交易所交易日历导航。

    ``exchange`` 为存储层交易所名（如 ``sse``、``shfe``），对应 ``{data_root}/asset/calendar/{exchange}.parquet``；
    文件缺失时直接抛 ``FileNotFoundError``。加载时自动去重排序，``is_trading_day`` 为 O(1)，其余导航方法为 O(log n)。
    """

    def __init__(
        self,
        exchange: str,
        data_root: str | Path = _DEFAULT_DATA_ROOT,
    ) -> None:
        rel = PARTITION_PATHS["calendar"].format(exchange=exchange)
        path = Path(data_root) / rel
        if not path.exists():
            legacy = Path(data_root) / "asset" / "calendar" / f"{exchange}.parquet"
            if legacy.exists():
                path = legacy
            else:
                raise FileNotFoundError(f"calendar file not found: {path}")
        trade_date = pd.read_parquet(path)["trade_date"]
        self._days: list[date] = sorted(
            set(pd.to_datetime(trade_date).dt.date.tolist())
        )
        self._set: set[date] = set(self._days)

    def is_trading_day(self, d: date) -> bool:
        """判断 *d* 是否为交易日（O(1) 集合查询）。"""
        return _as_date(d) in self._set

    def prev(self, d: date, n: int = 1) -> date:
        """返回严格早于 *d* 的第 *n* 个交易日；不足 *n* 个或早于日历起点时抛 ``IndexError``。"""
        i = bisect_left(self._days, _as_date(d))
        if i - n < 0:
            raise IndexError(f"prev({d}, {n}) out of range: before calendar start")
        return self._days[i - n]

    def next(self, d: date, n: int = 1) -> date:
        """返回严格晚于 *d* 的第 *n* 个交易日；超出日历末日时抛 ``IndexError``（不回退系统时钟）。"""
        i = bisect_right(self._days, _as_date(d))
        if i + n - 1 >= len(self._days):
            raise IndexError(f"next({d}, {n}) out of range: past calendar end")
        return self._days[i + n - 1]

    def range(self, start: date, end: date) -> list[date]:
        """返回闭区间 ``[start, end]`` 内的交易日列表。"""
        lo = bisect_left(self._days, _as_date(start))
        hi = bisect_right(self._days, _as_date(end))
        return self._days[lo:hi]

    def count(self, start: date, end: date) -> int:
        """返回闭区间 ``[start, end]`` 内的交易日天数（基于二分下标差，不构造列表）。"""
        lo = bisect_left(self._days, _as_date(start))
        hi = bisect_right(self._days, _as_date(end))
        return max(hi - lo, 0)


def assign_trade_date_futures(
    ts: pd.Timestamp,
    cal: TradingCalendar,
    source_trade_date: date | None = None,
) -> date:
    """按夜盘规则将期货 Bar 时间戳归属到交易日。

    ``source_trade_date`` 非空时直接采用；``hour >= 21`` 归入下一交易日（自动跳过周末）；
    ``hour < 3`` 与日盘共用规则：若当日为交易日则取当日，否则顺延至下一交易日。
    """
    if source_trade_date is not None:
        return _as_date(source_trade_date)
    d = ts.date()
    if ts.hour >= 21:
        return cal.next(d, 1)
    # hour < 3 与日盘共用顺延规则：非交易日时顺延至下一交易日
    return d if cal.is_trading_day(d) else cal.next(d, 1)


_REQUIRED_BARS = ("trade_date", "contract", "close", "volume")
_PASSTHROUGH = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "open_interest",
    "settle",
)


def build_continuous(symbol: str, bars: pd.DataFrame) -> pd.DataFrame:
    """构建单品种主力连续合约序列（每日全量重建，无增量状态）。

    规则：
    1. 按日取最大 ``volume`` 为主力合约（成交量相同时取合约代码字典序最小者）。
    2. 换月日步长 ``close_old / close_new`` 使用当日双边收盘价；旧合约收盘缺失或新合约非正时抛 ``ValueError``，旧合约非正时步长退化为 1.0。
    3. 累积 ``adjfactor`` 首日基准为 1.0；衍生 ``close_adj`` 为前复权价（落盘时由存储层丢弃，仅保留原始价与 ``adjfactor``）。
    """
    required = [c for c in _REQUIRED_BARS if c not in bars.columns]
    if required:
        raise ValueError(f"bars[{symbol}] missing required columns: {required}")
    if bars.empty:
        raise ValueError(f"bars[{symbol}] is empty")
    df = bars.copy()
    if df.duplicated(subset=["trade_date", "contract"]).any():
        raise ValueError(f"bars[{symbol}] has duplicate (trade_date, contract) rows")
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    # 按日取最大成交量为主力；平局取合约代码最小者
    ranked = df.sort_values(
        ["trade_date", "volume", "contract"],
        ascending=[True, False, True],
        kind="stable",
    )
    dom = (
        ranked.groupby("trade_date", sort=True, as_index=False)
        .head(1)
        .sort_values("trade_date", kind="stable")
        .reset_index(drop=True)
    )

    step = pd.Series(1.0, index=dom.index)
    for k in range(1, len(dom)):
        old_c = dom["contract"].iloc[k - 1]
        new_c = dom["contract"].iloc[k]
        if old_c == new_c:
            continue
        t = dom["trade_date"].iloc[k]
        new_close = float(dom["close"].iloc[k])
        if not new_close > 0:
            raise ValueError(
                f"bars[{symbol}]: dominant close {new_close} on {t.date()} "
                f"({new_c}) is not positive"
            )
        old_row = df[(df["trade_date"] == t) & (df["contract"] == old_c)]
        if old_row.empty:
            raise ValueError(
                f"bars[{symbol}]: roll on {t.date()} needs {old_c}'s close that "
                f"day (roll ratio denominator); row missing"
            )
        old_close = float(old_row["close"].iloc[0])
        if old_close > 0:
            # 累积 adjfactor（hfq = raw * adj，首日基准 1.0）累乘 old_close / new_close，使复权序列跨换月平滑衔接
            step.iloc[k] = old_close / new_close

    adjfactor = step.cumprod()
    out = pd.DataFrame(
        {
            "trade_date": dom["trade_date"].dt.date,
            "dominant_contract": dom["contract"],
        }
    )
    for col in _PASSTHROUGH:
        if col in dom.columns:
            out[col] = dom[col].to_numpy()
    out["adjfactor"] = adjfactor.to_numpy()
    out["close_adj"] = out["close"] * out["adjfactor"] / float(adjfactor.iloc[-1])
    return out


def _series_key(df: pd.DataFrame) -> str | None:
    """返回用于分组计算 ``adjfactor`` 的序列键（有 ``ticker`` 列时按标的分段，连续合约整表为单一序列）。"""
    return "ticker" if "ticker" in df.columns else None


class PriceAccessor:
    """原始日线帧的视图访问器，支持未复权、前复权与收益率计算（按需读取计算，不落盘）。

    要求序列内按 ``trade_date`` 升序排列；每次调用实时读取末端 ``adjfactor`` 作为 ``adj_latest``，确保因子更新零回填。
    """

    def __init__(self, daily_df: pd.DataFrame) -> None:
        self._df = daily_df

    def _adj_latest(self) -> pd.Series | float:
        key = _series_key(self._df)
        if key is None:
            return float(self._df["adjfactor"].iloc[-1])
        return self._df.groupby(key)["adjfactor"].transform("last")

    def raw(self, field: str = "close") -> pd.Series:
        """返回原始未复权字段。"""
        return self._df[field]

    def forward_adj(self, field: str = "close") -> pd.Series:
        """返回前复权（qfq）序列：``raw * adj / adj_latest``。"""
        return self._df[field] * self._df["adjfactor"] / self._adj_latest()

    def returns(self, n: int = 1) -> pd.Series:
        """返回 *n* 期收益率 ``close_T*adj_T / (close_{T-n}*adj_{T-n}) - 1``（首 *n* 行为 NaN）。"""
        adj_close = self.forward_adj("close")
        key = _series_key(self._df)
        if key is None:
            return adj_close.pct_change(n)
        return adj_close.groupby(self._df[key]).pct_change(n)
