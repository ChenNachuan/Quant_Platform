"""目标权重与当前持仓对比生成订单列表。

链路为 ``target_weights -> compute_order_diff -> generate_orders``。
本模块不执行撮合、计费或盘前风控（均在 ``broker.submit_orders()`` 中完成），
也不导入 ``research/`` 或 broker。行情读取复用 :mod:`execution.market_data`，
整手取整复用 :func:`execution.portfolio_construction.floor_to_lot`。
"""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping
from datetime import date

import pandas as pd

from execution.market_data import get_daily_volume, get_latest_price
from execution.portfolio_construction import floor_to_lot

logger = logging.getLogger(__name__)

TARGET_WEIGHT_COLUMNS = ("ticker", "weight")
ORDER_KEYS = ("ticker", "side", "shares", "limit_price")
SIDES = ("BUY", "SELL")


def compute_order_diff(
    target_weights: pd.DataFrame,
    account,
    nav: float,
    bars: pd.DataFrame,
    asof: date | str,
    *,
    lot_size: int = 100,
) -> list[dict]:
    """根据目标权重与当前持仓的差额生成 BUY/SELL 订单列表。

    按 ``asof`` 当日原始收盘价计算 ``target_shares = floor_to_lot(w * nav / price, lot_size)``。
    无有效价格（如停牌）的标的直接跳过；股数差为 0 不生成订单；
    已有持仓但未出现在 ``target_weights`` 中的标的视为清仓（目标权重 0）。
    ``account`` 通过 ``.holdings``（``Mapping[str, int]``）鸭子类型读取。
    非法权重、非正或非有限 ``nav``、负/小数持仓及缺列均抛出 ``ValueError``。
    """
    _validate_lot_size(lot_size)
    nav = _validate_nav(nav)
    target = _weight_map(target_weights)
    holdings = _holdings_map(account)

    orders: list[dict] = []
    for ticker in _diff_tickers(target_weights, target, holdings):
        price = get_latest_price(bars, ticker, asof, field="close")
        if price is None:
            continue
        raw_quantity = target.get(ticker, 0.0) * nav / price
        target_shares = floor_to_lot(raw_quantity, lot_size) if raw_quantity > 0 else 0
        diff = target_shares - holdings.get(ticker, 0)
        if diff == 0:
            continue
        side = "BUY" if diff > 0 else "SELL"
        orders.append(
            {
                "ticker": ticker,
                "side": side,
                "shares": int(abs(diff)),
                "limit_price": price,
            }
        )
    return orders


def generate_orders(
    target_weights: pd.DataFrame,
    account,
    nav: float,
    bars: pd.DataFrame,
    asof: date | str,
    *,
    lot_size: int = 100,
    scores: Mapping[str, float] | None = None,
) -> list[dict]:
    """完整订单生成：计算差额 -> 流动性告警（仅记录日志） -> 先卖后买排序。

    卖单按名义金额降序排列（先回笼资金），买单若提供 ``scores`` 则按得分降序，
    否则按目标权重降序；同分按代码字典序保证结果确定性。
    ``check_split_needed`` 仅记录告警，绝不拦截、丢弃或重排订单。
    """
    weights = _weight_map(target_weights)
    _validate_scores(scores)
    orders = compute_order_diff(
        target_weights, account, nav, bars, asof, lot_size=lot_size
    )
    for order in orders:
        for tag in check_split_needed(order, bars, asof):
            logger.warning("order check: %s %s", tag, order)

    def notional(order: dict) -> float:
        return order["shares"] * order["limit_price"]

    sells = sorted(
        (o for o in orders if o["side"] == "SELL"),
        key=lambda o: (-notional(o), o["ticker"]),
    )
    if scores is None:
        buys = sorted(
            (o for o in orders if o["side"] == "BUY"),
            key=lambda o: (-weights.get(o["ticker"], 0.0), o["ticker"]),
        )
    else:
        buys = sorted(
            (o for o in orders if o["side"] == "BUY"),
            key=lambda o: (
                -scores.get(o["ticker"], float("-inf")),
                -weights.get(o["ticker"], 0.0),
                o["ticker"],
            ),
        )
    return sells + buys


def check_split_needed(
    order: dict,
    bars: pd.DataFrame,
    asof: date | str,
    *,
    max_volume_share: float = 0.01,
) -> list[str]:
    """检查订单股数是否超过当日成交量的 ``max_volume_share``（仅告警，不拆单）。

    超限时返回 ``["liquidity:<ticker>"]``，否则返回 ``[]``。
    契约：绝不抛出异常、不丢弃且不修改 ``order``；缺量或异常输入仅静默返回 ``[]``。
    """
    try:
        ticker = order.get("ticker")
        shares = order.get("shares")
        if not isinstance(ticker, str) or not ticker:
            return []
        if isinstance(shares, bool) or not isinstance(shares, (int, float)):
            return []
        if not (shares > 0):
            return []
        if not (max_volume_share >= 0):
            return []
        volume = get_daily_volume(bars, ticker, asof)
        if volume is None or not (volume > 0):
            return []
        if shares > max_volume_share * volume:
            return [f"liquidity:{ticker}"]
    except Exception:  # noqa: BLE001 — 仅告警契约：绝不向订单路径抛异常
        return []
    return []


# --- 内部辅助函数 ---


def _diff_tickers(
    target_weights: pd.DataFrame,
    target: dict[str, float],
    holdings: Mapping[str, int],
) -> list[str]:
    """优先保持目标权重表行序，再追加需清仓的现有持仓。"""
    seen = set(target)
    extras = sorted(t for t in holdings if t not in seen)
    return list(target_weights["ticker"]) + extras


def _weight_map(target_weights: pd.DataFrame) -> dict[str, float]:
    """校验 ``target_weights``（需含 ``ticker``/``weight``、无重复且权重有限非负）并返回映射。"""
    if not isinstance(target_weights, pd.DataFrame):
        raise TypeError(
            f"target_weights must be a pandas DataFrame, got "
            f"{type(target_weights).__name__}"
        )
    missing = [c for c in TARGET_WEIGHT_COLUMNS if c not in target_weights.columns]
    if missing:
        raise ValueError(
            f"target_weights must carry columns {list(TARGET_WEIGHT_COLUMNS)}, "
            f"missing {missing}"
        )
    target: dict[str, float] = {}
    for ticker, weight in zip(target_weights["ticker"], target_weights["weight"]):
        if not isinstance(ticker, str) or not ticker:
            raise ValueError(f"ticker must be a non-empty string, got {ticker!r}")
        if ticker in target:
            raise ValueError(f"duplicate ticker row in target_weights: {ticker!r}")
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise ValueError(f"weight for {ticker!r} must be a float, got {weight!r}")
        if not (weight >= 0) or not math.isfinite(weight):
            raise ValueError(
                f"weight for {ticker!r} must be finite and >= 0 (NaN refused), "
                f"got {weight!r}"
            )
        target[ticker] = float(weight)
    return target


def _holdings_map(account) -> Mapping[str, int]:
    """校验 ``account.holdings`` 为非负整数股数映射。"""
    holdings = getattr(account, "holdings", None)
    if not isinstance(holdings, Mapping):
        raise TypeError(
            f"account must expose a .holdings Mapping[str, int], got "
            f"{type(holdings).__name__}"
        )
    for ticker, quantity in holdings.items():
        if not isinstance(ticker, str) or not ticker:
            raise ValueError(
                f"holding ticker must be a non-empty string, got {ticker!r}"
            )
        if isinstance(quantity, bool) or not isinstance(quantity, (int, float)):
            raise ValueError(
                f"holding quantity for {ticker!r} must be an int of shares, "
                f"got {quantity!r}"
            )
        if (
            not (quantity >= 0)
            or not math.isfinite(quantity)
            or int(quantity) != quantity
        ):
            raise ValueError(
                f"holding quantity for {ticker!r} must be a non-negative integer "
                f"of shares (NaN refused), got {quantity!r}"
            )
    return holdings


def _validate_nav(nav: float) -> float:
    if isinstance(nav, bool) or not isinstance(nav, (int, float)):
        raise ValueError(f"nav must be a float, got {nav!r}")
    if not (nav > 0) or not math.isfinite(nav):
        raise ValueError(
            f"nav must be strictly positive and finite (NaN refused), got {nav!r}"
        )
    return float(nav)


def _validate_lot_size(lot_size: int) -> None:
    if not isinstance(lot_size, int) or isinstance(lot_size, bool) or lot_size <= 0:
        raise ValueError(f"lot_size must be a positive int, got {lot_size!r}")


def _validate_scores(scores: Mapping[str, float] | None) -> None:
    if scores is None:
        return
    if not isinstance(scores, Mapping):
        raise TypeError(
            f"scores must be a Mapping[str, float] or None, got {type(scores).__name__}"
        )
    for ticker, score in scores.items():
        if isinstance(score, bool) or not isinstance(score, (int, float)):
            raise ValueError(f"score for {ticker!r} must be a float, got {score!r}")
        if not math.isfinite(score):
            raise ValueError(
                f"score for {ticker!r} must be finite (NaN refused), got {score!r}"
            )
