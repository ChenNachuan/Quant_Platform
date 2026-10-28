"""Target weights vs holdings -> the order list (design §66.2).

``target_weights -> compute_order_diff -> generate_orders``.  This module
does not match, charge fees or run risk checks — those are the three steps
inside ``broker.submit_orders()`` (S7-T4).  Order dicts are the shape
``execution.risk_control`` already consumes:
``{"ticker", "side" in {"BUY", "SELL"}, "shares", "limit_price"}``.

Data access is :mod:`execution.market_data` (gap #10) and lot rounding is
:func:`execution.portfolio_construction.floor_to_lot` — importing those here
is the contract; re-deriving prices, volumes or lots in this module is a
finding.  No ``research/`` import (gap #1 ruling A): the buy sort takes an
optional ``scores`` mapping instead of walking back to the signal.

Settled in the QUANT-27 gap #10 contract (pinned in issue metadata
``kb_snapshot``): ``target_shares = floor_to_lot(w * nav / price)`` at the
asof close, a ticker with no valid price is skipped, a zero diff produces no
order, sells run before buys, and ``check_split_needed`` only warns.
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
    """Target weights minus holdings -> BUY/SELL order dicts.

    ``target_shares = floor_to_lot(w * nav / price, lot_size)`` at the asof
    close (raw, unadjusted — gap #10 rule 4).  A ticker with no valid price
    is skipped entirely (suspended day: the order would be REJECTed at the
    broker); ``diff == 0`` produces no order.  A held ticker absent from
    *target_weights* is an exit (target weight 0).

    *account* is duck-typed on ``.holdings`` (``Mapping[str, int]`` — shares
    held per ticker).  ``SimAccount`` lands in S7-T4; this module never
    imports broker (gap #1 ruling A).

    Raises ``ValueError`` for NaN/inf/negative weights, a non-positive or
    non-finite ``nav``, negative or fractional holdings, or a *target_weights*
    frame without its ``ticker``/``weight`` columns.  Refusing is what makes
    every order this returns a tradeable integer lot at a real price.
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
    """Full chain: diff -> liquidity warnings (log only) -> sells before buys.

    Sells come first sorted by notional descending — free cash before
    spending it (design §66.2) — then buys sorted by *scores* descending
    when given, else by target weight descending.  Ties break on ticker
    ascending so the list is deterministic.  Tickers present in the frame
    with a zero weight are exits, not buys.

    ``check_split_needed`` tags are **logged and ignored** otherwise: the
    rule is warn-only (V1 does not split), so a tag never drops, blocks or
    reorders an order.  *scores* is a plain mapping (NaN refused) — this
    module never imports ``research/`` (gap #1 ruling A).
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
    """Warn tags for an order above *max_volume_share* of the day's volume.

    Warn-only (design §66.2, V1 does not split): returns
    ``["liquidity:<ticker>"]`` when ``shares`` exceeds 1% of that day's
    shares, ``[]`` otherwise.  **Never raises, never drops or mutates
    *order*** — a malformed order or a broken bars frame can only silence
    the warning, never block a trade.  A missing or unusable volume yields
    no tag (cannot judge).
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
    except Exception:  # noqa: BLE001 — warn-only contract: never raise into the order path
        return []
    return []


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------


def _diff_tickers(
    target_weights: pd.DataFrame,
    target: dict[str, float],
    holdings: Mapping[str, int],
) -> list[str]:
    """Frame order first (its sort feeds the buy ordering), then exits."""
    seen = set(target)
    extras = sorted(t for t in holdings if t not in seen)
    return list(target_weights["ticker"]) + extras


def _weight_map(target_weights: pd.DataFrame) -> dict[str, float]:
    """Validate *target_weights* and return ``{ticker: weight}`` in row order.

    Schema: ``[ticker, weight]`` columns (extra columns tolerated).  A
    duplicate ticker row is refused — two rows for one ticker have no single
    target.  Weights must be finite and >= 0: NaN would size a garbage
    order and a negative weight is not a long-only target (zero is an exit).
    """
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
    """``account.holdings`` — duck-typed; values must be non-negative integers."""
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
