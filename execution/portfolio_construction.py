"""组合构建约束层：原始权重 -> 约束剪裁 -> 整手取整。

仅做约束满足与整手取整，不执行权重优化。
固定管线顺序：个股/行业剪裁 -> 总权重归一化 -> 换手率剪裁 ->
约束重剪（上界与总仓位上限）-> ``pre_opt_weights`` 快照 ->
``floor_to_lot`` -> ``min_trade_value`` 过滤。

先剪裁后取整不变量：向下取整只会缩减权重，不会突破已剪裁的上界；
``min_weight`` 在取整后仅作参考（允许因整手向下取整而略低于下限）。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from core.types import Account, Position, TargetPortfolio, TargetPosition


@dataclass
class PortfolioConstraints:
    """组合约束配置；``min_cash`` 与 ``max_total_weight`` 取更紧者作为总仓位上限。"""

    max_weight: float = 0.10
    min_weight: float = 0.0
    max_turnover: float | None = None
    max_industry_weight: float | None = None
    max_total_weight: float = 0.95
    min_cash: float = 0.0
    lot_size: int = 100
    min_trade_value: float = 5000.0


def floor_to_lot(raw_quantity: float, lot_size: int = 100) -> int:
    """将 ``raw_quantity`` 向下取整为 ``lot_size`` 的整数倍（预算安全方向）。

    不足一手返回 0；采用 NaN 安全校验拒绝非正或非有限输入，
    保证返回值非负且不超过 ``raw_quantity``。
    """
    if not isinstance(lot_size, int) or isinstance(lot_size, bool) or lot_size <= 0:
        raise ValueError(f"lot_size must be a positive int, got {lot_size!r}")
    if not (raw_quantity > 0) or not math.isfinite(raw_quantity):
        raise ValueError(
            f"raw_quantity must be strictly positive and finite (NaN refused), "
            f"got {raw_quantity!r}"
        )
    lots = math.floor(raw_quantity / lot_size)
    qty = lots * lot_size
    if qty > raw_quantity:  # 浮点除法可能略微上舍入，向下修正一手
        qty -= lot_size
    return int(qty)


class PortfolioOptimizer:
    """约束满足求解器：仅剪裁与整手取整，不作优化。"""

    def solve(
        self,
        raw_weights: dict[str, float],
        current_positions: dict[str, Position],
        account: Account,
        constraints: PortfolioConstraints,
        prices: dict[str, float],
        industry_map: dict[str, str] | None = None,
        *,
        strategy_id: str,
        trade_date: date,
        exec_date: date | None = None,
        create_time: datetime | None = None,
    ) -> TargetPortfolio:
        """按约束剪裁 ``raw_weights`` 并取整生成 :class:`~core.types.TargetPortfolio`。

        管线顺序：个股/行业剪裁 -> 总权重归一化 -> 换手率剪裁 ->
        重施上界与总仓位上限 -> 记录 ``pre_opt_weights`` -> ``floor_to_lot`` ->
        ``min_trade_value`` 过滤。返回的 ``positions`` 仅含正持仓（缺席即下游清仓信号）。
        确保输出始终满足 ``max_weight``、``max_industry_weight`` 与总仓位上限；
        对 NaN/无穷大权重、缺失或非正价格、负持仓或当前总权重 > 1 抛出 ``ValueError``。
        """
        _validate_constraints(constraints)
        if not math.isfinite(account.total_asset) or not (account.total_asset > 0):
            raise ValueError(
                f"account.total_asset must be strictly positive and finite "
                f"(NaN refused), got {account.total_asset!r}"
            )
        needed = set(raw_weights)
        for ticker, pos in current_positions.items():
            if pos.quantity != 0:
                needed.add(ticker)
        for ticker in needed:
            _require_price(prices, ticker)

        applied: list[str] = []

        def note(name: str) -> None:
            if name not in applied:
                applied.append(name)

        # 第 1 步：个股上下界剪裁（仅针对目标候选，清仓保持 0）
        w: dict[str, float] = {}
        for ticker, raw in raw_weights.items():
            if not math.isfinite(raw):
                raise ValueError(
                    f"raw weight for {ticker!r} must be finite (NaN refused), "
                    f"got {raw!r}"
                )
            if raw < constraints.min_weight:
                note("min_weight")
            if raw > constraints.max_weight:
                note("max_weight")
            w[ticker] = min(max(raw, constraints.min_weight), constraints.max_weight)

        # 第 1b 步：行业权重剪裁（按行业等比例缩减）
        if constraints.max_industry_weight is not None and industry_map:
            if self._clip_industry(w, industry_map, constraints.max_industry_weight):
                note("max_industry_weight")

        # 第 2 步：总权重归一化（受 max_total_weight 与 1 - min_cash 中更紧者约束）
        total_cap = min(constraints.max_total_weight, 1.0 - constraints.min_cash)
        total = sum(w.values())
        if total > total_cap:
            scale = total_cap / total if total > 0 else 0.0
            w = {t: v * scale for t, v in w.items()}
            note("max_total_weight")
            if 1.0 - constraints.min_cash < constraints.max_total_weight:
                note("min_cash")

        # 第 3 步：换手率剪裁（向当前持仓凸线性插值）
        w_cur = self._current_weights(current_positions, account, prices)
        if constraints.max_turnover is not None:
            if self._clip_turnover(w, w_cur, constraints.max_turnover):
                note("max_turnover")

        # 第 3b 步：重施上界与总仓位约束（当前持仓漂移可能使插值结果越界，
        # 宁可小幅超出换手率也不输出越界组合；不重提 min_weight 以允许部分减仓）
        if any(v > constraints.max_weight for v in w.values()):
            for t in w:
                if w[t] > constraints.max_weight:
                    w[t] = constraints.max_weight
            note("max_weight")
        if constraints.max_industry_weight is not None and industry_map:
            if self._clip_industry(w, industry_map, constraints.max_industry_weight):
                note("max_industry_weight")
        # 再次归一化总权重；等比例缩减只降不升，已重施的个股/行业上界继续成立
        total = sum(w.values())
        if total > total_cap:
            scale = total_cap / total if total > 0 else 0.0
            w = {t: v * scale for t, v in w.items()}
            note("max_total_weight")
            if 1.0 - constraints.min_cash < constraints.max_total_weight:
                note("min_cash")

        # 第 4 步：保存取整前权重快照
        pre_opt = dict(w)

        # 第 5-6 步：整手取整并过滤低于 min_trade_value 的仓位
        positions: dict[str, TargetPosition] = {}
        targeted = 0
        for ticker, weight in w.items():
            if not (weight > 0):
                continue
            targeted += 1
            px = prices[ticker]
            raw_qty = weight * account.total_asset / px
            if not (raw_qty > 0):
                # 浮点下溢导致目标股数为 0 时跳过取整
                continue
            qty = floor_to_lot(raw_qty, constraints.lot_size)
            if qty < raw_qty:
                note("lot_size")
            if qty > 0 and qty * px < constraints.min_trade_value:
                qty = 0
                note("min_trade_value")
            if qty > 0:
                positions[ticker] = TargetPosition(
                    ticker=ticker,
                    target_weight=qty * px / account.total_asset,
                    target_quantity=qty,
                    target_notional=qty * px,
                    ref_price=px,
                    side="LONG",
                )

        if targeted == 0 or len(positions) == targeted:
            solver_status = "optimal"
        elif not positions:
            solver_status = "infeasible"
        else:
            solver_status = "suboptimal"

        total_weight = sum(p.target_weight for p in positions.values())
        estimated_turnover = sum(
            abs((positions[t].target_weight if t in positions else 0.0) - w_cur.get(t, 0.0))
            for t in set(positions) | set(w_cur)
        ) / 2.0

        return TargetPortfolio(
            strategy_id=strategy_id,
            trade_date=trade_date,
            exec_date=exec_date if exec_date is not None else trade_date + timedelta(days=1),
            create_time=create_time if create_time is not None else datetime.now(),
            positions=positions,
            total_weight=total_weight,
            num_holdings=len(positions),
            cash_weight=1.0 - total_weight,
            estimated_turnover=estimated_turnover,
            solver_status=solver_status,
            constraints_applied=applied,
            pre_opt_weights=pre_opt,
        )

    @staticmethod
    def _clip_industry(
        w: dict[str, float], industry_map: dict[str, str], cap: float
    ) -> bool:
        """按比例缩减超限行业权重至 ``cap``；未在 ``industry_map`` 登记的标的保持不变。"""
        by_industry: dict[str, list[str]] = {}
        for ticker in w:
            industry = industry_map.get(ticker)
            if industry is not None:
                by_industry.setdefault(industry, []).append(ticker)
        changed = False
        for members in by_industry.values():
            total = sum(w[t] for t in members)
            if total > cap and total > 0:
                scale = cap / total
                for t in members:
                    w[t] *= scale
                changed = True
        return changed

    @staticmethod
    def _clip_turnover(
        w: dict[str, float], w_cur: dict[str, float], max_turnover: float
    ) -> bool:
        """将目标权重向当前持仓线性插值，使单边换手率 ``sum(|Δw|)/2 <= max_turnover``。"""
        tickers = set(w) | set(w_cur)
        delta = {t: w.get(t, 0.0) - w_cur.get(t, 0.0) for t in tickers}
        turnover = sum(abs(d) for d in delta.values()) / 2.0
        if not (turnover > max_turnover):
            return False
        scale = max_turnover / turnover
        for t in tickers:
            w[t] = w_cur.get(t, 0.0) + delta[t] * scale
        return True

    @staticmethod
    def _current_weights(
        current_positions: dict[str, Position],
        account: Account,
        prices: dict[str, float],
    ) -> dict[str, float]:
        """按当前价格计算现有持仓权重；总权重 > 1 时抛出 ``ValueError``。"""
        w_cur: dict[str, float] = {}
        for ticker, pos in current_positions.items():
            if not isinstance(pos.quantity, int) or pos.quantity < 0:
                raise ValueError(
                    f"position quantity for {ticker!r} must be a non-negative int, "
                    f"got {pos.quantity!r}"
                )
            if pos.quantity == 0:
                continue
            px = _require_price(prices, ticker)
            w_cur[ticker] = pos.quantity * px / account.total_asset
        total = sum(w_cur.values())
        if total > 1.0 + 1e-9:
            raise ValueError(
                f"current book weights sum to {total!r} > 1 — incoherent account "
                f"state for a long-only solver"
            )
        return w_cur


def _require_price(prices: dict[str, float], ticker: str) -> float:
    px = prices.get(ticker)
    if not isinstance(px, (int, float)) or not (px > 0) or not math.isfinite(px):
        raise ValueError(
            f"price for {ticker!r} must be strictly positive and finite "
            f"(NaN refused), got {px!r}"
        )
    return float(px)


def _validate_constraints(constraints: PortfolioConstraints) -> None:
    c = constraints
    if not math.isfinite(c.min_weight) or c.min_weight < 0:
        raise ValueError(f"min_weight must be finite and >= 0, got {c.min_weight!r}")
    if not math.isfinite(c.max_weight) or not (c.min_weight <= c.max_weight <= 1.0):
        raise ValueError(
            f"max_weight must satisfy min_weight <= max_weight <= 1, "
            f"got {c.max_weight!r}"
        )
    if not math.isfinite(c.max_total_weight) or not (0 < c.max_total_weight <= 1.0):
        raise ValueError(
            f"max_total_weight must be in (0, 1] — V1 is long-only, no leverage, "
            f"got {c.max_total_weight!r}"
        )
    if not math.isfinite(c.min_cash) or not (0 <= c.min_cash < 1.0):
        raise ValueError(f"min_cash must be in [0, 1), got {c.min_cash!r}")
    if not isinstance(c.lot_size, int) or isinstance(c.lot_size, bool) or c.lot_size <= 0:
        raise ValueError(f"lot_size must be a positive int, got {c.lot_size!r}")
    if not math.isfinite(c.min_trade_value) or c.min_trade_value < 0:
        raise ValueError(
            f"min_trade_value must be finite and >= 0, got {c.min_trade_value!r}"
        )
    if c.max_turnover is not None and (
        not math.isfinite(c.max_turnover) or not (c.max_turnover > 0)
    ):
        raise ValueError(
            f"max_turnover must be None or strictly positive (NaN refused), "
            f"got {c.max_turnover!r}"
        )
    if c.max_industry_weight is not None and (
        not math.isfinite(c.max_industry_weight) or not (c.max_industry_weight > 0)
    ):
        raise ValueError(
            f"max_industry_weight must be None or strictly positive (NaN refused), "
            f"got {c.max_industry_weight!r}"
        )
