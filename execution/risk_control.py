"""盘前风控模块：七条风控规则、PASS/BLOCK 判定与追加写审计日志。

仅在下单前校验，防范策略失控与操作失误，所有拦截均可经 :func:`log_risk_audit` 追溯。
调用语义：
- 按整批汇总后状态判定，结果与订单遍历顺序无关。
- Rule 4（最小交易金额）优先作为过滤器执行：过小买单标记 ``skip``，不参与后续规则。
- 任一规则触发 BLOCK 即拒绝对整批放行（``passed=False``）。
- 保留 :func:`check_price_limit` 涨跌停校验接口槽位。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from core.asset_rules import ASSET_PREFIXES, get_asset_rules

#: 默认追加写审计日志路径（JSON Lines）
RISK_AUDIT_PATH = Path("logs/audit/risk_audit.jsonl")

#: 规则 ID 到审计原因的映射
RULE_REASONS: dict[str, str] = {
    "single_weight": "post-trade single-name weight exceeds max_single_weight",
    "total_weight": "post-trade total position weight exceeds max_total_weight",
    "turnover": "daily one-sided turnover (gross/2) exceeds max_daily_turnover",
    "short_forbidden": "sell quantity exceeds holdings; short selling prohibited",
    "st_forbidden": "ST instrument is outside the tradable universe",
    "liquidity": "order shares exceed max_volume_share of daily volume",
}


class SupportsHoldings(Protocol):
    """:func:`pre_trade_risk_check` 所需的账户接口协议。"""

    cash: float
    holdings: dict[str, int]


@dataclass
class RiskCheckResult:
    passed: bool
    blocked_by: list[str]  # 拦截条目，格式为 "rule:ticker"
    warnings: list[str]  # 告警条目，格式为 "rule:ticker"

    def is_blocked(self, ticker: str) -> bool:
        # 元素格式为 "rule:ticker"，按后缀精确匹配标的
        return any(
            b == ticker or b.split(":", 1)[-1] == ticker for b in self.blocked_by
        )


def pre_trade_risk_check(
    orders: list[dict],
    account: SupportsHoldings,
    nav: float,
    config: dict,
) -> RiskCheckResult:
    """对 ``orders`` 执行七条盘前风控检查。

    ``shares`` 与 ``limit_price`` 必须严格为正（拒绝 NaN），同批次同标的限价必须一致。
    Rule 4 会原地重算并覆写 ``o["skip"]``，保证重复执行幂等；Rule 6 仅拦截 ST 买入。
    """
    # NaN 安全守卫：用 not (nav > 0) 避免 NaN 在比较中返回 False 导致风控失效
    if not nav > 0:
        raise ValueError(f"nav must be strictly positive (NaN refused), got {nav!r}")

    max_single = config.get("max_single_weight", 0.25)
    max_total = config.get("max_total_weight", 0.95)
    max_turnover = config.get("max_daily_turnover", 1.5)
    min_trade = config.get("min_trade_value", 5000.0)
    max_vol_share = config.get("max_volume_share", 0.01)
    st_tickers = set(config.get("st_tickers", ()))
    daily_volumes = config.get("daily_volumes", {})

    blocked: list[str] = []
    warnings: list[str] = []

    # Rule 4 优先：过滤过小买单（小额卖单不过滤，避免碎股仓位无法清出）
    active: list[dict] = []
    for o in orders:
        if o["side"] not in ("BUY", "SELL"):
            raise ValueError(f"order side must be 'BUY' or 'SELL', got {o['side']!r}")
        # 拒绝非正值与 NaN，防止比较恒为 False 导致风控放行异常订单
        if not o["shares"] > 0:
            raise ValueError(
                f"order shares must be strictly positive (NaN refused), "
                f"got {o['shares']!r}"
            )
        if not o["limit_price"] > 0:
            raise ValueError(
                f"order limit_price must be strictly positive (NaN refused), "
                f"got {o['limit_price']!r}"
            )
        # 始终按当前阈值重算 skip，避免残留状态破坏幂等性
        skip = o["side"] == "BUY" and o["shares"] * o["limit_price"] < min_trade
        o["skip"] = skip
        if not skip:
            active.append(o)

    # 汇总批次净变动与卖出股数；同标的限价冲突直接报错，避免 Rule 1 结果依赖遍历顺序
    px: dict[str, float] = {}
    buy_value: dict[str, float] = {}
    sell_value: dict[str, float] = {}
    sell_shares: dict[str, int] = {}
    for o in active:
        ticker = o["ticker"]
        value = o["shares"] * o["limit_price"]
        if ticker in px and px[ticker] != o["limit_price"]:
            raise ValueError(
                f"conflicting limit_price for {ticker} in one batch: "
                f"{px[ticker]!r} vs {o['limit_price']!r} — one price basis "
                f"per ticker, or the rule-1 verdict depends on walk order"
            )
        px.setdefault(ticker, o["limit_price"])
        if o["side"] == "BUY":
            buy_value[ticker] = buy_value.get(ticker, 0.0) + value
        else:
            sell_value[ticker] = sell_value.get(ticker, 0.0) + value
            sell_shares[ticker] = sell_shares.get(ticker, 0) + o["shares"]
    tickers = list(px)

    # Rule 1：单标的事后持仓权重上限
    for ticker in tickers:
        held = account.holdings.get(ticker, 0) * px[ticker]
        post_value = held + buy_value.get(ticker, 0.0) - sell_value.get(ticker, 0.0)
        if post_value / nav > max_single:
            blocked.append(f"single_weight:{ticker}")

    # Rule 2：总事后持仓权重上限（保留现金缓冲）
    post_cash = account.cash - sum(buy_value.values()) + sum(sell_value.values())
    if (nav - post_cash) / nav > max_total:
        blocked.extend(f"total_weight:{ticker}" for ticker in tickers)

    # Rule 3：单日单边换手率（gross / 2 / nav）超限仅告警，不硬性拦截
    gross = sum(buy_value.values()) + sum(sell_value.values())
    turnover = gross / 2.0 / nav
    if turnover > max_turnover:
        warnings.extend(f"turnover:{ticker}" for ticker in tickers)

    # Rule 5：禁止裸卖空（按 ASSET_RULES 判定该资产类别是否允许做空）
    for ticker in tickers:
        if sell_shares.get(ticker, 0) > account.holdings.get(ticker, 0):
            if not _asset_rules_for(ticker)["short"]:
                blocked.append(f"short_forbidden:{ticker}")

    # Rule 6：禁止买入 ST 标的；允许卖出已持有 ST 仓位以防锁死
    for ticker in tickers:
        if ticker in st_tickers and ticker in buy_value:
            blocked.append(f"st_forbidden:{ticker}")

    # Rule 7：流动性检查（仅告警）；成交量为 0 视为停牌有效输入，仅缺失时不告警
    for o in active:
        volume = daily_volumes.get(o["ticker"])
        if volume is not None and o["shares"] > max_vol_share * volume:
            warnings.append(f"liquidity:{o['ticker']}")

    return RiskCheckResult(not blocked, blocked, warnings)


def log_risk_audit(check: RiskCheckResult, path: str | Path = RISK_AUDIT_PATH) -> None:
    """将拦截（BLOCK）与告警（WARN）追加写入 JSONL 审计日志。

    无异常时不写入；Rule 4 跳过的订单不在 ``RiskCheckResult`` 中，故不记入审计。
    """
    records = []
    timestamp = datetime.now(timezone.utc).isoformat()
    for action, entries in (("BLOCK", check.blocked_by), ("WARN", check.warnings)):
        for entry in entries:
            rule, _, ticker = entry.partition(":")
            records.append(
                {
                    "timestamp": timestamp,
                    "rule": rule,
                    "ticker": ticker,
                    "action": action,
                    "reason": RULE_REASONS.get(rule, rule),
                }
            )
    if not records:
        return
    audit_path = Path(path)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def check_price_limit(order: dict, prev_close: float, config: dict) -> str | None:
    """涨跌停价格限制检查预留接口（归属未决，暂不实现具体逻辑）。"""
    raise NotImplementedError(
        "gap #3: limit-price rejection ownership undecided — interface only"
    )


def _asset_rules_for(ticker: str) -> dict:
    """按标的前缀查询资产规则，未知前缀直接报错。"""
    prefix = ticker[:2]
    asset_type = ASSET_PREFIXES.get(prefix)
    if asset_type is None:
        raise ValueError(f"unknown ticker prefix {prefix!r} in {ticker!r}")
    return get_asset_rules(asset_type.value)
