"""Pre-trade risk control: seven rules, PASS/BLOCK, append-only audit.

Design authority: ``docs/design/execution/risk_control.md`` (§66.3).
Guards against strategy runaway and operator error — not market risk.
Pre-trade only, no intra-trade monitoring.  Every interception is traceable
via :func:`log_risk_audit`.

Settled call semantics (the §66.3 skeleton leaves these open):
- Rules run as a batch summary, not an order walk: the constraints are on the
  post-batch state, which walk order must not change.
- Rule 4 (min trade value) runs first — it is a filter, not a verdict.  A
  too-small BUY is marked ``skip`` and contributes to no later rule.
- Any BLOCK refuses the whole batch (``passed is False``); callers reject the
  batch on ``passed``, while :meth:`RiskCheckResult.is_blocked` attributes
  which ticker a rule named.  Batch-level rules (total weight, turnover) name
  every ticker they implicate so per-ticker matching stays batch-wide.
- The *account* argument must expose ``cash: float`` and
  ``holdings: dict[str, int]`` (broker.md ``SimAccount`` shape); held shares
  are valued at the batch's limit price, V1 style.

Gap #3 (limit-price rejection ownership) is undecided — the check position is
a reserved interface (:func:`check_price_limit`), not an implementation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from core.asset_rules import ASSET_PREFIXES, get_asset_rules

#: Default audit sink (design §66.3), append-only JSON Lines.
RISK_AUDIT_PATH = Path("data/execution/risk_audit.jsonl")

#: Rule-id -> audit reason.  Keep in sync with the seven-rule table (§66.3).
RULE_REASONS: dict[str, str] = {
    "single_weight": "post-trade single-name weight exceeds max_single_weight",
    "total_weight": "post-trade total position weight exceeds max_total_weight",
    "turnover": "daily gross turnover exceeds max_daily_turnover",
    "short_forbidden": "sell quantity exceeds holdings; short selling prohibited",
    "st_forbidden": "ST instrument is outside the tradable universe",
    "liquidity": "order shares exceed max_volume_share of daily volume",
}


class SupportsHoldings(Protocol):
    """Account shape required by :func:`pre_trade_risk_check`."""

    cash: float
    holdings: dict[str, int]


@dataclass
class RiskCheckResult:
    passed: bool
    blocked_by: list[str]  # offending "rule:ticker" strings
    warnings: list[str]  # warn-only "rule:ticker" strings

    def is_blocked(self, ticker: str) -> bool:
        # Elements are "rule:ticker"; match the ticker suffix exactly.
        return any(b == ticker or b.split(":", 1)[-1] == ticker for b in self.blocked_by)


def pre_trade_risk_check(
    orders: list[dict],
    account: SupportsHoldings,
    nav: float,
    config: dict,
) -> RiskCheckResult:
    """Run the seven pre-trade rules over *orders*.

    Order dicts follow the order_generation contract:
    ``ticker`` / ``side`` (``"BUY"``|``"SELL"``) / ``shares`` / ``limit_price``.
    ``shares`` and ``limit_price`` must be positive — malformed orders raise
    ``ValueError`` rather than get scored.  Rule 4 mutates its order in place
    (``o["skip"] = True``).

    Config knobs (defaults = the §66.3 rule table):
    ``max_single_weight`` 0.25 · ``max_total_weight`` 0.95 ·
    ``max_daily_turnover`` 1.5 · ``min_trade_value`` 5000 ·
    ``max_volume_share`` 0.01 · ``st_tickers`` iterable of tickers ·
    ``daily_volumes`` mapping ticker -> daily volume in shares (rule 7;
    the data source is gap #10, so volumes are injected here, not fetched).
    """
    if nav <= 0:
        raise ValueError(f"nav must be positive, got {nav}")

    max_single = config.get("max_single_weight", 0.25)
    max_total = config.get("max_total_weight", 0.95)
    max_turnover = config.get("max_daily_turnover", 1.5)
    min_trade = config.get("min_trade_value", 5000.0)
    max_vol_share = config.get("max_volume_share", 0.01)
    st_tickers = set(config.get("st_tickers", ()))
    daily_volumes = config.get("daily_volumes", {})

    blocked: list[str] = []
    warnings: list[str] = []

    # Rule 4 first: a skipped BUY fills nowhere, so it must not move any
    # weight, cash or turnover figure below.  Small SELLs are not filtered —
    # skipping them would strand odd-lot positions (design skeleton BUY-only).
    active: list[dict] = []
    for o in orders:
        if o["side"] not in ("BUY", "SELL"):
            raise ValueError(f"order side must be 'BUY' or 'SELL', got {o['side']!r}")
        # Malformed quantities invert the verdicts rather than fail them: a
        # SELL of -2000 passes rule 5 and flips rule 1's post-trade weight.
        # Refuse them instead of scoring them.
        if o["shares"] <= 0:
            raise ValueError(f"order shares must be positive, got {o['shares']!r}")
        if o["limit_price"] <= 0:
            raise ValueError(
                f"order limit_price must be positive, got {o['limit_price']!r}"
            )
        if o["side"] == "BUY" and o["shares"] * o["limit_price"] < min_trade:
            o["skip"] = True
        if not o.get("skip"):
            active.append(o)

    # Batch aggregates: per-ticker post-trade value, per-ticker sell shares.
    px: dict[str, float] = {}
    buy_value: dict[str, float] = {}
    sell_value: dict[str, float] = {}
    sell_shares: dict[str, int] = {}
    for o in active:
        ticker = o["ticker"]
        value = o["shares"] * o["limit_price"]
        px.setdefault(ticker, o["limit_price"])
        if o["side"] == "BUY":
            buy_value[ticker] = buy_value.get(ticker, 0.0) + value
        else:
            sell_value[ticker] = sell_value.get(ticker, 0.0) + value
            sell_shares[ticker] = sell_shares.get(ticker, 0) + o["shares"]
    tickers = list(px)

    # Rule 1 — single-name post-trade weight (held + this batch's net).
    for ticker in tickers:
        held = account.holdings.get(ticker, 0) * px[ticker]
        post_value = held + buy_value.get(ticker, 0.0) - sell_value.get(ticker, 0.0)
        if post_value / nav > max_single:
            blocked.append(f"single_weight:{ticker}")

    # Rule 2 — total post-trade position weight (cash gap keeps the buffer).
    post_cash = account.cash - sum(buy_value.values()) + sum(sell_value.values())
    if (nav - post_cash) / nav > max_total:
        blocked.extend(f"total_weight:{ticker}" for ticker in tickers)

    # Rule 3 — daily turnover = gross traded notional / nav.  Warn only:
    # hard-blocking turnover destroys the signal (§66.3 key decision).
    gross = sum(buy_value.values()) + sum(sell_value.values())
    if gross / nav > max_turnover:
        warnings.extend(f"turnover:{ticker}" for ticker in tickers)

    # Rule 5 — no short selling.  The prohibition is the ASSET_RULES
    # per-type "short" flag (§69.5 parameter source), not a hardcoded False.
    for ticker in tickers:
        if sell_shares.get(ticker, 0) > account.holdings.get(ticker, 0):
            if not _asset_rules_for(ticker)["short"]:
                blocked.append(f"short_forbidden:{ticker}")

    # Rule 6 — ST instruments are excluded upstream (research filter); this
    # is the defense-in-depth backstop on the order stream.
    for ticker in tickers:
        if ticker in st_tickers:
            blocked.append(f"st_forbidden:{ticker}")

    # Rule 7 — liquidity: per-order shares vs daily volume.  Warn only (same
    # treatment as the order_generation split warning).  Volume 0 is real
    # input (a halted name), not "absent": 1% of 0 is 0, so any order is over
    # the cap.  Only a missing entry stays silent.
    for o in active:
        volume = daily_volumes.get(o["ticker"])
        if volume is not None and o["shares"] > max_vol_share * volume:
            warnings.append(f"liquidity:{o['ticker']}")

    return RiskCheckResult(not blocked, blocked, warnings)


def log_risk_audit(
    check: RiskCheckResult, path: str | Path = RISK_AUDIT_PATH
) -> None:
    """Append one JSONL record per finding to *path* (append-only).

    Records carry ``timestamp`` / ``rule`` / ``ticker`` / ``action`` /
    ``reason``.  ``action`` is ``BLOCK`` for each ``blocked_by`` entry and
    ``WARN`` for each warning.  A clean check writes nothing — only
    interceptions and warnings must be traceable.  Rule-4 skips are marked on
    the order dict and are outside :class:`RiskCheckResult`, so they are not
    audited here.
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
    """Reserved check position for limit-price rejection (gap #3) — no body.

    Ownership is undecided (ROADMAP gap #3): §69.5 puts limit rejection in
    ``pre_trade_risk_check``, §18.3 six-gate ② puts it in broker, §24.6/§24.7
    ``pre_check`` has its own ``strict_limit`` switch.  Implementing any of
    them here would pre-judge the ruling, so this slot stays an interface.

    Intended shape once ruled: given one order dict (``ticker`` / ``side`` /
    ``shares`` / ``limit_price``), the previous close and config (e.g.
    ``strict_limit``), return ``"price_limit:{ticker}"`` when the order's
    limit price breaches the board band derived from ASSET_RULES
    ``price_limits`` / ``st_limit``, else ``None``.  Whether the band is
    judged against prev close or the session open is part of the same ruling.
    """
    raise NotImplementedError(
        "gap #3: limit-price rejection ownership undecided — interface only"
    )


def _asset_rules_for(ticker: str) -> dict:
    """ASSET_RULES row for *ticker*'s prefix.  Unknown prefix fails loudly."""
    prefix = ticker[:2]
    asset_type = ASSET_PREFIXES.get(prefix)
    if asset_type is None:
        raise ValueError(f"unknown ticker prefix {prefix!r} in {ticker!r}")
    return get_asset_rules(asset_type.value)
