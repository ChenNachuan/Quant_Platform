"""A-share fee model: commission / stamp tax / transfer fee / slippage.

Design authority: ``docs/design/execution/fee_model.md`` (§18, §66.4).
Answers one question only — what a fill costs.  Fill-price determination
belongs to broker; bookkeeping to ledger.

Stamp-tax exemption is read from :mod:`core.asset_rules` (gap #17 ruling):
the derived set ``STAMP_TAX_EXEMPT_PREFIXES`` of listed-fund prefixes is the
single source.  This module deliberately keeps no copy of it and no ``_is_etf``
predicate — the exempt set is not an ETF list (50/16 are FUND), so the
naming says listed-fund / stamp-tax-exempt, never ETF.
"""

from __future__ import annotations

from core.asset_rules import is_stamp_tax_exempt_prefix


class FeeModel:
    """A-share fee suite (教程 §3.2): one :meth:`compute` per fill.

    Slippage is modelled as a pure cost on both sides — the buy-advantageous
    sign lives in the fill price (buy fills higher, sell fills lower), never
    in the fee, so a wrong sign can never turn slippage into profit.
    """

    COMMISSION_RATE = 0.00025  # 万 2.5, double-sided
    COMMISSION_MIN = 5.0  # floor — the small-order killer
    STAMP_TAX_RATE = 0.0005  # 万 5, SELL only
    TRANSFER_FEE_RATE = 0.00001  # 万 0.1, double-sided
    SLIPPAGE_RATE = 0.001  # 0.1%, always against us

    def compute(self, side: str, price: float, shares: int, ticker: str) -> float:
        """Total fee for one fill = commission + stamp tax + transfer + slippage.

        *side* follows the order-dict convention: ``"BUY"`` or ``"SELL"``.
        Stamp tax is charged on SELL only and is waived for tickers whose
        prefix is stamp-tax-exempt per :mod:`core.asset_rules` (gap #17).

        Raises ``ValueError`` on non-positive *price* or *shares*: a negative
        share count would turn cost into a credit and falsify both advertised
        invariants (fee >= 0, sell >= buy).  Rejection, not precondition, is
        what makes those invariants hold over the whole input space.
        """
        if side not in ("BUY", "SELL"):
            raise ValueError(f"side must be 'BUY' or 'SELL', got {side!r}")
        if price <= 0 or shares <= 0:
            raise ValueError(
                f"price and shares must be positive, got price={price!r}, shares={shares!r}"
            )
        value = price * shares
        commission = max(value * self.COMMISSION_RATE, self.COMMISSION_MIN)
        stamp = (
            value * self.STAMP_TAX_RATE
            if side == "SELL" and not is_stamp_tax_exempt_prefix(ticker)
            else 0.0
        )
        transfer = value * self.TRANSFER_FEE_RATE
        slippage = value * self.SLIPPAGE_RATE
        return commission + stamp + transfer + slippage

    def buy_fee(self, price: float, shares: int, ticker: str = "") -> float:
        """BUY-side total fee.  *ticker* defaults to a taxable instrument."""
        return self.compute("BUY", price, shares, ticker)

    def sell_fee(self, price: float, shares: int, ticker: str = "") -> float:
        """SELL-side total fee.  *ticker* defaults to a taxable instrument."""
        return self.compute("SELL", price, shares, ticker)
