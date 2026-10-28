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

    Slippage (0.1%) is one of the four parts, modelled as a pure cost on both
    sides.  Contract (OCR finding 1 — charge it exactly once): *price* is a
    raw reference price (e.g. the decision-day close), **not** a fill price a
    slippage model already skewed.  The fill-price path and this term are the
    same 0.1% assumption seen from two sides — a caller that passes an
    adjusted price and also bills this total charges slippage twice.  The
    buy/sell sign convention of the fill-price path (buy fills higher, sell
    fills lower) belongs to broker and is never applied again here, so a
    wrong sign can never turn slippage into profit.
    """

    COMMISSION_RATE = 0.00025  # 万 2.5, double-sided
    COMMISSION_MIN = 5.0  # floor — the small-order killer
    STAMP_TAX_RATE = 0.0005  # 万 5, SELL only
    TRANSFER_FEE_RATE = 0.00001  # 万 0.1, double-sided
    SLIPPAGE_RATE = 0.001  # 0.1%, always against us

    def compute(self, side: str, price: float, shares: int, ticker: str) -> float:
        """Total cost of one fill = commission + stamp tax + transfer + slippage.

        *side* follows the order-dict convention: ``"BUY"`` or ``"SELL"``.
        *price* must be a raw reference price (see the class docstring) and
        *ticker* is required — a SELL with an empty ticker would silently pay
        stamp tax on an exempt instrument.  Stamp tax is charged on SELL only
        and is waived for tickers whose prefix is stamp-tax-exempt per
        :mod:`core.asset_rules` (gap #17).

        Raises ``ValueError`` unless *price* and *shares* are strictly
        positive.  The guard is NaN-safe (``not (x > 0)``, not ``x <= 0``):
        ``nan <= 0`` is False, so the naive form scores a NaN fee and returns
        NaN — falsifying both advertised invariants (fee >= 0, sell >= buy).
        Refusal is what makes those invariants true of every fee this
        function can return.
        """
        if side not in ("BUY", "SELL"):
            raise ValueError(f"side must be 'BUY' or 'SELL', got {side!r}")
        if side == "SELL" and not ticker:
            raise ValueError(
                "ticker is required on SELL — an empty ticker would silently "
                "pay stamp tax on a stamp-tax-exempt instrument"
            )
        if not (price > 0 and shares > 0):
            raise ValueError(
                f"price and shares must be strictly positive (NaN refused), "
                f"got price={price!r}, shares={shares!r}"
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
        """BUY-side total cost.  *ticker* may default: BUY never pays stamp tax."""
        return self.compute("BUY", price, shares, ticker)

    def sell_fee(self, price: float, shares: int, ticker: str) -> float:
        """SELL-side total cost.  *ticker* is required (OCR finding 2).

        The exemption is prefix-based, so an omitted ticker defaults to
        taxable and an exempt instrument would silently pay stamp tax —
        the failure mode the single-source rule exists to prevent.
        """
        return self.compute("SELL", price, shares, ticker)
