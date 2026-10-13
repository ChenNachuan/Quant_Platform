"""Multi-asset trading rules — single lookup table.

Pure definitions, no IO (red line 3).  Rule execution lives in each layer
(see ``design/core/asset_rules.md``).
"""

import re

from core.types import AssetType

# ---------------------------------------------------------------------------
# Stamp-tax-exempt listed-fund prefixes (gap #17)
# ---------------------------------------------------------------------------
# Shanghai 51/56/58 + Shenzhen 15/16.  Single source of truth —
# ``execution/fee_model.py`` must import from here.  ``16`` covers Shenzhen LOF
# as well as ETF.  Supersedes the old 51/15/16 in ``design/core/asset_rules.md``.
STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES: tuple[str, ...] = ("51", "56", "58", "15", "16")

# ---------------------------------------------------------------------------
# ASSET_RULES — per-asset trading rules
# ---------------------------------------------------------------------------
ASSET_RULES: dict[str, dict] = {
    AssetType.STOCK.value: {
        "t_plus": 1,
        "lot_size": 100,
        "stamp_tax": True,  # charged on sell
        "price_limits": {
            "main": 0.10,  # main board 60/00
            "gem": 0.20,  # ChiNext 30
            "star": 0.20,  # STAR Market 68
            "bse": 0.30,  # Beijing Stock Exchange 8
        },
        "st_limit": 0.05,  # ST +/-5%
        "short": False,  # short selling prohibited
    },
    AssetType.ETF.value: {
        "t_plus": 1,
        "lot_size": 100,
        "stamp_tax": False,  # ETF exempt
        "price_limits": {"main": 0.10},
        "st_limit": None,
        "short": False,
    },
    AssetType.FUND.value: {
        "t_plus": 1,
        "lot_size": 100,
        "stamp_tax": False,  # listed fund (LOF etc.) exempt
        "price_limits": {"main": 0.10},  # LOF trades like stock, +/-10%
        "st_limit": None,
        "short": False,
    },
    # AssetType.BOND:   V2 reserved (T+0, no stamp tax)
    # AssetType.FUTURES: V3 reserved (margin + daily mark-to-market)
    # AssetType.OPTION:  V4 reserved
}


def get_asset_rules(asset_type: str) -> dict:
    """Look up rules for *asset_type*.

    Raises ``KeyError`` for unknown types — no silent fallback.
    """
    return ASSET_RULES[asset_type]


def is_stamp_tax_exempt_listed_fund(ticker: str) -> bool:
    """Return True when *ticker* is a stamp-tax-exempt listed fund.

    Matches Shanghai ETF segments (51/56/58) and Shenzhen ETF/LOF (15/16).
    """
    return ticker[:2] in STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES


# ---------------------------------------------------------------------------
# ASSET_PREFIXES — ticker prefix -> AssetType (gap #9)
# ---------------------------------------------------------------------------
# A-share two-digit numeric prefixes only.  B-share prefixes 90/20 are
# deliberately absent (V1 scope is stock + ETF).  Futures product codes
# (IF/SC/AU...) are NOT here — they go through ``validate_futures_ticker``.
ASSET_PREFIXES: dict[str, AssetType] = {
    # Shanghai
    "60": AssetType.STOCK,  # main board
    "68": AssetType.STOCK,  # STAR Market
    "51": AssetType.ETF,  # traditional ETF
    "56": AssetType.ETF,  # new numbering segment
    "58": AssetType.ETF,  # STAR / cross-market ETF
    "50": AssetType.FUND,  # Shanghai LOF / closed-end fund (501xxx/502xxx)
    # Shenzhen
    "00": AssetType.STOCK,  # main board
    "30": AssetType.STOCK,  # ChiNext
    "15": AssetType.ETF,  # Shenzhen ETF
    "16": AssetType.FUND,  # LOF / structured fund
    # Beijing Stock Exchange
    "83": AssetType.STOCK,
    "87": AssetType.STOCK,
    "43": AssetType.STOCK,  # V2 old third-board transfer
}

# ---------------------------------------------------------------------------
# EXCHANGE_MAP — exchange code -> calendar / storage exchange name
# ---------------------------------------------------------------------------
EXCHANGE_MAP: dict[str, str] = {
    "SH": "sse",
    "SZ": "szse",
    "BJ": "bse",
    "CFFEX": "cffex",
    "SHFE": "shfe",
    "DCE": "dce",
    "CZCE": "czce",
    "INE": "ine",
    "GFEX": "gfex",
}

# Calendar collector generates its exchange list from this set, not a
# hardcoded filename list.  V1 runs only sse + szse; which exchanges are
# actually collected is run configuration.
CALENDAR_EXCHANGES: set[str] = set(EXCHANGE_MAP.values())


# ---------------------------------------------------------------------------
# Ticker validators (gap #9)
# ---------------------------------------------------------------------------
# A-share equity: ``{code}.{exch}`` where code is exactly 6 digits and its
# two-digit prefix is in ASSET_PREFIXES.  Replaces the retired L13 regex.
_equity_re = re.compile(r"^(\d{6})\.(SH|SZ|BJ)$")

# Futures: product letters + month digits, e.g. IF2406, T2406, SC2412.
_futures_re = re.compile(r"^([A-Z]{1,2})\d{4}\.(CFFEX|SHFE|DCE|CZCE|INE|GFEX)$")


def validate_equity_ticker(ticker: str) -> bool:
    """Validate an A-share equity ticker (L13 replacement).

    Format: ``{6-digit code}.{SH|SZ|BJ}``.  Code's two-digit prefix must be
    in :data:`ASSET_PREFIXES`.  Unknown prefix or exchange returns ``False``
    (fails loudly, no silent fallback).
    """
    m = _equity_re.match(ticker)
    if m is None:
        return False
    code, _exchange = m.groups()
    return code[:2] in ASSET_PREFIXES


def validate_futures_ticker(ticker: str) -> bool:
    """Validate a futures ticker.

    Format: ``{product letters}{month digits}.{exchange}``, e.g. ``IF2406.CFFEX``.
    Futures product codes are NOT in :data:`ASSET_PREFIXES` (they are not
    two-digit numeric prefixes).
    """
    return _futures_re.match(ticker) is not None
