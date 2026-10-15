"""Multi-asset trading rules — single lookup table.

Pure definitions, no IO (red line 3).  Rule execution lives in each layer
(see ``design/core/asset_rules.md``).
"""

import re

from core.types import AssetType

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


def is_stamp_tax_exempt_prefix(ticker: str) -> bool:
    """Return True when *ticker*'s two-digit prefix is stamp-tax-exempt.

    Checks against :data:`STAMP_TAX_EXEMPT_PREFIXES` (derived from the tables).
    """
    return ticker[:2] in STAMP_TAX_EXEMPT_PREFIXES


# ---------------------------------------------------------------------------
# PREFIX_RULES — ticker prefix -> (AssetType, exchange token) (gap #9)
# ---------------------------------------------------------------------------
# One table, two derived views. A prefix names both an asset class and a
# venue, and the two used to live in separate hand-written tables — which is
# exactly how gap #17 split (a 5-tuple that missed `50` while the lookup table
# had it). A-share two-digit numeric prefixes only: B-share 90/20 are out of
# V1 scope, and futures product codes go through ``validate_futures_ticker``.
PREFIX_RULES: dict[str, tuple[AssetType, str]] = {
    # Shanghai
    "60": (AssetType.STOCK, "SH"),  # main board
    "68": (AssetType.STOCK, "SH"),  # STAR Market
    "51": (AssetType.ETF, "SH"),  # traditional ETF
    "56": (AssetType.ETF, "SH"),  # new numbering segment
    "58": (AssetType.ETF, "SH"),  # STAR / cross-market ETF
    "50": (AssetType.FUND, "SH"),  # Shanghai LOF / closed-end fund (501xxx/502xxx)
    # Shenzhen
    "00": (AssetType.STOCK, "SZ"),  # main board
    "30": (AssetType.STOCK, "SZ"),  # ChiNext
    "15": (AssetType.ETF, "SZ"),  # Shenzhen ETF
    "16": (AssetType.FUND, "SZ"),  # LOF / structured fund
    # Beijing Stock Exchange
    "83": (AssetType.STOCK, "BJ"),
    "87": (AssetType.STOCK, "BJ"),
    "43": (AssetType.STOCK, "BJ"),  # V2 old third-board transfer
}

#: ``prefix -> AssetType``. Derived — never edited by hand.
ASSET_PREFIXES: dict[str, AssetType] = {
    prefix: rule[0] for prefix, rule in PREFIX_RULES.items()
}

#: ``prefix -> the only exchange token that prefix may carry``. Derived, so a
#: ``600000.SZ`` (Shanghai prefix, Shenzhen suffix) cannot be validated by one
#: table and ignored by the other.
PREFIX_EXCHANGE: dict[str, str] = {
    prefix: rule[1] for prefix, rule in PREFIX_RULES.items()
}

#: Futures product code -> the only exchange it trades on. Same reason as
#: ``PREFIX_RULES``: an ``IF2406.SHFE`` must fail, not pass, because the
#: suffix drives calendar selection and instrument identity.
#:
#: This is a **contradiction detector, not an admission list**. A product
#: that is absent is not thereby illegal — exchanges list new contracts, and
#: a closed whitelist turns ``validate_futures_ticker`` into an engine that
#: rejects live instruments at write time. Unknown product + a valid
#: exchange token therefore passes; a known product with the wrong venue
#: fails. Keep the map as complete as the listings are known, so the
#: contradiction check has reach, but never make correctness depend on it
#: being exhaustive.
FUTURES_EXCHANGE: dict[str, str] = {
    # CFFEX — index & bond futures
    "IF": "CFFEX", "IH": "CFFEX", "IC": "CFFEX", "IM": "CFFEX", "T": "CFFEX",
    "TF": "CFFEX", "TS": "CFFEX", "TL": "CFFEX",
    # SHFE — metals & chemicals
    "CU": "SHFE", "AL": "SHFE", "ZN": "SHFE", "PB": "SHFE", "NI": "SHFE",
    "SN": "SHFE", "AU": "SHFE", "AG": "SHFE", "RB": "SHFE", "HC": "SHFE",
    "SS": "SHFE", "BU": "SHFE", "RU": "SHFE", "FU": "SHFE", "SP": "SHFE",
    "BR": "SHFE", "AO": "SHFE",
    # DCE — agricultural & industrial
    "M": "DCE", "Y": "DCE", "P": "DCE", "C": "DCE", "CS": "DCE", "A": "DCE",
    "B": "DCE", "JD": "DCE", "L": "DCE", "V": "DCE", "PP": "DCE", "J": "DCE",
    "JM": "DCE", "I": "DCE", "EG": "DCE", "EB": "DCE", "PG": "DCE", "LH": "DCE",
    "RR": "DCE", "BB": "DCE", "FB": "DCE", "LG": "DCE",
    # CZCE — agricultural & chemical
    "SR": "CZCE", "CF": "CZCE", "ZC": "CZCE", "TA": "CZCE", "MA": "CZCE",
    "OI": "CZCE", "RM": "CZCE", "FG": "CZCE", "SA": "CZCE", "UR": "CZCE",
    "SF": "CZCE", "SM": "CZCE", "AP": "CZCE", "CJ": "CZCE", "PK": "CZCE",
    "PF": "CZCE", "SH": "CZCE", "PX": "CZCE", "CY": "CZCE", "WH": "CZCE",
    "RI": "CZCE", "RS": "CZCE", "JR": "CZCE", "LR": "CZCE", "PM": "CZCE",
    # INE — energy & international
    "SC": "INE", "NR": "INE", "LU": "INE", "BC": "INE", "EC": "INE",
    # GFEX — green / new materials
    "SI": "GFEX", "LC": "GFEX", "PS": "GFEX",
}

# ---------------------------------------------------------------------------
# STAMP_TAX_EXEMPT_PREFIXES — derived, not maintained by hand (gap #17)
# ---------------------------------------------------------------------------
# Derived from ASSET_PREFIXES + ASSET_RULES: a prefix is stamp-tax-exempt
# when its asset type's rules say stamp_tax is False.  Adding a prefix to
# ASSET_PREFIXES is the single edit that makes this set follow.
# ``execution/fee_model.py`` must import from here, not copy.
STAMP_TAX_EXEMPT_PREFIXES: frozenset[str] = frozenset(
    p for p, at in ASSET_PREFIXES.items()
    if not ASSET_RULES[at.value]["stamp_tax"]
)

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
_equity_re = re.compile(r"^(\d{6})\.(SH|SZ|BJ)\Z")

# Futures: product letters + month digits, e.g. IF2406, T2406, SC2412.
_futures_re = re.compile(r"^([A-Z]{1,2})\d{4}\.(CFFEX|SHFE|DCE|CZCE|INE|GFEX)\Z")


def validate_equity_ticker(ticker: str) -> bool:
    """Validate an A-share equity ticker (L13 replacement).

    Format: ``{6-digit code}.{SH|SZ|BJ}``.  Code's two-digit prefix must be
    in :data:`ASSET_PREFIXES`.  Unknown prefix or exchange returns ``False``
    (fails loudly, no silent fallback).
    """
    m = _equity_re.match(ticker)
    if m is None:
        return False
    code, exchange = m.groups()
    prefix = code[:2]
    # Prefix and suffix must agree. Checking them independently accepted
    # `600000.SZ` (Shanghai prefix, Shenzhen suffix) as valid, and the suffix
    # is what drives calendar selection and instrument identity downstream.
    return PREFIX_EXCHANGE.get(prefix) == exchange


def validate_futures_ticker(ticker: str) -> bool:
    """Validate a futures ticker.

    Format: ``{product letters}{month digits}.{exchange}``, e.g. ``IF2406.CFFEX``.
    Futures product codes are NOT in :data:`ASSET_PREFIXES` (they are not
    two-digit numeric prefixes).
    """
    m = _futures_re.match(ticker)
    if m is None:
        return False
    product, exchange = m.groups()
    known = FUTURES_EXCHANGE.get(product)
    # Contradiction check, not admission control. A known product must carry
    # its own venue (`IF2406.SHFE` fails). An unknown product has nothing to
    # contradict, so a valid exchange token is accepted — refusing it would
    # mean every new listing is an ingest failure until this map is edited,
    # and the map is not a source of truth the exchanges consult.
    return known is None or known == exchange
