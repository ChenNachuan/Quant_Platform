"""Multi-asset trading rules — single lookup table.

Pure definitions, no IO (red line 3).  Rule execution lives in each layer
(see ``design/core/asset_rules.md``).
"""

from core.types import AssetType

# Stamp-tax-exempt listed-fund prefixes (Shanghai 51/56/58 + Shenzhen 15/16).
# Single source of truth — ``execution/fee_model.py`` must import from here.
# ``16`` covers Shenzhen LOF as well as ETF.  Supersedes the old 51/15/16 in
# ``design/core/asset_rules.md`` (ROADMAP gap #17).
STAMP_TAX_EXEMPT_LISTED_FUND_PREFIXES: tuple[str, ...] = ("51", "56", "58", "15", "16")

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
