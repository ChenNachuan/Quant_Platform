"""多资产交易规则单源查表模块。

纯定义无 I/O（规则执行位于各业务层，见 ``design/core/asset_rules.md``）。
"""

import re

from core.types import AssetType

# --- ASSET_RULES：分资产交易规则 ---
ASSET_RULES: dict[str, dict] = {
    AssetType.STOCK.value: {
        "t_plus": 1,
        "lot_size": 100,
        "stamp_tax": True,  # 卖出征收
        "price_limits": {
            "main": 0.10,  # 主板 60/00
            "gem": 0.20,  # 创业板 30
            "star": 0.20,  # 科创板 68
            "bse": 0.30,  # 北交所 8/4/92
        },
        "st_limit": 0.05,  # ST ±5%
        "short": False,  # 禁止做空
    },
    AssetType.ETF.value: {
        "t_plus": 1,
        "lot_size": 100,
        "stamp_tax": False,  # ETF 免印花税
        "price_limits": {"main": 0.10},
        "st_limit": None,
        "short": False,
    },
    AssetType.FUND.value: {
        "t_plus": 1,
        "lot_size": 100,
        "stamp_tax": False,  # LOF 等场内基金免印花税
        "price_limits": {"main": 0.10},  # LOF 按股票交易规则，±10%
        "st_limit": None,
        "short": False,
    },
    # AssetType.BOND: V2 预留（T+0，免印花税）
    # AssetType.FUTURES: V3 预留（保证金 + 逐日盯市）
    # AssetType.OPTION: V4 预留
}


def get_asset_rules(asset_type: str) -> dict:
    """查询 *asset_type* 对应交易规则；未知类型抛出 ``KeyError``（不做静默回退）。"""
    return ASSET_RULES[asset_type]


def is_stamp_tax_exempt_prefix(ticker: str) -> bool:
    """判断 *ticker* 的两位前缀是否属于 :data:`STAMP_TAX_EXEMPT_PREFIXES`（免印花税）。"""
    return ticker[:2] in STAMP_TAX_EXEMPT_PREFIXES


# --- PREFIX_RULES：两位数字前缀 -> (资产类型, 交易所代码) ---
# 单一真源表，派生 ASSET_PREFIXES 与 PREFIX_EXCHANGE。
# 仅覆盖 A 股两位数字前缀（B 股 90/20 不在 V1 范围；期货走 ``validate_futures_ticker``）。
PREFIX_RULES: dict[str, tuple[AssetType, str]] = {
    # 上海
    "60": (AssetType.STOCK, "SH"),  # 主板
    "68": (AssetType.STOCK, "SH"),  # 科创板
    "51": (AssetType.ETF, "SH"),  # 传统 ETF
    "52": (AssetType.ETF, "SH"),  # 上交所 ETF 号段
    "53": (AssetType.ETF, "SH"),  # 上交所 ETF 号段
    "55": (AssetType.ETF, "SH"),  # 上交所 ETF 号段
    "56": (AssetType.ETF, "SH"),  # 新编 ETF 号段
    "58": (AssetType.ETF, "SH"),  # 科创 / 跨市场 ETF
    "50": (AssetType.FUND, "SH"),  # 上海 LOF / 封闭式基金（501xxx/502xxx）
    # 深圳
    "00": (AssetType.STOCK, "SZ"),  # 主板
    "30": (AssetType.STOCK, "SZ"),  # 创业板
    "15": (AssetType.ETF, "SZ"),  # 深交所 ETF
    "16": (AssetType.FUND, "SZ"),  # LOF / 分级基金
    # 北京证券交易所
    "92": (AssetType.STOCK, "BJ"),  # 北交所统一 920xxx 号段
    "83": (AssetType.STOCK, "BJ"),
    "87": (AssetType.STOCK, "BJ"),
    "43": (AssetType.STOCK, "BJ"),  # V2 老三板平移代码
}

#: 前缀 -> 资产类型映射（由 PREFIX_RULES 派生，勿手改）。
ASSET_PREFIXES: dict[str, AssetType] = {
    prefix: rule[0] for prefix, rule in PREFIX_RULES.items()
}

#: 前缀 -> 唯一合法交易所代码（由 PREFIX_RULES 派生，用于拦截如 ``600000.SZ`` 的跨所错配）。
PREFIX_EXCHANGE: dict[str, str] = {
    prefix: rule[1] for prefix, rule in PREFIX_RULES.items()
}

#: 期货品种代码 -> 唯一上市交易所。
#: 用于矛盾检测而非白名单准入：未知品种搭配合法交易所仍可通过（避免新上市合约被误拒），
#: 已知品种若交易所不匹配（如 ``IF2406.SHFE``）则拒绝。
FUTURES_EXCHANGE: dict[str, str] = {
    # 中金所：股指与国债期货
    "IF": "CFFEX",
    "IH": "CFFEX",
    "IC": "CFFEX",
    "IM": "CFFEX",
    "T": "CFFEX",
    "TF": "CFFEX",
    "TS": "CFFEX",
    "TL": "CFFEX",
    # 上期所：金属与化工
    "CU": "SHFE",
    "AL": "SHFE",
    "ZN": "SHFE",
    "PB": "SHFE",
    "NI": "SHFE",
    "SN": "SHFE",
    "AU": "SHFE",
    "AG": "SHFE",
    "RB": "SHFE",
    "HC": "SHFE",
    "SS": "SHFE",
    "BU": "SHFE",
    "RU": "SHFE",
    "FU": "SHFE",
    "SP": "SHFE",
    "BR": "SHFE",
    "AO": "SHFE",
    # 大商所：农产品与工业品
    "M": "DCE",
    "Y": "DCE",
    "P": "DCE",
    "C": "DCE",
    "CS": "DCE",
    "A": "DCE",
    "B": "DCE",
    "JD": "DCE",
    "L": "DCE",
    "V": "DCE",
    "PP": "DCE",
    "J": "DCE",
    "JM": "DCE",
    "I": "DCE",
    "EG": "DCE",
    "EB": "DCE",
    "PG": "DCE",
    "LH": "DCE",
    "RR": "DCE",
    "BB": "DCE",
    "FB": "DCE",
    "LG": "DCE",
    # 郑商所：农产品与化工
    "SR": "CZCE",
    "CF": "CZCE",
    "ZC": "CZCE",
    "TA": "CZCE",
    "MA": "CZCE",
    "OI": "CZCE",
    "RM": "CZCE",
    "FG": "CZCE",
    "SA": "CZCE",
    "UR": "CZCE",
    "SF": "CZCE",
    "SM": "CZCE",
    "AP": "CZCE",
    "CJ": "CZCE",
    "PK": "CZCE",
    "PF": "CZCE",
    "SH": "CZCE",
    "PX": "CZCE",
    "CY": "CZCE",
    "WH": "CZCE",
    "RI": "CZCE",
    "RS": "CZCE",
    "JR": "CZCE",
    "LR": "CZCE",
    "PM": "CZCE",
    # 上期能源：能源与国际化品种
    "SC": "INE",
    "NR": "INE",
    "LU": "INE",
    "BC": "INE",
    "EC": "INE",
    # 广期所：新能源与新材料
    "SI": "GFEX",
    "LC": "GFEX",
    "PS": "GFEX",
}

# --- STAMP_TAX_EXEMPT_PREFIXES：由规则派生的免印花税前缀集合 ---
# 依据 ASSET_PREFIXES 与 ASSET_RULES 自动推导，``execution/fee_model.py`` 应直接引用而非复制。
STAMP_TAX_EXEMPT_PREFIXES: frozenset[str] = frozenset(
    p for p, at in ASSET_PREFIXES.items() if not ASSET_RULES[at.value]["stamp_tax"]
)

# --- EXCHANGE_MAP：交易所代码 -> 日历/存储层交易所名称 ---
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

# 日历采集器据此集合生成交易所列表（实际采集范围由运行配置决定）
CALENDAR_EXCHANGES: set[str] = set(EXCHANGE_MAP.values())


# --- 代码格式校验器 ---
# A 股场内权益/ETF/LOF：{6位数字}.{SH|SZ|BJ}，且两位前缀须在 ASSET_PREFIXES 中
_equity_re = re.compile(r"^(\d{6})\.(SH|SZ|BJ)\Z")

# 场外公募基金：{6位数字}.OF
_otc_fund_re = re.compile(r"^(\d{6})\.OF\Z")

# A 股指数及行业/概念板块（SSE/SZSE/BSE/CSI/CNI/SW/CI/TI/DC/TS）
_index_re = re.compile(r"^[A-Za-z0-9_]+\.(SH|SZ|BJ|CSI|CNI|SI|CI|TI|DC|TS)\Z")

# 期货合约：品种字母 + 4位年月，如 IF2406、T2406、SC2412
_futures_re = re.compile(r"^([A-Z]{1,2})\d{4}\.(CFFEX|SHFE|DCE|CZCE|INE|GFEX)\Z")


def validate_equity_ticker(ticker: str) -> bool:
    """校验场内 A 股股票/ETF/LOF 代码（``{6位数字}.{SH|SZ|BJ}``），且两位前缀须匹配对应交易所。"""
    m = _equity_re.match(ticker)
    if m is None:
        return False
    code, exchange = m.groups()
    prefix = code[:2]
    return PREFIX_EXCHANGE.get(prefix) == exchange


def validate_stock_ticker(ticker: str) -> bool:
    """校验 A 股股票代码（不含 ETF/LOF 前缀）。"""
    if not validate_equity_ticker(ticker):
        return False
    return ASSET_PREFIXES[ticker[:2]] is AssetType.STOCK


def validate_etf_ticker(ticker: str) -> bool:
    """校验场内 ETF 代码（51/52/53/55/56/58.SH、15.SZ）。"""
    if not validate_equity_ticker(ticker):
        return False
    return ASSET_PREFIXES[ticker[:2]] is AssetType.ETF


def validate_otc_fund_ticker(ticker: str) -> bool:
    """校验场外公募基金代码（``{6位数字}.OF``）。"""
    return _otc_fund_re.match(ticker) is not None


def validate_fund_ticker(ticker: str) -> bool:
    """校验公募基金代码（场外 ``.OF`` 或场内 ``50/16`` LOF/封闭式基金）。"""
    if validate_otc_fund_ticker(ticker):
        return True
    if not validate_equity_ticker(ticker):
        return False
    return ASSET_PREFIXES[ticker[:2]] is AssetType.FUND


def validate_index_ticker(ticker: str) -> bool:
    """校验 A 股指数、行业或概念板块代码。"""
    return _index_re.match(ticker) is not None


def validate_futures_ticker(ticker: str) -> bool:
    """校验期货合约代码（``{品种字母}{4位年月}.{交易所}``，如 ``IF2406.CFFEX``）。

    期货品种代码不在 :data:`ASSET_PREFIXES` 中。
    """
    m = _futures_re.match(ticker)
    if m is None:
        return False
    product, exchange = m.groups()
    known = FUTURES_EXCHANGE.get(product)
    # 矛盾检测而非准入白名单：已知品种须匹配其交易所（如 IF2406.SHFE 失败），
    # 未知品种搭配合法交易所默认放行，避免新上市品种写入失败。
    return known is None or known == exchange
