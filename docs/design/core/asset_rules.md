# asset_rules

> 域：core · 对应代码 core/asset_rules.py · 来源 §69.5（archive/OPEN_QUESTIONS.md）

## 职责与边界

多资产交易规则的**唯一查表入口**：T+1、手数、印花税、涨跌停幅度、ST、做空等 A 股差异规则集中一处，执行层参数从这里取而不是散落硬编码。

边界：规则的**执行**在各层——T+1 可卖判定在 ../execution/order_generation.md、涨跌停/做空检查在 ../execution/risk_control.md、印花税/手数在 ../execution/fee_model.md、ST/停牌过滤在 ../research/factor_interface.md、板块编码在 ../data/ingestion.md；asset_rules 只提供规则值。

## 接口签名

```python
ASSET_RULES: dict[str, dict]

def get_asset_rules(asset_type: str) -> dict: ...
```

差异落层表（§69.5）：

| 差异 | 处理层 | 落点 |
|---|---|---|
| T+1 可卖 | execution | generate_orders 当日可卖判定（§66.1） |
| 涨跌停 / 禁做空 | execution | pre_trade_risk_check（§66.3 风控七规则） |
| 印花税 / 手数 / 佣金 | execution | FeeModel / compute_order_diff（§66.1/§66.4） |
| ST / 停牌 | research | universe 过滤（§57） |
| 板块涨跌停差异 | data | ticker 编码前缀（60/00/30/68/8） |

分阶段支持：

| 阶段 | 资产 | 备注 |
|---|---|---|
| V1 | 股票 + ETF | 主战场；FeeModel._is_etf 已处理印花税差异（§66.4） |
| V2 | 可转债 | T+0、无印花税——规则表加一行 |
| V3 | 期货 | 保证金 + 逐日盯市——新账本（不复用 SimAccount） |
| V4+ | 期权 | 教程 §11.2 不推荐散户，最后做 |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 配置区分 + 规则表 + execution 层处理差异 | C++ Traits 编译期多态（DataTraits<Category,Market,Exchange>） | 月频一次调仓 dispatch 开销可忽略，编译期多态是 tick 级性能需求 |
| A 股差异几乎全落 execution/ + data/ | 在 research/ 做资产分支 | research 因子计算对资产透明——因子只看 DataFrame |
| 期货/期权不复用 SimAccount | V1 就预抽象保证金账本 | 保证金、逐日盯市是另一套账本，排 V3/V4 而不是预抽象 |
| `get_asset_rules()` 未知类型 KeyError | 静默回落默认规则 | 不静默回落——错误配置必须立刻炸 |
| 与 FeeModel._is_etf 同源一致 | 两处各自判断 | 前者按代码前缀自动判（51/15/16 免印花税），后者按资产类型显式配，必须对得上 |

## 实现要点

```python
ASSET_RULES = {
    "stock": {
        "t_plus": 1,
        "lot_size": 100,
        "stamp_tax": True,                      # 卖出征收
        "price_limits": {                       # 涨跌停幅度（教程 §15）
            "main": 0.10,                       # 主板 60/00
            "gem": 0.20,                        # 创业板 30
            "star": 0.20,                       # 科创板 68
            "bse": 0.30,                        # 北交所 8
        },
        "st_limit": 0.05,                       # ST ±5%
        "short": False,                         # 禁做空
    },
    "etf": {
        "t_plus": 1,
        "lot_size": 100,
        "stamp_tax": False,                     # ETF 免印花税
        "price_limits": {"main": 0.10},
        "st_limit": None,
        "short": False,
    },
    # "bond": V2 预留    "futures": V3 预留    "option": V4 预留
}


def get_asset_rules(asset_type: str) -> dict:
    """查表入口。未知类型直接 KeyError——不静默回落。"""
    return ASSET_RULES[asset_type]
```

一致性校验点（§69 验证 4 条之一）：`get_asset_rules("etf")["stamp_tax"] == False` 必须与 `FeeModel._is_etf("510300")` 判定一致；stock 前缀 60/00/30/68/8 的 price_limits 映射正确。

## 实施时定稿项

- V2 可转债规则表预填（T+0 / 无印花税 / 20% 涨跌幅?，§69.6）
- ~~涨跌停/做空规则参数来源~~ 已定 → §66.3 参数来源 = ASSET_RULES（不再散落硬编码）
