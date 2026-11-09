# asset_rules

> 域：core · 对应代码 core/asset_rules.py

## 职责与边界

多资产交易规则的**唯一查表入口**：T+1、手数、印花税、涨跌停幅度、ST、做空等 A 股差异规则集中一处，执行层参数从这里取而不是散落硬编码。

边界：规则的**执行**在各层——T+1 可卖判定在 ../execution/order_generation.md、涨跌停/做空检查在 ../execution/risk_control.md、印花税/手数在 ../execution/fee_model.md、ST/停牌过滤在 ../research/factor_interface.md、板块编码在 ../data/ingestion.md；asset_rules 只提供规则值。

## 接口签名

```python
ASSET_RULES: dict[str, dict]

def get_asset_rules(asset_type: str) -> dict: ...

def is_stamp_tax_exempt_prefix(ticker: str) -> bool: ...
```

**入参契约**（`is_stamp_tax_exempt_prefix`）：canonical 入参是完整 ticker `CODE.EXCH`。函数只读 `[:2]`，因此 6 位码与 2 字符前缀同解；调用方（如 `FeeModel`）一律传完整 ticker。

差异落层表：

| 差异 | 处理层 | 落点 |
|---|---|---|
| T+1 可卖 | execution | generate_orders 当日可卖判定 |
| 涨跌停 / 禁做空 | execution | pre_trade_risk_check |
| 印花税 / 手数 / 佣金 | execution | FeeModel / compute_order_diff |
| ST / 停牌 | research | universe 过滤 |
| 板块涨跌停差异 | data | ticker 编码前缀（60/00/30/68/8） |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 配置区分 + 规则表 + execution 层处理差异 | C++ Traits 编译期多态（DataTraits<Category,Market,Exchange>） | 月频一次调仓 dispatch 开销可忽略，编译期多态是 tick 级性能需求 |
| A 股差异几乎全落 execution/ + data/ | 在 research/ 做资产分支 | research 因子计算对资产透明——因子只看 DataFrame |
| 期货/期权不复用 SimAccount | V1 就预抽象保证金账本 | 保证金、逐日盯市是另一套账本，排 V3/V4 而不是预抽象 |
| `get_asset_rules()` 未知类型 KeyError | 静默回落默认规则 | 不静默回落——错误配置必须立刻炸 |
| 与 FeeModel._is_etf 同源一致 | 两处各自判断 | **`STAMP_TAX_EXEMPT_PREFIXES`**是唯一来源，FeeModel从此派生 |

## 实现要点

详见core/asset_rules.py

本表以 `core/asset_rules.py` 的 `ASSET_RULES` 为**单源**；LOF 落 `FUND`（前缀 `50`/`16`），不落 `stock` 也不落 `etf`。

一致性校验点：`get_asset_rules("etf")["stamp_tax"] == False` 必须与 `FeeModel._is_etf("510300")` 判定一致；stock 前缀 60/00/30/68/8 的 price_limits 映射正确；`is_stamp_tax_exempt_prefix("501050.SH") is True`（完整 ticker 入参契约）。
