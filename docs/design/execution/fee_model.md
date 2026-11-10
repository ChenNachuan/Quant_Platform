# fee_model

> 域：execution · 对应代码 [execution/fee_model.py](execution/fee_model.py)

## 职责与边界

交易成本建模：费用四件套（佣金/印花税/过户费/滑点）计算 + 成交成本假设（滑点模型、成交价基准）。只管「一笔成交花多少成本」；撮合时机与成交价的确定在 broker（[broker.md](broker.md)），账本记账在 ledger（[ledger.md](ledger.md)）。

## 接口签名

```python
class FeeModel:
    """A 股费用四件套。ETF 免印花税，与 ASSET_RULES 一致。"""
    COMMISSION_RATE = 0.00025      # 佣金万 2.5，最低 5 元
    COMMISSION_MIN = 5.0
    STAMP_TAX_RATE = 0.0005        # 印花税万 5，仅卖出
    TRANSFER_FEE_RATE = 0.00001    # 过户费万 0.1，双边
    SLIPPAGE_RATE = 0.001          # 滑点 0.1%，方向不利（买+卖−）

    @staticmethod
    def _is_etf(ticker: str) -> bool:
        # 与 ASSET_RULES 同源：51/56/58（沪）+ 15/16（深）
        return ticker[:2] in ("51", "56", "58", "15", "16")

    def compute(self, side: str, price: float, shares: int, ticker: str) -> float:
        """返回本笔总费用 = 佣金 + 印花税 + 过户费 + 滑点。"""
```

**费用表**（A 股）：

| 费用项 | 费率 | 方向 | 备注 |
|---|---|---|---|
| 佣金 | 万 2.5 | 双边 | 最低 5 元 |
| 印花税 | 万 5 | **卖出** | ETF 免（51/56/58/15/16 前缀，与 ASSET_RULES 同源 |
| 过户费 | 万 0.1 | 双边 | |
| 滑点 | 0.1% | 方向不利 | 买+卖− |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 固定比例滑点 0.1% | 固定跳数/平方根定律/盘口撮合 | 小资金单量冲击可忽略 |
| 成交价收盘价；保守版 = T+1 开盘 ± 滑点 | 只取其一当唯一口径 | 双口径——保守版防前视 |
| 滑点方向永远对己不利 | 符号随意 | 搞反符号滑点变收益，回测虚高 |
| 费用按买卖不对称建模（印花税卖出单边） | 对称简化 | ETF/股票成本差全在这里 |
| 最低 5 元佣金必须建模 | 忽略 | 小资金杀手 |
| 期货手续费 V3 预留 | V1 预填 | 另一套费率体系 |

## 实现要点

详见[execution/fee_model.py](execution/fee_model.py)

核心约定:
- compute返回总费用=佣金 +印花税+过户费+滑点
- _is_etf 与 core/asset_rules.py 的 STAMP_TAX_EXEMPT_PREFIXES 同源
- 成本量级:ETF单边~0.15-0.2%,股票单边~0.35-0.4%(印花税决定差异）
- 月频换手 30-100%时年成本约2-5%--alpha必须覆盖
- 真实回测六道关(顺序执行):停牌拒→涨跌停封板拒 成交量约束→本金/手数floor→成交价→扣费 更新持仓现金
