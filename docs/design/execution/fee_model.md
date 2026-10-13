# fee_model

> 域：execution · 对应代码 execution/fee_model.py · 来源 §18 / §66.4（archive/OPEN_QUESTIONS.md）

## 职责与边界

交易成本建模：费用四件套（佣金/印花税/过户费/滑点）计算 + 成交成本假设（滑点模型、成交价基准）。只管「一笔成交花多少成本」；撮合时机与成交价的确定在 broker（[broker.md](broker.md)），账本记账在 ledger（[ledger.md](ledger.md)）。§18.3「真实回测六道关」中费用是最后一关，前五关（停牌/涨跌停/流动性/手数/成交价）由 broker + order_generation + risk_control 分担。

## 接口签名

```python
class FeeModel:
    """A 股费用四件套（教程 §3.2）。ETF 免印花税，与 ASSET_RULES 一致（§69.5）。"""
    COMMISSION_RATE = 0.00025      # 佣金万 2.5，最低 5 元
    COMMISSION_MIN = 5.0
    STAMP_TAX_RATE = 0.0005        # 印花税万 5，仅卖出
    TRANSFER_FEE_RATE = 0.00001    # 过户费万 0.1，双边
    SLIPPAGE_RATE = 0.001          # 滑点 0.1%，方向不利（买+卖−）

    @staticmethod
    def _is_etf(ticker: str) -> bool:
        # 与 ASSET_RULES 同源（§69.5）：51/56/58（沪）+ 15/16（深）
        return ticker[:2] in ("51", "56", "58", "15", "16")

    def compute(self, side: str, price: float, shares: int, ticker: str) -> float:
        """返回本笔总费用 = 佣金 + 印花税 + 过户费 + 滑点。"""
```

**费用表**（教程 §3.2，A 股）：

| 费用项 | 费率 | 方向 | 备注 |
|---|---|---|---|
| 佣金 | 万 2.5 | 双边 | 最低 5 元 |
| 印花税 | 万 5 | **卖出** | ETF 免（51/56/58/15/16 前缀，与 ASSET_RULES 同源 §69.5） |
| 过户费 | 万 0.1 | 双边 | |
| 滑点 | 0.1% | 方向不利 | 买+卖− |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 固定比例滑点 0.1% | 固定跳数/平方根定律/盘口撮合 | 小资金单量占成交量微乎其微，冲击可忽略；盘口撮合是 tick 级需求 |
| 成交价：V1 = 决策日收盘价（简化）；保守版 = T+1 开盘 ± 滑点 | 只取其一当唯一口径 | §66.4 双口径——T 收盘信号用 T 收盘价成交通常虚高但月频影响小，黄金样本对拍用手算口径一致；保守版防前视（§18.2） |
| 滑点方向永远对己不利 | 符号随意 | 搞反符号滑点变收益，回测虚高 |
| 费用按买卖不对称建模（印花税卖出单边） | 对称简化 | ETF/股票成本差全在这里 |
| 最低 5 元佣金必须建模 | 忽略 | 小资金杀手 |
| 期货手续费 V3 预留 | V1 预填 | 另一套费率体系 |

## 实现要点

```python
def compute(self, side, price, shares, ticker):
    value = price * shares
    commission = max(value * self.COMMISSION_RATE, self.COMMISSION_MIN)
    stamp = value * self.STAMP_TAX_RATE if (side == "SELL" and not self._is_etf(ticker)) else 0.0
    transfer = value * self.TRANSFER_FEE_RATE
    slippage = value * self.SLIPPAGE_RATE
    return commission + stamp + transfer + slippage
```

**成本量级**：ETF 单边 0.15–0.2%，股票单边 0.35–0.4%（印花税卖出单边决定差异）。月频换手 30–100% 时年成本约 2–5%——alpha 必须覆盖。

**成交成本假设要点**（§18.1/18.2）：V1 滑点固定 0.1% 一刀切（§66.4）；流动性分档是原则提醒（§18.1），不是 V1 实现；成交价基准本身就是成本假设。**真实回测六道关**（顺序执行，漏一步回测偏乐观）：①停牌拒 → ②涨跌停封板拒 → ③成交量约束部分成交 → ④本金/手数 floor → ⑤成交价（V1=决策日收盘价；保守版=T+1 开盘±滑点）→ ⑥扣费（含最低 5 元）→ 更新持仓现金。

## 实施时定稿项

- 无——费率表与流动性阈值已定 → §66.4 四件套 / §66（check_split_needed >1% 日成交量）（2026-10-10 回改）
- 期货手续费具体表（V3 预留）
