# order_generation

> 域：execution · 对应代码 execution/order_generation.py · 来源 §66.2（archive/OPEN_QUESTIONS.md）

## 职责与边界

目标权重 vs 当前持仓 → 差分 → 订单列表（Utrade 等价物的个人简化版）。输入 `TargetPortfolio`/目标权重与账户状态，输出 `list[dict]` 订单；**不撮合、不收费用、不做风控**（那是 broker.submit_orders() 内部三步）。挂单处理、拆单执行 V1 都不归本模块。

## 接口签名

```python
def compute_order_diff(
    target_weights: pd.DataFrame,   # [ticker, weight]
    account: "SimAccount",
    nav: float,
) -> list[dict]:
    """目标权重 → 订单差分。
    target_shares = floor(weight × nav / price / lot_size) × lot_size（§17.3/§34.4 手数取整）。
    无有效价格（停牌/≤0）直接跳过；diff==0 不产单。"""

def generate_orders(
    target_weights: pd.DataFrame,
    account: "SimAccount",
    nav: float,
) -> list[dict]:
    """全链路: diff → check_split_needed 告警 → 排序(先卖后买)。"""

def check_split_needed(order: dict) -> None:
    """单笔 > 1% 日成交量 → 告警（V1 不拆单）。"""
```

订单 dict 字段：`ticker / side("BUY"|"SELL") / shares / limit_price`。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| pandas 自研 <300 行 | backtrader / vnpy | 教程 §25.2 能用 pandas 别上框架，框架学习成本 > 收益 |
| 手数取整 floor(w×nav/price/100)×100 | 向上取整 | 向下取整保证不超预算 |
| 先卖后买；卖出按市值 desc；买入按信号 desc | 随机/按 ticker 序 | 释放资金再买入（教程 §17） |
| 拆单 V1 不做，check_split_needed 仅告警 | TWAP/VWAP 拆单 | 教程 §17.5：>1% 日成交量才拆——月频 ETF/大盘股远不到 |
| 挂单 V1 不处理（当日全成或不成） | 跨日挂单管理 | 月频挂单跨日是高频需求 |
| 订单类型 LIMIT，limit_price = close | 市价单 | 简单诚实；市价单 V2 |

## 实现要点

```python
def generate_orders(target_weights, account, nav):
    orders = compute_order_diff(target_weights, account, nav)
    for o in orders:
        check_split_needed(o)                    # >1% 日成交量 → 告警（不拆）
    sells = sorted((o for o in orders if o["side"] == "SELL"),
                   key=lambda o: -o["shares"] * o["limit_price"])
    buys = sorted((o for o in orders if o["side"] == "BUY"),
                  key=lambda o: -abs(get_signal_score(o["ticker"])))
    return sells + buys
```

`compute_order_diff` 骨架：逐行取最新价 → `target_shares = int(w * nav / price // 100) * 100` → 与 `account.holdings` 求差 → 非零产单。

## 实施时定稿项

- ~~`get_latest_price` / `get_prices` 的数据接口签名（从 bars 读当日 close？）（§66）~~ **已定稿** → `execution/market_data.py`（QUANT-27 Review 确认）：`get_latest_price(bars, ticker, asof, field="close"|"open") -> float | None`；`get_prices(bars, tickers, asof, field="close") -> dict[str, float]`。语义：严格 trade-date 等值（停牌返回 `None` 不回退）、raw 未复权、NaN/非正返回 `None`、`field` 仅 `close`/`open`、重复 `(ticker, date)` 行 raise。
- ~~`check_split_needed` 的成交量数据来源（当日成交量从 bars 取）（§66）~~ **已定稿** → `execution/market_data.py`：`get_daily_volume(bars, ticker, asof) -> float | None`，当日成交**股数**（`bars_daily.volume`），停牌/NaN/负值返回 `None`（零成交量返回 `0.0`）。
- 港股/期货 lot_size 主数据从哪来（§17.3）
