# cross_sectional_backtest

> 域：research · 对应代码 [research/cross_sectional_backtest.py](../../../research/cross_sectional_backtest.py)

## 职责与边界

截面策略回测主链（按调仓日遍历时间模型）：策略只负责在调仓日输出目标权重 `{ticker: weight}`，订单生成统一委托 [order_generation.md](../execution/order_generation.md)（`generate_orders`），撮合、风控、费用、滑点与 `Journal` 记账统一交由 `broker.submit_orders()` 完成，确保研究回测与实盘共享同一执行路径。

- 负责：`BaseCrossStrategy`（`on_rebalance` / `universe` / `rebalance_freq`）接口定义、`CrossSectionalBacktest.run()` 调仓日历驱动、盘前市值重估（mark-to-market）、T+1 撮合时钟推进。
- 不负责：策略信号生成本身（[alpha_signal.md](signal/alpha_signal.md) / [ml/signal.md](ml/signal.md)）、组合约束求解（[portfolio_construction.md](../execution/portfolio_construction.md)）、订单拆分与风控撮合（[order_generation.md](../execution/order_generation.md) / [broker.md](../execution/broker.md)）、账本与 `Journal` 格式（[ledger.md](../execution/ledger.md)）、费用与滑点参数（[fee_model.md](../execution/fee_model.md)）。

## 接口签名

```python
REBALANCE_FREQS: tuple[str, ...] = ("daily", "weekly", "monthly")

class BaseCrossStrategy(ABC):
    @abstractmethod
    def on_rebalance(
        self,
        date: date,
        bars: pd.DataFrame,
        positions: dict,
        account,
    ) -> dict[str, float]:
        """在指定调仓日输出目标权重 {ticker: weight}。"""

    @property
    def universe(self) -> list[str]: ...

    @property
    def rebalance_freq(self) -> str:
        return "daily"

class CrossSectionalBacktest:
    def __init__(self, broker): ...
    def run(
        self,
        strategy: BaseCrossStrategy,
        bars: pd.DataFrame,
        dates: list[date],
    ) -> list[dict]:
        """按 strategy.rebalance_freq 推进调仓日，返回每日 {"date", "weights", "orders", "fills"} 记录。"""
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 自研轻量调仓主链 + 统一 `execution/` 执行内核 | backtrader / vnpy 事件驱动塞入截面 | 截面是「全市场目标权重 → 差额调仓」，多 feed 事件框架内存高且无原生差额调仓支持 |
| 回测直接复用 `generate_orders` + `broker.submit_orders` | 在回测模块重复写撮合与记账 | 保证回测与实盘共享完全一致的停牌/涨跌停、手数、费用、滑点与 `Journal` 逻辑 |
| VectorBT 留作未来可选加速备选，不作主链 | VectorBT 主力回测 | VectorBT 缺乏 A 股整数手数、逐笔涨跌停拒单与统一 `Journal` 原生对接 |
| 数据加载与信号计算向量化，交易执行逐笔模拟 | 全向量化近似 / 全逐笔驱动 | 兼顾研究计算速度与执行约束真实性 |
| T 日收盘信号计算，推进至 T+1 撮合（`_advance_fill_clock`） | T 日收盘价即时成交 | 严格避免日频回测中的未来函数 |
| 周频 / 月频调仓默认取周期内首个交易日 | 跨周期任意日漂移 | 规则确定且可复现，与组合再平衡节奏对齐 |

## 实现要点

详见 [research/cross_sectional_backtest.py](../../../research/cross_sectional_backtest.py)。

核心约定：
- 主循环按 `_select_rebalance_dates(dates, freq)` 选出调仓日，每个调仓日依次执行：`_mark_to_market`（按当日收盘价重估净值）→ `strategy.on_rebalance` 输出权重 → `execution.order_generation.generate_orders` 生成目标订单 → `_advance_fill_clock` 推进至下一交易日（T+1）→ `broker.submit_orders(orders)` 完成风控、撮合与账本更新。
- 内部 `_BrokerAccount` 将 `broker.get_account()` 与 `broker.get_positions()` 适配为 `generate_orders` 与策略所需的统一账户/持仓视图。
- 截面策略（矩阵计算目标权重）与时序策略（逐 bar 单票信号）最终写入同一 `Journal` 结构，使下游绩效归因与净值对拍接口完全统一。
