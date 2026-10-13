# cross_sectional_backtest

> 域：research · 对应代码 research/cross_sectional_backtest.py · 来源 §26（archive/OPEN_QUESTIONS.md）

## 职责与边界

截面策略回测路径（A+C 混合）：自研截面组合模拟器 `cs_portfolio_sim`（约 300–400 行）+ 截面回测引擎。截面策略每天收盘 → 全市场一起算 → 输出目标权重矩阵，是纯矩阵运算范式，与逐 bar 单票的时序范式不同——硬塞进事件驱动框架等于用 Excel 跑矩阵运算。

- 负责：`BaseCrossStrategy.on_rebalance` 接口、rebalance 差额调仓模拟（先卖后买、逐笔约束）、`simulate_single_trade`、向量化外壳 + 逐笔内核。
- 不负责：策略信号本身（[alpha_signal.md](alpha_signal.md) / [ml_signal.md](ml_signal.md)）、账本 dataclass 与 Journal 格式（[execution/ledger.md](execution/ledger.md)）、费用与滑点参数（[execution/fee_model.md](execution/fee_model.md)）、实盘柜台（[execution/broker.md](execution/broker.md)）。
- 模拟器是薄壳的一部分：直接用账本四件套 + 预检/佣金/手数/持仓更新公共逻辑 + 统一 Journal，不是独立引擎。

> 回改（2026-10-10）：本节的 backtrader / vnpy 相关结论已被 §66.2/§68 修正——**backtrader/vnpy 不用**（pandas 自研 <300 行 + SimBroker/QmtBroker）；alphalens-reloaded 与自研路径不受影响；VectorBT 留 V2 备选。以 §66/§68 为准。

## 接口签名

```python
class BaseCrossStrategy:
    def on_rebalance(self, date, bars, positions, account) -> dict[str, float]:
        """每个调仓日调一次，输出目标权重 {ticker: weight}。"""
        raise NotImplementedError
    @property
    def universe(self) -> list[str]: ...
    @property
    def rebalance_freq(self) -> str:      # "daily" | "weekly" | "monthly"
        return "daily"
    # 注：§32 升级返回类型为 CrossSignal、§34 再改 rebalance(target_portfolio)
    #     ——以 §34/§68 三步主链为准（目标权重交给 Portfolio 层求解）

class CrossSectionalBacktest:
    def run(self, strategy, bars, dates) -> BacktestResult:
        """向量化外壳：批量加载数据 + 逐调仓日内核。"""

def rebalance(target_weights, account_book, bars, account) -> list[Trade]:
    """一次截面调仓的完整模拟：差额 → 先卖后买 → 逐笔模拟成交。"""

def simulate_single_trade(ticker, direction, qty, bar, account, book) -> (Order, Trade):
    """预检停牌/涨跌停 → 资金/手数 floor_to_lot → 成交量约束 → 滑点成交价 → Order+Trade → 更新持仓账户 → 写 Journal。"""
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| A+C 混合：自研截面组合模拟器 + 统一 Journal | 全能回测框架 | 两条范式不同；"对比→差额→批量下单"框架里没有原生支持，写完本质是自己的组合管理器，框架只剩撮合器 |
| backtrader 不用（§66.2/§68 回改） | 多标的 Cerebro 截面 / 时序事件驱动 | 300 feed 内存×300、50+ feed 后内存管理急剧恶化、notify_order/notify_trade 回调风暴、差额批量下单无原生支持 |
| VectorBT 留 V2 备选，不当主力 | VectorBT 主力截面回测 | 无整数手数取整、无停牌/涨跌停逐笔拒单、不支持部分成交、对接 Journal 需额外适配 |
| 不做部分成交 | 部分成交状态机 | 日频截面假设当天能成交（不能的已被预检拦）；分钟频截面再加 |
| 不做限价单/止损单/事件回调 | 完整订单状态机 | 截面是"调到目标权重"，本质是市价单（次日开盘/VWAP 成交）；一个 for 循环走完一天调仓 |
| 先卖后买 | 按比例并行 | 卖出释放资金，否则买单受现金约束被拒 |
| 数据加载 + 信号计算向量化，成交模拟逐笔 | 全向量化 / 全逐笔 | **快在数据+信号，严在执行** |
| 成交量约束按 §66 `check_split_needed`（>1% 日成交量） | 固定 10% 示例值 | §26 旧示例 10% 已定 → §66 |

## 实现要点

```python
def rebalance(target_weights, account_book, bars, account):
    # [1] 目标 vs 当前 → 差额
    current = account_book.get_positions()
    target_shares = {
        t: floor_to_lot(w * account.total_asset / bars[t].close, lot_size=100)
        for t, w in target_weights.items()
    }
    diff = {t: target_shares.get(t, 0) - current.get(t, Position()).quantity
            for t in set(target_shares) | set(current)}          # 正=买 负=卖
    # [2] 先卖后买（释放资金）
    sells = {t: q for t, q in diff.items() if q < 0}
    buys  = {t: q for t, q in diff.items() if q > 0}
    # [3] 逐笔约束检查 + 模拟成交
    trades = []
    for t, q in sells.items():
        o, tr = simulate_single_trade(t, Direction.SELL, abs(q), bars[t], account, account_book)
        if tr: trades.append(tr)
    for t, q in buys.items():
        o, tr = simulate_single_trade(t, Direction.BUY, q, bars[t], account, account_book)
        if tr: trades.append(tr)
    return trades

# simulate_single_trade 内部顺序（复用公共逻辑）：
# pre_check_bar(停牌/涨跌停拒单) → 资金约束 min(qty, floor_to_lot(affordable))
#   → 成交量约束 min(qty, floor_to_lot(max_vol)) → 滑点成交价
#   → Order(status=FILLED) + Trade → 更新持仓/账户 → 写 Journal（与实盘同格式）

# 回测主循环（for-loop 时间推进，§68.2）：
# for date in rebalance_dates(strategy.rebalance_freq):
#     weights = strategy.on_rebalance(date, bars, positions, account)
#     trades = rebalance(weights, book, bars[date], account)
```

截面 vs 时序范式对照（§26.1）：截面 = 每天全市场一起算 → 权重矩阵 `[ticker: weight]`，纯矩阵运算天然向量化；时序 = 每根 bar 逐票判断 → 单票信号，if/else 状态机天然事件驱动。两类策略输出到同一 Journal，下游分析/对拍/净值计算统一。

## 实施时定稿项

- ~~成交量约束 10% 示例值~~ 已定 → §66 `check_split_needed` >1% 日成交量（2026-10-10 回改）
- 分钟频截面的部分成交模拟（日频不做）
- rebalance_freq = weekly/monthly 时调仓日的具体判定规则（哪天算月末/周几调仓）
