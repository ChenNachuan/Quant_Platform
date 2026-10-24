# broker

> 域：execution · 对应代码 execution/broker.py · 来源 §22 / §66.4 / §66.5（archive/OPEN_QUESTIONS.md）

## 职责与边界

柜台抽象与撮合：`Broker` ABC 四方法封顶，`SimBroker`（回测/paper 共用）与 `QmtBroker`（V2 实盘）经工厂切换——**回测与实盘同一代码路径**，差异只注入在 Broker 实现。`submit_orders()` 内部三步 = 风控 + 撮合 + 记账（Utrade/Themis/TB 的坍缩等价物，§68.5：不是三个模块）。不做撤单/改单/行情/事件回调。

## 接口签名

```python
class Broker(ABC):
    """柜台抽象。4 方法封顶。"""
    @abstractmethod
    def submit_orders(self, orders: list[dict]) -> list[dict]: ...
    @abstractmethod
    def get_holdings(self) -> dict[str, int]: ...
    @abstractmethod
    def get_cash(self) -> float: ...
    @abstractmethod
    def get_nav(self) -> float: ...

class SimBroker(Broker):
    """模拟柜台：封装 风控+撮合+费用+记账。"""
    def __init__(self, config: dict):
        self.account = SimAccount(config["initial_cash"])
        self.fee_model = FeeModel()

@dataclass
class SimAccount:
    cash: float
    holdings: dict[str, int]           # ticker → 股数
    cost_basis: dict[str, float]       # ticker → 均价（含费摊入）
    trade_log: list[dict]
    def execute_fill(self, order, fill_price, fees): ...
    def get_nav(self, prices) -> float: ...
    def get_snapshot(self, prices) -> dict: ...

def create_broker(mode: str, config: dict) -> Broker:
    """工厂: sim / qmt 切换。mode 读 config[execution].mode（§69.2）。"""
```

**撮合模型 V1**（§66.4）：收盘价成交；无价格 → REJECTED（停牌）；部分成交不模拟（全成或不成）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| SimBroker/QmtBroker 工厂，同一代码路径 | 回测/实盘两套 | 回测-实盘分歧从根上消除（§22.2 薄壳思想的落点） |
| submit_orders 内部三步（风控+撮合+记账） | Utrade/Themis/TB 三个独立模块 | 接口边界从「产品」移到「函数」（§68.5） |
| V1 收盘价撮合 | T+1 开盘撮合 | V1 简化（黄金样本对拍用手算口径一致）；保守版用 T+1 开盘（§18.2） |
| Broker 只 4 方法 | 含 cancel/modify/行情/回调 | 月频够用，V2 再扩 |
| V2 实盘路径 QMT > easytrader > 券商 API | vnpy Gateway | QMT Python API 完善支持 mini-qmt；easytrader 逆向不稳；引整个 vnpy 生态不值（修正 §22.5/§24.4 早期结论，见 §68） |
| 对拍（B）+ 模拟盘校准期（C）保留为上线流程 | 只靠回测 | fill 层偏差是物理性的，需量化校准；新策略模拟盘 2–4 周过对拍才上真钱 |
| 实盘执行层自研 D 不做 | 一个人维护订单状态机/断线恢复 | 公司几十人团队维护执行层；有同代码路径后执行层对策略透明 |

## 实现要点

```python
class SimBroker(Broker):
    def submit_orders(self, orders):
        check = pre_trade_risk_check(orders, self.account, self.get_nav(), self.config)
        log_risk_audit(check)                                   # 步骤 1 风控
        results = []
        for o in orders:
            if check.is_blocked(o["ticker"]) or o.get("skip"):
                results.append({**o, "status": "REJECTED"}); continue
            price = get_latest_price(o["ticker"])               # 步骤 2 撮合（V1 close）
            if price is None:
                results.append({**o, "status": "REJECTED"}); continue
            fees = self.fee_model.compute(o["side"], price, o["shares"], o["ticker"])
            self.account.execute_fill(o, price, fees)           # 步骤 3 记账
            results.append({**o, "status": "FILLED", "fill_price": price})
        return results
```

**回测-实盘对拍指标**（§22.3，B 保留）：signal 一致率（应 100%，诊断）/ 成交价偏差分布（诊断）/ 成交率偏差（判断）/ 累计净值跟踪误差（判断）。偏差物理来源：滑点模型、涨跌停封板、延迟、部分成交、最低 5 元佣金——方向全部「实盘比回测更差」，回测是收益上限。回测侧对应补丁：涨跌停拒单、成交量约束、1-bar 延迟（分钟频）。

## 实施时定稿项

- ~~get_latest_price/get_prices 数据接口签名~~ **已定稿** → `execution/market_data.py`（QUANT-27 Review 确认），与 order_generation 同源。`get_latest_price(bars, ticker, asof, field="close"|"open") -> float | None`；`get_prices(bars, tickers, asof, field="close") -> dict[str, float]`。撮合取 `field="open"`（T+1 开盘，gap #5 裁决 B）；严格 trade-date 等值、raw 未复权、重复行 raise。
- QmtBroker 的方法签名（V2 再定）（§66）
- ~~BT/VN Adapter（§24）~~ 不再需要——§66.2/§68 不用 backtrader/vnpy
- ~~vnpy 网关子包按实际账户选（CTP/IB）~~ 不再需要——§66.2 不用 backtrader/vnpy
- ~~backtrader patch 是否回流自维护副本~~ 不再需要——§66.2 不用 backtrader/vnpy
