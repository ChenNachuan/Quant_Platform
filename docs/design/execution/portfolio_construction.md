# portfolio_construction

> 域：execution · 对应代码 execution/portfolio_construction.py · 来源 §17 / §34（archive/OPEN_QUESTIONS.md）

## 职责与边界

Signal → Portfolio 的**约束满足层**：接受策略产出的连续目标权重，做约束裁剪 + 手数取整，产出整数手数的 `TargetPortfolio`。

边界（§34.3 澄清）：**权重分配（等权 / score 加权 / 风险平价 / 均值-方差四法）发生在策略内部**（scores → weights），本模块**不求最优、只做约束满足**。信号链三层拆分：

```text
Signal（我觉得这些票好）→ Portfolio（我要持有这些仓位）→ Order（买卖差额）
CrossSignal.target_weights → PortfolioOptimizer.solve() → TargetPortfolio.positions
```

时序策略无此层：`SignalData` 直接转 Order（逐票决策，无组合级约束）。CTA 组合约束（如总保证金上限）将来再做简化版。

## 接口签名

```python
@dataclass
class PortfolioConstraints:          # 可变配置对象
    max_weight: float = 0.10         # 单票权重上限
    min_weight: float = 0.0
    max_turnover: float | None = None    # 换手率上限, turnover = sum(|Δw|)/2
    max_industry_weight: float | None = None
    max_total_weight: float = 0.95   # 留 5% 现金缓冲
    min_cash: float = 0.0
    lot_size: int = 100              # 手数硬约束（优化后执行）
    min_trade_value: float = 5000.0

@dataclass(frozen=True)
class TargetPosition:
    ticker: str
    target_weight: float        # 主单位（优化空间）
    target_quantity: int        # 执行单位（整数手）
    target_notional: float      # 风控/报告用
    ref_price: float            # T 日收盘价，仅用于权重↔股数换算
    side: str                   # "LONG" / "FLAT"

@dataclass(frozen=True)
class TargetPortfolio:
    strategy_id: str
    trade_date: date            # 信号日 T
    exec_date: date             # 执行日 T+1
    create_time: datetime
    positions: dict[str, TargetPosition]
    total_weight: float; num_holdings: int
    cash_weight: float; estimated_turnover: float
    solver_status: str          # "optimal" / "suboptimal" / "infeasible"
    constraints_applied: list[str]
    pre_opt_weights: dict[str, float] | None   # 取整前权重（调试/偏离分析）

class PortfolioOptimizer:
    def solve(self, raw_weights: dict[str, float],
              current_positions: dict[str, Position], account: Account,
              constraints: PortfolioConstraints, prices: dict[str, float],
              industry_map: dict[str, str] | None = None) -> TargetPortfolio: ...
```

换算链：`raw_quantity = w × total_asset / ref_price` → `target_quantity = floor_to_lot(raw_quantity, 100)` → 反算 `actual_weight`。`TargetPortfolio` 存取整后量，`pre_opt_weights` 存取整前。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 纯 numpy 裁剪两步走 | cvxpy / scipy.optimize | 策略已给目标权重，约束全是 box+线性，不需要二次规划 |
| 约束权威 = `PortfolioConstraints`（solve 入参） | `CrossSignal.max_weight`/`turnover_limit` 字段 | 信号不携带约束——两字段废弃，解掉 §32.8 遗留问题 |
| 手数取整放本模块 | 留在 `cs_portfolio_sim.rebalance()` | §34.8 修订：rebalance 只做差额→逐笔 |
| 取整做法 1+3（floor 取整+现金缓冲+门槛过滤） | 做法 2 整数规划 ILP | 小组合收益有限，与「不用 cvxpy」一致 |
| 权重为主单位，股数为执行单位 | 只存一种单位 | 优化在权重空间，执行要整数手，三单位共存于 TargetPosition |
| T 日收盘产出 / T+1 执行 | T 日收盘价直接执行 | 防前视（与 §18.2 成交价基准一致） |
| Signal:TargetPortfolio = 1:1 | 多对多 | 每个 CrossSignal 产一个目标组合 |
| 波动率倒数加权是权重分配性价比之选（策略侧） | mean-variance | Σ⁻¹ 不稳定；简化风险平价一行代码优于等权 |

## 实现要点

```python
# solve() 步骤 1 约束裁剪（纯 numpy）→ 步骤 2 手数取整
w = {t: min(max(v, constraints.min_weight), constraints.max_weight) for t, v in raw_weights.items()}
if constraints.max_industry_weight and industry_map:
    w = self._clip_industry(w, industry_map, constraints.max_industry_weight)
if sum(w.values()) > constraints.max_total_weight:        # 总权重归一
    s = constraints.max_total_weight / sum(w.values()); w = {t: v * s for t, v in w.items()}
if constraints.max_turnover:
    w = self._clip_turnover(w, current_positions, account, constraints.max_turnover)
pre_opt = dict(w)
positions = {}
for ticker, weight in w.items():
    qty = int(weight * account.total_asset / prices[ticker] // constraints.lot_size) * constraints.lot_size
    if qty * prices[ticker] < constraints.min_trade_value:
        qty = 0                                            # 门槛过滤
    if qty > 0:
        positions[ticker] = TargetPosition(ticker, qty * prices[ticker] / account.total_asset,
                                           qty, qty * prices[ticker], prices[ticker], "LONG")
return TargetPortfolio(..., positions=positions, solver_status="optimal", pre_opt_weights=pre_opt)
```

## 实施时定稿项

- `solver_status` 何时返回 `"suboptimal"` / `"infeasible"`（全被 min_trade_value 过滤光算 infeasible 吗？）（§34）
- `_clip_turnover` / `_clip_industry` 具体裁剪算法（按比例缩放？逐个裁？）（§34）
- `estimated_turnover` 精确定义（分母旧总资产还是新总资产）（§34）
- `constraints_applied` 记录粒度（§34）
- ~~期货/期权 lot_size 与合约乘数~~ 已定 → §69.5（V3/V4 预留，V1 不预抽象）；港股 lot_size 主数据来源仍留（§17.3）
- CTA 版简化 optimizer 触发条件（§34）
- cash drag 上限多少算可接受（§17.3）
