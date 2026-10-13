# alpha_type

> 域：research · 对应代码 research/alpha_type.py · 来源 §65（archive/OPEN_QUESTIONS.md）

## 职责与边界

策略配方治理：alphaType 注册（meta-info + publish 双表）、7 维标签、五态生命周期、规则库（Rule ABC + RulePipeline）、Signal 产出调度、Portfolio 约束求解入口。公司 PMS 的 alphaType 维表 + 计算器注册 + 发布流水线的个人版——取其结构，去其流程（单人 <10 策略，无审批流）。

- 负责：`alphaType = signal_source + rules + params` 的注册与流转；标签与查询；规则实例化与串联；run_daily_signal 调度与失败 hold；build_portfolio 解析解 + 约束检查。
- 不负责：信号计算（[alpha_signal.md](alpha_signal.md)）、ML 模型治理（[ml_signal.md](ml_signal.md)）、因子注册（FactorRegistry，[factor_governance.md](factor_governance.md)）、手数取整/订单（[execution/order_generation.md](execution/order_generation.md) §17.3/§34.4）、TargetPortfolio 字段级定义（[execution/portfolio_construction.md](execution/portfolio_construction.md)）。
- 与 configs/strategies/*.toml 的分工：**配方 = 结构引用，config = 数值参数**（§13.1）。

## 接口签名

```python
# data/registry/alpha_types.json        ← dim-alphaType-meta-info（15 字段）
# data/registry/alpha_type_publishes.json ← dim-alphaType-publish（发布记录）
# config_snapshot = "path@hash" + git commit（版本化）

TAG_DIMENSIONS = {
    "asset_type":     {...},          # 封闭枚举
    "frequency":      {...},
    "holding_period": {"1d", "1W", "2W", "1M", "3M"},
    "style":          {"momentum", "value", ...},
    "capacity":       {...},
    "turnover":       {"low", "medium", "high"},   # <50% / 50-150% / >150% 每月
    "status":         {"draft", "published", "production", "deprecated", "archived"},
}   # 7 维全枚举，禁自由文本

def list_alpha_types(filters: dict = None) -> list[dict]:
    """按标签过滤查询，如 {"style": "momentum", "asset_type": "etf"}。"""

def promote_alpha_type(alpha: dict, eval_report: dict) -> bool:
    """晋升 production。自动检查晋升线（§64.5），无审批流但全审计。"""
def log_audit(event: dict) -> None:
    """data/registry/audit_log.jsonl，append-only 不可改（更正靠新条目）。"""

class Rule(ABC):
    rule_id: str = ""
    category: str = ""        # signal_transform | filter | position | risk_gate
    param_schema: dict = {}   # 如 {"k": int} / {"max_turnover": float}
    def apply(self, data, **params): ...

class RulePipeline:
    """规则流水线：串联执行。不支持并联/嵌套（YAGNI）。"""
    def __init__(self, rules: list[Rule]): ...
    def run(self, data, params: dict): ...

def run_daily_signal(alpha: dict) -> None:
    """每日信号产出。失败 hold——不清仓是铁律。"""
def build_portfolio(signal, alpha, prev_weights=None) -> pd.DataFrame:
    """信号 → 目标权重（带约束检查）。V1 解析解。"""
def compute_turnover(new_w, old_w) -> float:
    """换手率 = Σ|Δw| / 2。"""
```

生命周期转换条件：

| 转换 | 条件 | 备注 |
|---|---|---|
| draft→published | 配方完整 + 标签 7 维全 | 写 publish 记录 |
| published→production | Walk-Forward 通过 + §64.5 晋升线 + 优于等权 | adopted_at |
| production→deprecated | §62.4/§64.7 exit 判据 或人工退役 | retired_at；停用回退等权 |
| deprecated→archived | 确认无引用 | 归档不删 |
| 任意→draft | 配方修改 | 回炉，version+1；可退回 published 改配方重发 |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 注册表 = data/registry/ JSON 双表 | SQLite / YAML | 零依赖、grep/git 友好、<10 策略够用 |
| alphaType = 策略配方（signal_source + rules + params） | 把代码当注册项 | 配方是可实例化规格，代码在 strategies/ 引用 |
| 标签 7 维全枚举禁自由文本 | 自由文本标签 | 防碎片化（momentum vs Mom vs 动量） |
| 打标：创建必填 / 发布校验 / 晋升可修正 | 只在创建时填 | 回测后统计的 capacity/turnover 需要修正窗口 |
| 五态生命周期，无审批流，自动检查 + 审计 | 审批/灰度流程 | 公司多人多策略的流程，单人不需要 |
| audit_log.jsonl append-only | 可改写日志 | 审计可信度 |
| Rule ABC + param_schema 校验 + RulePipeline 串联 | 并联/嵌套组合 | 串联可预期；嵌套 YAGNI |
| 规则四分类 signal_transform/filter/position/risk_gate | 无分类 | 对齐公司 Decision Engine 控制指令 |
| 规则 vs 变体判据：改它只影响精细度→规则；变成另一策略→新 alphaType | 一律新建 | 一句话判据防注册表膨胀 |
| 调度 cron + 手动，日频 15:30 后（cron 建议 `"30 16 * * 1-5"` 留缓冲） | 事件驱动 / 盘中 | 个人 Job<10；盘中是公司需求 |
| 失败：重试 1 次 → hold 上日 Portfolio → 告警，**不清仓** | 失败清仓 | 数据故障不是平仓理由（与 §10.2 同精神） |
| 约束集 V1 = 集中度检查告警 + 换手告警不裁剪 + 资金乘法 | 行业暴露/杠杆/市场中性 | Barra 级需求不做；硬裁破坏信号 |
| 求解器解析解（等权/top_k）默认，cvxpy V2 备选 | scipy.optimize | 约束集里只有检查项，无真正优化问题 |
| 求解失败 → 放松重试一次 → 回退等权不空仓 | 空仓 | 空仓是巨大偏离 |

## 实现要点

```python
def run_daily_signal(alpha: dict) -> None:
    try:
        signal = compute_alpha_signal(alpha)          # AlphaSignal（§63.5）
        portfolio = build_portfolio(signal, alpha)    # §65.7 解析解
        persist(portfolio)
    except Exception as e:
        for _ in range(1):                            # 重试 1 次
            ...
        hold_previous_portfolio(alpha)                # 不清仓
        send_alert("HIGH", f"signal failed {alpha['alpha_type_id']}: {e}")

def build_portfolio(signal, alpha, prev_weights=None):
    weights = signal_to_weights(signal, method="top_k", k=alpha["rules"]["top_k"])  # §63.5
    weights["weight"] *= alpha["params"].get("total_position", 1.0)  # 资金乘法，默认满仓
    # 集中度检查——告警不修正（等权 1/N 天然满足）
    # 换手检查 compute_turnover=Σ|Δw|/2——告警不裁剪
    return weights

# 规则 vs 变体（§65.5）：
# | 判据              | 是规则           | 是新 alphaType      |
# | 改动影响          | 只影响精细度     | 变成另一个策略      |
# 例：SmoothSignal(halflife=5→10) 是规则参数；momentum→value 是新 alphaType

# 性能：全 A 5000 标的 × 月频 60 期 <100ms——性能非瓶颈。
```

## 实施时定稿项

- RulePipeline 与 build_portfolio 的关系（rules 里 position 类规则是否就是 signal_to_weights 的参数化）
- audit_log.jsonl schema 字段定稿（action/from/to/metrics/ts 已列，还要什么）
- schedule_config 与 APScheduler 的装配位置（谁读 cron 配置建 Job）→ [../ops/scheduler.md](../ops/scheduler.md)
- 退回 published 改配方时 version 如何递增（patch 还是 minor）
