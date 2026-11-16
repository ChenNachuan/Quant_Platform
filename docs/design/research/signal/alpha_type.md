# alpha_type

> 域：research · 对应代码 [research/signal/alpha_type.py](../../../../research/signal/alpha_type.py)

## 职责与边界

策略配方治理层：`alphaType` 注册（meta-info + publish 双 JSON 表）、7 维封闭标签检索、五态生命周期、规则流水线（`Rule` ABC + `RulePipeline`）、日频信号产出调度与解析解权重构建。

- 负责：`alphaType = signal_source + rules + params` 的注册与状态流转；标签过滤查询；规则实例化与串联执行；`run_daily_signal` 调度与失败 hold；`build_portfolio` 解析解与集中度/换手告警检查。
- 不负责：信号分数计算（[alpha_signal.md](alpha_signal.md)）、ML 模型治理（[ml/signal.md](../ml/signal.md)）、因子注册（`FactorRegistry`，见 [governance.md](../factor/governance.md)）、手数取整与订单生成（[order_generation.md](../../execution/order_generation.md)）、`TargetPortfolio` 约束求解（[portfolio_construction.md](../../execution/portfolio_construction.md)）。
- 与 `config/strategies/*.toml` 的分工：**配方负责结构引用，TOML 配置负责数值参数**。

## 接口签名

```python
# 持久化载体（$QUANT_DATA_ROOT/registry/）：
# - alpha_types.json           （配方元信息）
# - alpha_type_publishes.json  （发布记录）
# - audit_log.jsonl            （append-only 审计流水）

TAG_DIMENSIONS = {
    "asset_type":     ...,  # 封闭枚举
    "frequency":      ...,
    "holding_period": {"1d", "1W", "2W", "1M", "3M"},
    "style":          {"momentum", "value", ...},
    "capacity":       ...,
    "turnover":       {"low", "medium", "high"},
    "status":         {"draft", "published", "production", "deprecated", "archived"},
}
RULE_CATEGORIES = frozenset({"signal_transform", "filter", "position", "risk_gate"})

def create_alpha_type(alpha: dict) -> dict: ...
def list_alpha_types(filters: dict | None = None) -> list[dict]: ...
def transition_alpha_type(alpha: dict, target: str) -> dict: ...
def promote_alpha_type(alpha: dict, eval_report: dict) -> bool: ...
    # 自动检查 Walk-Forward 晋升线（IC > 0.04, IR > 0.5, Sharpe > 1.0），无人工审批流但写全审计
def log_audit(event: dict) -> None: ...

class Rule(ABC):
    rule_id: str = ""
    category: str = ""
    param_schema: dict = {}
    def apply(self, data: pd.DataFrame, **params: Any) -> pd.DataFrame: ...

class TopKFilter(Rule): ...
def create_rule(rule_id: str, **params: Any) -> Rule: ...

class RulePipeline:
    def __init__(self, rules: list[Rule]): ...
    def run(self, data: pd.DataFrame, params: dict) -> pd.DataFrame: ...

def compute_turnover(new_weights, old_weights) -> float: ...
def build_portfolio(signal, alpha: dict, prev_weights=None) -> pd.DataFrame: ...
def run_daily_signal(alpha: dict, date=None, prev_portfolio=None) -> None: ...
```

五态生命周期转换条件：

| 转换 | 条件 | 备注 |
|---|---|---|
| `draft → published` | 配方字段完整 + 7 维标签齐全 | 写入发布记录 `alpha_type_publishes.json` |
| `published → production` | Walk-Forward 通过 + 达到晋升线 + 优于等权基准 | 记录 `adopted_at` |
| `production → deprecated` | 触发衰减退出判据或人工退役 | 记录 `retired_at`；停用时回退等权基准 |
| `deprecated → archived` | 确认无策略引用 | 归档保留，不物理删除 |
| `任意 → draft` | 修改配方结构 | 退回草稿重走发布并递增版本号 |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 注册表 = `data/registry/` 双 JSON + JSONL 审计 | SQLite / YAML | 零依赖、git 与 grep 友好，个人 <10 策略完全够用 |
| `alphaType` = 策略配方（`signal_source + rules + params`） | 把策略代码直接当注册项 | 配方是可实例化的结构声明，具体参数由 TOML 注入 |
| 7 维标签全封闭枚举 | 自由文本标签 | 防止 `momentum` / `Mom` / `动量` 碎片化导致检索失效 |
| 分阶段打标：创建必填、发布校验、晋升可修正 | 创建后锁定 | `capacity` / `turnover` 需依赖回测统计结果再修正 |
| 五态生命周期、自动检查 + 审计 | 多人审批/灰度流 | 单人环境无需审批流，靠不可篡改审计日志追溯 |
| `audit_log.jsonl` append-only | 原地修改日志 | 保证历史状态变迁可审计，更正仅追加新条目 |
| `Rule` ABC + `param_schema` + `RulePipeline` 串联 | 并联/嵌套有向规则树 | 串联执行行为可预期，避免规则编排过度复杂 |
| 规则 vs 新 `alphaType` 判据 | 任意改动都新建或硬改 | 仅影响精细度参数→规则；改变核心策略逻辑→新 `alphaType` |
| cron + 手动调度（盘后 `30 16 * * 1-5` 留缓冲） | 盘中事件驱动 | 适配日频研究与盘后批处理节奏 |
| 执行失败：重试 1 次 → hold 上日 Portfolio → 告警，**不清仓** | 异常即清仓 | 数据或网络临时故障不是平仓理由 |
| V1 组合检查 = 集中度告警 + 换手告警不硬裁 + 资金乘法 | 硬裁剪 / 杠杆 / 市场中性 | 集中度/换手不粗暴截断信号，约束裁剪交 `PortfolioOptimizer` |

## 实现要点

详见 [research/signal/alpha_type.py](../../../../research/signal/alpha_type.py)。

核心约定：
- `promote_alpha_type` 强制校验证明门槛：`_PROMOTE_IC_MIN = 0.04`、`_PROMOTE_IR_MIN = 0.5`、`_PROMOTE_SHARPE_MIN = 1.0`。
- `run_daily_signal` 串联信号计算、规则流水线与 `build_portfolio`，单次失败重试后仍异常时自动保持上日持仓并调用 `send_alert("HIGH", ...)`（参见 [scheduler.md](../../ops/scheduler.md) 与 [alerting.md](../../ops/alerting.md)）。
- `compute_turnover = sum(|Δw|) / 2`；`build_portfolio` 对 `signal_to_weights` 产出乘以 `total_position`（默认 `1.0`）并做集中度与换手告警检查。
