# alpha_signal

> 域：research · 对应代码 [research/signal/alpha_signal.py](../../../../research/signal/alpha_signal.py)

## 职责与边界

统一信号层：将多因子合成分数或 ML 模型分数汇聚为单一的截面 `AlphaSignal` 对象，并提供 `score → target_weight` 的无约束初值转换。构成平台主链 `AlphaSignal → Portfolio → Execution` 的第一环。

- 负责：信号契约（截面分数 + 原因 + 因子快照）、双路径汇聚（`from_multi_factor` / `from_ml_factor`）、`score → weights` 转换、信号日终落盘与 `SIGNAL` Journal 记录。
- 不负责：因子合成（[combiner.md](combiner.md)）、组合约束求解（约束权威归 `PortfolioConstraints`，见 [portfolio_construction.md](../../execution/portfolio_construction.md)）、实盘盘中 tick 级信号、订单生成（[order_generation.md](../../execution/order_generation.md)）。

## 接口签名

```python
WEIGHT_METHODS: tuple[str, ...] = ("top_k", "proportional")

@dataclass(frozen=True)
class AlphaSignal:
    frame: pd.DataFrame                  # [date, ticker, score]，已截面 z-score
    meta: dict                           # strategy_id / signal_time / trade_date / config_snapshot
    reason: str | dict = ""              # 人可读信号归因
    factor_snapshot: dict | None = None  # 触发信号的因子快照

    @classmethod
    def from_multi_factor(cls, combined: pd.DataFrame, **meta) -> "AlphaSignal": ...
        # 接收 FactorCombiner 输出 [date, ticker, combined_score] → 截面 z-score 封装为 AlphaSignal
    @classmethod
    def from_ml_factor(cls, ml_scores: pd.DataFrame, **meta) -> "AlphaSignal": ...
        # 接收 MLSignalAdapter 输出（见 ml/signal.md）
    def to_weights(self, method: str = "top_k", k: int = 20, **kw) -> pd.DataFrame: ...
        # 委托 signal_to_weights → [date, ticker, target_weight]

def signal_to_weights(
    signal_frame: pd.DataFrame,
    method: str = "top_k",   # top_k | proportional
    k: int = 20,
    **kw,
) -> pd.DataFrame: ...
    # top_k: 每日选取 score 最高的 k 只等权 1/k
    # proportional: w_i ∝ max(score_i, 0) 归一化（负 score clip 为 0）

def write_signal(signal: AlphaSignal, weights: pd.DataFrame, ...) -> Path: ...
```

信号日终归档路径（运行时内存传递 `AlphaSignal` 实例不序列化，盘后归档 Parquet）：

| 落点 | 字段 |
|---|---|
| `$QUANT_DATA_ROOT/signals/cross/{strategy_id}/{date}.parquet` | `signal_time` / `ticker` / `target_weight` / `score` / `strategy_id` / `trade_date` |
| `$QUANT_DATA_ROOT/signals/timeseries/{strategy_id}/{date}.parquet` | `datetime` / `ticker` / `direction` / `strength` / `quantity` / `factor_snapshot` / `reason` |

`SIGNAL` 事件写入统一 `Journal`（事件枚举见 [ledger.md](../../execution/ledger.md)）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 单层 `AlphaSignal`（截面分数矩阵） | 多层 Signal 体系 / Tick 信号层 | 离线月/日频截面研究不需盘中事件流，接口边界保持在函数层 |
| 信号形态 = 连续 float 截面分数 | 分位分组 / 二值多空 | 分组只是 `signal_to_weights` 的一种映射方式，保留连续分数信息量更完整 |
| 进入信号前统一截面 z-score | 原始分数量纲混用 | 保证多因子与 ML 两路输出量纲可比 |
| `to_weights` 默认 `top_k` 等权，`proportional` 可选 | 默认信号幅度加权 | 等权稳健、不引入额外尾部放大参数 |
| `max_weight` / `turnover_limit` 不放入信号对象 | 在信号层内嵌组合约束 | 约束权威唯一归口 `PortfolioConstraints`，信号层只表达排序/强度 |
| 运行时内存中传递 dataclass，日终落盘 Parquet | 每步序列化 JSON | 兼顾内存零拷贝性能与盘后复盘/Journal 对账可追溯性 |
| `reason` + `factor_snapshot` 随信号保留 | 只留裸分数字段 | 便于归因定位与异常排查 |

## 实现要点

详见 [research/signal/alpha_signal.py](../../../../research/signal/alpha_signal.py)。

核心约定：
- `AlphaSignal.from_multi_factor` 与 `AlphaSignal.from_ml_factor` 均在构造成 `AlphaSignal` 前执行截面 `z-score` 归一化。
- `signal_to_weights(method="proportional")` 对负 score 执行 `clip(lower=0.0)` 后按截面和归一化（A 股不支持做空）；`to_weights` 产出的是**无约束初始权重**，随后交由 `PortfolioOptimizer.solve()`（[portfolio_construction.md](../../execution/portfolio_construction.md)）完成集中度/换手裁剪与手数取整。
