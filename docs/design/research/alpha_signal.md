# alpha_signal

> 域：research · 对应代码 research/alpha_signal.py · 来源 §32、§63.5（archive/OPEN_QUESTIONS.md）

## 职责与边界

统一信号层：把合成分数 / ML 分数汇聚成 `AlphaSignal`，并给出 score→持仓权重的转换。三步主链的第一步（AlphaSignal → Portfolio → Execution）。

- 负责：信号契约（截面分数 + 原因 + 因子快照）、双路径汇聚（from_multi_factor / from_ml_factor）、score→weights、信号落盘。
- 不负责：因子合成（[factor_combiner.md](factor_combiner.md)）、权重约束求解（约束权威 = PortfolioConstraints，[execution/portfolio_construction.md](execution/portfolio_construction.md)）、时序单票信号（Tick/盘中实时，月频不涉及）、订单生成（[execution/order_generation.md](execution/order_generation.md)）。
- 历史修正一句话：公司 Signal 三层（Blueshift/USignal/Tick）已被 §68.5 坍缩为一层 AlphaSignal；§32 的 CrossSignal/SignalData 两层 schema 随之收敛为本文的 AlphaSignal 一层（Tick 层不做）。

## 接口签名

```python
@dataclass(frozen=True)
class AlphaSignal:
    frame: pd.DataFrame      # [date, ticker, score]，已截面 z-score
    meta: dict               # strategy_id / signal_time / trade_date / config_snapshot
    reason: str | dict = ""  # 人可读产生原因
    factor_snapshot: dict | None = None   # 产生信号所用因子值

    @classmethod
    def from_multi_factor(cls, combined: pd.DataFrame, **meta) -> "AlphaSignal": ...
        # FactorCombiner 输出 [date, ticker, combined_score] → frame
    @classmethod
    def from_ml_factor(cls, ml_scores: pd.DataFrame, **meta) -> "AlphaSignal": ...
        # MLSignalAdapter 输出（见 ml_signal.md）

    def to_weights(self, method: str = "top_k", k: int = 20, **kw) -> pd.DataFrame: ...
        # → [date, ticker, target_weight]，委托 signal_to_weights

def signal_to_weights(signal_frame: pd.DataFrame,
                      method: str = "top_k",   # top_k | proportional
                      k: int = 20, **kw) -> pd.DataFrame:
    """
    top_k(默认):   score 最高的 k 个等权 1/k
    proportional:  w_i ∝ max(score_i, 0) 归一化（负 score clip 0）
    """
```

信号落盘（运行时 dataclass 内存传递不序列化；日终归档 Parquet）：

| 落点 | 字段 |
|---|---|
| `data_platform/signals/cross/{strategy_id}/{date}.parquet` | signal_time / ticker / target_weight / score / strategy_id / trade_date |
| `data_platform/signals/timeseries/{strategy_id}/{date}.parquet` | datetime / ticker / direction / strength / quantity / factor_snapshot / reason |

SIGNAL 事件写 Journal（EventType 12→13，§34 补 PORTFOLIO→14；事件总表见 [../execution/ledger.md](../execution/ledger.md)）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 一层 AlphaSignal（截面分数矩阵） | 公司三层 / CrossSignal+SignalData 两层 | 月频无盘中实时需求，Tick 层不用；接口边界从产品移到函数（§68.5） |
| 信号形态 = 连续 float 截面分数 | 分位分组 / 二值多空 | 分组是 signal_to_weights 的一种方式，不是信号形态 |
| score 前统一截面 z-score | 原始分数量纲混合 | 不同因子量纲不同，z-score 后可比可加 |
| to_weights 默认 top_k 等权，proportional 可选 | 默认信号加权 | 教程 §16.6「Top N 等权」；等权稳健不引入额外参数 |
| max_weight/turnover_limit 不放信号里（§34 废弃） | CrossSignal 内嵌约束 | 约束权威 = PortfolioConstraints，信号只给分数 |
| 运行时 dataclass 内存传递、不序列化 | 每步落 JSON | 信号是中间产物，日终归档 Parquet 即可 |
| 信号日终落盘（cross / timeseries 两目录） | 只留内存 | 复盘与 Journal 对账需要 |
| reason + factor_snapshot 进信号 | 只留分数 | 可解释、可追溯哪只因子推的 |

## 实现要点

```python
# §63.5 双路径汇聚
sig = AlphaSignal.from_multi_factor(combined, strategy_id="mom_top5", trade_date=date)
# 或
sig = AlphaSignal.from_ml_factor(ml_scores, strategy_id="lgbm_v1", trade_date=date)

# score → weights
w = sig.to_weights(method="top_k", k=5)        # → [date, ticker, target_weight]
# proportional: w_i ∝ max(score_i, 0)；负 score clip 0（A 股不能做空）

# 骨架
@dataclass(frozen=True)
class AlphaSignal:
    frame: pd.DataFrame
    meta: dict
    reason: str | dict = ""
    factor_snapshot: dict | None = None

    @classmethod
    def from_multi_factor(cls, combined, **meta):
        frame = cross_zscore(combined.rename(columns={"combined_score": "score"}))
        return cls(frame=frame, meta=meta)

    def to_weights(self, method="top_k", k=20, **kw):
        return signal_to_weights(self.frame, method=method, k=k, **kw)
```

下游契约：`to_weights` 产出的 target_weight 只是**无约束初值**，集中度/换手/资金约束由 PortfolioConstraints 求解（失败回退等权不空仓，见 [execution/portfolio_construction.md](execution/portfolio_construction.md)）。

## 实施时定稿项

- factor_snapshot 存全量因子还是只存用到的因子
- 混合策略（截面 + 时序）的 Journal 关联键
- proportional 对负 score 的 clip 0 定稿（§63.6 附带问题，与 [factor_combiner.md](factor_combiner.md) 同源）
