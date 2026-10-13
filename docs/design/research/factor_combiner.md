# factor_combiner

> 域：research · 对应代码 research/factor_combiner.py · 来源 §16、§63（archive/OPEN_QUESTIONS.md）

## 职责与边界

多因子合成：把通过单因子检验的因子捏成一个综合 `score`，并给出合成质量评估。实现级收口在 §63（FactorCombiner 类、权重窗口、评估指标表、eval_report）。

- 负责：合成前预处理原则（去极值 → 逐截面 z-score → 中性化）、合成方法选型与权重窗口、score→weights 之外的合成本体、组合评估指标与 factor_contribution、eval_report 落盘。
- 不负责：单因子评价门控（[factor_evaluation.md](factor_evaluation.md)）、后处理流水线实现（[post_processor.md](post_processor.md)）、score→持仓权重（[alpha_signal.md](alpha_signal.md) `signal_to_weights`）、ML 合成（归 MLFactor，[factor_runtime.md](factor_runtime.md) §59）。
- 与 §60（后处理）、§62（治理指标）、§65（alphaType signal_source）衔接。

## 接口签名

```python
class FactorCombiner:
    def __init__(self, method: str = "equal_weight",   # equal_weight | ic_weight | ic_ir_weight
                 lookback: int = 60):                  # IC 权重窗口(交易日)
        ...
    def combine(self, factor_scores: pd.DataFrame,     # [date, ticker] × N 因子 z-score 值
                ic_history: pd.DataFrame | None = None # [date] × N 因子 Rank IC；来源 factor_eval/{name}/ic_series.parquet
                ) -> pd.DataFrame:                     # → [date, ticker, combined_score]
        ...
    def _equal_weights(self, factor_cols) -> dict: ...
    def _ic_weights(self, ic_history, factor_cols) -> dict:
        # w_i = mean(IC_i 最近 lookback 期)，负 IC 截断为 0，归一化和 1
    def _ic_ir_weights(self, ic_history, factor_cols) -> dict:
        # w_i = mean(IC_i)/std(IC_i) 最近 lookback 期，负 ICIR 截断为 0，归一化
```

合成前预处理契约（`preprocess(factor_df)`，逐截面做）：`去极值(3 MAD winsorize) → 截面 z-score → 中性化回归(行业 + log 市值，个股池必做、ETF 池可省)`。

评估产出（§63.6）：8 指标表 + factor_contribution 拆解 + `data/signals/{name}/eval_report.json`（含 config_snapshot path@hash）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 主路径等权 z-score，永远先跑等权 baseline | 一上来就加权 | 铁律：跑不赢等权的复杂度就是过拟合，退回等权 |
| ICIR 加权可选（lookback=60） | IC 加权 | ICIR 惩罚不稳定因子，比 IC 均值更稳 |
| 回归 max ICIR 不做 | `w* ∝ Σ⁻¹·IC` | Σ⁻¹ 不稳定，样本外常不如等权（Markowitz 失灵经典现象） |
| ML 合成归 MLFactor | combiner 内嵌模型 | 合成层单一职责；ML 合成放大多重检验 |
| 不做 Gram-Schmidt/PCA 正交化，改相关性聚类去重（>0.7 留 IC 最高） | 正交化 | 正交化对顺序敏感、扭曲因子经济含义；聚类更简单可靠 |
| 中性化在预处理层，组合层不做 Barra | 组合层行业暴露中性 | 风险层只做集中度/换手检查（§65.6） |
| 权重窗口 = 扩张窗口 + 月频更新 + IC 只用 t-1 | 滚动窗口 / 日频 / 当期 IC | 因子少历史短滚动噪声大；当期 IC 定当期权重 = 前视 |
| 负 IC 权重截断为 0 | 负权重做空 | A 股不能做空，负权重不稳健 |
| IC 历史只读 `factor_eval/{name}/ic_series.parquet` | combiner 自己算 IC | 单一来源，与评价层口径一致 |
| 预处理必须逐截面 | 全样本一次性标准化 | 全样本分布 = look-ahead |

## 实现要点

```python
# 合成（§63.3）
combiner = FactorCombiner(method="ic_ir_weight", lookback=60)
combined = combiner.combine(factor_scores, ic_history)
# → [date, ticker, combined_score]

# 权重窗口表（§63.4）
# 窗口类型: 扩张窗口（起点固定，终点滚动）
# 更新频率: 月频（月末算权重，下月用），与再平衡对齐
# IC 取值:   只用 t-1 及之前的 IC
# 负 IC:     权重截断 0

# 评估 8 指标（§63.6，期望区间 / 警戒线）
# Rank IC > 0.04 / < 0.03        IC IR > 0.5 / < 0.3
# 年化夏普 0.8–1.2 / > 2.0 疑过拟合   最大回撤 < 20% / > 30%
# Calmar（年化/|MDD|）            换手 Σ|Δw|/2 月频 30–100% / > 150%
# 因子贡献 最强 < 50% / > 70% = 伪分散

# factor_contribution: 组合收益按因子暴露分解
# eval_report.json → data/signals/{name}/（含 config_snapshot path@hash、walk_forward_folds）
# 验证纪律: Walk-Forward + 测试集只看一次 + eval_report.json 落盘
```

合成方法速查（§16.3/§63.2 五法裁决）：等权首选、ICIR 加权可选、IC 加权不做（ICIR 更稳）、回归 max ICIR 不做、ML 合成归 §59。自动挖掘因子必须先聚类去重再合成，否则冗余让任何加权失真（入库「相关 < 0.5」判据的价值在此体现）。

## 实施时定稿项

- ensemble（§64.5）的权重如何进 FactorCombiner（复用 ic_ir_weight？）
- factor_contribution 简化版算法精度（用分组收益当因子收益的近似误差）
- signal_to_weights 的 proportional 对负 score 的处理（clip 0 vs 保留负权——A 股不能做空所以倾向 clip）→ 详见 [alpha_signal.md](alpha_signal.md)
- eval_report.json 的 walk_forward_folds 字段结构
