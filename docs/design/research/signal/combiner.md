# factor_combiner

> 域：research · 对应代码 [research/signal/combiner.py](../../../../research/signal/combiner.py)

## 职责与边界

多因子合成层：将通过单因子检验的因子合成为综合 `score`，并输出组合级评估与因子贡献拆解。

- 负责：合成前预处理契约（去极值 → 逐截面 z-score → 中性化）、合成方法与权重窗口、8 项组合评估指标与 `factor_contribution` 拆解、`eval_report.json` 落盘。
- 不负责：单因子评价门控（[evaluation.md](../factor/evaluation.md)）、后处理基础算子（[post_processor.md](../post_processor.md)）、`score → weights` 信号映射（[alpha_signal.md](alpha_signal.md)）、ML 因子内部合成（[runtime.md](../factor/runtime.md)）。

## 接口签名

```python
WEIGHT_METHODS: tuple[str, ...] = ("equal_weight", "ic_weight", "ic_ir_weight")

def preprocess(df: pd.DataFrame, factor_col: str, ...) -> pd.DataFrame: ...
    # 逐截面执行：3 MAD winsorize → 截面 z-score → 中性化（个股池必做、ETF 池可省）

def factor_contribution(weights: dict[str, float], factor_returns: pd.DataFrame, factor_exposures: pd.DataFrame) -> dict: ...
    # 按因子暴露与因子收益拆解组合收益贡献

def write_eval_report(name: str, metrics: dict, config_path: str | None = None, ...) -> Path: ...
    # 落盘 data/signals/{name}/eval_report.json（含 config_snapshot path@hash）

class FactorCombiner:
    def __init__(self, method: str = "equal_weight", lookback: int = 60): ...
    def combine(
        self,
        factor_scores: pd.DataFrame,          # [date, ticker] × N 因子 z-score 值
        ic_history: pd.DataFrame | None = None,  # [date] × N Rank IC；来源 factor_eval/{name}/ic_series.parquet
    ) -> pd.DataFrame: ...                    # → [date, ticker, combined_score]
    def evaluate(self, ...) -> dict: ...
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 主路径采用等权 z-score baseline | 一上来就做复杂加权 | 复杂方法若跑不赢等权即视为过拟合，回退等权 |
| `ic_ir_weight` 为首选加权可选项（`lookback=60`） | 仅按均值 `ic_weight` | ICIR 惩罚波动大的不稳定因子，样本外更稳健 |
| 不做回归 `max ICIR`（`w* ∝ Σ⁻¹·IC`） | 协方差逆矩阵加权 | 因子协方差矩阵求逆在样本外噪声大，极易过拟合 |
| ML 合成归 `MLFactor` | `combiner` 内嵌模型 | 保持线性合成层单一职责，避免放大多重检验风险 |
| 用相关性聚类去重（`>0.7` 留 IC 高者）代替正交化 | Gram-Schmidt / PCA 正交化 | 正交化对因子排列顺序敏感且会扭曲因子经济学含义 |
| 中性化前置到预处理层，组合层不做 Barra 行业约束 | 组合层重复中性化 | 风险层只做集中度与换手检查 |
| 权重窗口 = 扩张窗口 + 月频更新 + IC 严格取 `t-1` | 滚动窗口 / 日频 / 当期 IC | 防止样本短期噪声与当期权重前视泄漏 |
| 负 IC / 负 ICIR 权重截断为 0 | 允许负权重做空 | A 股现货不做空，负权重信号在组合中不稳定 |
| IC 历史只读 `factor_eval/{name}/ic_series.parquet` | `combiner` 内部重算 IC | 保证评价口径与合成口径单一来源一致 |

## 实现要点

详见 [research/signal/combiner.py](../../../../research/signal/combiner.py)。

核心约定：
- 权重计算：`_ic_weights` 按最近 `lookback` 期 `mean(IC_i)` 计算并把负值截断为 `0` 后归一化；`_ic_ir_weights` 按 `mean(IC_i) / std(IC_i)` 同口径截断归一化（全部非正时退回等权）。
- 权重更新节奏：月末使用 `t-1` 及之前历史计算权重，供下一月度再平衡使用，并与单因子去重（相关性 `< 0.5` 入库线）协同工作。
- 评估 8 指标参考区间与警戒线：Rank IC `> 0.04`（警戒 `< 0.03`）、IC IR `> 0.5`（警戒 `< 0.3`）、年化夏普 `0.8–1.2`（`> 2.0` 排查过拟合）、最大回撤 `< 20%`（警戒 `> 30%`）、Calmar、月频换手 `30–100%`（警戒 `> 150%`）、最强单因子贡献 `< 50%`（`> 70%` 视为伪分散）。
- 验证纪律：Walk-Forward 分折输出至 `eval_report.json`（含 `config_snapshot path@hash`），测试集严格只看一次。
