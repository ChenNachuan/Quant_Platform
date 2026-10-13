# post_processor

> 域：research · 对应代码 research/post_processor.py · 来源 §60（archive/OPEN_QUESTIONS.md）

## 职责与边界

因子后处理流水线：原始因子值 → 干净因子值。四步 **去极值 → 填充 → 标准化 → 中性化（可选）**，Pipeline 模式运行时可配置。所有步骤 `groupby(date)` 截面独立计算，天然无前视。

- 输入：`compute()` 原始输出（[factor_storage.md](factor_storage.md) 存的就是原始值）
- 输出：干净因子值 → 对接 [factor_evaluation.md](factor_evaluation.md)（alphalens）
- 不属于本模块：因子计算（[factor_interface.md](factor_interface.md)）、独立正交化步骤（并入中性化回归）

## 接口签名

```python
def winsorize_mad(s: pd.Series, n: float = 3.0) -> pd.Series: ...
    # 边界 = median ± n × 1.4826 × MAD，clip 到边界
def fillna_cross_section(df: pd.DataFrame, col: str) -> pd.DataFrame: ...
    # 截面中位数填充（备选 ffill / 0 / dropna，参数可选）
def standardize_zscore(s: pd.Series) -> pd.Series: ...   # (x-mean)/std，默认
def standardize_rank(s: pd.Series) -> pd.Series: ...     # rank(pct=True)，偏态分布备选
def neutralize(df, factor_col, industry_col="industry",
               size_col="log_market_cap",
               method="industry_size") -> pd.DataFrame: ...
    # 每截面 OLS 取残差：factor ~ 行业哑变量 + log(市值)

@dataclass
class PostProcessStep:
    name: str
    func: Callable[[pd.Series], pd.Series]
    params: dict = field(default_factory=dict)

class PostProcessor:
    def __init__(self, steps: list[PostProcessStep] | None = None): ...
    def transform(self, df, factor_col, date_col="date") -> pd.DataFrame: ...
        # 逐步 groupby(date).transform()，保证无前视
```

预设 Pipeline：`PIPELINE_RESEARCH`（winsorize→fillna→zscore）/ `PIPELINE_STOCK_EVAL`（+ 中性化，中性化需额外列单独调 `neutralize()`）/ `PIPELINE_ETF`（不中性化）/ `PIPELINE_RANK`（standardize 换 rank）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 四步顺序：去极值→填充→标准化→中性化 | 旧顺序 winsorize→standardize→fillna | 极值扭曲 z-score；标准化/中性化需要完整截面 |
| 去极值 = MAD winsorize（n=3.0） | 3σ / 分位数 winsorize | median/MAD 不受极值污染；3σ 用被污染的统计量处理污染源 |
| 标准化默认 z-score | 仅 rank | 保留相对幅度；严重偏态因子再用 rank |
| 中性化 = OLS 残差（行业哑变量 + log 市值） | 不中性化 | 「没有中性化的 IC 都是不可信的」——80% 伪 alpha 中性化后归零 |
| 个股池必须中性化、ETF 池不做 | 全池统一中性化 | ETF 本来就在做行业/风格轮动赚 Beta |
| **存原始值，消费时做后处理** | 存后处理值 | 换参数不用全量重算；原始分布可调试；后处理 <1 秒 |
| 所有步骤 groupby(date) 截面独立 | 全样本统计量 | 全样本 mean/std/MAD 是前视偏差 |
| 不独立实现正交化 | 单独正交化模块 | 中性化回归加其他因子列即 csfacneutral 思路 |
| Pipeline 可配置步骤列表 | UFormula 公式链内嵌后处理 | 研究阶段快速迭代；Python pipeline 比 XML 公式灵活 |

## 实现要点

```python
def winsorize_mad(s, n=3.0):
    med = s.median()
    mad = (s - med).abs().median()
    return s.clip(med - n * 1.4826 * mad, med + n * 1.4826 * mad)

class PostProcessor:
    def transform(self, df, factor_col, date_col="date"):
        result = df.copy()
        for step in self.steps:
            result[factor_col] = result.groupby(date_col)[factor_col].transform(
                step.func, **step.params)
        return result

# 中性化核心（每截面独立回归）
for date, group in df.groupby("date"):
    y = group[factor_col].values
    valid = ~np.isnan(y)
    if valid.sum() < 10: continue
    X_parts = [pd.get_dummies(group[industry_col], drop_first=True).values]
    if "size" in method:
        X_parts.append(group[size_col].values.reshape(-1, 1))
    X = np.column_stack([np.ones(len(y)), np.hstack(X_parts)])
    beta, _, _, _ = lstsq(X[valid], y[valid], rcond=None)
    result.loc[group.index, factor_col] = y - X @ beta
```

前视风险对照：去极值/填充/z-score/中性化 用全样本统计量均有前视 → 一律截面（每 date 独立）；rank 天然无前视。与 §58 缓存一致：缓存的是原始 `compute()` 输出，后处理不进缓存 key。新增代码 ≈110 行。

## 实施时定稿项

- 停牌股在消费端（策略/回测）的具体处理规则（§60）
- log 市值用流通还是总市值（行业分类已定 → §63 申万一级 31 行业哑变量，~~中性化数据来源~~ 已定 → §63）（§60）
- ~~PostProcessor 与 alphalens 入参的对接顺序~~ 已定 → §27（FactorPipeline preprocess 中性化在前，alphalens 只评价不再中性化）（§60）
- `PostProcessStep.params` 传参形式（`transform(step.func, **step.params)` 与 pandas transform 签名匹配）（§60）
