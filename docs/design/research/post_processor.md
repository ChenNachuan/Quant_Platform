# post_processor

> 域：research · 对应代码 [research/post_processor.py](../../../research/post_processor.py)

## 职责与边界

因子后处理流水线：将原始因子值清洗为可直接用于评估或合成的干净截面因子值。严格按 **去极值 → 填充 → 标准化 → 中性化（可选）** 顺序执行，所有步骤按 `groupby(date)` 截面独立计算，天然无前视偏差。

- 输入：`compute()` 原始输出（[storage.md](factor/storage.md) 仅持久化原始值）。
- 输出：干净因子值 → 对接 [evaluation.md](factor/evaluation.md) 与 [combiner.md](signal/combiner.md)。
- 不属于本模块：因子计算（[interface.md](factor/interface.md)）、独立正交化模块（因子去重归 [governance.md](factor/governance.md)，回归控制并入中性化）。

## 接口签名

```python
MAD_SCALE: float = 1.4826
MIN_NEUTRALIZE_ROWS: int = 10

def winsorize_mad(s: pd.Series, n: float = 3.0) -> pd.Series: ...
    # 边界 = median ± n × 1.4826 × MAD，clip 到边界
def fillna_median(s: pd.Series) -> pd.Series: ...
def fillna_cross_section(df: pd.DataFrame, col: str) -> pd.DataFrame: ...
    # 按 date 截面中位数填充，缺失键行原样保留
def standardize_zscore(s: pd.Series) -> pd.Series: ...   # (x - mean) / std，默认
def standardize_rank(s: pd.Series) -> pd.Series: ...     # rank(pct=True)，偏态分布备选
def neutralize(
    df: pd.DataFrame,
    factor_col: str,
    industry_col: str = "industry",
    size_col: str = "log_market_cap",
    method: str = "industry_size",
) -> pd.DataFrame: ...
    # 按日截面 OLS 取残差：factor ~ 行业哑变量 + log(市值)

@dataclass
class PostProcessStep:
    name: str
    func: Callable[[pd.Series], pd.Series]
    params: dict = field(default_factory=dict)

class PostProcessor:
    def __init__(self, steps: list[PostProcessStep] | None = None): ...
    def transform(self, df: pd.DataFrame, factor_col: str, date_col: str = "date") -> pd.DataFrame: ...
```

预设 Pipeline：`PIPELINE_RESEARCH`（`winsorize → fillna → zscore`）、`PIPELINE_STOCK_EVAL`（股票评估预设，配合单独调用 `neutralize()`）、`PIPELINE_ETF`（不做中性化）、`PIPELINE_RANK`（标准化替换为 `standardize_rank`）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 四步顺序：去极值 → 填充 → 标准化 → 中性化 | 先标准化后填充 / 先填充后去极值 | 极值会扭曲 z-score 均值与方差；标准化与中性化需要完整截面 |
| 去极值 = MAD winsorize（`n=3.0`） | 3σ / 分位数裁剪 | median 与 MAD 不受极端离群值污染；3σ 使用了被污染的统计量 |
| 标准化默认 z-score | 仅用 rank | 保留截面相对幅度；严重偏态因子可选 `PIPELINE_RANK` |
| 中性化 = OLS 残差（申万一级行业哑变量 + log 市值） | 不做中性化 | 未中性化的 IC 易混入行业与市值 Beta 伪 alpha |
| 个股池中性化、ETF 池不做 | 全池统一中性化 | ETF 策略本身交易行业/风格 Beta |
| 存原始值，消费侧做后处理 | 存储后处理结果 | 参数可调无需全量重算，且原始分布可直接排查异常 |
| 全步骤 `groupby(date)` 截面独立 | 全样本统计量 | 使用全样本 mean / std / MAD 属于典型前视偏差 |
| 不独立实现正交化模块 | 单独正交化流水线 | 需要控制暴露时可在回归中性化侧扩展因子列 |

## 实现要点

详见 [research/post_processor.py](../../../research/post_processor.py)。

核心约定：
- `PostProcessor.transform` 逐步骤执行 `groupby(date_col)[factor_col].transform(step.func, **step.params)`，并通过 `_keep_null_key_rows` 防止分组键缺失行被静默覆写为 `NaN`。
- `standardize_zscore` 在截面有效离散度低于相对容差 `SIGMA_RTOL`（`1e-12`）时将非缺失值统一置为 `0.0`，避免除零溢出。
- `neutralize` 单截面有效样本不足 `MIN_NEUTRALIZE_ROWS`（`10`）时原样透传；构建设计矩阵时自动检查矩阵秩，列满秩与欠定情形统一通过 `numpy.linalg.lstsq` 求解残差。
- 缓存与存储仅针对 `Factor.compute()` 原始输出，后处理结果与参数不进入因子缓存 key。
