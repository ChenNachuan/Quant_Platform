# factor_evaluation

> 域：research · 对应代码 research/factor_evaluation.py · 来源 §27（archive/OPEN_QUESTIONS.md）

## 职责与边界

alphalens（**alphalens-reloaded 0.4.6**，§28.2/§28.6）接入与因子评价流水线：`FactorPipeline`（compute → preprocess → evaluate → register）+ `AlphalensAdapter` 格式适配 + 门控判定 + 产出落盘 + 月度重评估。

- 负责：因子数据来源桥接（开发期现算 / 注册后读表）、pricing 口径归一、评价指标提取、准入/退出门控、attempt_log 多重检验记账。
- 不负责：组合回测（门控通过后才进策略）、中性化算法本身（[post_processor.md](post_processor.md)）、注册表治理流程（[factor_governance.md](factor_governance.md)）。
- 下游策略**不直接依赖 alphalens**，只经 `DataAPI.get_factor()` 消费注册后因子。

## 接口签名

```python
class AlphalensAdapter:
    """格式适配（platform ↔ alphalens），纯静态无状态。"""
    @staticmethod
    def to_al_factor(factor_df: pd.DataFrame) -> pd.Series: ...
        # 宽表(date × ticker) → MultiIndex Series(date, asset)
    @staticmethod
    def to_al_prices(bars_df: pd.DataFrame, price_col='close') -> pd.DataFrame: ...
        # 长表 → 宽表(DatetimeIndex × ticker)；ffill(limit=5) 处理停牌
    @staticmethod
    def extract_metrics(factor_data, ic_series, mean_quant_ret) -> dict: ...
        # rank_ic_mean / ir / ic_t_stat / quantile_monotonicity / top_minus_bottom / turnover
    @staticmethod
    def check_gate(metrics: dict, n_attempts: int = 1) -> tuple[bool, dict]: ...

@dataclass
class EvalResult:
    summary: dict                  # 标量指标（决策面）
    gate_passed: bool
    gate_detail: dict
    ic_series: pd.DataFrame        # 日频 IC 序列(1D/5D/10D/20D)
    quantile_returns: pd.DataFrame
    turnover: pd.DataFrame
    report_path: str | None        # HTML tearsheet
```

`FactorPipeline` 流程：`compute(expr, universe, start, end)` → `preprocess(raw)` → `evaluate(processed, periods=(1, 5, 10, 20))` → `register(name, expr, result)`。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| pricing = 日频收盘价，前复权 qfq | hfq（§25.2 旧示例）/ 分钟数据 | 历史收益率可比；分钟频在评价阶段无意义 |
| alphalens = 回测前顺序门控 | 与回测并行 / 直接进回测 | 评价分钟级、全组合回测小时级；IC>0.04/IR>0.5/t>2 拦噪声 |
| alphalens 在方法体内 lazy import | 模块级依赖 | 薄壳不碰因子检验逻辑 |
| 产出三层落地：标量→注册表 eval 块、时间序列→factor_eval/ Parquet、tearsheet→HTML | 单一落点 | 标量是决策面、序列是诊断、HTML 是人读 |
| 多重检验 attempt_log 记 N，t 阈值 = max(2, √(2lnN))，per-factor-family | 全局统一 N | 防「挖过很多 value 把 momentum 门槛也抬高」 |
| 三阈值生命周期：准入 IC>0.04、退出 IC<0.03、中间观察区 | 单阈值 | 准入/退出分开，月度重评估标记 under_review |
| periods 默认 (1, 5, 10, 20) | 其他周期集 | §27.10 定稿 |
| 因子侧注册 = FactorRegistry + metadata.json（代码即注册表，§62.3） | factor_registry.yaml 单文件（§27 旧案） | 与策略侧 alphaType JSON 双表各司其职（§65） |

## 实现要点

```python
pipe = FactorPipeline(data_api)
raw = pipe.compute(expr="close / rolling_mean(close, 20) - 1",
                   universe_fn=lambda d: data_api.get_universe(d),
                   start="2020-01-01", end="2024-12-31")   # 开发期现算
processed = pipe.preprocess(raw)        # MAD → z-score → 中性化（§60）
result = pipe.evaluate(processed, periods=(1, 5, 10, 20))
if result.gate_passed:
    pipe.register(name="momentum_20d", expr="...", result=result)
# 月度重评估：IC < 0.03 → under_review（§62.3/§62.4 接降级流程）
```

门控公式：`t > max(2, sqrt(2*ln(N)))`（N=该因子族 attempt 数，含失败）。`to_al_prices` 的 `ffill(limit=5)` 是停牌边界坑在评价侧的处理。验证纪律：Adapter 四方法单测、3–5 只票集成、已知 IC 因子门控测试、直接调 alphalens API 对拍 IC/IR 一致。

## 实施时定稿项

- ~~factor_registry.yaml 与 alphaType 注册表的关系~~ 已定 → §62.3/§65（因子侧 FactorRegistry+metadata.json；策略侧 alphaType JSON 双表）（2026-10-10 回改）
- ~~periods 默认取值~~ 已定 → §27（正文已定 (1,5,10,20)，清单笔误列入未决）（2026-10-10 回改）
- ~~under_review 之后的降级/淘汰流程~~ 已定 → §62.3/§62.4（warning→降权/提前重训、exit→deprecated 不物理删除）（2026-10-10 回改）
- `factor_eval/` Parquet 的具体列 schema（§27.4 仅定落点未定列）
