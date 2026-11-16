# factor_evaluation

> 域：research · 对应代码 [research/factor/evaluation.py](../../../../research/factor/evaluation.py)

## 职责与边界

alphalens（`alphalens-reloaded`）集成与研究侧因子准入流水线：`FactorPipeline`（`compute → preprocess → evaluate → register`）+ `AlphalensAdapter` 格式适配 + 门控与衰减判定 + 产出落盘。

- 负责：开发期表达式安全求值计算、pricing 口径归一、评价指标提取、准入/衰减门控、`attempt_log` 多重检验记账与评测产物落盘。
- 不负责：组合回测（门控通过后进策略）、截面清洗算法本身（[post_processor.md](../post_processor.md)）、因子生命周期治理流转（[governance.md](governance.md)）。
- 下游策略不直接依赖 alphalens，只消费注册与入库后的因子数据。

## 接口签名

```python
DEFAULT_PERIODS: tuple[int, ...] = (1, 5, 10, 20)
GATE_IC_MIN: float = 0.04
GATE_IR_MIN: float = 0.5
GATE_T_BASE: float = 2.0
EXIT_IC_THRESHOLD: float = 0.03

class AlphalensAdapter:
    @staticmethod
    def to_al_factor(factor_df: pd.DataFrame) -> pd.Series: ...
        # 长表 [date, ticker, col] → MultiIndex Series(date, asset)
    @staticmethod
    def to_al_prices(bars_df: pd.DataFrame, price_col: str = "close") -> pd.DataFrame: ...
        # 长表 → 宽表 (DatetimeIndex × ticker)；ffill(limit=5) 处理停牌
    @staticmethod
    def extract_metrics(factor_data, ic_series, mean_quant_ret) -> dict: ...
        # 提取 rank_ic_mean / ir / ic_t_stat / quantile_monotonicity / top_minus_bottom / turnover
    @staticmethod
    def check_gate(metrics: dict, n_attempts: int = 1) -> tuple[bool, dict]: ...
        # 准入门限：rank_ic_mean > 0.04, ir > 0.5, ic_t_stat > max(2.0, sqrt(2*ln(N)))

def check_exit(metrics: dict) -> bool: ...
    # rank_ic_mean < 0.03 触发衰减退出警戒

@dataclass
class EvalResult:
    summary: dict                  # 标量指标（决策面）
    gate_passed: bool
    gate_detail: dict
    ic_series: pd.DataFrame        # 日频 IC 序列 (1D/5D/10D/20D)
    quantile_returns: pd.DataFrame
    turnover: pd.DataFrame
    report_path: str | None        # HTML tearsheet 路径

class FactorPipeline:
    def compute(self, expr: str, universe, start: str, end: str) -> pd.DataFrame: ...
    def preprocess(self, raw: pd.DataFrame) -> pd.DataFrame: ...
    def evaluate(self, processed: pd.DataFrame, periods: Sequence[int] = DEFAULT_PERIODS) -> EvalResult: ...
    def register(self, name: str, expr: str, result: EvalResult) -> None: ...
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| pricing = 日频收盘价（前复权 qfq） | 后复权 / 分钟数据 | 前复权历史收益率可比，评估前向周期以交易日为准 |
| alphalens 作为回测前顺序门控 | 与回测并行 / 直接进回测 | 评价为分钟级而全组合回测为小时级，先用 `IC>0.04 / IR>0.5 / t>2` 过滤噪声 |
| alphalens 在方法体内 lazy import | 模块级强制导入 | 保持轻量模块解耦与可测试性 |
| 产出三层落地：标量→`metadata.json` `eval` 块、时序→`factor_eval/{name}/`、HTML→`research/reports/` | 单一落点 | 标量服务门控决策，时序服务衰减诊断，HTML 服务人工复核 |
| 多重检验 `attempt_log` 按因子族（`factor_family`）记 `N`，`t > max(2, √(2·ln N))` | 全局统一 `N` | 防止某类因子试错过多抬高另一类因子的准入门槛 |
| 三阈值生命周期：准入 `IC > 0.04`、退出 `IC < 0.03`、中间观察区 | 单阈值 | 准入与退出留出缓冲区间，避免临界抖动频繁下线 |
| `periods` 默认 `(1, 5, 10, 20)` | 其他周期集合 | 覆盖短、中、月频换手研究区间 |
| 因子侧注册 = `FactorRegistry` + `metadata.json`（代码即注册表） | 独立 `factor_registry.yaml` 单文件 | 与策略侧 `alphaType` JSON 注册各司其职，消除代码与外部表同步漂移 |

## 实现要点

详见 [research/factor/evaluation.py](../../../../research/factor/evaluation.py)。

核心约定：
- 表达式求值通过白名单 AST 节点（`_BIN_OPS` / `_UNARY_OPS` / `_EXPR_FUNCS`）安全解析，`delta` / `pct_change` 严格校验正整数滞后阶数 `_positive_periods`，禁止前视偏移。
- `FactorPipeline.preprocess` 默认使用 `PIPELINE_STOCK_EVAL`（去极值 → 填充 → 标准化），并在元数据含 `industry` / `log_market_cap` 时于评估前先调用 `neutralize()`（alphalens 阶段不再重复中性化）。
- `to_al_prices` 向前延伸 45 个日历日缓冲区计算前向收益，并以 `ffill(limit=5)` 有限度处理短期停牌。
- 注册落盘：`save_factor_full` 写入因子目录并更新 `metadata.json`（含 `eval` / `attempts` / `attempt_log`），时序结果存 `$QUANT_DATA_ROOT/factor_eval/{name}/`，完整 HTML 报告存 `research/reports/{name}_{date}.html`。
