# testing

> 域：core · 对应代码 core/testing.py（fixtures 归 tests/） · 来源 §69.4 / §68.7（archive/OPEN_QUESTIONS.md）

## 职责与边界

平台测试策略：pytest 三层目录、单测覆盖约定、hypothesis 性质测试、测试替身、黄金样本端到端对拍。规定**测什么、怎么组织、容差多少**，不含具体测试用例实现。

边界：因子单测四范式归 ../research/factor_governance.md；黄金样本的策略逻辑是等权 top-5 ETF 动量（§68.7），验证方法在此收口。

## 接口签名

```text
tests/
├── unit/         # 单测: 函数级，快
├── integration/  # 集成: 管道级
└── golden/       # 黄金样本: 端到端对拍（衔接 §68.7 五步验证）
```

**集成 3 场景**（§69.4）：①数据管道（fetch→DQC→parquet 落盘）②因子管道（FactorDAG→PostProcessor→per-factor 表）③回测主循环（build_portfolio→generate_orders→SimBroker→nav）。

```python
TOLERANCE = 1e-4

def test_pandas_matches_hand_calc() -> None: ...
def test_simbroker_matches_pandas() -> None: ...
```

单测覆盖表（9 模块）+ 性质（hypothesis）：

| 模块 | 测什么 | 性质（hypothesis） |
|---|---|---|
| FeeModel | 四件套费率手算对拍 | `fee ≥ 0`；卖出费用 ≥ 买入费用（印花税） |
| winsorize/zscore | 极值/标准化 | `zscore 后 mean≈0, std≈1`（逐截面） |
| build_portfolio | 权重合法 | `weights.sum() ≤ 1 + 1e-9`；无负权重（禁做空） |
| SimAccount | 账本恒等式 | `cash + market_value == nav`（每笔成交后） |
| compute_order_diff | 手数取整 | 结果是 lot_size 整数倍；不超预算 |
| FactorDAG | 拓扑序 | 环检测抛错；依赖先算 |
| load_config | 覆盖合并 | 策略级覆盖全局同名键 |
| run_job | 重试 | 失败 n 次后 status=FAILED |
| ASSET_RULES | 查表 | 未知 asset_type 抛 KeyError |

测试替身表：

| 外部依赖 | 替身 |
|---|---|
| akshare 网络 | fixture Parquet（预存 5 ETF × 3 月） |
| DuckDB/磁盘 | 内存 DataFrame / tmp_path |
| QMT 柜台 | SimBroker（本就是 V1 唯一实现） |
| 时钟 | 固定日期参数注入 |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| pytest 三层（unit/integration/golden） | 单层测试堆 | 按速度与意图分层，三档耗时预算可控 |
| hypothesis 性质测试补充示例测试 | 只写示例测试 | 费率/权重/账本恒等式适合性质化（fee≥0、cash+mkt_value==nav 等） |
| 黄金样本 = 等权 top-5 ETF 动量 | 旧 baseline_spec.py（sharpe 16.001135 不可信、trade_count 12686 暗示高频、期货 ALSTM vs V1 A 股、VectorBT/ZVT vs pandas） | ≤30 行 akshare 可重现，任何环境跑出同一组数字——旧基线保留作期货 ALSTM 自身回归基准，不作平台级黄金样本 |
| 五步验证法 | 只对拍最终指标 | 手算 Excel→pandas→SimBroker→扣费→五年基线，逐层定位分歧 |
| TOLERANCE = 1e-4 | — | golden_expected.json 手算 4 位小数 |
| V1 无 CI，本地三档耗时预算 | 先上 GitHub Actions | unit < 10s / integration < 60s / golden < 5min；CI 是 V2 |
| 测试替身表 | 真调网络/柜台 | 时钟固定日期注入，QMT 用 SimBroker |

## 实现要点

```python
from hypothesis import given, strategies as st

@given(price=st.floats(1, 1000), shares=st.integers(100, 10000))
def test_fee_non_negative(price: float, shares: int) -> None:
    fee = FeeModel().buy_fee(price, shares)
    assert fee >= 0
    assert fee >= 5.0          # 最低 5 元（§66.4）


def test_pandas_matches_hand_calc() -> None:
    """pandas 实现 vs golden_expected.json 手算（Excel 4 位小数）。"""
    result = golden_baseline()
    expected = json.loads(Path("tests/golden/golden_expected.json").read_text())
    for key in ("total_return", "sharpe", "mdd"):
        assert abs(result[key] - expected[key]) < TOLERANCE
```

黄金样本五步验证（§68.7）：

| 步骤 | 内容 | 容差 |
|---|---|---|
| 1 | 手算 Excel 对拍（月末调仓 3 个月） | 4 位小数一致 |
| 2 | pandas 实现 vs Excel | < 0.01% |
| 3 | SimBroker 全链路 vs pandas | < 0.01% |
| 4 | 扣费前后对比 | 费用与 FeeModel 手算一致 |
| 5 | 2020–2024 五年完整基线 | 落盘 metrics 做回归基准 |

预期区间：年化 10–15% / 夏普 0.8–1.2 / MDD 15–25%。超范围排查表：

| 症状 | 排查方向 |
|---|---|
| 年化 > 20% | 查未来函数（shift(1)/成交价基准） |
| 夏普 > 2 | 查前视（权重用了当期信息） |
| MDD < 10% | 查是否漏了极端行情段（2022/2024） |
| 费用 < 1% | 查 FeeModel 是否漏计（印花税卖出/最低 5 元） |

fixtures：`tests/golden/golden_prices.parquet`（5 ETF × 3 月）+ `golden_expected.json`（手算期望值）。

## 实施时定稿项

- 黄金样本 ETF 标的池定稿（教程 §2 具体清单 vs 510300/510500/518880/511010/159915 自选池，§68.8）
- GitHub Actions workflow 具体写法（V2，§69.6）
