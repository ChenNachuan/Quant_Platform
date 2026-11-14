# testing

> 域：core · 对应代码 [core/testing.py](../../../core/testing.py)（fixtures 归 tests/）
## 职责与边界

平台测试策略：pytest 三层目录、单测覆盖约定、hypothesis 性质测试、测试替身、黄金样本端到端对拍。规定**测什么、怎么组织、容差多少**，不含具体测试用例实现。

边界：因子单测四范式归 ../research/factor_governance.md；黄金样本的策略逻辑是等权 top-5 ETF 动量，验证方法在此收口。

## 测试分层

```text
tests/
├── unit/         # 单测: 函数级
├── integration/  # 集成: 管道级
└── golden/       # 黄金样本: 端到端对拍
```

**集成 3 场景**：
1. 数据管道（fetch→DQC→parquet 落盘）
2. 因子管道（FactorDAG→PostProcessor→per-factor 表）
3. 回测主循环（build_portfolio→generate_orders→SimBroker→nav）

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
| hypothesis 性质测试补充示例测试 | 只写示例测试 | 费率/权重/账本恒等式适合性质化 |
| 黄金样本 = 等权 top-5 ETF 动量 | 旧 baseline | ≤30 行 akshare 可重现，任何环境跑出同一组数字 |
| 五步验证法 | 只对拍最终指标 | 逐层定位分歧 |
| TOLERANCE = 1e-4 | — | golden_expected.json 手算 4 位小数 |
| V1 无 CI，本地三档耗时预算 | 先上 GitHub Actions | CI 是 V2 |

黄金样本五步验证：

| 步骤 | 内容 | 容差 |
|---|---|---|
| 1 | 手算 Excel 对拍 | 4 位小数一致 |
| 2 | pandas 实现 vs Excel | < 0.01% |
| 3 | SimBroker 全链路 vs pandas | < 0.01% |
| 4 | 扣费前后对比 | 费用与 FeeModel 手算一致 |
| 5 | 2020–2024 五年完整基线 | 落盘 metrics 做回归基准 |

预期区间：年化 10–15% / 夏普 0.8–1.2 / MDD 15–25%。

| 异常 | 排查方向 |
|---|---|
| 年化 > 20% | 查未来函数（shift(1)/成交价基准） |
| 夏普 > 2 | 查前视（权重用了当期信息） |
| MDD < 10% | 查是否漏了极端行情段（2022/2024） |
| 费用 < 1% | 查 FeeModel 是否漏计（印花税卖出/最低 5 元） |

组合层详细约定见tests/golden/portfolio_handcalc.md
