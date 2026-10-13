# factor_governance

> 域：research · 对应代码 research/factor_governance.py · 来源 §62（archive/OPEN_QUESTIONS.md）

## 职责与边界

因子治理层：入库/退出的量化标准、生命周期状态机、血缘、命名与文档规范、相似因子去重、容量控制、异常排查、单测范式、四图报告。消费 [factor_evaluation.md](factor_evaluation.md) 的 IC 序列，与 [factor_storage.md](factor_storage.md) 的 metadata.json、[factor_runtime.md](factor_runtime.md) 的 depends_hash 衔接。

注册表机制（`FactorRegistry` / `@register_factor` / 工厂创建）归 [../core/registry.md](../core/registry.md)，本模块只定治理口径（代码即注册表、元数据载体、状态三态）。

不属于本模块：IC 计算与 alphalens tearsheet（[factor_evaluation.md](factor_evaluation.md)）、多因子合成（[factor_combiner.md](factor_combiner.md)）、独立数据库/JSON 注册表（不建）。

## 接口签名

```python
META_FIELDS = ["name", "version", "description", "category", "frequency",
               "asset_types", "depends_on", "status", "created_at"]

def list_factor_meta() -> list[dict]: ...
    # 遍历 FactorRegistry 汇总类属性元数据（因子 <100，遍历足够）

def evaluate_factor(factor_values: pd.DataFrame,   # [date, ticker, factor_name]
                    returns: pd.DataFrame,         # [date, ticker, fwd_return]
                    factor_name: str) -> dict: ...
    # → {ic_mean, ic_ir, ic_t, n_periods, coverage}；t = IR × √n

def check_factor_health(ic_history: pd.Series) -> str: ...
    # → "healthy" | "warning" | "exit"（rolling 6 月 IC vs 历史均值三档）

def detect_redundant_factors(factor_matrix: pd.DataFrame,   # 宽表 [date, ticker, f1, ...]
                             threshold: float = 0.7) -> list[tuple[str, str, float]]: ...
    # 按日截面 Spearman 相关取均值，超阈值的因子对按 |corr| 降序

def diagnose_factor(df: pd.DataFrame, factor_name: str) -> dict: ...
    # 五分支排查第一步：nan_ratio/inf_count/zero_ratio/coverage_min/
    # min_date(NaN 率最高日)/value_min/max/mean/std/cs_std_cv

def plot_factor_report(factor_values, returns, factor_name,
                       n_groups: int = 5) -> None: ...
    # 2×2 四图 → research/reports/{factor_name}_tearsheet.png（§27.8）
```

生命周期：`draft →(入库检验通过)→ production →(衰减检测触发)→ deprecated`；production 期间 rolling 监控可进 `warning`（提前重训/降权）再降级。deprecated 不物理删除（保留代码+历史数据）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 代码即注册表：FactorRegistry + 类属性 + metadata.json | 独立 JSON/SQLite 注册表 | 因子 <100 遍历足够；独立注册表引入代码-注册表同步问题 |
| 质量 8 指标：IC/IR/t/分组单调/换手/半衰期/覆盖率/相关 | 只看 IC | 教程 §8.9 八条判据的量化实现 |
| 入库线 IC>0.04 / IR>0.5 / t>2 / 相关<0.5 / 可解释 | 拿退出线当入库线 | 「不要拿退出线当入库线用」；中间灰色地带继续观察 |
| 退出线 IC<0.03 或 rolling 36 月 t<1.5 | 只按 IC 一条线 | 教程 §18.4 衰减退出判据 |
| 三态 draft→production→deprecated | 公司 5 色灯 | 状态传导用 depends_hash 触发重算替代，不引入 ProductionStatus |
| 血缘=depends_on + FactorDAG + depends_hash 三件套 | 独立血缘存储/per-di 版本记录 | 全量重算 <5 秒，知道「依赖了谁」即可；图从 DAG 自动生成 |
| 命名 `{类型}_{核心语义}_{参数}` snake_case | `mom20`/`Momentum20`/`factor_001`/中文 | 可读性优先；UFormula 短 tag 是公式热路径需求 |
| 文档=docstring + description 属性 | 独立文档文件 | compute() 是权威定义；研究过程写研究日志 |
| 去重：截面 Spearman 均值 >0.7 标冗余，4 档处理 | — | 「不做相关性检查，5 个相关 0.9 的因子 = 1 个因子」（§7.8） |
| 冗余处理：保留 IC 高者，**不做合并** | 等权平均合并 | 合并引入新自由度；合成在 [factor_combiner.md](factor_combiner.md) |
| deprecated 不物理删除 | 删代码/删数据 | 保回测复现与排查 |
| 容量 20-50 个 production 因子 | 无上限 | 「3-5 个就够」是单策略层面；因子库供多策略选用；>50 说明去重没做好 |
| 多重检验校正 t=√(2·ln N) | 固定 t>2 不看 N | 试 100 个才找到的 t=2.5 不可信（§8.9[8]，对齐 attempt_log） |
| 单测四范式含 test_no_lookahead | 只测数值对拍 | 前视偏差是三大坑之首，必须显式测 |

## 实现要点

```python
# —— 质量评估（evaluate_factor 核心）——
merged = factor_values.merge(returns, on=["date", "ticker"])
ic_series = merged.groupby("date").apply(
    lambda g: g[factor_name].corr(g["fwd_return"], method="spearman"))
ic_mean, ic_std = ic_series.mean(), ic_series.std()
ic_ir = ic_mean / ic_std if ic_std > 0 else 0.0
ic_t = ic_ir * np.sqrt(len(ic_series))

# —— 健康检查三档（check_factor_health）——
mean_ic = ic_history.mean()
recent_6m, recent_12m = ic_history.tail(126).mean(), ic_history.tail(252).mean()
# recent_6m > 0.7*mean_ic → healthy；> 0.5*mean_ic → warning（降权/提前重训）；
# recent_12m < 0.3*mean_ic → exit

# —— 去重 4 档（detect_redundant_factors）——
# >0.9 高度冗余：留 IC 高者，另一个 deprecated
# 0.7–0.9 可能冗余：经济逻辑不同则保留（动量 vs 反转）
# 0.5–0.7 警告：记录（入库线 <0.5 已约束）；<0.5 独立

# —— 单测四范式（TestMomentum20D）——
# test_basic_computation: 手算 3 天窗口精确对拍 + schema 断言
# test_nan_handling: 停牌 NaN 不污染整列；notna().mean() > 0.8
# test_output_schema: columns == ["date", "ticker", "momentum_20d"]
# test_no_lookahead: 删最后一天后历史值不变（np.allclose(equal_nan=True)）

# —— 四图报告（plot_factor_report，2×2 figsize(14,10)）——
# [1] IC 时序柱状+均值红线 [2] 累计 IC [3] 截面分布直方 [4] 分组累计收益(n_groups)
```

容量预算：经典风险因子 5–8 / 行业风格 3–5 / 另类 3–5 / ML 2–3，production 合计 15–20（+draft/deprecated 共 20–50）。五分支排查：全部标的→数据问题、特定标的→停牌/ST/退市、特定日期→查 depends_hash、数值范围→除零/溢出/NaN 传播、IC 突变→参数或衰减。新增代码 ≈180 行。

## 实施时定稿项

- `detect_redundant_factors` 的 factor_matrix 来源（load_factor_matrix() 拼宽表后截面去 NaN？）（§62）
- ~~check_factor_health 的 ic_history 存储位置~~ 已定 → §61（factor_eval/{name}/ 序列）（§62）
- `diagnose_factor` 的 cs_std_cv 阈值（多少算异常截面）（§62）
- `plot_factor_report` 的 n_groups 默认值确认（5 与 §8.9[4] 分组单调判据一致）（§62）
