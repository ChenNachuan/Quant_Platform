# dqc_rules

> 域：data · 对应代码 data/dqc/rules/ · 来源 §36（archive/OPEN_QUESTIONS.md）

## 职责与边界

DQC **规则库**：44 条四维规则的定义、阈值、版本化与 pandera 执行引擎。四维 = 完整性 / 合法性 / 一致性 / 时序性；每条规则显式标注 HARD（阻断入库，27 条）或 SOFT（告警+标记入库，17 条）——硬软边界从「逐场景争论」变成「逐规则标注」。

边界分工：本模块只定「**查什么、多严**」；「查出来怎么办」（drop/flag、升级阈值、quality_flag）在 [dqc_severity.md](dqc_severity.md)；执行时机、耗时与报告双表在 [dqc_runtime.md](dqc_runtime.md)；被拒数据去处在 [quarantine_raw.md](quarantine_raw.md)。与公司 DQC(185868851) 的强规则=熔断 / 弱规则=告警对齐（§21.1）。

**条数口径注记**：来源材料标题写「完整规则集（40 条）」、阈值拆分 24+14+2=40；但四张维度表实际 10+14+11+9=**44 行**。本文以表 44 条为准，差异照录。

## 接口签名

规则库按 5 类资产存放，对应 5 个 YAML（主配置粒度）：

| YAML | 资产 | 规则适用标记 |
|---|---|---|
| `bars_daily.yaml` | 日线行情 | daily |
| `bars_minute.yaml` | 分钟行情 | minute |
| `fundamental.yaml` | 基本面 | fund |
| `factor.yaml` | 因子 | factor |
| `reference.yaml` | 日历/行业/合约规格 | reference |

```yaml
# YAML 每条 check 的声明形态
- id: L01
  name: price_positive
  severity: HARD            # HARD | SOFT（之上再叠 dqc_severity 的 row_action/escalation）
  type: column_check        # column_check | custom_check
  columns: [open, high, low, close]
  condition: "gt"
  value: 0
  filter: "suspend == False"
  # custom_check 形态：
  # function: "checks.custom_checks.check_ohlc_relation"
  # params: {default_limit: 0.10, chinext_star_limit: 0.20, tolerance: 0.005}
```

```python
build_schema(yaml_path) -> pandera.DataFrameSchema   # YAML → schema
schema.validate(df, lazy=True)                        # lazy 收集全部错误
# CLI 按需重验（历史不自动重跑）：
# python -m dqc.engine revalidate --asset bars_daily --start 2024-01-01 --end 2024-12-31
# 结果落 _dqc_results.parquet，带 rule_version 列，可审计
```

### 44 条规则一览（ID | 检查名 | 严重度 | 阈值 | 适用）

完整性 C01–C10：

| ID | 检查名 | 严重度 | 阈值 | 适用 |
|---|---|---|---|---|
| C01 | row_count_vs_calendar | HARD | 去重 trade_date 数 = 日历天数 | daily, fund |
| C02 | row_count_vs_universe | SOFT | 单日 ticker ≥ universe 95% | daily, fund |
| C03 | minute_bar_count | HARD | 每 ticker 每日 240 根（A 股）/按合约定 | minute |
| C04 | null_critical_fields | HARD | 非停牌 OHLCV 零空值 | daily, minute |
| C05 | null_soft_fields | SOFT | amount/adjfactor/PE/PB 空值率 ≤1% | daily, fund |
| C06 | adjfactor_present | HARD | ticker×date 缺失 0% | daily |
| C07 | calendar_completeness | HARD | 当年+未来 30 交易日无断点 | reference |
| C08 | factor_date_coverage | SOFT | 滚动 252 日缺失 ≤2 天 | factor |
| C09 | factor_ticker_coverage | SOFT | ≥ universe 90% | factor |
| C10 | industry_coverage | HARD | 活跃 ticker 行业覆盖 100% | reference |

合法性 L01–L14：

| ID | 检查名 | 严重度 | 阈值 | 适用 |
|---|---|---|---|---|
| L01 | price_positive | HARD | 非停牌 OHLC > 0 | daily, minute |
| L02 | volume_non_negative | HARD | volume, amount ≥ 0 | daily, minute |
| L03 | ohlc_relation | HARD | low ≤ open,close ≤ high | daily, minute |
| L04 | price_limit_stock | SOFT | 主板 10.5% / 创·科 20.5% / IPO 前 5 日 30.5%（=板块涨跌停 + 0.5% 容差） | daily |
| L05 | price_limit_futures | SOFT | 合约涨跌停 + 0.5%（查 reference） | daily(期货) |
| L06 | adjfactor_positive | HARD | > 0 | daily |
| L07 | adjfactor_range | SOFT | (0.01, 1000) | daily |
| L08 | boolean_flags | HARD | suspend 等 flags 为 bool 无空 | daily |
| L09 | pe_pb_range | SOFT | PE∈(-1e4,1e4), PB∈(-1e3,1e3) | fund |
| L10 | market_cap_positive | HARD | 总市值 > 0 | fund |
| L11 | factor_finite | HARD | 无 inf/-inf | factor |
| L12 | factor_nan_rate | SOFT | NaN 率 ≤10% | factor |
| L13 | ticker_format | HARD | `^[036]\d{5}\.(SH\|SZ)$` | all |
| L14 | date_format | HARD | ISO 8601 | all |

一致性 K01–K11：

| ID | 检查名 | 严重度 | 阈值 | 适用 |
|---|---|---|---|---|
| K01 | open_gap_check | SOFT | 非除权日复权 open/昨收缺口 ≤10% | daily |
| K02 | volume_amount_match | HARD | volume>0 ⇔ amount>0 | daily, minute |
| K03 | suspend_consistency | HARD | 停牌 → volume=0 且 OHLC 相等 | daily |
| K04 | limit_flag_consistency | SOFT | limit_up→close=high；limit_down→close=low（容差 0.01） | daily |
| K05 | adjfactor_stable | HARD | 非公司行动日 adjfactor 不变 | daily |
| K06 | daily_vs_minute_close | HARD | 日线 close = 末根分钟 close（±0.01） | daily+minute |
| K07 | daily_vs_minute_volume | HARD | 日线 volume = 分钟 volume 之和（≤0.1%） | daily+minute |
| K08 | daily_vs_minute_hl | HARD | 日线 high/low 包含分钟极值 | daily+minute |
| K09 | factor_cross_section | SOFT | 截面 std>1e-8 且 abs(mean)<100 | factor |
| K10 | industry_universe_match | HARD | universe 集合 = industry 集合 | reference |
| K11 | fund_vs_daily_tickers | SOFT | 差集 ≤5 | fund+daily |

时序性 T01–T09：

| ID | 检查名 | 严重度 | 阈值 | 适用 |
|---|---|---|---|---|
| T01 | daily_freshness | HARD | 最新 trade_date 距今 ≤1 交易日 | daily, fund |
| T02 | minute_freshness | HARD | 同上 | minute |
| T03 | no_future_dates | HARD | 所有 trade_date ≤ 今天 | all |
| T04 | date_ascending | HARD | 分区内严格递增无重复 | daily, minute, fund |
| T05 | minute_timestamp_order | HARD | ticker×date 内分钟戳严格递增连续 | minute |
| T06 | adjfactor_idempotent | SOFT | 重入库 adjfactor 不变 | daily |
| T07 | factor_lag | SOFT | D 日数据 D+2 前到达 | factor |
| T08 | calendar_forward | SOFT | 日历延伸 ≥30 交易日 | reference |
| T09 | late_correction | SOFT | 历史被改 >0.01% 即告警 | daily, fund |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 44 条四维规则逐条带 ID/严重度/阈值/来源/适用资产 | 抽查式少量规则 | 四维全覆盖，硬软边界落在规则上 |
| YAML（简单规则）+ Python 函数（复杂规则）混合 | 纯 pandera Python / 纯 YAML | 改阈值不改代码，又表达得了跨表勾稽 |
| pandera 做验证引擎 | 自研校验循环 | schema→validate 成熟方案 |
| semver 版本化（patch=阈值 / minor=加规则 / major=改语义） | 无版本 | 结果带 `rule_version`，可审计 |
| 历史数据**不自动重跑**，CLI `revalidate` 按需 | 规则一改就全量重验 | 阈值放宽旧数据必过、收紧追溯意义不大 |
| 两时点执行：入库时 HARD 阻断+SOFT 记录；每周批量全量 SOFT + K06–K08 + T09 | 每天全量跑跨资产 | K06–K08 要求 daily+minute 同时在，跨日拼批 |
| 不抽样：增量全量 + 定期全量 | 抽样检查 | 抽样漏检 |
| 自适应阈值 phase-2 不做 | 初版就做滚动统计 | 先固定（~监管/定义）+统计（历史分位）跑通 |
| 粒度：按资产为主 + per-check columns + check 参数（板块）+ reference 查表（合约） | 按 ticker 定制 | 按 ticker 维护成本 > 收益 |

阈值三类来源：**固定**（交易所规则/数学定义/schema，不可调）~24 条；**统计**（3+ 年历史分位数，建议年度复核）~14 条；**自适应**（滚动计算）2 条 phase-2。逐条表统计约为固定 32 / 统计 12，与占比表口径差异照录，实施以逐条表为准。

## 实现要点

```text
dqc/
  rules/          bars_daily.yaml  bars_minute.yaml  fundamental.yaml  factor.yaml  reference.yaml
  checks/         custom_checks.py   # ~15 个复杂检查函数（OHLC 关系、涨跌停…）
                  cross_checks.py    # K06-K08 跨粒度（需两张表同时在）
  schemas/        *_schema.py        # 从 YAML 生成 pandera DataFrameSchema
  engine.py       # YAML→schema + 执行 + 结果收集
  report.py       # 结果写 _dqc_results.parquet
```

```python
def ingest_daily_bars(df, date):                      # 入库时档（§36.10）
    schema = build_schema("dqc/rules/bars_daily.yaml")
    result = schema.validate(df, lazy=True)           # lazy 收集所有错误
    hard_failures = [e for e in result.errors if severity(e) == "HARD"]
    if hard_failures:
        raise DQCHardFailure(hard_failures)           # 阻断入库（处置见 dqc_severity）
    log_results([e for e in result.errors if severity(e) == "SOFT"], date)
    write_parquet(df, date)
```

代码量（`dqc/` 全模块，不计入薄壳）：5 个 YAML ~300 + custom_checks ~200 + cross_checks ~80 + engine ~150 + report ~60 ≈ **790 行**（§37 修订为 ~870，见 dqc_severity）。验证：干净小样本 44 条 0 failure；每条 HARD 构造恰好触发的脏数据验证阻断；K06–K08 构造不一致 case；真实 hs300×1 年看 SOFT 失败率。

## 实施时定稿项

- ~~被拒收数据的隔离区（§3.5）~~ 已定 → §39
- ~~DQC 耗时预算与并行度（§3.3）~~ 已定 → §38
- ~~`_dqc_results.parquet` 完整字段 schema、保留期、看板与告警通道（§3.4）~~ 已定 → §38
- 另类数据（舆情/卫星等）的规则集——暂无，等有数据源再加第 6 个 YAML（§36）
- L13 正则 `^[036]\d{5}\.(SH|SZ)$` 是否覆盖科创板/北交所代码形态（目前只含沪深 6 位）（§36）
- K09 自适应阈值的具体滚动算法（phase-2）（§36）
- `contract_specs` reference 表（L05 合约涨跌停查表用）的字段 schema（§36）
- ~~修正检测的应用/影响传播/是否重跑回测~~ 已定 → §45（检测→应用→影响传播流程落 [data_revision.md](data_revision.md)；T09 只做检测告警）
- 各规则阈值来源（固定/统计/自适应）逐条归属见 §36 原表，本文 44 条表略该列（聚合口径见「阈值三类来源」注）
