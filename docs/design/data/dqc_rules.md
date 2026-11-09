# dqc_rules

> 域：data · 对应代码 [data/dqc/rules/](data/dqc/rules/) ·  [data/dqc/checks/](data/dqc/checks/) 

## 职责与边界

DQC **规则库**：44 条四维规则的定义、阈值、版本化与 pandera 执行引擎。四维 = 完整性 / 合法性 / 一致性 / 时序性；每条规则显式标注 HARD（阻断入库，27 条）或 SOFT（告警+标记入库，17 条）。

边界分工：本模块只定「**查什么、多严**」；「查出来怎么办」在 [dqc_severity.md](dqc_severity.md)；被拒数据去处在 [quarantine_raw.md](quarantine_raw.md)。

## 规则配置

规则库按 5 类资产存放，对应 5 个 YAML：

| YAML | 资产 | 规则适用标记 |
|---|---|---|
| `bars_daily.yaml` | 日线行情 | daily |
| `bars_minute.yaml` | 分钟行情 | minute |
| `fundamental.yaml` | 基本面 | fund |
| `factor.yaml` | 因子 | factor |
| `reference.yaml` | 日历/行业/合约规格 | reference |

两种规则形态:
- **column_check**(声明式)：YAML内指定 columns/condition/value/filter
- **custom_check**(函数式)：指向 checks/custom_checks.py`或`checks/cross_checks.py`的函数

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

## 44 条规则一览

### 完整性 C01–C10：

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

### 合法性 L01–L14：

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

### 一致性 K01–K11：

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

### 时序性 T01–T09：

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
| semver 版本化（patch=阈值 / minor=加规则 / major=改语义） | 无版本 | 结果带 `rule_version`，可审计 |
| 历史数据**不自动重跑**，CLI `revalidate` 按需 | 规则一改就全量重验 | 阈值放宽旧数据必过、收紧追溯意义不大 |
| 入库时 HARD 阻断+SOFT 记录；每周批量全量 SOFT + K06–K08 + T09 | 每天全量跑跨资产 | K06–K08 要求 daily+minute 同时在，跨日拼批 |
| 不抽样：增量全量 + 定期全量 | 抽样检查 | 抽样漏检 |

阈值三类来源：**固定**（交易所规则/数学定义/schema，不可调）~24 条；**统计**（3+ 年历史分位数，建议年度复核）~14 条；**自适应**（滚动计算）2 条 phase-2。逐条表统计约为固定 32 / 统计 12，与占比表口径差异照录，实施以逐条表为准。

## 实现要点

详见 [data/dqc/rules/](data/dqc/rules/) ·  [data/dqc/checks/](data/dqc/checks/) 

核心约定:
- 简单规则在YAML声明(column_check),复杂规则在 checks/custom_checks.py'和 checks/cross_checks.py'实现
- K06-K08跨粒度检查需要daily+minute两张表同时在,按周批量执行
- 规则版本 semver:patch=阈值调整,minor=加规则,major=语义变更
- CLI按需重验: `python -m data.dqc.engine revalidate --asset bars_daily --start ... --end ...
