# dqc_rules

> 域：data · 对应代码 [data/dqc/rules/](../../../data/dqc/rules/) · [data/dqc/checks/](../../../data/dqc/checks/)

## 职责与边界

DQC **规则库**：基于四维质量体系（完整性 / 合法性 / 一致性 / 时序性），在原有 5 个核心资产 YAML（47 条规则：`C01–C12`、`L01–L14`、`K01–K12`、`T01–T09`）基础上扩展至 **14 个规则分组 YAML、共 69 条独立规则**，实现 `data/storage_schema.py` 全部 **52 张规范表** 在 `TABLE_TO_DQC_GROUP` 中的 100% 映射覆盖。每条规则显式标注 HARD（阻断入库 / 剔除问题行）或 SOFT（告警 + 标记入库）。

边界分工：本模块只定「**查什么、多严**」；「查出来怎么办」在 [dqc_severity.md](dqc_severity.md)；被拒数据去处在 [quarantine_raw.md](quarantine_raw.md)。

## 规则配置

规则库按 **14 个资产/表分组** 存放于 [data/dqc/rules/](../../../data/dqc/rules/)，在 `data/ingestion.py` 中通过 `TABLE_TO_DQC_GROUP` 映射全部 52 张规范表：

| YAML | 对应 DQC 组 | 覆盖表 / 适用范围 |
|---|---|---|
| `bars_daily.yaml` | `bars_daily` | `cn_stock_bars_daily` |
| `bars_minute.yaml` | `bars_minute` | `cn_stock_bars_minute` |
| `fundamental.yaml` | `fundamental` | `cn_stock_fundamental` |
| `factor.yaml` | `factor` | `factor` |
| `reference.yaml` | `reference` | 复合参考组（兼容旧调用方） |
| `calendar.yaml` | `calendar` | `cn_calendar` |
| `stock_basic.yaml` | `stock_basic` | `cn_stock_basic` |
| `corporate_actions.yaml` | `corporate_actions` | `cn_stock_corporate_actions` |
| `daily_status.yaml` | `daily_status` | `cn_stock_daily_status` |
| `etf_bars_daily.yaml` | `etf_bars_daily` | `cn_etf_bars_daily` |
| `index_bars_daily.yaml` | `index_bars_daily` | `cn_stock_index_bars_daily` |
| `etf_bars_minute.yaml` | `etf_bars_minute` | `cn_etf_bars_minute` |
| `futures_bars_daily.yaml` | `futures_bars_daily` | `cn_futures_bars_daily` |
| `tabular.yaml` | `tabular` | 通用 Schema 驱动组（行业、指数权重、PIT 财报、资金流、股东、公募基金、逐笔等其余 40 张表） |

两种规则形态:
- **column_check**（声明式）：YAML 内指定 `columns` / `condition` / `value` / `filter`
- **custom_check**（函数式）：指向 `checks/custom_checks.py` 或 `checks/cross_checks.py` 的函数

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
  # params: {main_limit: 0.10, chinext_star_limit: 0.20, tolerance: 0.005}
```

## 69 条规则一览

### 完整性 C01–C15：

| ID | 检查名 | 严重度 | 阈值 | 适用 |
|---|---|---|---|---|
| C01 | row_count_vs_calendar | HARD | 去重 trade_date 数 = 日历天数 | daily, fund |
| C02 | row_count_vs_universe | SOFT | 单日 ticker >= universe 95% | daily, fund |
| C03 | minute_bar_count | HARD | 每 ticker 每日 240/241 根（A 股）/ 按合约定 | minute |
| C04 | null_critical_fields | HARD | 非停牌 OHLCV 零空值 | daily, minute |
| C05 | null_soft_fields | SOFT | amount/adjfactor/PE/PB/PS 空值率 <= 1% | daily, fund |
| C06 | adjfactor_present | HARD | ticker x date 缺失 0% | daily |
| C07 | calendar_completeness | HARD | 当年 + 未来 30 交易日无断点 | reference |
| C08 | factor_date_coverage | SOFT | 滚动 252 日缺失 <= 2 天 | factor |
| C09 | factor_ticker_coverage | SOFT | >= universe 90% | factor |
| C10 | industry_coverage | HARD | 活跃 ticker 行业覆盖 100% | reference |
| C11 | row_count_vs_prev_day | SOFT | 当日行数 / 前日行数 in [0.9, 1.1] | daily |
| C12 | ticker_set_drift | SOFT | 跨日新增 <= 5% 且消失 <= 2% | daily |
| C13 | required_non_null | HARD | Schema 必填列零空值 | stock_basic, corporate_actions, tabular |
| C14 | daily_status_nullable_rate | SOFT | 非停牌核心限价列空值率 <= 1% | daily_status |
| C15 | calendar_gap_free | HARD | 日历非空且相邻交易日最大间隔 <= 12 天 | calendar |

### 合法性 L01–L27：

| ID | 检查名 | 严重度 | 阈值 | 适用 |
|---|---|---|---|---|
| L01 | price_positive | HARD | 非停牌 OHLC > 0 | daily, minute |
| L02 | volume_non_negative | HARD | volume, amount >= 0 | daily, minute |
| L03 | ohlc_relation | HARD | low <= open,close <= high | daily, minute |
| L04 | price_limit_stock | SOFT | 主板 10.5% / 创·科 20.5% / IPO 前 5 日 30.5% | daily |
| L05 | price_limit_futures | SOFT | 合约涨跌停 + 0.5%（查 reference） | futures_bars_daily |
| L06 | adjfactor_positive | HARD | > 0 | daily |
| L07 | adjfactor_range | SOFT | (0.01, 1000) | daily |
| L08 | boolean_flags | HARD | suspend 等 flags 为 bool 无空 | daily |
| L09 | pe_pb_range | SOFT | PE in (-1e4,1e4), PB in (-1e3,1e3) | fund |
| L10 | market_cap_positive | HARD | 总市值 > 0 | fund |
| L11 | factor_finite | HARD | 无 inf/-inf | factor |
| L12 | factor_nan_rate | SOFT | NaN 率 <= 10% | factor |
| L13 | ticker_format | HARD | `^([036]\d{5}\.(SH\|SZ)\|(43\|83\|87\|92)\d{4}\.BJ)$` | stock / daily_status / stock_basic |
| L14 | date_format | HARD | ISO 8601 | all |
| L15 | enum_values_valid | HARD | 枚举列匹配 Schema 允许集合 | stock_basic, corporate_actions, tabular |
| L16 | date_columns_valid | HARD | 非空日期/时间列可合法解析 | stock_basic, corporate_actions, tabular |
| L17 | equity_ticker_format | HARD | A 股 + 场内 ETF/LOF 代码正则 | corporate_actions |
| L18 | non_negative_nullable | HARD | 可空数值列满足正/非负与有限性 | corporate_actions, daily_status |
| L19 | etf_ticker_format | HARD | `^(5[123568]\d{4}\.SH\|15\d{4}\.SZ)$` | etf_bars_daily, etf_bars_minute |
| L20 | price_limit_etf | SOFT | 主板 ETF 10.5% / 科创 58 ETF 20.5% | etf_bars_daily |
| L21 | index_ticker_format | HARD | 指数代码正则（SH/SZ/BJ/CSI/CNI 等） | index_bars_daily |
| L22 | ohlc_relation_soft | SOFT | 指数严格 `low <= open,close <= high` 告警 | index_bars_daily |
| L23 | ohlc_relation_rel_tol | HARD | 指数 `low <= high` 且越界相对差 <= 1% | index_bars_daily |
| L24 | futures_ticker_format | HARD | 期货合约代码正则 | futures_bars_daily |
| L25 | futures_settle_oi_valid | HARD | settle > 0 且 open_interest >= 0 | futures_bars_daily |
| L26 | schema_contract_valid | HARD | Schema 驱动代码/枚举/数值约束契约 | tabular |
| L27 | daily_status_boolean_flags | HARD | `cn_stock_daily_status` 7 个布尔状态列非空且为 bool | daily_status |

### 一致性 K01–K15：

| ID | 检查名 | 严重度 | 阈值 | 适用 |
|---|---|---|---|---|
| K01 | open_gap_check | SOFT | 非除权日复权 open/昨收缺口 <= 10% | daily |
| K02 | volume_amount_match | HARD | volume > 0 <-> amount > 0 | daily, minute |
| K03 | suspend_consistency | HARD | 停牌 -> volume = 0 且 OHLC 相等 | daily |
| K04 | limit_flag_consistency | SOFT | limit_up -> close=high; limit_down -> close=low | daily |
| K05 | adjfactor_stable | HARD | 非公司行动日 adjfactor 不变 | daily |
| K06 | daily_vs_minute_close | HARD | 日线 close = 末根分钟 close (+/- 0.01) | daily+minute |
| K07 | daily_vs_minute_volume | HARD | 日线 volume = 分钟 volume 之和 (<= 0.1%) | daily+minute |
| K08 | daily_vs_minute_hl | HARD | 日线 high/low 包含分钟极值 | daily+minute |
| K09 | factor_cross_section | SOFT | 截面 std > 1e-8 且 abs(mean) < 100 | factor |
| K10 | industry_universe_match | HARD | universe 集合 = industry 集合 | reference |
| K11 | fund_vs_daily_tickers | SOFT | 基本面与日线标的对等差集 <= 5 | fund |
| K12 | value_distribution_shift | SOFT | 跨日核心数值均值偏移 <= 10% | daily |
| K13 | unique_key_no_duplicates | HARD | Schema 主键无重复行 | stock_basic, corporate_actions, tabular |
| K14 | date_order_valid | HARD | `delist_date >= list_date`（非空时） | stock_basic |
| K15 | daily_status_consistency | HARD | `up_limit >= down_limit`、触板及停牌自洽 | daily_status |

### 时序性 T01–T12：

| ID | 检查名 | 严重度 | 阈值 | 适用 |
|---|---|---|---|---|
| T01 | daily_freshness | HARD | 最新 trade_date 距今 <= 1 交易日 | daily, fund, daily_status |
| T02 | minute_freshness | HARD | 同上 | minute |
| T03 | no_future_dates | HARD | 所有 trade_date <= today | daily, minute, fund, factor, reference |
| T04 | date_ascending | HARD | 分区内严格递增无重复 | daily, minute, fund, calendar |
| T05 | minute_timestamp_order | HARD | ticker x date 内分钟戳严格递增连续 | minute |
| T06 | adjfactor_idempotent | SOFT | 重入库 adjfactor 不变 | daily |
| T07 | factor_lag | SOFT | D 日数据 D+2 前到达 | factor |
| T08 | calendar_forward | SOFT | 日历向前延伸 >= 30 交易日 | calendar, reference |
| T09 | late_correction | SOFT | 历史数值变动 > 0.01% 即告警 | daily, fund |
| T10 | no_future_ref_date | HARD | `list_date <= today` | stock_basic |
| T11 | no_future_ex_date | SOFT | 未来除权日公告仅软标记（不拒收） | corporate_actions |
| T12 | no_future_primary_date | SOFT | 通用表主日期晚于 today 仅软告警 | tabular |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 14 组 69 条四维规则覆盖 52/52 规范表 | 仅覆盖 4 张股票核心表 | 消除了其余 48 张表裸奔写入无 DQC 门禁与无报告审计的缺口 |
| YAML（简单规则）+ Python 函数（复杂/Schema 驱动规则）混合 | 纯 pandera Python / 纯 YAML | 改阈值不改代码，又可借助 `SCHEMA[table]` 自动驱动 `tabular` 组 |
| semver 版本化（patch=阈值 / minor=加规则 / major=改语义） | 无版本 | 结果带 `rule_version`，可审计 |
| 历史数据**不自动重跑**，CLI `revalidate` 按需 | 规则一改就全量重验 | 阈值放宽旧数据必过、收紧追溯意义不大 |
| 入库时 HARD 阻断 + SOFT 记录；`SOFT` 永不丢行或拒批 | 统一拒收 | 保留真实现实极值与可追踪审计位掩码 |

## 实现要点

详见 [data/dqc/rules/](../../../data/dqc/rules/) · [data/dqc/checks/](../../../data/dqc/checks/)

核心约定:
- 简单规则在 YAML 声明 (`column_check`)，复杂规则与 Schema 驱动校验在 `checks/custom_checks.py` 和 `checks/cross_checks.py` 实现，集中阈值覆盖配置在 [config/dqc_thresholds.yaml](../../../config/dqc_thresholds.yaml)。
- K06–K08 跨粒度检查支持 `cn_stock_bars_*` 与 `cn_etf_bars_*`。
- CLI 按需重验: `python -m data.dqc.report revalidate --source ... --table ... --start ... --end ...`
