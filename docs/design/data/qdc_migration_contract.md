# QDC 落盘布局与字段映射契约（M1）

状态：**binding**。M2–M5 依本契约落盘，互不打架；M6 按 manifest 对账。
源 `/Users/nachuan/Documents/raw/QDCData` 只读；`data/storage_schema.py` 是 schema 权威，本契约**不修改它**。
契约表示不了真实数据时：拒收计数 + 登记 §8 缺口 + 上报，**不静默改口径**。

- 落盘根（下称 `L`）：`/Users/nachuan/Documents/Data`
- 源（下称 `QDC`）：154GB、1401 分卷、45 数据集；batch-1 覆盖其中 35 个子表（日/频段，不含分钟与快照行情）
- 契约版本：`qdc-m1-v1`（manifest 逐表记录该字符串）

---

## 1. 权威与边界

| 事项 | 裁定 |
|---|---|
| 平台表结构 | `data/storage_schema.py` 的 pandera 契约，一一对应 `PARTITION_PATHS`；只读 |
| ticker 口径 | `core/asset_rules.py`：`{6位代码}.{SH\|SZ\|BJ}`，前缀与后缀互证（PREFIX_RULES / PREFIX_EXCHANGE） |
| 日期口径 | 一律落 `DATE`；解析后**断言无 NaT** |
| 复权 | 平台存不复权价 + 累计 `adjfactor`；qfq 派生不落盘（§5 I-1） |
| 交易日历 | `trade_calendar` 恒空（§5 I-2） |
| 表示不了的数据 | §8 缺口登记 + manifest 拒收桶计数；改 schema 另开 issue，由人裁定 |
| 生产写 | 本契约及 M2–M5 不含生产数据仓写；如需 `prod_data_write`，先取记录在案的人工批准 |

---

## 2. 落盘布局（schema 层 + ext 层 + manifest）

### 2.1 schema 层——pandera 可校验的 11 张表

路径与 `data/storage_schema.py::PARTITION_PATHS` 逐字一致。batch-1 落其中 5 张；其余 6 张留给后续批次，路径先占位不落数。

| 平台表 | 落盘路径（相对 `L`） | batch-1 写者 | 源 |
|---|---|---|---|
| bars_daily | `asset/bars_daily/{date}.parquet` | M2（价量）+ M3（adjfactor 更新） | cn_daily_full、cn_etf_1d、cn_adj_factor |
| fundamental | `asset/fundamental/{date}.parquet` | M5（股票域）+ M2（基金域） | cn_valuation_1d、imai_etf、cn_etf_1d（close 派生） |
| calendar | `asset/calendar/{exchange}.parquet` | M4（**只写 manifest，不落行**） | ——（§5 I-2） |
| industry | `asset/reference/industry.parquet` | M4 | cn_sw_index/index_member_history |
| corporate_actions | `asset/reference/corporate_actions.parquet` | M3 | cn_dividend |
| bars_minute / bars_daily_futures / tick_futures_l2 / factor / contract_specs / continuous | 同 `PARTITION_PATHS` | 非 batch-1 | —— |

`{date}` 为交易日 `YYYY-MM-DD`；一文件一交易日（bars_daily / fundamental）。
`{exchange}` 为交易所代码（calendar，batch-1 恒空）。

### 2.2 ext 层——schema 表示不了、但必须落盘留痕的源表

```
L/ext/<dataset>/<subtable>/<volume>.parquet
```

`<volume>` 与源分卷同名（`all`、`2024`、`2026-01` …），一源卷一落盘卷，机械对应、便于对账。
ext 层**不接 pandera**（`storage_schema.py` 不扩表）；接 §3 通用归一 + §4.6 逐子表契约 + 落盘时断言。
ext 层覆盖：财务三表/财务指标、预期/分部/评级、股本明细、ST/停复牌区间、指数成分与行业快照、回购/增减持/质押/解禁事件。
每张 ext 表在 §8 登记 schema 扩展后续项；扩展落地前 ext 是唯一落点。

### 2.3 manifest——每表一份，M6 对账依据

```
L/manifest/<table>.json          # schema 层：bars_daily.json …
L/manifest/ext_<dataset>_<subtable>.json
```

```json
{
  "contract": "qdc-m1-v1",
  "table": "bars_daily",
  "tier": "schema",
  "writer_batch": "M2",
  "sources": [
    {"dataset": "cn_daily_full", "subtable": "full",
     "volume_paths": ["cn_daily_full/full/2012.parquet", "…源分卷实录，不推断"]}
  ],
  "volumes": [
    {"source_volume": "cn_daily_full/full/2012.parquet",
     "rows_in": 0, "rows_out": 0, "rejected": 0}
  ],
  "row_count": 0,
  "ticker_count": 0,
  "date_range": ["YYYY-MM-DD", "YYYY-MM-DD"],
  "pk_unique": true,
  "checks": {"date_dtype": "DATE", "nat_free": true,
             "columns_match_schema": true, "schema_readback": "pass"},
  "quality_flag": {"clean": 0, "bits": {"C02_COVERAGE": 0, "C05_NULL_RATE": 0,
                   "L04_BEYOND_LIMIT": 0, "L07_ADJFACTOR_RANGE": 0, "K01_OVERNIGHT_GAP": 0}},
  "rejections": [{"reason": "ticker_prefix_unmapped", "prefix": "92", "rows": 0}],
  "derivations": [{"id": "D-4", "rows_defaulted": 0}]
}
```

规则：

- `volumes[]` 逐**源分卷**记 `rows_in / rows_out / rejected`，三者恒等 `rows_in = rows_out + rejected`。
- `sources[].volume_paths` 是源盘实测路径（源的目录深度不统一：单子表集如 `imai_etf/full/all.parquet`，多子表集如 `cn_sw_index/index_member_history/full/all.parquet`），**不许按模板推断**。
- ext 层 manifest 的 `checks` 用 §4.6 的逐表断言结果替换 `schema_readback`（`"contract_asserts": "pass"`）。
- manifest 与数据同批原子写（§7）；M6 只认 manifest，不认口头数字。

### 2.4 行序、列序、写参

- 行序：schema 层按 `(ticker, trade_date)` 升序；参考表按 `(ticker, …键)` 升序；ext 层按其键列升序。
- 列序：schema 层与 pandera 列序一致；ext 层 = 键列在前，其余按源列序。
- 写参：`data/storage_ops.py::PARQUET_WRITE_CONFIG`；原子写用 `write_parquet_atomic`。
- 数值列不四舍五入、不截断；`DOUBLE → int64` 只在断言整数后转换（R3）。

---

## 3. 通用归一规则（所有表，含 ext）

**R1 ticker 归一。** 所有 equity 键列（`symbol` / `ts_code` / `code` / `stock_code` / `seccode` / `component_code` / `SECUCODE` 等，逐表见 §4）→ 平台 `ticker`：

1. `NNNNNN.XSHG → NNNNNN.SH`，`.XSHE → .SZ`，`.XBSE → .BJ`；
2. 已是 `NNNNNN.SH|SZ|BJ` 的原样通过（cn_st、cn_suspend 等 tushare 形态）；
3. 裸 6 位代码：按 `core/asset_rules.PREFIX_RULES` 定交易所（`code`+`exchange` 列同表出现时，二者归一后**断言一致**）；
4. 不能归一或前缀未在 PREFIX_RULES 注册 → 整行拒收，manifest `rejections[].reason = ticker_prefix_unmapped` 记前缀与行数（§8 G2）；
5. 同表两个键列指向同一标的（如 index_member 的 `stock_code` 与 `symbol`）：归一后**断言相等**，落 `ticker`，另一列丢弃。

**R2 日期归一。** 一切日期列 → `DATE`，解析后**断言无 NaT**（QUANT-19 缺陷类）：

- `ts`（BIGINT epoch 毫秒）：断言 `ts % 86400000 == 0`，取 UTC 日历日；
- `DATE` 列：原样；
- `VARCHAR`：接受 `YYYY-MM-DD` 与 `YYYYMMDD` 两种实测形态（cn_valuation_1d 为前者，index_member_history 为后者），其余形态**先拒收上报**，不猜；
- 解析出 NaT 或字面 `NaT`/空串 → 拒收计数（`reason = date_parse_failed`）；
- `TIMESTAMP` 列（`updated_at`、`ingested_at`）→ naive `datetime64[ns]`（北京时间口径），不改列名。

**R3 整型转换。** `DOUBLE → int64` 前断言逐值整数；非整数**拒收**，不截断。适用于 `volume`、`total_shares`、`float_shares`。

**R4 拒收不静默。** 任何拒收都进 manifest `rejections[]`，带 `reason`、行数、（可得时）样本键。禁止静默丢行。

**R5 值域断言。** schema 的 `gt0/ge0/nullable/isin` 约束落盘前自检（pandera 读回兜底）；违反 → 拒收计数，**不改 schema、不改值**。

---

## 4. 表级字段映射

派生编号 D-1…D-8 见各表；缺口编号 G1…G10 见 §8。

### 4.1 `bars_daily` ← cn_daily_full（M2）+ cn_etf_1d（M2）+ cn_adj_factor（M3）

| 平台列 | 源 | 变换 / 派生 | 缺口 |
|---|---|---|---|
| ticker | symbol | **D-1** R1：`.XSHG→.SH` 等；不能归一拒收 | G2 |
| trade_date | ts | **D-2** R2：epoch 毫秒 → DATE，断言整日对齐 | |
| open, high, low, close | 同名 | 原样（DOUBLE，schema `gt0`） | |
| volume | volume | **D-3** R3：DOUBLE→int64，断言整数 | |
| amount | amount | 原样（DOUBLE，`ge0`） | |
| adjfactor | cn_adj_factor.hfq_factor | **D-4**（§5 I-1）：按 ticker 对交易日向前填充（含 1900-01-01 基准行）；无任何因子行的 ticker → 1.0 并计数 | |
| turnover | 无源 | NULL（schema 可空例外） | **G1** |
| suspend | 无源 | False | **G5** |
| is_limit_up, is_limit_down | 无源 | False | **G5** |
| quality_flag | — | **D-6**（§6） | |

- cn_daily_full 覆盖 A 股（含退市）；cn_etf_1d 覆盖 ETF——两源同入一张 bars_daily，行域天然不交（前缀不同），写入仍走 §7 upsert。
- `source` 列（数据来源标签）→ 丢弃，来源记入 manifest `sources[]`。
- 涨跌停/停牌三列的 `False` 是**源限制不是市场事实**（源只在交易日出 bar；`is_limit_*` 源根本不给），沿 `scripts/bootstrap/_common.py` 先例；任何人不得据这三列推断涨跌停/停牌。

### 4.2 `fundamental` ← cn_valuation_1d（M5，股票域）+ imai_etf（M2，基金域）

两写者、行域不交（前缀域断言：M5 只写股票前缀行，M2 只写 ETF 前缀行），冲突即停（§7）。

| 平台列 | cn_valuation_1d 源 | imai_etf 源 | 缺口 |
|---|---|---|---|
| ticker | symbol（`600519.XSHG`→`600519.SH`）D-1 | code + exchange（SSE/SZSE）D-1，断言前缀互证 | G2 |
| trade_date | trade_date（VARCHAR→DATE，R2 断言无 NaT） | date（DATE 原样） | |
| total_market_cap | market_cap | **D-7** shares × close（同日 cn_etf_1d）；无同日 close → 拒收计数（实测 2024 年 ETF 筛后 2.0%） | **G8** |
| float_market_cap | circulating_market_cap | **D-7** 同上（ETF 全流通，与 total 同值） | G8 |
| pe_ttm | pe_ttm | NULL | |
| pe_lyr | pe_latest_yr（市盈率（最新年报）） | NULL | **G9** |
| pb | pb | NULL | |
| ps_ttm | ps_ttm | NULL | |
| total_shares | total_shares（R3 → int64） | shares（R3 → int64） | |
| float_shares | circulating_shares（R3 → int64） | shares（同 total） | |
| quality_flag | D-6 | D-6 | |

- imai_etf 行域 = 数据字典的 ETF 筛选：`exchange='SSE' OR (exchange='SZSE' AND etf_type='ETF' AND code NOT IN ('159002','159004','159006'))`。筛选外的 LOF/SF/CEF/空类型行**不入表**，manifest 计数（实测筛后 1,566,726 行 / 全表 2,034,443 行）。
- cn_valuation_1d 其余 33 个估值列 + `close` + 2 个 status 字符串列 → 丢弃（**G9** 登记，不落 ext——`close` 归 bars_daily，估值扩展列等 schema 扩展）。
- imai_etf 的 `name` / `etf_type` / `source` → 丢弃（**G9** 登记：基金身份属性无平台列）。

### 4.3 `corporate_actions` ← cn_dividend（M3）

**D-8 行拆分**：一行源按金额拆至多 3 行落盘，键 `(ticker, ex_date, event_type)` 天然不交：

| event_type | 触发 | 金额映射 |
|---|---|---|
| `dividend` | `cash > 0` | `cash_dividend ← cash`（每 10 股派息） |
| `bonus` | `bonus_share > 0` | `stock_dividend ← bonus_share`（每 10 股送股） |
| `split` | `convert_share > 0` | `stock_dividend ← convert_share`（每 10 股转增；enum 无 `convert`，映射裁定见 **G7**） |
| `rights` | 不产生 | 无配股源字段（**G7**） |

| 平台列 | 源 | 变换 | 缺口 |
|---|---|---|---|
| ticker | symbol | D-1 | G2 |
| ex_date | ex_date | R2；NULL → **整行拒收**（实测 669/57,576 = 1.2%） | |
| event_type | — | D-8 拆分 | G7 |
| announce_date | disclosure_date | R2；NULL 保留 NULL（可空） | |
| record_date | record_date | R2；NULL 保留 NULL | |
| detail | 未映射列 | **D-8**：`k=v` 以 `;` 连接（`report_period`、`plan_date`、`bonus_total`、`div_yield`、`total_shares`、`progress`）；恒非空，空则 `src=cn_dividend` | |
| cash_dividend | cash | 仅 dividend 行；其余行 NULL | |
| stock_dividend | bonus_share / convert_share | 仅 bonus / split 行；其余行 NULL | |
| rights_price, rights_ratio | 无源 | NULL | G7 |

`plan_date` 与 `report_period` 原值保留在 `detail` 里（“别合并成一个日期”）。

### 4.4 `industry` ← cn_sw_index/index_member_history（M4，`is_new='Y'` 当期行）

| 平台列 | 源 | 变换 | 缺口 |
|---|---|---|---|
| ticker | ts_code（平台形态 `600359.SH`）/ symbol（源形态 `600359.XSHG`）——同标的两种形态，**归一后断言相等** | D-1 | |
| industry_l1, industry_l2, industry_l3 | l1_name, l2_name, l3_name | 原样；`is_new='Y'` 行断言三列非空（实测 5,920 行 0 空） | |
| classification | — | 固定 `'sw'` | **G6** |
| effective_date | in_date（VARCHAR `YYYYMMDD`→DATE） | R2 | |

- 只存当期（设计裁定“初版只存最新”）；`is_new='N'` 历史行（2,006 行）与 `cn_sw_index/index_member`（变动时点快照）→ ext 层，两源语义不同**不合并**。
- 断言：`is_new='Y'` ⇔ `out_date IS NULL`（实测成立，5,920/5,920）。
- cn_industry_tdx（通达信分类）→ ext 层：`classification` enum 只有 `citics|sw`，tdx 无处安放（**G6**）；batch-1 无 citics 源（**G6**）。

### 4.5 `calendar` —— 恒空（§5 I-2）

M4 只写 manifest（`row_count: 0`），不落任何行。

### 4.6 ext 层逐子表契约

通用（E-1…E-3）：E-1 equity 键列 → `ticker`（R1）；`index_code` 等指数标识**原值保留**（不是 equity ticker）。E-2 日期列 → DATE（R2）；`TIMESTAMP` 列转 naive `datetime64`。E-3 未列名的其余列**全部保留**源名源型（金融三表 2,402 列按此机械处理，宽表行业空列是正常形态不算缺失）；列名语言（英文大写/中文）原样。

| ext 子表 | ticker 键 | 日期列（→DATE） | 丢弃列 | 键 / 断言 |
|---|---|---|---|---|
| cn_float_shares/full | symbol | trade_date（断言与 ts 同日） | ts（epoch 重复）、`source` 保留 | 唯一 `(ticker, trade_date)`；`free_float_shares` 全保留（**G3**） |
| cn_st/full | ts_code | st_start, st_end(可空) | — | 唯一 `(ticker, st_start)`；`st_start ≤ st_end`（空=仍在戴帽） |
| cn_suspend/full | ts_code | suspend_date, resume_date(可空) | — | 唯一 `(ticker, suspend_date)`；`suspend_date ≤ resume_date` |
| cn_index_cons/full | component_code | trade_date | component_name, component_name_en | 唯一 `(trade_date, index_code, ticker)` |
| cn_industry_tdx/full | symbol | snapshot_date | code6 | 唯一 `(ticker)`（当期快照） |
| cn_sw_index/index_member | symbol（=stock_code 断言） | snapshot_date, included_date | stock_code, stock_name | 唯一 `(snapshot_date, index_code, ticker)` |
| cn_sw_index/index_member_history | symbol（=ts_code 断言） | in_date, out_date(可空, `YYYYMMDD`) | ts_code, name | 唯一 `(ticker, l3_code, in_date)`；`out_date IS NULL ⇔ is_new='Y'` |
| cn_sw_index/index_daily | —（index_code 原值） | trade_day | — | 唯一 `(index_code, trade_day)` |
| cn_sw_index/index_valuation | —（index_code 原值） | trade_day | — | 唯一 `(index_code, trade_day)` |
| cn_sw_index/index_info | —（index_code 原值） | — | — | 唯一 `(index_code)`（静态名录） |
| cn_equity_events/buyback | symbol | repurchase_start_date, latest_announcement_date, snap_date | name | 事件流水，不作唯一断言；`(ticker, snap_date)` 非空 |
| cn_equity_events/holder_change | symbol | change_start_date, change_end_date, announce_date, snap_date | name | 事件流水；`(ticker, snap_date)` 非空 |
| cn_equity_events/pledge | symbol | trade_date, pledge_date | name | 事件流水；`(ticker, trade_date)` 非空 |
| cn_equity_events/repurchase | ts_code | ann_date, end_date, exp_date | — | 事件流水；`(ticker, ann_date)` 非空 |
| cn_equity_events/stk_holdertrade | ts_code | ann_date | —（`holder_name` 是股东名=内容，保留） | 事件流水；`(ticker, ann_date)` 非空 |
| cn_restricted_release/restricted | symbol | unlock_date | name | 唯一 `(ticker, unlock_date, seq_no)` |
| cn_restricted_release/share_float | ts_code | ann_date, float_date | — | 事件流水；`(ticker, float_date)` 非空 |
| cn_fin_statements/income, balance, cashflow, income_rev | symbol | report_date, notice_date, UPDATE_DATE | SECUCODE, SECURITY_CODE, SECURITY_NAME_ABBR, ORG_CODE, REPORT_DATE_1（与 report_date 重复的源形态日期） | 唯一 `(ticker, report_date, REPORT_TYPE)` |
| cn_fin_statements/income_delisted, balance_delisted, cashflow_delisted | symbol | 报告期 | symbol6 | 唯一 `(ticker, 报告期)`；中文列名原样保留（E-3） |
| cn_financial_indicator/full | SECUCODE | REPORT_DATE, NOTICE_DATE, UPDATE_DATE | SECURITY_CODE, SECURITY_NAME_ABBR, ORG_CODE | 唯一 `(ticker, REPORT_DATE, REPORT_TYPE)` |
| cn_estimates/full | stock_code | updated_at（TIMESTAMP→naive） | — | 唯一 `(ticker, fiscal_year)` |
| cn_revenue_segment/full | symbol | report_date | — | 分部明细，不作唯一断言；`(ticker, report_date)` 非空 |
| cn_analyst_rating/cninfo_rating_obs | seccode（裸 6 位，PREFIX_RULES 定所） | declare_date, ingested_at（TIMESTAMP→naive） | secname | 唯一 `(obs_key)` |
| cn_analyst_rating/report_rc | ts_code | report_date | name | 研报流水，不作唯一断言；`(ticker, report_date)` 非空 |

注：

- 金融三表主表已有小写 `symbol` / `report_date` / `notice_date`（源内已归一形态）与原始 EC 标识块并存——按表内映射取小写列为键与日期，标识块整列丢弃。
- imai_etf 的 `code` 可带历史后缀（如 `164814!1`，实测 2,612 行）——后缀码不得去后缀合并；本批 ETF 筛选后 0 行受影响（后缀码全在筛选外）。
- `cn_valuation_1d` 不进 ext（§4.2 直入 fundamental）；`cn_adj_factor` / `cn_dividend` / `cn_daily_full` / `cn_etf_1d` 同理。

---

## 5. 两条不变量（可判定表述）

### I-1 `adj_factor` 累计、锚定在链首；qfq 只派生不落盘

**陈述。** 落盘的 `bars_daily.adjfactor` 是逐 ticker 的**累计复权因子**：`cn_adj_factor.hfq_factor` 按交易日前向填充（源只在除权除息日出行，含 `d=1900-01-01、factor=1.0` 的基准行，数据字典记 5,499 行）；基准行之前（首行之前）取 1.0。锚定约定 = **链首 1.0**（后复权口径，`hfq = raw × adj`），与 `data/calendar_adjustment.py::build_continuous` 的 kb-K4 约定一致。**qfq 永不落盘**；读取时派生：

```
qfq = raw × adj / adj_latest     # adj_latest = arg_max(adjfactor, trade_date)，全历史口径
hfq = raw × adj / adj_first      # adj_first = 链首因子（本契约下 = 1.0）
```

与 `data/data_api.py::_adjust` 同式。源 `hfq_factor` **原样落盘**，不做再归一（QUANT-32 裁定）。

**三条可判定检查**（M3 落盘后逐项跑，结果进 manifest `checks`）：

1. **形状检查**：落盘列集合 == schema 列集合；任何一列叫 `qfq_factor` / 复权价 → 违约。
2. **锚定检查**（逐 ticker，容差 1e-6）：
   a. 经读路径派生的最新一根 bar 的 qfq == 该 bar 的 raw close；
   b. 该 ticker 首根 bar 的 ffill 因子 == 1.0。
   2b 不满足且该 ticker 无基准行（1900-01-01 或显式 1.0 首行）→ **判锚定错误：该 ticker 的 adjfactor 行整ticker拒收**（`reason = adjfactor_anchor_unverified`），**禁止再归一**（不许除以首/尾因子强行造 1.0——那会静默改写 raw×adj 的后复权关系）。
3. **派生一致性检查**（有界抽样）：契约式 qfq 与 `data_api` 读路径 qfq 逐值相等，容差 1e-6（同 frame、同 `adj_latest` 口径）。

**已观测形态**（QDC 实测，供 M3 校验比对）：因子链 99.6% 单调不减，末值中位数约 2.52，最大 10,054（>1000 的行按 §6 L07 置位，是标记不是拒绝）。手算样例 A（§9）给出完整数值。

### I-2 `trade_calendar` 恒空

**陈述。** batch-1 全程 `asset/calendar/` **不落任何行**：不从 bars 推导交易日历，不从任何渠道下载，M4 的任务里**没有**落 calendar 行这一步。

**可判定检查**（M6 对账项）：

1. `L/asset/calendar/` 下任一 parquet 行数为 0（或目录不存在）；
2. manifest `calendar.json` 的 `row_count == 0`；
3. 代码侧：M2–M5 的 diff 中不出现任何写 calendar 的调用。

违反任一条即违约。缺日历导致的后果（非交易日行、涨跌停基准缺口）由使用者在读侧承担，不以此为由在本批补日历。

---

## 6. `quality_flag` 接线

**定义**（`data/storage_schema.py`）：`int64` 位掩码，5 位，`CLEAN = 0`，值域 `< 32`。位名封闭：

| 位 | 名称 | 值 |
|---|---|---|
| 0 | C02_COVERAGE | 1 |
| 1 | C05_NULL_RATE | 2 |
| 2 | L04_BEYOND_LIMIT | 4 |
| 3 | L07_ADJFACTOR_RANGE | 8 |
| 4 | K01_OVERNIGHT_GAP | 16 |

**接线**（行级点亮；规则语义以 `data/dqc/rules/*.yaml` 为准，此处只定“结果写到哪”）：

| 位 | DQC 规则 | 行级点亮条件 | 适用表 |
|---|---|---|---|
| C02_COVERAGE | row_count_vs_universe（SOFT, min_ratio 0.95） | 该 `trade_date` 的覆盖率 < 0.95 时，**当日全部行**置位 | bars_daily, fundamental |
| C05_NULL_RATE | null_soft_fields（SOFT, ≤1%） | 行内在软字段（amount/adjfactor/turnover/pe_ttm/pe_lyr/pb/ps_ttm，按表列交集）有 NULL，且该 (日, 列) 空值率 > 1% 时置位 | bars_daily, fundamental |
| L04_BEYOND_LIMIT | price_limit_stock（SOFT, 板块限价 + 0.5% 容差） | 该行涨跌幅超容差上限（主板 10.5% / 创·科 20.5% / IPO 前 5 日 30.5%）置位 | bars_daily |
| L07_ADJFACTOR_RANGE | adjfactor_range（SOFT, (0.01, 1000)） | 该行 `adjfactor ∉ (0.01, 1000)` 置位（QDC 链最大 10,054，**必然有置位行，属预期**） | bars_daily |
| K01_OVERNIGHT_GAP | open_gap_check（SOFT, 非除权日复权缺口 ≤10%） | 非除权日 \|open_adj/prev_close_adj − 1\| > 10% 置位 | bars_daily |

规则：

- SOFT = 只置位不拒行；HARD 规则（如有）→ 拒收进 manifest 桶（R4），不是置位。
- 多位命中按位或合并；干净行 = 0。
- **不扩位**：不新增 `QUALITY_FLAG_BITS` 名字，不复用未定义位；要新规则先改 `data/storage_schema.py`（另开 issue，本契约禁止）。
- 有 `quality_flag` 列的只有 `bars_daily` 与 `fundamental`（schema 定义如此）；calendar/industry/corporate_actions/ext 无此列。
- manifest 逐位计数（§2.3），M6 对账位计数之和与行数关系。

---

## 7. 写协议

1. **原子写**：`data/storage_ops.py::write_parquet_atomic` + `PARQUET_WRITE_CONFIG`；同表并发写者走 2–4。
2. **PK-upsert**：键 = schema 的唯一约束（bars_daily/fundamental `(ticker, trade_date)` 等）。更新 adjfactor（M3）= 读-并-写已有键行；插入 = 新键行。写前读回旧文件按 PK 并集，写后断言 `pk_unique`。
3. **写者唯一性**：一行域一个写者。fundamental 的股票域（M5）与基金域（M2）按 ticker 前缀域断言不交；发现同键两写者 → **立即停手上报**，不选边。
4. **读回校验**：schema 层每次写后 `SCHEMA[table].validate(frame)` 必须过（`coerce=False, strict=True`）；ext 层跑 §4.6 逐表断言。失败 → 该卷拒收计数，不改值凑合。
5. **manifest 同批更新**：数据落盘与 manifest 同事务语义（先写数据、再写 manifest、manifest 记的行数必须与读回行数一致）。

---

## 8. 缺口登记（显式标注；等 owner 裁决）

| # | 现象 | 契约处置 | 后续 |
|---|---|---|---|
| **G1** | `turnover` 无源 | 恒 NULL（schema 可空例外） | 换手率由读侧用 amount/市值 派生，或等新源 |
| **G2** | ticker 前缀未注册：`92*`（北交所 920 段，cn_daily_full 约 35 万行）、`52/53/55*`（沪市 ETF 新段，cn_etf_1d+imai_etf 筛后 22,761 行）；`20/90*`（B 股）按 V1 既定范围排除 | 整行拒收 + manifest 按前缀计数 | **owner 决策点**：给 `core/asset_rules.PREFIX_RULES` 增加 `92→STOCK BJ`、`52/53/55→ETF SH` 是否做？做则另开 issue（含测试），M2 放行相应行；不做则这些行永久在拒收桶 |
| **G3** | `cn_float_shares.free_float_shares` 等股本细目无平台列 | 整表进 ext（保真） | schema 扩展（fundamental 加列或独立股本表）另开 issue |
| **G4** | 财务三表/财务指标/预期/分部/评级共 12 个子表无平台表 | 整体进 ext | schema 扩展另开 issue |
| **G5** | `suspend` / `is_limit_up` / `is_limit_down` 无源（源只在交易日出 bar） | 恒 `False`，沿 bootstrap 先例；**不得据以推断市场事实** | 与 G2 的 92 段、K01 基准共同影响 L04 可执行度；等交易日历/涨跌停源 |
| **G6** | `cn_industry_tdx` 是通达信分类，`classification` enum 仅 `citics\|sw`；batch-1 无 citics 源 | tdx 全表进 ext；industry 只落 sw | enum 扩展或 citics 源另开 issue |
| **G7** | `cn_dividend` 无配股字段（rights 行产不出）；enum 无 `convert`，转增映射到 `split` | rights_price/rights_ratio 恒 NULL；转增→`split`（D-8） | **owner 确认**：转增算 `split` 还是并入 `bonus`？配股源另找 |
| **G8** | imai_etf 基金域部分行无同日 cn_etf_1d close（2024 实测 2.0%），市值派生 D-7 不能算，而两市值列 schema 非空 | 整行拒收 + 计数（`reason = etf_close_missing`） | 独立基金份额表或市值列放宽另开 issue |
| **G9** | cn_valuation_1d 的 33 个估值列、`close`、2 个 status 列；imai_etf 的 `name`/`etf_type`/`source`；`pe_lyr` 只有 `pe_latest_yr` 一个近似源 | 丢弃 + manifest `dropped_columns` 记列名 | schema 加列（add-column-only 演进）另开 issue |
| **G10** | `trade_calendar` 无源且裁定不落 | 恒空（I-2），**不是缺陷是裁定** | 读侧自行承担非交易日判断 |

---

## 9. 手算样例（派生有界证明）

### 样例 A——`adjfactor` ffill 与 qfq（000001.XSHE → `000001.SZ`）

源 `cn_adj_factor/full/all.parquet` 该 ticker 共 33 行（稀疏除权日）：

| d | hfq_factor |
|---|---|
| 1900-01-01（基准） | 1.0 |
| 2024-10-10 | 106.63841922804983 |
| 2025-06-12 | 109.97957074433333 |
| 2025-10-15 | 112.30923508490183 |

- ffill：2025-06-30（无因子行）→ 取 2025-06-12 的 `109.97957074433333`。
- 取 2025 年 bars 为 frame（`adj_latest = 112.30923508490183`，即 2025-10-15 行）：
  - 2025-01-02：raw close = 11.43，ffill 因子 = 106.63841922804983
    → `qfq = 11.43 × 106.63841922804983 / 112.30923508490183 = 10.852866`
  - 2025-12-31（最新 bar）：raw close = 11.41
    → `qfq = 11.41 × 112.30923508490183 / 112.30923508490183 = 11.410000 == raw`（I-1 检查 2a）
  - 首 bar 因子 = 106.63841922804983 ≠ 1.0，但链含 1900 基准行 → I-1 检查 2b 以基准行为准（首行之前 ffill = 1.0）。

### 样例 B——imai_etf 市值派生 D-7（510010）

- `imai_etf/full/all.parquet`：`date=2024-01-03, code=510010, exchange=SSE, etf_type=单市, shares=162524400.0`
- ticker 归一：`510010` + SSE → `510010.SH`（前缀 51→ETF SH 与 exchange 列互证 ✓）
- `cn_etf_1d/full/2024.parquet` 同日 `510010.XSHG` close = `1.322`
- `total_market_cap = float_market_cap = 162524400 × 1.322 = 214857256.80`；`pe_ttm/pe_lyr/pb/ps_ttm = NULL`；`total_shares = float_shares = 162524400`

---

## 10. M2–M5 执行清单（谁写什么）

| 批次 | 源 | 落点 | 关键派生 |
|---|---|---|---|
| M2 | cn_daily_full + cn_etf_1d | bars_daily | D-1, D-2, D-3, D-4(默认), D-5, D-6 |
| M2 | imai_etf（ETF 筛） | fundamental（基金域） | D-1, D-7, D-6 |
| M3 | cn_adj_factor | bars_daily（更新 adjfactor） | D-4 + I-1 三检查 |
| M3 | cn_dividend | corporate_actions | D-8 |
| M3 | cn_equity_events(5) + cn_restricted_release(2) + cn_float_shares | ext | E-1…E-3 |
| M4 | cn_sw_index/index_member_history（is_new='Y'） | industry | §4.4 |
| M4 | cn_sw_index 其余 + cn_index_cons + cn_industry_tdx + cn_st + cn_suspend | ext | E-1…E-3 |
| M4 | —— | calendar 空 + manifest | I-2 |
| M5 | cn_valuation_1d | fundamental（股票域） | §4.2 |
| M5 | cn_fin_statements(7) + cn_financial_indicator + cn_estimates + cn_revenue_segment + cn_analyst_rating(2) | ext | E-1…E-3 |

每批交付时带 manifest + `uv run pytest`/校验脚本输出；M6 只按 manifest 对账。
