# 02. A 股基础数据与交易日历

[← 返回目录](README.md) · 对应官网「股票数据 → 基础数据」（`doc_id=24`）

---

## 1. 接口清单（13 项）

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分/权限 & 单次上限 |
|---|---|---|---|---|---|
| `25` | 股票列表 | `stock_basic` | `ts_code`, `name`, `market`, `list_status`, `exchange`, `is_hs` | 17 列：`ts_code`, `symbol`, `name`, `area`, `industry`, `fullname`, `market`, `exchange`, `list_status`, `list_date`, `delist_date`, `is_hs` | 2,000 分（50 次/分）；6,000 行/次（全市场一次拉完建议本地缓存） |
| `329` | 每日股本（盘前） | `stk_premarket` | `ts_code`, `trade_date`, `start_date`, `end_date` | 7 列：`trade_date`, `ts_code`, `total_share`, `float_share`, `pre_close`, `up_limit`, `down_limit` | **独立权限**（勾选“盘前股本”，500/年）；8,000 行/次 |
| `26` | 交易日历 | `trade_cal` | `exchange`, `start_date`, `end_date`, `is_open` | 4 列：`exchange`, `cal_date`, `is_open`, `pretrade_date` | 2,000 分 |
| `397` | ST 股票列表 | `stock_st` | `ts_code`, `trade_date`, `start_date`, `end_date` | 5 列：`ts_code`, `name`, `trade_date`, `type`, `type_name` | 3,000 分起 |
| `423` | ST 风险警示板股票 | `st` | `ts_code`, `pub_date`, `imp_date` | 7 列：`ts_code`, `name`, `pub_date`, `imp_date`, `st_type`, `st_reason`, `st_explain` | 6,000 分；1,000 行/次 |
| `398` | 沪深港通股票列表 | `stock_hsgt` | `ts_code`, `trade_date`, `type`, `start_date`, `end_date` | 6 列：`ts_code`, `trade_date`, `type`（`HK_SZ`/`SZ_HK`/`HK_SH`/`SH_HK`）, `name`, `type_name` 等 | 3,000 分起 |
| `100` | 股票曾用名 | `namechange` | `ts_code`, `start_date`, `end_date` | 6 列：`ts_code`, `name`, `start_date`, `end_date`, `ann_date`, `change_reason` | 基础积分 |
| `112` | 上市公司基本信息 | `stock_company` | `ts_code`, `exchange` | 18 列：`ts_code`, `com_name`, `com_id`, `exchange`, `chairman`, `manager`, `secretary`, `reg_capital`, `setup_date`, `province`, `main_business` 等 | 120 分起 |
| `193` | 上市公司管理层 | `stk_managers` | `ts_code`, `ann_date`, `start_date`, `end_date` | 12 列：`ts_code`, `ann_date`, `name`, `gender`, `lev`, `title`, `edu`, `national`, `birthday`, `begin_date` 等 | 2,000 分起（5,000+ 高频） |
| `194` | 管理层薪酬和持股 | `stk_rewards` | `ts_code`, `end_date` | 7 列：`ts_code`, `ann_date`, `end_date`, `name`, `title`, `reward`, `hold_vol` | 2,000 分起（5,000+ 高频） |
| `375` | 北交所新旧代码对照 | `bse_mapping` | `o_code`, `n_code` | 4 列：`name`, `o_code`, `n_code`, `list_date` | 2,000 分；1,000 行/次（总量 < 300） |
| `123` | IPO 新股上市 | `new_share` | `start_date`, `end_date` | 12 列：`ts_code`, `sub_code`, `name`, `ipo_date`, `issue_date`, `amount`, `market_amount`, `price`, `pe`, `limit_amount` 等 | 120 分起；2,000 行/次，每日 19:00 更新 |
| `262` | 股票历史列表 | `bak_basic` | `trade_date`, `ts_code` | 24 列：`trade_date`, `ts_code`, `name`, `industry`, `area`, `pe`, `float_share`, `total_share`, `total_assets`, `liquid_assets` 等 | 5,000 分；7,000 行/次（含每日历史截面基本面） |

---

## 2. 常用枚举与使用要点

- **交易所代码（`exchange`）**：`SSE`（上交所）、`SZSE`（深交所）、`BSE`（北交所）。`stock_basic` 默认仅返回 `list_status='L'`（上市），回测避免幸存者偏差需同时拉取 `'D'`（退市）与 `'P'`（暂停上市）及 `'G'`/`'UN'`。
- **市场板块（`market`）**：`主板` / `创业板` / `科创板` / `CDR` / `北交所`。
- **沪深港通标识（`is_hs`）**：`N`（否）、`H`（沪股通）、`S`（深股通）。
- **盘前涨跌停与股本**：`stk_premarket` 为独立权限，与盘后 `stk_limit` 不同，可在开盘前提前拿到当日涨跌停价与流通/总股本；未开通时盘后用 `daily_basic` + `stk_limit`。
- **与本平台联动**：交易日历 `trade_cal` 在 [`config/source_priority.yaml`](../../../config/source_priority.yaml) 中作为 `reference.calendar` 的主数据源（`priority: [tushare, akshare]`）。
