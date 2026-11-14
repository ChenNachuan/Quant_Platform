# 03. A 股行情、复权因子与停复牌

[← 返回目录](README.md) · 对应官网「股票数据 → 行情数据」（`doc_id=15`）

---

## 1. 接口清单（16 API + 复权专题说明）

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分/权限 & 单次上限 |
|---|---|---|---|---|---|
| `27` | 历史日线（未复权） | `daily` | `ts_code`, `trade_date`, `start_date`, `end_date` | 11 列：`ts_code`, `trade_date`, `open`, `high`, `low`, `close`, `pre_close`, `change`, `pct_chg`, `vol`, `amount` | 120 分试用（50 次/分）；5,000 分 500 次/分；6,000 行/次；每日 15:00–16:00 更新 |
| `372` | 实时日线 | `rt_k` | `ts_code`（支持通配/多代码） | 15 列：`ts_code`, `name`, `pre_close`, `high`, `open`, `low`, `close`, `vol`, `amount`, `num`, 盘口等 | **独立权限**（“A股日线RT”，200/月）；6,000 行/次（一次全市场） |
| `370` | 历史分钟 | `stk_mins` | `ts_code`, `freq`（`1/5/15/30/60min`）, `start_date`, `end_date` | 8 列：`ts_code`, `trade_time`, `open`, `close`, `high`, `low`, `vol`, `amount` | **独立权限**（“A股历史分钟”，2,000/年）；8,000 行/次，500 次/分，超过 10 年历史 |
| `374` | 实时分钟 | `rt_min` | `freq`（`1/5/15/30/60MIN`）, `ts_code`（逗号分隔多股） | 8 列：`ts_code`, `freq`, `time`, `open`, `close`, `high`, `low`, `vol`, `amount` | **独立权限**（“A股分钟RT”，1,000/月）；1,000 行/次，最多 300 股 |
| `457` | A 股实时分钟-日累计 | `rt_min_daily` | `freq`, `ts_code` | 9 列：`ts_code`, `freq`, `time`, `open`, `close`, `high`, `low`, `vol`, `amount` | **独立权限**（“A股分钟RT”） |
| `144` | 周线行情 | `weekly` | `ts_code`, `trade_date`, `start_date`, `end_date` | 11 列：`ts_code`, `trade_date`, `close`, `open`, `high`, `low`, `pre_close`, `change`, `pct_chg`, `vol`, `amount` | 2,000 分；6,000 行/次 |
| `145` | 月线行情 | `monthly` | `ts_code`, `trade_date`, `start_date`, `end_date` | 11 列（同周线） | 2,000 分；4,500 行/次 |
| `336` | 周/月线（每日更新） | `stk_weekly_monthly` | `ts_code`, `trade_date`, `start_date`, `end_date`, `freq` | 13 列：`ts_code`, `trade_date`, `end_date`, `freq`, `open`, `high`, `low`, `close`, `pre_close`, `vol` 等 | 2,000 分；6,000 行/次（相比 `weekly`/`monthly` 日内滚动更新） |
| `365` | 周/月线复权（每日更新） | `stk_week_month_adj` | `ts_code`, `trade_date`, `start_date`, `end_date`, `freq` | 21 列：未复权 + `open_qfq`..`close_qfq` + `open_hfq`..`close_hfq` 等 | 2,000 分；6,000 行/次 |
| `28` | 复权因子 | `adj_factor` | `ts_code`, `trade_date`, `start_date`, `end_date` | 3 列：`ts_code`, `trade_date`, `adj_factor` | 2,000 分起（5,000+ 高频）；盘前 9:15–9:20 入库 |
| `32` | 每日指标 | `daily_basic` | `ts_code`/`trade_date`（二选一）, `start_date`, `end_date` | 19 列：`close`, `turnover_rate`, `turnover_rate_f`, `volume_ratio`, `pe`, `pe_ttm`, `pb`, `ps`, `ps_ttm`, `dv_ratio`, `dv_ttm`, `total_share`, `float_share`, `free_share`, `total_mv`, `circ_mv` 等 | 2,000 分（5,000+ 无总量限制）；6,000 行/次；每日 15:00–17:00 |
| `109`/`146` | 通用/复权行情接口（SDK） | `ts.pro_bar` | `ts_code`, `start_date`, `end_date`, `asset` (`E`/`I`/`FT`/`FD`/`O`/`CB`), `adj` (`None`/`qfq`/`hfq`), `freq`, `ma`, `factors` (`tor`,`vr`), `adjfactor` | 动态拼装对应资产行情 + 均线/因子；**仅 Python SDK，不可 HTTP 直调** | 随对应资产权限；`ts_code` 不支持多值 |
| `183` | 每日涨跌停价格 | `stk_limit` | `ts_code`, `trade_date`, `start_date`, `end_date` | 7 列：`trade_date`, `ts_code`, `pre_close`, `up_limit`, `down_limit`, `asset_type`, `exchange` | 2,000 分起；5,800 行/次；每日 9:00 |
| `214` | 每日停复牌信息 | `suspend_d` | `ts_code`, `trade_date`, `start_date`, `end_date`, `suspend_type` | 4 列：`ts_code`, `trade_date`, `suspend_timing`, `suspend_type`（`S` 停牌 / `R` 复牌） | 基础积分（`31` 旧版停复牌已由 `214` 替代） |
| `48` | 沪深股通十大成交股 | `hsgt_top10` | `ts_code`, `trade_date`, `start_date`, `end_date`, `market_type` | 11 列：`trade_date`, `ts_code`, `name`, `close`, `change`, `rank`, `market_type`, `amount`, `net_amount`, `buy`, `sell` | 基础积分（`104` 为沪深股通旧分类节点） |
| `255` | 备用行情 | `bak_daily` | `ts_code`, `trade_date`, `start_date`, `end_date`, `offset`, `limit` | 31 列：`ts_code`, `trade_date`, `name`, `pct_change`, `close`, `change`, `open`, `high`, `low`, `pre_close`, 振幅/量比/均价等 | 5,000 分；7,000 行/次 |

---

## 2. 本平台标准化与单位契约

[`TushareAdapter`](../../../data/adapters/tushare_adapter.py) 拉取未复权 `pro.daily(trade_date=...)` 后，由 [`tushare_normalizer.normalize()`](../../../data/normalizers/tushare_normalizer.py) 转为统一 `bars_daily` schema：

| Tushare 原始字段 | 原始口径 / 单位 | 平台标准字段 | 转换逻辑与边界约束 |
|---|---|---|---|
| `ts_code`, `trade_date` | `000001.SZ`, `YYYYMMDD` | `ticker`, `trade_date` | `trade_date` 转为 `datetime.date` |
| `vol` | **手**（100 股） | `volume` | `(vol * 100).round().astype("Int64")`（保留 NaN） |
| `amount` | **千元** | `amount` | `amount * 1000.0`（元） |
| `adj_factor` | `adj_factor` 接口（`doc_id=28`） | `adjfactor` | 合并时保留源值，未合并时缺省 `1.0`；**绝不因 `suspend` 覆写 `adjfactor`** |
| `suspend_d` | `'S'` 停牌 / `' '` 正常 | `suspend` | `'S'` → `True`，`' '` 或缺失 → `False`；支持 `suspend_d` 与预合并 `suspend` 优先级合并 |
| `is_limit_up` / `is_limit_down` | `daily` 不提供 | `is_limit_up` / `is_limit_down` | 默认置 `False`（涨跌停判断需结合 `stk_limit`） |
