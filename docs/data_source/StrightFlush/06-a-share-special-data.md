# 06. A 股集合竞价与特色数据

[← 返回目录](README.md)

---

## 1. 集合竞价数据与机会筛选

| 接口 | REST 端点 / MCP | 核心参数与说明 |
|---|---|---|
| **A 股集合竞价快照** | `GET /api/a-share/auction/snapshot`<br>`get_a_share_auction_snapshot` | `thscodes`（必填，逗号分隔 **最多 100 个**）、`stage`（`final` 默认 / `live`）。返回 `auction_phase`、`data_status`、`auction_price`、`auction_pct`、`auction_volume`、`auction_amount`、`auction_unmatched`、`auction_turnover_pct`、`auction_yesterday_ratio_pct`、`auction_volume_ratio`、`pre_close_price`、`open_price`、`last_price`、`float_market_cap` |
| **短线风向标竞价基准** | `GET /api/a-share/auction/short-term-benchmark`<br>`get_a_share_auction_short_term_benchmark` | `date`（可选 `yyyy-MM-dd`，默认上海当日；指定非交易日不自动回退）。返回 `thscode`、`ticker`、`name`、`auction_pct`、`tags[]` |
| **集合竞价机会筛选** | `GET /api/a-share/special-data/auction-stock-opportunities`<br>`get_a_share_special_data_auction_stock_opportunities` | `date` (`YYYY-MM-DD`)、`warn_types` (`68710`..`68727`)、`market` (`a_share`/`sh_sz_main`/`sh_a`/`sz_a`/`gem`/`star`)、`exclude_st`、`thscodes`（<=50）、`sort_by` (`warn_time`/`warn_type`)、`sort_order`、`page`/`size`（1–20）。注意 `bid_change_ratio` 为**小数比例** |
| **竞价可选交易日期** | `GET /api/a-share/special-data/auction-opportunity-dates`<br>`get_a_share_special_data_auction_opportunity_dates` | 无参数，返回可用 `dates[]`（升序）与 `latest_date` |

---

## 2. 涨跌停池、炸板池、连板天梯与统计

| 接口 | REST 端点 / MCP | 参数与核心字段 |
|---|---|---|
| **涨停股票池** | `GET /api/a-share/special-data/limit-up-pool`<br>`get_a_share_special_data_limit_up_pool` | `date_ms`（上海零点毫秒戳，可选）、`page`/`size`（1–200，默认 50）、`sort_field` (`last_price`/`continue_day_cnt`/`seal_money`/`limit_up_time`)、`sort_dir`。返回 `is_st`、`is_new`、`limit_up_time`、`limit_up_reason`、`continue_day_text`、`continue_day_cnt`、`seal_money`、`max_seal_money`、`open_times` |
| **跌停股票池** | `GET /api/a-share/special-data/limit-down-pool`<br>`get_a_share_special_data_limit_down_pool` | `date_ms`、`page`/`size`、`sort_field` (`last_limit_time`/`first_limit_time`/`last_price`/`price_change_ratio_pct`/`turnover_ratio_pct`)、`sort_dir`。返回 `first_limit_time`、`last_limit_time`、`turnover_ratio_pct` |
| **炸板股票池** | `GET /api/a-share/special-data/limit-break-pool`<br>`get_a_share_special_data_limit_break_pool` | `date_ms`、`page`/`size`、`sort_field` (`price_change_ratio_pct`/`open_times`/`last_price`/`turnover_ratio_pct`/`turnover`)、`sort_dir`。返回 `open_times`、`first_seal_time`、`last_seal_time`、`turnover` |
| **连板天梯** | `GET /api/a-share/special-data/limit-up-ladder`<br>`get_a_share_special_data_limit_up_ladder` | 无参数。返回近 30 个交易日连板矩阵：`window`、`board_num`、`seal_nextday`（最新交易日固定 `null`）、`sign_level` |
| **涨停市场统计** | `GET /api/a-share/special-data/limit-up-statistics`<br>`get_a_share_special_data_limit_up_statistics` | `date`（`YYYY-MM-DD`，近 1 年）。返回 `seal_rate`（小数比例）、`open_stock_count`、`limit_up_stock_count`、`touched_limit_up_stock_count`、`page_boards` (`one`/`two`/`three`/`four`/`five_plus` 含 `promotion_rate`)、`exact_boards`（精确 N>=2 板家数） |

---

## 3. 龙虎榜与营业部

| 接口 | REST 端点 / MCP | 参数与核心字段 |
|---|---|---|
| **龙虎榜榜单** | `GET /api/a-share/special-data/dragon-tiger-list`<br>`get_a_share_special_data_dragon_tiger_list` | `board_type` (`all`/`org`/`hot_money`)、`date` (`yyyy-MM-dd`，近 1 年且必须为交易日，非交易日返回 `1002`)。`all`/`org` 填充 `stock_items`；`hot_money` 填充 `hot_money_items`（按游资聚合 `rows`）。注意 `change`、`net_rate`、`org_net_rate` 等为**小数比例** |
| **个股机构交易详情** | `GET /api/a-share/special-data/dragon-tiger-stock-institution`<br>`get_a_share_special_data_dragon_tiger_stock_institution` | `thscode`（必填）、`date`（可选，近 1 年）、`range_days` (`1` / `3`，省略返回全部可用区间）。返回机构买/卖/净买额与家数、净买比例及买卖营业部席位索引（含 `department_id`） |
| **营业部经典战绩** | `GET /api/a-share/special-data/dragon-tiger-department-classic-record`<br>`get_a_share_special_data_dragon_tiger_department_classic_record` | `department_id`（必填）。返回营业部名称、组织 ID 与代表战绩（`thscode`, `name`, `board_pattern`） |
| **营业部历史上榜记录** | `GET /api/a-share/special-data/dragon-tiger-department-history`<br>`get_a_share_special_data_dragon_tiger_department_history` | `department_id`（必填）、`page`/`size`（1–30）、`min_net_buy_amount` (`5000000`/`30000000`/`50000000`/`100000000`)、`sort_field` (`trade_date`/`change_ratio`/`net_buy_amount`)、`sort_dir` |

---

## 4. 同花顺热榜

| 接口 | REST 端点 / MCP | 参数与说明 |
|---|---|---|
| **飙升榜** | `GET /api/a-share/special-data/skyrocket-list`<br>`get_a_share_special_data_skyrocket_list` | `period` (`day`/`hour`)、`limit`（1–100，默认 30）。返回 `rank`、`heat`（原始字符串）、`rank_change`、`rank_trend` (`up`/`down`/`flat`/`unknown`) |
| **A 股热股榜单** | `GET /api/a-share/special-data/hot-stock-list`<br>`get_a_share_special_data_hot_stock_list` | `period` (`day` 24 小时 / `hour` 小时)、`limit`（1–100，默认 30） |
| **历史热股排行** | `GET /api/a-share/special-data/hot-stock-list-history`<br>`get_a_share_special_data_hot_stock_list_history` | `date`（必填 `yyyy-MM-dd`，近 1 年）。返回当日最多 30 条 `rank` 记录 |
| **个股排名走势** | `GET /api/a-share/special-data/hot-stock-rank-trend`<br>`get_a_share_special_data_hot_stock_rank_trend` | `thscode`、`start_date`、`end_date`（均必填 `yyyy-MM-dd`，近 1 年且窗口 <= 1 年） |

---

## 5. 异动监控与当日异动解读

| 接口 | REST 端点 / MCP | 参数与说明 |
|---|---|---|
| **严重异动与重点监控** | `GET /api/a-share/special-data/abnormal-monitor-list`<br>`get_a_share_special_data_abnormal_monitor_list` | `state` (`current` 默认 / `history`)、`page`/`size`（1–30）。`monitor_label` 中 `1/2`=严重异动、其余=重点监控 |
| **个股异动原因列表** | `GET /api/a-share/special-data/anomaly-analysis-list`<br>（不提供 MCP） | `tag_codes`（可选，逗号分隔 OR 过滤：`LIMIT_UP`, `LIMIT_DOWN`, `SHARP_RISE`, `SHARP_FALL`, `RAPID_RALLY`, `RAPID_DECLINE`）。返回当日 `analysis_content` 与 `keyword_list` |
| **按股查异动原因** | `GET /api/a-share/special-data/anomaly-analysis-stock`<br>`get_a_share_special_data_anomaly_analysis_stock` | `thscodes`（必填，逗号分隔去重前 **最多 50 个**）。当日暂无数据返回 `code=3002`，有快照但无匹配返回 `item: []` |
