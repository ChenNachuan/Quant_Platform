# 11. AI 客户端专属与规划中接口

[← 返回目录](README.md)

> [!WARNING]
> 本页列出的接口**当前不作为端外公开 `X-api-key` 接入入口**，仅用于说明同花顺 AI 客户端（`lumi.10jqka.com.cn`）内的数据契约或后续规划接口，在编写端外量化 `data/adapters/` 时切勿依赖这些路径。

---

## 1. 仅同花顺 AI 客户端内（`lumi.10jqka.com.cn`）接口

| 模块 | 路径 | 契约概要与端外差异 |
|---|---|---|
| **主力资金** | `GET /api/a-share/capital-flow/snapshot`<br>`GET /api/a-share/capital-flow/historical` | 单只 A 股（不支持批量）；返回 `super_large`、`large`、`medium`、`small` 四档资金的 `inflow_amount` / `outflow_amount`（历史接口含 `1m`<=7 天与 `1d`<=1 年，不返 `net_amount`） |
| **高频动向** | `GET /api/a-share/high-frequency/historical`<br>`GET /api/a-share/high-frequency/intraday` | 单只 A 股个股或指数；限最近 30 个交易日内；`historical` 支持 `1d` / `1m`（返回 `hf_direction`, `hf_participation`）；`intraday` 返单日分时（`interval="trend"`） |
| **筹码形态** | `GET /api/a-share/special-data/chip-shape-dates`<br>`GET /api/a-share/special-data/chip-shape-stocks` | 查询可用日期及按形态 `shape` (`double_peak` / `high_position_dense` / `low_position_dense` / `low_position_locked`) 分页筛选个股（每页 1–20） |
| **市场成交额预测** | `GET /api/a-share/special-data/turnover-prediction-snapshot` | 无参数，返回全天预测成交额 `predict_turnover`、昨日成交额 `previous_turnover` 与 `predict_turnover_change` |
| **宽基 ETF 申购净流入** | `GET /api/fund/etf-flow/net-inflow-historical` | 返回全市场宽基 ETF 日统计净流入序列 `{date_ms, net_inflow}` 及可用起止日 |
| **同花顺新增概念** | `GET /api/a-share-index/baseinfo/new-concepts` | 分页返回同花顺新增概念事件（`record_id`, `concept_code`, `thscode`, `name`, `created_at`），每页 1–15 |
| **资讯事件库** | `GET /api/news/events/search` | 按标题/摘要、时间、评级 (`event_grading_score`)、行业/概念编码检索结构化事件（单实例限 5 QPS，超限 `code=4001`） |
| **期货 F10 基本面历史** | `GET /api/futures/fundamentals/indicators-historical` | 计划接入同花顺 AI 客户端；`indicator_ids`（1–5 个）、`start_date`/`end_date`（最长 5 年） |
| **期货/期权高级行情** | `GET /api/futures/prices/{intraday,daily}`<br>`GET /api/options/prices/{intraday,daily}` | 这 4 个端点本身可端外调用，但**历史分时**（`trade_date=yyyyMMdd`）与**非日线多周期**（`min_1`/`min_10`/`hour_1`/`week_1`/`month_1`/`quarter_1`/`year_1`）仅在 AI 客户端内开放 |

---

## 2. 敬请期待（规划中接口）

以下两项当前官网仅保留文档占位，尚未提供可用端点或契约：

| 模块 | 官方说明 | 现有替代方案 |
|---|---|---|
| **股票基础信息** (`stock-basics`) | 规划提供 A 股基本资料、上市状态、行业归属、市值等 | 标的列表/上市日期用 [`02-meta-and-calendar.md`](02-meta-and-calendar.md)；行业归属用 `ths-index-list-by-stock`；估值用 `valuations/snapshot` |
| **指数概况** (`index-overview`) | 规划提供指数基本信息、成分股、权重查询 | 指数目录/成分股/行情见 [`05-a-share-index.md`](05-a-share-index.md)（目前不含成分股权重） |
