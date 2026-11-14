# 03. A 股行情、全市场导出与除复权

[← 返回目录](README.md)

---

## 1. A 股行情快照

`GET /api/a-share/prices/snapshot` · MCP: `get_a_share_prices_snapshot`

- **批量模式**：传入 `thscodes`（英文逗号分隔，如 `600519.SH,000001.SZ`），按入参顺序返回，忽略分页参数。
- **全市场模式**：省略 `thscodes` 时按 `thscode` 升序分页遍历全 A 股，支持 `limit`（默认 `100`）与 `offset`（默认 `0`）。

### 返回字段 (`data`)

- `data.timestamp` (long \| null)：本次快照最新的上游有效时间。
- `data.total` (int)：全市场代码表总数（或匹配总数）。
- `data.item[]` (`PriceSnapshotItem`)：`thscode`、`ticker`、`last_price`、`price_change`、`price_change_ratio_pct`（百分数原值）、`open_price`、`high_price`、`low_price`、`prev_price`、`volume`（股）、`turnover`（元）。
- **注意**：快照不返回中文名 `name`，需结合 [标的检索/列表](02-meta-and-calendar.md) 获取。

---

## 2. A 股历史 K 线（单标的 vs 批量）

| 维度 | 单标的 `GET /api/a-share/prices/historical` | 批量 `GET /api/a-share/prices/historical-batch` |
|---|---|---|
| **MCP Tool** | `get_a_share_prices_historical` | `get_a_share_prices_historical_batch` |
| **代码参数** | `thscode`（单个，不接受逗号） | `thscodes`（逗号分隔，去重前 **最多 10 个**） |
| **支持周期 `interval`** | **当前仅支持 `1d`**（默认 `1d`） | 支持 **`1d` / `1w` / `1mo`**（必填） |
| **时间窗口 `[start, end]`** | 必填毫秒时间戳，跨度 **<= 10 年** | 必填非负毫秒时间戳，跨度 **<= 10 个自然年** |
| **复权方式 `adjust`** | `forward`（默认）/ `none` / `backward` | `forward`（默认）/ `none` / `backward` |

- **K 线记录字段 (`PriceBarItem`)**：`date_ms`、`open_price`、`high_price`、`low_price`、`close_price`、`volume`（股）、`turnover`（原始货币）。
- **批量外层结构**：`data = {interval, adjust, item: [{thscode, timestamp, item: [PriceBarItem]}]}`（部分有效语义见 [01-auth-and-protocol.md](01-auth-and-protocol.md)）。

---

## 3. 全市场数据导出（Market Dumps — Parquet 直下）

面向批量回测、离线分析与本地数据湖初始化，提供整库 S3 预签名 Parquet 下载。**预签名 URL 有效期通常仅 5 分钟，切勿持久化或缓存，每次下载前实时获取。**

- **浏览器页面入口**：`/dump/market-dumps/*`（使用登录态 Cookie）。
- **API 客户端入口**：`/api/dump/market-dumps/*`（携带 `X-api-key` 请求头）。

| `dump_id` | 覆盖范围 | 主键 / 时间字段 | API 下载链接端点 |
|---|---|---|---|
| `a_share_daily_k_1d_none_10y` | 全 A 股近 10 年未复权日 K | `(thscode, date_ms)` | `GET /api/dump/market-dumps/daily-k/download-url` |
| `a_share_daily_k_1d_none_10d` | 全 A 股最近 10 个交易日未复权日 K | `(thscode, date_ms)` | `GET /api/dump/market-dumps/daily-k-10d/download-url` |
| `a_share_adjustment_factors_event_none_all` | 全 A 股全部历史分红/送股/配股事件流 | `(thscode, ex_date_ms)` | `GET /api/dump/market-dumps/adjustment-factors/download-url` |

### Parquet 列结构

- **日 K (`daily_k`)**：`thscode`、`currency` (`CNY`)、`interval` (`1d`)、`adjusted` (`none`)、`date_ms`、`open_price`、`high_price`、`low_price`、`close_price`、`volume`、`turnover`。
- **复权因子 (`adjustment_factors`)**：`thscode`、`ticker`、`ex_date_ms`、`dividend_per_share`（税前每股派息）、`per_share_bonus`（每股送股比例）、`allotment_ratio`（配股比例）、`allotment_price`（配股价）、`currency`。

---

## 4. 除复权事件流（单标 REST）

`GET /api/a-share/corporate-actions/adjustment-factors` · MCP: `get_a_share_corporate_actions_adjustment_factors`

返回单只 A 股原始分红/送股事件流，按 `ex_date_ms` 降序排列。

| 参数 | 类型 | 必需 | 说明 |
|---|---|---|---|
| `thscode` | string | 是 | 单只 A 股代码，不接受逗号 |
| `from` | string | 否 | 起始日 `YYYY-MM-DD` |
| `to` | string | 否 | 截止日 `YYYY-MM-DD` |

- **返回结构**：`data = {thscode, ticker, item: [{ticker, ex_date_ms, dividend_per_share, per_share_bonus}]}`。
- **注意差异**：该 REST 接口**不返回** `event_type` / `record_date` / `adjust_factor` 字段，也不返回 `allotment_ratio` / `allotment_price`（配股字段仅在 Market Dumps 全量复权 Parquet 中提供）；如需直接消费复权后价格请直接调用 `prices/historical?adjust=forward|backward`。
