# 02. 元信息与交易日历

[← 返回目录](README.md)

---

## 1. 标的检索（跨市场消歧）

`GET /api/meta/tickers/search` · MCP: `get_meta_tickers_search`

按关键词（`thscode` / `ticker` / 中英文名称子串）跨市场消歧，返回完整标准代码与上市/到期信息。**调用业务接口前务必先消歧，切勿猜测 `.SH` / `.SZ` / `.BJ` 后缀。**

### 请求参数

| 参数 | 类型 | 必需 | 默认值 | 说明 |
|---|---|---|---|---|
| `q` | string | 是 | — | 搜索关键词（`thscode` / `ticker` / 中英文名称子串） |
| `exchange` | string | 否 | — | 交易所过滤：`SSE`、`SZSE`、`CFFEX`、`SHFE`、`INE`、`DCE`、`CZCE`、`GFEX` |
| `asset_type` | string | 否 | — | 规范化资产类型，支持逗号分隔多值（完整枚举见第 3 节） |
| `limit` | integer | 否 | `10` | 返回上限，最大 `50` |

### 返回字段 (`data.item[]` -> `TickerItem`)

| 字段 | 类型 | 说明 |
|---|---|---|
| `thscode` | string | 完整标准代码（如 `601318.SH`） |
| `ticker` | string | 纯代码（如 `601318`） |
| `name` | string | 展示名称 |
| `exchange` | string \| null | 交易所后缀（`SH`/`SZ`/`BJ` 等）；场外基金为 `null`（`.OF` 不是交易所） |
| `asset_type` | string | 规范化叶子资产类型 |
| `currency` | string | 币种代码（当前基金统一 `CNY`） |
| `list_date` | string \| null | 上市日期（`yyyy-MM-dd`） |
| `end_date` | string \| null | 合约到期/结束日期（`yyyy-MM-dd`） |
| `last_trade_date` | string \| null | 最后交易日（`yyyy-MM-dd`） |
| `last_delivery_date` | string \| null | 最后交割日（`yyyy-MM-dd`） |

---

## 2. 标的列表获取（代码表批量拉取）

`GET /api/meta/tickers/list` · MCP: `get_meta_tickers_list`

按 `asset_type` 分页获取全市场代码表。采用 `offset / limit` 分页，调用方循环递增 `offset += limit` 直至 `len(item) < limit` 即可取尽。

| 参数 | 类型 | 必需 | 默认值 | 说明 |
|---|---|---|---|---|
| `asset_type` | string | 否 | 全部类型 | 单值或英文逗号分隔多值（如 `fund-etf,fund-lof`）；非法值返回 `code=1003` |
| `limit` | integer | 否 | `1000` | 单页条数，最大 `10000` |
| `offset` | integer | 否 | `0` | 分页偏移量 |

响应结构与 `TickerItem` 相同，`data.timestamp` 为当前代码表快照的上游加载时间。

---

## 3. 规范化资产类型 (`asset_type`) 枚举

| `asset_type` | 含义 |
|---|---|
| `a-share` | A 股股票 |
| `a-share-index` | A 股指数、同花顺指数或板块 |
| `fund-otc` | 场外公募基金 |
| `fund-etf` | ETF 基金 |
| `fund-lof` | LOF 基金 |
| `fund-reits` | 公募 REITs |
| `forex` | 外汇 |
| `futures` | 期货合约 |
| `futures-commodity-index` | 期货商品指数合约 |
| `options` | 期权合约 |

---

## 4. A 股交易日历

`GET /api/a-share/calendar/trading-days` · MCP: `get_a_share_calendar_trading_days`

查询指定闭区间内的 A 股交易日序列（升序）。

| 参数 | 类型 | 必需 | 默认值 | 说明 |
|---|---|---|---|---|
| `start_date` | string | 否 | 今日减 1 年 | 起始日期 `yyyyMMdd`（含当日），须与 `end_date` 成对提供 |
| `end_date` | string | 否 | 今日 | 结束日期 `yyyyMMdd`（含当日），跨度上限 **10 个自然年** |

- **约束说明**：仅传单边、空值或格式错误返回 `1002`；`start_date > end_date` 或跨度超 10 年返回 `1003`（`20200229` 的十年上限为 `20300228`）；单日休市返回 `item: []`。
- **返回字段 (`data.item[]`)**：
  - `date_ms` (long)：交易日在 `Asia/Shanghai` `00:00:00` 的毫秒时间戳。
  - `date` (string)：可读日期 `yyyyMMdd`。
