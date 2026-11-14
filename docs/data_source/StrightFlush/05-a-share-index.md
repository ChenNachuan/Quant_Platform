# 05. A 股指数与同花顺板块

[← 返回目录](README.md)

覆盖上证/深证交易所指数（如 `000001.SH`、`399001.SZ`、`000300.SH`）以及同花顺特色概念、行业、地域与特色指数（`.TI` 后缀，如 `886042.TI` 白酒概念、`881101.TI` 行业指数）。

---

## 1. 同花顺指数目录

`GET /api/a-share-index/catalog/ths-index-list` · MCP: `get_a_share_index_catalog_ths_index_list`

一次性全量返回指定分类的同花顺指数清单（无分页）。

| 参数 | 类型 | 必需 | 默认值 | 说明 |
|---|---|---|---|---|
| `tag` | string | 否 | `cn_concept` | `cn_concept`（概念）/ `industry`（行业）/ `region`（地域）/ `tszs`（特色），大小写不敏感 |

- **返回字段 (`data.item[]`)**：`thscode`、`name`（注意指数目录不返回纯代码 `ticker`）。

---

## 2. 指数成分股与个股板块反查

| 接口 | REST 端点 / MCP | 参数与限制 | 返回字段 |
|---|---|---|---|
| **指数查成分股** | `GET /api/a-share-index/constituents/ths-stock-list`<br>`get_a_share_index_constituents_ths_stock_list` | `thscode`（必填，单个指数，支持 `.TI` 与 `.SH`/`.SZ`，不接受逗号） | `item[]`: `{thscode, ticker, name}` |
| **个股反查板块** | `GET /api/a-share-index/constituents/ths-index-list-by-stock`<br>`get_a_share_index_constituents_ths_index_list_by_stock` | `thscode`（必填，注意参数名为单数 `thscode`，逗号分隔 **1–100 个**个股，去重前计数）<br>`type`（可选，单值且大小写敏感：`cn_concept` / `industry` / `region` / `tszs`，省略返回四类） | `item[]`: `{thscode, index_list: [{thscode, name, type}]}`（按 `type, thscode` 升序） |

---

## 3. 指数行情快照与历史 K 线

| 接口 | REST 端点 / MCP | 关键参数与约束 |
|---|---|---|
| **指数行情快照** | `GET /api/a-share-index/prices/snapshot`<br>`get_a_share_index_prices_snapshot` | **必传** `thscodes`（逗号分隔）；**不支持空入参全量遍历**；字段同 A 股行情快照 |
| **单指数历史 K 线** | `GET /api/a-share-index/prices/historical`<br>`get_a_share_index_prices_historical` | `thscode`（单个）、`interval`（仅 `1d`）、`start`/`end`（毫秒戳）。仅支持**从查询日向前滚动近 5 个自然年**，无 `adjust` 参数（响应 `adjust: null`） |
| **批量指数历史 K 线** | `GET /api/a-share-index/prices/historical-batch`<br>`get_a_share_index_prices_historical_batch` | `thscodes`（去重前 **最多 10 个**）、`interval=1d`、`start`/`end` 需落在近 5 年滚动窗内 |

> 另注：同花顺新增概念列表 `/api/a-share-index/baseinfo/new-concepts` 当前仅在同花顺 AI 客户端内开放，详见 [11-ai-client-only.md](11-ai-client-only.md)；指数概况（基本信息/权重）处于“敬请期待”规划中。
