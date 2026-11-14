# 09. 资讯、公告与快讯

[← 返回目录](README.md)

公开资讯域共有 **6 组 7 个 REST 端点**（均同步暴露对应 MCP Tool）；注：`data` 中多数接口列表字段名为 `items`（而非 `item`），与个股接口的差异请按各表说明解析。

---

## 接口总览

| 接口 | REST 端点 / MCP | 参数与分页约定 | 核心返回字段与注意事项 |
|---|---|---|---|
| **快讯列表** | `GET /api/news/flash/list`<br>`get_news_flash_list` | `tag_id`（栏目 ID，必填）、`seq`（不透明十进制字符串游标，首屏传 `"0"`） | `items[]`: `{id, seq, title, encoded_url, summary, tags[], type, create_time_ms, update_time_ms, pic_url}`；`id`/`seq` 为保留精度的十进制字符串 |
| **快讯刷新** | `GET /api/news/flash/refresh`<br>`get_news_flash_refresh` | `tag_id`、`seq`、`create_time_ms`（均为必填，**毫秒值必须能被 `1000` 整除**） | 返回增量快讯 `items[]`，结构同快讯列表 |
| **资讯详情** | `GET /api/news/article/detail`<br>`get_news_article_detail` | `client_type` (`WEB`/`WEB_ENCRYPTED`/`PC`/`PC_LIST`，不区分大小写)、`encoded_seq`（密文资讯标识，二者均必填） | 返回 `encoded_seq`（不返明文 seq）、`title`、`author`、`summary`、`content`（**HTML 正文**，展示需做 XSS 过滤）、`source`、`publish_time_ms`、`is_kuaixun` |
| **公告详情与解读** | `GET /api/news/notice/detail`<br>`get_news_notice_detail` | `seq`（必填，公告序号字符串） | 返回 `seq`、`guid`、`title`、`report_period`、`summary` (`{notice_type, items[]}`)、`monthly_report_interpretation` (`{has_findings, findings[], summary_html}`) |
| **热门深度专题** | `GET /api/news/topic/hot-list`<br>`get_news_topic_hot_list` | `tab_id`（必填）、`cursor`（可选）、`limit`（1–10，默认 `10`）、`important_only` (`false` 默认) | 返回 `items[]`（含 `id`, `title`, `url`, `propagation_chain`, `source_created_at` 等）、`next_cursor`、`has_more` |
| **日期范围异动解读** | `GET /api/news/anomaly/analysis`<br>`get_news_anomaly_analysis` | `begin_date`、`end_date` (`yyyy-MM-dd`，均必填)、`thscode`（可选，省略查全量） | 指定标的最多返回 **365 天**，全量最多 **30 天**，超限从结束日向前截断并置 `was_truncated=true`；空列表可能表示服务降级，不能据此断言无异动 |
| **个股分时新闻** | `GET /api/news/stock/timeline-list`<br>`get_news_stock_timeline_list` | `thscode`（必填）、`offset`（必填，首屏传 `"first"`，后续原样回传 `data.offset`）、`size`（1–100，可选） | 返回 `items[]`（`id`, `type`, `title`, `author`, `source`, `summary`, `publish_time_ms`, `jump_url`, `pic_url`）、`has_more`、`offset` |

> 另注：资讯事件库检索 `/api/news/events/search` 当前仅限同花顺 AI 客户端内访问（且有单实例 5 QPS 限制），详见 [11-ai-client-only.md](11-ai-client-only.md)。
