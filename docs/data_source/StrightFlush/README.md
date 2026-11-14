# 同花顺金融数据 API（Fuyao / StraightFlush）文档目录

> 面向 AI Agent、量化研究与 Fintech 应用的 A 股及多资产结构化金融数据服务（REST API + MCP Tools + CLI + Python Toolkit + 全市场 Parquet 导出）。

- **官方站点**：<https://fuyao.aicubes.cn/docs/>
- **LLM 聚合文档**：<https://fuyao.aicubes.cn/llms.txt> · <https://fuyao.aicubes.cn/llms-full.txt>
- **开源仓库（SDK / CLI / Skill）**：<https://github.com/HiThink-Tech/Financial-API> · <https://gitee.com/HiThink-Tech/Financial-API>
- **API Key 管理 / 在线调试**：<https://fuyao.aicubes.cn/admin/> · <https://fuyao.aicubes.cn/playground/>

---

## 文档导航

为便于按业务域按需查阅，文档拆分为以下 11 篇短文档：

| 序号 | 文档 | 覆盖范围 |
|---|---|---|
| 01 | [鉴权、通用协议与限流](01-auth-and-protocol.md) | `X-api-key`、`ApiResponse` 信封、时区与空值约定、原生批量规则、限流策略、完整错误码（`0`–`5003`） |
| 02 | [元信息与交易日历](02-meta-and-calendar.md) | 标的检索 `search`、全量代码表分页 `list`（10 种 `asset_type` / 8 大交易所）、A 股交易日历 |
| 03 | [A 股行情、全市场导出与除复权](03-a-share-market.md) | A 股行情快照、单标与批量历史 K 线、Market Dumps（Parquet 直下）、除复权事件流 |
| 04 | [A 股财务、估值与新股 IPO](04-a-share-financials-ipo.md) | 三大合并财报（单标/批量）、五类 24 项财务指标、5 项估值快照、A 股打新与港股打新 |
| 05 | [A 股指数与同花顺板块](05-a-share-index.md) | THS 概念/行业/地域/特色指数目录、成分股、个股反查板块（最多 100 股）、指数快照与 K 线 |
| 06 | [A 股集合竞价与特色数据](06-a-share-special-data.md) | 集合竞价快照/机会、涨跌停/炸板/连板天梯/涨停统计、龙虎榜与营业部、热股榜、异动监控与解读 |
| 07 | [公募基金（ETF / LOF / OTC）](07-funds.md) | 基金资料、公司、分红、诊断、财务、持有人、重仓与配置、基金经理、场内行情、募集、净值、QDII 额度、在线回测 |
| 08 | [期货与期权](08-futures-and-options.md) | 期货品种/合约/主连主力/持仓/仓单/基差/日历/行情，以及期权品种/合约/时间轴/行情（含端外限制说明） |
| 09 | [资讯、公告与快讯](09-news-and-events.md) | 快讯列表与刷新、资讯详情、公告解读、热门专题、日期异动解读、个股分时新闻 |
| 10 | [MCP 服务与开发工具](10-mcp-and-dev-tools.md) | 8 个公开 HTTP MCP Server、110 个 MCP Tool 清单、CLI / Python Toolkit (`marketdb`) / Agent Skill |
| 11 | [AI 客户端专属与规划中接口](11-ai-client-only.md) | 仅限同花顺 AI 客户端内（`lumi.10jqka.com.cn`）的数据契约与“敬请期待”接口说明 |

---

## 在本量化平台中的接入与落盘约定

- **凭证管理**：统一存放在 `config/.env` 中的 `STRIGHT_FLUSH_TOKEN`（官方工具链环境变量名 `HITHINK_FINANCE_API_KEY`），通过 [`core.config.load_secrets()`](../../../core/config.py) 读取并注入 HTTP 请求头 `X-api-key`。严禁将真实 Key 写入代码、配置文件、日志或 Git（红线 8）。
- **代码格式（`thscode`）**：全部业务接口均强依赖带市场后缀的完整代码，如 `600519.SH`、`000001.SZ`、`430047.BJ`、`510300.SH`、`025480.OF`、`886042.TI`、`CU2601.SHF`、`IO2601-C-4000.CFE`。不确定的代码应先调 `/api/meta/tickers/search` 消歧，勿自行拼接后缀。
- **适配与存储布局**：由 [`StraightFlushAdapter`](../../../data/adapters/straightflush_adapter.py)（注册键 `"straightflush"`，内置 `adjust="none"` 未复权 raw 契约、10 标分批、东八区毫秒时间戳与 `ApiResponse` 业务码处理）与 [`straightflush_normalizer`](../../../data/normalizers/straightflush_normalizer.py) 接入，按 `{nation}_{asset_type}_{data_type}` 目录落盘至 `$QUANT_DATA_ROOT/asset/{nation}_{asset_type}_{data_type}/`，并注册至持久化 DuckDB 元数据目录 `$QUANT_DATA_ROOT/catalog.duckdb`（参见 [storage_schema.md](../../design/data/storage_schema.md) 与 [ARCHITECTURE.md](../../design/ARCHITECTURE.md)）。
