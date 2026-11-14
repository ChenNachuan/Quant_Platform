# 10. MCP 服务与开发工具

[← 返回目录](README.md)

---

## 1. 公开 HTTP MCP Servers（8 个）

MCP Tools 是 REST API 的轻量 Agent 适配器（与 REST 共享同一套后端 capability、字段和错误码）。`fuyao-meta` 负责跨市场代码消歧，**建议与业务域 MCP 一同挂载**。

```json
{
  "mcpServers": {
    "fuyao-meta": { "type": "http", "url": "https://fuyao.aicubes.cn/mcp/meta", "headers": { "X-api-key": "<your-api-key>" } },
    "fuyao-a-share": { "type": "http", "url": "https://fuyao.aicubes.cn/mcp/a-share", "headers": { "X-api-key": "<your-api-key>" } },
    "fuyao-a-share-index": { "type": "http", "url": "https://fuyao.aicubes.cn/mcp/a-share-index", "headers": { "X-api-key": "<your-api-key>" } },
    "fuyao-fund": { "type": "http", "url": "https://fuyao.aicubes.cn/mcp/fund", "headers": { "X-api-key": "<your-api-key>" } },
    "fuyao-futures": { "type": "http", "url": "https://fuyao.aicubes.cn/mcp/futures", "headers": { "X-api-key": "<your-api-key>" } },
    "fuyao-options": { "type": "http", "url": "https://fuyao.aicubes.cn/mcp/options", "headers": { "X-api-key": "<your-api-key>" } },
    "fuyao-hk-share": { "type": "http", "url": "https://fuyao.aicubes.cn/mcp/hk-share", "headers": { "X-api-key": "<your-api-key>" } },
    "fuyao-news": { "type": "http", "url": "https://fuyao.aicubes.cn/mcp/news", "headers": { "X-api-key": "<your-api-key>" } }
  }
}
```

- **配置文件位置**：Claude Desktop (`claude_desktop_config.json`) · Cursor (`~/.cursor/mcp.json`) · Windsurf (`~/.codeium/windsurf/mcp_config.json`) · Cline (`Cline: Open MCP Settings`)。
- **典型跨服务调用链路**：先调用 `fuyao-meta` 的 `get_meta_tickers_search`（或 `fuyao-a-share-index` 的 `get_a_share_index_catalog_ths_index_list` / `constituents`）确认标准 `thscode`，再调用对应业务服务工具取数。

---

## 2. 已注册公开 MCP Tools 清单（共 110 个）

| MCP 服务 (`endpoint`) | 数量 | 工具列表 |
|---|---|---|
| `fuyao-meta-mcp`<br>(`/mcp/meta`) | 2 | `get_meta_tickers_search` · `get_meta_tickers_list` |
| `fuyao-a-share-mcp`<br>(`/mcp/a-share`) | 33 | `get_a_share_prices_snapshot` · `get_a_share_prices_historical` · `get_a_share_prices_historical_batch`<br>`get_a_share_valuations_snapshot` · `get_a_share_corporate_actions_adjustment_factors` · `get_a_share_calendar_trading_days`<br>`get_a_share_financials_income_statements[_batch]` · `get_a_share_financials_balance_sheets[_batch]` · `get_a_share_financials_cash_flow_statements[_batch]` · `get_a_share_financials_indicators`<br>`get_a_share_auction_snapshot` · `get_a_share_auction_short_term_benchmark`<br>`get_a_share_special_data_limit_up_pool` · `limit_down_pool` · `limit_break_pool` · `limit_up_ladder` · `limit_up_statistics`<br>`get_a_share_special_data_dragon_tiger_list` · `dragon_tiger_stock_institution` · `dragon_tiger_department_classic_record` · `dragon_tiger_department_history`<br>`get_a_share_special_data_anomaly_analysis_stock` · `abnormal_monitor_list` · `auction_stock_opportunities` · `auction_opportunity_dates`<br>`get_a_share_special_data_skyrocket_list` · `hot_stock_list` · `hot_stock_list_history` · `hot_stock_rank_trend`<br>`get_a_share_ipo_calendar_list` |
| `fuyao-a-share-index-mcp`<br>(`/mcp/a-share-index`) | 6 | `get_a_share_index_catalog_ths_index_list` · `get_a_share_index_constituents_ths_stock_list` · `get_a_share_index_constituents_ths_index_list_by_stock`<br>`get_a_share_index_prices_snapshot` · `get_a_share_index_prices_historical` · `get_a_share_index_prices_historical_batch` |
| `fuyao-fund-mcp`<br>(`/mcp/fund`) | 33 | `get_fund_profile_detail` · `get_fund_companies_detail` · `get_fund_corporate_actions_dividends` · `get_fund_diagnostics_detail`<br>`get_fund_market_snapshot` · `get_fund_market_historical` · `get_fund_market_historical_batch`<br>`get_fund_performance_nav` · `returns` · `indicators_historical` · `drawdowns`<br>`get_fund_portfolio_holdings` · `stock_history` · `stock_report_dates` · `bond_history` · `bond_report_dates` · `asset_allocation` · `industry_allocation`<br>`get_fund_holders_detail` · `top` · `get_fund_financials_indicators` · `income_statements` · `balance_sheets`<br>`get_fund_managers_detail` · `experience` · `investment_style` · `performance`<br>`get_fund_news_article_list` · `get_fund_offerings_list` · `get_fund_quota_summary` · `list` · `get_fund_backtest_result` · `indicators` |
| `fuyao-futures-mcp`<br>(`/mcp/futures`) | 20 | `get_futures_varieties_list` · `get_futures_variety_plates_list` · `get_futures_contracts_detail` · `list`<br>`main_continuous_list` · `main_list` · `secondary_main_list` · `commodity_index_list`<br>`get_futures_positions_company_list` · `variety_daily` · `company_variety_daily` · `contract_daily` · `contract_historical`<br>`get_futures_warehouse_receipts_historical` · `get_futures_basis_main_continuous_latest` · `historical`<br>`get_futures_calendar_trading_schedule` · `session_timeline` · `get_futures_prices_intraday` · `daily` |
| `fuyao-options-mcp`<br>(`/mcp/options`) | 6 | `get_options_varieties_list` · `get_options_contracts_detail` · `list` · `get_options_calendar_session_timeline` · `get_options_prices_intraday` · `daily` |
| `fuyao-hk-share-mcp`<br>(`/mcp/hk-share`) | 3 | `get_hk_share_ipo_application_list` · `get_hk_share_ipo_calendar_list` · `get_hk_share_ipo_issue_detail` |
| `fuyao-news-mcp`<br>(`/mcp/news`) | 7 | `get_news_flash_list` · `refresh` · `get_news_article_detail` · `get_news_notice_detail` · `get_news_topic_hot_list` · `get_news_anomaly_analysis` · `get_news_stock_timeline_list` |

> 注：以上带前缀缩写（如 `limit_down_pool`）对应完整工具名 `get_a_share_special_data_limit_down_pool`，各域均按 `get_<domain>_<endpoint_snake_case>` 规则命名；`GET /api/a-share/special-data/anomaly-analysis-list` 与 Market Dumps 不暴露为 MCP Tool。

---

## 3. 官方开发者工具链 (`HiThink-Tech/Financial-API`)

除 REST 与 MCP 外，官方开源仓库提供三套本地工具（统一复用环境变量 `HITHINK_FINANCE_API_KEY` 或系统凭据库）：

| 工具形态 | 安装与前置要求 | 核心用法与特性 |
|---|---|---|
| **官方 CLI**<br>`@hithink-tech/hithink-finance-cli` | Node.js `>= 22.12`<br>`npm install -g @hithink-tech/hithink-finance-cli` | - 安全登录：`hithink-finance auth login`（支持 `--api-key-stdin`，避免 `--api-key` 暴露命令历史）<br>- 能力自省：`hithink-finance doctor --format json`、`capabilities --format json`、`schema market.snapshot --format json`<br>- 业务查询：`symbol search`、`market snapshot`、`financials income`、`index constituents`、`data status`<br>- 安装时自动 best-effort 同步 **10 个领域 Skills**（`hithink-finance-{shared,symbol,market,financials,valuation,index,special-data,fund,data,research}`），可用 `hithink-finance skills sync --repair` 修复 |
| **Python Toolkit & `marketdb`** | Python `>= 3.11`（从 GitHub monorepo 源码安装：`python -m pip install -e ./python`） | - 远程客户端：`fuyao_client`（`tickers_search`, `prices_snapshot` 等，业务异常抛出 `FuyaoApiError`）及 CLI 脚本 `python/toolkit/fuyao/scripts/fuyao.py`（退出码 `0` 成功 / `2` 上游业务错误 / `3` 本地参数错误 / `4` 环境错误）<br>- 本地 DuckDB 研究库：`marketdb`（`python/bootstrap.py` 初始化，`MarketDB.open(...)` 读取日线/复权/面板） |
| **统一 Agent Skill**<br>`hithink-finance` | `npx skills add HiThink-Tech/Financial-API --skill hithink-finance -g --yes` | 作为 Codex / Claude Code 等 Agent 的统一路由入口（需保留完整 `skills/hithink-finance/` 与 `references/` 目录），根据当前环境自动路由至 REST / MCP / CLI / Python |
