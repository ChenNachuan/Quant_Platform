# Tushare Pro 数据源文档目录

> 面向个人与机构量化研究的开源金融数据开放平台（Python SDK / HTTP RESTful API / Matlab & R SDK / MCP Server / Agent Skills），覆盖沪深北 A 股、ETF、公募基金、指数与申万/中信行业、期货、期权、债券与可转债、外汇与储备资产、港股、美股、国内外宏观及大模型语料。

- **官网与接口文档**：<https://tushare.pro/document/2>（数据接口目录）· <https://tushare.pro/document/1>（平台指南 / 权限 / SDK）
- **HTTP Gateway**：`POST http://api.tushare.pro`
- **Python SDK 安装**：`pip install tushare>=1.2.10`（部分 `pro_bar` 功能需 `>=1.2.39`）
- **AI 工具链**：<https://github.com/waditu-tushare/skills>（Skills）· `https://api.tushare.pro/mcp/token=<TUSHARE_TOKEN>`（MCP Server）

---

## 文档导航

按平台协议与资产业务域拆分为以下 10 篇短文档（涵盖全量 `document/2` 与核心 `document/1` 接口）：

| 序号 | 文档 | 覆盖范围 |
|---|---|---|
| 01 | [鉴权、通用协议、权限与坑点](01-auth-protocol-and-limits.md) | `TUSHARE_TOKEN`、Python SDK 与 HTTP POST、积分阶梯表、独立权限表、日期/分钟/复权/`update_flag` 约定 |
| 02 | [A 股基础数据与交易日历](02-a-share-basic-and-calendar.md) | 股票列表、盘前股本、交易日历、ST、沪深港通标的、曾用名、公司与高管、北交所映射、IPO 与历史列表（13 项） |
| 03 | [A 股行情、复权因子与停复牌](03-a-share-market-and-adj.md) | 日/周/月 K 线、实时日/分、历史分钟、`pro_bar`、复权因子、每日指标、涨跌停、停复牌、十大成交与平台标准化映射（16 项） |
| 04 | [A 股财务报表与参考数据](04-a-share-financials-and-ref.md) | 三大报表/业绩预告快报/分红/财务指标/审计/主营/披露日（10 项）与异动、股东、质押、回购、解禁、大宗等参考数据（15 项） |
| 05 | [特色数据、两融、资金流向与打板专题](05-a-share-special-margin-flow-hot.md) | 盈利预测/筹码/因子/竞价/调研（11 项）、两融转融通（7 项）、个股板块资金流向（7 项）与龙虎榜/连板/游资/THS·DC·TDX·KP（22 项） |
| 06 | [ETF、指数行业与公募基金](06-etf-index-and-funds.md) | ETF（PCF/份额/IOPV/实时分钟，15 项）、指数与申万/中信行业（23 项）、公募基金净值/分红/持仓（8 项） |
| 07 | [期货、现货黄金与期权](07-futures-options-and-spot.md) | 期货合约/日线复权/分钟/持仓/仓单/交易参数/南华指数（22 项 + Tick 网盘）、上海黄金（2 项）、期权（3 项） |
| 08 | [债券可转债、外汇、港股与美股](08-bonds-fx-hk-us.md) | 可转债/回购/国债收益率/财经日历（17 项）、外汇与储备资产（7 项）、港股（11 项）、美股与美股 ETF（11 项） |
| 09 | [宏观经济（国内、美国与韩国市场）](09-macro-economy.md) | 中国日程/利率（Shibor/LPR/Libor/Hibor/民间）/GDP/CPI/PPI/M0-M2/社融/PMI、美国利率/BLS/PCE、韩国 KOFIA（28 项） |
| 10 | [大模型语料、量化因子库、自选组合与 MCP](10-llm-corpus-factors-mcp.md) | 新闻/公告/研报/互动问答/政策（10 项）、272 量化因子库（2 项）、自选组合（4 项）、Tushare Skills 与 MCP Server |

---

## 在本量化平台中的接入与落盘约定

- **凭证管理**：Token 统一保存在 `config/.env` 的 `TUSHARE_TOKEN`，由 [`core.config.load_secrets()`](../../../core/config.py) 读取并注入环境，严禁在代码、文档、配置快照或日志中明文出现真实 Token（红线 8）。
- **适配器与限流**：由 [`TushareAdapter`](../../../data/adapters/tushare_adapter.py) 封装 `ts.pro_api(token).daily(trade_date=...)`（`requests_per_second = 5.0`），当前已支持 `asset_type = "stock"`，以 `trade_date`（`YYYYMMDD`）作为增量 watermark。
- **标准化与单位转换**：由 [`tushare_normalizer.normalize()`](../../../data/normalizers/tushare_normalizer.py) 映射至统一 `bars_daily` schema：`vol * 100`（手 → 股，`Int64`）、`amount * 1000.0`（千元 → 元）、合并 `suspend_d`（`'S'` → `True` / `' '` → `False`）与 `adj_factor`（缺省 `1.0`，绝不因 `suspend` 覆写 `adjfactor`）。
- **多源仲裁优先级**（[`source_priority.yaml`](../../../config/source_priority.yaml)）：
  - `bars_daily`：主源 `[tushare, akshare]`，`field_override.adjfactor = tushare`。
  - `bars_minute`：唯一源 `[tushare]`。
  - `fundamental`：`[akshare, tushare]`，`field_override.pe_ttm = tushare`。
  - `reference.calendar`：`[tushare, akshare]`；`reference.industry`：`[tushare]`。
- **存储布局**：标准化结果落盘至 `$QUANT_DATA_ROOT/asset/{nation}_{asset_type}_{data_type}/`（如 `cn_stock_daily`），并注册至 `$QUANT_DATA_ROOT/catalog.duckdb`（参见 [`storage_schema.md`](../../design/data/storage_schema.md) 与 [`ingestion.md`](../../design/data/ingestion.md)）。
