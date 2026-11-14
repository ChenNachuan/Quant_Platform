# AmazingData（中国银河证券星耀数智）文档目录

> 面向量化研究与实盘订阅的底层 gRPC over TLS 金融数据 SDK（`AmazingData` + Docker 托管容器 + MCP Server），覆盖沪深北股票、指数、ETF、可转债、逆回购、港股通、国债收益率、期货与 ETF 期权。

- **官方文档站点**：<https://amazing.ptradeapi.com/>（星耀数智 AmazingData API 文档，互链 Ptrade / QMT 文档）
- **官方开源仓库 / 安装包**：<https://github.com/Rockyzsu/AmazingData_API_Doc.git>（含 `tgw` 与 `AmazingData` wheel 包）
- **MCP 开源入口（WealthManager）**：<https://cloud.chinastock.com.cn/p/DWqgFyEQx2IYwJECIAA>
- **功能边界**：仅提供数据查询与实时行情订阅，**不支持自动下单**（实盘交易可配合 miniQMT / 本平台 [`QmtBroker`](../../../execution/broker.py)）。
- **运行环境与版本**：Python 3.8–3.13（Linux / Windows / 官方 Docker 镜像 `docker-amazingdata`）· SDK 基线 `V1.0.24`（算子扩展最低 `1.0.30`）

---

## 文档导航

按功能域与资产类别拆分为以下 8 篇短文档：

| 序号 | 文档 | 覆盖范围 |
|---|---|---|
| 01 | [认证、本地缓存与 Docker 执行](01-auth-cache-and-docker.md) | 推荐运行环境与安装、`ad.login` / `logout` / `update_password`、`DownloadInfoData` / `is_local`、`docker-compose.yml` 与 SDK 限制 |
| 02 | [基础数据与全量枚举](02-base-data-and-enums.md) | `BaseData` 日历/代码表/证券信息/复权因子（前/后复权公式）/ETF 申赎清单入口、`market` / `security_type` / `Period` 全表 |
| 03 | [行情查询与实时订阅](03-market-data-and-subscribe.md) | `MarketData` 历史 K 线与快照（严格区分 `HHMM` vs `HHMMSSmmm`）、`SubscribeData` 全 9 类实时回调与数据结构 |
| 04 | [A 股基本面、股本与交易异动](04-a-share-financials-and-corporate.md) | `InfoData` 证券基础/历史状态/财报三表/业绩快报预告/分红配股/限售解禁质押/股本股东/两融/龙虎榜与大宗交易 |
| 05 | [ETF、指数、可转债、国债与期权](05-etf-index-kzz-and-options.md) | ETF（PCF 申赎/份额/净值/IOPV）、指数与行业权重日线、可转债 11 项全量接口、国债收益率（`m3`–`y30`）、期权 10 标分批规则 |
| 06 | [技术指标库（57 项）](06-technical-indicators.md) | `TechnicalIndicators` 7 大类 57 个指标签名、默认参数、输出键名及前复权边界说明 |
| 07 | [量化算子库（108 项）](07-quant-operators.md) | `MathFunction`（23）、`TimeSeriesFunction`（51）、`StatisticsFunction`（18）、`CrossSectionFunction`（16）及未来函数提示 |
| 08 | [MCP Server 工具清单（26 项）](08-mcp-server.md) | `stdio` / Docker 环境变量配置、26 个 MCP Tool 入参与返回值清单、原文档勘误说明 |

---

## 在本量化平台中的接入与落盘约定

- **凭证与环境变量**：SDK 账号凭证（`AMAZINGDATA_USER` / `AMAZINGDATA_PASSWORD` / `AMAZINGDATA_HOST` / `AMAZINGDATA_PORT` 或宿主机 Docker `.env`）严禁写入 Git、配置快照或日志（红线 8）。
- **Docker 与脚本调度**：本仓库根目录 [`docker-compose.yml`](../../../docker-compose.yml) 将宿主机数据目录挂载至容器 `/data`、`./scripts` 挂载至 `/scripts`；[`ops.scheduler.fetch_stock_daily()`](../../../ops/scheduler.py) 通过 `docker-compose run --rm amazingdata` 调用容器内抓取脚本（严禁重建 `docker-amazingdata` 镜像，详见 [`AGENTS.md`](../../../AGENTS.md)）。
- **适配器与标准化层**：由 [`AmazingdataAdapter`](../../../data/adapters/amazingdata_adapter.py) 封装 SDK 限流（`requests_per_second = 2.0`）、`local_path` 强制 `str`、期权 10 标分批及 `close()` 释放连接；由 [`amazingdata_normalizer`](../../../data/normalizers/amazingdata_normalizer.py) 统一字段命名（优先采信 `NAV_ADJ_FACTOR` 且不因 `suspend` 覆写 `adjfactor`）。
- **落盘与目录注册**：标准化行情与元信息统一落盘至 `$QUANT_DATA_ROOT/asset/{nation}_{asset_type}_{data_type}/`（如 `cn_stock_daily`、`cn_option_basic`），并注册至持久化 DuckDB 目录 `$QUANT_DATA_ROOT/catalog.duckdb`（参见 [`storage_schema.md`](../../design/data/storage_schema.md) 与 [`source_priority.yaml`](../../../config/source_priority.yaml)）。
