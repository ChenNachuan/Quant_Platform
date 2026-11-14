# QDCData 数据源文档目录

> 面向个人与机构量化研究的高性价比全市场金融数据服务（`qdc-cli` 本地命令行下载器 / 本地 stdio MCP Server / Python SDK `qdc` / HTTP RESTful API / 远程 Streamable HTTP MCP Server / 网盘 L2 原始归档），官方目录共收录 **111 个在售数据集（200+ 张数据表）**，覆盖沪深北 A 股（含退市股）、指数与申万/通达信/同花顺/东财板块、ETF、可转债、期权、国内商品期货、港股、美股、全球股指/期货/外汇金属、数字货币、新闻舆情与宏观另类数据。

- **官网与文档入口**：<https://qdc-data.com/> · [格式说明](https://qdc-data.com/format.html)（[`format.md`](https://api.qdc-data.com:8443/samples/format.md?v=2)） · [数据文档](https://qdc-data.com/docs.html) · [数据字典 JSON](https://qdc-data.com/data_dict.json) · [免费样本](https://qdc-data.com/samples.html)
- **命令行与下载器**：`qdc-cli`（v1.3.0 / 下载页：<https://qdc-data.com/downloads.html>），支持断点续传、CRC 校验对账、`Parquet + zstd` 分卷落盘、本地转 `duckdb` / `csv` / `xlsx`、本地周期合并与复权，内置 stdio MCP Server（`qdc-cli mcp`，10 个本地管理工具）。
- **在线 API 与远程 MCP**：<https://qdc-data.com/api.html> · `pip install qdc` · HTTP Base `https://data.qdc-data.com` · 远程 MCP `https://data.qdc-data.com/mcp`（18 个在线工具：7 个查询取数 + 11 个 AI 包在线计算；**仅限盘后研究，非盘中实时行情服务**）。

---

## 文档导航

按交付协议与资产业务域拆分为以下 7 篇短文档（涵盖全量 `qdc-cli`、`format.md?v=2`、`api.html` 与 `data_dict.json` 111 个在售数据集）：

| 序号 | 文档 | 覆盖范围 |
|---|---|---|
| 01 | [命令行下载器与本地 MCP](01-cli-downloader-and-local-mcp.md) | `qdc-cli` 全命令与环境变量鉴权、`convert` 不可逆与先加工铁律、本地 MCP 10 工具、`cn_tick_l2` 网盘 7z 工作流 |
| 02 | [存储格式、时区与防踩坑约定](02-format-timezone-and-pit-conventions.md) | Parquet 分卷、`ts` 毫秒戳与 DuckDB `TRY_CAST` 陷阱、北京日 vs UTC 日双口径、代码后缀映射、复权因子基线行三铁律、单位/口径坑汇总 |
| 03 | [在线 API、Python SDK 与远程 MCP](03-online-api-and-remote-mcp.md) | `qdc.connect` / `qdc.pro_api`、`/v1/query` Parquet 流与 `X-QDC-*` 游标分页、限额与 `422`/`429`、远程 MCP 18 工具清单 |
| 04 | [A 股行情、指数、板块、ETF、可转债与期权](04-a-share-bars-tick-and-derivatives.md) | 股票行情（10）、复权与竞价/技术因子（6）、指数（8）、板块与申万/题材（5）、ETF 全套（13）、可转债（4）、A 股期权（2）共 48 项 |
| 05 | [A 股财务、预期、资金流、事件与参考数据](05-a-share-financials-flows-and-ref.md) | 财务三表/指标/估值/预告/分部（5）、评级与 PIT 一致预期（2）、资金流/两融/龙虎/打板/大宗/筹码（7）、分红/解禁/股东/治理/状态日历（11）共 25 项 |
| 06 | [国内期货、港股、美股、外汇国际与数字货币](06-futures-hk-us-fx-and-crypto.md) | 国内期货与期权基差（8）、港股（5）、美股与 SEC XBRL（2）、外汇金属与全球股指/外盘期货（5）、数字货币 `crypto` 6 表（1）共 21 项 |
| 07 | [新闻事件、宏观经济与另类稀缺数据](07-news-macro-and-alternative.md) | 新闻统一库/事件流/信息层/快讯/联播（5）、利率/中国宏观/全球宏观/EIA/USDA/CMO/航运/研报/机构持仓 13F/关税制裁（12）共 17 项 |

---

## 全库 111 个在售数据集分组统计

数据口径依据官方 `data_dict.json`（生成日期 `2026-11-12`），与本地 `qdc-cli catalog` 返回的 111 个 SKU 完全一致：

| 分组 | 数量 | 核心代表数据集 | 对应文档 |
|---|---:|---|---|
| A 股（行情/衍生/板块/ETF/转债/期权） | 48 | `stocks_1m..60m`、`cn_daily_full`、`cn_tick`/`cn_tick_l2`、`cn_adj_factor`、`cn_board`、`cn_sw_index`、`cn_etf_*`、`cn_cb_*`、`cn_opt_*` | [04 篇](04-a-share-bars-tick-and-derivatives.md) |
| A 股（财务/预期/资金/事件/治理/日历） | 25 | `cn_fin_statements`、`cn_valuation_1d`、`cn_estimates`、`cn_hsgt`、`cn_lhb`、`cn_limit_sentiment`、`trade_calendar` | [05 篇](05-a-share-financials-flows-and-ref.md) |
| 国内期货 | 8 | `cn_futures_1d`/`1m`/`tick`/`aux`、`cn_futures_inventory`/`receipt`、`cn_opt_commodity_1d`、`cn_spot_basis` | [06 篇](06-futures-hk-us-fx-and-crypto.md) |
| 港股 / 美股 | 7 | `hk_daily`/`1m`/`index_1d`/`fin_statements`/`cn_hk_ccass`、`us_stocks_1d`/`us_fundamentals` | [06 篇](06-futures-hk-us-fx-and-crypto.md) |
| 国际 / 外汇 | 5 | `cn_fx_1d`/`fx_1m`（67 品种）、`global_index_1d`、`intl_ref`、`us_futures_1d` | [06 篇](06-futures-hk-us-fx-and-crypto.md) |
| 数字货币 | 1 | `crypto`（币安现货 94 币 + 永续合约 43 币 × 日/时/1m 共 6 张表，UTC 日口径） | [06 篇](06-futures-hk-us-fx-and-crypto.md) |
| 新闻与信息层 | 5 | `imai_news`、`imai_events`、`imai_infolayer`、`news_flash`、`news_cctv` | [07 篇](07-news-macro-and-alternative.md) |
| 宏观 / 另类 / 稀缺 | 12 | `cn_macro_monthly`、`cn_rate_cn`、`imai_macro`、`imai_insthold`、`imai_eia`、`imai_usda`、`cmo_commodity`、`imai_shipping`、`imai_research*`、`intl_*` | [07 篇](07-news-macro-and-alternative.md) |

---

## 在本量化平台中的接入与落盘约定

- **凭证管理（红线 8）**：CLI 登录凭据由本机 `qdc-cli login` 持久化管理（不走 API Key）；如额外启用在线 HTTP REST 通道（`mode="http"`），API 密钥（`QDC_API_KEY`、`QDC_DEVICE`）与可选环境变量（`QDC_EMAIL`、`QDC_PASSWORD`）统一存放于本地 `config/.env`（已被 `.gitignore` 忽略），由 [`core.config.load_secrets()`](../../../core/config.py#L44-L57) 读取，**严禁在代码、文档、配置文件、配置快照或日志中明文写入真实邮箱、密码或 API Key**。
- **适配器与落盘契约**：由 [`QdcAdapter`](../../../data/adapters/qdc_adapter.py) 与 [`qdc_normalizer`](../../../data/normalizers/qdc_normalizer.py) 提供接入：默认采用 **`qdc-cli` 模式**（自动读取 `qdc-cli config` 的 `download_root/QDCData/` 并按年懒加载 `qdc-cli get <sku> --years <YYYY>`，也可用 `QDC_DATA_DIR` / `local_dir` 直接指定本地目录，不消耗在线 API 行数额度），同时保留可选 `mode="http"` 通道（`/v1/query` Parquet 游标分页、`requests_per_second = 5.0`）；批量历史迁移落盘遵循绑定契约 [`QDC_ingestion_contract.md`](../../design/data/QDC_ingestion_contract.md)（`qdc-m1-v1`）清洗落盘至 `$QUANT_DATA_ROOT`（参见 [`storage_schema.md`](../../design/data/storage_schema.md) 与 [`config/settings.toml`](../../../config/settings.toml)）。
- **三层存储结构**：
  - **Schema 层**（`$QUANT_DATA_ROOT/asset/{nation}_{asset_type}_{data_type}/`）：通过 `pandera` 校验的标准化表（batch-1 覆盖 `bars_daily`、`fundamental`、`industry`、`corporate_actions` 等），并注册至 `$QUANT_DATA_ROOT/catalog.duckdb`。
  - **Ext 层**（`$QUANT_DATA_ROOT/ext/<dataset>/<subtable>/<volume>.parquet`）：Schema 暂未容纳但须留痕的扩展源表（财务三表/指标、预期、评级、股本、ST/停复牌、回购/增减持/质押/解禁、指数成分等）。
  - **Manifest 层**（`$QUANT_DATA_ROOT/manifest/<table>.json` 与 `ext_<dataset>_<subtable>.json`）：逐源分卷统计 `rows_in = rows_out + rejected` 与校验摘要供对账。
- **核心字段归一规则**：
  - **代码后缀转换**：QDC `symbol`（`.XSHG`/`.XSHE`/`.XBSE`）按 `XSHG → SH`、`XSHE → SZ`、`XBSE → BJ` 转为平台 `ticker`；裸 6 位代码按 [`core/asset_rules.py`](../../../core/asset_rules.py) 的 `PREFIX_RULES` 推导交易所，未映射前缀整行拒收并在 manifest 计数。
  - **时间与单位换算**：行情 `ts` 按 `Asia/Shanghai` 转为 `DATE` 并断言无 `NaT`（`crypto` 保留 `UTC`）；`cn_tick` 的 `volume` 为整数手须 `× 100` 转股；`cn_cb_*` 的 `volume` 为手（1 手 = 10 张）；`cn_tick_l2` 价格为万分之一元须 `÷ 10000` 转元。
