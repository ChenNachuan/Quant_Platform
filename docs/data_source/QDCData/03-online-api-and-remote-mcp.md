# 03. 在线 API、Python SDK 与远程 MCP

[← 返回目录](README.md)

> 官网 API 文档：<https://qdc-data.com/api.html> · 试用文档：<https://qdc-data.com/trial.html>
> **边界定位**：适用于按需查询、盘后每日复盘、云端批量计算选股及 AI 助手在线分析，**非盘中实时行情服务**（盘后数据入库后可用 `qdc.data_through(sku)` 或 `/v1/describe` 的 `coverage.end` 确认时效）。

---

## 1. 认证与线路配置

- **共用凭据**：Python SDK、HTTP RESTful API 与远程 Streamable HTTP MCP **共用同一 API 密钥**（`QDC_API_KEY`，试用 Key 以 `QAK` 开头）。
- **设备标识**：建议同步传入 `X-Device`（HTTP/MCP 请求头）或 `device`（Python SDK 参数），同一台机器使用 Python 与远程 MCP 时保持相同标识。
- **平台配置（红线 8）**：密钥保存在 `config/.env`（`QDC_API_KEY=...`、`QDC_DEVICE=...`），严禁写入代码、文档或快照。

---

## 2. Python SDK（`pip install qdc`）

支持 Python `>= 3.8`，同时提供自有研究函数、兼容 Tushare 习惯的 `pro_api` 封装，以及 AI 包服务端在线计算接口。

```python
import qdc

# 初始化连接（line="auto" 自动选择国内或海外线路，cache_dir 可启用本地缓存）
qdc.connect("<QDC_API_KEY>", device="my-pc", line="auto", cache_dir="~/.qdc")

# 1. 标准日线与复权查询（返回 pandas DataFrame）
df = qdc.daily("600519.SH", "20240101", "20241231", adjust="qfq")

# 2. Tushare Pro 兼容写法
pro = qdc.pro_api("<QDC_API_KEY>")
df_ts = pro.daily(ts_code="000001.SZ", start_date="20240101", end_date="20241231")

# 3. 查询数据时效
print(qdc.data_through("cn_tick"))

# 4. 服务端在线计算（AI 包，0.2.5+ 起支持复合选股/复盘）
k5 = qdc.bars("cn_tick", "600519", "20240603", "20240607", freq="5m")
st = qdc.stats("cn_daily_full", "600519", "20240101", "20241231", metrics=["close", "pct_chg", "amplitude"])

# 多表联动全市场盘后选股
qdc.screen_day(board="up", sort="streak")
qdc.screen_day(new_high_days=60, turnover_min=5, float_mktcap_max=5e9)
qdc.screen_day(cross_up_ma=250, vol_ratio_min=1.5)
qdc.screen_day(roe_min=15, profit_yoy_min=30, pe_max=30, pe_min=0)

qdc.profile("600519")
qdc.perf("20260101", "20260930", top=20)
qdc.limit_stats(by="seal_time")
```

---

## 3. HTTP RESTful API（`https://data.qdc-data.com`）

### 3.1 元数据与结构发现端点

请求头均需携带 `X-API-Key: <QDC_API_KEY>`（建议同时带 `X-Device`），服务端默认支持 `gzip` 压缩（传输体积约为 1/6～1/10）。

| 端点（`GET`） | 关键入参 | 返回说明 |
|---|---|---|
| `/v1/datasets` | `keyword`、`only_bought=true` | 列出数据集清单、中文名与账号授权情况 |
| `/v1/tables` | `sku`（如 `cn_fin_statements`） | 列出指定数据集下的全部子表名称及中文名 |
| `/v1/describe` | `sku`, `table`, `metadata_only=true` | 返回表字段（`name`/`type`/`description`）、覆盖起止（`coverage.end`）及字典对齐状态（`dictionary.schema_status` / `not_returned_fields`） |

### 3.2 通用取数端点 `/v1/query` 与 Parquet 游标分页

`/v1/query` 支持 `format=json`（默认）与 **`format=parquet`**（`Content-Type: application/vnd.apache.parquet`，传输体积约为 JSON 的 1/8～1/15，行列与 JSON 完全一致）。

- **Parquet 二进制分页响应头**：
  - `X-QDC-Row-Count`：本次返回行数
  - `X-QDC-Truncated`：`true` / `false`，是否因触达单次上限被截断
  - `X-QDC-Next-Cursor`：下一页游标（翻页时**必须连同原始查询参数一并提交**）
  - `X-QDC-Meta`：请求与额度元信息

```python
import io, requests, pandas as pd

BASE = "https://data.qdc-data.com"
HEAD = {"X-API-Key": "<QDC_API_KEY>", "X-Device": "my-pc"}
params = {
    "sku": "stocks_1m",
    "code": "000333.XSHE",
    "start": "2026-09-01",
    "end": "2026-09-30",
    "format": "parquet",
}

frames = []
while True:
    r = requests.get(f"{BASE}/v1/query", params=params, headers=HEAD, timeout=120)
    r.raise_for_status()
    frames.append(pd.read_parquet(io.BytesIO(r.content)))
    if r.headers.get("X-QDC-Truncated", "").lower() != "true":
        break
    params["cursor"] = r.headers["X-QDC-Next-Cursor"]

df = pd.concat(frames, ignore_index=True)
```

### 3.3 AI 包在线计算 HTTP 端点汇总

| 端点（`GET`） | 核心能力与关键参数 |
|---|---|
| `/v1/screen` | 全市场盘后选股（涨跌停/炸板/连板 `board=up&sort=streak`、技术均线/MACD/KDJ/RSI、财务 ROE/增速/估值、`sector=`、`exclude_st`，金额单位元） |
| `/v1/board_rank` | 行业/概念板块排行（`kind=industry&sort=main_net`，含涨跌幅、主力净流入、涨跌家数、领涨股） |
| `/v1/profile` | 个股全景画像（`code=600519`：行情、近 5/20/60/120/250 日涨跌、3 年估值分位、板块、涨停历史、近 5 日主力、龙虎、两融、解禁、预告分红、近 3 年财务） |
| `/v1/market` | 全市场每日复盘（涨跌分布、涨停/跌停/炸板/封板率、连板梯队、成交额、主指数与领涨领跌板块） |
| `/v1/perf` | 区间表现排行（复权涨跌幅、最大回撤、年化波动率、振幅、涨停炸板次数；单次最长：全市场 1 年 / 板块 3 年 / 指定股票 10 年） |
| `/v1/limit_stats` | 涨停/炸板/跌停次日表现统计（可按 `by=seal_time` 封板时间、连板数、开板次数、月份分组） |
| `/v1/lhb` | 龙虎榜与营业部跟踪（支持 `seat=` 按游资营业部名称反查参与个股） |
| `/v1/calendar` | 未来已公布事件日历（`types=unlock` 解禁、财报披露预约、除权除息） |
| `/v1/fut_rank` | 国内期货品种日度排行（涨跌、成交、持仓增减、现货价与基差） |
| `/v1/bars` | 在线合成 K 线（股票 `cn_tick`/分钟/日线合周月，支持 `adj=qfq/hfq`；期货 `cn_futures_tick` 与 `cn_futures_1m` 填品种如 `RB`/`AG` 合成，Tick 单次限 `品种数×天数 <= 31`，分钟最长 2 年） |
| `/v1/stats` | 区间/逐笔条件统计（支持 `side=buy`、`min_lots=500`、`time_to=10:00` 等盘口成交过滤） |

---

## 4. 配额、限流与错误码

- **每日行额度**：按返回行数计入，**在线计算按服务端计算读取的行数计入**；每日 **北京时间 00:00** 重置；每次响应 `meta` 包含 `daily_rows_used` / `daily_rows_limit` / `daily_rows_left` / `daily_used_pct`。
- **单次查询与跨度约束**：
  - 高密度数据（如逐笔 Tick）单次查询跨度 **不得超过 14 个自然日**（约 10 个交易日），超出返回 **`HTTP 422`** 并在提示中给出分段建议。
  - `/v1/query` 通用单次返回上限为 **5 万行或 50 MB**（基础套餐按档位约 20 万行/20 MB，**正式 AI 包支持单次 100 万行、300 次/分、双机互备保障**），超出触发 `truncated=true` 返回 `next_cursor`。
- **异常状态码**：
  - `403`：账号未购买对应数据集或 AI 包，或仅限 `api_only` / `trial` 范围受限（试用期通常为 A 股 K 线近 2 年、其它数据近半年）。
  - `422`：日期跨度超 14 日（逐笔等高密度表）或参数校验不通过。
  - `429`：触发频次限流，读取响应头 **`Retry-After`** 退避重试。

---

## 5. 远程 Streamable HTTP MCP（`https://data.qdc-data.com/mcp`）

仅支持 **Streamable HTTP（`streamableHttp`）**（不支持纯 SSE；Claude Desktop 等仅支持本地 `stdio` 的客户端需用 `npx -y mcp-remote` 转接）。

### 5.1 客户端配置范例（Cursor / Claude Code / Cherry Studio）

```json
{
  "mcpServers": {
    "qdc-data": {
      "type": "http",
      "url": "https://data.qdc-data.com/mcp",
      "headers": {
        "X-API-Key": "<QDC_API_KEY>",
        "X-Device": "my-pc"
      }
    }
  }
}
```

> Claude Code CLI 快捷接入：`claude mcp add --transport http qdc-data https://data.qdc-data.com/mcp --header "X-API-Key: <QDC_API_KEY>" --header "X-Device: my-pc"`。

### 5.2 在线 18 个 MCP 工具清单

| 分类 | 工具名 | 对应能力 / HTTP 映射 |
|---|---|---|
| **查询取数（7 个）** | `search_code` | 按名称查股票/基金/期货证券代码 |
| | `list_datasets` | 列出当前密钥可查数据集（`/v1/datasets`） |
| | `describe` | 查询指定表字段定义、类型与覆盖时间（`/v1/describe`） |
| | `get` | 按代码与日期取原始记录（`/v1/query`） |
| | `latest` | 获取目标表最新一期横截面数据 |
| | `screen` | 按单表字段条件筛选记录 |
| | `download_day` | 获取某日全市场整包文件下载链接（适合成批取数，计入 API 行数） |
| **在线计算 · AI 包（11 个）** | `screen_day` | 跨表全市场复合选股（`/v1/screen`） |
| | `stock_profile` | 个股综合画像（`/v1/profile`） |
| | `market_day` | 全市场盘后复盘（`/v1/market`） |
| | `board_rank` | 行业/概念板块涨跌与资金流排行（`/v1/board_rank`） |
| | `perf_rank` | 区间涨跌幅、回撤与波动率排行（`/v1/perf`） |
| | `limit_stats` | 涨停/炸板/跌停次日表现统计（`/v1/limit_stats`） |
| | `lhb_rank` | 龙虎榜与营业部席位检索（`/v1/lhb`） |
| | `event_calendar` | 解禁、财报披露、除权除息未来日程（`/v1/calendar`） |
| | `futures_rank` | 国内期货品种行情/持仓/基差排行（`/v1/fut_rank`） |
| | `bars` | 在线合成任意周期 K 线及复权（`/v1/bars`） |
| | `stats` | 区间与逐笔条件聚合统计（`/v1/stats`） |
