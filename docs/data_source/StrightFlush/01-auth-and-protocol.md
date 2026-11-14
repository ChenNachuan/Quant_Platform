# 01. 鉴权、通用协议与限流

[← 返回目录](README.md)

---

## 1. 鉴权方式

1. 登录 [API Key 管理](https://fuyao.aicubes.cn/admin/) 创建具名 API Key（提交弹窗仅显示一次完整 Key，后续可在列表页管理）。
2. **REST API**：请求头携带 `X-api-key: <your-api-key>`。
3. **本量化平台配置**：
   - 将密钥写入 `config/.env`：
     ```dotenv
     STRIGHT_FLUSH_TOKEN=<your-api-key>
     ```
   - 官方 CLI / Python Toolkit / Agent Skill 默认读取环境变量 `HITHINK_FINANCE_API_KEY` 或用户级 `hithink-finance/credentials.env`。

### 最小 Python 调用示例

```python
import json
import urllib.request
from core.config import load_secrets

api_key = load_secrets("config/.env")["STRIGHT_FLUSH_TOKEN"]
url = "https://fuyao.aicubes.cn/api/a-share/prices/snapshot?thscodes=600519.SH,000001.SZ"
req = urllib.request.Request(url, headers={"X-api-key": api_key})

with urllib.request.urlopen(req, timeout=10) as resp:
    payload = json.loads(resp.read().decode("utf-8"))
    if payload["code"] != 0:
        raise RuntimeError(f"Fuyao API error {payload['code']}: {payload['message']}")
    items = payload["data"]["item"]
```

---

## 2. 通用协议约定

| 项目 | 约定说明 |
|---|---|
| **Base URL** | `https://fuyao.aicubes.cn` |
| **路由规范** | `/api/<标的宇宙>/<数据类型>/<动作>`（Market Dumps 页面用 `/dump/**` Cookie 入口，API 用 `/api/dump/**` + `X-api-key`） |
| **时间戳与时区** | 统一为毫秒级 Unix 时间戳（`long`），按 `Asia/Shanghai` 解释（日线与报告期末通常对齐东八区 `00:00:00`） |
| **价格与币种** | 原始货币计价，A 股/指数/公募基金默认 `CNY`，港股 IPO 为 `HKD`；财报金额单位为原币元，`basic_eps` 为元/股 |
| **百分比与比例** | 大多数行情/指标字段为**百分数原值**（如 `1.74` 表示 `1.74%`）；少数特色/龙虎榜字段为**小数比例**，需以具体接口返回表为准 |
| **空值语义** | 字段为 `null` 表示“当期未披露”或“上游缺失”，透传不补零；合法空列表返回 `[]`；记录中无法确定 `thscode` 时可为 `null` |
| **代码规范** | 必须传完整 `thscode`（如 `600519.SH`），禁止传纯数字 `ticker`；基金经理与基金公司分别使用 `manager_id`、`company_id` |

---

## 3. 统一响应信封 (`ApiResponse`)

正常进入业务处理的请求（含业务错误）均返回 **HTTP 200**，由响应体内 `code` 区分成败；触发接入限流时可能返回 **HTTP 429**。

```json
{
  "code": 0,
  "message": "success",
  "request_id": "a1b2c3d4e5f6789012345678abcdef01",
  "data": {
    "timestamp": 1716105600000,
    "item": []
  }
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `code` | integer | 业务结果码，`0` 表示成功，非 `0` 表示业务错误 |
| `message` | string | 结果描述 |
| `request_id` | string | 请求追踪 ID（排障时务必保留） |
| `data` | object \| array \| null | 业务数据容器（错误时通常为 `null`） |
| `data.timestamp` | long \| null | 数据有效/组装时间（毫秒） |
| `data.item` / `data.items` | array | 业务记录数组（资讯域多数用 `items`，其余多数用 `item`） |

---

## 4. 原生批量查询统一规则

以下 6 个批量历史/报表接口共用同一套原生批量契约：

| 数据域 | 端点 | 单次代码上限 |
|---|---|---|
| A 股历史 K 线 | `GET /api/a-share/prices/historical-batch` | 10 |
| 指数历史 K 线 | `GET /api/a-share-index/prices/historical-batch` | 10 |
| ETF 历史日线 | `GET /api/fund/market/historical-batch` | 10 |
| A 股合并利润表 | `GET /api/a-share/financials/income-statements-batch` | 10 |
| A 股资产负债表 | `GET /api/a-share/financials/balance-sheets-batch` | 10 |
| A 股现金流量表 | `GET /api/a-share/financials/cash-flow-statements-batch` | 10 |

- **去重前校验上限**：先按逗号分割检验原始代码数量（`<= 10`，重复代码占额度），再 trim、规范化并稳定去重，按首次出现顺序返回。
- **部分有效语义**：未知或资产类型不匹配的代码会被省略；只要仍有有效目标，外层仍返回 `code=0`。有效标的无记录时保留 `{thscode, timestamp: null, item: []}`。
- **全无效语义**：去重后仅一个目标且类型不支持返回 `3004`；其余零有效目标情况返回 `3001`。
- **大批量快照例外**：`GET /api/fund/market/snapshot`、`GET /api/a-share/auction/snapshot`、`GET /api/a-share/valuations/snapshot` 与 `GET /api/a-share-index/constituents/ths-index-list-by-stock` 支持最多 **100 个**代码。

---

## 5. 限流策略与完整错误码

- **限流策略**：服务当前不限制累计调用次数，但会根据实时负载动态限流。收到 **HTTP 429** 或响应体 `code=4001` 时均应退避重试，降低并发与请求频率。
- **完整业务错误码**：

| `code` | 含义 | 典型触发场景 |
|---|---|---|
| `0` | 成功 (`success`) | 请求成功处理 |
| `1001` | 缺少必填参数 | 漏传 `thscode`、`start`、`end`、`q` 等必填字段 |
| `1002` | 参数格式错误 | 代码格式非法、空逗号段、日期格式非 `yyyyMMdd` / `YYYY-MM-DD`、枚举白名单不匹配 |
| `1003` | 参数取值越界 | `limit <= 0`、`start > end`、查询窗口超过接口上限（1 年/5 年/10 年）、`thscodes` 数量超限 |
| `1004` | 取数模式冲突 | 财报接口混用 `limit` 与 `start/end`，或只传单边半开区间 |
| `2001` | 未认证 | `X-api-key` 缺失或无效 |
| `2003` | 权限不足 | 当前 API Key 无权访问该 capability |
| `3001` | 标的不存在 | 找不到有效目标标的（如全部代码未知或不在代码表） |
| `3002` | 数据未就绪 | 标的存在但当前暂无可用业务数据 |
| `3004` | 标的类型不支持 | 目标资产类型不支持所请求的能力（如向股票接口传指数/ETF） |
| `4001` | 频率超限 | 触发动态限流（同 HTTP 429，资讯事件检索另有单实例 5 QPS 上限） |
| `5001` | 服务内部错误 | 服务端未知错误 |
| `5002` | 上游服务超时 | 上游数据源响应超时 |
| `5003` | 数据源不可用 | 上游服务暂不可用或返回异常无法按契约映射 |
