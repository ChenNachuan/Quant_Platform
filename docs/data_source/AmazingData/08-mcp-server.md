# 08. AmazingData MCP Server 工具清单（26 项）

- **开源来源**：中国银河证券星耀数智开源金融智能体 **WealthManager**（<https://cloud.chinastock.com.cn/p/DWqgFyEQx2IYwJECIAA>）
- **本地/容器环境变量**：`user` / `AMAZINGDATA_USER`、`password` / `AMAZINGDATA_PASSWORD`、`host` / `AMAZINGDATA_HOST`、`port` / `AMAZINGDATA_PORT`

---

## 1. 客户端配置示例

### Docker 容器方式（本平台推荐）

```json
{
  "mcpServers": {
    "AmazingData": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "--env-file", "/path/to/docker/.env",
        "amazingdata-mcp"
      ]
    }
  }
}
```

### 本地 `stdio` Python 方式

- `type`: `"stdio"`，`command`: Python 3.12 路径，`args`: `[".../WealthManager/mcp_server/server.py"]`，并在 `env` 中配置 `user` / `password` / `host` / `port`。
- 启动本地 MCP 服务参见 [`AGENTS.md`](../../../AGENTS.md)（不要修改 `~/AmazingData/docker/docker-compose.yml`）。

---

## 2. 已注册 MCP Tool 清单（26 个）

### 2.1 基础、日历与行情（10 个）

| # | Tool 名称 | 核心参数 | 返回类型 | 说明 |
|---|---|---|---|---|
| 1 | `mcp_kzz` | — | `int` | 查询可转债代码数量 |
| 2 | `mcp_code_info` | `security_type` | `dict` | 每日最新证券信息（交易日 9:00 前更新） |
| 3 | `mcp_code_list` | `security_type` | `list` | 每日最新代码表（无历史） |
| 4 | `mcp_backward_factor` | `code_list=[]` | `dict` | 获取并缓存后复权因子 |
| 5 | `mcp_calendar` | `market='SH'`, `date` | `list` | 查询交易日历 |
| 6 | `mcp_stock_basic` | `code_list=[]` | `DataFrame` | 上市公司基础资料 |
| 7 | `mcp_history_stock_status` | `code_list=[]`, `begin_date`, `end_date` | `DataFrame` | 日频历史证券状态（ST / 停牌 / 涨跌停） |
| 8 | `mcp_bj_code_mapping` | — | `DataFrame` | 北交所新旧代码对照表 |
| 9 | `mcp_kline` | `code_list`, `begin_date`, `end_date`, `begin_time=900`, `end_time=1700`, `period=day` | `dict` | 历史 K 线 |
| 10 | `mcp_snapshot` | `code_list` | `dict` | 最新快照数据 |

### 2.2 财报、股本、两融与交易行为（16 个）

| # | Tool 名称 | 核心参数 | 返回类型 | 说明 |
|---|---|---|---|---|
| 11 | `mcp_balance_sheet` | `code_list`, `statement_type`, `report_type`, `begin_date`, `end_date` | `dict` | 资产负债表 |
| 12 | `mcp_cash_flow` | 同 `mcp_balance_sheet` | `dict` | 现金流量表 |
| 13 | `mcp_income` | 同 `mcp_balance_sheet` | `dict` | 利润表 |
| 14 | `mcp_profit_express` | `code_list`, `begin_date`, `end_date` | `dict` | 业绩快报 |
| 15 | `mcp_profit_notice` | `code_list`, `report_type`, `begin_date`, `end_date` | `dict` | 业绩预告 |
| 16 | `mcp_share_holder` | `code_list`, `begin_date`, `end_date` | `dict` | 十大股东 |
| 17 | `mcp_holder_num` | `code_list`, `begin_date`, `end_date` | `dict` | 股东户数 |
| 18 | `mcp_equity_structure` | `code_list`, `begin_date`, `end_date` | `dict` | 股本结构 |
| 19 | `mcp_equity_pledge_freeze` | `code_list`, `begin_date`, `end_date` | `dict` | 股权冻结 / 质押 |
| 20 | `mcp_equity_restricted` | `code_list`, `begin_date`, `end_date` | `dict` | 限售股解禁 |
| 21 | `mcp_dividend` | `code_list`, `begin_date`, `end_date` | `dict` | 分红数据 |
| 22 | `mcp_right_issue` | `code_list`, `begin_date`, `end_date` | `dict` | 配股数据 |
| 23 | `mcp_margin_summary` | `begin_date`, `end_date` | `dict` | 融资融券汇总 |
| 24 | `mcp_margin_detail` | `code_list`, `begin_date`, `end_date` | `dict` | 融资融券明细 |
| 25 | `mcp_long_hu_bang` | `code_list`, `begin_date`, `end_date` | `dict` | 龙虎榜数据 |
| 26 | `mcp_block_trading` | `code_list`, `begin_date`, `end_date` | `dict` | 大宗交易数据 |

> **原官方文档勘误提示**：原汇总文档第 25 项曾误把工具名写成 `mcp_holder_num (龙虎榜数据)`、第 26 项 `mcp_block_trading` 描述误写为“股东户数”；实际应分别对应 `mcp_long_hu_bang`（龙虎榜）与 `mcp_block_trading`（大宗交易），调用前建议在 MCP 客户端内通过 `tools/list` 核实工具名。
