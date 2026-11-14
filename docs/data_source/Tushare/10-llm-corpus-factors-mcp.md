# 10. 大模型语料、量化因子库、自选组合与 MCP

[← 返回目录](README.md) · 对应官网 `doc_id=142 / 485 / 474` 与 `doc1_id=450 / 463`

---

## 1. 大模型语料（10 项，`doc_id=142`）

除 `fintech_award` 免费及 `research_report` 外，其余均需独立开通对应权限。

| `doc_id` | 名称 | `api_name` | 核心入参 | 核心输出字段 | 权限 & 更新/上限 |
|---|---|---|---|---|---|
| `406` | 国家政策库 | `npr` | `org`, `start_date`, `end_date`, `ptype` | 7 列：`pubtime`, `title`, `url`, `content_html`, `pcode`, `puborg`, `ptype` | **独立权限**（“政策法规库”，1,000/年）；500 行/次 |
| `415` | 券商研究报告 | `research_report` | `trade_date`, `start_date`, `end_date`, `report_type`, `ts_code`, `inst_csname`, `ind_name` | 10 列：`trade_date`, `abstr`, `title`, `report_type`, `author`, `name`, `ts_code`, `inst_csname`, `ind_name`, `url` | 券商研报库（500/年）；每 12h 更新，1,000 行/次 |
| `465` | 央行货币政策执行报告 | `monetary_policy` | `start_date`, `end_date` | 5 列：`pub_date`, `title`, `url`, `pdf_url`, `content_html` | **独立权限**（200/年）；1,000 行/次 |
| `143` | 新闻快讯（短讯） | `news` | `start_date`, `end_date`（`YYYY-MM-DD HH:MM:SS`）, `src`（`sina`/`wallstreetcn`/`10jqka`/`eastmoney`/`yuncaijing`/`fenghuang`/`jinrongjie`/`cls`/`yicai`） | 4 列：`datetime`, `content`, `title`, `channels` | **独立权限**（“新闻资讯”，1,000/年）；每小时第 50 分更新，1,500 行/次 |
| `195` | 新闻通讯（长篇） | `major_news` | `src`, `start_date`, `end_date` | 4 列：`title`, `content`, `pub_time`, `src` | **独立权限**（“新闻资讯”）；每 2h 第 50 分更新，400 行/次 |
| `154` | 新闻联播文字稿 | `cctv_news` | `date`（`YYYYMMDD`） | 3 列：`date`, `title`, `content` | **独立权限**（“新闻资讯”） |
| `176` | 上市公司公告 | `anns_d` | `ts_code`, `ann_date`, `start_date`, `end_date` | 6 列：`ann_date`, `ts_code`, `name`, `title`, `url`, `rec_time` | **独立权限**（“公告数据”，1,000/年）；2,000 行/次 |
| `366` | 上证 e 互动问答 | `irm_qa_sh` | `ts_code`, `trade_date`, `start_date`, `end_date`, `pub_start`, `pub_end` | 6 列：`ts_code`, `name`, `trade_date`, `q`, `a`, `pub_time` | **独立权限**（“沪深董秘问答”，500/年）；3,000 行/次 |
| `367` | 深证易互动问答 | `irm_qa_sz` | 同上 | 7 列（另含 `industry`） | **独立权限**（“沪深董秘问答”）；3,000 行/次 |
| `534` | 金融科技发展奖获奖 | `fintech_award` | `year`, `org`, `start_year`, `end_year` | 7 列：`year`, `seq_no`, `project_name`, `lead_org`, `participant_org`, `contributors`, `award_grade` | 免费；一次提取全部 |

---

## 2. 量化因子库（2 项，272 因子，`doc_id=485`）

数据回溯至 **2010 年**、每日更新，覆盖 9 大类共 **272 个因子**：`Alpha101`（101）、`Growth`（15）、`Liquidity`（35）、`Momentum`（20）、`Quality`（59）、`Reversal`（3）、`Risk`（25）、`Size`（3）、`Value`（11）。

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段 | 权限 & 上限 |
|---|---|---|---|---|---|
| `486` | 因子列表 | `factor_list` | `factor_name`, `asset_type`, `factor_type` | `factor_name`, `asset_type`, `factor_type`, `factor_desc` | **独立权限**（“因子库”，个人 2,000/年，机构 2 万/年）；一次全取 |
| `490` | 因子数据 | `factor_value` | `ts_code`, `factor_name`, `trade_date`, `start_date`, `end_date` | `factor_name`, `ts_code`, `trade_date`, `factor_value` | **独立权限**（5,000+ 积分可直接试用）；6,000 行/次 |

---

## 3. 自选组合管理（4 项，`doc_id=474`）

用于将选股结果持久化至 Tushare 云端，每账号上限 **200 个组合**：

| `doc_id` | 名称 | `api_name` | 核心入参 | 说明 |
|---|---|---|---|---|
| `446` | 组合列表 | `p_list` | `name`（可选） | 返回 `id`, `name`, `desc`, `create_time`, `update_time` |
| `449` | 成分查询 | `p_get` | `name` | 返回组合内 `ts_code`, `ts_type`, `name`, `desc`, `weight`, 时间戳 |
| `445` | 组合保存 | `p_save` | `name`, `desc`, `items`（含必填 `ts_code`） | 同名自动覆盖（调仓） |
| `447` | 组合删除 | `p_delete` | `name` | 删除指定组合并返回 `status` |

---

## 4. Tushare Skills 与 MCP Server（`doc1_id=450 / 463`）

- **Tushare Skills**：
  - 大模型编程环境安装：`npx skills add https://github.com/waditu-tushare/skills --skill tushare`
  - OpenClaw 安装：`clawhub install tushare-data`（离线包：`https://tushare.pro/files/pro/tushare-data.zip`）
- **Tushare MCP Server**：
  - 支持 ClaudeCode / Cursor / Trae / Cline / CodeBuddy / Lingma / OpenClaw 系工具。
  - 配置远程 Streamable/HTTP MCP（注意勿将真实 Token 提交至版本库）：
    ```json
    {
      "mcpServers": {
        "tushareMcp": {
          "url": "https://api.tushare.pro/mcp/token=<TUSHARE_TOKEN>"
        }
      }
    }
    ```
- **选型建议**：日常临时查数用 **Skills**（零配置）；批量建模、自动化脚本与跨端复用优先用 **MCP** 或本地 Python SDK（[`TushareAdapter`](../../../data/adapters/tushare_adapter.py)）。
