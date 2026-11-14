# 01. 鉴权、通用协议、权限与坑点

[← 返回目录](README.md)

---

## 1. 鉴权与环境配置

1. 注册并登录 [Tushare Pro](https://tushare.pro)，在个人中心复制 Token。
2. **本量化平台配置**：写入 `config/.env`（已被 `.gitignore` 忽略），不要将真实 Token 写入代码或文档：
   ```dotenv
   TUSHARE_TOKEN=<your-tushare-token>
   ```
3. [`TushareAdapter`](../../../data/adapters/tushare_adapter.py) 通过环境变量 `TUSHARE_TOKEN` 初始化 `ts.pro_api(self._token)`；如独立脚本使用，优先通过 [`core.config.load_secrets()`](../../../core/config.py) 读取。

---

## 2. 接入方式

### Python SDK（推荐，版本 `>= 1.2.10`）

```python
import tushare as ts

pro = ts.pro_api("<TUSHARE_TOKEN>")  # 或提前执行 ts.set_token(...)
df = pro.trade_cal(exchange="", start_date="20250101", end_date="20250131", is_open="0")
# 等价通用写法：
df = pro.query("trade_cal", exchange="", start_date="20250101", end_date="20250131", is_open="0")
```

> [!NOTE]
> `ts.pro_bar(...)` 是 SDK 层封装的复合接口（支持动态复权、均线 `ma`、`factors`），**无法通过 HTTP 接口直接调用**。

### HTTP POST RESTful API（语言无关）

- **Endpoint**：`POST http://api.tushare.pro`
- **请求体（JSON）**：

| 字段 | 类型 | 说明 |
|---|---|---|
| `api_name` | str | 接口名，如 `stock_basic`、`daily` |
| `token` | str | 用户 Token |
| `params` | object | 接口入参键值对 |
| `fields` | str | 逗号分隔返回字段；空字符串返回默认展示字段 |

- **响应体（JSON）**：`{"code": 0, "msg": null, "data": {"fields": [...], "items": [[...]]}}`。`code != 0` 表示失败（如 `2002` 表示权限不足）。

---

## 3. 双轨权限与频次体系

Tushare Pro 采用**「积分阶梯制 + 独立权限制」**双轨机制，现阶段**调取数据不消耗积分**，积分仅作门槛与频次流控（注：恶意注册或到期活动积分会被清理）。

### 3.1 积分与频次对照表（`doc1_id=290`）

| 积分数 | 每分钟频次 | 每天总量上限 | 访问范围（不含独立权限接口） | 参考年费（元/年） |
|---|---|---|---|---|
| 120 | 50 次/分 | 8,000 次 | 仅未复权日线（`daily` 等试用） | 0 |
| 2,000 以上 | 200 次/分 | 100,000 次/API | 基础行权、财务、日历、基金/期货基础接口 | 200 |
| 5,000 以上 | 500 次/分 | 常规数据无上限 | 绝大部分日频行情与特征数据 | 500 |
| 10,000 以上 | 500 次/分（特色 300 次/分） | 常规无上限 | 含高阶特色数据（盈利预测、筹码、游资等） | 1,000 |
| 15,000 以上 | 500 次/分 | 特色数据无总量限制 | 特色数据专属全量权限 | 1,500 |

### 3.2 独立权限接口（与积分无关，需单独开通）

| 权限模块 | 代表接口 | 历史起始 | 个人价格 | 频次 / 单次上限 |
|---|---|---|---|---|
| A 股历史分钟 | `stk_mins` | 2009 | 2,000/年 | 500 次/分，8,000 行/次（30 万次/24h） |
| A 股实时分钟 | `rt_min`, `rt_min_daily` | 实时 | 1,000/月 | 500 次/分，单次最多 300 股/1,000 行 |
| 实时日线（股/指/申万/ETF） | `rt_k`, `rt_idx_k`, `rt_sw_k`, `rt_etf_k` | 9:30 起 | 各 200/月 | 50 次/分，单次全市场 |
| ETF 实时参考 IOPV | `rt_etf_sz_iopv` | 盘中 | 300/月 | 50 次/分，单次全市场 |
| 指数 / 申万 / ETF / 期权历史分钟 | `idx_mins`, `sw_mins`, `etf_mins`, `opt_mins` | 2009–2015 | 各 2,000/年 | 500 次/分，5,000–8,000 行/次 |
| 期货历史 / 实时分钟 | `ft_mins` / `rt_fut_min` | 2010 / 实时 | 2,000/年 · 1,000/月 | 500 次/分（实时支持 WebSocket） |
| 盘前股本 / 集合竞价 | `stk_premarket` / `stk_auction*`, `etf_auction` | 近 2 年 / 2010 | 各 500/年 | 500 次/分，8,000–10,000 行/次 |
| 港股 / 美股日线与财报 | `hk_daily*`, `hk_*` / `us_daily*`, `us_*` | 2000 / 全历史 | 1,000–2,000/年（财报 500/年） | 500 次/分 |
| 可转债转股价变动 | `cb_price_chg` | 全历史 | 500/年 | 500 次/分，2,000 行/次 |
| 资讯/公告/互动/研报/政策/央行 | `news`, `anns_d`, `irm_qa_*`, `research_report`, `npr`, `monetary_policy` | 2–25 年 | 200–1,000/年 | 200–500 次/分 |
| 量化因子库（272 因子） | `factor_list`, `factor_value` | 2010 | 2,000/年（个人） | 5,000+ 积分可试用 `factor_value` |

---

## 4. 数据口径与平台踩坑清单（`doc1_id=122, 234`）

1. **代码（`ts_code`）与日期格式**：
   - 股票/基金/债券后缀：`.SH`（上交所）、`.SZ`（深交所）、`.BJ`（北交所）、`.HK`（港股）；美股多无统一 `.US` 后缀，期货见 `.SHF` / `.DCE` / `.ZCE` / `.CFX` / `.INE` / `.GFE`。
   - 日频参数统一 `YYYYMMDD`（如 `20250401`）；分钟与资讯带时分秒参数统一 `YYYY-MM-DD HH:MM:SS`（如 `2025-04-01 09:30:00`）。
   - **分钟线左闭右开坑**：若 `end_date` 未带时分秒（只传日期），**返回结果不含 `end_date` 当日**（严格 `< end_date`）。
2. **动态前复权基准差异（极重要）**：
   - `ts.pro_bar(..., adj='qfq')` 以传入的 **`end_date` 当日复权因子为基准往前复权**，而非以“最新交易日”为基准，且采用**分红再投**算法；因此随 `end_date` 改变同一天历史价格也会变化，并与行情软件展示口径不同。
   - **本平台规定**：不直接存储 `pro_bar` 前复权价格，而是拉取未复权 `daily` + `adj_factor`，统一在标准化层与查询层计算固定锚点后复权。
3. **单位陷阱**：A 股 `daily.vol` 为**手**（100 股）、`daily.amount` 为**千元**，接入本平台时已由 [`tushare_normalizer`](../../../data/normalizers/tushare_normalizer.py) 分别 `×100`（股）和 `×1000`（元）；但**指数/部分板块行情**的 `vol` / `amount` 单位随交易所略有不同，使用前须核对具体接口字段说明。
4. **财务数据修正与去重**：同一报告期若出现多条记录，通常为公司会计重述/更正，需在 `fields` 中显式指定 `update_flag`（`1` 为修正后最新数据，`0` 为初始披露）进行去重。
5. **限流与代理超时**：遇到限流或 `timeout` 时，检查是否开启全局 VPN/代理，并在批处理循环中配合指数退避与断点水印（`watermark`）重试。
