# scripts/bootstrap — 黄金样本临时取数（QUANT-35）

**临时 Bootstrap 脚本，不是 S9 的正式 Adapter。** 没有 DQC、没有 quarantine、没有
cross_check。形状固定：**拉数据 → 对齐 `data/storage_schema.py` 的 pandera schema →
调 `write_parquet_atomic` 落盘**。`quality_flag` 恒为 `0`（CLEAN）。S9 的正式
Adapter 落地后这批脚本可整体替换，不是浪费。

放 `scripts/bootstrap/` 而不是 `data/adapters/`，就是为了让这条边界一眼可见。

## 用法

```bash
# 一次性拉黄金样本最小数据集（日历 + 5 只 ETF 日线）
bash scripts/bootstrap/run_golden_sample.sh

# 或分开跑
uv run python scripts/bootstrap/fetch_calendar.py --exchange SSE --start 2024-12-01 --end 2025-03-01
uv run python scripts/bootstrap/fetch_bars_daily.py --tickers 510300.SH,510500.SH,518880.SH,511010.SH,159915.SZ --start 2024-12-01 --end 2025-03-01

# 可选（黄金样本不需要）
uv run python scripts/bootstrap/fetch_fundamental.py --tickers … --start … --end …
uv run python scripts/bootstrap/fetch_corporate_actions.py --tickers … --start … --end …
uv run python scripts/bootstrap/fetch_industry.py --tickers …
```

`--start` / `--end` 接受 `YYYYMMDD` 或 `YYYY-MM-DD`。`--data-root` 可覆盖落盘根目录
（默认见下）。

`TUSHARE_TOKEN` 只从 `.env` 或 `config/secrets` 读，不打印、不写进任何提交的文件、
不出现在日志里。

## 落盘根目录（AC-A1）

**根目录是 `data_platform/`。** 依据：

| 证据 | 位置 |
| --- | --- |
| 分区路径模板的注释「relative to `data_platform/`」 | `data/storage_schema.py` `PARTITION_PATHS` 声明处 |
| `PARTITION_PATHS["bars_daily"] = "asset/bars_daily/{date}.parquet"` | 同上 |
| `cleanup_expired(root="data_platform")` / `check_capacity(data_root="data_platform")` | `data/storage_ops.py` |
| `DataAPI` 默认根 `data_platform/asset` | `data/data_api.py` `_DEFAULT_DATA_ROOT` |

所以一行落盘就是 `data_platform/asset/bars_daily/2025-01-15.parquet`。本单描述里的
`asset/…` 与 `data_platform/asset/…` 两种写法，以前者为**相对模板**、后者为**完整
路径**理解，二者不矛盾；以 `PARTITION_PATHS` + `storage_ops` 的 root 为准。

`write_parquet_atomic(df, path, table=…)` 本身不带 root，path 由调用方拼 ——
`_common.table_path()` 是唯一的拼接点。

## turnover 口径（AC-A2）

`turnover = amount / float_market_cap`，两者都换算成元再除。

- 取不到 `float_market_cap` 时标 **NaN**，不猜数。
- `SCHEMA["bars_daily"].columns["turnover"]` 是 `float64 / nullable=True`，
  **schema 允许 nullable**，因此不需要改 schema。实测约束输出：

```
dtype      = float64
nullable   = True
required   = True            # 列必须在，值可空
checks     = ['<Check non_negative: greater_than_or_equal_to(0)>']
```

单位换算（对齐 `tests/unit/test_storage_schema.py` 的量级：茅台
`total_market_cap=2.0e12` 元、`total_shares=1.256e9` 股）：

| 字段 | tushare 原始单位 | 落盘单位 | 换算 |
| --- | --- | --- | --- |
| `vol` → `volume` | 手 | 股 | ×100 |
| `amount` | 千元 | 元 | ×1000 |
| `daily_basic.circ_mv` | 万元 | 元 | ×1e4 |
| `daily_basic.total_share` | 万股 | 股 | ×1e4 |

ETF 的 `float_market_cap` 取不到（`daily_basic` 只覆盖股票），所以黄金样本 5 只 ETF
的 `turnover` 全为 NaN —— 这是数据源覆盖范围，不是数据缺陷。

## ticker 格式（AC-A3）

**两边是同一个字符串，`ticker_convert` 是恒等映射。** 实测对照表：

```
 tushare ts_code |  repo ticker | equal | validate_equity_ticker
------------------------------------------------------------------------
       000001.SZ |    000001.SZ |  True | True
       000002.SZ |    000002.SZ |  True | True
       000004.SZ |    000004.SZ |  True | True
       600519.SH |    600519.SH |  True | True
       510300.SH |    510300.SH |  True | True
       159915.SZ |    159915.SZ |  True | True
```

两边都是 `{6 位数字}.{SH|SZ|BJ}`（tushare `ts_code` ↔ `core.asset_rules` 的
`validate_equity_ticker`）。没有转换表可写。函数保留在
`_common.ticker_convert`，格式将来分叉时只有一个改动点；测试覆盖见
`tests/unit/test_bootstrap_common.py::TestTickerConvert`。

## 涨跌停标记（AC-A4）——实测，不是推断

真的调了，结论是**这个 token 连接口都够不着**，不是"拉到了 0 行"：

| 调用 | 实测结果 |
| --- | --- |
| `pro.limit_list(...)` | `请指定正确的接口名` —— **不是真实接口名** |
| `pro.limit_list_d(...)` | `您没有接口(limit_list_d)访问权限` |
| `pro.stk_limit(...)` | `您没有接口(stk_limit)访问权限` |

实际行数：**0**（接口不可达）。因此 `is_limit_up` / `is_limit_down` 全填 `False`。

> **这是数据源限制，不是市场事实。** ETF 当日可能真实涨停/跌停，本列的 `False` 只
> 表示"没拿到数据"，不能拿来做涨跌停相关的推理。原始探测日志：
> `task/probe_tushare_quant35*.log`。

## 日历源（AC-A5）

**选定：tushare `trade_cal`。** 只留这一条取数路径，不接 akshare。

理由：本单的三源分工表把 tushare 作为唯一主源，日历跟行情同一个 token、同一套
重试与配额语义；akshare 无 token 但接口形态与 tushare 不同，接进来就要养第二条
初始化/异常/落盘路径，对"临时 bootstrap"不划算。文件落在
`data_platform/asset/calendar/{exchange}.parquet`（exchange 取小写存储名
`sse` / `szse`，见 `core.asset_rules.EXCHANGE_MAP`）。

注意：本 token 上 `trade_cal` 的配额是 **1 次/小时**，跑批前留意。

## 已知的 tushare 接口限制（实测）

探测日志在 `task/probe_tushare_quant35*.log`。这张表是拉数前必须知道的事：

| 接口 | 状态 | 影响 |
| --- | --- | --- |
| `daily` | 可用，但**只覆盖股票** | ETF/LOF 的 `ts_code` 返回 0 行 |
| `fund_daily` | **无访问权限** | ETF 日线**拿不到** |
| `fund_adj` | 无访问权限 | ETF 复权因子拿不到 |
| `adj_factor` | 可用，仅股票；配额 1 次/小时 | ETF 复权因子拿不到 |
| `daily_basic` | 可用，仅股票；配额 1 次/小时 | ETF 的 `float_market_cap` 拿不到 |
| `suspend_d` | 无访问权限 | `suspend` 全填 `False` |
| `limit_list_d` / `stk_limit` | 无访问权限 | 涨跌停标记全填 `False` |
| `trade_cal` | 可用；配额 1 次/小时 | 日历可用 |

**这意味着黄金样本 5 只 ETF 的日线目前拉不下来。** `fund_daily` 是 ETF 行情的正
确接口（`daily` 是个股接口），而当前 token 的积分档位没有开它。脚本遇到这种情况
**直接报错退出**，不写空文件、不写假行：

```
RuntimeError: 510300.SH: no daily bars from tushare. `daily()` is stock-only
(returns 0 rows for an ETF/LOF code) and `fund_daily()` is the ETF interface —
if the log above says 没有接口(fund_daily)访问权限, the token's points tier
does not include fund_daily and the golden-sample pull cannot run on it.
```

同理 `adjfactor` 是 schema 里非空且 `> 0` 的列，取不到就报错，**不会**拿 `1.0` 冒充
复权因子（`ingestion.md` 记着停牌复权那个 244038313 的坑）。

## 脚本清单

| 文件 | 产出 | 黄金样本是否需要 |
| --- | --- | --- |
| `_common.py` | token 初始化 / schema 对齐 / 写盘 | — |
| `fetch_calendar.py` | `asset/calendar/{exchange}.parquet` | 需要 |
| `fetch_bars_daily.py` | `asset/bars_daily/{date}.parquet` | 需要 |
| `fetch_fundamental.py` | `asset/fundamental/{date}.parquet` | 可选 |
| `fetch_corporate_actions.py` | `asset/reference/corporate_actions.parquet` | 可选 |
| `fetch_industry.py` | `asset/reference/industry.parquet` | 可选 |
| `run_golden_sample.sh` | 日历 + 5 只 ETF 日线一键拉取 | 需要 |

## 边界

- 不做 S9 正式 Adapter ABC，不写 DQC，不实现 quarantine / cross_check
- 不动 `data/adapters/`，不改 `storage_schema.py` / `storage_ops.py` 的既有签名
- 不接 akshare、不接 AmazingData（本单只接 tushare）
- `quality_flag` 恒为 `0`，不做任何质量判定
- 不碰 live-trading 配置与生产凭据

## 测试

```bash
uv run pytest tests/unit/test_bootstrap_common.py -q   # 离线，15 条
uv run ruff check scripts/bootstrap/
```

`_common.py` 的 `ticker_convert` / `align_schema` / 落盘路径有单元测试；拉数本身要
网络，不在单测里跑。
