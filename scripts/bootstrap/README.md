# scripts/bootstrap — 黄金样本临时取数（QUANT-35）

**临时 Bootstrap 脚本，不是 S9 的正式 Adapter。** 没有 DQC、没有 quarantine、没有
cross_check。形状固定：**拉数据 → 对齐 `data/storage_schema.py` 的 pandera schema →
调 `write_parquet_atomic` 落盘**。`quality_flag` 恒为 `0`（CLEAN）。S9 的正式
Adapter 落地后这批脚本可整体替换，不是浪费。

放 `scripts/bootstrap/` 而不是 `data/adapters/`，就是为了让这条边界一眼可见。

## 数据源（2026-10-28 人工授权）

**黄金样本走 AmazingData。** 单据原文写的是「本单只接 tushare」，但这个 tushare token
的积分档位没开 `fund_daily`，ETF 日线一行都拉不到（见下表实测）。2026-10-28 人工
明确授权改用 AmazingData，本文件按这个决定写。

两条取数路径都在，`run_golden_sample.sh` 默认走 AmazingData，`SOURCE=tushare` 切回去：

| | AmazingData（默认） | tushare（`SOURCE=tushare`） |
| --- | --- | --- |
| ETF 日线 | `MarketData.query_kline` ✅ 实测 290 行 | `fund_daily` ❌ 无权限 |
| 复权因子 | `InfoData.get_fund_nav` 的 `NAV_ADJ_FACTOR` ✅ | `fund_adj` ❌ 无权限 |
| 流通份额 | `InfoData.get_fund_share` 的 `FLOAT_SHARE` ✅ | `daily_basic` 只覆盖股票 ❌ |
| 交易日历 | `BaseData.get_calendar` ✅（全市场统一） | `trade_cal` ✅，配额 1 次/小时 |
| 运行位置 | 必须在容器内（SDK 只装在镜像里） | 宿主机 `uv run python` |

两段式是因为两边的依赖凑不到一起：**SDK 只在 `docker-amazingdata` 镜像里，pandera 只在
宿主机的 venv 里**（镜像 `requirements.txt` 只有 pandas/pyarrow/numba/scipy/tables）。
所以：

```
1. amazing_pull.py        （容器）→ data_platform/raw/amazingdata/*.parquet   原样，源列名+源单位
2. fetch_*.py --from-amazing（宿主）→ data_platform/asset/…                  对齐 schema 后原子写
```

镜像 `docker-amazingdata` 是预构建的，**没有也不需要 rebuild**；`~/AmazingData/docker/
docker-compose.yml` 没动。脚本在 `finally` 里调 `ad.logout()` 释放连接槽位。凭据由
容器的 `env_file` 提供，脚本既不打印也不落盘。

## 用法

```bash
# 一次性拉黄金样本最小数据集（日历 + 5 只 ETF 日线），默认 AmazingData
bash scripts/bootstrap/run_golden_sample.sh

# 切回 tushare 路径（需要积分档位含 fund_daily 的 token）
SOURCE=tushare bash scripts/bootstrap/run_golden_sample.sh

# 两段分开跑（AmazingData 路径）
docker compose run --rm --no-deps amazingdata \
  python3 /opt/project/scripts/bootstrap/amazing_pull.py \
  --tickers 510300.SH,510500.SH,518880.SH,511010.SH,159915.SZ \
  --start 2024-12-01 --end 2025-03-01 \
  --staging /opt/project/data_platform/raw/amazingdata
uv run python scripts/bootstrap/fetch_calendar.py --exchange SSE --from-amazing data_platform/raw/amazingdata
uv run python scripts/bootstrap/fetch_bars_daily.py --tickers 510300.SH,… --from-amazing data_platform/raw/amazingdata

# 可选（黄金样本不需要，仍走 tushare）
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

实测落地（2026-10-28，`bash scripts/bootstrap/run_golden_sample.sh` 退出码 0）：

```
$ find data_platform -name '*.parquet' | sort | head
data_platform/asset/bars_daily/2024-12-02.parquet
data_platform/asset/bars_daily/2024-12-03.parquet
data_platform/asset/bars_daily/2024-12-04.parquet
data_platform/asset/bars_daily/2024-12-05.parquet
data_platform/asset/bars_daily/2024-12-06.parquet
data_platform/asset/bars_daily/2024-12-09.parquet
data_platform/asset/bars_daily/2024-12-10.parquet
data_platform/asset/bars_daily/2024-12-11.parquet
data_platform/asset/bars_daily/2024-12-12.parquet
data_platform/asset/bars_daily/2024-12-13.parquet

$ find data_platform -name '*.parquet' | wc -l
63      # 58 个 bars_daily + 1 个 calendar + 4 个 raw staging

$ find data_platform/raw -name '*.parquet' | sort
data_platform/raw/amazingdata/calendar.parquet
data_platform/raw/amazingdata/fund_nav.parquet
data_platform/raw/amazingdata/fund_share.parquet
data_platform/raw/amazingdata/kline.parquet
```

行数：`bars_daily` **290 行**（5 只 × 58 个交易日，每日一文件 5 行），`calendar`
**58 行**，合计 348。`SCHEMA["bars_daily"]` / `SCHEMA["calendar"]` 全量 validate 通过。

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

| 字段 | tushare 原始单位 | AmazingData 原始单位 | 落盘单位 | tushare 换算 | AmazingData 换算 |
| --- | --- | --- | --- | --- | --- |
| `vol`/`volume` → `volume` | 手 | **股** | 股 | ×100 | **不换算** |
| `amount` | 千元 | **元** | 元 | ×1000 | **不换算** |
| `daily_basic.circ_mv` / `FLOAT_SHARE` | 万元 / 万份 | 万份 | 元 / 股 | ×1e4 | ×1e4 |
| `daily_basic.total_share` | 万股 | — | 股 | ×1e4 | — |

AmazingData 的单位是实测的，不是查表来的：`510300.SH` 2024-12-02 的
`volume=981634781`、`close=4.031`、`amount=3.948067e9`，
`981634781 × 4.031 ≈ 3.957e9 ≈ amount` —— 两边都是「股 × 元 = 元」，所以既不 ×100
也不 ×1000。照搬 tushare 的换算会把 volume 放大 100 倍、amount 放大 1000 倍。

**ETF 的 `float_market_cap` 在 AmazingData 路径下取得到**：`InfoData.get_fund_share`
的 `FLOAT_SHARE`（万份）× 1e4 × 当日 `close`。所以黄金样本 5 只 ETF 的 `turnover` 是
实算值，不是 NaN。实测分布（290 行）：

```
min     0.008164
25%     0.018327
50%     0.034355
75%     0.062990
max     0.924333
```

tushare 路径下 `daily_basic` 只覆盖股票，ETF 的 `turnover` 仍是 NaN —— 保留这个回退
语义：**取不到市值就写 NaN，不猜数、不写 0**。

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

换到 AmazingData 也一样：`query_kline` 的日线返回里没有涨跌停价（`high_limited` /
`low_limited` 只在 L2 快照里），`InfoData.get_history_stock_status` 对这 5 只 ETF 返回
空表。**两条路径都拿不到，所以都写 `False`。**

> **这是数据源限制，不是市场事实。** ETF 当日可能真实涨停/跌停，本列的 `False` 只
> 表示"没拿到数据"，不能拿来做涨跌停相关的推理。原始探测日志：
> `task/probe_tushare_quant35*.log`、`task/probe_amazing_quant35.log`。

`suspend` 同理写 `False`。有 bar 的行就是有成交的行，停牌日根本不会产生
`(ticker, trade_date)` 行，所以这个列在当前写入方式下恒为 `False`。

## 复权因子口径

`adjfactor` 在 schema 里非空且 `> 0`。两条路径的取法不同，但都**不拿 `1.0` 冒充**：

| 路径 | 来源 | 说明 |
| --- | --- | --- |
| AmazingData | `InfoData.get_fund_nav` 的 `NAV_ADJ_FACTOR` | 实测值，原样写入，不重新归一 |
| tushare | `adj_factor` / `fund_adj` | 取不到就报错退出 |

为什么原样写入不用改尺度：`data/data_api.py::_apply_adjustment` 的分母是
`arg_max(adjfactor)` / `arg_min(adjfactor)`，即

```
forward  qfq = raw × adj / adj_latest
backward hfq = raw × adj / adj_first
```

**两种复权都把 `adjfactor` 的常数尺度约掉了**，所以 `NAV_ADJ_FACTOR` 是 0.447898
还是 1.0 不影响任何一条复权价。相对价格与收益率因此是对的。

实测 `NAV_ADJ_FACTOR`（58 个交易日，5 只 ETF）：

| ticker | 取值个数 | min | max | 含义 |
| --- | --- | --- | --- | --- |
| `510300.SH` | 1 | 0.447898 | 0.447898 | 窗口内无分配 |
| `510500.SH` | **2** | 0.326226 | 0.331605 | **窗口内有一次分配，因子跳变** |
| `518880.SH` | 1 | 0.377614 | 0.377614 | 窗口内无分配 |
| `511010.SH` | 1 | 0.010121 | 0.010121 | 窗口内无分配 |
| `159915.SZ` | 1 | 1.145588 | 1.145588 | 窗口内无分配 |

`510500.SH` 的两段取值是**测出来的分配事件**，不是噪声：同一窗口内
`ACCUM_UNIT_DIST` 从 0.087 涨到 0.178，因子比值
`0.331605 / 0.326226 = 1.01649`，和 `1 + 0.091 / NAV_ex` 对得上。这正是
`adjfactor` 存在的意义 —— 少了它，这只 ETF 的收益率会在除息日多出一段假涨。

补充实测：`get_dividend` / `get_right_issue` 对这 5 只 ETF 在 1990–2025 全区间返回
**0 行**（ETF 分配不进这两张表），所以 `NAV_ADJ_FACTOR` 是这个 vendor 唯一露出来的
复权来源。

## 日历源（AC-A5）

**黄金样本选定：AmazingData `BaseData.get_calendar()`。** 一个代码路径，
不接 akshare。

理由：日历跟行情同一个登录、同一套连接语义，而且这条路径**没有配额限制**；tushare
`trade_cal` 在本 token 上是 **1 次/小时**，和 ETF 日线拉不到是两件独立的坑。
`tushare trade_cal` 保留为 `SOURCE=tushare` 路径的日历源，也是唯一那条。

`BaseData.get_calendar()` 返回的是**全市场统一**交易日历（实测 8750 天，
自 1990-12-19 起），不区分交易所 —— A 股六个交易所的开休市本来就一致。
`--exchange` 只决定输出文件名（小写存储名 `sse` / `szse`，见
`core.asset_rules.EXCHANGE_MAP`），落盘 `data_platform/asset/calendar/{exchange}.parquet`。

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

**这意味着黄金样本 5 只 ETF 的日线用 tushare 拉不下来** —— `fund_daily` 是 ETF 行情
的正确接口（`daily` 是个股接口），而当前 token 的积分档位没有开它。这正是改走
AmazingData 的原因。tushare 路径遇到这种情况**直接报错退出**，不写空文件、不写假行：

```
RuntimeError: 510300.SH: no daily bars from tushare. `daily()` is stock-only
(returns 0 rows for an ETF/LOF code) and `fund_daily()` is the ETF interface —
if the log above says 没有接口(fund_daily)访问权限, the token's points tier
does not include fund_daily and the golden-sample pull cannot run on it.
```

同理 `adjfactor` 是 schema 里非空且 `> 0` 的列，取不到就报错，**不会**拿 `1.0` 冒充
复权因子（`ingestion.md` 记着停牌复权那个 244038313 的坑）。

## 脚本清单

| 文件 | 产出 | 跑在哪 | 黄金样本是否需要 |
| --- | --- | --- | --- |
| `_common.py` | source→schema 映射 / schema 对齐 / 写盘 | 宿主 | — |
| `amazing_pull.py` | `raw/amazingdata/{calendar,kline,fund_nav,fund_share}.parquet` | **容器** | 需要 |
| `fetch_calendar.py` | `asset/calendar/{exchange}.parquet` | 宿主 | 需要 |
| `fetch_bars_daily.py` | `asset/bars_daily/{date}.parquet` | 宿主 | 需要 |
| `fetch_fundamental.py` | `asset/fundamental/{date}.parquet` | 宿主 | 可选 |
| `fetch_corporate_actions.py` | `asset/reference/corporate_actions.parquet` | 宿主 | 可选 |
| `fetch_industry.py` | `asset/reference/industry.parquet` | 宿主 | 可选 |
| `run_golden_sample.sh` | 日历 + 5 只 ETF 日线一键拉取 | 两段都跑 | 需要 |

`fetch_calendar.py` / `fetch_bars_daily.py` 都带 `--from-amazing <STAGING_DIR>`：读
`amazing_pull.py` 的原样落盘，走 `_common.frame_from_amazing_*` 映射后写 schema 文件。
不带这个参数就是原来的 tushare 路径，`--start` / `--end` 必填。

## 边界

- 不做 S9 正式 Adapter ABC，不写 DQC，不实现 quarantine / cross_check
- 不动 `data/adapters/`，不改 `storage_schema.py` / `storage_ops.py` 的既有签名
- 不接 akshare；AmazingData 于 2026-10-28 经人工授权接入（原单据写「只接 tushare」）
- 不 rebuild `docker-amazingdata` 镜像，不改 `~/AmazingData/docker/docker-compose.yml`
- `quality_flag` 恒为 `0`，不做任何质量判定
- 不碰 live-trading 配置与生产凭据；凭据只经容器 `env_file` 传入，不打印、不落盘

## 测试

```bash
uv run pytest tests/unit/test_bootstrap_common.py -q    # 离线，15 条
uv run pytest tests/unit/test_bootstrap_amazing.py -q   # 离线，13 条
uv run ruff check scripts/bootstrap/
```

`_common.py` 的 `ticker_convert` / `align_schema` / 落盘路径 / AmazingData 帧映射有
单元测试；拉数本身要网络与容器登录，不在单测里跑。
