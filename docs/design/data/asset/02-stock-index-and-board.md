# 02. 股票指数、行业与概念板块（4 张）

> 所属目录：[docs/design/data/asset/](README.md) · Schema 源码：[data/storage_schema.py](../../../../data/storage_schema.py)
>
> 说明：明确归属股票域，全部使用 `cn_stock_*` 前缀；支持交易所/中证/国证/申万/中信/同花顺/东财/通达信等板块代码（`validate_index_ticker`）。

---

## 1. `cn_stock_index_basic`（指数 / 行业 / 概念板块档案）

- **路径**：`asset/cn_stock_index_basic/index_basic.parquet`（全量单文件）
- **family**：`stock`
- **主键**：`[ticker]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush` → `amazingdata`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_index_ticker`（后缀 `SH`/`SZ`/`BJ`/`CSI`/`CNI`/`SI`/`CI`/`TI`/`DC`/`TS`） |
| `name` | `str` | N | 指数或行业/概念板块名称 |
| `category` | `str` | N | 分类（宽基/规模/申万行业/中信行业/同花顺概念等） |
| `publisher` | `str` | Y | 发布机构 |
| `base_date` | `pa.Date` | Y | 基期日期 |
| `base_point` | `float` | Y | `> 0`（基点） |
| `list_date` | `pa.Date` | Y | 发布/上市日期 |

---

## 2. `cn_stock_index_bars_daily`（指数与板块日线行情）

- **路径**：`asset/cn_stock_index_bars_daily/{month}.parquet`（按自然月）
- **family**：`stock`
- **主键**：`[ticker, trade_date]`
- **数据源优先级**：`straightflush` → `qdc` → `amazingdata`
- **多源交叉校验**：**已配置并实测验证**（`2026-11-13` 截面 `matched=554`；`straightflush` vs `qdc` 为 `WARNING 0.36%`，`straightflush` vs `amazingdata` 为 `ERROR 2.89%`）
- **交叉比对数值列**：`open`, `high`, `low`, `close`, `volume`, `amount`, `pe`, `pb`, `turnover`, `total_market_cap`, `float_market_cap`
- **细节与已知问题**：
  - **中证 `.CSI` 后缀统一**：同花顺对上交所同步发布的 `000xxx.CSI` 指数（如 `000510.CSI`、`000860.CSI`）在 [`straightflush_normalizer`](../../../../data/normalizers/straightflush_normalizer.py#L128-L137) 中在不与既有 `.SH` 碰撞的前提下统一映射为 `.SH`，与 QDC/AmazingData 对齐（`932000.CSI` 等纯中证指数保持 `.CSI`）；同花顺另含约 850 只 `.TI` 同花顺概念/行业独有指数。
  - **AmazingData 上证手/股差异**：AmazingData `EXTRA_INDEX_A` 对 `.SH` 指数 `volume` 按手回报（已在 [`amazingdata_normalizer`](../../../../data/normalizers/amazingdata_normalizer.py#L92-L96) 对 `.SH` 自动 `* 100` 转股，`.SZ`/`.BJ` 保持股）；整数型 `turnover`/`amount` 统一转 `float64`。
  - **QDC 已知 `WARNING`（保留告警）**：QDC `sse_native_recovered` 仅对 `000012.SH`（国债指数）、`000022.SH`（企债指数）的 `volume` 存在整 100 倍偏小（两只 `close`/`amount` 与其余债券指数如 `000061.SH` 均一致），保留作为上游单位异常提示。
  - **AmazingData 已知 `ERROR`（验证优先级）**：约 `2.9%`（16 只，含 `399001.SZ`/`399002.SZ`/`399006.SZ` 及部分 `.SH` 债券/全收益指数）在 AmazingData 出现 `volume`/`amount`/`close` 偏差（而同花顺与 QDC 彼此一致），从实证确认 `straightflush → qdc → amazingdata` 的优先级配置合理。

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_index_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `open` | `float` | N | `> 0` |
| `high` | `float` | N | `> 0` |
| `low` | `float` | N | `> 0` |
| `close` | `float` | N | `> 0` |
| `volume` | `int` | N | `>= 0` |
| `amount` | `float` | N | `>= 0` |
| `pe` | `float` | Y | 指数市盈率 |
| `pb` | `float` | Y | 指数市净率 |
| `turnover` | `float` | Y | `>= 0`（换手率） |
| `total_market_cap` | `float` | Y | `>= 0`（总市值） |
| `float_market_cap` | `float` | Y | `>= 0`（流通市值） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 3. `cn_stock_industry`（股票行业分类映射）

- **路径**：`asset/cn_stock_industry/industry.parquet`（全量单文件，独立目录不再共用 `cn_reference/`）
- **family**：`stock`（旧别名：`industry`）
- **主键**：`[ticker, classification]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_equity_ticker` |
| `industry_l1` | `str` | N | 一级行业名称/代码 |
| `industry_l2` | `str` | N | 二级行业名称/代码 |
| `industry_l3` | `str` | Y | 三级行业名称/代码 |
| `classification` | `str` | N | `isin(["citics", "sw", "ths", "tdx"])` |
| `effective_date` | `pa.Date` | N | 行业归属生效日 |

---

## 4. `cn_stock_index_weights`（指数 / 板块成分与权重）

- **路径**：`asset/cn_stock_index_weights/{year}.parquet`（按年）
- **family**：`stock`
- **主键**：`[index_code, ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`weight`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `index_code` | `str` | N | `validate_index_ticker` |
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 权重截面日期 |
| `weight` | `float` | Y | `>= 0`（成分权重，百分比或小数按源约定统一） |
| `in_date` | `pa.Date` | Y | 纳入日期 |
| `out_date` | `pa.Date` | Y | 剔除日期 |
