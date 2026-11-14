# 02. 基础数据（`BaseData`）与全量枚举

`base = ad.BaseData()` 提供交易日历、代码表、证券静态信息、复权因子及 ETF 申赎清单（PCF）接口。

---

## 1. `BaseData` 方法汇总

| 方法 | 核心参数 | 返回类型 | 说明 |
|---|---|---|---|
| `get_calendar(data_type='str', market='SH')` | `data_type`: `'str'` \| `'datetime'`；`market`: 见枚举 | `list` | 获取指定交易所全量交易日历（官网输出表误标 `List[int]`，实际默认 `'str'`） |
| `get_code_list(security_type='EXTRA_STOCK_A')` | `security_type`: `str` | `list[str]` | 每日最新代码表（早 9 点前更新；无法查历史退市/到期标的） |
| `get_hist_code_list(security_type, start_date, end_date, local_path)` | 默认 `'EXTRA_STOCK_A_SH_SZ'`；`start_date`/`end_date`: `int` (`YYYYMMDD`)；`local_path`: `str` | `list[str]` | 区间内历史代码表（支持沪深北及期货；注意官方 ETF 示例漏传起止日期为笔误） |
| `get_code_info(security_type='EXTRA_STOCK_A')` | `security_type`: `str` | `DataFrame` | 每日最新证券信息（早 9 点前更新），index 为代码，列：`symbol`, `pre_close`, `high_limited`, `low_limited`, `price_tick` |
| `get_future_code_list(security_type='ZJ_FUTURE')` | `security_type`: 期货枚举 | `list[str]` | 每日最新期货合约代码表（官网默认 `'ZJ_FUTURE'`，全市场五所请传 `'EXTRA_FUTURE'`） |
| `get_option_code_list(security_type='EXTRA_ETF_OP')` | `security_type`: `'EXTRA_ETF_OP'` 等 | `list[str]` | 每日最新 ETF 期权合约代码表 |
| `get_backward_factor(code_list, local_path, is_local=True)` | `code_list`: `list[str]`（股票、ETF）；`local_path`: `str` | `DataFrame` | 累计后复权因子矩阵（`index=交易日期`, `columns=股票代码`） |
| `get_adj_factor(code_list, local_path, is_local=True)` | `code_list`: `list[str]`（股票、ETF）；部分版本含可选起止日期 | `DataFrame` | 单次复权因子矩阵（`index=交易日期`, `columns=股票代码`） |
| `get_etf_pcf(code_list)` | `code_list`: `list[str]`（沪深 ETF） | `tuple[DataFrame, dict]` | ETF 每日最新申赎参数与成分股表 `(etf_pcf_info, etf_pcf_constituent)`，字段详见 [05 篇](05-etf-index-kzz-and-options.md) |

### 前 / 后复权换算公式

SDK 提供的 `get_backward_factor` 为累计后复权因子矩阵，换算关系如下：

- **后复权价格** $= \text{原始价格} \times \text{backward\_factor}$
- **前复权因子** $= \text{backward\_factor} / \text{backward\_factor}_{\text{latest}}$
- **前复权价格** $= \text{原始价格} \times (\text{backward\_factor} / \text{backward\_factor}_{\text{latest}})$

---

## 2. `market` 市场枚举

| 枚举值 | 说明 |
|---|---|
| `SH` | 上海证券交易所（默认） |
| `SZ` | 深圳证券交易所 |
| `BJ` | 北京证券交易所 |

---

## 3. `security_type` 代码类型枚举

### 3.1 沪深北现货、指数、ETF、可转债与港股通

| 枚举值 | 说明 | 备注 |
|---|---|---|
| `EXTRA_STOCK_A` | 沪深北全部 A 股 | `get_code_list` 默认值 |
| `EXTRA_STOCK_A_SH_SZ` | 沪深 A 股（含历史退市） | `get_hist_code_list` 默认值 |
| `EXTRA_STOCK_A_MAIN` / `_GEM` / `_STAR` | A 股主板 / 创业板 / 科创板 | 子板块过滤 |
| `SH_A` / `SZ_A` / `BJ_A` | 上交所 / 深交所 / 北交所 A 股 | 分市场股票 |
| `EXTRA_INDEX_A` / `EXTRA_IDNEX_A` | 沪深北指数列表 | 官方文档存在 `INDEX` / `IDNEX` 拼写差异，调用前建议容器内实测 |
| `EXTRA_IDNEX_A_SH_SZ` | 沪深指数列表 | 同上 |
| `SH_INDEX` / `SZ_INDEX` / `BJ_INDEX` | 上交所 / 深交所 / 北交所指数 | 分市场指数 |
| `EXTRA_ETF` | 沪深 ETF 列表 | `SH_ETF`（上交所）/ `SZ_ETF`（深交所） |
| `EXTRA_KZZ` | 沪深可转债列表 | 配合 [05 篇](05-etf-index-kzz-and-options.md) 可转债 11 项接口使用 |
| `EXTRA_GLRA` | 沪深债券逆回购列表 | 用于 `onSnapshotglra` 订阅 |
| `EXTRA_HKT` | 沪深港股通标的列表 | 用于 `onSnapshothkt` 订阅与历史快照查询 |

### 3.2 期货交易所

| 枚举值 | 交易所 |
|---|---|
| `EXTRA_FUTURE` | 全部期货（中金所 / 上期所 / 大商所 / 郑商所 / 上期能源） |
| `ZJ_FUTURE` | 中国金融期货交易所（CFFEX，`get_future_code_list` 默认值） |
| `SQ_FUTURE` | 上海期货交易所（SHFE） |
| `DS_FUTURE` | 大连商品交易所（DCE） |
| `ZS_FUTURE` | 郑州商品交易所（CZCE） |
| `SN_FUTURE` | 上海国际能源交易中心（INE） |

### 3.3 ETF 期权

| 枚举值 | 交易所 |
|---|---|
| `EXTRA_ETF_OP` | 全部 ETF 期权（上交所 + 深交所，`get_option_code_list` 默认值） |
| `SH_OPTION` | 上交所 ETF 期权 |
| `SZ_OPTION` | 深交所 ETF 期权 |

---

## 4. `Period` 数据周期枚举

通过 `ad.constant.Period.<name>.value` 获取整型周期枚举：

| 枚举 | 数值 | 说明 | 适用范围 |
|---|---|---|---|
| `Period.snapshot.value` | — | 股票 / 指数 / ETF / 可转债 / 逆回购实时快照 | `SubscribeData` |
| `Period.snapshotfuture.value` | — | 期货实时快照 | `SubscribeData` |
| `Period.snapshotoption.value` | — | ETF 期权实时快照 | `SubscribeData` |
| `Period.snapshotHKT.value` | — | 港股通实时快照（见 [03 篇](03-market-data-and-subscribe.md) 枚举差异提示） | `SubscribeData` |
| `Period.min1` ~ `min120` | `1, 3, 5, 10, 15, 30, 60, 120` | 分钟级 K 线 | 历史查询 & 实时订阅 |
| `Period.day` | `10000` | 日线 | 历史查询 & 实时订阅 |
| `Period.week` | `10001` | 周线 | 历史查询 |
| `Period.month` | `10002` | 月线 | 历史查询 |
| `Period.season` / `Period.year` | — | 季线 / 年线 | 历史查询 |
