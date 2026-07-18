# AmazingData SDK API Reference

中国银河证券星耀数智量化平台金融资讯数据说明

## Overview

AmazingData (中国银河证券星耀数智) provides financial data via a Docker-hosted SDK. Three main data classes:

| Class | Purpose |
|-------|---------|
| `BaseData` | Security codes, adjusted factors, backward factors, calendars |
| `MarketData` | K-line (OHLCV), snapshots, tick data |
| `InfoData` | Financial reports, ETF data, index data, convertible bonds |

## Connection

```python
import AmazingData as ad

ad.login(username='username', password='password', host='***.***.***.***', port=****)
# ... use SDK ...
ad.logout(username='username')
```

**Docker execution** (required):

```bash
docker run --rm \
  -v $(pwd)/data_lake:/data \
  --env-file ~/AmazingData/docker/.env \
  --entrypoint python3 \
  docker-amazingdata \
  /app/scripts/your_script.py
```

---

## 1. BaseData

### 1.1 Code Lists

#### `get_code_list(security_type)`

Returns current list of securities for a given type.

```python
base = BaseData()
code_list = base.get_code_list(security_type='EXTRA_ETF')
# Returns: list[str]
```

#### `get_hist_code_list(security_type, start_date, end_date, local_path)`

Returns historical code lists (including delisted securities). Returns dict keyed by date.

```python
base = BaseData()
result = base.get_hist_code_list(
    security_type='EXTRA_STOCK_A_SH_SZ',
    start_date=20240101,
    end_date=20240701,
    local_path='/data/hist_code_list/'
)
# Returns: dict[date -> list[str]]
```

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `security_type` | str | Yes | Security type code |
| `start_date` | int | No | Start date YYYYMMDD |
| `end_date` | int | No | End date YYYYMMDD |
| `local_path` | str | Yes | Local cache path (absolute) |

**Note**: When `local_path` does not exist, the method tries local cache first. If not found, it returns empty. Must provide a valid local path for server fallback.

#### Security Types

| Code | Description |
|------|-------------|
| `EXTRA_STOCK_A` | A股全部 |
| `EXTRA_STOCK_A_SH_SZ` | A股沪深（含退市） |
| `EXTRA_STOCK_A_MAIN` | A股主板 |
| `EXTRA_STOCK_A_GEM` | A股创业板 |
| `EXTRA_STOCK_A_STAR` | A股科创板 |
| `EXTRA_ETF` | ETF |
| `EXTRA_INDEX_A` | A股指数 |
| `EXTRA_KZZ` | 可转债 |
| `EXTRA_OPTION` | 期权 |

### 1.2 Adjusted Factors

#### `get_adj_factor(code_list, is_local, local_path, begin_date, end_date)`

```python
adj_factor = base.get_adj_factor(code_list, is_local=False)
```

### 1.3 Backward Factors

#### `get_backward_factor(code_list, is_local, local_path, begin_date, end_date)`

```python
backward_factor = base.get_backward_factor(code_list, is_local=False)
```

### 1.4 Calendar

#### `get_calendar()`

Returns trading calendar.

### 1.5 Code Info

#### `get_code_info(code_list)`

Returns code metadata (name, market, etc.)

### 1.6 Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `stock_list` | list | List of stock codes |
| `code_list_hist` | list | Historical code list |

---

## 2. MarketData

### Constructor

```python
md = MarketData(calendar='CN')
```

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `calendar` | str | Yes | Trading calendar. Use `'CN'` for A-share market |

### 2.1 K-Line Data — `query_kline`

```python
md = MarketData(calendar='CN')

# Single stock
result = md.query_kline(
    code_list=['600000.SH'],
    begin_date=20240101,
    end_date=20240110,
    period=10000,
)
# Returns: dict[code -> DataFrame]
```

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `code_list` | list[str] | Yes | List of security codes |
| `begin_date` | int | Yes | Start date YYYYMMDD (integer!) |
| `end_date` | int | Yes | End date YYYYMMDD (integer!) |
| `period` | int | Yes | Data period (see Period table below) |
| `begin_time` | int | No | Intraday start time HHMM (e.g. 900) |
| `end_time` | int | No | Intraday end time HHMM (e.g. 1725) |

**Returns**: `dict[str, DataFrame]` — key is code, value is DataFrame with datetime index and Kline columns.

**Important**: `begin_date` and `end_date` must be **integers** (e.g., `20240101`), NOT strings.

### 2.2 Snapshot Data — `query_snapshot`

```python
result = md.query_snapshot(
    code_list=['600000.SH'],
    begin_date=20240101,
    end_date=20240110,
)
```

### 2.3 Period Codes

通过 `ad.constant.Period.{name}.value` 获取：

| Enum | Value | 说明 |
|------|-------|------|
| `Period.snapshot` | - | 快照 |
| `Period.day` | 10000 | 日线 |
| `Period.min1` | 1 | 1分钟线 |
| `Period.min3` | 3 | 3分钟线 |
| `Period.min5` | 5 | 5分钟线 |
| `Period.min10` | 10 | 10分钟线 |
| `Period.min15` | 15 | 15分钟线 |
| `Period.min30` | 30 | 30分钟线 |
| `Period.min60` | 60 | 60分钟线 |
| `Period.min120` | 120 | 120分钟线 |
| `Period.week` | 10001 | 周线 |
| `Period.month` | 10002 | 月线 |
| `Period.season` | - | 季度线 |
| `Period.year` | - | 年线 |

---

## 3. InfoData

### 3.1 Financial Statements

#### `get_financial_report(code_list, report_type, is_local, local_path, begin_date, end_date)`

```python
info = InfoData()
report = info.get_financial_report(
    code_list=['600000.SH'],
    report_type='INCOME',  # BALANCE_SHEET, CASH_FLOW, INCOME
    is_local=False
)
```

| `report_type` | Description |
|---------------|-------------|
| `BALANCE_SHEET` | 资产负债表 |
| `CASH_FLOW` | 现金流量表 |
| `INCOME` | 利润表 |

### 3.2 Shareholder Data

#### `get_top10_shareholder(code_list, is_local, local_path, begin_date, end_date)`

```python
top10 = info.get_top10_shareholder(code_list=['600000.SH'], is_local=False)
```

#### `get_holder_num(code_list, is_local, local_path, begin_date, end_date)`

```python
holder_num = info.get_holder_num(code_list=['600000.SH'], is_local=False)
```

#### `get_equity_structure(code_list, is_local, local_path, begin_date, end_date)`

```python
equity = info.get_equity_structure(code_list=['600000.SH'], is_local=False)
```

### 3.3 ETF Data

#### `get_fund_share(code_list, local_path, is_local, begin_date, end_date)`

```python
base = BaseData()
etf_codes = base.get_code_list(security_type='EXTRA_ETF')
info = InfoData()
fund_share = info.get_fund_share(etf_codes, is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: FUND_SHARE, CHANGE_REASON, IS_CONSOLIDATED_DATA, MARKET_CODE,
#           ANN_DATE, TOTAL_SHARE, CHANGE_DATE, FLOAT_SHARE
```

#### `get_fund_nav(code_list, local_path, is_local, begin_date, end_date)`

```python
fund_nav = info.get_fund_nav(etf_codes, is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: MARKET_CODE, ANN_DATE, PRICE_DATE, UNIT_NAV, ACCUM_NAV, ACCUM_DIV,
#           NAV_ADJ_FACTOR, IS_EX_DIVIDEND_DATE, ACCUM_UNIT_DIST,
#           TOTAL_NET_ASSET_VALUE, ADJ_UNIT_NAV, IS_MERGED_DATA,
#           NET_ASSET_VALUE, INNER_CODE, OUTER_CODE, ACCUM_UNIT_NAV
```

#### `get_fund_iopv(code_list, local_path, is_local, begin_date, end_date)`

```python
fund_iopv = info.get_fund_iopv(etf_codes, is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: MARKET_CODE, PRICE_DATE, IOPV_NAV
```

### 3.4 Index Data

#### `get_index_constituent(code_list, local_path, is_local)`

```python
base = BaseData()
index_codes = base.get_code_list(security_type='EXTRA_INDEX_A')
info = InfoData()
index_constituent = info.get_index_constituent(index_codes, is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: INDEX_CODE, CON_CODE, INDATE, OUTDATE, INDEX_NAME
```

#### `get_index_weight(code_list, local_path, is_local, begin_date, end_date)`

Supported indices: 000016.SH (上证50), 000300.SH (沪深300), 000905.SH (中证500), 000906.SH (中证800), 000852.SH (中证1000)

```python
index_weight = info.get_index_weight(
    ['000016.SH', '000300.SH', '000905.SH', '000906.SH', '000852.SH'],
    is_local=False
)
# Returns: dict[code -> DataFrame]
# Columns: INDEX_CODE, CON_CODE, TRADE_DATE, TOTAL_SHARE, FREE_SHARE_RATIO,
#           CALC_SHARE, WEIGHT_FACTOR, WEIGHT, CLOSE
```

### 3.5 Industry Index Data

#### `get_industry_base_info(local_path, is_local)`

```python
info = InfoData()
industry_base_info = info.get_industry_base_info(is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: INDEX_CODE, INDUSTRY_CODE, LEVEL_TYPE (1=L1, 2=L2, 3=L3),
#           LEVEL1_NAME, LEVEL2_NAME, LEVEL3_NAME, IS_PUB, CHANGE_REASON
```

#### `get_industry_constituent(code_list, local_path, is_local)`

```python
industry_list = list(industry_base_info['INDEX_CODE'])
industry_constituent = info.get_industry_constituent(industry_list, is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: INDEX_CODE, CON_CODE, INDATE, OUTDATE, INDEX_NAME
```

#### `get_industry_weight(code_list, local_path, is_local, begin_date, end_date)`

```python
industry_weight = info.get_industry_weight(industry_list, is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: WEIGHT, CON_CODE, TRADE_DATE, INDEX_CODE
```

#### `get_industry_daily(code_list, local_path, is_local, begin_date, end_date)`

```python
industry_daily = info.get_industry_daily(industry_list, is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: OPEN, HIGH, CLOSE, LOW, AMOUNT, VOLUME, PB, PE, TOTAL_CAP,
#           A_FLOAT_CAP, INDEX_CODE, PRE_CLOSE, TRADE_DATE
```

### 3.6 Convertible Bond Data

#### `get_kzz_issuance(code_list, local_path, is_local)`

```python
kzz_codes = base.get_code_list(security_type='EXTRA_KZZ')
kzz_issuance = info.get_kzz_issuance(kzz_codes, is_local=False)
# Returns: dict[code -> DataFrame]
# Many columns including: MARKET_CODE, STOCK_CODE, CRNCY_CODE, ANN_DT, ...
```

#### `get_kzz_share(code_list, local_path, is_local)`

```python
kzz_share = info.get_kzz_share(kzz_codes, is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: CHANGE_DATE, ANN_DATE, MARKET_CODE, BOND_SHARE, CONV_SHARE, CHANGE_REASON
```

#### `get_kzz_conv(code_list, local_path, is_local)`

```python
kzz_conv = info.get_kzz_conv(kzz_codes, is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: MARKET_CODE, ANN_DATE, CONV_CODE, CONV_NAME, CONV_PRICE,
#           CURRENCY_CODE, CONV_START_DATE, CONV_END_DATE, TRADE_DATE_LAST,
#           FORCED_CONV_DATE, FORCED_CONV_PRICE, REL_CONV_MONTH, IS_FORCED,
#           FORCED_CONV_REASON
```

#### `get_kzz_conv_change(code_list, local_path, is_local)`

```python
kzz_conv_change = info.get_kzz_conv_change(kzz_codes, is_local=False)
# Returns: dict[code -> DataFrame]
# Columns: MARKET_CODE, CHANGE_DATE, ANN_DATE, CONV_PRICE, CHANGE_REASON
```

---

## Key Patterns

### Batch Processing

Most `InfoData` methods accept `code_list` (list[str]) and return `dict[code -> DataFrame]`.

For APIs with limits (e.g., `get_option_basic_info`), batch in groups of 10:

```python
all_results = []
for i in range(0, len(codes), 10):
    batch = codes[i:i+10]
    result = info.get_option_basic_info(batch, is_local=False)
    all_results.append(result)
final_df = pd.concat(all_results, ignore_index=True)
```

### `is_local` Parameter

Most `InfoData` methods have `is_local` (default `True`):

- `True` (default): Read from local cache only
- `False`: Fetch from server, then cache locally
- First run: must use `is_local=False` to populate cache

### Date Format

Dates are integers: `YYYYMMDD` (e.g., `20240101`)

### Output Format

Most `InfoData` methods return `dict[str, pd.DataFrame]`:
- Key: security code (e.g., `'600000.SH'`)
- Value: DataFrame with date index and data columns

### Logout

**Always call `ad.logout()` when done** to release connection slots:

```python
try:
    # ... work ...
finally:
    ad.logout(username=os.getenv("USER"))
```
