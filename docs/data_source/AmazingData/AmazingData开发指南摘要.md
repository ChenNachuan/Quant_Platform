# AmazingData 开发指南摘要

> 基于《AmazingData开发手册》和星耀数智微信公众号内容整理
> 整理日期：2026-05-24

---

## 目录

1. [SDK 概述](#1-sdk-概述)
2. [核心 API 接口](#2-核心-api-接口)
3. [数据结构定义](#3-数据结构定义)
4. [技术指标库](#4-技术指标库)
5. [量化算子库](#5-量化算子库)
6. [MCP Server 工具](#6-mcp-server-工具)
7. [使用示例](#7-使用示例)

---

## 1. SDK 概述

### 1.1 基本信息

| 项目 | 说明 |
|------|------|
| SDK 名称 | AmazingData |
| 来源 | 中国银河证券星耀数智 |
| Python 版本 | 3.8 - 3.13 |
| 操作系统 | Linux / Windows |
| 底层协议 | gRPC over TLS |
| 文档版本 | V1.0.24 |

### 1.2 安装方式

```bash
# 安装 tgw 底层库
pip install tgw-1.7.1-py3-none-any.whl

# 安装 AmazingData (选择对应 Python 版本)
pip install AmazingData-1.0.0-cp312-none-any.whl
```

### 1.3 数据覆盖

| 品种 | 数据类型 | 数据起点 | 实时订阅 |
|------|----------|----------|----------|
| 股票 | Level-1 快照、K 线 | 2013年至今 | 支持 |
| 指数 | Level-1 快照、K 线 | 2013年至今 | 支持 |
| 债券 | Level-1 快照、K 线 | - | 支持 |
| 场内基金 | Level-1 快照、K 线 | - | 支持 |
| 期权 | Level-1 快照、K 线 | 2015年至今 | 支持 |
| 港股通 | 行情快照 | 2023年至今 | 支持 |
| 期货 | Level-1 快照、K 线 | 2010年4月至今 | 支持 |

---

## 2. 核心 API 接口

### 2.1 登录认证

**所有接口调用前必须先登录**

```python
import AmazingData as ad

ad.login(
    username='your_username',
    password='your_password',
    host='***.***.***.***',
    port=****  # 整数端口号
)
```

### 2.2 BaseData - 基础数据类

| 方法 | 功能 | 返回类型 |
|------|------|----------|
| `get_code_list(security_type)` | 获取每日最新代码表 | list[str] |
| `get_code_info(security_type)` | 获取每日最新证券信息 | DataFrame |
| `get_calendar(data_type, market)` | 获取交易日历 | list[int] |
| `get_backward_factor(code_list, local_path, is_local)` | 获取后复权因子 | DataFrame |
| `get_adj_factor(code_list, local_path, is_local)` | 获取单次复权因子 | DataFrame |
| `get_hist_code_list(security_type, start_date, end_date, local_path)` | 获取历史代码表 | list[str] |
| `get_stock_basic(code_list)` | 获取证券基础信息 | DataFrame |
| `get_history_stock_status(code_list, local_path, is_local)` | 获取历史证券信息 | DataFrame |
| `get_future_code_list(security_type)` | 获取期货代码表 | list[str] |
| `get_option_code_list(security_type)` | 获取期权代码表 | list[str] |
| `get_bj_code_mapping()` | 获取北交所新旧代码对照表 | DataFrame |

### 2.3 MarketData - 行情数据类

**初始化**: `market_data_object = ad.MarketData(calendar)`

| 方法 | 功能 | 返回类型 |
|------|------|----------|
| `query_kline(code_list, begin_date, end_date, period, begin_time, end_time)` | 查询历史K线 | dict[str, DataFrame] |
| `query_snapshot(code_list, begin_date, end_date, begin_time, end_time)` | 查询历史快照 | dict[str, DataFrame] |

### 2.4 InfoData - 财务数据类

| 方法 | 功能 | 返回类型 |
|------|------|----------|
| `get_balance_sheet(code_list, ...)` | 获取资产负债表 | dict[str, DataFrame] |
| `get_cash_flow(code_list, ...)` | 获取现金流量表 | dict[str, DataFrame] |
| `get_income(code_list, ...)` | 获取利润表 | dict[str, DataFrame] |
| `get_profit_express(code_list, ...)` | 获取业绩快报 | dict[str, DataFrame] |
| `get_profit_notice(code_list, ...)` | 获取业绩预告 | dict[str, DataFrame] |
| `get_share_holder(code_list, ...)` | 获取十大股东 | dict[str, DataFrame] |
| `get_holder_num(code_list, ...)` | 获取股东户数 | dict[str, DataFrame] |
| `get_equity_structure(code_list, ...)` | 获取股本结构 | dict[str, DataFrame] |
| `get_equity_pledge_freeze(code_list, ...)` | 获取股权冻结/质押 | dict[str, DataFrame] |
| `get_equity_restricted(code_list, ...)` | 获取限售股解禁 | dict[str, DataFrame] |
| `get_dividend(code_list, ...)` | 获取分红数据 | dict[str, DataFrame] |
| `get_right_issue(code_list, ...)` | 获取配股数据 | dict[str, DataFrame] |
| `get_margin_summary(...)` | 获取融资融券汇总 | dict[str, DataFrame] |
| `get_margin_detail(code_list, ...)` | 获取融资融券明细 | dict[str, DataFrame] |
| `get_long_hu_bang(code_list, ...)` | 获取龙虎榜 | dict[str, DataFrame] |
| `get_block_trading(code_list, ...)` | 获取大宗交易 | dict[str, DataFrame] |

### 2.5 SubscribeData - 实时订阅类

```python
sub_data = ad.SubscribeData()

@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshot.value)
def on_snapshot(data, period):
    print(period, data)

sub_data.run()
```

**回调函数类型**:
- `onSnapshot` - 股票/ETF/债券/基金快照
- `onSnapshotindex` - 指数快照
- `onSnapshotfuture` - 期货快照
- `onSnapshotoption` - 期权快照
- `onSnapshotglra` - 逆回购快照
- `onSnapshotHKT` - 港股通快照
- `OnKLine` - K线数据

---

## 3. 数据结构定义

### 3.1 K线数据 (Kline)

| 字段 | 类型 | 说明 |
|------|------|------|
| code | str | 证券代码+市场 |
| trade_time | datetime | 交易所行情数据时间 |
| open | float | 今开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| volume | int | 成交总量 |
| amount | float | 成交总金额 |

### 3.2 股票快照 (Snapshot)

| 字段 | 类型 | 说明 |
|------|------|------|
| code | str | 证券代码+市场 |
| trade_time | datetime | 交易所行情数据时间 |
| trading_phase_code | str | 交易阶段代码 |
| volume | float | 成交总量 |
| amount | float | 成交总金额 |
| pre_close | float | 昨收价 |
| last | float | 最新价 |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| high_limited | float | 涨停价 |
| low_limited | float | 跌停价 |
| ask_price1-5 | float | 卖1-5档价格 |
| ask_volume1-5 | int | 卖1-5档量 |
| bid_price1-5 | float | 买1-5档价格 |
| bid_volume1-5 | int | 买1-5档量 |
| total_long_position | int | 总持仓量 |

### 3.3 指数快照 (SnapshotIndex)

类似 Snapshot，但无买卖盘数据

### 3.4 期货快照 (SnapshotFuture)

| 字段 | 类型 | 说明 |
|------|------|------|
| code | str | 证券代码+市场 |
| trade_time | datetime | 交易所行情数据时间 |
| action_day | str | 业务日期 |
| trading_day | str | 交易日期 |
| pre_close | float | 昨收价 |
| pre_settle | float | 上次结算价 |
| pre_open_interest | int | 昨持仓量 |
| open_interest | int | 持仓量 |
| last | float | 最新价 |
| open/high/low/close | float | OHLC |
| volume | float | 成交总量 |
| amount | float | 成交总金额 |
| high_limited/low_limited | float | 涨跌停价 |
| ask_price1-5 | float | 卖1-5档价格 |
| bid_price1-5 | float | 买1-5档价格 |
| average_price | float | 当日均价 |
| settle | float | 本次结算价 |

### 3.5 期权快照 (SnapshotOption)

类似 Snapshot，增加期权特有字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| contract_type | str | 合约类别 |
| expire_date | int | 到期日 |
| underlying_security_code | str | 标的代码 |
| exercise_price | float | 行权价 |

---

## 4. 技术指标库

### 4.1 概述

共 **57 个**技术指标，分为 7 大类：

| 类别 | 数量 | 说明 |
|------|------|------|
| 超买超卖型 | 14 | 判断市场超买超卖状态 |
| 趋势型 | 14 | 识别市场趋势方向和强度 |
| 能量型 | 5 | 衡量市场多空能量对比 |
| 成交量型 | 10 | 分析成交量变化和资金流向 |
| 均线型 | 4 | 各类移动平均线指标 |
| 路径型 | 6 | 构建价格通道和轨道 |
| 其他型 | 4 | 其他常用技术指标 |

### 4.2 超买超卖型 (14个)

| 指标 | 函数 | 输出 | 说明 |
|------|------|------|------|
| KDJ | `KDJ(close, high, low, n=9, m1=3, m2=3)` | K, D, J | K>80超买，K<20超卖 |
| RSI | `RSI(close, n1=6, n2=12, n3=24)` | RSI6, RSI12, RSI24 | RSI>80超买，RSI<20超卖 |
| WR | `WR(close, high, low, n1=10, n2=6)` | WR10, WR6 | WR>80超卖，WR<20超买 |
| CCI | `CCI(close, high, low, n=14)` | CCI | CCI>100超买，CCI<-100超卖 |
| ROC | `ROC(close, n=12, m=6)` | ROC, MAROC | 变动率指标 |
| MTM | `MTM(close, n=12, m=6)` | MTM, MAMTM | 动量指标 |
| BIAS | `BIAS(close, n1=6, n2=12, n3=24)` | BIAS6, BIAS12, BIAS24 | 乖离率 |
| SKDJ | `SKDJ(close, high, low, n=9, m=3)` | K, D | 慢速随机指标 |
| MFI | `MFI(close, high, low, volume, n=14)` | MFI | MFI>80超买，MFI<20超卖 |
| OSC | `OSC(close, n=20, m=6)` | OSC, MAOSC | 变动速率线 |
| UDL | `UDL(close, n1=3, n2=5, n3=10, n4=20, m=6)` | UDL, MAUDL | 引力线 |
| ACCER | `ACCER(close, n=8)` | ACCER | 幅度涨速 |
| RCCD | `RCCD(close, n=59, short=26, long=52, m=26)` | DIF, RCCD | 异同离差乖离率 |
| MARSI | `MARSI(close, n=6, m=12)` | RSI, MARSI | 相对强弱平均线 |

### 4.3 趋势型 (14个)

| 指标 | 函数 | 输出 | 说明 |
|------|------|------|------|
| MACD | `MACD(close, short=12, long=26, mid=9)` | DIF, DEA, MACD | 指数平滑异同移动平均线 |
| DMI | `DMI(close, high, low, n=14, m=6)` | PDI, MDI, ADX, ADXR | 趋向指标 |
| DMA | `DMA(close, n1=10, n2=50, m=10)` | DIF, AMA | 平行线差指标 |
| TRIX | `TRIX(close, n=12, m=9)` | TRIX, MATRIX | 三重指数平滑移动平均 |
| ARBR | `ARBR(close, open_, high, low, n=26)` | AR, BR | 人气意愿指标 |
| EMV | `EMV(close, high, low, volume, n=14, m=9)` | EMV, MAEMV | 简易波动指标 |
| DPO | `DPO(close, n=20, m=6)` | DPO, MADPO | 区间震荡线 |
| VHF | `VHF(close, n=28)` | VHF | 十字过滤线 |
| CHO | `CHO(close, high, low, volume, n1=10, n2=20, m=6)` | CHO, MACHO | 佳庆指标 |
| DBCD | `DBCD(close, n=5, m=16, t=17)` | DBCD, MM | 异同离差乖离率 |
| DDI | `DDI(close, high, low, n=13, n1=26, m=1, m1=5)` | DDI, ADDI, AD | 方向标准离差指数 |
| JS | `JS(close, n=5, m1=5, m2=10, m3=20)` | JS, MAJ5, MAJ10, MAJ20 | 加速线 |
| QACD | `QACD(close, n1=12, n2=26, m=9)` | DIF, MACD, QACD | 快速异同移动平均 |
| UOS | `UOS(close, high, low, n1=7, n2=14, n3=28, m=6)` | UOS, MAUOS | 终极波动指标 |

### 4.4 能量型 (5个)

| 指标 | 函数 | 输出 | 说明 |
|------|------|------|------|
| CR | `CR(close, high, low, n=26)` | CR | CR>200超买，CR<40超卖 |
| PSY | `PSY(close, n=12, m=6)` | PSY, MAPSY | PSY>75超买，PSY<25超卖 |
| MASS | `MASS(high, low, n1=9, n2=25, m=6)` | MASS, MAMASS | 梅斯线 |
| PCNT | `PCNT(close)` | PCNT | 幅度比 |
| WAD | `WAD(close, high, low)` | WAD, MAWAD | 威廉多空力度线 |

### 4.5 成交量型 (10个)

| 指标 | 函数 | 输出 | 说明 |
|------|------|------|------|
| OBV | `OBV(close, volume)` | OBV | 能量潮 |
| VR | `VR(close, volume, n=26)` | VR | VR>450超买，VR<70超卖 |
| VOLMA | `VOLMA(volume, n1=5, n2=10)` | VOLMA5, VOLMA10 | 成交量均线 |
| WVAD | `WVAD(close, open_, high, low, volume, n=24, m=6)` | WVAD, MAWVAD | 威廉变异离散量 |
| VOSC | `VOSC(volume, short=12, long=26)` | VOSC | 成交量震荡 |
| VRSI | `VRSI(volume, n1=6, n2=12, n3=24)` | VRSI6, VRSI12, VRSI24 | 量相对强弱 |
| VSTD | `VSTD(volume, n=10)` | VSTD | 成交量标准差 |
| AMO | `AMO(amount, n1=5, n2=10)` | AMO5, AMO10 | 成交额均线 |
| HSL | `HSL(turnover_rate, n=5, m=10)` | HSL5, HSL10 | 换手线 |
| TAPI | `TAPI(close, amount, n=6)` | TAPI, MATAPI | 加权指数成交值 |

### 4.6 均线型 (4个)

| 指标 | 函数 | 输出 | 说明 |
|------|------|------|------|
| MA | `MA(close, n1=5, n2=10, n3=20, n4=60)` | MA5, MA10, MA20, MA60 | 移动平均线 |
| EXPMA | `EXPMA(close, n1=12, n2=50)` | EXPMA12, EXPMA50 | 指数平均线 |
| BBI | `BBI(close)` | BBI | 多空指标 |
| AMV | `AMV(volume, amount, n1=5, n2=13, n3=34)` | AMV5, AMV13, AMV34 | 成本价均线 |

### 4.7 路径型 (6个)

| 指标 | 函数 | 输出 | 说明 |
|------|------|------|------|
| BOLL | `BOLL(close, n=20, k=2)` | UPPER, MID, LOWER | 布林线 |
| ENE | `ENE(close, n=25, m1=6, m2=6)` | UPPER, ENE, LOWER | 轨道线 |
| MIKE | `MIKE(close, high, low, n=12)` | WR, MR, SR, WS, MS, SS | 麦克指标 |
| PBX | `PBX(close, n1=4, n2=6, n3=9, n4=13, n5=18, n6=24)` | PBX4...PBX24 | 瀑布线 |
| XS | `XS(close, high, low, n=13)` | UPP, SUP, SDN, LWN | 薛斯通道 |
| BBIBOLL | `BBIBOLL(close, n=11, k=6)` | BBIBOLL, UPPER, LOWER | BBI多空布林线 |

### 4.8 其他型 (4个)

| 指标 | 函数 | 输出 | 说明 |
|------|------|------|------|
| ASI | `ASI(close, open_, high, low)` | SI, ASI | 振动升降指标 |
| ATR | `ATR(close, high, low, n=14)` | TR, ATR | 真实波幅均值 |
| SAR | `SAR(close, high, low, n=4, step=0.02, max_af=0.2)` | SAR | 抛物线转向指标 |
| CDP | `CDP(close, high, low)` | AH, NH, CDP, NL, AL | 逆势操作 |

---

## 5. 量化算子库

### 5.1 时序函数 (51个)

#### 位置信息函数
- `BARSTATUS(X)` - 返回数据位置信息 (1=第一根K线, 2=最后一根, 0=中间)
- `CURRBARSCOUNT(X)` - 从最新K线倒数编号
- `TOTALBARSCOUNT(X)` - 从第一根K线递增编号

#### 条件周期统计
- `BARSLAST(X)` - 上一次条件成立到当前的周期数
- `BARSLASTCOUNT(X)` - 连续满足条件的周期数
- `BARSLASTS(X,N)` - 倒数第N次成立距今周期数
- `BARSNEXT(X)` - 下一次条件成立到当前周期数 (未来函数)
- `BARSSINCE(X)` - 第一次条件成立到当前周期数
- `BARSSINCEN(X,N)` - N周期内第一次成立到当前周期数
- `COUNT(X,N)` - N周期中满足条件的周期数

#### 最值函数
- `HHV(X,N)` - N周期内最高值
- `HHVBARS(X,N)` - 最高值到当前周期数
- `HOD(X,N)` - 当前值是第几个高值
- `LLV(X,N)` - N周期内最低值
- `LLVBARS(X,N)` - 最低值到当前周期数
- `LOD(X,N)` - 当前值是第几个低值
- `HHVLLV(X,T,N1,N2)` - 阶段最高/最低值

#### 引用函数
- `REF(X,A)` - 引用A周期前的值
- `REFV(X,A)` - 引用A周期前的值(平滑)
- `REFX(X,A)` - 引用A周期后的值 (未来函数)
- `REFXV(X,A)` - 引用A周期后的值(平滑) (未来函数)
- `SHIFT(A,N)` - 获取N个交易日前的值
- `REVERSE(X)` - 返回相反数

#### 累计函数
- `SUM(X,N)` - N周期总和
- `CUMSUM(X)` - 累计求和
- `MULAR(X,N)` - N周期累乘
- `SUMBARS(X,A)` - 向前累加到指定值的周期数

#### 移动平均函数
- `MA(X,N)` - 简单移动平均
- `EMA(X,N)` - 指数移动平均
- `SMA(X,N,M)` - 移动平均 (M为权重)
- `WMA(X,N)` - 加权移动平均
- `DMA(X,A)` - 动态移动平均
- `AMA(X,A)` - 自适应均线
- `MEMA(X,N)` - 平滑移动平均
- `EXPMEMA(X,N)` - 指数平滑移动平均
- `TMA(X,A,B)` - 移动平均

#### 条件判断函数
- `CROSS(A,B)` - A从下方穿过B
- `LONGCROSS(A,B,N)` - A在N周期内都小于B后上穿
- `UPNDAY(CLOSE,M)` - 连涨M个周期
- `DOWNNDAY(CLOSE,M)` - 连跌M个周期
- `NDAY(CLOSE,OPEN,3)` - 连续3日收阳
- `EXIST(X,N)` - N日内是否存在
- `EVERY(X,N)` - N日内一直满足
- `LAST(X,A,B)` - 从前A日到前B日一直满足
- `RANGE(A,B,C)` - A在B和C之间

#### 信号过滤
- `FILTER(X,N)` - 满足条件后N周期内数据置0
- `FILTERX(X,N)` - 满足条件前N周期内数据置0

#### 其他
- `TR(HIGH,LOW,CLOSE)` - 真实波幅
- `SAR(HIGH,LOW,CLOSE,N,STEP,MAXAF)` - 抛物线转向

### 5.2 统计函数 (18个)

| 函数 | 说明 |
|------|------|
| `AVEDEV(X,N)` | 平均绝对偏差 |
| `STD(X,N)` | 估算标准差(样本标准差) |
| `STDP(X,N)` | 总体标准差 |
| `STDDEV(X,N)` | 标准偏差 |
| `VAR(X,N)` | 估算样本方差 |
| `VARP(X,N)` | 总体样本方差 |
| `DEVSQ(X,N)` | 数据偏差平方和 |
| `COVAR(X,Y,N)` | 协方差 |
| `RELATE(X,Y,N)` | 相关系数 |
| `BETA(X,BENCHMARK,N)` | 贝塔系数 |
| `BETAEX(X,Y,N)` | 相关放大系数 |
| `SLOPE(X,N)` | 线性回归斜率 |
| `FORCAST(X,N)` | 线性回归预测值 |
| `SKEW(X,N)` | 偏度 |
| `KURTOSIS(X,N)` | 峰度 |
| `MEAN(X,N)` | 平均值 |
| `MEDIAN(X,N)` | 中位数 |
| `QUANTILE(X,N,M)` | 分位数 |

### 5.3 截面函数 (16个)

| 函数 | 说明 |
|------|------|
| `CSMEAN(X)` | 截面平均值 |
| `CSMEDIAN(X)` | 截面中位数 |
| `CSMAX(X)` | 截面最大值 |
| `CSMIN(X)` | 截面最小值 |
| `CSSTD(X)` | 截面标准差 |
| `CSVAR(X)` | 截面方差 |
| `CSSUM(X)` | 截面求和 |
| `CSCOUNT(X)` | 截面标的个数 |
| `CSRANK(X,B)` | 截面排名 |
| `CSPCTRANK(X)` | 截面百分位排名 |
| `CSQUANTILE(X,N)` | 截面分位数 |
| `CSZSCORE(X)` | 截面Z-score标准化 |
| `CSNORMALIZE(X)` | 截面归一化 [0,1] |
| `CSDEMEAN(X)` | 截面去均值 |
| `CSCORR(X,Y)` | 截面相关度 |
| `CSCOV(X,Y)` | 截面协方差 |

---

## 6. MCP Server 工具

### 6.1 工具列表 (26个)

| 工具 | 功能 |
|------|------|
| `mcp_kzz` | 查询可转债代码表 |
| `mcp_code_info` | 获取每日最新证券信息 |
| `mcp_code_list` | 获取代码表 |
| `mcp_backward_factor` | 获取后复权因子 |
| `mcp_calendar` | 查询交易日历 |
| `mcp_stock_basic` | 获取证券基础数据 |
| `mcp_history_stock_status` | 获取历史证券数据 |
| `mcp_bj_code_mapping` | 获取北交所新旧代码对照表 |
| `mcp_kline` | 查询K线数据 |
| `mcp_snapshot` | 查询快照数据 |
| `mcp_balance_sheet` | 获取资产负债表 |
| `mcp_cash_flow` | 获取现金流量表 |
| `mcp_income` | 获取利润表 |
| `mcp_profit_express` | 获取业绩快报 |
| `mcp_profit_notice` | 获取业绩预告 |
| `mcp_share_holder` | 获取十大股东 |
| `mcp_holder_num` | 获取股东户数 |
| `mcp_equity_structure` | 获取股本结构 |
| `mcp_equity_pledge_freeze` | 获取股权冻结/质押 |
| `mcp_equity_restricted` | 获取限售股解禁 |
| `mcp_dividend` | 获取分红数据 |
| `mcp_right_issue` | 获取配股数据 |
| `mcp_margin_summary` | 获取融资融券汇总 |
| `mcp_margin_detail` | 获取融资融券明细 |
| `mcp_long_hu_bang` | 获取龙虎榜 |
| `mcp_block_trading` | 获取大宗交易 |

### 6.2 配置方式

```json
{
  "mcpServers": {
    "AmazingData": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "--env-file", "/path/to/.env",
        "amazingdata-mcp"
      ]
    }
  }
}
```

`.env` 文件配置:
```
AMAZINGDATA_USER=your_username
AMAZINGDATA_PASSWORD=your_password
AMAZINGDATA_HOST=***.***.***.***
AMAZINGDATA_PORT=****
```

---

## 7. 使用示例

### 7.1 基础数据获取

```python
import AmazingData as ad

# 登录
ad.login(username='****', password='****', host='***.***.***.***', port=****)

# 获取基础数据
base_data = ad.BaseData()

# 获取代码表
stock_codes = base_data.get_code_list(security_type='EXTRA_STOCK_A')
etf_codes = base_data.get_code_list(security_type='EXTRA_ETF')

# 获取交易日历
calendar = base_data.get_calendar()

# 获取复权因子
backward_factor = base_data.get_backward_factor(
    ['600519.SH'],
    local_path='/path/to/local_data/',
    is_local=False
)
```

### 7.2 K线数据查询

```python
# 实例化行情数据类
market_data = ad.MarketData(calendar)

# 查询日K线
kline_dict = market_data.query_kline(
    code_list=['600519.SH', '000001.SZ'],
    begin_date=20240101,
    end_date=20241231,
    period=ad.constant.Period.day.value
)

# 获取单只股票数据
df = kline_dict['600519.SH']
print(df[['open', 'high', 'low', 'close', 'volume']].tail())
```

### 7.3 前复权处理

```python
def forward_adjust(df, backward_factor, code):
    """对K线数据做前复权"""
    df_adj = df.copy()
    factor = backward_factor[code]
    kline_dates = pd.to_datetime(df_adj['kline_time'])
    factor_aligned = factor.reindex(kline_dates, method='ffill')
    latest_factor = factor_aligned.dropna().iloc[-1]
    adj_ratio = factor_aligned / latest_factor
    
    for col in ['open', 'high', 'low', 'close']:
        if col in df_adj.columns:
            df_adj[col] = df_adj[col] * adj_ratio
    
    return df_adj
```

### 7.4 技术指标计算

```python
from AmazingData.operator.technical_indicators import TechnicalIndicators

TI = TechnicalIndicators

# 获取数据
close = df['close']
high = df['high']
low = df['low']
open_ = df['open']
volume = df['volume']

# 计算 MACD
macd = TI.MACD(close)
print(f"DIF: {macd['DIF'].iloc[-1]:.4f}")
print(f"DEA: {macd['DEA'].iloc[-1]:.4f}")
print(f"MACD: {macd['MACD'].iloc[-1]:.4f}")

# 计算 KDJ
kdj = TI.KDJ(close, high, low)
print(f"K: {kdj['K'].iloc[-1]:.2f}, D: {kdj['D'].iloc[-1]:.2f}, J: {kdj['J'].iloc[-1]:.2f}")

# 计算 RSI
rsi = TI.RSI(close)
print(f"RSI6: {rsi['RSI6'].iloc[-1]:.2f}")

# 计算布林线
boll = TI.BOLL(close)
print(f"UPPER: {boll['UPPER'].iloc[-1]:.2f}")
print(f"MID: {boll['MID'].iloc[-1]:.2f}")
print(f"LOWER: {boll['LOWER'].iloc[-1]:.2f}")
```

### 7.5 财务数据获取

```python
info_data = ad.InfoData()

# 获取资产负债表
balance_sheet = info_data.get_balance_sheet(
    code_list=['600519.SH'],
    local_path='/path/to/local_data/',
    is_local=False
)

# 获取利润表
income = info_data.get_income(['600519.SH'])

# 获取十大股东
share_holders = info_data.get_share_holder(['600519.SH'])
```

### 7.6 实时订阅

```python
# 订阅股票快照
sub_data = ad.SubscribeData()

@sub_data.register(
    code_list=['600519.SH', '000001.SZ'],
    period=ad.constant.Period.snapshot.value
)
def on_snapshot(data, period):
    print(f"[{period}] {data.code}: {data.last}")

sub_data.run()
```

---

## 附录

### A. 代码类型 security_type

#### 沪深北
| 枚举值 | 说明 |
|--------|------|
| EXTRA_STOCK_A | 上交所A股、深交所A股和北交所股票列表 |
| SH_A | 上交所A股 |
| SZ_A | 深交所A股 |
| BJ_A | 北交所 |
| EXTRA_INDEX_A | 沪深北指数 |
| EXTRA_ETF | 沪深ETF |
| EXTRA_KZZ | 沪深可转债 |

#### 期货交易所
| 枚举值 | 说明 |
|--------|------|
| EXTRA_FUTURE | 期货(全部) |
| EXTRA_FUTURE_CFFEX | 中金所期货 |
| EXTRA_FUTURE_SHFE | 上期所期货 |
| EXTRA_FUTURE_DCE | 大商所期货 |
| EXTRA_FUTURE_CZCE | 郑商所期货 |
| EXTRA_FUTURE_INE | 上海国际能源交易中心期货 |

#### 期权
| 枚举值 | 说明 |
|--------|------|
| EXTRA_ETF_OP | ETF期权(上交所+深交所) |
| SH_OPTION | 上交所ETF期权 |
| SZ_OPTION | 深交所ETF期权 |

### B. 数据周期 Period

| 枚举值 | 说明 |
|--------|------|
| Period.snapshot.value | 快照 |
| Period.min1.value | 1分钟线 |
| Period.min3.value | 3分钟线 |
| Period.min5.value | 5分钟线 |
| Period.min10.value | 10分钟线 |
| Period.min15.value | 15分钟线 |
| Period.min30.value | 30分钟线 |
| Period.min60.value | 60分钟线 |
| Period.day.value | 日线 |
| Period.week.value | 周线 |
| Period.month.value | 月线 |
| Period.year.value | 年线 |

### C. 市场类型 market

| 枚举值 | 说明 |
|--------|------|
| SH | 上海 |
| SZ | 深圳 |
| BJ | 北京 |

---

## 注意事项

1. **登录必须先于所有接口调用**
2. **本地缓存**: 部分接口支持 `is_local=True` 使用本地缓存，可提高查询速度
3. **复权处理**: 股票K线数据需要做前复权处理，指数和基金不需要
4. **数据格式**: 日期格式为 `YYYYMMDD` 整数类型
5. **代码格式**: 统一使用 `XXXXXX.SZ` 或 `XXXXXX.SH` 格式