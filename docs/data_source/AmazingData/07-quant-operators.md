# 07. 量化算子库（108 项）

底层算子按适用维度分为四大类（`MathFunction` 最低要求 SDK `1.0.3`，其余三类最低要求 `1.0.30`）：

```python
from AmazingData.operator.math_function import MathFunction
from AmazingData.operator.time_series_function import TimeSeriesFunction
from AmazingData.operator.statistics_function import StatisticsFunction
from AmazingData.operator.cross_section_function import CrossSectionFunction
```

> **未来函数警告**：回测/因子生产严禁直接使用 `BARSNEXT`、`REFX`、`REFXV`（包含未来数据）。

---

## 1. 数学函数 `MathFunction`（23 项）

输入 `pd.Series`，逐元素数学运算：

- **最值与区间**：`MAX(*args)`, `MIN(*args)`, `BETWEEN(a, b, c)`
- **三角函数**：`ACOS(x)`, `ASIN(x)`, `ATAN(x)`, `COS(x)`, `SIN(x)`, `TAN(x)`
- **指对幂与根**：`EXP(x)`, `LN(x)`, `LOG(x)`（以 10 为底）, `SQRT(x)`, `POW(a, b)`
- **取整与符号**：`ABS(x)`, `CEILING(x)`, `FLOOR(x)`, `INTPART(x)`, `FRACPART(x)`, `ROUND(x, n)`, `SIGN(x)`
- **取模与随机**：`MOD(x, n)`, `RAND(a, b)`

---

## 2. 时序函数 `TimeSeriesFunction`（51 项）

输入单标的时间序列 `pd.Series`：

| 子类 | 算子清单 |
|---|---|
| **位置编号（3）** | `BARSTATUS(X)`（1 首根 / 2 末根 / 0 中间）、`CURRBARSCOUNT(X)`（倒数编号）、`TOTALBARSCOUNT(X)`（正序编号） |
| **条件周期统计（8）** | `BARSLAST(X)`, `BARSLASTCOUNT(X)`, `BARSLASTS(X,N)`, `BARSNEXT(X)` *(未来)*, `BARSSINCE(X)`, `BARSSINCEN(X,N)`, `COUNT(X,N)`, `EXIST(X,N)` |
| **多周期条件（9）** | `EXISTR(X,A,B)`, `EVERY(X,N)`, `LAST(X,A,B)`, `CROSS(A,B)`, `LONGCROSS(A,B,N)`, `UPNDAY(CLOSE,M)`, `DOWNNDAY(CLOSE,M)`, `NDAY(X,Y,N)`, `RANGE(A,B,C)` |
| **极值与名次（7）** | `HHV(X,N)`, `HHVBARS(X,N)`, `LLV(X,N)`, `LLVBARS(X,N)`, `HOD(X,N)`, `LOD(X,N)`, `HHVLLV(X,T,N1,N2)` |
| **滞后引用（6）** | `REF(X,A)`, `REFV(X,A)`, `REFX(X,A)` *(未来)*, `REFXV(X,A)` *(未来)*, `SHIFT(A,N)`, `REVERSE(X)` |
| **累计与乘积（5）** | `SUM(X,N)`, `CUMSUM(X)`, `MULAR(X,N)`, `SUMBARS(X,A)`, `SUMBARSX(X,A)`（未达返回 `NaN`） |
| **移动平均（9）** | `MA(X,N)`, `EMA(X,N)`, `SMA(X,N,M)`, `WMA(X,N)`, `DMA(X,A)`, `AMA(X,A)`, `MEMA(X,N)`, `EXPMEMA(X,N)`, `TMA(X,A,B)` |
| **信号过滤（2）** | `FILTER(X,N)`（满足后向后置 0）, `FILTERX(X,N)`（满足后向前置 0） |
| **波动 / 转向（2）** | `TR(HIGH,LOW,CLOSE)`, `SAR(HIGH,LOW,CLOSE,N,STEP,MAXAF)` |

---

## 3. 统计函数 `StatisticsFunction`（18 项）

输入 `pd.Series`（支持滚动窗口 `N`）：

| 类别 | 函数与说明 |
|---|---|
| **分布特征（6）** | `MEAN(X,N)`, `MEDIAN(X,N)`, `QUANTILE(X,N,M)`, `AVEDEV(X,N)`, `SKEW(X,N)`, `KURTOSIS(X,N)` |
| **离散与方差（6）** | `STD(X,N)`（样本）, `STDP(X,N)`（总体）, `STDDEV(X,N)`, `VAR(X,N)`（样本）, `VARP(X,N)`（总体）, `DEVSQ(X,N)`（偏差平方和） |
| **相关性与回归（6）** | `COVAR(X,Y,N)`, `RELATE(X,Y,N)`, `BETA(X,BENCHMARK,N)`, `BETAEX(X,Y,N)`, `SLOPE(X,N)`, `FORCAST(X,N)` |

---

## 4. 截面函数 `CrossSectionFunction`（16 项）

输入要求为**二维 `pd.DataFrame`（`index=交易日期`, `columns=标的代码`）**：

| 子类 | 算子清单 |
|---|---|
| **截面描述统计（8）** | `CSCOUNT(X)`, `CSSUM(X)`, `CSMEAN(X)`, `CSMEDIAN(X)`, `CSMAX(X)`, `CSMIN(X)`, `CSSTD(X)`, `CSVAR(X)` |
| **截面排序与分位（3）** | `CSRANK(X,B)`, `CSPCTRANK(X)`, `CSQUANTILE(X,N)` |
| **截面标准化（3）** | `CSDEMEAN(X)`（去均值）, `CSZSCORE(X)`（Z-Score）, `CSNORMALIZE(X)`（归一化 `[0, 1]`） |
| **双指标截面关系（2）** | `CSCORR(X,Y)`（相关度）, `CSCOV(X,Y)`（协方差） |
