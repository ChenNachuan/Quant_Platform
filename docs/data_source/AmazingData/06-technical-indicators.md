# 06. 技术指标库（`TechnicalIndicators` · 57 项）

基于 AmazingData 底层算子库实现的 7 大类 **57 个** 常用技术指标。全部为 `@staticmethod`，输入 `pandas.Series`，统一返回 `dict[str, pd.Series]`。

```python
from AmazingData.operator.technical_indicators import TechnicalIndicators as TI

macd = TI.MACD(close, short=12, long=26, mid=9)  # 返回 {'DIF': ..., 'DEA': ..., 'MACD': ...}
```

> **前复权使用边界**：计算股票技术指标前须先做前复权（`backward_factor / latest_factor`）；**指数与 ETF/基金不做前复权**。

---

## 1. 超买超卖型（14 项）

| 指标 | 调用签名与默认参数 | 输出键名 | 常见判定 / 说明 |
|---|---|---|---|
| `KDJ` | `KDJ(close, high, low, n=9, m1=3, m2=3)` | `K`, `D`, `J` | K>80 超买，K<20 超卖 |
| `RSI` | `RSI(close, n1=6, n2=12, n3=24)` | `RSI6`, `RSI12`, `RSI24` | RSI>80 超买，RSI<20 超卖 |
| `WR` | `WR(close, high, low, n1=10, n2=6)` | `WR10`, `WR6` | 威廉指标（>80 超卖，<20 超买） |
| `CCI` | `CCI(close, high, low, n=14)` | `CCI` | >100 超买，<-100 超卖 |
| `ROC` | `ROC(close, n=12, m=6)` | `ROC`, `MAROC` | 变动率指标 |
| `MTM` | `MTM(close, n=12, m=6)` | `MTM`, `MAMTM` | 动量线 |
| `BIAS` | `BIAS(close, n1=6, n2=12, n3=24)` | `BIAS6`, `BIAS12`, `BIAS24` | 乖离率 |
| `SKDJ` | `SKDJ(close, high, low, n=9, m=3)` | `K`, `D` | 慢速随机指标 |
| `MFI` | `MFI(close, high, low, volume, n=14)` | `MFI` | 资金流量指标（>80 超买，<20 超卖） |
| `OSC` | `OSC(close, n=20, m=6)` | `OSC`, `MAOSC` | 变动速率线 |
| `UDL` | `UDL(close, n1=3, n2=5, n3=10, n4=20, m=6)` | `UDL`, `MAUDL` | 引力线 |
| `ACCER` | `ACCER(close, n=8)` | `ACCER` | 幅度涨速 |
| `RCCD` | `RCCD(close, n=59, short=26, long=52, m=26)` | `DIF`, `RCCD` | 异同离差变化率 |
| `MARSI` | `MARSI(close, n=6, m=12)` | `RSI`, `MARSI` | 相对强弱平均线 |

---

## 2. 趋势型（14 项）

| 指标 | 调用签名与默认参数 | 输出键名 | 说明 |
|---|---|---|---|
| `MACD` | `MACD(close, short=12, long=26, mid=9)` | `DIF`, `DEA`, `MACD` | 指数平滑异同移动平均 |
| `DMI` | `DMI(close, high, low, n=14, m=6)` | `PDI`, `MDI`, `ADX`, `ADXR` | 趋向指标 |
| `DMA` | `DMA(close, n1=10, n2=50, m=10)` | `DIF`, `AMA` | 平行线差 |
| `TRIX` | `TRIX(close, n=12, m=9)` | `TRIX`, `MATRIX` | 三重指数平滑 |
| `ARBR` | `ARBR(close, open_, high, low, n=26)` | `AR`, `BR` | 人气意愿指标 |
| `EMV` | `EMV(close, high, low, volume, n=14, m=9)` | `EMV`, `MAEMV` | 简易波动指标 |
| `DPO` | `DPO(close, n=20, m=6)` | `DPO`, `MADPO` | 区间震荡线 |
| `VHF` | `VHF(close, n=28)` | `VHF` | 十字过滤线（趋势强度） |
| `CHO` | `CHO(close, high, low, volume, n1=10, n2=20, m=6)` | `CHO`, `MACHO` | 佳庆指标 |
| `DBCD` | `DBCD(close, n=5, m=16, t=17)` | `DBCD`, `MM` | 异同离差乖离率 |
| `DDI` | `DDI(close, high, low, n=13, n1=26, m=1, m1=5)` | `DDI`, `ADDI`, `AD` | 方向标准离差指数 |
| `JS` | `JS(close, n=5, m1=5, m2=10, m3=20)` | `JS`, `MAJ5`, `MAJ10`, `MAJ20` | 加速线 |
| `QACD` | `QACD(close, n1=12, n2=26, m=9)` | `DIF`, `MACD`, `QACD` | 快速异同移动平均 |
| `UOS` | `UOS(close, high, low, n1=7, n2=14, n3=28, m=6)` | `UOS`, `MAUOS` | 终极波动指标 |

---

## 3. 能量型（5 项）与成交量型（10 项）

### 能量型（5 项）

| 指标 | 调用签名与默认参数 | 输出键名 | 说明 |
|---|---|---|---|
| `CR` | `CR(close, high, low, n=26)` | `CR` | 能量指标（>200 超买，<40 超卖） |
| `PSY` | `PSY(close, n=12, m=6)` | `PSY`, `MAPSY` | 心理线（>75 超买，<25 超卖） |
| `MASS` | `MASS(high, low, n1=9, n2=25, m=6)` | `MASS`, `MAMASS` | 梅斯线 |
| `PCNT` | `PCNT(close)` | `PCNT` | 幅度比 |
| `WAD` | `WAD(close, high, low)` | `WAD`, `MAWAD` | 威廉多空力度线 |

### 成交量型（10 项）

| 指标 | 调用签名与默认参数 | 输出键名 | 说明 |
|---|---|---|---|
| `OBV` | `OBV(close, volume)` | `OBV` | 能量潮 |
| `VR` | `VR(close, volume, n=26)` | `VR` | 成交量比率（>450 超买，<70 超卖） |
| `VOLMA` | `VOLMA(volume, n1=5, n2=10)` | `VOLMA5`, `VOLMA10` | 成交量均线 |
| `WVAD` | `WVAD(close, open_, high, low, volume, n=24, m=6)` | `WVAD`, `MAWVAD` | 威廉变异离散量 |
| `VOSC` | `VOSC(volume, short=12, long=26)` | `VOSC` | 成交量震荡 |
| `VRSI` | `VRSI(volume, n1=6, n2=12, n3=24)` | `VRSI6`, `VRSI12`, `VRSI24` | 量相对强弱 |
| `VSTD` | `VSTD(volume, n=10)` | `VSTD` | 成交量标准差 |
| `AMO` | `AMO(amount, n1=5, n2=10)` | `AMO5`, `AMO10` | 成交额均线 |
| `HSL` | `HSL(turnover_rate, n=5, m=10)` | `HSL5`, `HSL10` | 换手线（需外部传入 `turnover_rate`） |
| `TAPI` | `TAPI(close, amount, n=6)` | `TAPI`, `MATAPI` | 加权指数成交值 |

---

## 4. 均线型（4 项）、路径型（6 项）与其他型（4 项）

| 类别 | 指标 | 调用签名与默认参数 | 输出键名 |
|---|---|---|---|
| 均线型 | `MA` | `MA(close, n1=5, n2=10, n3=20, n4=60)` | `MA5`, `MA10`, `MA20`, `MA60` |
| 均线型 | `EXPMA` | `EXPMA(close, n1=12, n2=50)` | `EXPMA12`, `EXPMA50` |
| 均线型 | `BBI` | `BBI(close)` | `BBI` |
| 均线型 | `AMV` | `AMV(volume, amount, n1=5, n2=13, n3=34)` | `AMV5`, `AMV13`, `AMV34` |
| 路径型 | `BOLL` | `BOLL(close, n=20, k=2)` | `UPPER`, `MID`, `LOWER` |
| 路径型 | `ENE` | `ENE(close, n=25, m1=6, m2=6)` | `UPPER`, `ENE`, `LOWER` |
| 路径型 | `MIKE` | `MIKE(close, high, low, n=12)` | `WR`, `MR`, `SR`, `WS`, `MS`, `SS` |
| 路径型 | `PBX` | `PBX(close, n1=4, n2=6, n3=9, n4=13, n5=18, n6=24)` | `PBX4` … `PBX24` |
| 路径型 | `XS` | `XS(close, high, low, n=13)` | `UPP`, `SUP`, `SDN`, `LWN` |
| 路径型 | `BBIBOLL` | `BBIBOLL(close, n=11, k=6)` | `BBIBOLL`, `UPPER`, `LOWER` |
| 其他型 | `ASI` | `ASI(close, open_, high, low)` | `SI`, `ASI` |
| 其他型 | `ATR` | `ATR(close, high, low, n=14)` | `TR`, `ATR` |
| 其他型 | `SAR` | `SAR(close, high, low, n=4, step=0.02, max_af=0.2)` | `SAR` |
| 其他型 | `CDP` | `CDP(close, high, low)` | `AH`, `NH`, `CDP`, `NL`, `AL` |
