【模型篇】常见技术指标的计算方法


一、概述
本文档基于AmazingData算子函数实现的常用技术指标。所有指标方法均为静态方法，输入为pandas Series，输出为dict of Series。
数据要求：所有技术指标函数需要以下基础数据：
- close: 收盘价 Series
– high: 最高价 Series（部分指标需要）
- low: 最低价 Series（部分指标需要）
- open_: 开盘价 Series（部分指标需要）
- volume: 成交量 Series（部分指标需要）
- amount: 成交额 Series（部分指标需要）
返回格式：所有指标函数返回 dict of Series，可通过键名获取对应指标序列。
二、技术指标分类
本文档包含七大类技术指标，共计57个技术指标：
1.超买超卖型（14个）- 判断市场超买超卖状态
2.趋势型（14个）- 识别市场趋势方向和强度
3.能量型（5个）- 衡量市场多空能量对比
4.成交量型（10个）- 分析成交量变化和资金流向
5.均线型（4个）- 各类移动平均线指标
6.路径型（6个）- 构建价格通道和轨道
7.其他型（4个）- 其他常用技术指标
2.1 超买超卖型
超买超卖型指标用于衡量市场买卖力量的强弱，判断价格是否处于超买或超卖区域。
指标列表
序号

指标名称

中文名称

输出

1

KDJ

随机指标

K, D, J

2

RSI

相对强弱指标

RSI6, RSI12, RSI24

3

WR

威廉指标

WR10, WR6

4

CCI

顺势指标

CCI

5

ROC

变动率指标

ROC, MAROC

6

MTM

动量指标

MTM, MAMTM

7

BIAS

乖离率

BIAS6, BIAS12, BIAS24

8

SKDJ

慢速随机指标

K, D

9

MFI

资金流量指标

MFI

10

OSC

变动速率线

OSC, MAOSC

11

UDL

引力线

UDL, MAUDL

12

ACCER

幅度涨速

ACCER

13

RCCD

异同离差乖离率

DIF, RCCD

14

MARSI

相对强弱平均线

RSI, MARSI



指标说明
（1）KDJ(close, high, low, n=9, m1=3, m2=3) 随机指标
说明: 通过最高价、最低价及收盘价之间的关系来判断超买超卖，K>80超买，K<20超卖
公式: RSV=(CLOSE-LLV(LOW,N))/(HHV(HIGH,N)-LLV(LOW,N))*100; K=SMA(RSV,M1,1); D=SMA(K,M2,1); J=3*K-2*D
输出: K, D, J
（2）RSI(close, n1=6, n2=12, n3=24) 相对强弱指标
说明: 通过比较一段时期内的平均收盘涨幅和平均收盘跌幅来分析买卖力量，RSI>80超买，RSI<20超卖
公式: LC=REF(CLOSE,1); RSI=SMA(MAX(CLOSE-LC,0),N,1)/SMA(ABS(CLOSE-LC),N,1)*100
输出: RSI6, RSI12, RSI24
（3）WR(close, high, low, n1=10, n2=6) 威廉指标
说明: 利用最高价、最低价和收盘价来判断超买超卖，WR>80超卖，WR<20超买
公式: WR=(HHV(HIGH,N)-CLOSE)/(HHV(HIGH,N)-LLV(LOW,N))*100
输出: WR10, WR6
（4）CCI(close, high, low, n=14) 顺势指标
说明: 测量价格偏离统计平均值的程度，CCI>100超买，CCI<-100超卖
公式: TYP=(HIGH+LOW+CLOSE)/3; CCI=(TYP-MA(TYP,N))/(0.015*AVEDEV(TYP,N))
输出: CCI
（5）ROC(close, n=12, m=6) 变动率指标
说明: 当前价格与N日前价格的变化百分比，反映价格变动速度
公式: ROC=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)*100; MAROC=MA(ROC,M)
输出: ROC, MAROC
（6）MTM(close, n=12, m=6) 动量指标
说明: 当前价格与N日前价格的差值，反映价格变动的绝对动量
公式: MTM=CLOSE-REF(CLOSE,N); MAMTM=MA(MTM,M)
输出: MTM, MAMTM
（7）BIAS(close, n1=6, n2=12, n3=24) 乖离率
说明: 收盘价与移动平均线之间的偏离程度，正值表示价格高于均线，负值表示低于均线
公式: BIAS=(CLOSE-MA(CLOSE,N))/MA(CLOSE,N)*100
输出: BIAS6, BIAS12, BIAS24
（8）SKDJ(close, high, low, n=9, m=3) 慢速随机指标
说明: KDJ的慢速版本，通过双重平滑减少噪音，更适合中长线判断超买超卖
公式: RSV=EMA((CLOSE-LLV(LOW,N))/(HHV(HIGH,N)-LLV(LOW,N))*100,M); K=EMA(RSV,M); D=MA(K,M)
输出: K, D
（9）MFI(close, high, low, volume, n=14) 资金流量指标
说明: 结合价格和成交量的RSI变体，MFI>80超买，MFI<20超卖
公式: TYP=(HIGH+LOW+CLOSE)/3; MR=TYP*VOL; PMF=SUM(IF(TYP>REF(TYP,1),MR,0),N); NMF=SUM(IF(TYP
输出: MFI
（10）OSC(close, n=20, m=6) 变动速率线
说明: 当前价格与移动平均线的差值放大100倍，反映价格偏离均线的速率
公式: OSC=(CLOSE-MA(CLOSE,N))*100; MAOSC=EMA(OSC,M)
输出: OSC, MAOSC
（11）UDL(close, n1=3, n2=5, n3=10, n4=20, m=6) 引力线
说明: 将不同周期均线综合平均，反映价格的引力中心
公式: UDL=(MA(CLOSE,N1)+MA(CLOSE,N2)+MA(CLOSE,N3)+MA(CLOSE,N4))/4; MAUDL=MA(UDL,M)
输出: UDL, MAUDL
（12）ACCER(close, n=8) 幅度涨速
说明: 价格变动幅度除以周期数，衡量单位时间内的价格变动速率
公式: ACCER=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)/N*100
输出: ACCER
（13）RCCD(close, n=59, short=26, long=52, m=26) 异同离差乖离率
说明: 通过价格比率的短长期均线差值来判断趋势变化，类似MACD的变体
公式: RC=CLOSE/REF(CLOSE,N); ARC=SMA(REF(RC,1),N,1); DIF=MA(ARC,SHORT)-MA(ARC,LONG); RCCD=SMA(DIF,M,1)
输出: DIF, RCCD
（14）MARSI(close, n=6, m=12) 相对强弱平均线
说明: RSI的移动平均线，平滑RSI波动，更适合判断中期超买超卖
公式: RSI=SMA(MAX(CLOSE-LC,0),N,1)/SMA(ABS(CLOSE-LC),N,1)*100; MARSI=MA(RSI,M)
输出: RSI, MARSI
2.2 趋势型
趋势型指标用于判断市场的运行方向和趋势强度，帮助投资者识别多空趋势。
指标列表
序号

指标名称

中文名称

输出

1

MACD

指数平滑异同移动平均线

DIF, DEA, MACD

2

DMI

趋向指标

PDI, MDI, ADX, ADXR

3

DMA

平行线差指标

DIF, AMA

4

TRIX

三重指数平滑移动平均

TRIX, MATRIX

5

ARBR

人气意愿指标

AR, BR

6

EMV

简易波动指标

EMV, MAEMV

7

DPO

区间震荡线

DPO, MADPO

8

VHF

十字过滤线

VHF

9

CHO

佳庆指标

CHO, MACHO

10

DBCD

异同离差乖离率

DBCD, MM

11

DDI

方向标准离差指数

DDI, ADDI, AD

12

JS

加速线

JS, MAJ5, MAJ10, MAJ20

13

QACD

快速异同移动平均

DIF, MACD, QACD

14

UOS

终极波动指标

UOS, MAUOS



指标说明
（1）MACD(close, short=12, long=26, mid=9) 指数平滑异同移动平均线
说明: 由快慢均线的聚合与分离来判断买卖时机，DIF为快慢线差值，DEA为DIF的均线，MACD柱为两者差值的2倍
公式: DIF=EMA(CLOSE,SHORT)-EMA(CLOSE,LONG); DEA=EMA(DIF,MID); MACD=2*(DIF-DEA)
输出: DIF, DEA, MACD
（2）DMI(close, high, low, n=14, m=6) 趋向指标
说明: 通过分析多空双方力量的变化来判断趋势，PDI>MDI多头占优，PDI
公式: HD=HIGH-REF(HIGH,1); LD=REF(LOW,1)-LOW; DMP=SMA(IF(HD>0且HD>LD,HD,0),N,1); DMM=SMA(IF(LD>0且LD>HD,LD,0),N,1); PDI=DMP/TR*100; MDI=DMM/TR*100; ADX=SMA(ABS(MDI-PDI)/(MDI+PDI)*100,M,1); ADXR=(ADX+REF(ADX,M))/2
输出: PDI, MDI, ADX, ADXR
（3）DMA(close, n1=10, n2=50, m=10) 平行线差指标
说明: 短期均线与长期均线的差值，反映多空力量对比
公式: DIF=MA(CLOSE,N1)-MA(CLOSE,N2); AMA=MA(DIF,M)
输出: DIF, AMA
（4）TRIX(close, n=12, m=9) 三重指数平滑移动平均
说明: 对收盘价进行三次指数平滑后求变化率，过滤短期波动，反映中长期趋势
公式: TR=EMA(EMA(EMA(CLOSE,N),N),N); TRIX=(TR-REF(TR,1))/REF(TR,1)*100; MATRIX=MA(TRIX,M)
输出: TRIX, MATRIX
（5）ARBR(close, open_, high, low, n=26) 人气意愿指标
说明: AR反映开盘价在最高最低价之间的位置(人气指标)，BR反映收盘价在最高最低价之间的位置(意愿指标)
公式: AR=SUM(HIGH-OPEN,N)/SUM(OPEN-LOW,N)*100; BR=SUM(MAX(0,HIGH-REF(CLOSE,1)),N)/SUM(MAX(0,REF(CLOSE,1)-LOW),N)*100
输出: AR, BR
（6）EMV(close, high, low, volume, n=14, m=9) 简易波动指标
说明: 结合价格变动幅度和成交量来衡量价格波动的难易程度，EMV>0多头占优，EMV<0空头占优
公式: VOLUME=MA(VOL,N)/VOL; MID=100*(HIGH+LOW-REF(HIGH,1)-REF(LOW,1))/(HIGH+LOW); EMV=MA(MID*VOLUME*(HIGH-LOW)/MA(HIGH-LOW,N),N); MAEMV=MA(EMV,M)
输出: EMV, MAEMV
（7）DPO(close, n=20, m=6) 区间震荡线
说明: 去除趋势后的价格震荡，用于识别价格周期和超买超卖
公式: DPO=CLOSE-REF(MA(CLOSE,N),N/2+1); MADPO=MA(DPO,M)
输出: DPO, MADPO
（8）VHF(close, n=28) 十字过滤线
说明: 衡量市场是处于趋势状态还是震荡状态，VHF值越大趋势越明显
公式: HCP=HHV(CLOSE,N); LCP=LLV(CLOSE,N); VHF=(HCP-LCP)/SUM(ABS(CLOSE-REF(CLOSE,1)),N)
输出: VHF
（9）CHO(close, high, low, volume, n1=10, n2=20, m=6) 佳庆指标
说明: 基于累积/派发线(AD线)的短长期均线差值，反映资金流入流出的趋势变化
公式: MID=CUMSUM(VOL*(2*CLOSE-HIGH-LOW)/(HIGH-LOW)); CHO=MA(MID,N1)-MA(MID,N2); MACHO=MA(CHO,M)
输出: CHO, MACHO
（10）DBCD(close, n=5, m=16, t=17) 异同离差乖离率
说明: 乖离率的变化量经平滑处理后的指标，用于判断价格偏离均线的加速或减速
公式: BIAS=(CLOSE-MA(CLOSE,N))/MA(CLOSE,N); DIF=BIAS-REF(BIAS,M); DBCD=SMA(DIF,T,1); MM=MA(DBCD,5)
输出: DBCD, MM
（11）DDI(close, high, low, n=13, n1=26, m=1, m1=5) 方向标准离差指数
说明: 通过最高价和最低价的变化方向来判断多空力量，DDI>0多头占优，DDI<0空头占优
公式: TR=MAX(ABS(HIGH-REF(HIGH,1)),ABS(LOW-REF(LOW,1))); DMZ/DMF根据HIGH+LOW与前日比较; DDI=DIZ-DIF
输出: DDI, ADDI, AD
（12）JS(close, n=5, m1=5, m2=10, m3=20) 加速线
说明: 价格变动百分比及其多周期均线，反映价格加速上涨或下跌的程度
公式: JS=(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)*100; MAJ=MA(JS,M)
输出: JS, MAJ5, MAJ10, MAJ20
（13）QACD(close, n1=12, n2=26, m=9) 快速异同移动平均
说明: MACD的变体，QACD为DIF与其均线的差值，反映短期动量变化
公式: DIF=EMA(CLOSE,N1)-EMA(CLOSE,N2); MACD=EMA(DIF,M); QACD=DIF-MACD
输出: DIF, MACD, QACD
（14）UOS(close, high, low, n1=7, n2=14, n3=28, m=6) 终极波动指标
说明: 综合三个不同周期的买压比率，消除单一周期的偏差，UOS>50多头，UOS<50空头
公式: TH=MAX(HIGH,REF(C,1)); TL=MIN(LOW,REF(C,1)); UOS=(ACC1*N2*N3+ACC2*N1*N3+ACC3*N1*N2)/(N1*N2+N1*N3+N2*N3)*100
输出: UOS, MAUOS
3.3 能量型
能量型指标通过分析多空双方的力量对比，衡量市场参与者的情绪和意愿。
指标列表
序号

指标名称

中文名称

输出

1

CR

能量指标

CR

2

PSY

心理线

PSY, MAPSY

3

MASS

梅斯线

MASS, MAMASS

4

PCNT

幅度比

PCNT

5

WAD

威廉多空力度线

WAD, MAWAD



指标说明
（1）CR(close, high, low, n=26) 能量指标
说明: 以昨日中间价为基准，衡量多空双方的能量对比，CR>200超买，CR<40超卖
公式: MID=REF((HIGH+LOW+CLOSE)/3,1); CR=SUM(MAX(0,HIGH-MID),N)/SUM(MAX(0,MID-LOW),N)*100
输出: CR
（2）PSY(close, n=12, m=6) 心理线
说明: 统计N日内上涨天数的比例，反映投资者的心理预期，PSY>75超买，PSY<25超卖
公式: PSY=COUNT(CLOSE>REF(CLOSE,1),N)/N*100; MAPSY=MA(PSY,M)
输出: PSY, MAPSY
（3）MASS(high, low, n1=9, n2=25, m=6) 梅斯线
说明: 通过最高价与最低价波幅的指数平滑比值累计，判断趋势反转信号
公式: MASS=SUM(EMA(HIGH-LOW,N1)/EMA(EMA(HIGH-LOW,N1),N1),N2); MAMASS=MA(MASS,M)
输出: MASS, MAMASS
（4）PCNT(close) 幅度比
说明: 当日收盘价相对于前一日收盘价的涨跌幅百分比
公式: PCNT=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*100
输出: PCNT
（5）WAD(close, high, low) 威廉多空力度线
说明: 通过累计多空力度来判断趋势，WAD上升表示多方力量增强，下降表示空方力量增强
公式: 当CLOSE>REF(CLOSE,1): MIDA=CLOSE-MIN(LOW,REF(CLOSE,1)); 当CLOSE
输出: WAD, MAWAD
3.4 成交量型
成交量型指标结合成交量与价格变化，分析资金流向和买卖力量。
指标列表
序号

指标名称

中文名称

输出

1

OBV

能量潮

OBV

2

VR

成交量变异率

VR

3

VOLMA

成交量均线

VOLMA5, VOLMA10

4

WVAD

威廉变异离散量

WVAD, MAWVAD

5

VOSC

成交量震荡

VOSC

6

VRSI

量相对强弱

VRSI6, VRSI12, VRSI24

7

VSTD

成交量标准差

VSTD

8

AMO

成交额均线

AMO5, AMO10

9

HSL

换手线

HSL5, HSL10

10

TAPI

加权指数成交值

TAPI, MATAPI



指标说明
（1）OBV(close, volume) 能量潮
说明: 通过累计成交量的正负来衡量买卖压力，OBV上升表示买方力量增强
公式: 若收盘价>昨收: OBV=前日OBV+今日成交量; 若收盘价<昨收: OBV=前日OBV-今日成交量
输出: OBV
（2）VR(close, volume, n=26) 成交量变异率
说明: 通过上涨日与下跌日的成交量比值来判断市场人气，VR>450超买，VR<70超卖
公式: AV=SUM(IF(C>REF(C,1),VOL,0),N); BV=SUM(IF(C
输出: VR
（3）VOLMA(volume, n1=5, n2=10) 成交量均线
说明: 成交量的简单移动平均线，用于判断成交量的趋势变化
公式: VOLMA=MA(VOLUME,N)
输出: VOLMA5, VOLMA10
（4）WVAD(close, open_, high, low, volume, n=24, m=6) 威廉变异离散量
说明: 结合价格涨跌幅与成交量，衡量多空双方的实际力量对比
公式: WVAD=SUM((CLOSE-OPEN)/(HIGH-LOW)*VOL,N); MAWVAD=MA(WVAD,M)
输出: WVAD, MAWVAD
（5）VOSC(volume, short=12, long=26) 成交量震荡
说明: 短期成交量均线与长期成交量均线的偏离百分比，反映成交量的变化趋势
公式: VOSC=(MA(VOL,SHORT)-MA(VOL,LONG))/MA(VOL,SHORT)*100
输出: VOSC
（6）VRSI(volume, n1=6, n2=12, n3=24) 量相对强弱
说明: 将RSI的计算方法应用于成交量，衡量成交量的相对强弱
公式: LV=REF(VOL,1); VRSI=SMA(MAX(VOL-LV,0),N,1)/SMA(ABS(VOL-LV),N,1)*100
输出: VRSI6, VRSI12, VRSI24
（7）VSTD(volume, n=10) 成交量标准差
说明: 成交量在N日内的标准差，衡量成交量的波动程度
公式: VSTD=STD(VOL,N)
输出: VSTD
（8）AMO(amount, n1=5, n2=10) 成交额均线
说明: 成交额的简单移动平均线，用于判断资金流入流出的趋势
公式: AMO=MA(AMOUNT,N)
输出: AMO5, AMO10
（9）HSL(turnover_rate, n=5, m=10) 换手线
说明: 换手率的移动平均线，反映市场交投活跃程度的趋势变化
公式: HSL=MA(TURNOVER_RATE,N)
输出: HSL5, HSL10
（10）TAPI(close, amount, n=6) 加权指数成交值
说明: 成交额与收盘价的比值，衡量每单位价格对应的成交金额
公式: TAPI=AMOUNT/CLOSE; MATAPI=MA(TAPI,N)
输出: TAPI, MATAPI
3.5 均线型
均线型指标通过对价格进行不同方式的平均处理，反映价格的趋势方向和支撑压力。
指标列表
序号

指标名称

中文名称

输出

1

MA

移动平均线

MA5, MA10, MA20, MA60

2

EXPMA

指数平均线

EXPMA12, EXPMA50

3

BBI

多空指标

BBI

4

AMV

成本价均线

AMV5, AMV13, AMV34



指标说明
（1）MA(close, n1=5, n2=10, n3=20, n4=60) 移动平均线
说明: N日简单移动平均线，算法: (X1+X2+...+Xn)/N
公式: MA(CLOSE, N)
输出: MA5, MA10, MA20, MA60
（2）EXPMA(close, n1=12, n2=50) 指数平均线
说明: N日指数移动平均线，对近期数据赋予更大权重
公式: EMA(CLOSE, N)
输出: EXPMA12, EXPMA50
（3）BBI(close) 多空指标
说明: 将不同周期的移动平均线加权平均，综合反映多空力量
公式: BBI=(MA(CLOSE,3)+MA(CLOSE,6)+MA(CLOSE,12)+MA(CLOSE,24))/4
输出: BBI
（4）AMV(volume, amount, n1=5, n2=13, n3=34) 成本价均线
说明: 以成交均价(成交额/成交量)的移动平均线，反映市场平均持仓成本
公式: AVG_PRICE=AMOUNT/VOL; AMV=MA(AVG_PRICE,N)
输出: AMV5, AMV13, AMV34
3.6路径型
路径型指标通过构建价格运行的上下轨道，帮助判断价格的支撑与压力位。
指标列表
序号

指标名称

中文名称

输出

1

BOLL

布林线

UPPER, MID, LOWER

2

ENE

轨道线

UPPER, ENE, LOWER

3

MIKE

麦克指标

WR, MR, SR, WS, MS, SS

4

PBX

瀑布线

PBX4, PBX6, PBX9, PBX13, PBX18, PBX24

5

XS

薛斯通道

UPP, SUP, SDN, LWN

6

BBIBOLL

BBI多空布林线

BBIBOLL, UPPER, LOWER



指标说明
（1）BOLL(close, n=20, k=2) 布林线
说明: 以移动平均线为中轨，上下各加减K倍标准差构成通道，价格触及上轨为超买，触及下轨为超卖
公式: MID=MA(CLOSE,N); UPPER=MID+K*STD(CLOSE,N); LOWER=MID-K*STD(CLOSE,N)
输出: UPPER, MID, LOWER
（2）ENE(close, n=25, m1=6, m2=6) 轨道线
说明: 以移动平均线为基准，按固定百分比上下偏移构成轨道
公式: UPPER=MA(CLOSE,N)*(1+M1/100); LOWER=MA(CLOSE,N)*(1-M2/100); ENE=(UPPER+LOWER)/2
输出: UPPER, ENE, LOWER
（3）MIKE(close, high, low, n=12) 麦克指标
说明: 利用典型价格与最高最低价构建三条压力线(WR/MR/SR)和三条支撑线(WS/MS/SS)
公式: TYP=(HIGH+LOW+CLOSE)/3; LL=LLV(LOW,N); HH=HHV(HIGH,N); WR=TYP+(TYP-LL); MR=TYP+(HH-LL); SR=2*HH-LL; WS=TYP-(HH-TYP); MS=TYP-(HH-LL); SS=2*LL-HH
输出: WR, MR, SR, WS, MS, SS
（4）PBX(close, n1=4, n2=6, n3=9, n4=13, n5=18, n6=24) 瀑布线
说明: 多条不同周期的三重EMA均线，形似瀑布，用于判断趋势方向和支撑压力
公式: PBX=(EMA(CLOSE,N)+EMA(CLOSE,2*N)+EMA(CLOSE,4*N))/3
输出: PBX4, PBX6, PBX9, PBX13, PBX18, PBX24
（5）XS(close, high, low, n=13) 薛斯通道
说明: 基于最高价、收盘价、最低价的SMA构建四条通道线，形成内外两个通道
公式: SMA_C=SMA(CLOSE,N,1); SMA_H=SMA(HIGH,N,1); SMA_L=SMA(LOW,N,1); UPP=SMA_H*1.06; SUP=SMA_C*1.06; SDN=SMA_C*0.94; LWN=SMA_L*0.94
输出: UPP, SUP, SDN, LWN
（6）BBIBOLL(close, n=11, k=6) BBI多空布林线
说明: 以BBI多空指标为中轨，加减K倍标准差构成布林通道，结合多空判断与通道分析
公式: BBI=(MA(C,3)+MA(C,6)+MA(C,12)+MA(C,24))/4; UPPER=BBI+K*STD(BBI,N); LOWER=BBI-K*STD(BBI,N)
输出: BBIBOLL, UPPER, LOWER
3.7 其他型
其他常用技术指标，包括振动升降、真实波幅、抛物线转向、逆势操作等。
指标列表
序号

指标名称

中文名称

输出

1

ASI

振动升降指标

SI, ASI

2

ATR

真实波幅均值

TR, ATR

3

SAR

抛物线转向指标

SAR

4

CDP

逆势操作

AH, NH, CDP, NL, AL



指标说明
（1）ASI(close, open_, high, low) 振动升降指标
说明: 以开盘、最高、最低、收盘价与前一日价格比较，计算出真实的价格变动量并累计
公式: SI=16*(CLOSE-REF(CLOSE,1)+(CLOSE-OPEN)/2+(REF(CLOSE,1)-REF(OPEN,1))/4)/R*MAX(A,B); ASI=CUMSUM(SI)
输出: SI, ASI
（2）ATR(close, high, low, n=14) 真实波幅均值
说明: 真实波幅的N日移动平均，衡量市场波动性大小
公式: ATR=MA(TR,N)
输出: TR, ATR
（3）SAR(close, high, low, n=4, step=0.02, max_af=0.2) 抛物线转向指标
说明: 随时间推移不断调整止损点位，当价格跌破SAR时为卖出信号，突破SAR时为买入信号
公式: 初始方向根据前N日趋势判定，加速因子从step开始，每创新高/低增加step，最大max_af
输出: SAR
（4）CDP(close, high, low) 逆势操作
说明: 根据前一日价格计算今日的最高值(AH)、近高值(NH)、均价(CDP)、近低值(NL)、最低值(AL)五个价位
公式: CDP=(REF(HIGH,1)+REF(LOW,1)+2*REF(CLOSE,1))/4; AH=CDP+(REF(HIGH,1)-REF(LOW,1)); NH=2*CDP-REF(LOW,1); NL=2*CDP-REF(HIGH,1); AL=CDP-(REF(HIGH,1)-REF(LOW,1))
输出: AH, NH, CDP, NL, AL
三、模型源码
# -*- coding: utf-8 -*-

# ------------------------------
# @Time    : 2026/2/24
# @Author  : gao
# @File    : technical_indicators.py
# @Project : AmazingData
# ------------------------------
import AmazingData as ad
import pandas as pd
import numpy as np

from AmazingData.operator.math_function import MathFunction
from AmazingData.operator.statistics_function import StatisticsFunction
from AmazingData.operator.time_series_function import TimeSeriesFunction


class TechnicalIndicators:
    """常用技术指标

    所有方法均为静态方法，输入为 pandas Series (OHLCV)，输出为 dict of Series。
    分类: 超买超卖型 / 趋势型 / 能量型 / 成交量型 / 均线型 / 路径型 / 其他型
    """

    # ================================================================
    #  一、超买超卖型
    # ================================================================

    @staticmethod
    def KDJ(close, high, low, n=9, m1=3, m2=3):
        """KDJ 随机指标
        RSV = (CLOSE - LLV(LOW, N)) / (HHV(HIGH, N) - LLV(LOW, N)) * 100
        K = SMA(RSV, M1, 1)
        D = SMA(K, M2, 1)
        J = 3 * K - 2 * D
        """
        llv = TimeSeriesFunction.LLV(low, n)
        hhv = TimeSeriesFunction.HHV(high, n)
        denom = hhv - llv
        denom = denom.replace(0, float('nan'))  # 防止除零
        rsv = (close - llv) / denom * 100
        k = TimeSeriesFunction.SMA(rsv, m1, 1)
        d = TimeSeriesFunction.SMA(k, m2, 1)
        j = 3 * k - 2 * d
        return {'K': k, 'D': d, 'J': j}

    @staticmethod
    def RSI(close, n1=6, n2=12, n3=24):
        """RSI 相对强弱指标
        LC = REF(CLOSE, 1)
        RSI = SMA(MAX(CLOSE-LC, 0), N, 1) / SMA(ABS(CLOSE-LC), N, 1) * 100
        """
        lc = TimeSeriesFunction.REF(close, 1)
        diff = close - lc
        zero = pd.Series(0.0, index=close.index)
        pos_diff = MathFunction.MAX(diff, zero)
        abs_diff = MathFunction.ABS(diff)
        return {
            f'RSI{n1}': TimeSeriesFunction.SMA(pos_diff, n1, 1) / TimeSeriesFunction.SMA(abs_diff, n1, 1) * 100,
            f'RSI{n2}': TimeSeriesFunction.SMA(pos_diff, n2, 1) / TimeSeriesFunction.SMA(abs_diff, n2, 1) * 100,
            f'RSI{n3}': TimeSeriesFunction.SMA(pos_diff, n3, 1) / TimeSeriesFunction.SMA(abs_diff, n3, 1) * 100,
        }

    @staticmethod
    def WR(close, high, low, n1=10, n2=6):
        """WR 威廉指标
        WR = (HHV(HIGH, N) - CLOSE) / (HHV(HIGH, N) - LLV(LOW, N)) * 100
        """
        result = {}
        for n in [n1, n2]:
            hhv = TimeSeriesFunction.HHV(high, n)
            llv = TimeSeriesFunction.LLV(low, n)
            denom = hhv - llv
            denom = denom.replace(0, float('nan'))  # 防止除零
            result[f'WR{n}'] = (hhv - close) / denom * 100
        return result

    @staticmethod
    def CCI(close, high, low, n=14):
        """CCI 顺势指标
        TYP = (HIGH + LOW + CLOSE) / 3
        CCI = (TYP - MA(TYP, N)) / (0.015 * AVEDEV(TYP, N))
        """
        typ = (high + low + close) / 3
        cci = (typ - TimeSeriesFunction.MA(typ, n)) / (0.015 * StatisticsFunction.AVEDEV(typ, n))
        return {'CCI': cci}

    @staticmethod
    def ROC(close, n=12, m=6):
        """ROC 变动率指标
        ROC = (CLOSE - REF(CLOSE, N)) / REF(CLOSE, N) * 100
        MAROC = MA(ROC, M)
        """
        ref_close = TimeSeriesFunction.REF(close, n)
        roc = (close - ref_close) / ref_close * 100
        maroc = TimeSeriesFunction.MA(roc, m)
        return {'ROC': roc, 'MAROC': maroc}

    @staticmethod
    def MTM(close, n=12, m=6):
        """MTM 动量指标
        MTM = CLOSE - REF(CLOSE, N)
        MAMTM = MA(MTM, M)
        """
        mtm = close - TimeSeriesFunction.REF(close, n)
        mamtm = TimeSeriesFunction.MA(mtm, m)
        return {'MTM': mtm, 'MAMTM': mamtm}

    @staticmethod
    def BIAS(close, n1=6, n2=12, n3=24):
        """BIAS 乖离率
        BIAS = (CLOSE - MA(CLOSE, N)) / MA(CLOSE, N) * 100
        """
        result = {}
        for n in [n1, n2, n3]:
            ma = TimeSeriesFunction.MA(close, n)
            result[f'BIAS{n}'] = (close - ma) / ma * 100
        return result

    @staticmethod
    def SKDJ(close, high, low, n=9, m=3):
        """SKDJ 慢速随机指标
        LOWV = LLV(LOW, N)
        HIGHV = HHV(HIGH, N)
        RSV = SMA((CLOSE-LOWV)/(HIGHV-LOWV)*100, M, 1)
        K = SMA(RSV, M, 1)
        D = MA(K, M)
        """
        lowv = TimeSeriesFunction.LLV(low, n)
        highv = TimeSeriesFunction.HHV(high, n)
        denom = highv - lowv
        denom = denom.replace(0, float('nan'))  # 防止除零
        rsv = TimeSeriesFunction.SMA((close - lowv) / denom * 100, m, 1)
        k = TimeSeriesFunction.SMA(rsv, m, 1)
        d = TimeSeriesFunction.MA(k, m)
        return {'K': k, 'D': d}

    @staticmethod
    def MFI(close, high, low, volume, n=14):
        """MFI 资金流量指标
        TYP = (HIGH + LOW + CLOSE) / 3
        MR = TYP * VOL
        PMF = SUM(IF(TYP>REF(TYP,1), MR, 0), N)
        NMF = SUM(IF(TYP<REF(TYP,1), MR, 0), N)
        MFI = 100 - 100 / (1 + PMF / NMF)
        """
        typ = (high + low + close) / 3
        mr = typ * volume
        ref_typ = TimeSeriesFunction.REF(typ, 1)
        zero = pd.Series(0.0, index=close.index)
        pmf = TimeSeriesFunction.SUM(MathFunction.IF(typ > ref_typ, mr, zero), n)
        nmf = TimeSeriesFunction.SUM(MathFunction.IF(typ < ref_typ, mr, zero), n)
        # 处理除零和全流入/全流出情况
        mfi = pd.Series(50.0, index=close.index)  # 默认值
        mfi.loc[nmf > 0] = 100 - 100 / (1 + pmf.loc[nmf > 0] / nmf.loc[nmf > 0])
        mfi.loc[pmf > 0] = 100  # 全资金流入
        mfi.loc[(pmf == 0) & (nmf == 0)] = 50  # 无资金流动
        return {'MFI': mfi}

    @staticmethod
    def OSC(close, n=20, m=6):
        """OSC 变动速率线
        OSC = (CLOSE - MA(CLOSE, N)) * 100
        MAOSC = EMA(OSC, M)
        """
        osc = (close - TimeSeriesFunction.MA(close, n)) * 100
        maosc = TimeSeriesFunction.EMA(osc, m)
        return {'OSC': osc, 'MAOSC': maosc}

    @staticmethod
    def UDL(close, n1=3, n2=5, n3=10, n4=20, m=6):
        """UDL 引力线
        UDL = (MA(CLOSE,N1)+MA(CLOSE,N2)+MA(CLOSE,N3)+MA(CLOSE,N4)) / 4
        MAUDL = MA(UDL, M)
        """
        udl = (TimeSeriesFunction.MA(close, n1) + TimeSeriesFunction.MA(close, n2) +
               TimeSeriesFunction.MA(close, n3) + TimeSeriesFunction.MA(close, n4)) / 4
        maudl = TimeSeriesFunction.MA(udl, m)
        return {'UDL': udl, 'MAUDL': maudl}

    @staticmethod
    def ACCER(close, n=8):
        """ACCER 幅度涨速
        ACCER = (CLOSE - REF(CLOSE, N)) / REF(CLOSE, N) / N * 100
        """
        ref_close = TimeSeriesFunction.REF(close, n)
        accer = (close - ref_close) / ref_close / n * 100
        return {'ACCER': accer}

    @staticmethod
    def RCCD(close, n=59, short=26, long=52, m=26):
        """RCCD 异同离差乖离率
        RC = CLOSE / REF(CLOSE, N)
        ARC = SMA(REF(RC, 1), N, 1)
        DIF = MA(ARC, SHORT) - MA(ARC, LONG)
        RCCD = SMA(DIF, M, 1)
        """
        rc = close / TimeSeriesFunction.REF(close, n)
        arc = TimeSeriesFunction.SMA(TimeSeriesFunction.REF(rc, 1), n, 1)
        dif = TimeSeriesFunction.MA(arc, short) - TimeSeriesFunction.MA(arc, long)
        rccd = TimeSeriesFunction.SMA(dif, m, 1)
        return {'DIF': dif, 'RCCD': rccd}

    @staticmethod
    def MARSI(close, n=6, m=12):
        """MARSI 相对强弱平均线
        RSI = SMA(MAX(CLOSE-LC,0),N,1) / SMA(ABS(CLOSE-LC),N,1) * 100
        MARSI = MA(RSI, M)
        """
        lc = TimeSeriesFunction.REF(close, 1)
        diff = close - lc
        zero = pd.Series(0.0, index=close.index)
        pos_diff = MathFunction.MAX(diff, zero)
        abs_diff = MathFunction.ABS(diff)
        rsi = TimeSeriesFunction.SMA(pos_diff, n, 1) / TimeSeriesFunction.SMA(abs_diff, n, 1) * 100
        marsi = TimeSeriesFunction.MA(rsi, m)
        return {'RSI': rsi, 'MARSI': marsi}

    # ================================================================
    #  二、趋势型
    # ================================================================

    @staticmethod
    def MACD(close, short=12, long=26, mid=9):
        """MACD 指数平滑异同移动平均线
        DIF = EMA(CLOSE, SHORT) - EMA(CLOSE, LONG)
        DEA = EMA(DIF, MID)
        MACD = 2 * (DIF - DEA)
        """
        dif = TimeSeriesFunction.EMA(close, short) - TimeSeriesFunction.EMA(close, long)
        dea = TimeSeriesFunction.EMA(dif, mid)
        macd = 2 * (dif - dea)
        return {'DIF': dif, 'DEA': dea, 'MACD': macd}

    @staticmethod
    def DMI(close, high, low, n=14, m=6):
        """DMI 趋向指标
        HD = HIGH - REF(HIGH, 1)
        LD = REF(LOW, 1) - LOW
        DMP = SMA(IF(HD>0 AND HD>LD, HD, 0), N, 1)
        DMM = SMA(IF(LD>0 AND LD>HD, LD, 0), N, 1)
        PDI = DMP / TR * 100
        MDI = DMM / TR * 100
        ADX = SMA(ABS(MDI-PDI)/(MDI+PDI)*100, M, 1)
        ADXR = (ADX + REF(ADX, M)) / 2
        """
        hd = high - TimeSeriesFunction.REF(high, 1)
        ld = TimeSeriesFunction.REF(low, 1) - low
        zero = pd.Series(0.0, index=close.index)

        dmp_raw = MathFunction.IF((hd > 0) & (hd > ld), hd, zero)
        dmm_raw = MathFunction.IF((ld > 0) & (ld > hd), ld, zero)

        dmp = TimeSeriesFunction.SMA(dmp_raw, n, 1)
        dmm = TimeSeriesFunction.SMA(dmm_raw, n, 1)

        tr = TimeSeriesFunction.SMA(TimeSeriesFunction.TR(high, low, close), n, 1)
        pdi = dmp / tr * 100
        mdi = dmm / tr * 100

        dx = MathFunction.ABS(mdi - pdi) / (mdi + pdi) * 100
        adx = TimeSeriesFunction.SMA(dx, m, 1)
        adxr = (adx + TimeSeriesFunction.REF(adx, m)) / 2
        return {'PDI': pdi, 'MDI': mdi, 'ADX': adx, 'ADXR': adxr}

    @staticmethod
    def DMA(close, n1=10, n2=50, m=10):
        """DMA 平行线差指标
        DIF = MA(CLOSE, N1) - MA(CLOSE, N2)
        AMA = MA(DIF, M)
        """
        dif = TimeSeriesFunction.MA(close, n1) - TimeSeriesFunction.MA(close, n2)
        ama = TimeSeriesFunction.MA(dif, m)
        return {'DIF': dif, 'AMA': ama}

    @staticmethod
    def TRIX(close, n=12, m=9):
        """TRIX 三重指数平滑移动平均
        TR = EMA(EMA(EMA(CLOSE, N), N), N)
        TRIX = (TR - REF(TR, 1)) / REF(TR, 1) * 100
        MATRIX = MA(TRIX, M)
        """
        tr = TimeSeriesFunction.EMA(TimeSeriesFunction.EMA(TimeSeriesFunction.EMA(close, n), n), n)
        ref_tr = TimeSeriesFunction.REF(tr, 1)
        trix = (tr - ref_tr) / ref_tr * 100
        matrix = TimeSeriesFunction.MA(trix, m)
        return {'TRIX': trix, 'MATRIX': matrix}

    @staticmethod
    def ARBR(close, open_, high, low, n=26):
        """ARBR 人气意愿指标 (BRAR)
        AR = SUM(HIGH - OPEN, N) / SUM(OPEN - LOW, N) * 100
        BR = SUM(MAX(0, HIGH-REF(CLOSE,1)), N) / SUM(MAX(0, REF(CLOSE,1)-LOW), N) * 100
        """
        ar = (TimeSeriesFunction.SUM(high - open_, n) /
              TimeSeriesFunction.SUM(open_ - low, n) * 100)
        ref_close = TimeSeriesFunction.REF(close, 1)
        zero = close * 0
        br = (TimeSeriesFunction.SUM(MathFunction.MAX(high - ref_close, zero), n) /
              TimeSeriesFunction.SUM(MathFunction.MAX(ref_close - low, zero), n) * 100)
        return {'AR': ar, 'BR': br}

    @staticmethod
    def EMV(close, high, low, volume, n=14, m=9):
        """EMV 简易波动指标
        VOLUME = MA(VOL, N) / VOL
        MID = 100 * (HIGH+LOW-REF(HIGH,1)-REF(LOW,1)) / (HIGH+LOW)
        EMV = MA(MID*VOLUME*(HIGH-LOW)/MA(HIGH-LOW,N), N)
        MAEMV = MA(EMV, M)
        """
        vol_ratio = TimeSeriesFunction.MA(volume, n) / volume
        mid = 100 * (high + low - TimeSeriesFunction.REF(high, 1) - TimeSeriesFunction.REF(low, 1)) / (high + low)
        hl = high - low
        emv = TimeSeriesFunction.MA(mid * vol_ratio * hl / TimeSeriesFunction.MA(hl, n), n)
        maemv = TimeSeriesFunction.MA(emv, m)
        return {'EMV': emv, 'MAEMV': maemv}

    @staticmethod
    def DPO(close, n=20, m=6):
        """DPO 区间震荡线
        DPO = CLOSE - REF(MA(CLOSE, N), N/2+1)
        MADPO = MA(DPO, M)
        """
        ma_close = TimeSeriesFunction.MA(close, n)
        dpo = close - TimeSeriesFunction.REF(ma_close, n // 2 + 1)
        madpo = TimeSeriesFunction.MA(dpo, m)
        return {'DPO': dpo, 'MADPO': madpo}

    @staticmethod
    def VHF(close, n=28):
        """VHF 十字过滤线
        HCP = HHV(CLOSE, N)
        LCP = LLV(CLOSE, N)
        VHF = (HCP - LCP) / SUM(ABS(CLOSE - REF(CLOSE, 1)), N)
        """
        hcp = TimeSeriesFunction.HHV(close, n)
        lcp = TimeSeriesFunction.LLV(close, n)
        denom = TimeSeriesFunction.SUM(MathFunction.ABS(close - TimeSeriesFunction.REF(close, 1)), n)
        denom = denom.replace(0, float('nan'))  # 防止除零
        vhf = (hcp - lcp) / denom
        return {'VHF': vhf}

    @staticmethod
    def CHO(close, high, low, volume, n1=10, n2=20, m=6):
        """CHO 佳庆指标
        MID = SUM(VOL*(2*CLOSE-HIGH-LOW)/(HIGH-LOW), 0)
        CHO = MA(MID, N1) - MA(MID, N2)
        MACHO = MA(CHO, M)
        """
        mid = TimeSeriesFunction.CUMSUM(volume * (2 * close - high - low) / (high - low))
        cho = TimeSeriesFunction.MA(mid, n1) - TimeSeriesFunction.MA(mid, n2)
        macho = TimeSeriesFunction.MA(cho, m)
        return {'CHO': cho, 'MACHO': macho}

    @staticmethod
    def DBCD(close, n=5, m=16, t=17):
        """DBCD 异同离差乖离率
        BIAS = (CLOSE - MA(CLOSE, N)) / MA(CLOSE, N)
        DIF = BIAS - REF(BIAS, M)
        DBCD = SMA(DIF, T, 1)
        MM = MA(DBCD, 5)
        """
        ma = TimeSeriesFunction.MA(close, n)
        bias = (close - ma) / ma
        dif = bias - TimeSeriesFunction.REF(bias, m)
        dbcd = TimeSeriesFunction.SMA(dif, t, 1)
        mm = TimeSeriesFunction.MA(dbcd, 5)
        return {'DBCD': dbcd, 'MM': mm}

    @staticmethod
    def DDI(close, high, low, n=13, n1=26, m=1, m1=5):
        """DDI 方向标准离差指数
        TR = MAX(ABS(HIGH-REF(HIGH,1)), ABS(LOW-REF(LOW,1)))
        DMZ = IF(HIGH+LOW>REF(HIGH,1)+REF(LOW,1), TR, 0)
        DMF = IF(HIGH+LOW<REF(HIGH,1)+REF(LOW,1), TR, 0)
        DIZ = SUM(DMZ,N) / (SUM(DMZ,N)+SUM(DMF,N))
        DIF = SUM(DMF,N) / (SUM(DMF,N)+SUM(DMZ,N))
        DDI = DIZ - DIF
        """
        ref_h = TimeSeriesFunction.REF(high, 1)
        ref_l = TimeSeriesFunction.REF(low, 1)
        tr = MathFunction.MAX(MathFunction.ABS(high - ref_h), MathFunction.ABS(low - ref_l))
        zero = pd.Series(0.0, index=close.index)
        dmz = MathFunction.IF(high + low > ref_h + ref_l, tr, zero)
        dmf = MathFunction.IF(high + low < ref_h + ref_l, tr, zero)
        sum_dmz = TimeSeriesFunction.SUM(dmz, n)
        sum_dmf = TimeSeriesFunction.SUM(dmf, n)
        diz = sum_dmz / (sum_dmz + sum_dmf)
        dif = sum_dmf / (sum_dmf + sum_dmz)
        ddi = diz - dif
        addi = TimeSeriesFunction.SMA(ddi, n1, m)
        ad_line = TimeSeriesFunction.MA(addi, m1)
        return {'DDI': ddi, 'ADDI': addi, 'AD': ad_line}

    @staticmethod
    def JS(close, n=5, m1=5, m2=10, m3=20):
        """JS 加速线
        JS = (CLOSE - REF(CLOSE, N)) / REF(CLOSE, N) * 100
        MAJ1 = MA(JS, M1); MAJ2 = MA(JS, M2); MAJ3 = MA(JS, M3)
        """
        ref_close = TimeSeriesFunction.REF(close, n)
        js = (close - ref_close) / ref_close * 100
        return {
            'JS': js,
            f'MAJ{m1}': TimeSeriesFunction.MA(js, m1),
            f'MAJ{m2}': TimeSeriesFunction.MA(js, m2),
            f'MAJ{m3}': TimeSeriesFunction.MA(js, m3),
        }

    @staticmethod
    def QACD(close, n1=12, n2=26, m=9):
        """QACD 快速异同移动平均
        DIF = EMA(CLOSE, N1) - EMA(CLOSE, N2)
        MACD = EMA(DIF, M)
        QACD = DIF - MACD
        """
        dif = TimeSeriesFunction.EMA(close, n1) - TimeSeriesFunction.EMA(close, n2)
        macd = TimeSeriesFunction.EMA(dif, m)
        qacd = dif - macd
        return {'DIF': dif, 'MACD': macd, 'QACD': qacd}

    @staticmethod
    def UOS(close, high, low, n1=7, n2=14, n3=28, m=6):
        """UOS 终极波动指标
        TH = MAX(HIGH, REF(CLOSE,1)); TL = MIN(LOW, REF(CLOSE,1))
        ACC1 = SUM(CLOSE-TL,N1)/SUM(TH-TL,N1)
        ACC2 = SUM(CLOSE-TL,N2)/SUM(TH-TL,N2)
        ACC3 = SUM(CLOSE-TL,N3)/SUM(TH-TL,N3)
        UOS = (ACC1*N2*N3+ACC2*N1*N3+ACC3*N1*N2)/(N1*N2+N1*N3+N2*N3)*100
        MAUOS = MA(UOS, M)
        """
        ref_c = TimeSeriesFunction.REF(close, 1)
        th = MathFunction.MAX(high, ref_c)
        tl = MathFunction.MIN(low, ref_c)
        acc1 = TimeSeriesFunction.SUM(close - tl, n1) / TimeSeriesFunction.SUM(th - tl, n1)
        acc2 = TimeSeriesFunction.SUM(close - tl, n2) / TimeSeriesFunction.SUM(th - tl, n2)
        acc3 = TimeSeriesFunction.SUM(close - tl, n3) / TimeSeriesFunction.SUM(th - tl, n3)
        uos = (acc1 * n2 * n3 + acc2 * n1 * n3 + acc3 * n1 * n2) / (n1 * n2 + n1 * n3 + n2 * n3) * 100
        mauos = TimeSeriesFunction.MA(uos, m)
        return {'UOS': uos, 'MAUOS': mauos}

    # ================================================================
    #  三、能量型
    # ================================================================

    @staticmethod
    def CR(close, high, low, n=26):
        """CR 能量指标
        MID = REF((HIGH + LOW + CLOSE) / 3, 1)
        CR = SUM(MAX(0, HIGH-MID), N) / SUM(MAX(0, MID-LOW), N) * 100
        """
        mid = TimeSeriesFunction.REF((high + low + close) / 3, 1)
        zero = close * 0
        cr = (TimeSeriesFunction.SUM(MathFunction.MAX(high - mid, zero), n) /
              TimeSeriesFunction.SUM(MathFunction.MAX(mid - low, zero), n) * 100)
        return {'CR': cr}

    @staticmethod
    def PSY(close, n=12, m=6):
        """PSY 心理线
        PSY = COUNT(CLOSE > REF(CLOSE, 1), N) / N * 100
        MAPSY = MA(PSY, M)
        """
        cond = close > TimeSeriesFunction.REF(close, 1)
        psy = TimeSeriesFunction.COUNT(cond, n) / n * 100
        mapsy = TimeSeriesFunction.MA(psy, m)
        return {'PSY': psy, 'MAPSY': mapsy}

    @staticmethod
    def MASS(high, low, n1=9, n2=25, m=6):
        """MASS 梅斯线
        MASS = SUM(EMA(HIGH-LOW, N1) / EMA(EMA(HIGH-LOW, N1), N1), N2)
        MAMASS = MA(MASS, M)
        """
        hl_ema = TimeSeriesFunction.EMA(high - low, n1)
        mass = TimeSeriesFunction.SUM(hl_ema / TimeSeriesFunction.EMA(hl_ema, n1), n2)
        mamass = TimeSeriesFunction.MA(mass, m)
        return {'MASS': mass, 'MAMASS': mamass}

    @staticmethod
    def PCNT(close):
        """PCNT 幅度比
        PCNT = (CLOSE - REF(CLOSE, 1)) / REF(CLOSE, 1) * 100
        """
        ref_close = TimeSeriesFunction.REF(close, 1)
        pcnt = (close - ref_close) / ref_close * 100
        return {'PCNT': pcnt}

    @staticmethod
    def WAD(close, high, low):
        """WAD 威廉多空力度线
        MIDA = CLOSE - MIN(LOW, REF(CLOSE, 1))  (当CLOSE>REF(CLOSE,1))
        MIDB = CLOSE - MAX(HIGH, REF(CLOSE, 1)) (当CLOSE<REF(CLOSE,1))
        WAD = CUMSUM(MIDA or MIDB or 0)
        """
        ref_c = TimeSeriesFunction.REF(close, 1)
        zero = close * 0
        mida = close - MathFunction.MIN(low, ref_c)
        midb = close - MathFunction.MAX(high, ref_c)
        val = MathFunction.IF(close > ref_c, mida, MathFunction.IF(close < ref_c, midb, zero))
        wad = TimeSeriesFunction.CUMSUM(val)
        mawad = TimeSeriesFunction.MA(wad, 6)
        return {'WAD': wad, 'MAWAD': mawad}

    # ================================================================
    #  四、成交量型
    # ================================================================

    @staticmethod
    def OBV(close, volume):
        """OBV 能量潮
        若当日收盘价 > 昨日收盘价，OBV = 前日OBV + 今日成交量
        若当日收盘价 < 昨日收盘价，OBV = 前日OBV - 今日成交量
        若当日收盘价 = 昨日收盘价，OBV = 前日OBV
        """
        ref_close = TimeSeriesFunction.REF(close, 1)
        direction = MathFunction.SIGN(close - ref_close).fillna(0)  # 首日设为0，避免NaN累积
        obv = TimeSeriesFunction.CUMSUM(direction * volume)
        # 首日OBV等于首日成交量（标准做法）
        if len(obv) > 0 and len(volume) > 0:
            obv.iloc[0] = volume.iloc[0]
        return {'OBV': obv}

    @staticmethod
    def VR(close, volume, n=26):
        """VR 成交量变异率
        AV = SUM(IF(CLOSE>REF(CLOSE,1), VOLUME, 0), N)
        BV = SUM(IF(CLOSE<REF(CLOSE,1), VOLUME, 0), N)
        CV = SUM(IF(CLOSE=REF(CLOSE,1), VOLUME, 0), N)
        VR = (AV + CV/2) / (BV + CV/2) * 100
        """
        ref_close = TimeSeriesFunction.REF(close, 1)
        zero = pd.Series(0.0, index=close.index)
        av = TimeSeriesFunction.SUM(MathFunction.IF(close > ref_close, volume, zero), n)
        bv = TimeSeriesFunction.SUM(MathFunction.IF(close < ref_close, volume, zero), n)
        cv = TimeSeriesFunction.SUM(MathFunction.IF(close == ref_close, volume, zero), n)
        vr = (av + cv / 2) / (bv + cv / 2) * 100
        return {'VR': vr}

    @staticmethod
    def VOLMA(volume, n1=5, n2=10):
        """VOLMA 成交量均线"""
        return {
            f'VOLMA{n1}': TimeSeriesFunction.MA(volume, n1),
            f'VOLMA{n2}': TimeSeriesFunction.MA(volume, n2),
        }

    @staticmethod
    def WVAD(close, open_, high, low, volume, n=24, m=6):
        """WVAD 威廉变异离散量
        WVAD = SUM((CLOSE-OPEN)/(HIGH-LOW)*VOL, N)
        MAWVAD = MA(WVAD, M)
        """
        wvad = TimeSeriesFunction.SUM((close - open_) / (high - low) * volume, n)
        mawvad = TimeSeriesFunction.MA(wvad, m)
        return {'WVAD': wvad, 'MAWVAD': mawvad}

    @staticmethod
    def VOSC(volume, short=12, long=26):
        """VOSC 成交量震荡
        VOSC = (MA(VOL, SHORT) - MA(VOL, LONG)) / MA(VOL, SHORT) * 100
        """
        ma_short = TimeSeriesFunction.MA(volume, short)
        ma_long = TimeSeriesFunction.MA(volume, long)
        vosc = (ma_short - ma_long) / ma_short * 100
        return {'VOSC': vosc}

    @staticmethod
    def VRSI(volume, n1=6, n2=12, n3=24):
        """VRSI 量相对强弱
        LV = REF(VOL, 1)
        VRSI = SMA(MAX(VOL-LV, 0), N, 1) / SMA(ABS(VOL-LV), N, 1) * 100
        """
        lv = TimeSeriesFunction.REF(volume, 1)
        diff = volume - lv
        zero = pd.Series(0.0, index=volume.index)
        pos_diff = MathFunction.MAX(diff, zero)
        abs_diff = MathFunction.ABS(diff)
        result = {}
        for n in [n1, n2, n3]:
            result[f'VRSI{n}'] = TimeSeriesFunction.SMA(pos_diff, n, 1) / TimeSeriesFunction.SMA(abs_diff, n, 1) * 100
        return result

    @staticmethod
    def VSTD(volume, n=10):
        """VSTD 成交量标准差
        VSTD = STD(VOL, N)
        """
        vstd = StatisticsFunction.STD(volume, n)
        return {'VSTD': vstd}

    @staticmethod
    def AMO(amount, n1=5, n2=10):
        """AMO 成交额均线
        AMOW = MA(AMOUNT, N)
        """
        return {
            f'AMO{n1}': TimeSeriesFunction.MA(amount, n1),
            f'AMO{n2}': TimeSeriesFunction.MA(amount, n2),
        }

    @staticmethod
    def HSL(turnover_rate, n=5, m=10):
        """HSL 换手线
        HSL = MA(TURNOVER_RATE, N)
        """
        return {
            f'HSL{n}': TimeSeriesFunction.MA(turnover_rate, n),
            f'HSL{m}': TimeSeriesFunction.MA(turnover_rate, m),
        }

    @staticmethod
    def TAPI(close, amount, n=6):
        """TAPI 加权指数成交值
        TAPI = AMOUNT / CLOSE
        MATAPI = MA(TAPI, N)
        """
        tapi = amount / close
        matapi = TimeSeriesFunction.MA(tapi, n)
        return {'TAPI': tapi, 'MATAPI': matapi}

    # ================================================================
    #  五、均线型
    # ================================================================

    @staticmethod
    def MA(close, n1=5, n2=10, n3=20, n4=60):
        """MA 移动平均线"""
        return {
            f'MA{n1}': TimeSeriesFunction.MA(close, n1),
            f'MA{n2}': TimeSeriesFunction.MA(close, n2),
            f'MA{n3}': TimeSeriesFunction.MA(close, n3),
            f'MA{n4}': TimeSeriesFunction.MA(close, n4),
        }

    @staticmethod
    def EXPMA(close, n1=12, n2=50):
        """EXPMA 指数平均线"""
        return {
            f'EXPMA{n1}': TimeSeriesFunction.EMA(close, n1),
            f'EXPMA{n2}': TimeSeriesFunction.EMA(close, n2),
        }

    @staticmethod
    def BBI(close):
        """BBI 多空指标
        BBI = (MA(CLOSE,3) + MA(CLOSE,6) + MA(CLOSE,12) + MA(CLOSE,24)) / 4
        """
        bbi = (TimeSeriesFunction.MA(close, 3) + TimeSeriesFunction.MA(close, 6) +
               TimeSeriesFunction.MA(close, 12) + TimeSeriesFunction.MA(close, 24)) / 4
        return {'BBI': bbi}

    @staticmethod
    def AMV(volume, amount, n1=5, n2=13, n3=34):
        """AMV 成本价均线
        AMOV = VOL*(OPEN+CLOSE)/2
        AMV = SUM(AMOV, N) / SUM(VOL, N)
        此处用 AMOUNT 近似 AMOV (成交额 ≈ 成交量*均价)，按成交量加权平均
        """
        return {
            f'AMV{n1}': TimeSeriesFunction.SUM(amount, n1) / TimeSeriesFunction.SUM(volume, n1),
            f'AMV{n2}': TimeSeriesFunction.SUM(amount, n2) / TimeSeriesFunction.SUM(volume, n2),
            f'AMV{n3}': TimeSeriesFunction.SUM(amount, n3) / TimeSeriesFunction.SUM(volume, n3),
        }

    # ================================================================
    #  六、路径型
    # ================================================================

    @staticmethod
    def BOLL(close, n=20, k=2):
        """BOLL 布林线
        MID = MA(CLOSE, N)
        UPPER = MID + K * STD(CLOSE, N)
        LOWER = MID - K * STD(CLOSE, N)
        """
        mid = TimeSeriesFunction.MA(close, n)
        std = StatisticsFunction.STD(close, n)
        upper = mid + k * std
        lower = mid - k * std
        return {'UPPER': upper, 'MID': mid, 'LOWER': lower}

    @staticmethod
    def ENE(close, n=25, m1=6, m2=6):
        """ENE 轨道线
        UPPER = MA(CLOSE, N) * (1 + M1/100)
        LOWER = MA(CLOSE, N) * (1 - M2/100)
        ENE = (UPPER + LOWER) / 2
        """
        ma = TimeSeriesFunction.MA(close, n)
        upper = ma * (1 + m1 / 100)
        lower = ma * (1 - m2 / 100)
        ene = (upper + lower) / 2
        return {'UPPER': upper, 'ENE': ene, 'LOWER': lower}

    @staticmethod
    def MIKE(close, high, low, n=12):
        """MIKE 麦克指标
        TYP = (HIGH + LOW + CLOSE) / 3
        LL = LLV(LOW, N);  HH = HHV(HIGH, N)
        WR = TYP+(TYP-LL); MR = TYP+(HH-LL); SR = 2*HH-LL
        WS = TYP-(HH-TYP); MS = TYP-(HH-LL); SS = 2*LL-HH
        """
        typ = (high + low + close) / 3
        ll = TimeSeriesFunction.LLV(low, n)
        hh = TimeSeriesFunction.HHV(high, n)
        wr = typ + (typ - ll)
        mr = typ + (hh - ll)
        sr = 2 * hh - ll
        ws = typ - (hh - typ)
        ms = typ - (hh - ll)
        ss = 2 * ll - hh
        return {'WR': wr, 'MR': mr, 'SR': sr, 'WS': ws, 'MS': ms, 'SS': ss}

    @staticmethod
    def PBX(close, n1=4, n2=6, n3=9, n4=13, n5=18, n6=24):
        """PBX 瀑布线
        PBX = (EMA(CLOSE, N1) + EMA(CLOSE, 2*N1) + EMA(CLOSE, 4*N1)) / 3
        多条瀑布线
        """
        return {
            f'PBX{n1}': (TimeSeriesFunction.EMA(close, n1) + TimeSeriesFunction.EMA(close, n1 * 2) + TimeSeriesFunction.EMA(close, n1 * 4)) / 3,
            f'PBX{n2}': (TimeSeriesFunction.EMA(close, n2) + TimeSeriesFunction.EMA(close, n2 * 2) + TimeSeriesFunction.EMA(close, n2 * 4)) / 3,
            f'PBX{n3}': (TimeSeriesFunction.EMA(close, n3) + TimeSeriesFunction.EMA(close, n3 * 2) + TimeSeriesFunction.EMA(close, n3 * 4)) / 3,
            f'PBX{n4}': (TimeSeriesFunction.EMA(close, n4) + TimeSeriesFunction.EMA(close, n4 * 2) + TimeSeriesFunction.EMA(close, n4 * 4)) / 3,
            f'PBX{n5}': (TimeSeriesFunction.EMA(close, n5) + TimeSeriesFunction.EMA(close, n5 * 2) + TimeSeriesFunction.EMA(close, n5 * 4)) / 3,
            f'PBX{n6}': (TimeSeriesFunction.EMA(close, n6) + TimeSeriesFunction.EMA(close, n6 * 2) + TimeSeriesFunction.EMA(close, n6 * 4)) / 3,
        }

    @staticmethod
    def XS(close, high, low, n=13):
        """XS 薛斯通道
        SMA_C = SMA(CLOSE, N, 1)
        SMA_H = SMA(HIGH, N, 1)
        SMA_L = SMA(LOW, N, 1)
        UPP = SMA_H * 1.06
        SUP = SMA_C * 1.06
        SDN = SMA_C * 0.94
        LWN = SMA_L * 0.94
        """
        sma_c = TimeSeriesFunction.SMA(close, n, 1)
        sma_h = TimeSeriesFunction.SMA(high, n, 1)
        sma_l = TimeSeriesFunction.SMA(low, n, 1)
        upp = sma_h * 1.06
        sup = sma_c * 1.06
        sdn = sma_c * 0.94
        lwn = sma_l * 0.94
        return {'UPP': upp, 'SUP': sup, 'SDN': sdn, 'LWN': lwn}

    @staticmethod
    def BBIBOLL(close, n=11, k=6):
        """BBIBOLL BBI多空布林线
        BBI = (MA(CLOSE,3)+MA(CLOSE,6)+MA(CLOSE,12)+MA(CLOSE,24))/4
        UPPER = BBI + K * STD(BBI, N)
        LOWER = BBI - K * STD(BBI, N)
        """
        bbi = (TimeSeriesFunction.MA(close, 3) + TimeSeriesFunction.MA(close, 6) +
               TimeSeriesFunction.MA(close, 12) + TimeSeriesFunction.MA(close, 24)) / 4
        std = StatisticsFunction.STD(bbi, n)
        upper = bbi + k * std
        lower = bbi - k * std
        return {'BBIBOLL': bbi, 'UPPER': upper, 'LOWER': lower}

    # ================================================================
    #  七、其他型
    # ================================================================

    @staticmethod
    def ASI(close, open_, high, low):
        """ASI 振动升降指标
        A = ABS(HIGH - REF(CLOSE, 1))
        B = ABS(LOW - REF(CLOSE, 1))
        C = ABS(HIGH - REF(LOW, 1))
        D = ABS(REF(CLOSE, 1) - REF(OPEN, 1))
        R = 根据A/B/C大小关系取不同值
        SI = 16 * (CLOSE-REF(CLOSE,1) + (CLOSE-OPEN)/2 + (REF(CLOSE,1)-REF(OPEN,1))/4) / R * MAX(A,B)
        ASI = SUM(SI, 0)  即累计
        """
        ref_c = TimeSeriesFunction.REF(close, 1)
        ref_o = TimeSeriesFunction.REF(open_, 1)
        ref_l = TimeSeriesFunction.REF(low, 1)

        a = MathFunction.ABS(high - ref_c)
        b = MathFunction.ABS(low - ref_c)
        c = MathFunction.ABS(high - ref_l)
        d = MathFunction.ABS(ref_c - ref_o)

        em = close - ref_c + (close - open_) / 2 + (ref_c - ref_o) / 4
        max_ab = MathFunction.MAX(a, b)

        # R: 当A>B且A>C时 R=A+B/2+D; 当B>A且B>C时 R=B+A/2+D; 否则 R=C+D
        r_a = a + b / 2 + d
        r_b = b + a / 2 + d
        r_c = c + d
        r = MathFunction.IF((a >= b) & (a >= c), r_a,
                            MathFunction.IF((b > a) & (b >= c), r_b, r_c))
        r = r.replace(0, np.nan)

        si = 16 * em / r * max_ab
        asi = TimeSeriesFunction.CUMSUM(si)
        return {'SI': si, 'ASI': asi}

    @staticmethod
    def ATR(close, high, low, n=14):
        """ATR 真实波幅均值
        ATR = MA(TR, N)
        """
        tr = TimeSeriesFunction.TR(high, low, close)
        atr = TimeSeriesFunction.MA(tr, n)
        return {'TR': tr, 'ATR': atr}

    @staticmethod
    def SAR(close, high, low, n=4, step=0.02, max_af=0.2):
        """SAR 抛物线转向指标
        初始方向根据前N日趋势判定，加速因子从step开始，每创新高/低增加step，最大max_af
        """
        sar = TimeSeriesFunction.SAR(high, low, close, n, step, max_af)
        return {'SAR': sar}

    @staticmethod
    def CDP(close, high, low):
        """CDP 逆势操作
        CDP = (REF(HIGH,1) + REF(LOW,1) + 2*REF(CLOSE,1)) / 4
        AH = CDP + (REF(HIGH,1) - REF(LOW,1))
        NH = 2 * CDP - REF(LOW,1)
        NL = 2 * CDP - REF(HIGH,1)
        AL = CDP - (REF(HIGH,1) - REF(LOW,1))
        """
        ref_h = TimeSeriesFunction.REF(high, 1)
        ref_l = TimeSeriesFunction.REF(low, 1)
        ref_c = TimeSeriesFunction.REF(close, 1)
        cdp = (ref_h + ref_l + 2 * ref_c) / 4
        ah = cdp + (ref_h - ref_l)
        nh = 2 * cdp - ref_l
        nl = 2 * cdp - ref_h
        al = cdp - (ref_h - ref_l)
        return {'AH': ah, 'NH': nh, 'CDP': cdp, 'NL': nl, 'AL': al}


def is_stock(code):
    """判断代码是否为股票
    规则: 数字部分为6位，SH市场首位必须是6，SZ市场首位必须是0或3
    """
    parts = code.split('.')
    pure_code, market = parts[0], parts[1].upper()
    if len(pure_code) != 6 or not pure_code.isdigit():
        return False
    if market == 'SH' and pure_code[0] == '6':
        return True
    if market == 'SZ' and pure_code[0] in ('0', '3'):
        return True
    return False


def forward_adjust(df, backward_factor, code):
    """对单只股票的kline DataFrame做前复权（只调整OHLC价格）
    df: kline DataFrame，含 kline_time, open, high, low, close, volume, amount
    backward_factor: get_backward_factor 返回的 DataFrame (index=datetime, columns=code)
    code: 股票代码
    返回: 复权后的 DataFrame 副本
    """
    df_adj = df.copy()

    if code not in backward_factor.columns:
        print(f"警告: 未找到 {code} 的复权因子，跳过前复权")
        return df_adj

    factor = backward_factor[code]

    # 按 kline_time 对齐复权因子
    if 'kline_time' in df_adj.columns:
        kline_dates = pd.to_datetime(df_adj['kline_time'])
    else:
        kline_dates = pd.to_datetime(df_adj.index)

    # reindex 到 kline 日期，前向填充缺失值
    factor_aligned = factor.reindex(kline_dates, method='ffill')
    factor_aligned = factor_aligned.values

    # 取最后一个有效因子值作为 latest_factor
    latest_factor = factor_aligned[~pd.isna(factor_aligned)][-1]

    # 前复权: price_adj = price_raw * backward_factor / latest_factor
    adj_ratio = factor_aligned / latest_factor
    for col in ['open', 'high', 'low', 'close']:
        if col in df_adj.columns:
            df_adj[col] = df_adj[col] * adj_ratio

    return df_adj


# ================================================================
#  演示主程序
# ================================================================
if __name__ == '__main__':
    # 登录
    ad.login(username="****",
             password="****",
             host="***.***.***.***.***",
             port="****")

    # 获取数据
    base_data_object = ad.BaseData()
    calendar = base_data_object.get_calendar()
    market_data_object = ad.MarketData(calendar)

    code = '600519.SH'
    kline_day = market_data_object.query_kline([code], begin_date=20130101, end_date=20260306,
                                               period=ad.constant.Period.day.value)
    df = kline_day[code]

    # 前复权处理（仅对股票，不对指数和基金）
    if is_stock(code):
        backward_factor = base_data_object.get_backward_factor([code], is_local=False)
        df = forward_adjust(df, backward_factor, code)
        print(f"已对 {code} 进行前复权处理")
    else:
        print(f"{code} 非股票，跳过前复权")

    close = df['close']
    open_ = df['open']
    high = df['high']
    low = df['low']
    volume = df['volume']
    amount = df['amount'] if 'amount' in df.columns else close * volume

    TI = TechnicalIndicators

    # ==================== 一、超买超卖型 ====================
    print("=" * 60)
    print("一、超买超卖型")
    print("=" * 60)

    kdj = TI.KDJ(close, high, low)
    print(f"\n【KDJ 随机指标】")
    for k, v in kdj.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    rsi = TI.RSI(close)
    print(f"\n【RSI 相对强弱指标】")
    for k, v in rsi.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    wr = TI.WR(close, high, low)
    print(f"\n【WR 威廉指标】")
    for k, v in wr.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    cci = TI.CCI(close, high, low)
    print(f"\n【CCI 顺势指标】")
    for k, v in cci.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    roc = TI.ROC(close)
    print(f"\n【ROC 变动率指标】")
    for k, v in roc.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    mtm = TI.MTM(close)
    print(f"\n【MTM 动量指标】")
    for k, v in mtm.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    bias = TI.BIAS(close)
    print(f"\n【BIAS 乖离率】")
    for k, v in bias.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    skdj = TI.SKDJ(close, high, low)
    print(f"\n【SKDJ 慢速随机指标】")
    for k, v in skdj.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    mfi = TI.MFI(close, high, low, volume)
    print(f"\n【MFI 资金流量指标】")
    for k, v in mfi.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    osc = TI.OSC(close)
    print(f"\n【OSC 变动速率线】")
    for k, v in osc.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    udl = TI.UDL(close)
    print(f"\n【UDL 引力线】")
    for k, v in udl.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    accer = TI.ACCER(close)
    print(f"\n【ACCER 幅度涨速】")
    for k, v in accer.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    rccd = TI.RCCD(close)
    print(f"\n【RCCD 异同离差乖离率】")
    for k, v in rccd.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    marsi = TI.MARSI(close)
    print(f"\n【MARSI 相对强弱平均线】")
    for k, v in marsi.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    # ==================== 二、趋势型 ====================
    print("\n" + "=" * 60)
    print("二、趋势型")
    print("=" * 60)

    macd = TI.MACD(close)
    print(f"\n【MACD 指数平滑异同移动平均线】")
    for k, v in macd.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    dmi = TI.DMI(close, high, low)
    print(f"\n【DMI 趋向指标】")
    for k, v in dmi.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    dma = TI.DMA(close)
    print(f"\n【DMA 平行线差指标】")
    for k, v in dma.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    trix = TI.TRIX(close)
    print(f"\n【TRIX 三重指数平滑移动平均】")
    for k, v in trix.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    arbr = TI.ARBR(close, open_, high, low)
    print(f"\n【ARBR 人气意愿指标】")
    for k, v in arbr.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    emv = TI.EMV(close, high, low, volume)
    print(f"\n【EMV 简易波动指标】")
    for k, v in emv.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    dpo = TI.DPO(close)
    print(f"\n【DPO 区间震荡线】")
    for k, v in dpo.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    vhf = TI.VHF(close)
    print(f"\n【VHF 十字过滤线】")
    for k, v in vhf.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    cho = TI.CHO(close, high, low, volume)
    print(f"\n【CHO 佳庆指标】")
    for k, v in cho.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    dbcd = TI.DBCD(close)
    print(f"\n【DBCD 异同离差乖离率】")
    for k, v in dbcd.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    ddi = TI.DDI(close, high, low)
    print(f"\n【DDI 方向标准离差指数】")
    for k, v in ddi.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    js = TI.JS(close)
    print(f"\n【JS 加速线】")
    for k, v in js.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    qacd = TI.QACD(close)
    print(f"\n【QACD 快速异同移动平均】")
    for k, v in qacd.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    uos = TI.UOS(close, high, low)
    print(f"\n【UOS 终极波动指标】")
    for k, v in uos.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    # ==================== 三、能量型 ====================
    print("\n" + "=" * 60)
    print("三、能量型")
    print("=" * 60)

    cr = TI.CR(close, high, low)
    print(f"\n【CR 能量指标】")
    for k, v in cr.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    psy = TI.PSY(close)
    print(f"\n【PSY 心理线】")
    for k, v in psy.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    mass = TI.MASS(high, low)
    print(f"\n【MASS 梅斯线】")
    for k, v in mass.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    pcnt = TI.PCNT(close)
    print(f"\n【PCNT 幅度比】")
    for k, v in pcnt.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    wad = TI.WAD(close, high, low)
    print(f"\n【WAD 威廉多空力度线】")
    for k, v in wad.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    # ==================== 四、成交量型 ====================
    print("\n" + "=" * 60)
    print("四、成交量型")
    print("=" * 60)

    obv = TI.OBV(close, volume)
    print(f"\n【OBV 能量潮】")
    for k, v in obv.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    vr = TI.VR(close, volume)
    print(f"\n【VR 成交量变异率】")
    for k, v in vr.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    vol_ma = TI.VOLMA(volume)
    print(f"\n【VOLMA 成交量均线】")
    for k, v in vol_ma.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    wvad = TI.WVAD(close, open_, high, low, volume)
    print(f"\n【WVAD 威廉变异离散量】")
    for k, v in wvad.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    vosc = TI.VOSC(volume)
    print(f"\n【VOSC 成交量震荡】")
    for k, v in vosc.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    vrsi = TI.VRSI(volume)
    print(f"\n【VRSI 量相对强弱】")
    for k, v in vrsi.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    vstd = TI.VSTD(volume)
    print(f"\n【VSTD 成交量标准差】")
    for k, v in vstd.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    amo = TI.AMO(amount)
    print(f"\n【AMO 成交额均线】")
    for k, v in amo.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    tapi = TI.TAPI(close, amount)
    print(f"\n【TAPI 加权指数成交值】")
    for k, v in tapi.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    # ==================== 五、均线型 ====================
    print("\n" + "=" * 60)
    print("五、均线型")
    print("=" * 60)

    ma = TI.MA(close)
    print(f"\n【MA 移动平均线】")
    for k, v in ma.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    expma = TI.EXPMA(close)
    print(f"\n【EXPMA 指数平均线】")
    for k, v in expma.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    bbi = TI.BBI(close)
    print(f"\n【BBI 多空指标】")
    for k, v in bbi.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    amv = TI.AMV(volume, amount)
    print(f"\n【AMV 成本价均线】")
    for k, v in amv.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    # ==================== 六、路径型 ====================
    print("\n" + "=" * 60)
    print("六、路径型")
    print("=" * 60)

    boll = TI.BOLL(close)
    print(f"\n【BOLL 布林线】")
    for k, v in boll.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    ene = TI.ENE(close)
    print(f"\n【ENE 轨道线】")
    for k, v in ene.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    mike = TI.MIKE(close, high, low)
    print(f"\n【MIKE 麦克指标】")
    for k, v in mike.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    pbx = TI.PBX(close)
    print(f"\n【PBX 瀑布线】")
    for k, v in pbx.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    xs = TI.XS(close, high, low)
    print(f"\n【XS 薛斯通道】")
    for k, v in xs.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    bbiboll = TI.BBIBOLL(close)
    print(f"\n【BBIBOLL BBI多空布林线】")
    for k, v in bbiboll.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    # ==================== 七、其他型 ====================
    print("\n" + "=" * 60)
    print("七、其他型")
    print("=" * 60)

    asi = TI.ASI(close, open_, high, low)
    print(f"\n【ASI 振动升降指标】")
    for k, v in asi.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    atr = TI.ATR(close, high, low)
    print(f"\n【ATR 真实波幅均值】")
    for k, v in atr.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    sar = TI.SAR(close, high, low)
    print(f"\n【SAR 抛物线转向】")
    for k, v in sar.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    cdp = TI.CDP(close, high, low)
    print(f"\n【CDP 逆势操作】")
    for k, v in cdp.items():
        print(f"  {k}: {v.tail(3).tolist()}")

    print("\n" + "=" * 60)
    print("所有技术指标计算完成!")
    print("=" * 60)


【量化算子篇】截面函数
0


一、截面函数列表 

AmazingData 最低版本号：1.0.30

截面函数用于计算同一交易日内多个标的之间的统计指标。输入数据为DataFrame，行为日期，列为标的代码。

序号

函数名称

函数用法

1

CSCORR

CSCORR(X,Y) 返回每交易日两个指标的当日相关度

2

CSCOUNT

CSCOUNT(X) 统计交易日截面的标的个数

3

CSCOV

CSCOV(X,Y) 返回每交易日两个指标(X,Y)的当日协方差

4

CSDEMEAN

CSDEMEAN(X) 对每个交易日的截面数据减去均值

5

CSMAX

CSMAX(X) 计算交易日截面指标的最大值

6

CSMEAN

CSMEAN(X) 计算交易日截面指标的平均值

7

CSMEDIAN

CSMEDIAN(X) 计算交易日截面指标的中位数

8

CSMIN

CSMIN(X) 计算交易日截面指标的最小值

9

CSNORMALIZE

CSNORMALIZE(X) 对每个交易日的截面数据进行归一化到[0,1]

10

CSPCTRANK

CSPCTRANK(X) 计算交易日截面指标的百分位排名

11

CSQUANTILE

CSQUANTILE(X,N) 计算交易日截面指标的分位数N

12

CSRANK

CSRANK(X,B) 计算交易日截面指标的排名

13

CSSTD

CSSTD(X) 计算交易日截面指标的标准差

14

CSSUM

CSSUM(X) 计算交易日截面指标的求和

15

CSVAR

CSVAR(X) 计算交易日截面指标的方差

16

CSZSCORE

CSZSCORE(X) 对每个交易日的截面数据进行Z-score标准化



二、数据函数说明 

（1）CSCORR(x: DataFrame, y: DataFrame) 截面相关度

用法: CSCORR(X,Y) 返回每交易日两个指标的当日相关度

（2）CSCOUNT(x: DataFrame) 截面标的个数

用法: CSCOUNT(X)统计交易日截面的标的个数

（3）CSCOV(x: DataFrame, y: DataFrame) 截面协方差

用法: CSCOV(X,Y)返回每交易日两个指标(X,Y)的当日协方差

（4）CSDEMEAN(x: DataFrame) 截面去均值

用法: CSDEMEAN(X) 对每个交易日的截面数据减去均值

（5）CSMAX(x: DataFrame) 截面最大值

用法: CSMAX(X) 计算交易日截面指标的最大值

（6）CSMEAN(x: DataFrame) 截面平均值

用法: CSMEAN(X) 计算交易日截面指标的平均值

（7）CSMEDIAN(x: DataFrame) 截面中位数

用法: CSMEDIAN(X)计算交易日截面指标的中位数

（8）CSMIN(x: DataFrame) 截面最小值

用法: CSMIN(X) 计算交易日截面指标的最小值

（9）CSNORMALIZE(x: DataFrame) 截面归一化(Min-Max)

用法: CSNORMALIZE(X) 对每个交易日的截面数据进行归一化到[0,1]

（10）CSPCTRANK(x: DataFrame) 截面百分位排名

用法: CSPCTRANK(X) 计算交易日截面指标的百分位排名

（11）CSQUANTILE(x: DataFrame, n: float) 截面分位数

用法: CSQUANTILE(X,N) 计算交易日截面指标的分位数N

（12）CSRANK(x: DataFrame, ascending: bool) 截面排名

用法: CSRANK(X,B) 计算交易日截面指标的排名

（13）CSSTD(x: DataFrame) 截面标准差

用法: CSSTD(X) 计算交易日截面指标的标准差

（14）CSSUM(x: DataFrame) 截面求和

用法: CSSUM(X) 计算交易日截面指标的求和

（15）CSVAR(x: DataFrame) 截面方差

用法: CSVAR(X) 计算交易日截面指标的方差

（16）CSZSCORE(x: DataFrame) 截面Z-score标准化

用法: CSZSCORE(X) 对每个交易日的截面数据进行Z-score标准化



三、api案例

import AmazingData as ad

from AmazingData.operator.function import MathFunction

ad.login(username='username',
password='password',
host='***.***.***.***',port=****)

 # 获取数据
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
market_data_object = ad.MarketData(calendar)
# 多只股票数据 (用于截面函数)
codes = ['00000*.SZ', '00000*.SZ', '00000*.SZ',
'00000*.SZ', '00000*.SZ']
kline_multi =
market_data_object.query_kline(codes, begin_date=20240101, end_date=20250101,
period=ad.constant.Period.day.value)
# 构建截面数据 DataFrame (行:日期, 列:标的)
close_df = pd.DataFrame({c:
kline_multi[c]['close'] for c in codes if c in kline_multi})
open_df = pd.DataFrame({c: kline_multi[c]['open']
for c in codes if c in kline_multi})
# CSCOV - 截面协方差
result = CrossSectionFunction.CSCOV(close_df,
open_df)
# CSCOUNT - 截面标的个数
result = CrossSectionFunction.CSCOUNT(close_df)
# CSQUANTILE - 截面分位数
result = CrossSectionFunction.CSQUANTILE(close_df,
0.5)
# CSRANK - 截面排名
result = CrossSectionFunction.CSRANK(close_df,
ascending=True)
# CSSTD - 截面标准差
result = CrossSectionFunction.CSSTD(close_df)
# CSSUM - 截面求和
result = CrossSectionFunction.CSSUM(close_df)
# CSVAR - 截面方差
result = CrossSectionFunction.CSVAR(close_df)
# CSPCTRANK - 截面百分位排名
result = CrossSectionFunction.CSPCTRANK(close_df)
# CSMEAN - 截面平均值
result = CrossSectionFunction.CSMEAN(close_df)
# CSMAX - 截面最大值
result = CrossSectionFunction.CSMAX(close_df)
# CSCORR - 截面相关度
result = CrossSectionFunction.CSCORR(close_df,
open_df)
# CSMIN - 截面最小值
result = CrossSectionFunction.CSMIN(close_df)
# CSMEDIAN - 截面中位数
result = CrossSectionFunction.CSMEDIAN(close_df)
# CSZSCORE - 截面Z-score标准化
result = CrossSectionFunction.CSZSCORE(close_df)
# CSNORMALIZE - 截面归一化(Min-Max)
result =
CrossSectionFunction.CSNORMALIZE(close_df)
# CSDEMEAN - 截面去均值
result =
CrossSectionFunction.CSDEMEAN(close_df)

【量化算子篇】时序函数


一、时序函数列表 

AmazingData 最低版本号：1.0.30

时序函数用于时间序列数据的处理，包括引用、移动平均、条件统计等。

序号

函数名称

函数用法

1

AMA

AMA(X,A) A为自适应系数,必须小于1

2

BARSLAST

BARSLAST(X) 上一次X不为False到现在的周期数

3

BARSLASTCOUNT

BARSLASTCOUNT(X) 统计连续满足X条件的周期数

4

BARSLASTS

BARSLASTS(X,N) X倒数第N满足到现在的周期数,N支持变量

5

BARSNEXT

BARSNEXT(X) 下一次X不为0到现在的周期数

6

BARSSINCE

BARSSINCE(X) 第一次X不为0到现在的周期数

7

BARSSINCEN

BARSSINCEN(X,N) N周期内第一次X不为0到现在的周期数,N为常量

8

BARSTATUS

BARSTATUS(X) 结果1表示第一根K线,2表示最后一根K线,0表示在中间位置

9

COUNT

COUNT(X,N) 统计N周期中满足X条件的周期数,若N<=0则从第一个有效值开始

10

CROSS

CROSS(A,B) 表示当A从下方向上穿过B时返回1,否则返回0

11

CUMSUM

CUMSUM(X) 从第一个有效值开始对X累计求和

12

CURRBARSCOUNT

CURRBARSCOUNT(X) 从最新一根K线倒数编号,从1开始计数

13

DMA

DMA(X,A) 求X的动态移动平均

14

DOWNNDAY

DOWNNDAY(CLOSE,M) 表示连跌M个周期,M为常量

15

EMA

EMA(X,N) X的N日指数移动平均,算法:Y=(X*2+Y'*(N-1))/(N+1)

16

EVERY

EVERY(X,N) 表示N日内一直满足条件X (N应大于0,小于总周期数,N支持变量)

17

EXIST

EXIST(X,N) 表示N日内是否存在满足条件X

18

EXISTR

EXISTR(X,A,B) 表示从前A日内到前B日内是否存在满足条件X

19

EXPMEMA

EXPMEMA(X,N) X的N日指数平滑移动平均,N不支持变量

20

FILTER

FILTER(X,N) X满足条件后,将其后N周期内的数据置为0,N为常量

21

FILTERX

FILTERX(X,N) X满足条件后,将其前N周期内的数据置为0,N为常量

22

HHV

HHV(X,N) 求N周期内X最高值,N=0则从第一个有效值开始

23

HHVBARS

HHVBARS(X,N) 求N周期内X最高值到当前周期数,N=0表示从第一个有效值开始统计

24

HHVLLV

HHVLLV(X,T,N1,N2) 表示前N1日到前N2日时段内的X最高值(T=0时)或最低值(T=1时)

25

HOD

HOD(X,N) 求当前X数据是N周期内的第几个高值,N=0则从第一个有效值开始

26

LAST

LAST(X,A,B) 表示从前A日到前B日内一直满足条件X

27

LLV

LLV(X,N) 求N周期内X最低值,N=0则从第一个有效值开始

28

LLVBARS

LLVBARS(X,N) 求N周期内X最低值到当前周期数,N=0表示从第一个有效值开始统计

29

LOD

LOD(X,N) 求当前X数据是N周期内的第几个低值,N=0则从第一个有效值开始

30

LONGCROSS

LONGCROSS(A,B,N) 表示A在N周期内都小于B,本周期从下方向上穿过B时返回1,否则返回0

31

MA

MA(X,N) X的N日简单移动平均,算法(X1+X2+X3+...+Xn)/N,N支持变量

32

MEMA

MEMA(X,N) X的N日平滑移动平均,如Y=(X+Y'*(N-1))/N

33

MULAR

MULAR(X,N) 统计N周期中X的乘积,N=0则从第一个有效值开始

34

NDAY

NDAY(CLOSE,OPEN,3) 表示连续3日收阳线

35

RANGE

RANGE(A,B,C) A在B和C范围之间,B<A<C

36

REF

REF(X,A) 引用A周期前的X值,A可以是变量

37

REFV

REFV(X,A) 引用A周期前的X值,A可以是变量

38

REFX

REFX(X,A) 引用A周期后的X值,A可以是变量

39

REFXV

REFXV(X,A) 引用A周期后的X值,A可以是变量

40

REVERSE

REVERSE(X) 返回-X

41

SAR

SAR(HIGH,LOW,CLOSE,N,STEP,MAXAF)

42

SHIFT

SHIFT(A,N) 获取A的N个交易日前的值

43

SMA

SMA(X,N,M) X的N日移动平均,M为权重,如Y=(X*M+Y'*(N-M))/N

44

SUM

SUM(X,N) 统计N周期中X的总和,N=0则从第一个有效值开始

45

SUMBARS

SUMBARS(X,A) 将X向前累加直到大于等于A,返回这个区间的周期数

46

SUMBARSX

SUMBARSX(X,A) 将X向前累加直到大于等于A,返回这个区间的周期数

47

TMA

TMA(X,A,B) A和B必须小于1,算法 Y=(A*Y'+B*X),其中Y'表示上一周期Y值,初值为X

48

TOTALBARSCOUNT

TOTALBARSCOUNT(X) 从第一根K线开始编号,从1开始递增计数

49

TR

TR(HIGH,LOW,CLOSE) 求真实波幅

50

UPNDAY

UPNDAY(CLOSE,M) 表示连涨M个周期,M为常量

51

WMA

WMA(X,N) X的N日加权移动平均,算法:Yn=(1*X1+2*X2+...+n*Xn)/(1+2+...+n)



二、时序函数说明 

（1）AMA(x: Series, a: Series) 自适应均线值

用法: AMA(X,A) A为自适应系数,必须小于1

（2）BARSLAST(x: Series) 上一次条件成立到当前的周期数

用法: BARSLAST(X) 上一次X不为False到现在的周期数

（3）BARSLASTCOUNT(x: Series) 统计连续满足条件的周期数

用法: BARSLASTCOUNT(X) 统计连续满足X条件的周期数

（4）BARSLASTS(x: Series, n: int) 倒数第N次成立时距今的周期数

用法: BARSLASTS(X,N) X倒数第N满足到现在的周期数,N支持变量

（5）BARSNEXT(x: Series) 下一次条件成立到当前的周期数(未来函数)

用法: BARSNEXT(X) 下一次X不为0到现在的周期数

（6）BARSSINCE(x: Series) 第一个条件成立到当前的周期数

用法: BARSSINCE(X) 第一次X不为0到现在的周期数

（7）BARSSINCEN(x: Series, n: int) N周期内第一个条件成立到当前的周期数

用法: BARSSINCEN(X,N) N周期内第一次X不为0到现在的周期数,N为常量

（8）BARSTATUS(x: Series) 返回数据的位置信息

用法: BARSTATUS(X) 结果1表示第一根K线,2表示最后一根K线,0表示在中间位置

（9）COUNT(x: Series, n: int) 统计满足条件的周期数

用法: COUNT(X,N)统计N周期中满足X条件的周期数,若N<=0则从第一个有效值开始

（10）CROSS(a: Series, b: Series) 两条线交叉

用法: CROSS(A,B)表示当A从下方向上穿过B时返回1,否则返回0

（11）CUMSUM(x: Series) 累计求和

用法: CUMSUM(X) 从第一个有效值开始对X累计求和

（12）CURRBARSCOUNT(x: Series) 求到最后K线的周期数

用法: CURRBARSCOUNT(X) 从最新一根K线倒数编号,从1开始计数

（13）DMA(x: Series, a: Series) 动态移动平均

用法: DMA(X,A) 求X的动态移动平均

（14）DOWNNDAY(x: Series, n: int) 返回周期数内是否连跌

用法: DOWNNDAY(CLOSE,M) 表示连跌M个周期,M为常量

（15）EMA(x: Series, n: int) 指数移动平均

用法: EMA(X,N) X的N日指数移动平均,算法:Y=(X*2+Y'*(N-1))/(N+1)

（16）EVERY(x: Series, n: int) 一直存在

用法: EVERY(X,N)表示N日内一直满足条件X (N应大于0,小于总周期数,N支持变量)

（17）EXIST(x: Series, n: int) 是否存在

用法: EXIST(X,N)表示N日内是否存在满足条件X

（18）EXISTR(x: Series, a: int, b: int) 是否存在(前几日到前几日间)

用法: EXISTR(X,A,B) 表示从前A日内到前B日内是否存在满足条件X

（19）EXPMEMA(x: Series, n: int) 指数平滑移动平均

用法: EXPMEMA(X,N) X的N日指数平滑移动平均,N不支持变量

（20）FILTER(x: Series, n: int) 过滤连续出现的信号

用法: FILTER(X,N) X满足条件后,将其后N周期内的数据置为0,N为常量

（21）FILTERX(x: Series, n: int) 反向过滤连续出现的信号

用法: FILTERX(X,N) X满足条件后,将其前N周期内的数据置为0,N为常量

（22）HHV(x: Series, n: int) 求N周期内最高值

用法: HHV(X,N) 求N周期内X最高值,N=0则从第一个有效值开始

（23）HHVBARS(x: Series, n: int) 求上一高点到当前的周期数

用法: HHVBARS(X,N) 求N周期内X最高值到当前周期数,N=0表示从第一个有效值开始统计

（24）HHVLLV(x: Series, t: int, n1: int, n2: int) 阶段最高最低值

用法: HHVLLV(X,T,N1,N2) 表示前N1日到前N2日时段内的X最高值(T=0时)或最低值(T=1时)

（25）HOD(x: Series, n: int) 求高值名次

用法: HOD(X,N) 求当前X数据是N周期内的第几个高值,N=0则从第一个有效值开始

（26）LAST(x: Series, a: int, b: int) 持续存在

用法: LAST(X,A,B) 表示从前A日到前B日内一直满足条件X

（27）LLV(x: Series, n: int) 求N周期内最低值

用法: LLV(X,N) 求N周期内X最低值,N=0则从第一个有效值开始

（28）LLVBARS(x: Series, n: int) 求上一低点到当前的周期数

用法: LLVBARS(X,N) 求N周期内X最低值到当前周期数,N=0表示从第一个有效值开始统计

（29）LOD(x: Series, n: int) 求低值名次

用法: LOD(X,N) 求当前X数据是N周期内的第几个低值,N=0则从第一个有效值开始

（30）LONGCROSS(a: Series, b: Series, n: int) 两条线维持一定周期后交叉

用法: LONGCROSS(A,B,N) 表示A在N周期内都小于B,本周期从下方向上穿过B时返回1,否则返回0

（31）MA(x: Series, n: int) 简单移动平均

用法: MA(X,N) X的N日简单移动平均,算法(X1+X2+X3+...+Xn)/N,N支持变量

（32）MEMA(x: Series, n: int) 平滑移动平均

用法: MEMA(X,N) X的N日平滑移动平均,如Y=(X+Y'*(N-1))/N

（33）MULAR(x: Series, n: int) 求累乘

用法: MULAR(X,N)统计N周期中X的乘积,N=0则从第一个有效值开始

（34）NDAY(x: Series, y: Series, n: int) 返回是否持续存在X>Y

用法: NDAY(CLOSE,OPEN,3) 表示连续3日收阳线

（35）RANGE(a: Series, b: Series, c: Series) 范围判断

用法: RANGE(A,B,C) A在B和C范围之间,B<A<C

（36）REF(x: Series, n: int) 引用若干周期前的数据

用法: REF(X,A) 引用A周期前的X值,A可以是变量

（37）REFV(x: Series, n: int) 引用若干周期前的数据(平滑处理)

用法: REFV(X,A) 引用A周期前的X值,A可以是变量

（38）REFX(x: Series, n: int) 引用若干周期后的数据(未来函数)

用法: REFX(X,A) 引用A周期后的X值,A可以是变量

（39）REFXV(x: Series, n: int) 引用若干周期后的数据(平滑处理)(未来函数)

用法: REFXV(X,A)引用A周期后的X值,A可以是变量

（40）REVERSE(x: Series) 求相反数

用法: REVERSE(X)返回-X

（41）SAR(high: Series, low: Series, close: Series, n: int, step: float, max_af: float) 抛物线转向指标

用法: SAR(HIGH,LOW,CLOSE,N,STEP,MAXAF)

（42）SHIFT(x: Series, n: int) 获取N个交易日前的值

用法: SHIFT(A,N)获取A的N个交易日前的值

（43）SMA(x: Series, n: int, m: int) 移动平均

用法: SMA(X,N,M) X的N日移动平均,M为权重,如Y=(X*M+Y'*(N-M))/N

（44）SUM(x: Series, n: int) 求总和

用法: SUM(X,N) 统计N周期中X的总和,N=0则从第一个有效值开始

（45）SUMBARS(x: Series, a: float) 向前累加到指定值到现在的周期数

用法: SUMBARS(X,A) 将X向前累加直到大于等于A,返回这个区间的周期数

（46）SUMBARSX(x: Series, a: float) 向前累加到指定值到现在的周期数

用法: SUMBARSX(X,A) 将X向前累加直到大于等于A,返回这个区间的周期数

（47）TMA(x: Series, a: float, b: float) 移动平均

用法: TMA(X,A,B) A和B必须小于1,算法 Y=(A*Y'+B*X),其中Y'表示上一周期Y值,初值为X

（48）TOTALBARSCOUNT(x: Series) 求到当前的周期数

用法: TOTALBARSCOUNT(X) 从第一根K线开始编号,从1开始递增计数

（49）TR(high: Series, low: Series, close: Series) 求真实波幅

用法: TR(HIGH,LOW,CLOSE) 求真实波幅

（50）UPNDAY(x: Series, n: int) 返回周期数内是否连涨

用法: UPNDAY(CLOSE,M) 表示连涨M个周期,M为常量

（51）WMA(x: Series, n: int) 加权移动平均

用法: WMA(X,N) X的N日加权移动平均,算法:Yn=(1*X1+2*X2+...+n*Xn)/(1+2+...+n)

三、api案例

import AmazingData as ad

from AmazingData.operator.function import MathFunction

ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 

# 获取数据
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
market_data_object = ad.MarketData(calendar)
code = '00000*.SH'
kline_day = market_data_object.query_kline([code],
begin_date=20130101, end_date=20250722,
                                          
period=ad.constant.Period.day.value)
df = kline_day[code]
# ========== 位置信息函数 ==========
# BARSTATUS - 返回数据的位置信息
result = TimeSeriesFunction.BARSTATUS(df['close'])
# CURRBARSCOUNT - 求到最后K线的周期数
result = TimeSeriesFunction.CURRBARSCOUNT(df['close'])
# TOTALBARSCOUNT - 求到当前的周期数(从1开始递增)
result = TimeSeriesFunction.TOTALBARSCOUNT(df['close'])
# ========== 条件周期统计函数 ==========
# BARSLAST - 上一次条件成立到当前的周期数
condition = df['close'] > df['open']
result = TimeSeriesFunction.BARSLAST(condition)
# BARSLASTS - 倒数第N次成立时距今的周期数
result = TimeSeriesFunction.BARSLASTS(condition, 3)
# BARSNEXT - 下一次条件成立到当前的周期数(未来函数)
result = TimeSeriesFunction.BARSNEXT(condition)
# BARSSINCEN - N周期内第一个条件成立到当前的周期数
result = TimeSeriesFunction.BARSSINCEN(condition, 10)
# BARSSINCE - 第一个条件成立到当前的周期数
result = TimeSeriesFunction.BARSSINCE(condition)
# COUNT - 统计满足条件的周期数
result = TimeSeriesFunction.COUNT(condition, 20)
# BARSLASTCOUNT - 统计连续满足条件的周期数
result = TimeSeriesFunction.BARSLASTCOUNT(condition)
# ========== 最值函数 ==========
# HHV - 求N周期内最高值
result = TimeSeriesFunction.HHV(df['high'], 20)
# HHVBARS - 求上一高点到当前的周期数
result = TimeSeriesFunction.HHVBARS(df['high'], 20)
# HOD - 求高值名次
result = TimeSeriesFunction.HOD(df['high'], 20)
# LLV - 求N周期内最低值
result = TimeSeriesFunction.LLV(df['low'], 20)
# LLVBARS - 求上一低点到当前的周期数
result = TimeSeriesFunction.LLVBARS(df['low'], 20)
# LOD - 求低值名次
result = TimeSeriesFunction.LOD(df['low'], 20)
# HHVLLV - 阶段最高最低值
result = TimeSeriesFunction.HHVLLV(df['high'], 0, 20, 5)
# ========== 引用函数 ==========
# REVERSE - 求相反数
result = TimeSeriesFunction.REVERSE(df['close'])
# REF - 引用若干周期前的数据
result = TimeSeriesFunction.REF(df['close'], 1)
# REFX - 引用若干周期后的数据(未来函数)
result = TimeSeriesFunction.REFX(df['close'], 1)
# REFV - 引用若干周期前的数据(平滑处理)
result = TimeSeriesFunction.REFV(df['close'], 1)
# REFXV - 引用若干周期后的数据(平滑处理)(未来函数)
result = TimeSeriesFunction.REFXV(df['close'], 1)
# SHIFT - 获取N个交易日前的值
result = TimeSeriesFunction.SHIFT(df['close'], 5)
# ========== 累计函数 ==========
# SUM - 求总和
result = TimeSeriesFunction.SUM(df['volume'], 20)
# MULAR - 求累乘
result = TimeSeriesFunction.MULAR(df['close'] /
TimeSeriesFunction.REF(df['close'], 1), 5)
# SUMBARS - 向前累加到指定值到现在的周期数
result = TimeSeriesFunction.SUMBARS(df['volume'], 1000000000)
# SUMBARSX - 向前累加到指定值到现在的周期数(未达到返回nan)
result = TimeSeriesFunction.SUMBARSX(df['volume'], 1000000000)
# CUMSUM - 累计求和
result = TimeSeriesFunction.CUMSUM(df['volume'])
# ========== 移动平均函数 ==========
# MA - 简单移动平均
result = TimeSeriesFunction.MA(df['close'], 20)
# SMA - 移动平均
result = TimeSeriesFunction.SMA(df['close'], 20, 1)
# TMA - 移动平均
result = TimeSeriesFunction.TMA(df['close'], 0.9, 0.1)
# MEMA - 平滑移动平均
result = TimeSeriesFunction.MEMA(df['close'], 20)
# EMA - 指数移动平均
result = TimeSeriesFunction.EMA(df['close'], 20)
# EXPMEMA - 指数平滑移动平均
result = TimeSeriesFunction.EXPMEMA(df['close'], 20)
# WMA - 加权移动平均
result = TimeSeriesFunction.WMA(df['close'], 20)
# DMA - 动态移动平均
alpha = df['volume'] / df['volume'].rolling(20).sum()
result = TimeSeriesFunction.DMA(df['close'], alpha)
# AMA - 自适应均线值
result = TimeSeriesFunction.AMA(df['close'], alpha)
# ========== 信号过滤函数 ==========
# FILTER - 过滤连续出现的信号
result = TimeSeriesFunction.FILTER(condition, 5)
# FILTERX - 反向过滤连续出现的信号
result = TimeSeriesFunction.FILTERX(condition, 5)
# ========== 条件判断函数 ==========
# TR - 求真实波幅
result = TimeSeriesFunction.TR(df['high'], df['low'], df['close'])
# RANGE - 范围判断
result = TimeSeriesFunction.RANGE(df['close'], df['low'], df['high'])
# CROSS - 两条线交叉
ma5 = TimeSeriesFunction.MA(df['close'], 5)
ma10 = TimeSeriesFunction.MA(df['close'], 10)
result = TimeSeriesFunction.CROSS(ma5, ma10)
# LONGCROSS - 两条线维持一定周期后交叉
result = TimeSeriesFunction.LONGCROSS(ma5, ma10, 5)
# UPNDAY - 返回周期数内是否连涨
result = TimeSeriesFunction.UPNDAY(df['close'], 3)
# DOWNNDAY - 返回周期数内是否连跌
result = TimeSeriesFunction.DOWNNDAY(df['close'], 3)
# NDAY - 返回是否持续存在X>Y
result = TimeSeriesFunction.NDAY(df['close'], df['open'], 3)
# EXIST - 是否存在
result = TimeSeriesFunction.EXIST(condition, 10)
# EXISTR - 是否存在(前几日到前几日间)
result = TimeSeriesFunction.EXISTR(condition, 10, 5)
# EVERY - 一直存在
result = TimeSeriesFunction.EVERY(condition, 5)
# LAST - 持续存在
result = TimeSeriesFunction.LAST(condition, 10, 5)
# ========== 技术指标函数 ==========
# SAR - 抛物线转向指标
result = TimeSeriesFunction.SAR(df['high'], df['low'], df['close'], n=4,
step=0.02, max_af=0.2)



【量化算子篇】统计函数


一、统计函数列表 

AmazingData 最低版本号：1.0.30

统计函数用于计算时序数据的统计指标，如标准差、方差、相关系数等。

序号

函数名称

函数用法

1

AVEDEV

AVEDEV(X,N) 返回X在N周期内的平均绝对偏差

2

BETA

BETA(X,BENCHMARK,N) 返回当前证券N周期收益与对应大盘指数收益相比的贝塔系数,N支持变量

3

BETAEX

BETAEX(X,Y,N) 返回X与Y的N周期的相关放大系数,N支持变量

4

COVAR

COVAR(X,Y,N) 返回X和Y的N周期的协方差,N支持变量

5

DEVSQ

DEVSQ(X,N) 返回X在N周期内的数据偏差平方和

6

FORCAST

FORCAST(X,N) 返回X在N周期内的线性回归预测值,N支持变量

7

KURTOSIS

KURTOSIS(X,N) 计算指标在过去N个交易日的峰度

8

MEAN

MEAN(X,N) 计算指标在过去N个交易日的平均值

9

MEDIAN

MEDIAN(X,N) 计算指标在过去N个交易日的中位数

10

QUANTILE

QUANTILE(X,N,M) 计算指标在过去N个交易日排名M百分点对应的值

11

RELATE

RELATE(X,Y,N) 返回X和Y的N周期的相关系数,N支持变量

12

SKEW

SKEW(X,N) 计算指标在过去N个交易日的偏度

13

SLOPE

SLOPE(X,N) 返回X在N周期内的线性回归斜率,N支持变量

14

STD

STD(X,N) 返回X在N周期内的估算标准差,N支持变量

15

STDDEV

STDDEV(X,N) 返回X在N周期内的标准偏差

16

STDP

STDP(X,N) 返回X在N周期内的总体标准差,N支持变量

17

VAR

VAR(X,N) 返回X在N周期内的估算样本方差,N支持变量

18

VARP

VARP(X,N) 返回X在N周期内的总体样本方差,N支持变量



二、统计函数说明 

（1）AVEDEV(x: Series, n: int) 平均绝对偏差

用法: AVEDEV(X,N) 返回X在N周期内的平均绝对偏差

（2）BETA(x: Series, benchmark: Series, n: int)贝塔系数

用法: BETA(X,BENCHMARK,N) 返回当前证券N周期收益与对应大盘指数收益相比的贝塔系数,N支持变量

（3）BETAEX(x: Series, y: Series, n: int) 相关放大系数

用法: BETAEX(X,Y,N) 返回X与Y的N周期的相关放大系数,N支持变量

（4）COVAR(x: Series, y: Series, n: int) 协方差

用法: COVAR(X,Y,N) 返回X和Y的N周期的协方差,N支持变量

（5）DEVSQ(x: Series, n: int) 数据偏差平方和

用法: DEVSQ(X,N)返回X在N周期内的数据偏差平方和

（6）FORCAST(x: Series, n: int) 线性回归预测值

用法: FORCAST(X,N) 返回X在N周期内的线性回归预测值,N支持变量

（7）KURTOSIS(x: Series, n: int) 峰度

用法: KURTOSIS(X,N) 计算指标在过去N个交易日的峰度

（8）MEAN(x: Series, n: int) 平均值

用法: MEAN(X,N) 计算指标在过去N个交易日的平均值

（9）MEDIAN(x: Series, n: int) 中位数

用法: MEDIAN(X,N)计算指标在过去N个交易日的中位数

（10）QUANTILE(x: Series, n: int, m: float) 分位数

用法: QUANTILE(X,N,M) 计算指标在过去N个交易日排名M百分点对应的值

（11）RELATE(x: Series, y: Series, n: int) 相关系数

用法: RELATE(X,Y,N) 返回X和Y的N周期的相关系数,N支持变量

（12）SKEW(x: Series, n: int) 偏度

用法: SKEW(X,N) 计算指标在过去N个交易日的偏度

（13）SLOPE(x: Series, n: int) 线性回归斜率

用法: SLOPE(X,N)返回X在N周期内的线性回归斜率,N支持变量

（14）STD(x: Series, n: int) 估算标准差(样本标准差)

用法: STD(X,N) 返回X在N周期内的估算标准差,N支持变量

（15）STDDEV(x: Series, n: int) 标准偏差

用法: STDDEV(X,N) 返回X在N周期内的标准偏差

（16）STDP(x: Series, n: int) 总体标准差

用法: STDP(X,N) 返回X在N周期内的总体标准差,N支持变量

（17）VAR(x: Series, n: int) 估算样本方差

用法: VAR(X,N) 返回X在N周期内的估算样本方差,N支持变量

（18）VARP(x: Series, n: int) 总体样本方差

用法: VARP(X,N) 返回X在N周期内的总体样本方差,N支持变量



三、api案例

import AmazingData as ad

from AmazingData.operator.function import MathFunction

ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 

# 获取数据
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
market_data_object = ad.MarketData(calendar)
code = '00000*.SH'
kline_day = market_data_object.query_kline([code],
begin_date=20130101, end_date=20250722,
period=ad.constant.Period.day.value)
df = kline_day[code]
# AVEDEV - 平均绝对偏差
result = StatisticsFunction.AVEDEV(df['close'], 20)
# DEVSQ - 数据偏差平方和
result = StatisticsFunction.DEVSQ(df['close'], 20)
# FORCAST - 线性回归预测值
result = StatisticsFunction.FORCAST(df['close'], 20)
# SLOPE - 线性回归斜率
result = StatisticsFunction.SLOPE(df['close'], 20)
# STD - 估算标准差(样本标准差)
result = StatisticsFunction.STD(df['close'], 20)
# STDP - 总体标准差
result = StatisticsFunction.STDP(df['close'], 20)
# STDDEV - 标准偏差
result = StatisticsFunction.STDDEV(df['close'],
20)
# VAR - 估算样本方差
result = StatisticsFunction.VAR(df['close'], 20)
# VARP - 总体样本方差
result = StatisticsFunction.VARP(df['close'], 20)
# COVAR - 协方差
result = StatisticsFunction.COVAR(df['close'], df['open'], 20)
# RELATE - 相关系数
result = StatisticsFunction.RELATE(df['close'], df['open'], 20)
# BETA - 贝塔系数
result = StatisticsFunction.BETA(df['close'], df['open'], 20)
# BETAEX - 相关放大系数
result = StatisticsFunction.BETAEX(df['close'], df['open'], 20)
# KURTOSIS - 峰度
result = StatisticsFunction.KURTOSIS(df['close'], 20)
# SKEW - 偏度
result = StatisticsFunction.SKEW(df['close'], 20)
# MEAN - 平均值
result = StatisticsFunction.MEAN(df['close'], 20)
# MEDIAN - 中位数
result = StatisticsFunction.MEDIAN(df['close'], 20)
# QUANTILE - 分位数
result = StatisticsFunction.QUANTILE(df['close'], 20, 0.75)
图片


【AmazingData】获取期权标准合约属性数据


一、期权标准合约属性

函数接口：get_option_std_ctr_specs
功能描述：获取指定期权标准合约属性（沪深交易所的ETF期权）
输入参数：  

参数

 数据类型

必选

解释

code_list

list[str] 

是

支持沪深ETF的的代码列表，目前包含159919.SZ

159915.SZ

159922.SZ

159901.SZ

510300.SH

588000.SH

588080.SH

510050.SH

510500.SH

local_path

str 

是

本地存储数据的路径，需绝对路径，格式类似“

'D://AmazingData_local_data//'

”

is_local

bool

否

默认为True，本地数据缓存方案

输出参数：

参数

 数据类型

解释

option_std_ctr_specs

dataframe

column为option_std_ctr_specs的字段

index为序号（无意义）

二、api案例

# 第一步登录api

import AmazingData as ad

ad.login(username='username',   password='password',host='***.***.***.***',port=****)

info_data_object = ad.InfoData()

option_std_ctr_specs =info_data_object.get_option_std_ctr_specs(['510050.SH'], is_local=False)         
三、附录

option_std_ctr_specs的字段说明：

参数

 数据类型

字段说明

备注

EXERCISE_DATE

string

期权行权日

 

CONTRACT_UNIT

int

合约单位

 

POSITION_DECLARE_MIN

string

头寸申报下限

 

QUOTE_CURRENCY_UNIT

string

报价货币单位

 

LAST_TRADING_DATE

string

最后交易日

 

POSITION_LIMIT

string

头寸限制

 

DELIST_DATE

string

退市日期

 

NOTIONAL_VALUE

string

立约价值

 

EXERCISE_METHOD

string

行权方式

 

DELIVERY_METHOD

string

交割方式

 

SETTLEMENT_MONTH

string

合约结算月份

 

TRADING_FEE

string

交易费用

 

EXCHANGE_NAME

string

交易所名称

 

OPTION_EN_NAME

string

期权英文名称

 

CONTRACT_VALUE

float

合约价值

 

IS_SIMULATION

int

是否仿真合约

0 否 1 是

CONTRACT_UNIT_DIMENSION

string

合约单位量纲

 

OPTION_STRIKE_PRICE

string

期权行权价

 

IS_SIMULATION_TRADE

string

是否仿真交易

0 否 1 是

 

LISTED_DATE

string

上市日期

 

OPTION_NAME

string

期权名称

 

PREMIUM

string

期权金

 

OPTION_TYPE

string

期权类型

ETF期权等

TRADING_HOURS_DESC

string

交易时间说明

 

FINAL_SETTLEMENT_DATE

string

最后结算日

 

FINAL_SETTLEMENT_PRICE

string

最后结算价

 

MIN_PRICE_UNIT

string

最小报价单位

 

MARKET_CODE

string

市场代码

 

CONTRACT_MULTIPLIER

int

合约乘数

 

图片【AmazingData】获取月合约属性变动数据


一、期权月合约属性变动

函数接口：get_option_mon_ctr_specs

功能描述：获取指定期权月合约属性变动（沪深交易所的ETF期权）

输入参数：  

参数

 数据类型

必选

解释

code_list

list[str] 

是

支持沪深ETF期权的的代码列表，可见示例

local_path

str 

是

本地存储数据的路径，需绝对路径，格式类似“

'D://AmazingData_local_data//'

”

is_local

bool

否

默认为True，本地数据缓存方案

输出参数：

参数

 数据类型

解释

block_trading

dataframe

column为block_trading的字段

index为序号（无意义）

二、api案例

# 第一步登录api

import AmazingData as ad

ad.login(username='username',   password='password',host='***.***.***.***',port=****)

info_data_object = ad.InfoData()

base_data_object = ad.BaseData()

calendar = base_data_object.get_calendar()

today = calendar[-1]

code_list =   base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
   hist_code_list =   base_data_object.get_hist_code_list(security_type='EXTRA_ETF_OP'', start_date=20130101,                                                        end_date=today)

option_mon_ctr_specs =info_data_object.get_option_mon_ctr_specs(code_list, is_local=False)         
三、附录

option_mon_ctr_specs的字段说明：

参数

 数据类型

字段说明

CODE_OLD

string

原交易代码

CHANGE_DATE

string

调整日期

MARKET_CODE

string

市场代码

NAME_NEW

string

新合约简称

EXERCISE_PRICE_NEW

float

新行权价(元)

NAME_OLD

string

原合约简称

CODE_NEW

string

新交易代码

EXERCISE_PRICE_OLD

float

原行权价(元)

UNIT_OLD

float

原合约单位(股)

UNIT_NEW

float

新合约单位(股)

CHANGE_REASON

string

调整原因

图片


【AmazingData】获取期权基本资料数据


一、期权基本资料

函数接口：get_option_basic_info
功能描述：获取指定期权的基本资料（沪深交易所的ETF期权）

输入参数：  

参数

 数据类型

必选

解释

code_list

list[str] 

是

支持沪深ETF期权的的代码列表，可见示例

local_path

str 

是

本地存储数据的路径，需绝对路径，格式类似“

'D://AmazingData_local_data//'

”

is_local

bool

否

默认为True，本地数据缓存方案

输出参数：

参数

 数据类型

解释

option_basic_info

dataframe

column为option_basic_info的字段

index为序号（无意义）

二、api案例

# 第一步登录api

import AmazingData as ad

ad.login(username='username',   password='password',host='***.***.***.***',port=****)

info_data_object = ad.InfoData()

base_data_object = ad.BaseData()

calendar = base_data_object.get_calendar()

today = calendar[-1]

code_list =   base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
   hist_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_ETF_OP'', start_date=20130101,                                                        end_date=today)

option_basic_info =info_data_object.get_option_basic_info(code_list, is_local=False)         
三、附录

option_basic_info的字段说明：

参数

 数据类型

字段说明

备注

CONTRACT_FULL_NAME

string

合约全称

 

CONTRACT_TYPE

string

合约类别

C表示认购

P表示认沽

DELIVERY_MONTH

string

交割月份

 

EXPIRY_DATE

string

到期日

 

EXERCISE_PRICE

float

行权价格

 

EXERCISE_END_DATE

string

最后行权日

 

START_TRADE_DATE

string

开始交易日

 

LISTING_REF_PRICE

float

挂牌基准价

 

LAST_TRADE_DATE

string

最后交易日

 

EXCHANGE_CODE

string

合约交易所代码

 

DELIVERY_DATE

string

最后交割日

 

CONTRACT_UNIT

Int

合约单位

 

IS_TRADE

string

是否交易

 

EXCHANGE_SHORT_NAME

string

合约交易所简称

 

CONTRACT_ADJUST_FLAG

string

合约调整标志

 

MARKET_CODE

string

合约代码

 

图片


【AmazingData】期权历史行情数据查询



一、历史快照

函数接口：query_snapshot

功能描述：快照数据的历史数据查询接口

输入参数：

参数

 数据类型

必选

解释

code_list

list:[str]

是

可传入列表，支持上交所、深交所的ETF期权

begin_date

int

是

日期，填写8位的整型格式的日期，比如20240101

end_date

int

是

日期，填写8位的整型格式的日期，比如20240201

begin_time

int

否

时分秒毫秒的时间戳，填写8位或9位的整型格式的日期，时占一位或两位，分占两位，秒占两位，毫秒占三位，例如9点整

为90000000, 17点25分为172500000

end_time

int

否

时分秒毫秒的时间戳，填写8位或9位的整型格式的日期，时占一位或两位，分占两位，秒占两位，毫秒占三位，例如9点整

为90000000, 17点25分为172500000

输出参数：

参数

 数据类型

解释

snapshot_dict

dict

指字典的key：代码

字典的value：dataframe，

column为快照数据，

ETF期权为SnapshotOption（见附录）， 

index为日期（datetime）

 

# 第一步登录api
import AmazingData as ad
ad.login(username='username',   password='password',host='***.***.***.***',port=****)
base_data_object = ad.BaseData()
code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
calendar = base_data_object.get_calendar()
market_data_object=ad.MarketData(calendar)
snapshot_dict = market_data_object.query_snapshot(code_list, begin_date=20240530, end_date=20240530)   
二、历史K线
函数接口：query_kline

功能描述：K线数据的实时订阅回调函数，支持全部周期的K线数据查询

输入参数：

参数

 数据类型

必选

解释

code_list

list:[str]

是

可传入列表，支持上交所、深交所的ETF期权；

begin_date

int

是

日期，填写8位的整型格式的日期，比如20240101

end_date

int

是

日期，填写8位的整型格式的日期，比如20240201

period

Period

是

数据周期Period（见附录）

begin_time

int

否

时分的时间戳，填写3位或4位的整型格式的日期，时占一位或两位，分占两位，例如9点整

为900, 17点25分为1725

end_time

int

否

时分的时间戳，填写3位或4位的整型格式的日期，时占一位或两位，分占两位，例如9点整

为900, 17点25分为1725

输出参数：

参数

 数据类型

解释

kline_dict

dict

字典的key：代码

字典的value：dataframe，

column为K线数据Kline（见附录），

index为日期（datetime）



# 第一步登录api
import AmazingData as ad
ad.login(username='username',   password='password',host='***.***.***.***',port=****)
base_data_object = ad.BaseData()
code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
calendar = base_data_object.get_calendar()
market_data_object=ad.MarketData(calendar)
kline_dict = market_data_object.query_kline   (code_list, begin_date=20240530, end_date=20240530)    
三、附录 

（1）ETF期权快照SnapshotOption

数据类型

字段名称

说明

str

code

证券代码+市场

datetime

trade_time

交易所行情数据时间

str

trading_phase_code

交易阶段代码

int

total_long_position

总持仓量

float

volume

成交总量

float

amount

成交总金额

float

pre_close

昨收价

float

pre_settle:

上次结算价

float

auction_price

动态参考价（波动性中断参考价，仅上海有效），

int

auction_volume

虚拟匹配数量（仅上海有效）

float

last

最新价

float

open

开盘价

float

high

最高价

float

low

最低价

float

close

收盘价

float

settle

本次结算价

float

high_limited

涨停价

float

low_limited

跌停价

float

ask_price1

卖1档价格

float

ask_price2

卖2档价格

float

ask_price3

卖3档价格

float

ask_price4

卖4档价格

float

ask_price5

卖5档价格

int

ask _volume1

卖1档量

int

ask _volume2

卖2档量

int

ask _volume3

卖3档量

int

ask _volume4

卖4档量

int

ask _volume5

卖5档量

float

bid_price1

买1档价格

float

bid_price2

买2档价格

float

bid_price3

买3档价格

float

bid_price4

买4档价格

float

bid_price5

买5档价格

int

bid _volume1

买1档量

int

bid _volume2

买2档量

int

bid _volume3

买3档量

int

bid _volume4

买4档量

int

bid _volume5

买5档量

str

contract_type

合约类别

int

expire_date

到期日

str

underlying_security_cod

标的代码

float

exercise_price

行权价

（2）K线Kline

数据类型

字段名称

说明

str

code

证券代码+市场

datetime

trade_time

交易所行情数据时间

float

open

今开盘价

float

high

最高价

float

low

最低价

float

close

收盘价

int

volume

成交总量

float

amount

成交总金额







图片


【AmazingData】期权实时数据订阅


一、ETF期权实时快照
函数接口：onSnapshotoption

功能描述：港股通快照数据的实时订阅回调函数 

输入参数：入参需传入装饰器中SubscribeData.register  

参数

 数据类型

必选

解释

code_list

list:[str]

是

可传入列表，支持上交所、深交所的ETF期权

period

Period

是

Period.snapshotoption.value

输出参数：入参需传入装饰器中SubscribeData.register  

参数

 数据类型

解释

data

Object

ETF期权为SnapshotOption（见附录）

# 第一步登录api
import AmazingData as ad
ad.login(username='username',   password='password',host='***.***.***.***',port=****)
base_data_object = ad.BaseData()
option_code_list =   base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
# 实时订阅
sub_data = ad.SubscribeData()
@sub_data.register(code_list=option_code_list,   period=ad.constant.Period.snapshotoption.value)
def onSnapshotoption(data:   Union[ad.constant.SnapshotOption], period):
     print('onSnapshotoption: ', data)
sub_data.run()      
二、实时K线
函数接口：OnKLine

功能描述：K线数据的实时订阅回调函数 

输入参数：入参需传入装饰器中SubscribeData.register  

参数

 数据类型

必选

解释

code_list

list:[str]

是

可传入列表，支持上交所、深交所的ETF期权


period

Period

是

Period（见附录）

输出参数：入参需传入装饰器中SubscribeData.register  

参数

 数据类型

解释

data

Object

Kline（见附录）

# 第一步登录api
import AmazingData as ad
ad.login(username='username',   password='password',host='***.***.***.***',port=****)
base_data_object = ad.BaseData()
option_code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
# 实时订阅
sub_data = ad.SubscribeData()
# K线
@sub_data.register(code_list=option_code_list,   period=ad.constant.Period.min1.value)
def OnKLine(data: Union[ad.constant.Kline],   period):
 print('OnKLine: ', data)
sub_data.run()     
三、附录 

（1）ETF期权快照SnapshotOption

数据类型

字段名称

说明

str

code

证券代码+市场

datetime

trade_time

交易所行情数据时间

str

trading_phase_code

交易阶段代码

int

total_long_position

总持仓量

float

volume

成交总量

float

amount

成交总金额

float

pre_close

昨收价

float

pre_settle:

上次结算价

float

auction_price

动态参考价（波动性中断参考价，仅上海有效），

int

auction_volume

虚拟匹配数量（仅上海有效）

float

last

最新价

float

open

开盘价

float

high

最高价

float

low

最低价

float

close

收盘价

float

settle

本次结算价

float

high_limited

涨停价

float

low_limited

跌停价

float

ask_price1

卖1档价格

float

ask_price2

卖2档价格

float

ask_price3

卖3档价格

float

ask_price4

卖4档价格

float

ask_price5

卖5档价格

int

ask _volume1

卖1档量

int

ask _volume2

卖2档量

int

ask _volume3

卖3档量

int

ask _volume4

卖4档量

int

ask _volume5

卖5档量

float

bid_price1

买1档价格

float

bid_price2

买2档价格

float

bid_price3

买3档价格

float

bid_price4

买4档价格

float

bid_price5

买5档价格

int

bid _volume1

买1档量

int

bid _volume2

买2档量

int

bid _volume3

买3档量

int

bid _volume4

买4档量

int

bid _volume5

买5档量

str

contract_type

合约类别

int

expire_date

到期日

str

underlying_security_cod

标的代码

float

exercise_price

行权价

（2）K线Kline

数据类型

字段名称

说明

str

code

证券代码+市场

datetime

trade_time

交易所行情数据时间

float

open

今开盘价

float

high

最高价

float

low

最低价

float

close

收盘价

int

volume

成交总量

float

amount

成交总金额



图片


【AmazingData】获取期权代码列表


一、每日最新代码表（期权）

交易日早上9点前更新

函数接口：get_option_code_list

功能描述：获取代码表（每日最新），此接口无法获取历史代码表

输入参数：  

参数

 数据类型

必选

解释

security_type

list[str] 

是

代码类型security_type(期权交易所)
（见附录），默认为EXTRA_ETF_OP
（期权, 包含深交所/上交所）
输出参数：  

返回值

 数据类型

解释

code_list

list[str]

证券代码



二、历史代码表

函数接口：BaseData的get_hist_code_list

功能描述：获取历史代码表，先检查本地数据，再从服务端补充，最后返回数据输入参数：  

输入参数：  

参数

 数据类型

必选

解释

security_type

str 

是

默认为

"EXTRA_STOCK_A_SH_SZ"沪深A股，支持附录security_type
(沪深北)、
security_type(期货交易所)和security_type(期权)


start_date

int 

是

开始时间，闭区间

end_date

int 

是

结束时间，闭区间

local_path

str

是

本地存储数据的路径，需绝对路径，格式类似“

'D://AmazingData_local_data//'”

输出参数：  

返回值

数据类型

解释

code_list

list[str]

证券代码

三、附录security_type(期权)

数据类型

枚举值

说明

str

EXTRA_ETF_OP

ETF期权, 上交所/深交所

str

SH_OPTION

ETF期货, 包含上交所

str

SZ_OPTION

ETF期货, 包含深交所



四. api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
base_data_object = ad.BaseData()
#每日最新代码表
code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP')
# 历史代码表
code_list = base_data_object.get_hist_code_list(security_type='EXTRA_ETF_OP',start_date=20240101, end_date=20240701, local_path=local_path) 
图片

【AmazingData】获取期货K线数据
图片
本文将介绍历史期货K线行情历史数据的查询方法。

一、历史行情查询接口使用步骤

（1） 实例化AmazingData的MarketData，入参需交易日历

（2） 调用MarketData的方法获取数据

二、K线数据订阅

函数接口：query_kline

功能描述：历史K线数据的查询函数 ，支持全部周期的K线数据查询
输入参数：
参数	 数据类型	必选	解释
code_list	list:[str]	是	可传入列表，
支持中金所/上期所/大商所/郑商所/上海国际能源交易中心所
begin_date	in
t
是	日期，填写8位的整型格式的日期，比如20240101
end_date	int

是	日期，填写8位的整型格式的日期，比如20240201
period	Period	是	
数据周期Period（见附录）

填写除Period.snapshot.value外的Period所有value
输出参数：

回调返回值	 数据类型	解释
kline_dict	dict	
字典的key：代码

字典的value：dataframe，

column为K线数据Kline（见附录），

index为日期（datetime）



import AmazingData as ad
ad.login(username='username', password='password', host='***.***.***.***', port=****) 
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
code_list = base_data_object.get_code_list(security_type='EXTRA_FUTURE')
market_data_object = ad.MarketData(calendar)
snapshot_dict = market_data_object.query_kline(code_list, begin_date=20240530, end_date=20240530)
三、附录
当查询非中金所（大商所、郑商所、上期所、上期能源）的商品期货快照时，因涉及夜盘快照，需根据查询时间参数做相应区分，查询上以 20:00 作为夜盘的分割时间点，处理逻辑见下表。

归属T-1日范围20:00:00.000~23:59:59.999

归属T日范围：00:00:00.000~19:59:59.999


开始时间

结束时间

系统响应逻辑

20220407

093000000

150000000

开始、结束时间均归属T日，且开始时间<结束时间，为有效查询，返回[4月7日9:30, 4月7日15:00]的数据

20220407

200000000

235900000

开始、结束时间均归属T-1日，且开始时间<结束时间，为有效查询，返回[4月6日20:00, 4月6日23:59]的数据

20220407

200000000

010000000

开始时间归属T-1日，结束时间归属T日，为有效查询，返回[4月6日20:00,4月7日01:00]的数据

正常周一（未跨法定假节日）

235959999

030000000

开始时间归属T-1日，结束时间归属T日，为有效查询，返回[周五23:59:59.999,周一03:00]的数据，需包括周末的数据（部分品种周六0点~02:30 会有行情）

特殊日（跨法定假节日）

200000000

010000000

开始时间归属T-1日，结束时间归属T日，为有效查询，返回[T-1日20:00,T日01:00]的数据

20220407

230000000

200000000

开始、结束时间均归属T-1日，但开始时间>结束时间，为无效查询，无数据返回，并需弹出相应告警

20220407

030000000

010000000

开始、结束时间均归属T日，但开始时间>结束时间，为无效查询，无数据返回，并需弹出相应告警

20220407

030000000

230000000

开始时间归属T日，结束时间归属T-1日，为无效查询，无数据返回，并需弹出相应告警



图片

2025，出发了！为你的AI装上天线——星耀数智WealthManage


《0代码，亦量化！开源金融智能体WealthManager》已经介绍了星耀数智的mcp服务，本文介绍tools的目录

一、mcp服务的配置

1. 类型：

选择: "stdio",

2. 命令

写出本地python路径：

例如："D:\\\\ProgramData\\\\anaconda312\\\\python",

3.参数

例如："D:\\\\WealthManager\\\\WealthManager\\\\mcp_server\\\\server.py "

4.环境变量

设置星耀数智的账号信息：

（1）user   用户名

（2）password   密码

（3）host  ip地址

（4）port 端口号 

配置demo如下      

图片
二、tools的目录

1. mcp_kzz
描述: 查询可转债代码表，并返回可转债的数量。

返回: int - 可转债数量。

2. mcp_code_info
描述: 获取每日最新证券信息，交易日早上9点前更新当日最新证券信息。

参数:

security_type (str): 证券类型枚举，如 EXTRA_STOCK_A, SH_A, SZ_A, BJ_A 等。

返回: dict - 证券信息字典。

3. mcp_code_list
描述: 获取代码表（每日最新），此接口无法获取历史代码表。

参数:

security_type (str): 证券类型枚举，同上。

返回: list - 证券代码列表。

4. mcp_backward_factor
描述: 获取复权因子数据并本地存储，复权因子为根据交易所行情数据计算得出的后复权因子。

参数:

code_list (list, 可选): 股票代码列表，默认为空列表。

返回: dict - 复权因子数据。

5. mcp_calendar
描述: 查询交易日历。

参数:

market (str, 可选): 市场枚举，如 SH, SZ, BJ 等，默认为 SH。

date (int, 可选): 日期，格式为 YYYYMMDD，默认为当前日期。

返回: list - 交易日历列表。

6. mcp_stock_basic
描述: 获取指定股票列表的上市公司的证券基础数据。

参数:

code_list (list, 可选): 股票代码列表，默认为空列表。

返回: pd.DataFrame - 证券基础数据。

7. mcp_history_stock_status
描述: 获取指定股票列表的上市公司的历史证券数据，以日度为频率。

参数:

code_list (list, 可选): 股票代码列表，默认为空列表。

begin_date (int, 可选): 开始日期，格式为 YYYYMMDD。

end_date (int, 可选): 结束日期，格式为 YYYYMMDD。

返回: pd.DataFrame - 历史证券数据。

8. mcp_bj_code_mapping
描述: 获取北交所的存量上市公司股票新旧代码对照表。

返回: pd.DataFrame - 新旧代码对照表。

9. mcp_kline
描述: 查询股票的K线数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期，格式为 YYYYMMDD，默认为当前日期。

end_date (int, 可选): 结束日期，格式为 YYYYMMDD，默认为当前日期。

begin_time (int, 可选): 开始时间，格式为 HHMM，默认为 900。

end_time (int, 可选): 结束时间，格式为 HHMM，默认为 1700。

period (str, 可选): 数据周期，如 ad.constant.Period.day.value，默认为日线。

返回: dict - K线数据字典，key为股票代码，value为DataFrame。

10. mcp_snapshot
描述: 查询股票最新的快照数据。

参数:

code_list (list): 股票代码列表。

返回: dict - 快照数据字典，key为股票代码，value为DataFrame。

11. mcp_balance_sheet
描述: 获取指定股票列表的上市公司的资产负债表数据。

参数:

code_list (list): 股票代码列表。

statement_type (str, 可选): 报表类型，如 1 表示合并报表。

report_type (str, 可选): 报告期名称，如 1 表示3月。

begin_date (int, 可选): 开始日期，格式为 YYYYMMDD。

end_date (int, 可选): 结束日期，格式为 YYYYMMDD。

返回: dict - 资产负债表数据字典。

12. mcp_cash_flow
描述: 获取指定股票列表的上市公司的现金流量表数据。

参数: 同 mcp_balance_sheet。

返回: dict - 现金流量表数据字典。

13. mcp_income
描述: 获取指定股票列表的上市公司的利润表数据。

参数: 同 mcp_balance_sheet。

返回: dict - 利润表数据字典。

14. mcp_profit_express
描述: 获取指定股票列表的上市公司的业绩快报数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 业绩快报数据字典。

15. mcp_profit_notice
描述: 获取指定股票列表的上市公司的业绩预告数据。

参数:

code_list (list): 股票代码列表。

report_type (str, 可选): 报告期名称。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 业绩预告数据字典。

16. mcp_share_holder
描述: 获取指定股票列表的上市公司的十大股东数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 十大股东数据字典。

17. mcp_holder_num
描述: 获取指定股票列表的上市公司的股东户数数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 股东户数数据字典。

18. mcp_equity_structure
描述: 获取指定股票列表的上市公司的股本结构数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 股本结构数据字典。

19. mcp_equity_pledge_freeze
描述: 获取指定股票列表的上市公司的股权冻结/质押数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 股权冻结/质押数据字典。

20. mcp_equity_restricted
描述: 获取指定股票列表的上市公司的限售股解禁数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 限售股解禁数据字典。

21. mcp_dividend
描述: 获取指定股票列表的上市公司的分红数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 分红数据字典。

22. mcp_right_issue
描述: 获取指定股票列表的上市公司的配股数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 配股数据字典。

23. mcp_margin_summary
描述: 获取指定日期的上市公司的融资融券成交汇总数据。

参数:

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 融资融券成交汇总数据字典。

24. mcp_margin_detail
描述: 获取指定股票列表的上市公司的融资融券交易明细数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 融资融券交易明细数据字典。

25. mcp_holder_num (龙虎榜数据)
描述: 获取指定股票列表的上市公司的龙虎榜数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 龙虎榜数据字典。

26. mcp_block_trading
描述: 获取指定股票列表的上市公司的股东户数数据。

参数:

code_list (list): 股票代码列表。

begin_date (int, 可选): 开始日期。

end_date (int, 可选): 结束日期。

返回: dict - 股东户数数据字典。



三、开源地址

https://cloud.chinastock.com.cn/p/DWqgFyEQx2IYwJECIAA

图片

【AmazingData】历史期货快照行情数据查询
图片

本文将介绍期货快照行情历史数据的查询方法。

一、历史行情查询接口使用步骤

（1） 实例化AmazingData的MarketData，入参需交易日历

（2） 调用MarketData的方法获取数据

二、期货快照数据查询

函数接口：query_snapshot

功能描述：期货快照数据的历史数据查询接口
输入参数：
参数	 数据类型	必选	解释
code_list	list:[str]	是	可传入列表，支持中金所/上期所/大商所/郑商所/上海国际能源交易中心所  
begin_date	in
t
是	日期，填写8位的整型格式的日期，比如20240101
end_date	int

是	日期，填写8位的整型格式的日期，比如20240201
输出参数：

返回值	 数据类型	解释
snapshot_dict	dict	
指字典的key：代码

字典的value：dataframe，

column为快照数据（指数为SnapshotIndex（见附录），股票、ETF为Snapshot（见附录）

），

index为日期（datetime）

三、附录
期货快照SnapshotFuture     

数据类型

字段名称

说明

str

code

证券代码+市场

datetime

trade_time

交易所行情数据时间

str

action_day

业务日期

str

trading_day

交易日期

float

pre_close

昨收价

float

pre_settle:

上次结算价

int

pre_open_interest

昨持仓量

int

open_interest

持仓量

float

last

最新价

float

open

开盘价

float

high

最高价

float

low

最低价

float

close

收盘价

float

volume

成交总量

float

amount

成交总金额

float

high_limited

涨停价

float

low_limited

跌停价

float

ask_price1

卖1档价格

float

ask_price2

卖2档价格

float

ask_price3

卖3档价格

float

ask_price4

卖4档价格

float

ask_price5

卖5档价格

int

ask _volume1

卖1档量

int

ask _volume2

卖2档量

int

ask _volume3

卖3档量

int

ask _volume4

卖4档量

int

ask _volume5

卖5档量

float

bid_price1

买1档价格

float

bid_price2

买2档价格

float

bid_price3

买3档价格

float

bid_price4

买4档价格

float

bid_price5

买5档价格

int

bid _volume1

买1档量

int

bid _volume2

买2档量

int

bid _volume3

买3档量

int

bid _volume4

买4档量

int

bid _volume5

买5档量

float

average_price

当日均价

float

settle

本次结算价





图片
【AmazingData】实时期货快照行情订阅
图片
一、实时行情订阅接口使用步骤

（1） 实例化AmazingData的SubscribeData

（2） 回调函数的装饰器传入code_list(代码表)和period(数据周期)两个参数

（3） 回调函数中获取数据

二、Level-1快照数据订阅

函数接口：onSnapshotfuture

功能描述：期货快照数据的实时订阅回调函数 
输入参数：
入参需传入装饰器中SubscribeData.register  
参数	 数据类型	必选	解释
code_list	list:[str]	是	可传入列表，支持中金所/上期所/大商所/郑商所/上海国际能源交易中心所 
period	Period	是	Period.snapshotfuture.value
输出参数：
回调返回值	 数据类型	必选	解释
data	Object	是	
SnapshotFuture（见附录）



import AmazingData as ad
ad.login(username='username', password='password', host='***.***.***.***', port=****) 
base_data_object = ad.BaseData()
code_list = base_data_object.get_code_list(security_type='EXTRA__FUTURE')
# 实时订阅
sub_data = ad.SubscribeData()
@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshotfuture.value)
def onSnapshotfuture (data: Union[ad.constant.SnapshotFuture], period):    
    print(period, data) 
sub_data.run()  
三、附录
期货快照SnapshotFuture     

数据类型

字段名称

说明

str

code

证券代码+市场

datetime

trade_time

交易所行情数据时间

str

action_day

业务日期

str

trading_day

交易日期

float

pre_close

昨收价

float

pre_settle:

上次结算价

int

pre_open_interest

昨持仓量

int

open_interest

持仓量

float

last

最新价

float

open

开盘价

float

high

最高价

float

low

最低价

float

close

收盘价

float

volume

成交总量

float

amount

成交总金额

float

high_limited

涨停价

float

low_limited

跌停价

float

ask_price1

卖1档价格

float

ask_price2

卖2档价格

float

ask_price3

卖3档价格

float

ask_price4

卖4档价格

float

ask_price5

卖5档价格

int

ask _volume1

卖1档量

int

ask _volume2

卖2档量

int

ask _volume3

卖3档量

int

ask _volume4

卖4档量

int

ask _volume5

卖5档量

float

bid_price1

买1档价格

float

bid_price2

买2档价格

float

bid_price3

买3档价格

float

bid_price4

买4档价格

float

bid_price5

买5档价格

int

bid _volume1

买1档量

int

bid _volume2

买2档量

int

bid _volume3

买3档量

int

bid _volume4

买4档量

int

bid _volume5

买5档量

float

average_price

当日均价

float

settle

本次结算价

图片

【AmazingData】获取证券基础信息数据


一、获取证券基础信息

函数接口：get_stock_basic

功能描述：获取指定股票列表的上市公司的证券基础数据，包含沪深北三个交易所，所有股票（含退市）的中英文名称、上市日期、退市日期、上市板块等信息

输入参数：  

参数

 数据类型

必选

解释

code_list

list[str] 

是

支持沪深北三个交易所的代码列表

输出参数：  

返回值

 数据类型

解释

stock_basic

dataframe

column为stock_basic的字段

index为序号（无意义）

stock_basic的字段说明：

参数

数据类型

解释

备注

MARKET_CODE

str

证券代码

 

SECURITY_NAME

str

证券简称

 

COMP_NAME

str

证券中文名称

 

PINYIN

str

中文拼音简称

 

COMP_NAME_ENG

str

证券英文名称

 

LISTDATE

int

上市日期

 

DELISTDATE

int

退市日期

 

LISTPLATE_NAME

str

上市板块名称

 

COMP_ID

str

公司代码

 

COMP_SNAME_ENG

str

英文名称缩写

 

IS_LISTED

int

上市状态

1：上市交易

3：终止上市



二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
stock_basic = info_data_object.get_stock_basic (all_code_list)      


图片




【AmazingData】获取期货代码列表


一、每日最新代码表（期货交易所）

交易日早上9点前更新

函数接口：get_future_code_list

功能描述：获取代码表（每日最新），此接口无法获取历史代码表

输入参数：  

参数

 数据类型

必选

解释

security_type

list[str] 

是

代码类型security_type(期货交易所)
（见附录），默认为EXTRA_FUTURE（期货, 包含中金所/上期所/大商所/郑商所/上海国际能源交易中心所）
输出参数：  

返回值

 数据类型

解释

code_list

list[str]

证券代码



二、历史代码表

函数接口：BaseData的get_hist_code_list

功能描述：获取历史代码表，先检查本地数据，再从服务端补充，最后返回数据输入参数：  

输入参数：  

参数

 数据类型

必选

解释

security_type

str 

是

默认为

"EXTRA_STOCK_A_SH_SZ"沪深A股，支持附录security_type
(沪深北)和security_type(期货交易所)


start_date

int 

是

开始时间，闭区间

end_date

int 

是

结束时间，闭区间

local_path

str

是

本地存储数据的路径，需绝对路径，格式类似“

'D://AmazingData_local_data//'”

输出参数：  

返回值

数据类型

解释

code_list

list[str]

证券代码

三、附录security_type(期货交易所)

数据类型

枚举值

说明

str

EXTRA_FUTURE

期货, 包含中金所/上期所/大商所/郑商所/上海国际能源交易中心所

str

ZJ_FUTURE

期货, 包含中金所

str

SQ_FUTURE

期货, 包含上期所

str

DS_FUTURE

期货, 包含大商所

str

ZS_FUTURE

期货, 包含郑商所

str

SN_FUTURE

期货, 包含海国际能源交易中心所

四、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
base_data_object = ad.BaseData()
#每日最新代码表
code_list = base_data_object.get_future_code_list(security_type='EXTRA_FUTURE')
# 历史代码表
code_list = base_data_object.get_hist_code_list(security_type='EXTRA_FUTURE',start_date=20240101, end_date=20240701, local_path=local_path) 


图片

【量化算子篇】数学函数


一、数学函数列表 

AmazingData 最低版本号：1.0.3

序号
函数名称
函数用法
1
MAX

n个参数中的最大值

2
MIN

n个参数中的最小值

3
ACOS

反余弦值

4
ASIN

反正弦值

5
ATAN

反正切值

6
COS

余弦值

7
SIN

正弦值

8
TAN

正切值

9
EXP

X次幂

10
LN

自然对数

11
LOG

10为底的对数

12
SQRT

开平方

13
ABS

绝对值

14
POW

乘幂

15
CEILING

向上舍入

16
FLOOR

向下舍入

17
INTPART

取整数部分

18
BETWEEN

介于

19
FRACPART

取小数部分

20
ROUND

四舍五入

21
SIGN

取符号

22
MOD

取模

23
RAND

取随机数

二、数据函数说明 

（1）MAX(*args: pd.Series) 求n个参数中的最大值

        用法: MAX(A,B,C,D,E,F,...)返回较大值

        输入参数 *args: 1至多个  pd.Series

        输出结果: pd.Series

（2）MIN(*args: pd.Series)  求n个参数中的最小值

        用法: MIN(A,B,C,D,E,F,...)返回较大值        

        输入参数 *args: 1至多个 为 pd.Series

        输出结果: pd.Series

（3）ACOS(x: pd.Series)  反余弦值

        用法: ACOS(X)返回X的反余弦值

        输入参数 x: pd.Series

        输出结果: pd.Series       

 （4）ASIN(x: pd.Series) 反正弦值

        用法: ASIN(X)返回X的反正弦值        

        输入参数 x: pd.Series

        输出结果: pd.Series

（5）ATAN(x: pd.Series)  反正切值

        用法: ATAN(X)返回X的反正切值

        输入参数 x:  pd.Series

        输出结果: pd.Series

（6）COS(x: pd.Series)  余弦值

        用法: COS(X)返回X的余弦值

        输入参数 x: 输入为 pd.Series

        输出结果: pd.Series

（7）SIN(x: pd.Series) 正弦值

        用法: SIN(X)返回X的正弦值

        输入参数 x:  pd.Series

        输出结果: pd.Series

（8）TAN(x: pd.Series) 正切值

        用法: TAN(X)返回X的正切值

        输入参数 x:  pd.Series

        输出结果: pd.Series

（9）EXP(x: pd.Series) X次幂

        用法: EXP(X)返回e的 X次幂

        输入参数 x:  pd.Series

        输出结果: pd.Series

 （10） LN(x: pd.Series) n自然对数

        用法: LN(X)以e为底的对数

        输入参数 x:  pd.Series

        输出结果: pd.Series

（11） LOG(x: pd.Series) 10为底的对数

        用法: LOG(X)以10为底的对数

        输入参数 x:  pd.Series

        输出结果: pd.Series

（12）SQRT(x: pd.Series) 开平方

        用法:  SQRT(X)为X的平方根

        输入参数 x:  pd.Series

        输出结果: pd.Series

（13）ABS(x: pd.Series) 绝对值

        用法:  ABS(X)为X的平方根

        输入参数 x:  pd.Series

        输出结果: pd.Series

（14）POW(a: pd.Series, b: pd.Series) 乘幂

        用法:  POW(A,B)返回A的B次幂

        输入参数 a:  pd.Series

        输入参数 b:  pd.Series

        输出结果: pd.Series

（15）CEILING(x: pd.Series) 向上舍入

        用法:  CEILING(x)沿A数值增大方向最接近的整数

        输入参数 x:  pd.Series

        输出结果: pd.Series

（16）FLOOR(x: pd.Series) 向下舍入

        用法:  FLOOR(x)沿A数值减小方向最接近的整数

        输入参数 x:  pd.Series

        输出结果: pd.Series

（17）INTPART(x: pd.Series) 取整

        用法:  INTPART(x)沿A绝对值减小方向最接近的整数

        输入参数 x:  pd.Series

        输出结果: pd.Series

（18）BETWEEN(a: pd.Series, b: pd.Series, c: pd.Series ) 介于

        用法:  BETWEEN(A,B,C)表示A处于B和C之间时返回1(B<=A<=C或C<=A<=B),否则返回0

        输入参数 a:  pd.Series

        输入参数 b:  pd.Series

        输入参数 c:  pd.Series

        输出结果: pd.Series

（19）FRACPART(x: pd.Series) 小数部分

        用法:  FRACPART(X),返回X的小数部分

        输入参数 x:  pd.Series

        输出结果: pd.Series

（20）ROUND(x: pd.Series, n: int)  四舍五入

        用法:  ROUND(X,N),返回X四舍五入到N位小数的数值

        输入参数 x:  pd.Series

        输入参数 n:  int

        输出结果: pd.Series

（21）SIGN(x: pd.Series)  取符号

        用法:  SIGN(X),返回X的符号.当X>0,X=0,X<0分别返回1,0,-1

        输入参数 x:  pd.Series

        输出结果: pd.Series

（22） MOD(x: pd.Series, n: int) 取模

        用法:  MOD(M,N),返回M关于N的模(M除以N的余数)

        输入参数 x:  pd.Series

        输入参数 n:  int

        输出结果: pd.Series

（23）RAND(a: int, b: int)  取随机数

        用法:  RAND(a,b),返回一个范围在[a, b]的随机整数

        输入参数 a:  int

        输入参数 b:  int

        输出结果: pd.Series

三、api案例

import AmazingData as ad

from AmazingData.operator.function import MathFunction

ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 

base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
market_data_object = ad.MarketData(calendar)
code = '000001.SH'
kline_day = market_data_object.query_kline([code], begin_date=20130101, end_date=20250722,
                                           period=ad.constant.Period.day.value)
df = kline_day[code]
# 数学函数
result1 = MathFunction.MAX(df['close'], df['open'])
result2 = MathFunction.MIN(df['close'], df['open'])
result3 = MathFunction.ACOS(df['open'])
result4 = MathFunction.ASIN(df['close'])
result5 = MathFunction.ATAN(df['close'])
result6 = MathFunction.COS(df['close'])
result7 = MathFunction.SIN(df['close'])
result8 = MathFunction.TAN(df['close'])
result9 = MathFunction.EXP(df['close'])
result10 = MathFunction.LN(df['close'])
result11 = MathFunction.LOG(df['close'])
result12 = MathFunction.SQRT(df['close'])
result13 = MathFunction.ABS(df['close'])
result14 = MathFunction.POW(df['close'], df['open'])
result15 = MathFunction.CEILING(df['close'])
result16 = MathFunction.FLOOR(df['close'])
result17 = MathFunction.INTPART(df['close'])
result18 = MathFunction.BETWEEN(df['close'], df['open'], df['low'])
result19 = MathFunction.FRACPART(df['close'])
result20 = MathFunction.ROUND(df['close'], 2)
result21 = MathFunction.SIGN(df['close'])
result22 = MathFunction.MOD(df['close'], 2)
result23 = MathFunction.RAND(-50, 20)



图片

【AmazingData】获取上市公司分红数据


一、获取上市公司分红数据

函数接口：get_dividend

功能描述：获取上市公司分红数据

AmazingData最低版本：0.0.23

输入参数：  

参数	 数据类型	必选	解释
code_list
list[str] 
是
支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
dividend

dataframe
column为分红数据的的字段

index为序号（无意义）

dividend的字段说明：

字段名称

类型

字段说明

备注

MARKET_CODE

string

市场代码

 

DIV_PROGRESS

string

方案进度

参看股票分红进度代码表

DVD_PER_SHARE_STK

float

每股送转

 

DVD_PER_SHARE_PRE_TAX_CASH

float

每股派息(税前)(元)

 

DVD_PER_SHARE_AFTER_TAX_CASH

float

每股派息(税后)(元)

 

DATE_EQY_RECORD

string

股权登记日

 

DATE_EX

string

除权除息日

 

DATE_DVD_PAYOUT

string

派息日

 

LISTINGDATE_OF_DVD_SHR

string

红股上市日

 

DIV_PRELANDATE

string

预案公告日

董事会预案公告日期

DIV_SMTGDATE

string

股东大会公告日

 

DATE_DVD_ANN

string

分红实施公告日

 

DIV_BASEDATE

string

基准日期

 

DIV_BASESHARE

float

基准股本(万股)

 

CURRENCY_CODE

string

货币代码

 

ANN_DATE

string

最新公告日期

 

IS_CHANGED

int

方案是否变更

1：有变更过0：未变更

REPORT_PERIOD

string

分红年度

 

DIV_CHANGE

string

方案变更说明

 

DIV_BONUSRATE

float

每股送股比例

 

DIV_CONVERSEDRATE

float

每股转增比例

 

REMARK

string

备注

 

DIV_PREANN_DATE

string

预案预披露公告日

股东提议的公告日期

DIV_TARGET

string

分红对象

 

股票分红进度代码表

分红进度描述

进度代码

董事会预案

1

股东大会通过

2

实施

3

未通过

4

停止实施

12

股东提议

17

董事会预案预披露

19

 分红实施进程：股东提议--董事会预案--股东大会--实施

二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
dividend= info_data_object.get_dividend(all_code_list)


图片



【AmazingData】获取上市公司配股数据


一、获取上市公司配股

函数接口：get_right_issue

功能描述：获取上市公司配股数据

AmazingData最低版本：0.0.23

输入参数：  

参数	 数据类型	必选	解释
code_list
list[str] 
是
支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
right_issue

dataframe
column为配股数据的的字段

index为序号（无意义）


right_issue的字段说明：

字段名称

类型

字段说明

备注

MARKET_CODE

string

市场代码

 

PROGRESS

int32

方案进度

参看股票配股进度代码表

PRICE

double

配股价格(元)

 

RATIO

double

配股比例

 

AMT_PLAN

double

配股计划数量(万股)

 

AMT_REAL

double

配股实际数量(万股)

 

COLLECTION_FUND

double

募集资金(元)

 

SHAREB_REG_DATE

string

股权登记日

 

EX_DIVIDEND_DATE

string

除权日

 

LISTED_DATE

string

配股上市日

 

PAY_START_DATE

string

缴款起始日

 

PAY_END_DATE

string

缴款终止日

 

PREPLAN_DATE

string

预案公告日

 

SMTG_ANN_DATE

string

股东大会公告日

 

PASS_DATE

string

发审委通过公告日

 

APPROVED_DATE

string

证监会核准公告日

 

EXECUTE_DATE

string

配股实施公告日

 

RESULT_DATE

string

配股结果公告日

 

LIST_ANN_DATE

string

上市公告日

 

GUARANTOR

string

基准年度

 

GUARTYPE

double

基准股本(万股)

 

RIGHTSISSUE_CODE

string

配售代码

 

ANN_DATE

string

最新公告日期

 

RIGHTSISSUE_YEAR

string

配股年度

 

RIGHTSISSUE_DESC

string

配股说明

 

RIGHTSISSUE_NAME

string

配股简称

 

RATIO_DENOMINATOR

double

配股比例分母

 

RATIO_MOLECULAR

double

配股比例分子

 

SUBS_METHOD

string

认购方式

 

EXPECTED_FUND_RAISING

double

预计募集资金(元)

 



股票配股进度代码表
配股进度描述

进度代码

董事会预案

1

股东大会通过

2

实施

3

未通过

4

证监会核准

5

达成转让意向

6

签署转让协议

7

国资委批准

8

商务部批准

9

过户

10

延期实施

11

停止实施

12

分红方案待定

13

传闻

14

证监会受理

15

传闻被否认

16

股东提议

17

保监会批复

18

董事会预案预披露

19

发审委通过

20

发审委未通过

21

股东大会未通过

22

银监会批准

23

证监会恢复审核

24

预发行

25

提交注册

26



二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
right_issue = info_data_object.get_right_issue(all_code_list)


图片



【AmazingData】获取上市公司限售股解禁数据


一、获取上市公司限售股解禁

函数接口：get_equity_restricted

功能描述：获取上市公司限售股信息

AmazingData最低版本：0.0.22

输入参数：  

参数	 数据类型	必选	解释
code_list
list[str] 
是
支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
equity_restricted	dict	
key：code

value:dataframe

column为限售股信息的字段

index为序号（无意义）

equity_restricted的字段说明：

字段名称

类型

字段说明

备注

MARKET_CODE

string

市场代码

 

LIST_DATE

string

解禁日期

 

SHARE_RATIO

float

解禁股占总股本比(%)

 

SHARE_LST_TYPE_NAME

string

解禁股份类型名称

 

SHARE_LST

int

解禁数量（股）

 

SHARE_LST_IS_ANN

int

上市数量是否公布值

0：否，为预测值 1: 是, 为实际公布值

CLOSE_PRICE

float

前日收盘价（元）

 

SHARE_LST_MARKET_VALUE

float

解禁市值（元）

SHARE_LST* CLOSE_PRICE

二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
equity_restricted= info_data_object.get_equity_restricted(all_code_list)


图片



【AmazingData】获取上市公司股权冻结/质押数据


一、获取上市公司股权冻结/质押

函数接口：get_equity_pledge_freeze

功能描述：获取上市公司股权冻结/质押

AmazingData最低版本：0.0.22

输入参数：  

参数	 数据类型	必选	解释
code_list
list[str] 
是
支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
equity_pledge_freeze	dict	
key：code

value:dataframe

column为股权冻结/质押的字段

index为序号（无意义）

equity_pledge_freeze的字段说明：

字段名称

类型

字段说明

备注

MARKET_CODE

string

市场代码

 

ANN_DATE

string

公告日期

 

HOLDER_NAME

string

股东名称

 

HOLDER_TYPE_CODE

int

股东类型代码

2:公司3:个人

TOTAL_HOLDING_SHR"

float

持股总数（万股）

 

TOTAL_HOLDING_SHR_RATIO

float

持股总数占公司总股本比例

 

FRO_SHARES

float

本次冻结/质押股数

 

FRO_SHR_TO_TOTAL_HOLDING_RATIO

float

本次冻结/质押占所持股比例

 

FRO_SHR_TO_TOTAL_RATIO

float

本次冻结/质押占总股本比例

 

TOTAL_PLEDGE_SHR

float

累计冻结/质押股数

 

IS_EQUITY_PLEDGE_REPO

int

是否股权质押回购

1:是0:否

BEGIN_DATE

string

冻结/质押起始日

 

END_DATE

string

解冻/解押日期

 

IS_DISFROZEN

int

是否质押或解冻

1:是0:否

FROZEN_INSTITUTION

string

执行冻结机构/质权方

 

DISFROZEN_TIME

string

解压或解冻日期

 

SHR_CATEGORY_CODE

int

股份性质类别代码

1:法人股2:个人股3:国有股4:国有股,法人股5:流通股6:流通股,限售流通股7:外资股8:限售流通股9:优先股                     

FREEZE_TYPE

int

冻结/质押类型

1:质押2:司法3:质押式回购



二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
equity_pledge_freeze= info_data_object.get_equity_pledge_freeze(all_code_list)


图片



【AmazingData】获取上市公司股本结构数据


一、获取上市公司股本结构

函数接口：get_equity_structure

功能描述：获取上市公司股本结构数据

AmazingData最低版本：0.0.21

输入参数：  

参数	 数据类型	必选	解释
code_list
list[str] 
是
支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
equity_structure	dataframe	
column为股本结构的字段

index为序号（无意义）

equity_structure的字段说明：

字段名称

类型

字段说明

备注

MARKET_CODE

string

市场代码

 

ANN_DATE

string

公告日期

 

CHANGE_DATE

string

变动日期

注：股票分红送转股时的红股上市日;股票增发时的新股上市日

SHARE_CHANGE_REASON_STR

string

股本变动原因描述

 

EX_CHANGE_DATE

string

除权日期

股票分红送转股时的除权日;股票增发时的登记日

CURRENT_SIGN

int

最新标志

1:是0:否

IS_VALID

int

是否有效

用来区分除权日相同时，是否为公司公告公布的最新股份数

1:是0:否

TOT_SHARE

float

总股本(万股)

 

FLOAT_SHARE

float

流通股(万股)

 

FLOAT_A_SHARE

float

流通A股(万股)

 

FLOAT_B_SHARE

float

流通B股(万股)

 

FLOAT_HK_SHARE

float

香港流通股(万股)

 

FLOAT_OS_SHARE

float

海外流通股(万股)

 

TOT_TRADABLE_SHARE

float

流通股合计

 

RTD_A_SHARE_INST

float

限售A股(其他内资持股:机构配售股)

 

RTD_A_SHARE_DOMESNP

float

限售A股(其他内资持股:境内自然人持股)

 

RTD_SHARE_SENIOR

float

限售股份(高管持股)(万股)

 

RTD_A_SHARE_FOREIGN

float

限售A股(外资持股)

 

RTD_A_SHARE_FORJUR

float

限售A股(境外法人持股)

 

RTD_A_SHARE_FORNP

float

限售A股(境外自然人持股)

 

RESTRICTED_B_SHARE

float

限售B股(万股)

 

OTHER_RTD_SHARE

float

其他限售股

 

NON_TRADABLE_SHARE

float

非流通股

 

NTRD_SHARE_STATE_PCT

float

非流通股(国有股)

 

NTRD_SHARE_STATE

float

非流通股(国家股)

 

NTRD_SHARE_STATEJUR

float

非流通股(国有法人股)

 

NTRD_SHARE_DOMESJUR

float

非流通股(境内法人股)

 

NTRD_SHARE_DOMES_INITIATOR

float

非流通股(境内法人股:境内发起人股)

 

NTRD_SHARE_IPOJURIS

float

非流通股(境内法人股:募集法人股)

 

NTRD_SHARE_GENJURIS

float

非流通股(境内法人股:一般法人股)

 

NTRD_SHARE_STRA_INVESTOR

float

非流通股(境内法人股:战略投资者持股)

 

NTRD_SHARE_FUND

float

非流通股(境内法人股:基金持股)

 

NTRD_SHARE_NAT

float

非流通股(自然人股)

 

TRAN_SHARE

float

转配股(万股)

 

FLOAT_SHARE_SENIOR

float

流通股(高管持股)

 

SHARE_INEMP

float

内部职工股(万股)

 

PREFERRED_SHARE

float

优先股(万股)

 

NTRD_SHARE_NLIST_FRGN

float

非流通股(非上市外资股)

 

STAQ_SHARE

float

STAQ股(万股)

 

NET_SHARE

float

NET股(万股)

 

SHARE_CHANGE_REASON

string

股本变动原因

 

TOT_A_SHARE

float

A股合计

 

TOT_B_SHARE

float

B股合计

 

OTCA_SHARE

float

三板A股

 

OTCB_SHARE

float

三板B股

 

TOT_OTC_SHARE

float

三板合计

 

SHARE_HK

float

香港上市股

 

PRE_NON_TRADABLE_SHARE

float

股改前非流通股

 

RESTRICTED_A_SHARE

float

限售A股(万股)

 

RTD_A_SHARE_STATE

float

限售A股(国家持股)

 

RTD_A_SHARE_STATEJUR

float

限售A股(国有法人持股)

 

RTD_A_SHARE_OTHER_DOMES

float

限售A股(其他内资持股)

 

RTD_A_SHARE_OTHER_DOMESJUR

float

限售A股(其他内资持股:境内法人持股)

 

TOT_RESTRICTED_SHARE

float

限售股合计

 




二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
equity_structure = info_data_object.get_equity_structure(all_code_list)


图片



【AmazingData】获取上市公司股东户数数据


一、获取上市公司股东户数

函数接口：get_holder_num

功能描述：获取上市公司股东户数数据

AmazingData最低版本：0.0.20

输入参数：  

参数	 数据类型	必选	解释
code_list
list[str] 
是
支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
holder_num
dataframe	
column为股东户数的字段

index为序号（无意义）

holder_num的字段说明：

字段名称

类型

字段说明

MARKET_CODE	
string

市场代码

ANN_DT

string

公告日期

HOLDER_ENDDATE

string

股东户数统计的截止日期
HOLDER_TOTAL_NUM

float

A股、B股、H股、境外股的总户数

HOLDER_NUM

float

A
股股东户数


二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
holder_num = info_data_object.get_holder_num(all_code_list)


图片



【AmazingData】获取A股的融资融券成交汇总数据


一、获取A股的融资融券成交汇总

函数接口：get_margin_summary

功能描述：获取A股的融资融券成交汇总数据

输入参数：  

参数	 数据类型	必选	解释
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
margin_summary

dataframe	
column为融资融券成交汇总的字段

index为序号（无意义）

margin_summary的字段说明：

字段名称

类型

字段说明

TRADE_DATE

string

交易日期

SUM_BORROW_MONEY_BALANCE

float

融资余额(元)

SUM_PURCH_WITH_BORROW_MONEY

float

融资买入额(元)

SUM_REPAYMENT_OF_BORROW_MONEY

float

融资偿还额(元)

SUM_SEC_LENDING_BALANCE

float

融券余额(元)

SUM_SALES_OF_BORROWED_SEC

int

融券卖出量(股,份,手)

SUM_MARGIN_TRADE_BALANCE

float

融资融券余额(元)



二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
margin_summary= info_data_object.get_margin_summary()


图片

【AmazingData】获取A股的融资融券交易明细


一、获取A股的融资融券交易明细

函数接口：get_margin_detail

功能描述：获取指定股票列表的A股上市公司的融资融券交易明细

输入参数：  

参数	 数据类型	必选	解释
code_list	list[str] 	是	支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
margin_detail

dataframe	
column为利润的字段

index为序号（无意义）

margin_detail的字段说明：

字段名称

类型

字段说明

MARKET_CODE

string

市场代码

SECURITY_NAME

string

证券简称

TRADE_DATE

string

交易日期

BORROW_MONEY_BALANCE"

float

融资余额(元)

PURCH_WITH_BORROW_MONEY

float

融资买入额(元)

REPAYMENT_OF_BORROW_MONEY

float

融资偿还额(元)

SEC_LENDING_BALANCE

float

融券余额(元)

SALES_OF_BORROWED_SEC

int

融券卖出量(股,份,手)

REPAYMENT_OF_BORROW_SEC

int

融券偿还量(股,份,手)

SEC_LENDING_BALANCE_VOL

int

融券余量(股,份,手)

MARGIN_TRADE_BALANCE

float

融资融券余额(元)

二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
margin_detail= info_data_object.get_margin_detail(all_code_list)
图片


【AmazingData】获取北交所的存量上市公司股票新旧代码对照表


2024年12月13日，北交所发布《关于做好北京证券交易所存量上市公司证券代码切换准备工作的通知》），向市场明确了存量上市公司代码切换工作的业务连续和技术准备要求。启动存量上市公司代码切换工作，标志着北交所代码切换工作迈入新阶段。

2024年4月22日以前上市的存量上市公司，股票代码前三位变更为920，后三位保持不变。切换后股票代码重复的，对后上市公司按照上市时间先后顺序逐家对第四位代码做递进处理，直至股票代码与切换后其他公司的股票代码不重复。

一、获取北交所的存量上市公司股票新旧代码对照表

函数接口：get_bj_code_mapping

AmazingData版本：0.0.18

功能描述：获取北交所的存量上市公司股票新旧代码对照表

输入参数：  

参数	 数据类型	必选	解释
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
bj_code_mapping

dataframe	
column为利润的字段

index为序号（无意义）

bj_code_mapping的字段说明：

字段名称	类型	字段说明
OLD_CODE	string	旧代码
NEW_CODE	string	新代码
SECURITY_NAME	string	证券简称
LISTING_DATE	int	上市日期

二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
bj_code_mapping = info_data_object.get_bj_code_mapping()


图片

【AmazingData】获取上市公司的现金流量表数据


一、获取上市公司的现金流量表

函数接口：get_cash_flow

AmazingData版本：0.0.17

功能描述：获取指定股票列表的上市公司的现金流量表数据

输入参数：  

参数	 数据类型	必选	解释
code_list	list[str] 	是	支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
cash_flow

dataframe	
column为利润的字段

index为序号（无意义）

cash_flow的字段说明：

字段名称	类型	字段说明	备注
MARKET_CODE	string	市场代码	
SECURITY_NAME	string	证券简称	
STATEMENT_TYPE	string	报表类型	参看报表类型代码表
REPORT_TYPE	string	报告期名称	
REPORTING_PERIOD	string	报告期	
ANN_DATE	string	公告日期	
ACTUAL_ANN_DATE	string	实际公告日期	
ABSORB_CASH_RECP_INV	double	吸收投资收到的现金	
AMORT_INTAN_ASSETS	double	无形资产摊销	
AMORT_LT_DEFERRED_EXP	double	长期待摊费用摊销	
BEG_BAL_CASH_CASH_EQU	double	期初现金及现金等价物余额	
CASH_END_BAL	double	现金的期末余额	
CASH_FOR_CHARGE	double	支付手续费的现金	
CASH_PAID_INSUR_POLICY	double	支付保单红利的现金	
CASH_PAID_INV	double	投资支付的现金	
CASH_PAID_PUR_CONST_FIOLTA	double	购建固定资产、无形资产和其他长期资产支付的现金	
CASH_PAY_CLAIMS_OIC	double	支付原保险合同赔付款项的现金	
CASH_PAY_DIST_DIV_PRO_INT	double	分配股利、利润或偿付利息支付的现金	
CASH_PAY_EMPLOYEE	double	支付给职工以及为职工支付的现金	
CASH_PAY_FOR_DEBT	double	偿还债务支付的现金	
CASH_PAY_GOODS_SERVICES	double	购买商品、接受劳务支付的现金	
CASH_RECE_BORROW	double	取得借款收到的现金	
CASH_RECE_ISSUE_BONDS	double	发行债券收到的现金	
CASH_RECP_INV_INCOME	double	取得投资收益收到的现金	
CASH_RECP_PREM_OIC	double	收到原保险合同保费取得的现金	
CASH_RECP_RECOV_INV	double	收回投资收到的现金	
CASH_RECP_SG_AND_RS	double	销售商品、提供劳务收到的现金	
COMP_TYPE_CODE	string	公司类型代码	1：非金融类2：银行3：保险4：证券
CONV_CORP_BONDS_DUE_WITHIN_1Y	double	一年内到期的可转换公司债券	
CONV_DEBT_INTO_CAP	double	债务转为资本	
CREDIT_IMPAIR_LOSS	double	信用减值损失	
CURRENCY_CODE	string	货币代码	
DECR_DEFE_INC_TAX_ASSETS	double	递延所得税资产减少	
DECR_DEFERRED_EXPENSE	double	待摊费用减少	
DECR_INVENTORY	double	存货的减少	
DECR_OPERA_RECEIVABLE	double	经营性应收项目的减少	
DEPRE_FA_OGA_PBA	double	固定资产折旧、油气资产折耗、生产性生物资产折旧	
EFF_FX_FLUC_CASH	double	汇率变动对现金的影响	
END_BAL_CASH_CASH_EQU	double	期末现金及现金等价物余额	
FINANCIAL_EXP	double	财务费用	
FIXED_ASSETS_FIN_LEASE	double	融资租入固定资产	
FREE_CASH_FLOW	double	企业自由现金流量	
INCL_CASH_RECP_SAIMS	double	其中:子公司吸收少数股东投资收到的现金	
INCL_DIV_PRO_PAID_SMS	double	其中:子公司支付给少数股东的股利、利润	
INCR_ACCRUED_EXP	double	预提费用增加	
INCR_DEFE_INC_TAX_LIAB	double	递延所得税负债增加	
INCR_OPERA_PAYABLE	double	经营性应付项目的增加	
IND_NET_CASH_FLOWS_OPERA_ACT	double	间接法-经营活动产生的现金流量净额	
IND_NET_INCR_CASH_AND_EQU	double	间接法-现金及现金等价物净增加额	
INV_LOSS	double	投资损失	
IS_CALCULATION	int	是否计算报表	
LESS_OPEN_BAL_CASH	double	减:现金的期初余额	
LESS_OPEN_BAL_CASH_EQU	double	减:现金等价物的期初余额	
LOSS_DISP_FIOLTA	double	处置固定、无形资产和其他长期资产的损失	
LOSS_FAIRVALUE_CHG	double	公允价值变动损失	
LOSS_FIXED_ASSETS	double	固定资产报废损失	
NET_CASH_FLOWS_FIN_ACT	double	筹资活动产生的现金流量净额	
NET_CASH_FLOWS_INV_ACT	double	投资活动产生的现金流量净额	
NET_CASH_FLOWS_OPERA_ACT	double	经营活动产生的现金流量净额	
NET_CASH_PAID_SOBU	double	取得子公司及其他营业单位支付的现金净额	
NET_CASH_REC_SEC	double	代理买卖证券收到的现金净额	
NET_CASH_RECP_DISP_FIOLTA	double	处置固定资产、无形资产和其他长期资产收回的现金净额	
NET_CASH_RECP_DISP_SOBU	double	处置子公司及其他营业单位收到的现金净额	
NET_CASH_RECP_REINSU_BUS	double	收到再保业务现金净额	
NET_INCR_BORR_FUND	double	拆入资金净增加额	
NET_INCR_BORR_OFI	double	向其他金融机构拆入资金净增加额	
NET_INCR_CASH_AND_CASH_EQU	double	现金及现金等价物净增加额	
NET_INCR_CUS_LOAN_ADV	double	客户贷款及垫款净增加额	
NET_INCR_DEP_CB_IB	double	存放央行和同业款项净增加额	
NET_INCR_DEP_CUS_AND_IB	double	客户存款和同业存放款项净增加额	
NET_INCR_DISMANTLE_CAP	double	拆出资金净增加额	
NET_INCR_DISP_FAAS	double	处置可供出售金融资产净增加额	
NET_INCR_DISP_TFA	double	处置交易性金融资产净增加额	
NET_INCR_INSURED_SAVE	double	保户储金净增加额	
NET_INCR_INT_AND_CHARGE	double	收取利息和手续费净增加额	
NET_INCR_LOANS_CENTRAL_BANK	double	向中央银行借款净增加额	
NET_INCR_PLEDGE_LOAN	double	质押贷款净增加额	
NET_INCR_REPU_BUS_FUND	double	回购业务资金净增加额	
NET_PROFIT	double	净利润	
OTH_CASH_PAY_INV_ACT	double	支付其他与投资活动有关的现金	
OTH_CASH_PAY_OPERA_ACT	double	支付其他与经营活动有关的现金	
OTH_CASH_RECP_INV_ACT	double	收到其他与投资活动有关的现金	
OTHER_ASSETS_IMPAIR_LOSS	double	其他资产减值损失	
OTHER_CASH_PAY_FIN_ACT	double	支付其他与筹资活动有关的现金	
OTHER_CASH_RECP_FIN_ACT	double	收到其他与筹资活动有关的现金	
OTHER_CASH_RECP_OPER_ACT	double	收到其他与经营活动有关的现金	
OTHERS	double	其他（废弃）	
PAY_ALL_TAX	double	支付的各项税费	
PLUS_ASSETS_DEPRE_PREP	double	加:资产减值准备	
PLUS_END_BAL_CASH_EQU	double	加:现金等价物的期末余额	
RECP_TAX_REFUND	double	收到的税费返还	
SPE_BAL_CASH_INFLOW_FIN_ACT	double	筹资活动现金流入差额	
SPE_BAL_CASH_INFLOW_INV_ACT	double	投资活动现金流入差额	
SPE_BAL_CASH_INFLOW_OPERA_ACT	double	经营活动现金流入差额	
SPE_BAL_CASH_OUTFLOW_FIN	double	筹资活动现金流出差额	
SPE_BAL_CASH_OUTFLOW_INV	double	投资活动现金流出差额	
SPE_BAL_CASH_OUTFLOW_OPERA	double	经营活动现金流出差额	
SPE_BAL_NETCASH_INC_DIFF_IND	double	间接法-现金净增加额差额	
SPE_BAL_NETCASH_INCR_DIFF	double	现金净增加额差额	
SPE_BAL_NETCASH_OPERA_IND	double	间接法-经营活动现金流量净额差额	
TOT_BAL_CASH_INFLOW_FIN_ACT	double	筹资活动现金流入差额	
TOT_BAL_CASH_INFLOW_INV_ACT	double	投资活动现金流入差额	
TOT_BAL_CASH_INFLOW_OPERA_ACT	double	经营活动现金流入差额	
TOT_BAL_CASH_OUTFLOW_FIN	double	筹资活动现金流出差额	
TOT_BAL_CASH_OUTFLOW_INV	double	投资活动现金流出差额	
TOT_BAL_CASH_OUTFLOW_OPERA	double	经营活动现金流出差额	
TOT_BAL_NETCASH_FLOW_FIN	double	筹资活动产生的现金流量净额差额	
TOT_BAL_NETCASH_FLOW_INV	double	投资活动产生的现金流量净额差额	
TOT_BAL_NETCASH_FLOW_OPERA	double	经营活动产生的现金流量净额差额	
TOT_BAL_NETCASH_INC_DIFF_IND	double	间接法-现金净增加额差额	
TOT_BAL_NETCASH_INCR_DIFF	double	现金净增加额差额	
TOT_BAL_NETCASH_OPERA_IND	double	间接法-经营活动现金流量净额差额	
TOT_CASH_INFLOW_FIN_ACT	double	筹资活动现金流入小计	
TOT_CASH_INFLOW_INV_ACT	double	投资活动现金流入小计	
TOT_CASH_INFLOW_OPER_ACT	double	经营活动现金流入小计	
TOT_CASH_OUTFLOW_FIN_ACT	double	筹资活动现金流出小计	
TOT_CASH_OUTFLOW_INV_ACT	double	投资活动现金流出小计	
TOT_CASH_OUTFLOW_OPERA_ACT	double	经营活动现金流出小计	
UNCONFIRMED_INV_LOSS	double	未确认投资损失	
USE_RIGHT_ASSET_DEP	double	使用权资产折旧	


报表类型

报表类型代码

备注

1

合并报表

涵盖母公司的财务报表数据，为最新报表

2

合并报表(单季度)

合并报表(单季度)=合并报表(本期)-合并报表(上一季)

3

合并报表(单季度调整)

合并报表(单季度调整)=合并报表(本期调整)-合并报表(上一季调整)

4

合并报表(调整)

本年度公布上年同期的财务报表数据，报告期为上年度

5

合并报表(更正前)

即出更正公告后，把合并报表的记录修改为合并报表(更正前)；复制原来的记录，更正后报表类型改为合并报表

6

母公司报表

该公司母公司的财务报表数据

7

母公司报表(单季度)

母公司报表(单季度)=母公司报表(本期)-母公司报表(上一季)

8

母公司报表(单季度调整)

母公司报表(单季度调整)=母公司报表(本期调整)-母公司报表(上一季调整)

9

母公司报表(调整)

该公司母公司的本年度公布上年同期的财务报表数据

10

母公司报表(更正前)

之前上市公司已披露财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。

11

合并报表(未公开)

未在公开信息源披露的财报且加工为合并报表口径

12

合并报表(调整未公开)

未在公开信息源披露的财报且加工为合并报表调整口径

13

合并报表(单季度未公开)

未在公开信息源披露的财报且加工为合并报表单季度口径

14

合并报表(单季度调整未公开)

未在公开信息源披露的财报且加工为母公司报表口径

15

母公司报表(未公开)

未在公开信息源披露的财报且加工为母公司报表口径

16

母公司报表(调整未公开)

未在公开信息源披露的财报且加工为母公司报表调整口径

17

母公司报表(单季度未公开)

未在公开信息源披露的财报且加工或计算为母公司报表单季度口径

18

母公司报表(单季度调整未公开)

未在公开信息源披露的财报且加工或计算为母公司报表单季度调整口径

19

合并报表(调整借壳前)

借壳前的合并报表(调整)

20

合并调整

对合并前各公司的财务报表进行调整，以确保合并财务报表的准确性和可比性

21

合并报表(单季度借壳前)

借壳前的合并报表(单季度)

22

合并报表(单季度调整借壳前)

借壳前的合并报表(单季度调整)

23

母公司报表(借壳前)

借壳前的母公司报表

24

母公司报表(调整借壳前)

借壳前的母公司报表(调整)

25

母公司报表(单季度借壳前)

借壳前的母公司报表(单季度)

26

母公司报表(单季度调整借壳前)

借壳前的母公司报表(单季度调整)

27

合并报表(第一次更正)

有多次更正时，合并报表的第一次更正

28

合并报表(第二次更正)

有多次更正时，合并报表的第二次更正

29

合并调整(第一次更正)

有多次更正时，合并调整的第一次更正

30

合并报表(单月度)

根据披露的券商月报公告加工为合并报表口径

31

合并调整(第二次更正)

有多次更正时，合并调整的第二次更正

32

母公司调整(第二次更正)

有多次更正时，母公司调整的第二次更正

33

母公司调整(第一次更正)

有多次更正时，母公司调整的第一次更正

34

母公司报表(第二次更正)

有多次更正时，母公司报表的第二次更正

35

母公司报表(第一次更正)

有多次更正时，母公司报表的第一次更正

36

合并报表(第三次更正)

有多次更正时，合并报表的第三次更正

37

合并调整(第三次更正)

有多次更正时，合并调整的第三次更正

38

母公司报表(第三次更正)

有多次更正时，母公司报表的第三次更正

39

母公司调整(第三次更正)

有多次更正时，母公司调整的第三次更正

40

母公司报表(单月度)

根据披露的券商月报公告加工为母公司报表口径的数据

41

合并报表(业绩快报)

加工业绩快报中的财务数据（海外数据专用）

42

合并调整(第一次)

第一次合并调整数据

43

合并调整(第二次)

第二次合并调整数据

44

合并调整(第三次)

第三次合并调整数据

45

合并报表(第四次更正)

有多次更正时，合并报表的第四次更正

46

合并调整(第四次更正)

有多次更正时，合并调整的第四次更正

47

母公司报表(第四次更正)

有多次更正时，母公司报表的第四次更正

48

母公司调整(第四次更正)

有多次更正时，母公司调整的第四次更正

50

合并调整(更正前)

即出更正公告后，把合并报表（调整）的记录修改为合并调整(更正前)；复制原来的记录，更正后报表类型改为合并报表(调整)

51

合并报表(下半年报)

合并下半年度的报表

60

母公司调整(更正前)

该公司母公司的本年度公布上年同期的财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。

70

合并报表(借壳前)

公司主体在借壳上市前披露或者计算的为合并报表口径的报表类型

80

合并报表(预测)

REITS基金的定期报告中披露的预测的合并报表数据

90

项目资产报表

由项目资产管理人编制的一种财务报表，用于反映项目资产的财务状况和经营情况

二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
cash_flow = info_data_object.get_cash_flow(all_code_list)


图片

【AmazingData】获取上市公司的利润表数据


一、获取上市公司的利润表

函数接口：get_income

功能描述：获取指定股票列表的上市公司的利润表数据

输入参数：  

参数	 数据类型	必选	解释
code_list	list[str] 	是	支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
income

dataframe	
column为利润的字段

index为序号（无意义）

income的字段说明：

字段名称	类型	字段说明	备注
MARKET_CODE	string	市场代码	
SECURITY_NAME	string	证券简称	
STATEMENT_TYPE	string	报表类型	参看报表类型代码表
REPORT_TYPE	string	报告期名称	
REPORTING_PERIOD	string	报告期	
ANN_DATE	string	公告日期	
ACTUAL_ANN_DATE	string	实际公告日期	
AMORT_COST_FIN_ASSETS_EAR	float	以摊余成本计量的金融资产终止确认收益	
ANN_DATE	string	公告日期	
BASIC_EPS	float	基本每股收益	
BEG_UNDISTRIBUTED_PRO	float	年初未分配利润	
CAPITALIZED_COM_STOCK_DIV	float	转作股本的普通股股利	
COMMENTS	string	备注	
COMMON_STOCK_DIV_PAYABLE	float	应付普通股股利	
COMP_ID	string	公司ID	
COMP_TYPE_CODE	string	公司类型代码	1：非金融类2：银行3：保险4：证券
CONTINUED_NET_OPERA_PRO	float	持续经营净利润	
CREDIT_IMPAIR_LOSS	float	信用减值损失	
CURRENCY_CODE	string	货币代码	
DILUTED_EPS	float	稀释每股收益	
DISTRIBUTIVE_PRO	float	可分配利润	
DISTRIBUTIVE_PRO_SHAREHOLDER	float	可供股东分配的利润	
DIV_EXP_INSUR	float	保户红利支出	
EBIT	float	息税前利润	正向法
EBITDA	float	息税折旧摊销前利润	
EMPLOYEE_WELFARE	float	职工奖金福利	
END_NET_OPERA_PRO	float	终止经营净利润	
EXT_INSUR_CONT_RSRV	float	提取保险责任准备金	
EXT_UNEARNED_PREM_RES	float	提取未到期责任准备金	
FIN_EXP_INT_EXP	float	财务费用:利息费用	
FIN_EXP_INT_INC	float	财务费用:利息收入	
GAIN_DISPOSAL_ASSETS	float	资产处置收益	
HANDLING_CHRG_COMM_FEE	float	手续费及佣金收入	
INCL_INC_INV_JV_ENTP	float	其中:对联营企业和合营企业的投资收益	
INCL_LESS_LOSS_DISP_NCUR_ASSET	float	其中:减:非流动资产处置净损失	
INCL_REINSUR_PREM_INC	float	其中:分保费收入	
INCOME_TAX	float	所得税	
INSUR_EXP	float	保险业务支出	
INSUR_PREM	float	已赚保费	
INTEREST_INC	float	利息收入	
IS_CALCULATION	float	是否计算报表	
LESS_ADMIN_EXP	float	减:管理费用	
LESS_AMORT_COMPEN_EXP	float	减:摊回赔付支出	
LESS_AMORT_INSUR_CONT_RSRV	float	减:摊回保险责任准备金	
LESS_AMORT_REINSUR_EXP	float	减:摊回分保费用	
LESS_ASSETS_IMPAIR_LOSS	float	减:资产减值损失	
LESS_BUS_TAX_SURCHARGE	float	减:营业税金及附加	
LESS_FIN_EXP	float	减:财务费用	
LESS_HANDLING_CHRG_COMM_FEE	float	减:手续费及佣金支出	
LESS_INTEREST_EXP	float	减:利息支出	
LESS_NON_OPERA_EXP	float	减:营业外支出	
LESS_OPERA_COST	float	减:营业成本	
LESS_REINSUR_PREM	float	减:分出保费	
LESS_SELLING_EXP	float	减:销售费用	
MARKET_CODE	string	市场代码	
MIN_INT_INC	float	少数股东损益	
NET_EXPOSURE_HEDGING_GAIN	float	净敞口套期收益	
NET_HANDLING_CHRG_COMM_FEE	float	手续费及佣金净收入	
NET_INC_EC_ASSET_MGMT_BUS	float	受托客户资产管理业务净收入	
NET_INC_SEC_BROK_BUS	float	代理买卖证券业务净收入	
NET_INC_SEC_UW_BUS	float	证券承销业务净收入	
NET_INTEREST_INC	float	利息净收入	
NET_PRO_AFTER_DED_NR_GL	float	扣除非经常性损益后净利润（扣除少数股东损益）	
NET_PRO_AFTER_DED_NR_GL_COR	float	扣除非经常性损益后的净利润(财务重要指标(更正前))	
NET_PRO_EXCL_MIN_INT_INC	float	净利润(不含少数股东损益)	
NET_PRO_INCL_MIN_INT_INC	float	净利润(含少数股东损益)	
NET_PRO_UNDER_INT_ACC_STA	float	国际会计准则净利润	
OPERA_EXP	float	营业支出	
OPERA_PROFIT	float	营业利润	
OPERA_REV	float	营业收入	
OTH_ASSETS_IMPAIR_LOSS	float	其他资产减值损失	
OTH_BUS_COST	float	其他业务成本	
OTH_BUS_INC	float	其他业务收入	
OTH_COMPRE_INC	float	其他综合收益	
OTH_INCOME	float	其他收益	
OTH_NET_OPERA_INC	float	其他经营净收益	
PLUS_NET_FX_INC	float	加:汇兑净收益	
PLUS_NET_GAIN_CHG_FV	float	加:公允价值变动净收益	
PLUS_NET_INV_INC	float	加:投资净收益	
PLUS_NON_OPERA_REV	float	加:营业外收入	
PLUS_OTH_NET_BUS_INC	float	加:其他业务净收益	
PREFERRED_SHARE_DIV_PAYABLE	float	应付优先股股利	
PREM_BUS_INC	float	保费业务收入	
RD_EXP	float	研发费用	
REINSURANCE_EXP	float	分保费用	
REPORT_TYPE	string	报告期名称	
REPORTING_PERIOD	string	报告期	
SECURITY_NAME	string	证券简称	
SPE_BAL_NET_PRO_MARG	float	净利润差额(特殊报表科目)	
SPE_BAL_OPERA_PRO_MARG	float	营业利润差额(特殊报表科目)	
SPE_BAL_TOT_OPERA_COST_DIF	float	营业总成本差额(特殊报表科目)	
SPE_BAL_TOT_OPERA_INC_DIF	float	营业总收入差额(特殊报表科目)	
SPE_BAL_TOT_PRO_MARG	float	利润总额差额(特殊报表科目)	
SPE_TOT_OPERA_COST_DIF_STATE	string	营业总成本差额说明(特殊报表科目)	
SPE_TOT_OPERA_INC_DIF_STATE	string	营业总收入差额说明(特殊报表科目)	
SURR_VALUE	float	退保金	
TOT_BAL_NET_PRO_MARG	float	净利润差额(合计平衡项目)	
TOT_BAL_OPERA_PRO_MARG	float	营业利润差额(合计平衡项目)	
TOT_BAL_TOT_PRO_MARG	float	利润总额差额(合计平衡项目)	
TOT_COMPEN_EXP	float	赔付总支出	
TOT_COMPRE_INC	float	综合收益总额	
TOT_COMPRE_INC_MIN_SHARE	float	综合收益总额(少数股东)	
TOT_COMPRE_INC_PARENT_COMP	float	综合收益总额(母公司)	
TOT_OPERA_COST	float	营业总成本	
TOT_OPERA_COST2	float	营业总成本2	
TOT_OPERA_REV	float	营业总收入	
TOTAL_PROFIT	float	利润总额	
TRANSFER_HOUSING_REVO_FUNDS	float	住房周转金转入	
TRANSFER_OTHERS	float	其他转入	
TRANSFER_SURPLUS_RESERVE	float	盈余公积转入	
UNCONFIRMED_INV_LOSS	float	未确认投资损失	
WITHDRAW_ANY_SURPLUS_RESV	float	提取任意盈余公积金	
WITHDRAW_ENT_DEVELOP_FUND	float	提取企业发展基金	
WITHDRAW_LEG_PUB_WEL_FUND	float	提取法定公益金	
WITHDRAW_LEG_SURPLUS	float	提取法定盈余公积	
WITHDRAW_RESV_FUND	float	提取储备基金	
报表类型

报表类型代码

备注

1

合并报表

涵盖母公司的财务报表数据，为最新报表

2

合并报表(单季度)

合并报表(单季度)=合并报表(本期)-合并报表(上一季)

3

合并报表(单季度调整)

合并报表(单季度调整)=合并报表(本期调整)-合并报表(上一季调整)

4

合并报表(调整)

本年度公布上年同期的财务报表数据，报告期为上年度

5

合并报表(更正前)

即出更正公告后，把合并报表的记录修改为合并报表(更正前)；复制原来的记录，更正后报表类型改为合并报表

6

母公司报表

该公司母公司的财务报表数据

7

母公司报表(单季度)

母公司报表(单季度)=母公司报表(本期)-母公司报表(上一季)

8

母公司报表(单季度调整)

母公司报表(单季度调整)=母公司报表(本期调整)-母公司报表(上一季调整)

9

母公司报表(调整)

该公司母公司的本年度公布上年同期的财务报表数据

10

母公司报表(更正前)

之前上市公司已披露财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。

19

合并报表(调整借壳前)

借壳前的合并报表(调整)

21

合并报表(单季度借壳前)

借壳前的合并报表(单季度)

22

合并报表(单季度调整借壳前)

借壳前的合并报表(单季度调整)

23

母公司报表(借壳前)

借壳前的母公司报表

24

母公司报表(调整借壳前)

借壳前的母公司报表(调整)

25

母公司报表(单季度借壳前)

借壳前的母公司报表(单季度)

26

母公司报表(单季度调整借壳前)

借壳前的母公司报表(单季度调整)

27

合并报表(第一次更正)

有多次更正时，合并报表的第一次更正

28

合并报表(第二次更正)

有多次更正时，合并报表的第二次更正

29

合并调整(第一次更正)

有多次更正时，合并调整的第一次更正

33

母公司调整(第一次更正)

有多次更正时，母公司调整的第一次更正

34

母公司报表(第二次更正)

有多次更正时，母公司报表的第二次更正

35

母公司报表(第一次更正)

有多次更正时，母公司报表的第一次更正

50

合并调整(更正前)

即出更正公告后，把合并报表（调整）的记录修改为合并调整(更正前)；复制原来的记录，更正后报表类型改为合并报表(调整)

60

母公司调整(更正前)

该公司母公司的本年度公布上年同期的财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。

二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
income = info_data_object.get_income(all_code_list)


图片

【AmazingData】获取上市公司的利润表数据


一、获取上市公司的利润表

函数接口：get_income

功能描述：获取指定股票列表的上市公司的利润表数据

输入参数：  

参数	 数据类型	必选	解释
code_list	list[str] 	是	支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
income

dataframe	
column为利润的字段

index为序号（无意义）

income的字段说明：

字段名称	类型	字段说明	备注
MARKET_CODE	string	市场代码	
SECURITY_NAME	string	证券简称	
STATEMENT_TYPE	string	报表类型	参看报表类型代码表
REPORT_TYPE	string	报告期名称	
REPORTING_PERIOD	string	报告期	
ANN_DATE	string	公告日期	
ACTUAL_ANN_DATE	string	实际公告日期	
AMORT_COST_FIN_ASSETS_EAR	float	以摊余成本计量的金融资产终止确认收益	
ANN_DATE	string	公告日期	
BASIC_EPS	float	基本每股收益	
BEG_UNDISTRIBUTED_PRO	float	年初未分配利润	
CAPITALIZED_COM_STOCK_DIV	float	转作股本的普通股股利	
COMMENTS	string	备注	
COMMON_STOCK_DIV_PAYABLE	float	应付普通股股利	
COMP_ID	string	公司ID	
COMP_TYPE_CODE	string	公司类型代码	1：非金融类2：银行3：保险4：证券
CONTINUED_NET_OPERA_PRO	float	持续经营净利润	
CREDIT_IMPAIR_LOSS	float	信用减值损失	
CURRENCY_CODE	string	货币代码	
DILUTED_EPS	float	稀释每股收益	
DISTRIBUTIVE_PRO	float	可分配利润	
DISTRIBUTIVE_PRO_SHAREHOLDER	float	可供股东分配的利润	
DIV_EXP_INSUR	float	保户红利支出	
EBIT	float	息税前利润	正向法
EBITDA	float	息税折旧摊销前利润	
EMPLOYEE_WELFARE	float	职工奖金福利	
END_NET_OPERA_PRO	float	终止经营净利润	
EXT_INSUR_CONT_RSRV	float	提取保险责任准备金	
EXT_UNEARNED_PREM_RES	float	提取未到期责任准备金	
FIN_EXP_INT_EXP	float	财务费用:利息费用	
FIN_EXP_INT_INC	float	财务费用:利息收入	
GAIN_DISPOSAL_ASSETS	float	资产处置收益	
HANDLING_CHRG_COMM_FEE	float	手续费及佣金收入	
INCL_INC_INV_JV_ENTP	float	其中:对联营企业和合营企业的投资收益	
INCL_LESS_LOSS_DISP_NCUR_ASSET	float	其中:减:非流动资产处置净损失	
INCL_REINSUR_PREM_INC	float	其中:分保费收入	
INCOME_TAX	float	所得税	
INSUR_EXP	float	保险业务支出	
INSUR_PREM	float	已赚保费	
INTEREST_INC	float	利息收入	
IS_CALCULATION	float	是否计算报表	
LESS_ADMIN_EXP	float	减:管理费用	
LESS_AMORT_COMPEN_EXP	float	减:摊回赔付支出	
LESS_AMORT_INSUR_CONT_RSRV	float	减:摊回保险责任准备金	
LESS_AMORT_REINSUR_EXP	float	减:摊回分保费用	
LESS_ASSETS_IMPAIR_LOSS	float	减:资产减值损失	
LESS_BUS_TAX_SURCHARGE	float	减:营业税金及附加	
LESS_FIN_EXP	float	减:财务费用	
LESS_HANDLING_CHRG_COMM_FEE	float	减:手续费及佣金支出	
LESS_INTEREST_EXP	float	减:利息支出	
LESS_NON_OPERA_EXP	float	减:营业外支出	
LESS_OPERA_COST	float	减:营业成本	
LESS_REINSUR_PREM	float	减:分出保费	
LESS_SELLING_EXP	float	减:销售费用	
MARKET_CODE	string	市场代码	
MIN_INT_INC	float	少数股东损益	
NET_EXPOSURE_HEDGING_GAIN	float	净敞口套期收益	
NET_HANDLING_CHRG_COMM_FEE	float	手续费及佣金净收入	
NET_INC_EC_ASSET_MGMT_BUS	float	受托客户资产管理业务净收入	
NET_INC_SEC_BROK_BUS	float	代理买卖证券业务净收入	
NET_INC_SEC_UW_BUS	float	证券承销业务净收入	
NET_INTEREST_INC	float	利息净收入	
NET_PRO_AFTER_DED_NR_GL	float	扣除非经常性损益后净利润（扣除少数股东损益）	
NET_PRO_AFTER_DED_NR_GL_COR	float	扣除非经常性损益后的净利润(财务重要指标(更正前))	
NET_PRO_EXCL_MIN_INT_INC	float	净利润(不含少数股东损益)	
NET_PRO_INCL_MIN_INT_INC	float	净利润(含少数股东损益)	
NET_PRO_UNDER_INT_ACC_STA	float	国际会计准则净利润	
OPERA_EXP	float	营业支出	
OPERA_PROFIT	float	营业利润	
OPERA_REV	float	营业收入	
OTH_ASSETS_IMPAIR_LOSS	float	其他资产减值损失	
OTH_BUS_COST	float	其他业务成本	
OTH_BUS_INC	float	其他业务收入	
OTH_COMPRE_INC	float	其他综合收益	
OTH_INCOME	float	其他收益	
OTH_NET_OPERA_INC	float	其他经营净收益	
PLUS_NET_FX_INC	float	加:汇兑净收益	
PLUS_NET_GAIN_CHG_FV	float	加:公允价值变动净收益	
PLUS_NET_INV_INC	float	加:投资净收益	
PLUS_NON_OPERA_REV	float	加:营业外收入	
PLUS_OTH_NET_BUS_INC	float	加:其他业务净收益	
PREFERRED_SHARE_DIV_PAYABLE	float	应付优先股股利	
PREM_BUS_INC	float	保费业务收入	
RD_EXP	float	研发费用	
REINSURANCE_EXP	float	分保费用	
REPORT_TYPE	string	报告期名称	
REPORTING_PERIOD	string	报告期	
SECURITY_NAME	string	证券简称	
SPE_BAL_NET_PRO_MARG	float	净利润差额(特殊报表科目)	
SPE_BAL_OPERA_PRO_MARG	float	营业利润差额(特殊报表科目)	
SPE_BAL_TOT_OPERA_COST_DIF	float	营业总成本差额(特殊报表科目)	
SPE_BAL_TOT_OPERA_INC_DIF	float	营业总收入差额(特殊报表科目)	
SPE_BAL_TOT_PRO_MARG	float	利润总额差额(特殊报表科目)	
SPE_TOT_OPERA_COST_DIF_STATE	string	营业总成本差额说明(特殊报表科目)	
SPE_TOT_OPERA_INC_DIF_STATE	string	营业总收入差额说明(特殊报表科目)	
SURR_VALUE	float	退保金	
TOT_BAL_NET_PRO_MARG	float	净利润差额(合计平衡项目)	
TOT_BAL_OPERA_PRO_MARG	float	营业利润差额(合计平衡项目)	
TOT_BAL_TOT_PRO_MARG	float	利润总额差额(合计平衡项目)	
TOT_COMPEN_EXP	float	赔付总支出	
TOT_COMPRE_INC	float	综合收益总额	
TOT_COMPRE_INC_MIN_SHARE	float	综合收益总额(少数股东)	
TOT_COMPRE_INC_PARENT_COMP	float	综合收益总额(母公司)	
TOT_OPERA_COST	float	营业总成本	
TOT_OPERA_COST2	float	营业总成本2	
TOT_OPERA_REV	float	营业总收入	
TOTAL_PROFIT	float	利润总额	
TRANSFER_HOUSING_REVO_FUNDS	float	住房周转金转入	
TRANSFER_OTHERS	float	其他转入	
TRANSFER_SURPLUS_RESERVE	float	盈余公积转入	
UNCONFIRMED_INV_LOSS	float	未确认投资损失	
WITHDRAW_ANY_SURPLUS_RESV	float	提取任意盈余公积金	
WITHDRAW_ENT_DEVELOP_FUND	float	提取企业发展基金	
WITHDRAW_LEG_PUB_WEL_FUND	float	提取法定公益金	
WITHDRAW_LEG_SURPLUS	float	提取法定盈余公积	
WITHDRAW_RESV_FUND	float	提取储备基金	
报表类型

报表类型代码

备注

1

合并报表

涵盖母公司的财务报表数据，为最新报表

2

合并报表(单季度)

合并报表(单季度)=合并报表(本期)-合并报表(上一季)

3

合并报表(单季度调整)

合并报表(单季度调整)=合并报表(本期调整)-合并报表(上一季调整)

4

合并报表(调整)

本年度公布上年同期的财务报表数据，报告期为上年度

5

合并报表(更正前)

即出更正公告后，把合并报表的记录修改为合并报表(更正前)；复制原来的记录，更正后报表类型改为合并报表

6

母公司报表

该公司母公司的财务报表数据

7

母公司报表(单季度)

母公司报表(单季度)=母公司报表(本期)-母公司报表(上一季)

8

母公司报表(单季度调整)

母公司报表(单季度调整)=母公司报表(本期调整)-母公司报表(上一季调整)

9

母公司报表(调整)

该公司母公司的本年度公布上年同期的财务报表数据

10

母公司报表(更正前)

之前上市公司已披露财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。

19

合并报表(调整借壳前)

借壳前的合并报表(调整)

21

合并报表(单季度借壳前)

借壳前的合并报表(单季度)

22

合并报表(单季度调整借壳前)

借壳前的合并报表(单季度调整)

23

母公司报表(借壳前)

借壳前的母公司报表

24

母公司报表(调整借壳前)

借壳前的母公司报表(调整)

25

母公司报表(单季度借壳前)

借壳前的母公司报表(单季度)

26

母公司报表(单季度调整借壳前)

借壳前的母公司报表(单季度调整)

27

合并报表(第一次更正)

有多次更正时，合并报表的第一次更正

28

合并报表(第二次更正)

有多次更正时，合并报表的第二次更正

29

合并调整(第一次更正)

有多次更正时，合并调整的第一次更正

33

母公司调整(第一次更正)

有多次更正时，母公司调整的第一次更正

34

母公司报表(第二次更正)

有多次更正时，母公司报表的第二次更正

35

母公司报表(第一次更正)

有多次更正时，母公司报表的第一次更正

50

合并调整(更正前)

即出更正公告后，把合并报表（调整）的记录修改为合并调整(更正前)；复制原来的记录，更正后报表类型改为合并报表(调整)

60

母公司调整(更正前)

该公司母公司的本年度公布上年同期的财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。

二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)
income = info_data_object.get_income(all_code_list)


图片

【AmazingData】获取上市公司的资产负债表数据


一、获取上市公司的资产负债表

函数接口：get_balance_sheet

功能描述：获取指定股票列表的上市公司的资产负债表数据

输入参数：  

参数	 数据类型	必选	解释
code_list	list[str] 	是	支持沪深A的的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
balance_sheet

dataframe	
column为balance_sheet的字段

index为序号（无意义）

income的字段说明：



字段名称	类型	字段说明	备注
MARKET_CODE	string	市场代码	
SECURITY_NAME	string	证券简称	
STATEMENT_TYPE	string	报表类型	参看报表类型代码表
REPORT_TYPE	string	报告期名称	
REPORTING_PERIOD	string	报告期	
ANN_DATE	string	公告日期	
ACTUAL_ANN_DATE	string	实际公告日期	
ACC_PAYABLE	float	应付票据及应付账款	
ACC_RECEIVABLE	float	应收票据及应收账款	
ACC_RECEIVABLES	float	应收款项	
ACCRUED_EXP	float	预提费用	
ACCT_PAYABLE	float	应付账款	
ACCT_RECEIVABLE	float	应收账款	
ACT_TRADING_SEC	float	代理买卖证券款	
ACT_UW_SEC	float	代理承销证券款	
ADV_PREM	float	预收保费	
ADV_RECEIPT	float	预收款项	
AGENCY_ASSETS	float	代理业务资产	
AGENCY_BUSINESS_LIAB	float	代理业务负债	
ANTICIPATION_LIAB	float	预计负债	
ASSET_DEP_FUNDS_OTH_FIN_INST	float	存放同业和其它金融机构款项	
BONDS_PAYABLE	float	应付债券	
CAP_RESV	float	资本公积金	
CAP_STOCK	float	股本	金额（元），公布值
CASH_CENTRAL_BANK_DEPOSITS	float	现金及存放中央银行款项	
CED_INSUR_CONT_RESERVES_RCV	float	应收分保合同准备金	
CLAIMS_PAYABLE	float	应付赔付款	
CLIENTS_FUND_DEPOSIT	float	客户资金存款	
CLIENTS_RESERVES	float	客户备付金	
CNVD_DIFF_FOREIGN_CURR_STAT	float	外币报表折算差额	
COMP_TYPE_CODE	int	公司类型代码	1：非金融类2：银行3：保险4：证券
CONST_IN_PROC	float	在建工程	
CONST_IN_PROC_TOTAL	float	在建工程(合计)(元)	
CONSUMP_BIO_ASSETS	float	消耗性生物资产	
CONT_ASSETS	float	合同资产	单位（元）
CONT_LIABILITIES	float	合同负债	单位（元）
CURRENCY_CAP	float	货币资金	
CURRENCY_CODE	float	货币代码	
DEBT_INV	float	债权投资(元)	
DEFERRED_INC_NONCUR_LIAB	float	递延收益-非流动负债	
DEFERRED_INCOME	float	递延收益	
DEFERRED_TAX_ASSETS	float	递延所得税资产	
DEFERRED_TAX_LIAB	float	递延所得税负债	
DEP_RECEIVED_IB_DEP	float	吸收存款及同业存放	
DEPOSIT_CAP_RECOG	float	存出资本保证金	
DEPOSIT_TAKING	float	吸收存款	
DEPOSITS_RECEIVED	float	存入保证金	
DER_FIN_ASSETS	float	衍生金融资产	
DERI_FIN_LIAB	float	衍生金融负债	
DEVELOP_EXP	float	开发支出	
DISPOSAL_FIX_ASSETS	float	固定资产清理	
DIV_PAYABLE	float	应付股利	
DIV_RECEIVABLE	float	应收股利	
EMPL_PAY_PAYABLE	float	应付职工薪酬	
ENGIN_MAT	float	工程物资	
FIN_ASSETS_AVA_FOR_SALE	float	可供出售金融资产	
FIN_ASSETS_COST_SHARING	float	以摊余成本计量的金融资产	
FIN_ASSETS_FAIR_VALUE	float	以公允价值计量且其变动计入其他综合收益的金融资产	
FIXED_ASSETS	float	固定资产	
FIXED_ASSETS_TOTAL	float	固定资产(合计)(元)	
FIXED_TERM_DEPOSITS	float	定期存款	
GOODWILL	float	商誉	
GUA_DEPOSITS_PAID	float	存出保证金	
GUA_PLEDGE_LOANS	float	保户质押贷款	
HOLD_ASSETS_FOR_SALE	float	持有待售的资产	
HOLD_TO_MTY_INV	float	持有至到期投资	
INC_PLEDGE_LOAN	float	其中:质押借款	
INCL_TRADING_SEAT_FEES	float	其中:交易席位费	
IND_ACCT_ASSETS	float	独立账户资产	
IND_ACCT_LIAB	float	独立账户负债	
INSURED_DEPOSIT_INV	float	保户储金及投资款	
INSURED_DIV_PAYABLE	float	应付保单红利	
INT_RECEIVABLE	float	应收利息	
INTANGIBLE_ASSETS	float	无形资产	
INTEREST_PAYABLE	float	应付利息	
INV	float	存货	
INV_REALESTATE	float	投资性房地产	
LEASE_LIABILITY	float	租赁负债	
LEND_FUNDS	float	融出资金	
LENDING_FUNDS	float	拆出资金	
LESS_TREASURY_STK	float	减:库存股	
LIA_HFS	float	持有待售的负债	
LIAB_DEP_FUNDS_OTH_FIN_INST	float	同业和其它金融机构存放款项	
LIFE_INSUR_RESV	float	寿险责任准备金	
LOAN_CENTRAL_BANK	float	向中央银行借款	
LOANS_AND_ADVANCES	float	发放贷款及垫款	
LOANS_FROM_OTH_BANKS	float	拆入资金	
LT_DEFERRED_EXP	float	长期待摊费用	
LT_EMP_COMP_PAY	float	长期应付职工薪酬	
LT_EQUITY_INV	float	长期股权投资	
LT_HEALTH_INSUR_RESV	float	长期健康险责任准备金	
LT_LOAN	float	长期借款	
LT_PAYABLE	float	长期应付款	
LT_PAYABLE_TOTAL	float	长期应付款(合计)(元)	
LT_RECEIVABLES	float	长期应收款	
MINORITY_EQUITY	float	少数股东权益	
NOM_RISKS_PREP	float	一般风险准备	
NONCUR_ASSETS_DUE_WITHIN_1Y	float	一年内到期的非流动资产	
NONCUR_LIAB_DUE_WITHIN_1Y	float	一年内到期的非流动负债	
NOTES_PAYABLE	float	应付票据	
NOTES_RECEIVABLE	float	应收票据	
OIL_AND_GAS_ASSETS	float	油气资产	
OTH_COMP_INCOME	float	其他综合收益	
OTH_EQUITY_TOOLS	float	其他权益工具	
OTH_EQUITY_TOOLS_PRE_SHR	float	其他权益工具:优先股	
OTH_NONCUR_ASSETS	float	其他非流动资产	
OTHER_ASSETS	float	其他资产	
OTHER_CUR_ASSETS	float	其他流动资产	
OTHER_CUR_LIAB	float	其他流动负债	
OTHER_DEBT_INV	float	其他债权投资(元)	
OTHER_EQUITY_INV	float	其他权益工具投资(元)	
OTHER_LIAB	float	其他负债	
OTHER_NONCUR_FIN_ASSETS	float	其他非流动金融资产(元)	
OTHER_NONCUR_LIAB	float	其他非流动负债	
OTHER_PAYABLE	float	其他应付款	
OTHER_PAYABLE_TOTAL	float	其他应付款(合计)(元)	
OTHER_RCV_TOTAL	float	其他应收款(合计)（元）	
OTHER_RECEIVABLE	float	其他应收款	
OTHER_SUSTAIN_BOND	float	其他权益工具:永续债(元)	
OUT_LOSS_RESV	float	未决赔款准备金	
PAYABLE	float	应付款项	
PAYABLE_FOR_REINSURER	float	应付分保账款	
PRECIOUS_METAL	float	贵金属	
PREPAYMENT	float	预付款项	
PROD_BIO_ASSETS	float	生产性生物资产	
RCV_CED_CLAIM_RESV	float	应收分保未决赔款准备金	
RCV_CED_LIFE_INSUR_RESV	float	应收分保寿险责任准备金	
RCV_CED_LT_HEALTH_INSUR_RESV	float	应收分保长期健康险责任准备金	
RCV_CED_UNEARNED_PREM_RESV	float	应收分保未到期责任准备金	
RCV_FINANCING	float	应收款项融资	
RCV_INV	float	应收款项类投资	
RECEIVABLE_PREM	float	应收保费	
RED_MON_CAP_FOR_SALE	float	买入返售金融资产	
REINSURANCE_ACC_RCV	float	应收分保账款	
RSRV_FUND_INSUR_CONT	float	保险合同准备金	
SELL_REPO_FIN_ASSETS	float	卖出回购金融资产款	
SERVICE_CHARGE_COMM_PAYABLE	float	应付手续费及佣金	
SETTLE_FUNDS	float	结算备付金	
SPE_ASSETS_BAL_DIFF	float	资产差额(特殊报表科目)	
SPE_CUR_ASSETS_DIFF	float	流动资产差额(特殊报表科目)	
SPE_CUR_LIAB_DIFF	float	流动负债差额(特殊报表科目)	
SPE_LIAB_BAL_DIFF	float	负债差额(特殊报表科目)	
SPE_LIAB_EQUITY_BAL_DIFF	float	负债及股东权益差额(特殊报表项目)	
SPE_NONCUR_ASSETS_DIFF	float	非流动资产差额(特殊报表科目)	
SPE_NONCUR_LIAB_DIFF	float	非流动负债差额(特殊报表科目)	
SPE_SHARE_EQUITY_BAL_DIFF	float	股东权益差额(特殊报表科目)	
SPECIAL_PAYABLE	float	专项应付款	
SPECIAL_RESV	float	专项储备	
ST_BONDS_PAYABLE	float	应付短期债券	
ST_BORROWING	float	短期借款	
ST_FIN_PAYABLE	float	应付短期融资款	
SUBR_RCV	float	应收代位追偿款	
SURPLUS_RESV	float	盈余公积金	
TAX_PAYABLE	float	应交税费	
TOT_ASSETS_BAL_DIFF	float	资产差额(合计平衡项目)	
TOT_CUR_ASSETS_DIFF	float	流动资产差额(合计平衡项目)	
TOT_CUR_LIAB_DIFF	float	流动负债差额(合计平衡项目)	
TOT_LIAB_BAL_DIFF	float	负债差额(合计平衡项目)	
TOT_LIAB_EQUITY_BAL_DIFF	float	负债及股东权益差额(合计平衡项目)	
TOT_NONCUR_ASSETS	float	非流动资产合计	
TOT_NONCUR_ASSETS_DIFF	float	非流动资产差额(合计平衡项目)	
TOT_NONCUR_LIAB_DIFF	float	非流动负债差额(合计平衡项目)	
TOT_SHARE	float	期末总股本	单位（股）
TOT_SHARE_EQUITY_BAL_DIFF	float	股东权益差额(合计平衡项目)	
TOT_SHARE_EQUITY_EXCL_MIN_INT	float	股东权益合计(不含少数股东权益)	
TOT_SHARE_EQUITY_INCL_MIN_INT	float	股东权益合计(含少数股东权益)	
TOTAL_ASSETS	float	资产总计	
TOTAL_CUR_ASSETS	float	流动资产合计	
TOTAL_CUR_LIAB	float	流动负债合计	
TOTAL_LIAB	float	负债合计	
TOTAL_LIAB_SHARE_EQUITY	float	负债及股东权益总计	
TOTAL_NONCUR_LIAB	float	非流动负债合计	
TRADING_FIN_LIAB	float	交易性金融负债	
TRADING_FINASSETS	float	交易性金融资产	
UNAMORTIZED_EXP	float	待摊费用	
UNCONFIRMED_INV_LOSS	float	未确认的投资损失	
UNDISTRIBUTED_PRO	float	未分配利润	
UNEARNED_PREM_RESV	float	未到期责任准备金	
USE_RIGHT_ASSETS	float	使用权资产	



报表类型

报表类型代码

备注

1

合并报表

涵盖母公司的财务报表数据，为最新报表

2

合并报表(单季度)

合并报表(单季度)=合并报表(本期)-合并报表(上一季)

3

合并报表(单季度调整)

合并报表(单季度调整)=合并报表(本期调整)-合并报表(上一季调整)

4

合并报表(调整)

本年度公布上年同期的财务报表数据，报告期为上年度

5

合并报表(更正前)

即出更正公告后，把合并报表的记录修改为合并报表(更正前)；复制原来的记录，更正后报表类型改为合并报表

6

母公司报表

该公司母公司的财务报表数据

7

母公司报表(单季度)

母公司报表(单季度)=母公司报表(本期)-母公司报表(上一季)

8

母公司报表(单季度调整)

母公司报表(单季度调整)=母公司报表(本期调整)-母公司报表(上一季调整)

9

母公司报表(调整)

该公司母公司的本年度公布上年同期的财务报表数据

10

母公司报表(更正前)

之前上市公司已披露财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。

19

合并报表(调整借壳前)

借壳前的合并报表(调整)

21

合并报表(单季度借壳前)

借壳前的合并报表(单季度)

22

合并报表(单季度调整借壳前)

借壳前的合并报表(单季度调整)

23

母公司报表(借壳前)

借壳前的母公司报表

24

母公司报表(调整借壳前)

借壳前的母公司报表(调整)

25

母公司报表(单季度借壳前)

借壳前的母公司报表(单季度)

26

母公司报表(单季度调整借壳前)

借壳前的母公司报表(单季度调整)

27

合并报表(第一次更正)

有多次更正时，合并报表的第一次更正

28

合并报表(第二次更正)

有多次更正时，合并报表的第二次更正

29

合并调整(第一次更正)

有多次更正时，合并调整的第一次更正

33

母公司调整(第一次更正)

有多次更正时，母公司调整的第一次更正

34

母公司报表(第二次更正)

有多次更正时，母公司报表的第二次更正

35

母公司报表(第一次更正)

有多次更正时，母公司报表的第一次更正

50

合并调整(更正前)

即出更正公告后，把合并报表（调整）的记录修改为合并调整(更正前)；复制原来的记录，更正后报表类型改为合并报表(调整)

60

母公司调整(更正前)

该公司母公司的本年度公布上年同期的财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。

二、api案例

import AmazingData as ad
ad.login(username='username',
password='password',
host='***.***.***.***',port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,                                                        
end_date=today)balance_sheet = info_data_object.get_balance_sheet(all_code_list)


图片

【AmazingData】获取上市公司的龙虎榜数据


一、获取上市公司的龙虎榜

函数接口：get_long_hu_bang

功能描述：获取指定股票列表的上市公司的龙虎榜数据

输入参数：  

参数	 数据类型	必选	解释
code_list	list[str] 	是	支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
long_hu_bang	dataframe	
column为long_hu_bang的字段

index为序号（无意义）

long_hu_bang的字段说明：

参数	 数据类型	字段说明	备注
MARKET_CODE	string	市场代码	
TRADE_DATE	string	日期	
SECURITY_NAME	string	证券名称	
REASON_TYPE	string	上榜原因类型	
REASON_TYPE_NAME	string	上榜原因	
CHANGE_RANGE	float	涨跌幅（%）	
TRADER_NAME	string	营业部名称	
BUY_AMOUNT	float	买入金额（元）	
SELL_AMOUNT	float	卖出金额（元）	
FLOW_MARK	int	买卖表示	1表示买入，2表示卖出
TOTAL_AMOUNT	float	实际交易金额（元）	
TOTAL_VOLUME	float	实际交易量（万股）	


二、api案例

import AmazingData as ad
ad.login(username='username',
password='password', 
host='***.***.***.***',
port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
                                                        end_date=today)

long_hu_bang = info_data_object.get_long_hu_bang(all_code_list)


图片

【AmazingData】获取上市公司的历史证券信息数据

AmazingData系列文章

1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询
5. K线的合成算法介绍、如何订阅实时K线？
6. 如何获取历史K线数据
7. “历史代码表”数据为例——本地化的金融量化数据库解决方案
8. 复权算法解析与复权因子数据api介绍
9. 获取每日最新证券信息
10.获取大宗交易数据
11.获取上市公司十大股东数据
12.获取上市公司的业绩预告数据
13.获取上市公司的业绩快报数据
一、获取上市公司的历史证券信息

函数接口：get_history_stock_status

功能描述：获取指定股票列表的上市公司的历史证券数据

输入参数：  

参数	 数据类型	必选	解释
code_list	list[str] 	是	支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
history_stock_status	dataframe	
column为history_stock_status的字段

index为序号（无意义）

history_stock_status的字段说明：

参数	 数据类型	字段说明	备注
MARKET_CODE	string	市场代码	
TRADE_DATE	string	日期	
PRECLOSE	float	前收价

HIGH_LIMITED	float	涨停价

LOW_LIMITED	float	跌停价

PRICE_HIGH_LMT_RATE	float	涨停价上限

PRICE_LOW_LMT_RATE	float	跌停价下限	
IS_ST_SEC	string	是否ST	1表示是，0表示否
IS_SUSP_SEC	string	是否停牌	1表示是，0表示否
IS_WD_SEC	string	是否除息	1表示是，0表示否
IS_XR_SEC	string	是否除权	1表示是，0表示否


二、api案例

import AmazingData as ad
ad.login(username='username',
password='password', 
host='***.***.***.***',
port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
                                                        end_date=today)

history_stock_status = info_data_object.get_history_stock_status(all_code_list)


图片

【AmazingData】获取上市公司的业绩快报数据

AmazingData系列文章

1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询
5. K线的合成算法介绍、如何订阅实时K线？
6. 如何获取历史K线数据
7. “历史代码表”数据为例——本地化的金融量化数据库解决方案
8. 复权算法解析与复权因子数据api介绍
9. 获取每日最新证券信息
10.获取大宗交易数据
11.获取上市公司十大股东数据
12.获取上市公司的业绩预告数据
一、获取上市公司的业绩快报

函数接口：get_profit_express

功能描述：获取指定股票列表的上市公司的业绩快报数据

输入参数：  

参数	 数据类型	必选	解释
code_list	list[str] 	是	支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
profit_express	dataframe	
column为profit_excess的字段

index为序号（无意义）

profit_express的字段说明：

参数	 数据类型	字段说明	备注
MARKET_CODE	string	市场代码	
REPORTING_PERIOD	string	报告期	报告内容记录的截止时间点，报告成果的时期
ANN_DATE	string	公告日期	公告发布当天的日期；有多个阶段的事件，首次披露该事件的日期
ACTUAL_ANN_DATE	string	实际公告日期	实际数据来源公告的日期；更正发生公告的日期
TOTAL_ASSETS	float64	总资产(元)	指经济实体拥有或控制的能带来经济利益的全部资产
NET_PRO_EXCL_MIN_INT_INC	float64	净利润(元)	企业合并净利润中归属于母公司股东所有的那部分利润
TOT_OPERA_REV	float64	营业总收入(元)	企业从事销售商品、提供劳务和让渡资产使用权等日常业务过程形成的经济利益的总流入
TOTAL_PROFIT	float64	利润总额(元)	企业一定时期内的纯收入扣除应交纳后的余额
OPERA_PROFIT	float64	营业利润(元)	企业在其全部销售业务中实现的利润
EPS_BASIC	float64	每股收益-基本(元)	企业按照属于普通股股东的当期净利润，除以发行在外普通股的加权平均数计算得到的每股收益
TOT_SHARE_EQU_EXCL_MIN_INT	float64	股东权益合计(不含少数股东权益)(元)	公司集团的所有者权益中归属于母公司所有者权益的部分
IS_AUDIT	float64	是否审计	1:是 0：否
ROE_WEIGHTED	float64	净资产收益率-加权(%)	经营期间净资产赚取利润的结果的一个动态指标，反应企业净资产创造利润的能力
LAST_YEAR_REVISED_NET_PRO	float64	去年同期修正后净利润	元
PERFORMANCE_SUMMARY	string	业绩简要说明	针对业绩快报的简单说明
NET_ASSET_PS	float64	每股净资产	元
MEMO	string	备注	附加的注解说明
YOY_GR_GROSS_PRO	float64	同比增长率:营业利润	%
YOY_GR_GROSS_REV	float64	同比增长率:营业总收入	%
YOY_GR_NET_PROFIT_PARENT	float64	同比增长率:归属母公司股东的净利润	%
YOY_GR_TOT_PRO	float64	同比增长率:利润总额	%
YOY_ID_WAROE	float64	同比增减:加权平均净资产收益率	%
YOY_GR_EPS_BASIC	float64	同比增长率:基本每股收益	%
GROWTH_RATE_EQUITY	float64	比年初增长率:归属母公司的股东权益	%
GROWTH_RATE_ASSETS	float64	比年初增长率:总资产	%
GROWTH_RATE_NAPS	float64	比年初增长率:归属于母公司股东的每股净资产	%
LAST_YEAR_TOT_OPERA_REV	float64	去年同期营业总收入	元
LAST_YEAR_TOTAL_PROFIT	float64	去年同期利润总额	元
LAST_YEAR_OPERA_PRO	float64	去年同期营业利润	元
LAST_YEAR_EPS_DILUTED	float64	去年同期每股收益	元
LAST_YEAR_NET_PROFIT	float64	去年同期净利润	元
INITIAL_NET_ASSET_PS	float64	期初每股净资产	元
INITIAL_NET_ASSETS	float64	期初净资产	元

二、api案例

import AmazingData as ad
ad.login(username='username',
password='password', 
host='***.***.***.***',
port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
                                                        end_date=today)

profit_express = info_data_object.get_profit_express(all_code_list)


图片

【AmazingData】获取上市公司的业绩预告数据

AmazingData系列文章

1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询
5. K线的合成算法介绍、如何订阅实时K线？
6. 如何获取历史K线数据
7. “历史代码表”数据为例——本地化的金融量化数据库解决方案
8. 复权算法解析与复权因子数据api介绍
9. 获取每日最新证券信息
10.获取大宗交易数据
11.获取上市公司十大股东数据
一、获取上市公司的业绩预告

函数接口：get_profit_notice

功能描述：获取指定股票列表的上市公司的业绩预告数据

输入参数：  

参数	 数据类型	必选	解释
code_list	list[str] 	是	支持沪深A股的代码列表，可见示例
local_path

str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
is_local

bool	否
默认为True，首选从本地读取，读取失败再从服务器取数据

False，以本地数据为基础，增量从服务器取数据

输出参数：  

返回值	 数据类型	解释
profit_notice	dataframe	
column为profit_notice的字段

index为序号（无意义）

profit_notice的字段说明：

参数	 数据类型	字段说明	备注
MARKET_CODE	string	市场代码	
SECURITY_NAME	string	证券简称	
P_TYPECODE	string	业绩预告类型代码	
1：不确定

2：略减

3：略增

4：扭亏

5：其他

6：首亏

7：续亏

8：续盈

9：预减

10：预增

11：持平
REPORTING_PERIOD	string	报告期	分为年度、半年度、季度
ANN_DATE	string	公告日期	公告发布当天的日期
P_CHANGE_MAX	float64	预告净利润变动幅度上限（%）	对于净利润金额同比变动幅度预计的最高值
P_CHANGE_MIN	float64	预告净利润变动幅度下限（%）	对于净利润金额同比变动幅度预计的最低值
NET_PROFIT_MAX	float64	预告净利润上限（万元）	对于净利润金额预计的最高值
NET_PROFIT_MIN	float64	预告净利润下限（万元）	对于净利润金额预计的最低值
FIRST_ANN_DATE	string	首次公告日	首次披露本报告期业绩预告内容的公告日期
P_NUMBER	float64	公布次数	同一报告期的业绩预告公告的披露次数
P_REASON	string	业绩变动原因	
P_SUMMARY	string	业绩预告摘要	
P_NET_PARENT_FIRM	float64	上年同期归母净利润	业绩预告中直接公布的上年同期归母净利润
REPORT_TYPE	string	报告期名称	
1：一季度报表

2：中期报表

3：三季度报表

4：年度报表

二、api案例

import AmazingData as ad
ad.login(username='username',
password='password', 
host='***.***.***.***',
port=****) 
info_data_object = ad.InfoData()
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
today = calendar[-1]
all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
                                                        end_date=today)

profit_notice = info_data_object.get_profit_notice(all_code_list)


图片

【模型篇】大宗交易数据的应用
图片

一、指定日期分析沪深A股大宗交易情况

计算指定日期全部股票的总成交金额、总成交笔数和总股票数量，

明细数据包含如下数据：

日期

代码

证券简称

成交价（元）

当日收盘价

溢价率（%）

成交量（万股）

笔数

成交金额（万元）

每笔成交数量（万股）

买方营业部名称

卖方营业部名称

输出excel示例如下：

图片

图片

 （仅为效果展示，不保证数据准确，不可作为投资依据）

‌二、指定代码分析历史大宗交易情况

计算指定代码全部历史日期的总成交金额、总成交笔数和总成交天数

明细数据包含如下数据：

日期

代码

证券简称

成交价（元）

当日收盘价

溢价率（%）

成交量（万股）

笔数

成交金额（万元）

每笔成交数量（万股）

买方营业部名称

卖方营业部名称

输出excel示例如下：

图片

图片

 （仅为效果展示，不保证数据准确，不可作为投资依据）



三、模型源码

# -*- coding: utf-8 -*-

# ------------------------------
# @Time    : 2024/11/25
# @Author  : gao
# @File    : info_data_analysis.py
# @Project : AmazingData 
# ------------------------------
import pandas as pd

import AmazingData as ad
from AmazingData.utils.data_transfer import date_to_datetime
import config_user

if __name__ == '__main__':
    ad.login(username=config_user.user['username'],
             password=config_user.user['password'],
             host=config_user.user['host'],
             port=config_user.user['port'])
    info_data_object = ad.InfoData()
    base_data_object = ad.BaseData()
    calendar = base_data_object.get_calendar()
    today = calendar[-1]
    all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
                                                        end_date=today)

    block_trading = info_data_object.get_block_trading(all_code_list)


    # 指定日期分析沪深A股大宗交易情况
    date = '20241125'
    block_trading_date = block_trading[block_trading['TRADE_DATE'] == date].reset_index(drop=True)
    total_amount = round(block_trading_date['B_SHARE_AMOUNT'].sum(), 2)
    total_trade_nums = block_trading_date['B_FREQUENCY'].sum()
    total_stock_nums = block_trading_date['MARKET_CODE'].nunique()
    total_stock_list = list(block_trading_date['MARKET_CODE'].unique())
    market_data_object = ad.MarketData(calendar)
    kline_dict = market_data_object.query_kline(total_stock_list, begin_date=int(date), end_date=int(date),
                                                period=ad.constant.Period.day.value)

    block_trading_date['close'] = block_trading_date.apply(lambda x: kline_dict[x['MARKET_CODE']]['close'].values[0],
                                                           axis=1)
    block_trading_date['premium_ratio'] = block_trading_date.apply(
        lambda x: round((x['B_SHARE_PRICE'] / x['close'] - 1) * 100, 2),
        axis=1)
    column_dict = {'TRADE_DATE': '日期', 'MARKET_CODE': '代码', 'SECURITY_NAME': '证券简称',
                   'B_SHARE_PRICE': '成交价（元）', 'close': '当日收盘价', 'premium_ratio': '溢价率（%）',
                   'B_SHARE_VOLUME': '成交量（万股）', 'B_FREQUENCY': '笔数', 'B_SHARE_AMOUNT': '成交金额（万元）',
                   'BLOCK_AVG_VOLUME': '每笔成交数量（万股）',
                   'B_BUYER_NAME': '买方营业部名称', 'B_SELLER_NAME': '卖方营业部名称',
                   }

    block_trading_date.rename(columns=column_dict, inplace=True)
    block_trading_date = block_trading_date[list(column_dict.values())]

    message = f"""总成交金额为{total_amount}（万元）,
                总成交笔数{total_trade_nums}（笔）,
                总股票数量{total_stock_nums}（只）,
                """
    message_df = pd.DataFrame([message], columns=['日期'], index=['总结'])
    # 合并两个DataFrame
    block_trading_date = pd.concat([block_trading_date, message_df]).reset_index(drop=True)

    block_trading_date.to_excel(f'{date} 沪深A股大宗交易情况.xlsx')

    # 指定代码分析历史大宗交易情况
    code = '00***6.SZ'
    block_trading_code = block_trading[block_trading['MARKET_CODE'] == code].reset_index(drop=True)
    total_day_nums = block_trading_code['TRADE_DATE'].nunique()
    total_code_amount = round(block_trading_code['B_SHARE_AMOUNT'].sum(), 2)
    total_code_trade_nums = block_trading_code['B_FREQUENCY'].sum()

    start_date = block_trading_code['TRADE_DATE'].min()
    end_date = block_trading_code['TRADE_DATE'].max()

    kline_code_dict = market_data_object.query_kline([code], begin_date=int(start_date), end_date=int(end_date),
                                                     period=ad.constant.Period.day.value)
    kline_code = kline_code_dict[code]


    def get_date_close(x, kline_code):
        trade_date = date_to_datetime(x['TRADE_DATE'])
        try:
            close = kline_code[kline_code['kline_time'] == trade_date]['close'].values[0]
        except:
            close = None
        return close


    block_trading_code['close'] = block_trading_code.apply(lambda x: get_date_close(x, kline_code), axis=1)
    block_trading_code['premium_ratio'] = block_trading_code.apply(
        lambda x: round((x['B_SHARE_PRICE'] / x['close'] - 1) * 100, 2),
        axis=1)
    block_trading_code.rename(columns=column_dict, inplace=True)
    block_trading_code = block_trading_code[list(column_dict.values())]

    code_message = f"""总成交金额为{total_code_amount}（万元）,
                总成交笔数{total_code_trade_nums}（笔）,
                总成交天数{total_day_nums}（只）,
                """
    code_message_df = pd.DataFrame([code_message], columns=['日期'], index=['总结'])
    # 合并两个DataFrame
    block_trading_code = pd.concat([block_trading_code, code_message_df]).reset_index(drop=True)
    block_trading_code.to_excel(f'{code} 历史大宗交易情况.xlsx')


AmazingData系列文章

1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询
5. K线的合成算法介绍、如何订阅实时K线？
6. 如何获取历史K线数据
7. “历史代码表”数据为例——本地化的金融量化数据库解决方案
8. 复权算法解析与复权因子数据api介绍
9. 获取每日最新证券信息
图片


【模型篇】如何实时分析ETF的溢价率？
图片

ETF的折溢价率是指ETF在‌二级市场上的‌交易价格与其‌净值之间的差异。本篇文章将介绍利用实时数据（频率为3秒间隔）分析ETF折溢价率的模型算法，并用AmazingData程序实现。

一、ETF的折溢价率计算方法

1. 折价率

当ETF在二级市场上的交易价格低于其净值时，称为折价。

折价率 =（实时估值-最新价）/ 实时估值*100%，数据为正，意为折价。

2. 溢价率

当ETF在二级市场上的交易价格高于其净值时，称为溢价。

溢价率 =（最新价-实时估值）/ 实时估值*100%，数据为正，意为溢价。

‌二、ETF的折溢价率形成机制

ETF的折溢价率主要是由于其特殊的申赎交易机制导致的。ETF既可以在一级市场向‌基金公司申购或赎回基金份额，也可以在二级市场上按市场价格买入或卖出基金份额。由于一、二级市场之间的交易机制不同，导致两者之间的价格存在差异，从而形成折溢价。

三、ETF折溢价率的影响因素

ETF的折溢价率受到多种因素的影响。

首先，‌成份股的价格波动会影响ETF的净值，进而影响其折溢价率。例如，当成份股价格上涨时，ETF的净值上升，如果二级市场上的交易价格未能及时反映这一变化，就会导致折价；反之，成份股价格下跌时，如果二级市场上的交易价格仍然较高，就会导致溢价。

其次，市场供求关系也会影响ETF的折溢价率。当市场需求大于供应时，交易价格可能会高于净值，反之则可能会低于净值。

‌四、ETF折溢价率对于ETF交易时点的意义

ETF的折溢价率是衡量市场效率的重要指标。例如，当ETF折价时，投资者买入成本二级市场买入比一级市场申购更划算；反之，当ETF溢价时，投资者买入成本一级市场申购比二级市场买入更划算。

五、利用AmazingData实时计算ETF的折溢价率

溢价率效果示例如下：

图片

图片

         （仅为效果展示，不保证数据准确，不可作为投资依据）

五、利用AmazingData实时计算ETF的源码

# -*- coding: utf-8 -*-

# ------------------------------
# @Time    : 2024/11/7
# @Author  : gao
# @File    : etf_iopv.py 
# @Project : AmazingData 
# ------------------------------
from typing import Union

import pandas as pd

import AmazingData as ad
import config_user

import warnings

warnings.filterwarnings('ignore')

premium_rank = 10
discount_rank = 10

ad.login(username=config_user.user['username'],
         password=config_user.user['password'],
         host=config_user.user['host'],
         port=config_user.user['port'])
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()

code_info = base_data_object.get_code_info(security_type='EXTRA_ETF')
code_name_dict = dict(code_info['symbol'])
code_list = list(code_name_dict.keys())
# 溢价率 =（最新价-实时估值）/ 实时估值*100%，数据为正，意为溢价
premium_dict = {}
code_dict = {}
volume_dict = {}
amount_dict = {}
market_data_object = ad.MarketData(calendar)

sub_data = ad.SubscribeData()


@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshot.value)
def onSnapshot(data: Union[ad.constant.Snapshot, ad.constant.SnapshotIndex], period):
    code = data.code
    code_name = code_name_dict[code]
    if data.iopv > 0:
        code_dict[code_name] = code
        premium_dict[code_name] = round((data.last - data.iopv) / data.iopv * 100, 2)
        volume_dict[code_name] = round(data.volume / 1000000, 2)
        amount_dict[code_name] = round(data.amount / 100000000, 2)

        premium_df = pd.DataFrame([code_dict, premium_dict, volume_dict, amount_dict],
                                  index=['代码', '溢价率(%)', '成交量(万手)', '成交额(亿)']).T
        premium_df = premium_df.sort_values(by='溢价率(%)', ascending=False)

        premium_rank_df = premium_df.head(premium_rank)
        discount_rank_df = premium_df.tail(discount_rank).sort_values(by='溢价率(%)')
        print(
            f'{data.trade_time}\n溢价率前{premium_rank}名：\n{premium_rank_df}\n溢价率后{discount_rank}名：\n{discount_rank_df}')


sub_data.run()

AmazingData系列文章

1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询
5. K线的合成算法介绍、如何订阅实时K线？
6. 如何获取历史K线数据
7. “历史代码表”数据为例——本地化的金融量化数据库解决方案
8. 复权算法解析与复权因子数据api介绍
9. 获取每日最新证券信息
图片

【AmazingData】获取每日最新证券信息
图片

AmazingData系列文章

1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询
5. K线的合成算法介绍、如何订阅实时K线？
6. 如何获取历史K线数据
7. “历史代码表”数据为例——本地化的金融量化数据库解决方案
8. 复权算法解析与复权因子数据api介绍


一、获取每日最新证券信息

函数接口：get_code_info

功能描述：获取每日最新证券信息

输入参数：  

参数	 数据类型	必选	解释
security_type	str 	否	代码类型security_type（见附录），默认为EXTRA_STOCK_A（上交所A股、深交所A股和北交所的股票列表）
输出参数：  

返回值	 数据类型	解释
code_info	dataframe	
index为股票代码

column为

symbol (证券简称)
pre_close (昨收价)
high_limited  (涨停价)
low_limited ( 跌停价)
price_tick (最小价格变动单位)

import AmazingData as ad
ad.login(username='username',
password='password', 
host='***.***.***.***',
 port=****) 
base_data_object = ad.BaseData()
code_info = base_data_object.get_code_info(security_type='EXTRA_ETF')
三、附录
1.  security_type

字段描述：代码类型

数据类型

枚举值

说明

str

EXTRA_STOCK_A


上交所A股、深交所A股和北交所的股票列表

str

EXTRA_IDNEX_A

上交所、深交所和北交所的指数列表

str

EXTRA_ETF

上交所、深交所的ETF列表

str

EXTRA_STOCK_A_SH_SZ

上交所A股和深交所A股的股票列表
str

EXTRA_IDNEX_A_SH_SZ

上交所和深交所指数列表
str

SH_A

上交所A股的股票列表
str

SZ_A

深交所A股的股票列表
str

BJ_A

北交所的股票列表
str

SH_INDEX

上交所指数列表
str	
SZ_INDEX

深交所指数列表
str

BJ_INDEX

北交所的指数列表
str

SH_ETF

上交所的ETF列表
str

SZ_ETF

深交所的ETF列表


图片

【模型篇】如何实时分析市场的成交量变化情况？
图片

市场成交量是反映市场热度的重要指标之一，本篇文章将介绍利用实时数据（频率为3秒间隔）分析成交量的模型算法，并用AmazingData程序实现。

一、实时分析成交量的模型算法

市场成交量分析，选取9种算法供参考。

介绍相关算法细节如下：

1. 成交量、成交额

即指数的实时当日成交量和成交额。

2. 当日相对于前N日的成交额变化率、当日相对于前N日的成交量变化率

成交额和成交量，用当日数据与前N日数据相比较，做时间对齐（比如10:00:00时，前N日数据选取10:00:00的成交量和成交额），计算变化率。

3. 当日虚拟成交额、当日虚拟成交量

当日虚拟成交额 = 当日相对于前N日的成交额变化率 * 前N日的成交额；

当日虚拟成交量 = 当日相对于前N日的成交额变化率 * 前N日的成交量；

4. 最近M秒涨跌幅

取最近M秒的快照数据，计算涨跌幅

5. 最近M秒相对于前N日的M秒成交量变化率

最近M秒的成交量与前N日的M秒成交量相比较，计算成交量变化率；反映最近M秒的成交量相对与前几日的成交量变化情况

6. 最近前[M/2, 0]秒相对于前[M, M/2]秒成交量变化率

反映最近短时间内的成交量变化情况

二、利用AmazingData，实时分析市场成交量

N=5，M=120，选上证指数（000001.SH）、深证综指（399106.SZ）和创业板指（399102.SZ）三个指数作为示例，每3秒更新一次计算结果

# -*- coding: utf-8 -*-

# ------------------------------
# @Time    : 2024/10/9
# @Author  : gao
# @File    : market_volume1.py 
# @Project : AmazingData 
# ------------------------------
from datetime import datetime, timedelta, time

import numpy as np

from typing import Union
import AmazingData as ad
import config_user

from AmazingData.utils.data_transfer import datetime_to_int_millisecond

# import warnings
# warnings.filterwarnings('ignore')

day_num = 5
timedelta_seconds = 120

ad.login(username=config_user.user['username'],
         password=config_user.user['password'],
         host=config_user.user['host'],
         port=config_user.user['port'])
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()

index_code_dict = {'000001.SH': '上证指数', '399106.SZ': '深证综指', '399102.SZ': '创业板指'}

code_list = list(index_code_dict.keys())
market_data_object = ad.MarketData(calendar)
begin_date = calendar[-day_num - 1]
end_date = calendar[-2]

snapshot_dict = market_data_object.query_snapshot(code_list, begin_date=begin_date, end_date=end_date)

sub_data = ad.SubscribeData()


@sub_data.register(code_list=code_list, period=ad.constant.Period.snapshot.value)
def onSnapshot(data: Union[ad.constant.Snapshot, ad.constant.SnapshotIndex], period):
    code = data.code
    code_name = index_code_dict[data.code]
    print(f"-----------------{code_name}（{code}）行情刷新开始-----------------")
    print(f'当前时间：{data.trade_time}')
    trade_time = data.trade_time
    now_time = time(trade_time.hour, trade_time.minute, trade_time.second)
    print(f'指数名称：{code_name}')

    recent_timetag = trade_time - timedelta(seconds=timedelta_seconds)
    # print(recent_timetag)
    recent_timetag_int = datetime_to_int_millisecond(recent_timetag)
    recent_time = str(recent_timetag_int)[8:]
    if now_time < time(9, 15, 0):
        print('未开盘')
    else:
        total_value_now_list = []
        total_volume_now_list = []
        total_value_all_list = []
        total_volume_all_list = []
        recent_index_volume_list = []
        for date in snapshot_dict:
            date_datetime = datetime.strptime(str(date), "%Y%m%d")
            date_datetime = datetime.combine(date_datetime, now_time)
            index_tick = snapshot_dict[date][code]
            now_index_tick_all = index_tick[index_tick['trade_time'] <= date_datetime]
            
            total_value_now_list.append(now_index_tick_all.iloc[-1, :]['amount'])
            total_volume_now_list.append(now_index_tick_all.iloc[-1, :]['volume'])
            total_value_all_list.append(index_tick.iloc[-1, :]['amount'])
            total_volume_all_list.append(index_tick.iloc[-1, :]['volume'])
            recent_timetag_date_datetime = datetime.combine(date_datetime,
                                                            time(recent_timetag.hour, recent_timetag.minute,
                                                                 recent_timetag.second))
            recent_index_tick = now_index_tick_all[now_index_tick_all['trade_time'] >= recent_timetag_date_datetime]

            if not recent_index_tick.empty:
                recent_index_volume_list.append(
                    recent_index_tick.iloc[-1, :]['volume'] - recent_index_tick.iloc[0, :]['volume'])

        amount_average = np.mean(total_value_now_list)
        volume_average = np.mean(total_volume_now_list)

        total_value_all_average = np.mean(total_value_all_list)
        total_volume_all_average = np.mean(total_volume_all_list)


        total_value_now = data.amount
        total_volume_now = data.volume
        value_day_num_ratio = round((total_value_now / amount_average) * 100, 2)
        volume_day_num_ratio = round((total_volume_now / volume_average) * 100, 2)

        today_all_value = round(value_day_num_ratio * total_value_all_average / 100 / 100000000, 2)
        today_all_volume = round(volume_day_num_ratio * total_volume_all_average / 100 / 100000000, 2)

        print(f"{code_name}（{code}）当日最新成交额：{round(total_value_now / 100000000, 2)}亿元")
        print(f"{code_name}（{code}）当日最新成交量：{round(total_volume_now / 100000000, 2)}亿手")
        print(f"{code_name}（{code}）当日相对于前{str(day_num)}日的成交额变化率：{value_day_num_ratio}（%）")
        print(f"{code_name}（{code}）当日相对于前{str(day_num)}日的成交量变化率：{volume_day_num_ratio}（%）")
        print(f"{code_name}（{code}）当日虚拟成交额：{today_all_value}亿元")
        print(f"{code_name}（{code}）当日虚拟成交量：{today_all_volume}亿手")

        recent_index_volume = np.mean(recent_index_volume_list)
        index_snapshot_dict = market_data_object.query_snapshot([code], begin_date=calendar[-1], end_date=calendar[-1])

        index_df = index_snapshot_dict[calendar[-1]][code]

        today_recent_index_tick = index_df[index_df['trade_time'] >= recent_timetag]

        today_recent_close_ratio = (today_recent_index_tick.iloc[-1, :]['last'] -
                                    today_recent_index_tick.iloc[0, :]['last']) / \
                                   today_recent_index_tick.iloc[0, :]['last']

        print(f"{code_name}（{code}）最近{str(timedelta_seconds)}秒涨跌幅：{round(today_recent_close_ratio * 100, 4)}%")

        today_recent_volume = today_recent_index_tick.iloc[-1, :]['volume'] - today_recent_index_tick.iloc[0, :][
            'volume']

        if today_recent_volume > 0:
            recent_volume_ratio = round(100 * today_recent_volume / recent_index_volume, 2)
            recent_volume_ratio = str(recent_volume_ratio) + "%"
        else:
            recent_volume_ratio = f'最近前{str(timedelta_seconds)}秒无成交'
        print(f'{code_name}（{code}）最近{str(timedelta_seconds)}秒相对于前{str(day_num)}日成交量变化率：{recent_volume_ratio}')

        half_today_recent_index = int(today_recent_index_tick.shape[0] / 2)
        recent_index_volume1 = today_recent_index_tick.iloc[-half_today_recent_index, :]['volume'] - \
                               today_recent_index_tick.iloc[0, :]['volume']

        recent_index_volume2 = today_recent_index_tick.iloc[-1, :]['volume'] - \
                               today_recent_index_tick.iloc[-half_today_recent_index, :]['volume']
        timedelta_seconds_half = int(timedelta_seconds / 2)
        if recent_index_volume1 > 0:
            recent_volume_ratio1 = round(100 * recent_index_volume2 / recent_index_volume1, 2)
            recent_volume_ratio1 = str(recent_volume_ratio1) + "%"
        else:
            recent_volume_ratio1 = f'最近前{str(timedelta_seconds / 2)}-{str(timedelta_seconds)}秒无成交'
        print(
            f'{code_name}（{code}）最近前[{str(timedelta_seconds_half)}, 0]秒相对于前[{str(timedelta_seconds)}, {str(timedelta_seconds_half)}]秒成交量变化率：{recent_volume_ratio1}')

    print(f"-----------------{code_name}（{code}）行情刷新结束-----------------")


sub_data.run()

三、模型计算结果示例

图片



AmazingData系列文章

1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询
5. K线的合成算法介绍、如何订阅实时K线？
6. 如何获取历史K线数据
7. “历史代码表”数据为例——本地化的金融量化数据库解决方案
8. 复权算法解析与复权因子数据api介绍
图片


【AmazingData】复权算法解析与复权因子数据api介绍
图片

AmazingData系列文章
1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询
5. K线的合成算法介绍、如何订阅实时K线？
6. 如何获取历史K线数据
一、为什么要对行情数据进行复权？

一只股票在上市期间，上市公司的分红配送，会导致除权除息。除权除息股价的不可比性，对基于历史数据的指标因子计算造成困难，比如出现涨跌幅不正常等现象。

因此，复权，即对股价和成交量进行权息修复，按照股票的实际涨跌绘制股价走势图，复权因子并把成交量调整为相同的股本口径。

二、复权算法介绍

1. 单次复权因子

单次复权因子的计算有两种计算方法：

（1） 根据交易所行情数据计算
       这种计算方式与交易所价格一致，但策略回测时的收益计算不包含分红再投资收益，对收益率有一定影响。

（2） 根据除权除息数据计算

比例 = 送股比例 + 转增比例 + 缩减比例 
复
权
因
子
股
权
登
记
日
收
盘
价
比
例
配
股
比
例
增
发
比
例
股
权
登
记
日
收
盘
价
派
息
比
例
股
权
登
记
日
收
盘
价
比
例
配
股
价
格
配
股
比
例
增
发
价
格
增
发
比
例
 

        这种计算方式与交易所价格不一致，但策略回测时的收益计算包含分红再投资收益。

       第一种方式简单实用，第二种方式回测的收益计算更准确。



 2. 前复权

      最近的交易日作为基点，原始行情数据乘以前复权因子，得到前复权行情数据，从使得最新的真实行情数据与前复权行情数据相等；

      前复权的优点在于，指标计算的最新行情数据与委托价格相等；



3. 后复权

       最早的交易日作为基点，原始行情数据乘以后前复权因子，得到前复权行情数据，从使得最新的真实行情数据与前复权行情数据相等；

       后复权的优点在于，策略回测可避免一部分因分红配股产生的未来数据；



4. 累计复权因子计算

      为方便与原始行情数据相乘，累计复权因子的数据结构为矩阵，column为股票代码，index为交易日历。

       后复权因子的计算步骤如下：

       （1）单次复权因子累乘；

       （2）矩阵的index，从只有股份变动日扩充为全历史交易日历，并赋值nan延续前值；

       （3）由于上市初期的一段时间可能没有分红配股，所以赋值这段时间的为1；        

        前复权因子等于后复权因子除以最新的单次复权因子；



三、AmazingData 复权因子的api接口
函数接口：BaseData.get_backward_factor
功能描述：获取复权因子数据并本地存储，复权因子为根据交易所行情数据计算；
输入参数：  
参数	 数据类型	必选	解释
code_list

lis[str]	是	代码列表，支持股票、ETF
local_path

str 	是	本地存储复权因子数据的文件夹地址
is_local	Bool	是	
是否使用本地存储的数据，默认为Ture

注：

（1）local_path

类似'D://AmazingData_local_data//'，只写文件夹的绝对路径即可

（2）is_local

True: 

本地local_path有数据的情况下，从本地取数据，但有可能无法获取最新的数据

本地local_path无数据的情况下，从互联网取数据，并更新本地local_path的数据

False:从互联网取数据，并更新本地local_path的数据

import AmazingData as ad
ad.login(username='username', password='password', host='***.***.***.***', port=****) 
base_data_object = ad.BaseData()
code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A')
backward_factor = base_data_object.get_backward_factor(code_list, local_path='D://AmazingData_local_data//', is_local=False)
图片



【AmazingData】以“历史代码表”数据为例——本地化的金融量化数据库解决方案
图片

一、为什么要建立本地量化金融数据库

AmazingData 的原生sdk是通过在客户端从服务端获取数据，受数据传输的时延包含服务端排队限制、网络的带宽限制、网络传输速度等原因限制获取速度。

然而，量化策略的开发过程中，需要无数次的调用数据回测验证策略的有效性，为了提升获取数据的速度，AmazingData提供本地化的金融量化数据库解决方案。首次获取数据从服务端获取，第二次获取即可从本地获取，减少数据传输的时延。

本地化的金融量化数据库解决方案简要介绍

1. download_data模块 下载各类数据的接口，用户可做成每日的定时任务。

2. local_path参数，可设置本地数据的存储路径。

3. 获取数据api，先从本地获取数据，如本地无数据，则自动从服务端拉取数据并保存本地之后返回数据。

4. 用户无需关心本地数据的存储格式，仅需从api获取数据之后在内存中使用数据即可。

二、下载“历史代码表”数据，本地计算机落地文件

函数接口：download_data模块，DownloadInfoData类，download_hist_code_list函数
功能描述：下载历史代码表（从2013年1月1日开始的AmazingData支持的所有行情数据代码），供查询历史行情使用 
DownloadInfoData输入参数：  

参数	 数据类型	必选	解释
local_path	str 	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”
download_hist_code_list输入参数：  

参数	 数据类型	必选	解释
calendar	list[int] 	是	交易日列表，推荐上交所交易日历

import AmazingData as ad
ad.login(
username='username', 
password='password',
host='***.***.***.***',
port=****)
local_path='D://AmazingData_local_data//'
download_info_data_object = ad.DownloadInfoData(local_path)
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar(market='SH')
download_info_data_object.download_hist_code_list(calendar)

三、调用历史代码表数据接口
函数接口：BaseData的 get_hist_code_list
功能描述：获取历史代码表，先检查本地数据，再从服务端补充，最后返回数据
输入参数：  
参数	 数据类型	必选	解释
security_type	str 	是	默认为
"EXTRA_STOCK_A_SH_SZ"  沪深A股，详见附录

start_date	int 	是	开始时间，闭区间
end_date	int 	是	结束时间，闭区间
local_path	str	是	本地存储数据的路径，需绝对路径，格式类似“
'D://AmazingData_local_data//'

”

import AmazingData as ad
ad.login(
username='username', 
password='password',
host='***.***.***.***',
port=****)
base_data_object = ad.BaseData()
local_path = 'D://AmazingData_local_data//'
hist_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ',
                                                     start_date=20240101,
                                                     end_date=20240701,
                                                     local_path=local_path)

四、本地数据和服务器数据的获取数据速度对比
序号

测试案例

服务端抓取

落地本地数据库后，第二次获取
1

时间区间：

1年

【20230701,20240701】

代码范围：

沪深a股

305s	7.02s
2

时间区间：

2年

【20220701,20240701】

代码范围：

沪深a股

594s	11.37s
3	
时间区间：

3年

【20210701,20240701】

代码范围：

沪深a股

864s	17.84s
注：根据本地计算机性能和网络情况，测试数据仅供参考，不保证测试数据的准确性

五、附录

1. security_type

字段描述：代码类型

数据类型

枚举值

说明

str

EXTRA_STOCK_A

上交所A股、深交所和北交所的股票

str

EXTRA_IDNEX_A

上交所A股、深交所和北交所的指数

str

EXTRA_ETF

上交所A股、深交所的ETF



str

EXTRA_STOCK_A_SH_SZ

上交所A股、深交所A股的股票

str

EXTRA_IDNEX_A_SH_SZ

上交所、深交所的指数

str

SH_A

上交所A股股票

str

SZ_A

深交所A股股票

str

BJ_A

北交所股票

str

SH_INDEX

上交所指数

str

SZ_INDEX

深交所指数

str

BJ_INDEX

北交所指数

str

SH_ETF

上交所ETF

str

SZ_ETF

深交所ETF



AmazingData系列文章
1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询
5. K线的合成算法介绍、如何订阅实时K线？
6. 如何获取历史K线数据
图片


【AmazingData】如何获取历史K线数据
图片在《【AmazingData】K线的合成算法介绍、如何订阅实时K线？》文中已经介绍了使用AmazingData实时订阅K线行情数据的方法，本文将介绍K线行情历史数据的查询方法。

一、历史行情查询接口使用步骤

（1） 实例化AmazingData的MarketData，入参需交易日历

（2） 调用MarketData的方法获取数据

二、K线数据订阅

函数接口：query_kline

功能描述：K线数据的实时订阅回调函数 ，支持全部周期的K线数据查询
输入参数：
参数	 数据类型	必选	解释
code_list	list:[str]	是	可传入列表，已支持北交所、上交所、深交所的股票、ETF和指数 
begin_date	int	是	日期，填写8位的整型格式的日期，比如20240101
end_date	int
是	日期，填写8位的整型格式的日期，比如20240201
period	Period	是	
数据周期Period（见附录）

填写除Period.snapshot.value外的Period所有value
输出参数：

回调返回值	 数据类型	解释
kline_dict	dict	
字典的key：代码

字典的value：dataframe，

column为K线数据Kline（见附录），

index为日期（datetime）



import AmazingData as ad
ad.login(username='username', password='password', host='***.***.***.***', port=****) 
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
code_list = base_data_object.get_code_list(security_type='EXTRA_ETF')
market_data_object = ad.MarketData(calendar)
snapshot_dict = market_data_object.query_kline(code_list, begin_date=20240530, end_date=20240530)
三、附录
Period

字段描述：数据周期

数据类型

枚举值

说明

int

Period.snapshot.value

快照

int

Period.day.value

日线

int

Period.min1.value

1分钟线

int

Period.min3.value

3分钟线

int

Period.min5.value

5分钟线

int

Period.min10.value

10分钟线

int

Period.min15.value

15分钟线

int

Period.min30.value

30分钟线

int

Period.min60.value

60分钟线

int

Period.min120.value

120分钟线

int

Period.week.value

周线

int

Period.month.value

月线

int

Period.season.value

季度线

int

Period.year.value

年线

2. Kline数据结构

字段描述：K线的数据结构

数据类型

字段名称

说明

str

code

证券代码+市场

datetime

kline_time

交易所行情数据时间

float

open

最高价

float

high

最低价

float

low

最新价

float

close

收盘价

int

volume

成交总量

float	amount	成交总金额

AmazingData系列文章
1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询
5. K线的合成算法介绍、如何订阅实时K线？图片


【AmazingData】K线的合成算法介绍、如何订阅实时K线？
图片

一、K线合成算法介绍

K线使用level-1的快照行情合成

1. 前推算法

举例：

9:30的1分钟K线，计算的是9:30:00.000~9:30:59.999期间的K线。

9:35的5分钟K线，计算的是9:35:00.000~9:39:59.999期间的K线。

2. 1分钟K线的集合竞价处理

开盘集合竞价数据的成交量，在当日第一根K线；

收盘集合竞价数据的成交量，在当日最后一根K线。

特别地，14:57的1分钟K线表示由14:57:00.000~14:57:59.999的level-1快照数据合成，指数的14:57的1分钟K线量价不为0


二、实时行情订阅接口使用步骤

（1） 实例化AmazingData的SubscribeData

（2） 回调函数的装饰器传入code_list(代码表)和period(数据周期)两个参数

（3） 回调函数中获取数据

三、K线数据订阅

函数接口：onKline

功能描述：K线数据的实时订阅回调函数 
输入参数：
入参需传入装饰器中SubscribeData.register  
参数	 数据类型	必选	解释
code_list	list:[str]	是	可传入列表，已支持北交所、上交所、深交所的股票、ETF和指数 
period	Period	是	
数据周期Period（见附录）

填写除Period.snapshot.value外的Period所有value

输出参数：
回调返回值	 数据类型	解释
data	Object	
K线数据Kline（见附录）



import AmazingData as ad
ad.login(username='username', password='password', host='***.***.***.***', port=****) 
base_data_object = ad.BaseData()
etf_code_list = base_data_object.get_code_list(security_type='EXTRA_ETF')
# 实时订阅
sub_data = ad.SubscribeData()
@sub_data.register(code_list=etf_code_list, period=ad.constant.Period.min1.value) 
def onKline(data: Union[ad.constant.Kline], period): 
    print(period, data)
sub_data.run()
三、附录
Period

字段描述：数据周期

数据类型

枚举值

说明

int

Period.snapshot.value

快照

int

Period.day.value

日线

int

Period.min1.value

1分钟线

int

Period.min3.value

3分钟线

int

Period.min5.value

5分钟线

int

Period.min10.value

10分钟线

int

Period.min15.value

15分钟线

int

Period.min30.value

30分钟线

int

Period.min60.value

60分钟线

int

Period.min120.value

120分钟线

int

Period.week.value

周线

int

Period.month.value

月线

int

Period.season.value

季度线

int

Period.year.value

年线

2. Kline数据结构

字段描述：数据周期

数据类型

字段名称

说明

str

code

证券代码+市场

datetime

kline_time

交易所行情数据时间

float

open

开盘价

float

high

最高价

float

low

最低价

float

close

收盘价

int

volume

成交总量

float	amount	成交总金额

图片

AmazingData系列文章
1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）
3. 实时level-1快照行情订阅
4. 历史level-1快照行情数据查询


【AmazingData】实时level-1快照行情订阅
图片

一、实时行情订阅接口使用步骤

（1） 实例化AmazingData的SubscribeData

（2） 回调函数的装饰器传入code_list(代码表)和period(数据周期)两个参数

（3） 回调函数中获取数据

二、Level-1快照数据订阅

函数接口：onSnapshot

功能描述：level-1快照数据的实时订阅回调函数 
输入参数：
入参需传入装饰器中SubscribeData.register  
参数	 数据类型	必选	解释
code_list	list:[str]	是	可传入列表，已支持北交所、上交所、深交所的股票、ETF和指数 
period	Period	是	数据周期Period（见附录）
输出参数：
回调返回值	 数据类型	必选	解释
data	Object	是	
指数为SnapshotIndex（见附录）

股票、ETF为Snapshot（见附录）



import AmazingData as ad
ad.login(username='username', password='password', host='***.***.***.***', port=****) 
base_data_object = ad.BaseData()
etf_code_list = base_data_object.get_code_list(security_type='EXTRA_ETF')
# 实时订阅
sub_data = ad.SubscribeData()
@sub_data.register(code_list=etf_code_list, period=ad.constant.Period.snapshot.value)
def onSnapshot(data: Union[ad.constant.Snapshot, ad.constant.SnapshotIndex], period):
    print(period, data.ask_volume3) 
sub_data.run()
三、附录
Period

字段描述：数据周期

数据类型

枚举值

说明

int

Period.snapshot.value

快照

int

Period.day.value

日线

int

Period.min1.value

1分钟线

int

Period.min3.value

3分钟线

int

Period.min5.value

5分钟线

int

Period.min10.value

10分钟线

int

Period.min15.value

15分钟线

int

Period.min30.value

30分钟线

int

Period.min60.value

60分钟线

int

Period.min120.value

120分钟线

int

Period.week.value

周线

int

Period.month.value

月线

int

Period.season.value

季度线

int

Period.year.value

年线



2. SnapshotIndex

数据结构描述：level-1快照，指数

数据类型

字段名称

说明

str

code

证券代码+市场

datetime

trade_time

交易所行情数据时间

float

last

最新价

float

pre_close

前收盘价

float

open

今开盘价

float

high

最高价

float

low

最低价

float

close

收盘价（仅上海有效）

int

volume

成交总量（上交所:手，深交所:张）

float

amount

成交总金额

3. Snapshot
数据结构描述：level-1快照，股票、ETF



数据类型

字段名称

说明

str

code

证券代码+市场

datetime

trade_time

交易所行情数据时间

float

pre_close

昨收价

float

last

最新价

float

open

开盘价

float

high

最高价

float

low

最低价

float

close

收盘价

float

volume

成交总量

float

amount

成交总金额

float

num_trades

成交笔数

float

high_limited

涨停价

float

low_limited

跌停价

float

ask_price1

卖1档价格

float

ask_price2

卖2档价格

float

ask_price3

卖3档价格

float

ask_price4

卖4档价格

float

ask_price5

卖5档价格

int

ask _volume1

卖1档量

int

ask _volume2

卖2档量

int

ask _volume3

卖3档量

int

ask _volume4

卖4档量

int

ask _volume5

卖5档量

float

bid_price1

买1档价格

float

bid_price2

买2档价格

float

bid_price3

买3档价格

float

bid_price4

买4档价格

float

bid_price5

买5档价格

int

bid _volume1

买1档量

int

bid _volume2

买2档量

int

bid _volume3

买3档量

int

bid _volume4

买4档量

int

bid _volume5

买5档量

图片

AmazingData系列文章
1. SDK的介绍、安装和登录
2. 获取交易日历、代码表（每日最新）

【AmazingData】获取交易日历、代码表（每日最新）
图片

一、获取交易日历

函数接口：get_calendar

功能描述：获取交易所的交易日历

输入参数：  
参数	 数据类型	必选	解释
data_type	str 	否	选择返回数据的类型，默认为str ，可选datetime 或 str
market	str 	否	
选择市场marke
（见附录），默认为SH（上海）

输出参数：  

返回值	 数据类型	解释
calendar	list	日期 
import AmazingData as ad
ad.login(username='username',
password='password', 
host='***.***.***.***', port=****)
base_data_object = ad.BaseData()
calendar = base_data_object.get_calendar()
二、获取代码表（每日最新）

函数接口：get_code_list

功能描述：获取代码表（每日最新），此接口无法获取历史代码表

输入参数：  
参数	 数据类型	必选	解释
security_type	str 	否	代码类型security_type（见附录），默认为EXTRA_STOCK_A（上交所A股、深交所和北交所的股票列表）
输出参数：  

返回值	 数据类型	解释
code_list	list	证券代码
import AmazingData as ad
ad.login(username='username',
password='password', 
host='***.***.***.***',
 port=****) 
base_data_object = ad.BaseData()
code_list = base_data_object.get_code_list(security_type='EXTRA_ETF')
三、附录
1. market

字段描述：市场

数据类型

枚举值

说明

str

SH

上交所

str

SZ

深交所

str

BJ

北交所

2.  security_type
字段描述：代码类型

数据类型

枚举值

说明

str

EXTRA_STOCK_A

上交所A股、深交所和北交所的股票列表

str

EXTRA_IDNEX_A

上交所A股、深交所和北交所的指数列表

str

EXTRA_ETF

上交所A股、深交所的ETF列表

图片

AmazingData系列文章
1. SDK的介绍、安装和登录