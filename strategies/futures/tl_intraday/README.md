# TL 日内趋势策略复现

复现对象：中信期货《固收量化日内系列之一：基于趋势强度与动量拐点的 TL 合约日内量化策略研究》（2026-01-20）。

## 研报口径

- 标的：30 年期国债期货 TL 主力合约。
- 样本：2023-04-21 至 2025-12-31；2025-09-01 起为样本外。
- 换月：按主力切换前一交易日收盘价比例复权。
- 交易：一倍杠杆，单边手续费万 0.05，信号后 5 分钟 VWAP 成交，尾盘平仓。
- 策略 A：14 分钟 ADX 上穿 35 开仓，下穿 20 或价格进入前一日 10%/90% 极值区平仓。
- 策略 B：MACD(12, 26, 9) BAR 变号平仓；同号 BAR 首次动量峰值开仓，后续峰值每次减仓 20%。
- 日间止损：ADX 策略遇样本内单日收益后 10% 极端亏损后停做两日；动量策略在前一日 20 日 ER 均值低于样本内 25% 分位时停做。
- 复合：两个止损后策略各 50%。

## 项目接线

```text
scripts/data_processing/build_tl_min1_from_ticks.py # 逐笔数据聚合为 1 分钟
scripts/data_fetcher/fetch_tl_min1.py       # 远程数据备用拉取器
infra/futures_continuous.py                 # 主力选择与比例复权
factor_library/futures/tl_intraday.py       # ADX、MACD 拐点、ER
engine/backtest/intraday_futures.py         # 5 分钟 VWAP 延迟成交回测
strategies/futures/tl_intraday/              # 参数、信号组装与运行入口
artifacts/futures_tl_intraday/               # 净值、指标、成交和对比结果
```

## 运行

优先从本地 CFFEX 逐笔快照聚合研报区间的 TL 1 分钟数据：

```bash
uv run python scripts/data_processing/build_tl_min1_from_ticks.py
```

若本地没有 tick_l2，可使用备用远程拉取器：

```bash
docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_tl_min1.py
```

然后运行正式复现：

```bash
uv run python -m strategies.futures.tl_intraday.run
```

仅用于验证管线的 5 分钟代理模式：

```bash
uv run python -m strategies.futures.tl_intraday.run --allow-5min-proxy
```

5 分钟代理会将研报的分钟窗口换算为最近 bar 数，只能检查工程正确性，不得声称为结果复现。

## 已知口径差异

研报使用 Wind 主力连续合约，本项目从本地全合约行情按日成交量选取主力。ADX 按研报文字口径实现：ATR 使用 14 分钟滚动均值，方向移动量与 DX 使用 14 期指数加权平均；方向移动量保留标准 DMI 的正负方向筛选，因为研报正文关于“绝对值”的简写若逐字套用会使 +DI/-DI 几乎相等，无法形成文中所示交易频率。ER 参照研报引用的日内趋势度含义，使用 Kaufman Efficiency Ratio：日内净变动绝对值除以日内路径总变动。这些口径差异会在复现输出中定量披露。
