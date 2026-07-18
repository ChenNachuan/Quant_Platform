# Futures ALSTM Strategy

股指期货 ALSTM 策略的唯一工作区。这里保存策略参数、运行入口、研究 notebook 和人工结论；可复用实现仍由 `engine/`、`factor_library/` 与 `infra/` 提供。

## 目录

```text
alstm/
├── config.py       # 路径、阈值、窗口和模型默认参数
├── data.py         # 适配 data_lake 标准期货分区
├── train.py        # 滚动训练入口
├── reporting.py    # 净值和指标报告入口
├── notebooks/      # 历史研究与实验
└── reports/        # 已整理的研究结论
```

通用实现：

- 模型：`engine/models/alstm.py`
- 分布模型：`engine/models/distribution_nll.py`
- 回测：`engine/backtest/futures_backtest.py`
- 集成：`engine/ensemble/`
- 期货因子：`factor_library/futures/`
- Qlib 处理：`infra/qlib_processors.py`

## 数据和输出

- 1 分钟行情：`data_lake/future/market_data/1min/year=YYYY/<SYMBOL>_1min.parquet`
- 分钟特征：`data_lake/future/market_data/minute_features/`
- L2 数据：`data_lake/future/market_data/tick_l2/year=YYYY/month=MM/`
- Qlib Provider：`data_lake/future/market_data/IF_features_with_l2tick/`
- 策略产物：`artifacts/futures_alstm/`

路径与默认参数统一从 `config.py` 导入，策略文件中不得再硬编码项目绝对路径。

## 运行

```bash
uv run python -m strategies.futures.alstm.train
uv run python -m strategies.futures.alstm.reporting
```

`train.py` 目前保留 7.10 两阶段训练方案的窗口和标签定义，数据集接线仍需完成。notebook 是历史研究记录，不是生产入口；成熟逻辑应迁回 Python 模块。

已执行 notebook 属于生成物，统一保存到 `artifacts/futures_alstm/notebooks/`，不进入源码目录。
