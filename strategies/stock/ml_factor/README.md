# ML Factor Strategy

基于机器学习的股票多因子选股策略，支持 Random Forest 和 XGBoost 两种模型。

## 目录结构

```
strategies/stock/ml_factor/
├── __init__.py              # 模块初始化
├── config.py                # 配置参数
├── data_preprocessor.py     # 数据预处理
├── model_trainer.py         # 模型训练（RF 和 XGBoost）
├── evaluator.py             # 评估指标（IC、Barra、Fama-MacBeth 等）
├── backtest.py              # 回测和分组测试
├── visualizer.py            # 可视化模块
├── check_environment.py     # 环境检查脚本
├── run_random_forest.py     # RF 策略运行入口
├── run_xgboost.py           # XGBoost 策略运行入口
└── README.md                # 本文档
```

## 运行方式

### 0. 环境检查（推荐先运行）

```bash
uv run python -m strategies.stock.ml_factor.check_environment
```

### 1. Random Forest 策略

```bash
uv run python -m strategies.stock.ml_factor.run_random_forest
```

### 2. XGBoost 策略

```bash
uv run python -m strategies.stock.ml_factor.run_xgboost
```

## 数据要求

研究数据统一放在 `data_lake/stock/research/ml_factor/`；股票基础信息继续使用标准 basedata 目录：

- `cleaned_final_dataset2_2000_2025.parquet` - 主数据集
- `全A股月度cne5数据_2007-2025.csv` - Barra 因子数据
- `industry.xlsx` - 申万行业分类
- `data_lake/stock/basedata/stock_basic/stock_basic.parquet` - 股票基本信息
- `index_monthly_filtered.parquet` - 指数月度数据
- `CH3_factors_monthly.xlsx` - CH-3 因子数据
- `CH4_factors_monthly.xlsx` - CH-4 因子数据
- `Carhart4_factors_monthly.xlsx` - Carhart 因子数据

## 策略流程

### 1. 数据预处理
- 读取原始数据
- 构造时间索引
- 特征标准化（Rank Normalization）
- 构造截面排名目标

### 2. 模型训练
- 24 个月滚动训练窗口
- 使用 `rank_ret` 作为训练目标
- 每月重新训练模型

### 3. 回测
- 5 分位数组（quintile）
- 等权和市值加权两种方式
- 计算多空策略收益

### 4. 评估指标
- **IC 检验**：Rank IC (Spearman Correlation)
- **Barra 检验**：控制行业和风格因子后的 Alpha
- **Fama-MacBeth 检验**：截面回归检验
- **因子模型检验**：CH-3、CH-4、Carhart
- **GRS 检验**：联合假设检验

## 输出结果

运行结果保存在 `artifacts/stock_ml_factor/` 目录：

- `rf_predictions.parquet` / `xgb_predictions.parquet` - 预测结果
- `rf_performance.parquet` / `xgb_performance.parquet` - 回测结果
- `rf_ic_series.csv` / `xgb_ic_series.csv` - IC 序列
- `rf_cumulative_returns.png` / `xgb_cumulative_returns.png` - 累计净值图
- `rf_rolling_ic.png` / `xgb_rolling_ic.png` - 滚动 IC 图

## 配置参数

所有配置参数在 `config.py` 中定义，主要参数包括：

### Random Forest 参数
- `train_window_months`: 训练窗口（默认 24 个月）
- `n_estimators`: 树的数量（默认 100）
- `max_depth`: 树深度（默认 6）
- `min_samples_leaf`: 叶子节点最小样本数（默认 20）

### XGBoost 参数
- `train_window_months`: 训练窗口（默认 24 个月）
- `n_estimators`: 树的数量（默认 1000）
- `max_depth`: 树深度（默认 6）
- `learning_rate`: 学习率（默认 0.05）
- `device`: 设备（"cuda" 或 "cpu"）

## 注意事项

1. XGBoost 默认使用 GPU 加速，如果没有 GPU，请修改 `run_xgboost.py` 中的 `device="cpu"`
2. 运行时间较长，Random Forest 约需 30-60 分钟，XGBoost 约需 1-2 小时
3. 确保安装了所有依赖包：`pip install scikit-learn xgboost statsmodels scipy`
