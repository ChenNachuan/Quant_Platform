# Stock ML Factor Strategies Overview

## 项目结构

```
strategies/stock/
├── __init__.py
├── ml_factor/
│   ├── __init__.py              # 模块导出
│   ├── config.py                # 配置参数
│   ├── data_preprocessor.py     # 数据预处理
│   ├── model_trainer.py         # 模型训练（RF 和 XGBoost）
│   ├── evaluator.py             # 评估指标（IC、Barra、Fama-MacBeth 等）
│   ├── backtest.py              # 回测和分组测试
│   ├── visualizer.py            # 可视化模块
│   ├── check_environment.py     # 环境检查
│   ├── run_random_forest.py     # RF 策略运行入口
│   ├── run_xgboost.py           # XGBoost 策略运行入口
│   └── README.md                # 模块说明文档
    └── reports/
        ├── result_template.md   # 运行结果记录模板
        └── strategy_overview.md # 本文档
```

## 快速开始

### 1. 环境检查

```bash
uv run python -m strategies.stock.ml_factor.check_environment
```

### 2. 运行 Random Forest 策略

```bash
uv run python -m strategies.stock.ml_factor.run_random_forest
```

### 3. 运行 XGBoost 策略

```bash
uv run python -m strategies.stock.ml_factor.run_xgboost
```

## 策略流程

### 1. 数据预处理
- 读取 `cleaned_final_dataset2_2000_2025.parquet`
- 构造时间索引（year + month → date）
- 计算下期收益率（带日期校验，防止停牌/断层）
- 特征标准化：Rank Normalization 映射到 [-1, 1]
- 构造截面排名目标 `rank_ret`（0~1）

### 2. 模型训练
- **训练窗口**：24 个月滚动
- **训练目标**：`rank_ret`（截面排名）
- **预测频率**：每月重新训练

#### Random Forest 参数
```python
n_estimators = 100
max_depth = 6
min_samples_leaf = 20
```

#### XGBoost 参数
```python
n_estimators = 1000
max_depth = 6
learning_rate = 0.05
subsample = 0.8
colsample_bytree = 0.8
```

### 3. 分组回测
- 5 分位数组（quintile）
- Group 1 = 预测最低（做空组）
- Group 5 = 预测最高（做多组）
- 等权（EW）和市值加权（VW）两种方式

### 4. 评估指标

#### IC 检验
- Rank IC (Spearman Correlation)
- IC Mean 和 IC IR

#### Barra 检验
- 控制 10 个风格因子
- 控制行业哑变量
- 检验 ML 因子的纯 Alpha

#### Fama-MacBeth 检验
- 逐月截面回归
- 控制 size, Bm, MOM12, ILLIQ
- 计算 t 统计量

#### 因子模型检验
- CH-3（中国三因子）
- CH-4（中国四因子）
- Carhart（四因子）

#### GRS 检验
- 联合假设检验
- 检验 CH-3 模型是否能解释 5 组收益

## 输出文件

运行结果保存在 `artifacts/stock_ml_factor/`：

| 文件 | 说明 |
|------|------|
| `rf_predictions.parquet` | RF 预测结果 |
| `xgb_predictions.parquet` | XGBoost 预测结果 |
| `rf_performance.parquet` | RF 回测结果 |
| `xgb_performance.parquet` | XGBoost 回测结果 |
| `rf_ic_series.csv` | RF IC 序列 |
| `xgb_ic_series.csv` | XGBoost IC 序列 |
| `rf_cumulative_returns.png` | RF 累计净值图 |
| `xgb_cumulative_returns.png` | XGBoost 累计净值图 |
| `rf_rolling_ic.png` | RF 滚动 IC 图 |
| `xgb_rolling_ic.png` | XGBoost 滚动 IC 图 |

## 预期结果

根据原始 notebook 的运行结果：

### Random Forest
- **EW_Strategy**: 年化收益 33.12%，夏普 2.54，最大回撤 18.91%
- **VW_Strategy**: 年化收益 20.90%，夏普 1.05，最大回撤 37.02%
- **IC Mean**: ~0.05
- **Barra Alpha t值**: ~20.13（显著）

### XGBoost
- **EW_Strategy**: 年化收益 35.65%，夏普 3.02，最大回撤 12.41%
- **VW_Strategy**: 年化收益 25.06%，夏普 1.29，最大回撤 23.08%
- **IC Mean**: ~0.04
- **Barra Alpha t值**: ~15.18（显著）

## 注意事项

1. **运行时间**：RF 约 30-60 分钟，XGBoost 约 1-2 小时
2. **GPU 支持**：XGBoost 默认使用 GPU，无 GPU 时自动切换 CPU
3. **数据依赖**：确保研究数据存在于 `data_lake/stock/research/ml_factor/`
4. **内存需求**：建议至少 16GB 内存

## 代码结构说明

### 核心类

| 类名 | 功能 |
|------|------|
| `DataPreprocessor` | 数据加载和预处理 |
| `RandomForestTrainer` | RF 模型训练 |
| `XGBoostTrainer` | XGBoost 模型训练 |
| `BacktestEngine` | 回测引擎 |
| `ICEvaluator` | IC 检验 |
| `BarraEvaluator` | Barra 检验 |
| `FamaMacBethEvaluator` | Fama-MacBeth 检验 |
| `FactorModelEvaluator` | 因子模型检验 |
| `GRSEvaluator` | GRS 检验 |
| `StrategyVisualizer` | 可视化 |

### 使用示例

```python
from strategies.stock.ml_factor import (
    StrategyConfig, DataPreprocessor, create_trainer,
    BacktestEngine, ICEvaluator, StrategyVisualizer
)

# 1. 配置
config = StrategyConfig()

# 2. 数据预处理
preprocessor = DataPreprocessor(config.data, config.preprocess)
df = preprocessor.load_and_prepare()
feature_cols = preprocessor.get_feature_cols()

# 3. 模型训练
trainer = create_trainer("random_forest", rf_config=config.rf_model)
predictions = trainer.train_and_predict(df, feature_cols)

# 4. 回测
engine = BacktestEngine(config.backtest)
results = engine.run(predictions)

# 5. 可视化
visualizer = StrategyVisualizer()
visualizer.plot_cumulative_returns(results['performance_df'])
```

## 后续扩展

1. **添加新模型**：在 `model_trainer.py` 中继承 `ModelTrainer` 基类
2. **添加新因子**：修改 `data_preprocessor.py` 中的特征列
3. **调整参数**：修改 `config.py` 中的配置
4. **添加新评估指标**：在 `evaluator.py` 中添加新类
