# ML Factor Strategy 运行结果

这是 ML 因子策略的人工结果记录模板。机器生成的预测、回测结果和图表统一存放在 `artifacts/stock_ml_factor/`。

## 文件说明

### 预测结果
- `rf_predictions.parquet` - Random Forest 预测结果
- `xgb_predictions.parquet` - XGBoost 预测结果

### 回测结果
- `rf_performance.parquet` - Random Forest 回测结果
- `xgb_performance.parquet` - XGBoost 回测结果

### IC 序列
- `rf_ic_series.csv` - Random Forest IC 序列
- `xgb_ic_series.csv` - XGBoost IC 序列

### 图表
- `rf_cumulative_returns.png` - Random Forest 累计净值图
- `xgb_cumulative_returns.png` - XGBoost 累计净值图
- `rf_rolling_ic.png` - Random Forest 滚动 IC 图
- `xgb_rolling_ic.png` - XGBoost 滚动 IC 图

---

## 运行结果记录

### Random Forest 策略结果

**运行日期**: [待填写]

**IC 检验**:
- IC Mean: [待填写]
- IC IR: [待填写]

**策略表现统计**:

| 策略 | 月均收益 | 年化收益 | 年化波动率 | 夏普比率 | t统计量 | 最大回撤 | 胜率 |
|------|----------|----------|------------|----------|---------|----------|------|
| EW_Long | | | | | | | |
| EW_Short | | | | | | | |
| EW_Strategy | | | | | | | |
| VW_Long | | | | | | | |
| VW_Short | | | | | | | |
| VW_Strategy | | | | | | | |

**Barra 检验**:
- ML因子 t值: [待填写]
- 结论: [待填写]

**Fama-MacBeth 检验**:
- pred_ret t值: [待填写]
- 结论: [待填写]

**因子模型检验**:
- CH-3 Alpha t值: [待填写]
- CH-4 Alpha t值: [待填写]
- Carhart Alpha t值: [待填写]

**GRS 检验**:
- GRS Statistic: [待填写]
- P-Value: [待填写]
- 结论: [待填写]

---

### XGBoost 策略结果

**运行日期**: [待填写]

**IC 检验**:
- IC Mean: [待填写]
- IC IR: [待填写]

**策略表现统计**:

| 策略 | 月均收益 | 年化收益 | 年化波动率 | 夏普比率 | t统计量 | 最大回撤 | 胜率 |
|------|----------|----------|------------|----------|---------|----------|------|
| EW_Long | | | | | | | |
| EW_Short | | | | | | | |
| EW_Strategy | | | | | | | |
| VW_Long | | | | | | | |
| VW_Short | | | | | | | |
| VW_Strategy | | | | | | | |

**Barra 检验**:
- ML因子 t值: [待填写]
- 结论: [待填写]

**Fama-MacBeth 检验**:
- pred_ret t值: [待填写]
- 结论: [待填写]

**因子模型检验**:
- CH-3 Alpha t值: [待填写]
- CH-4 Alpha t值: [待填写]
- Carhart Alpha t值: [待填写]

**GRS 检验**:
- GRS Statistic: [待填写]
- P-Value: [待填写]
- 结论: [待填写]

---

## 策略对比

| 指标 | Random Forest | XGBoost |
|------|---------------|---------|
| EW_Strategy 年化收益 | | |
| VW_Strategy 年化收益 | | |
| EW_Strategy 夏普比率 | | |
| VW_Strategy 夏普比率 | | |
| IC Mean | | |
| IC IR | | |
| Barra Alpha t值 | | |

---

## 备注

[在此处添加任何额外的观察或备注]
