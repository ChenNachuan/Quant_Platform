  # 周报：高频策略优化研究（2026-07-17）

## 一、研究目标

在原有 ALSTM 三状态日内滚动策略（benchmark）基础上，尝试以下优化方向：
1. **HMM 市场状态感知**：识别上涨/下跌/震荡状态，动态调整仓位
2. **模型改进**：添加 L2 tick 高频因子，使用 Focal Loss 平衡类别

---

## 二、HMM 市场状态感知策略

### 2.1 方案设计

使用 HMM（隐马尔可夫模型）将市场分为三个状态：
- **Bull（上涨）**：只做多，仓位放大
- **Bear（下跌）**：只做空，仓位放大
- **Range（震荡）**：不开仓或缩小仓位

HMM 特征：
- 基础特征：returns_1min, volatility_20, momentum_10, volume_ratio_10
- L2 tick 特征：bid_ask_spread, order_book_imbalance, trade_imbalance

### 2.2 测试结果

| 策略 | 2025-01 | 2025-04 | 2025-07 | 2025-10 |
|------|---------|---------|---------|---------|
| 纯ALSTM (benchmark) | -0.30% | +2.62% | +0.57% | -2.35% |
| HMM震荡不开仓 | +3.69% | -0.46% | -2.45% | -25.44% |
| HMM+ALSTM保守 | +0.42% | +8.88% | +3.86% | -15.82% |
| HMM+ALSTM激进 | +0.69% | +7.48% | -5.99% | -24.09% |

### 2.3 失败原因分析

1. **HMM状态识别不够准确**：97%的数据被识别为"range"，导致几乎不开仓
2. **仓位放大导致亏损放大**：在2025-10窗口，所有策略都出现大幅亏损
3. **HMM特征不够区分性**：基于分钟数据计算的特征无法有效区分市场状态
4. **缺乏真正的tick级数据**：L2 tick因子只覆盖2025年7-12月，训练期数据不完整

### 2.4 结论

**HMM市场状态感知策略未能超越benchmark**。主要原因是HMM状态识别不够准确，且仓位放大导致亏损放大。

---

## 三、模型改进：L2 Tick因子

### 3.1 方案设计

在原有ALSTM模型基础上，添加6个L2 tick高频因子：
- `tick_l2_spread`：买卖价差
- `tick_l2_imbalance`：订单簿不平衡
- `tick_l2_mid_price`：中间价
- `tick_l2_volume_change`：成交量变化
- `tick_l2_oi_change`：持仓量变化
- `tick_l2_price_momentum`：价格动量

使用qlib ALSTM + 滚动窗口训练（与benchmark完全一致）。

### 3.2 测试结果

| 窗口 | Benchmark准确率 | v2准确率 | Benchmark收益 | v2收益 |
|------|---------------|---------|---------------|--------|
| 2025-01 | ~57% | 40.77% | -0.30% | - |
| 2025-04 | ~54% | 53.90% | +2.62% | - |
| 2025-07 | ~55% | 39.92% | +0.57% | - |
| 2025-10 | ~57% | 40.76% | -2.35% | - |

### 3.3 失败原因分析

1. **L2 tick因子与基础因子高度相关**：增加了噪声而非信号
2. **L2 tick因子覆盖时间不足**：只覆盖2025年7-12月，训练期数据不完整
3. **模型无法有效学习**：所有窗口都在第0轮触发早停
4. **特征维度增加但信息量未增加**：70个特征中大部分是冗余的

### 3.4 结论

**L2 tick因子未能提升模型准确率**，反而降低了模型性能。原始benchmark配置（不使用L2 tick因子）仍然是最优方案。

---

## 四、原始Benchmark配置

### 4.1 模型架构

- **Model A**：二分类，判断是否值得交易（阈值5bp）
- **Model B**：三分类，判断方向（阈值8bp）
- **训练方式**：qlib ALSTM + 滚动窗口（12月训练+6月验证+3月测试）

### 4.2 特征

使用 `MinuteFactorLibrary` 的118个分钟频因子，包括：
- 收益率因子：returns_1min, returns_5min, returns_15min, returns_30min
- 动量因子：momentum_5, momentum_10, momentum_20, momentum_30
- 波动率因子：volatility_5, volatility_10, volatility_20, volatility_30
- 技术指标：rsi_20, cci_20, bb_position_20, vwap_ratio
- 成交量因子：volume_ratio_5, volume_ratio_10, volume_ratio_20

### 4.3 回测结果

| 窗口 | 收益 | 夏普比率 | 最大回撤 | 交易次数 |
|------|------|---------|---------|---------|
| 2025-01 | -0.30% | -0.46 | -2.29% | 702 |
| 2025-04 | +2.62% | +3.16 | -1.55% | 348 |
| 2025-07 | +0.57% | +0.45 | -3.24% | 1506 |
| 2025-10 | -2.35% | -2.37 | -4.42% | 596 |

---

## 五、关键发现

### 5.1 模型准确率与回测收益的关系

- **高准确率不等于高收益**：v2模型准确率40-54%，但回测收益为负
- **信号过滤是关键**：benchmark使用概率阈值过滤（trade_prob>0.62, dir_margin>0.16），只在高置信度时交易
- **过度交易导致亏损**：v2模型73%的样本被预测为"trade"，导致过度交易

### 5.2 因子有效性

- **L2 tick因子无效**：与基础因子高度相关，增加噪声
- **分钟频因子足够**：118个分钟频因子已包含足够的信息
- **因子质量比数量重要**：添加更多因子不一定提升性能

### 5.3 训练策略

- **滚动窗口训练是关键**：12月训练+6月验证+3月测试的滚动窗口比单次训练更有效
- **早停机制重要**：防止过拟合
- **Focal Loss效果有限**：在当前数据上未能显著改善类别不平衡问题

---

## 六、后续优化方向

1. **优化信号过滤逻辑**：调整概率阈值，减少过度交易
2. **添加止损止盈机制**：限制单笔亏损，锁定利润
3. **优化HMM特征**：使用更长窗口的特征，或结合其他市场微观结构指标
4. **尝试其他模型架构**：如Transformer、GAT等
5. **多品种策略**：将策略扩展到其他期货品种（IC、T、TF、TS）

---

## 七、附录

### 7.1 文件清单

| 文件 | 说明 |
|------|------|
| `strategies/futures/alstm/notebooks/if_alstm_rolling_3state_intra.ipynb` | Benchmark策略 |
| `strategies/futures/alstm/notebooks/alstm_l2tick_v2.ipynb` | L2 tick因子策略 |
| `strategies/futures/alstm/notebooks/alstm_v2_qlib_l2tick.ipynb` | Qlib ALSTM + L2 tick策略 |
| `artifacts/futures_alstm/if_alstm_rolling_3state_intra_2025/` | Benchmark回测结果 |
| `artifacts/futures_alstm/alstm_v2_qlib_l2tick/` | v2模型训练结果 |

### 7.2 数据清单

| 数据 | 说明 |
|------|------|
| `data_lake/future/market_data/IF_all_minute_features_data_1min/` | 分钟频因子数据 |
| `data_lake/future/market_data/tick_l2/year=2024/` | 2024年L2 tick数据 |
| `data_lake/future/market_data/tick_l2/year=2025/` | 2025年L2 tick数据 |
| `data_lake/future/market_data/tick/year=2025/` | 2025年tick数据 |
