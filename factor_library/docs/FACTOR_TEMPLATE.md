# 因子文档模板

## 因子名称：[因子名称]

### 基本信息

| 属性   | 值                           |
|------|-----------------------------|
| 因子ID | `factor_name`               |
| 类别   | 技术因子 / 基本面因子 / 复合因子/深度学习因子  |
| 输入类型 | bar / tick                  |
| 依赖因子 | factor_1, factor_2（如无则填"无"） |
| 更新时间 | YYYYMMDD                    |

---

### 数学定义

**公式**:
```
[数学公式，使用 LaTeX 或文字描述]

例如：
momentum_return = mean(pct_change(close, 1), window)
volatility = std(pct_change(close, 1), window)
risk_adjusted_momentum = momentum_return / volatility
```

**参数**:
- `window`: 回看窗口，默认值 20
- `param2`: 参数说明

---

### 经济含义

**直觉解释**:  
[用通俗语言解释因子的经济学意义]

例如：
> 风险调整动量因子衡量单位风险下的收益，类似于 Sharpe Ratio。数值越高，表示在承担相同风险的情况下，收益越高。

**适用场景**:
- 场景1：横截面选股
- 场景2：风险管理

---

### 使用示例

#### 1. 创建因子实例

```python
from factor_library.technical.risk_adjusted_momentum import RiskAdjustedMomentum

factor = RiskAdjustedMomentum(
    timeframe='1d',
    para={'window': 20}
)
```

#### 2. 计算因子值

```python
# 准备数据（MultiIndex: timestamp, code）
data = pd.DataFrame({
    'timestamp': [...],
    'code': [...],
    'close': [...]
}).set_index(['timestamp', 'code'])

# 计算
result = factor.compute(data)

# 带后处理
result_processed = factor.compute_with_postprocess(data)
```

#### 3. ZVT 回测

```python
from factor_library.zvt_adapter import FactorAdapter
from zvt.trader.trader import StockTrader

class MyTrader(StockTrader):
    def init_factors(self, ...):
        return [
            FactorAdapter(
                custom_factor=factor,
                codes=codes,
                start_timestamp='2023-01-01',
                end_timestamp='2024-01-01'
            )
        ]

trader = MyTrader(...)
trader.run()
```

---

### 参数空间

| 时间框架 | 参数组合 |
|---------|----------|
| 1d | window = [10, 20, 60] |
| 1h | window = [20, 50, 100] |

---

### 后处理配置

- **Winsorize**: 去除1%和99%分位数的极值
- **标准化**: Z-Score标准化（均值0，标准差1）
- **填充**: 前向填充 NaN 值

---

### 性能表现（回测）

**测试配置**:
- 股票池: 沪深300
- 时间范围: 2020-01-01 至 2023-12-31
- 策略: Top 30 多头

**结果**:
| 指标 | 数值 |
|------|------|
| 年化收益率 | 15.2% |
| Sharpe Ratio | 1.35 |
| 最大回撤 | -12.5% |
| 胜率 | 58.3% |

**净值曲线**:  
[插入回测曲线图]

---

### 注意事项

⚠️ **使用建议**:
1. 波动率较低的股票可能产生极端值，建议结合 Winsorize 去极值
2. 市场震荡期该因子效果最佳
3. 需配合止损策略

❌ **不适用场景**:
- 成交量极低的股票（波动率不可靠）
- 新上市股票（历史数据不足）

---

### 相关因子

- `momentum_return`: 动量收益率（依赖因子）
- `volatility`: 波动率（依赖因子）
- `sharpe_factor`: 夏普因子（类似因子）

---

### 更新日志

- **2026-02-17**: 初始版本，支持日线和小时线
- **YYYY-MM-DD**: 优化XX功能

---

### 参考文献

[1] Author, "Paper Title", Journal, Year  
[2] WorldQuant 101 Alphas, Alpha#001

---

## 文件路径

`factor_library/technical/risk_adjusted_momentum.py`

## 作者

Quant Team

## License

MIT
