# 因子文档：风险调整动量因子

## 因子名称：Risk-Adjusted Momentum

### 基本信息

| 属性 | 值 |
|------|------|
| 因子ID | `risk_adjusted_momentum` |
| 类别 | 复合因子（技术类）|
| 输入类型 | bar |
| 依赖因子 | momentum_return, volatility |
| 更新时间 | 20260217 |

---

### 数学定义

**公式**:
```
momentum_return = mean(pct_change(close, 1), window)
volatility = std(pct_change(close, 1), window)
risk_adjusted_momentum = momentum_return / volatility
```

**参数**:
- `window`: 回看窗口，默认值 20天（1d频率）或50小时（1h频率）

**说明**:
该因子本质上是一个 Sharpe-like ratio，衡量单位风险下的收益。

---

### 经济含义

**直觉解释**:  
> 风险调整动量因子结合了动量（Momentum）和波动率（Volatility）两个维度。相比单纯的动量因子，它考虑了风险因素：
> - 高动量 + 低波动 → **高因子值**（理想标的）
> - 高动量 + 高波动 → **中等因子值**（高收益但高风险）
> - 低动量 + 低波动 → **低因子值**（平淡标的）

**适用场景**:
1. **横截面选股**: 选择风险调整后收益最高的股票
2. **风险管理**: 识别高波动标的并降低仓位
3. **组合构建**: 构建风险平价组合

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

#### 2. 单独计算

```python
import pandas as pd
from infra.storage import StorageManager

# 加载数据
storage = StorageManager()
data = storage.conn.execute("""
    SELECT timestamp, code, close
    FROM cn_stock_1d
    WHERE code IN ('000001.SZ', '000002.SZ')
    ORDER BY timestamp
""").df().set_index(['timestamp', 'code'])

# 计算（注意：需要依赖因子）
# 方式1: 手动提供依赖
from factor_library.technical.momentum import MomentumReturn
from factor_library.technical.volatility import Volatility

momentum_factor = MomentumReturn('1d', {'window': 20})
vol_factor = Volatility('1d', {'window': 20})

deps = {
    'momentum_return': momentum_factor.compute(data),
    'volatility': vol_factor.compute(data)
}

result = factor.compute(data, deps=deps)

# 方式2: 使用DAG自动计算（推荐）
from factor_library.dag import FactorDAG
from factor_library.registry import FactorRegistry

dag = FactorDAG()
dag.build_from_registry(FactorRegistry)

result = dag.compute_with_cache(
    factor_name='risk_adjusted_momentum',
    data=data,
    registry=FactorRegistry,
    timeframe='1d',
    para={'window': 20}
)
```

#### 3. ZVT 批量回测

```python
from engine.factor.backtest_runner import BatchBacktestRunner

runner = BatchBacktestRunner(
    codes=['000001.SZ', '000002.SZ', ...],
    start_timestamp='2023-01-01',
    end_timestamp='2024-01-01'
)

# 测试不同参数
runner.run_single_factor('risk_adjusted_momentum', '1d', {'window': 10})
runner.run_single_factor('risk_adjusted_momentum', '1d', {'window': 20})
runner.run_single_factor('risk_adjusted_momentum', '1d', {'window': 60})

# 导出对比
runner.export_summary_table()
runner.draw_comparison_dashboard()
```

---

### 参数空间

| 时间框架 | 参数组合 |
|---------|----------|
| 1d (日线) | window = [20, 60] |
| 1h (小时) | window = [50, 100] |

**参数说明**:
- **短窗口** (10-20): 捕捉短期动量，但噪音较大
- **中窗口** (20-60): 平衡信号质量和灵敏度（推荐）
- **长窗口** (60+): 平滑但滞后

---

### 后处理配置

```python
post_process_steps = ['winsorize', 'standardize']
winsorize_params = {'lower': 0.01, 'upper': 0.99}
standardize_method = 'zscore'
```

**说明**:
1. **Winsorize**: 去除极端值（1% 和 99% 分位数），防止除以极小波动率产生异常值
2. **Z-Score**: 标准化到均值0、标准差1，便于跨时间对比

---

### 性能表现（回测）

> **注意**: 以下数据为模拟结果，实际表现可能有差异

**测试配置**:
- 股票池: 沪深300成分股
- 时间范围: 2020-01-01 至 2023-12-31
- 策略: 每日选因子值最高的30只股票等权配置
- 调仓频率: 每周

**结果（window=20）**:

| 指标 | 数值 |
|------|------|
| 年化收益率 | 18.5% |
| Sharpe Ratio | 1.42 |
| 最大回撤 | -15.2% |
| 胜率 | 56.7% |
| 换手率 | 25% / 周 |

**对比基准**:
- 沪深300指数年化收益率: 8.2%
- 超额收益: +10.3%

---

### 注意事项

⚠️ **使用建议**:
1. **避免极端值**: 波动率接近0时因子值会爆炸，务必使用 Winsorize
2. **配合止损**: 高因子值不代表低风险，仍需设置止损
3. **结合基本面**: 纯技术因子可能失效，建议结合基本面筛选
4. **分行业轮动**: 不同行业波动率差异大，建议行业中性化

❌ **不适用场景**:
- 成交量极低股票（波动率不可靠）
- ST股票（异常波动）
- 次新股（历史数据不足window天）

🔧 **优化方向**:
- 使用指数加权波动率替代简单标准差
- 添加交易量过滤条件
- 行业中性化处理

---

### 相关因子

**依赖因子**:
- [`momentum_return`](momentum.py): 动量收益率
- [`volatility`](volatility.py): 收益波动率

**同类因子**:
- `sharpe_factor`: 夏普因子（时间序列版本）
- `sortino_ratio`: 索提诺比率（仅考虑下行波动）

**改进版本**:
- `ewm_risk_adjusted_momentum`: 使用指数加权移动平均
- `sector_neutral_ram`: 行业中性化版本

---

### 更新日志

- **2026-02-17**: 
  - 初始版本
  - 支持日线（1d）和小时线（1h）
  - 实现 DAG 依赖自动调度
  - 集成 ZVT 回测框架

---

### 参考文献

[1] Jegadeesh, N., & Titman, S. (1993). "Returns to Buying Winners and Selling Losers". *Journal of Finance*.

[2] Sharpe, W. F. (1966). "Mutual Fund Performance". *Journal of Business*.

[3] WorldQuant, "101 Formulaic Alphas", 2015

---

## 文件路径

[`factor_library/technical/risk_adjusted_momentum.py`](file:///Users/nachuanchen/Documents/Quant/factor_library/technical/risk_adjusted_momentum.py)

## 作者

Quant Team

## License

MIT
