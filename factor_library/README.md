# Factor Library

量化因子库 — 组织、计算、缓存、分析的统一框架。

## 设计哲学

### 目录结构：资产类型 → 因子种类

```
factor_library/
├── base.py              # Factor 抽象基类
├── registry.py          # @register_factor 装饰器 + 工厂模式
├── dag.py               # 因子 DAG 拓扑排序 + 缓存
├── preprocessor.py      # 后处理流水线（winsorize / standardize / fillna）
├── universe.py          # 股票池过滤（停牌 / ST / 退市）
├── operators/           # 跨资产共享算子
│   ├── time_series.py   # ts_mean, ts_rank, ts_decay_linear ...
│   └── cross_section.py # cs_rank, cs_zscore, cs_neutralize ...
│
├── stock/               # 资产: 股票
│   ├── fundamental/     #   基本面因子 (PE, ROE, 利润率...)
│   ├── technical/       #   技术因子 (动量, 波动率, MFI...)
│   └── liquidity/       #   流动性因子 (市值, 换手率...)
│
├── futures/             # 资产: 期货
│   ├── base.py          #   期货 L2 Tick 因子基类
│   ├── functions.py     #   期货专用共享函数
│   ├── quote_spread.py  #   价差因子
│   ├── orderbook_pressure.py  # 订单簿压力因子
│   ├── momentum_factors.py    # 动量因子
│   ├── trade_factors.py       # 交易驱动因子
│   ├── push_factors.py        # 冲击因子
│   ├── order_flow.py          # 订单流重构因子
│   └── statistical.py         # 统计特征因子
│
└── option/              # 资产: 期权
    └── ...
```

### 为什么按「资产 → 种类」组织？

| 维度 | 评价 |
|---|---|
| **先按资产** | ✅ **正确**。股票用 OHLCV + 财报，期货用 L2 tick + 持仓量，期权用 Greeks。混在一起导致耦合、难以维护 |
| **先按种类** | ❌ `technical/momentum.py` 同时 import 股票和期货代码，跨资产耦合、import 开销大 |
| **先按频率** | ❌ 频率通过 `Factor.timeframe` 参数（`'1d'`, `'1h'`, `'tick'`）控制，一个因子类支持多频率 |

### 频率管理

频率不作为目录维度，而是通过 `Factor` 基类的 `timeframe` 参数控制：

```python
factor = StockMomentum(timeframe='1d', para={'window': 20})   # 日频
factor = L2Momentum(timeframe='tick', para={'lag': 10})       # tick 频
```

每个因子类通过 `para_group` 声明支持哪些频率的参数空间。

---

## 使用方式

### 注册新因子

```python
from factor_library.base import Factor
from factor_library.registry import register_factor

@register_factor
class MyFactor(Factor):
    name = "my_factor"
    store_time = "20250101"
    applicable_market = ["stock"]
    input_type = "bar"
    para_group = {
        "default": {"window": [5, 10, 20]},
    }

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group["default"]["window"]]

    def compute(self, data, deps=None):
        # data: MultiIndex[timestamp, code] DataFrame
        return data.groupby('code')['close'].transform(
            lambda x: x.pct_change(self.para['window'])
        )
```

### 创建并计算

```python
from factor_library.registry import FactorRegistry

factor = FactorRegistry.create_instance(
    name='my_factor',
    timeframe='1d',
    para={'window': 20}
)
result = factor.compute(ohlcv_data)
postprocessed = factor.compute_with_postprocess(ohlcv_data)
```

### 期货 L2 因子

```python
from factor_library.futures import compute_all_l2_factors

# data: MultiIndex[timestamp, code], 列: bid_px_1~5, ask_px_1~5, 等
factor_df = compute_all_l2_factors(l2_data, params={
    "momentum": {"lag": 20},
    "push_size": {"n_bp": 5},
})
```

### 因子 DAG（处理复合因子的依赖顺序）

```python
from factor_library.dag import FactorDAG

dag = FactorDAG()
dag.add_factor(factor_a)   # 无依赖
dag.add_factor(factor_b, depends_on=['factor_a'])  # 依赖 factor_a
results = dag.execute(data)
```

---

## 规则

1. **新因子必须用 `@register_factor` 装饰器注册**，否则工厂方法和 DAG 都无法使用
2. **因子类必须定义**: `name`, `store_time`, `para_group`, `generate_para_space()`, `compute()`
3. **`compute()` 的 data 参数必须支持 `MultiIndex[timestamp, code]`**
4. **严禁未来函数 (Look-ahead Bias)**: `compute()` 内部不能使用未来数据
5. **频率通过 `timeframe` 参数传递**，不在目录层级体现
6. **跨资产通用算子**放在 `operators/` 目录，由各资产因子调用
7. **每个资产类型**应有自己的 `base.py` 提供该资产的 `Factor` 子类

---

## 文件清单

```
factor_library/
├── README.md
├── __init__.py
├── base.py                 # Factor(ABC)
├── registry.py             # FactorRegistry + register_factor
├── dag.py                  # FactorDAG
├── preprocessor.py         # PostProcessor
├── universe.py             # UniverseFilter
├── operators/
│   ├── __init__.py
│   ├── time_series.py
│   └── cross_section.py
├── stock/
│   ├── __init__.py
│   ├── fundamental/        (11 个因子)
│   │   ├── __init__.py
│   │   ├── cashflow.py
│   │   ├── efficiency.py
│   │   ├── growth.py
│   │   ├── leverage.py
│   │   ├── profitability.py
│   │   ├── standardized.py
│   │   └── valuation.py
│   ├── technical/          (42 个因子)
│   │   ├── __init__.py
│   │   ├── momentum.py
│   │   ├── volatility.py
│   │   ├── atr.py
│   │   ├── bollinger.py
│   │   ├── obv.py
│   │   ├── turnover.py
│   │   ├── rsi.py
│   │   ├── mfi.py
│   │   ├── pvt.py
│   │   ├── rvi.py
│   │   ├── risk_adjusted_momentum.py
│   │   ├── ichimoku.py
│   │   ├── frazzini_beta.py
│   │   ├── coppock.py
│   │   ├── reversal.py
│   │   ├── swma_tema.py
│   │   ├── max_price_52w.py
│   │   └── volume_price.py
│   └── liquidity/
│       ├── __init__.py
│       └── market_cap.py
├── futures/
│   ├── __init__.py
│   ├── base.py             # L2TickFactor
│   ├── functions.py        # L2 共享计算函数
│   ├── quote_spread.py     (3 个因子)
│   ├── orderbook_pressure.py (3 个因子)
│   ├── momentum_factors.py (2 个因子)
│   ├── trade_factors.py    (3 个因子)
│   ├── push_factors.py     (2 个因子)
│   ├── order_flow.py       (4 个因子)
│   └── statistical.py      (2 个因子)
└── option/
    └── __init__.py
```
