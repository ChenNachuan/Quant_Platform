# factor_interface

> 域：research · 对应代码 research/factor_interface.py · 来源 §57（archive/OPEN_QUESTIONS.md）

## 职责与边界

因子的抽象定义与 IO 契约：Factor ABC + `depends_on()` 依赖声明 + `compute()` 统一输出。只定义「一个因子是什么」；计算调度在 [factor_runtime.md](factor_runtime.md)，后处理在 [post_processor.md](post_processor.md)，落盘在 [factor_storage.md](factor_storage.md)，治理在 [factor_governance.md](factor_governance.md)。

- 输入：`data: dict[str, pd.DataFrame]`——`depends_on()` 声明的每个依赖名 → DataFrame（数据表经 `load_bars()` 加载，上游因子经 FactorDAG 传入）
- 输出：`DataFrame[date, ticker, {factor_name}]`，**原始值**（后处理不属于 Factor 接口）
- 不属于本模块：EventBrick / UFormula / DataPlugin（公司三种实现路径均不引入）

## 接口签名

```python
class Factor(ABC):
    # 元数据类属性（代码即元数据，不引入 JSON/YAML 注册）
    name: str
    description: str = ""
    category: str = "custom"          # time_series / cross_section / event / alternative
    frequency: str = "daily"
    asset_types: list[str] = ["equity"]
    version: str = "1.0"

    @abstractmethod
    def depends_on(self) -> list[str]:
        """数据表无前缀（"bars_daily"）；因子依赖带 "factor:" 前缀。"""

    @abstractmethod
    def compute(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """返回 DataFrame[date, ticker, {factor_name}]。"""
```

依赖前缀约定：

| 写法 | 含义 |
|---|---|
| `"bars_daily"` | 原始数据表，经 DataAPI / `load_bars()` 加载 |
| `"factor:momentum_20d"` | 上游因子，经因子存储加载或现算 |

四类因子（FACTOR_CATEGORIES）：`time_series`（单标的时序运算）/ `cross_section`（截面排名、中性化）/ `event`（财报、解禁）/ `alternative`（情绪、资金流）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| Factor ABC + `depends_on()` + `compute()` → DataFrame | EventBrick 四元素契约（name/publisher/Handle/invoker） | 事件驱动是实盘 tick 级需求，离线批量不需要 |
| 输出长表 DataFrame[date, ticker, value] | 三维矩阵（di × ti × 合约） | pandas 向量化的自然 IO；矩阵是 UFormula 引擎的形态 |
| 元数据 = 类属性 | 独立 JSON/YAML 元数据文件 | 因子 <100 个，代码即文档 |
| 依赖声明 = `depends_on()` 返回值 | AST 静态分析 compute() | 声明式简单明确；静态分析成本高、脆弱 |
| 实现路径只保留 Python | UFormula 公式引擎 / C++ EB | 需要 C++ build chain + GPU + XML 配置，pandas 有 ts*/cs* 等价物 |
| 输入 DataFrame | Udata 三维 tensor + DataPlugin 注册 | DataPlugin 是 C++ 数据源注册机制，`depends_on()` 字符串即声明 |
| universe 过滤（ST/停牌/退市）在 research 层过滤 | 执行层过滤 | 标的可交易性属于研究输入口径（差异落层表 → research） |
| 现有 Factor ABC / FactorRegistry / FactorDAG / PostProcessor 保留微调 | 推翻重建 | 设计方向正确，仅接口微调（加 depends_on、统一输出） |

## 实现要点

```python
class Momentum20D(Factor):
    name = "momentum_20d"
    description = "20日动量：close / close_lag20 - 1"
    category = "time_series"
    frequency = "daily"
    version = "1.0"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close"]].copy()
        df = df.sort_values(["ticker", "date"])
        df["momentum_20d"] = df.groupby("ticker")["close"].pct_change(20)
        return df[["date", "ticker", "momentum_20d"]].dropna()


class VolAdjMomentum(Factor):
    name = "vol_adj_momentum"
    category = "time_series"

    def depends_on(self):
        return ["factor:momentum_20d", "factor:volatility_20d"]  # 前缀区分因子依赖

    def compute(self, data):
        mom, vol = data["factor:momentum_20d"], data["factor:volatility_20d"]
        merged = mom.merge(vol, on=["date", "ticker"])
        merged["vol_adj_momentum"] = merged["momentum_20d"] / merged["volatility_20d"]
        return merged[["date", "ticker", "vol_adj_momentum"]].dropna()
```

输出格式直接对接 alphalens（`set_index(["date","ticker"])["factor_name"]`）、下游因子 compute()、Parquet 落盘。现有 factor_library 改动：Factor ABC 加 `depends_on()`；FactorRegistry 注册来源从外部 JSON 改为类属性；FactorDAG 依赖解析改读 `depends_on()`（Kahn 拓扑不变）。新增代码 ≈0 行（接口微调）。

## 实施时定稿项

- `version` 字段变更规范（compute 逻辑改了必须手动 bump？谁来强制——可在 CI 检查）（§57）
- `category="event"` / `"alternative"` 两类的接口约定（事件表输入、非 bars 输入如何在 `depends_on()` 声明）（§57）
- 因子命名规范（`momentum_20d` vs `mom_20d`，与因子注册表 name 字段对齐）（§57；命名格式已在 [factor_governance.md](factor_governance.md) §62.6 定为 `{类型}_{语义}_{参数}` snake_case，此处指强制校验手段）
