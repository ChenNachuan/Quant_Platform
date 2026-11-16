# factor_interface

> 域：research · 对应代码 [research/factor/interface.py](../../../../research/factor/interface.py)

## 职责与边界

因子的抽象定义与 IO 契约：`Factor` ABC + `depends_on()` 依赖声明 + `compute()` 统一输出 + `validate_output()` 严格契约校验。只定义「一个因子是什么」；计算调度在 [runtime.md](runtime.md)，落盘在 [storage.md](storage.md)，评价在 [evaluation.md](evaluation.md)，治理在 [governance.md](governance.md)，后处理在 [post_processor.md](../post_processor.md)。

- 输入：`data: dict[str, pd.DataFrame]` —— `depends_on()` 声明的每个依赖名映射到对应 DataFrame（裸表名经 `load_bars()` 加载，`"factor:xxx"` 经 `FactorDAG` 传入）。
- 输出：严格三列长表 `DataFrame[date, ticker, {factor_name}]` 的**原始因子值**（去极值/填充/标准化/中性化由消费侧 `PostProcessor` 完成）。
- 不属于本模块：EventBrick / UFormula / DataPlugin（不引入外部公式引擎或 C++ 数据插件）。

## 接口签名

```python
FACTOR_CATEGORIES: tuple[str, ...] = ("time_series", "cross_section", "event", "alternative")
DEFAULT_CATEGORY: str = "custom"
FACTOR_DEP_PREFIX: str = "factor:"

def parse_dependency(dep: str) -> tuple[str, str]: ...
    # 裸表名 → ("table", name)；"factor:xxx" → ("factor", "xxx")；未知前缀/空值抛 ValueError
def factor_meta(cls: type) -> dict[str, Any]: ...
def extract_factor_meta(factors: Iterable[type] | None = None) -> list[dict[str, Any]]: ...
def validate_output(factor_name: str, df: pd.DataFrame) -> pd.DataFrame: ...
    # 严格校验列序等于 ["date", "ticker", factor_name]

class Factor(ABC):
    name: str                                  # {type}_{semantic}_{params} 全小写 snake_case
    description: str = ""
    category: str = DEFAULT_CATEGORY           # 须属于 VALID_CATEGORIES
    frequency: str = "daily"
    asset_types: tuple[str, ...] = ("equity",) # 不可变元组，避免子类共享可变默认值
    version: str = "1.0"                       # compute() 逻辑变更时递增

    @abstractmethod
    def depends_on(self) -> list[str]: ...

    @abstractmethod
    def compute(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame: ...
```

依赖前缀约定：

| 写法 | 含义 |
|---|---|
| `"bars_daily"`（无冒号裸表名） | 原始数据表，经 `DataAPI` / `load_bars()` 加载 |
| `"factor:momentum_20d"` | 上游因子，从因子存储读取或由 `FactorDAG` 上游计算 |

四类基础因子（`FACTOR_CATEGORIES`）：`time_series`（单标的时序运算）/ `cross_section`（截面排名、中性化）/ `event`（财报、解禁等事件表输入）/ `alternative`（情绪、资金流等另类数据）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| `Factor` ABC + `depends_on()` + `compute()` → DataFrame | EventBrick 四元素契约 | 事件驱动是实盘 tick 级需求，离线批量计算不需要 |
| 输出三列长表 `DataFrame[date, ticker, {factor_name}]` | 三维矩阵（di × ti × 合约） | pandas 向量化的自然 IO；统一格式直接对接存储、下游因子与 alphalens |
| 元数据直接声明为类属性（代码即注册表） | 独立 JSON/YAML 元数据文件 | 因子规模 <100，类定义期 `__init_subclass__` 即时校验 `name` 与 `category` |
| 依赖声明 = `depends_on()` 静态列表 | AST 静态分析 `compute()` | 声明式简单明确；AST 静态分析脆弱且难维护 |
| 纯 Python / pandas 实现 | UFormula 公式引擎 / C++ 插件 | 避免引入 C++ build chain 与 XML 配置 |
| universe 过滤（ST/停牌/退市）在 research 层完成 | 执行层过滤 | 标的可交易性属于研究输入口径 |

## 实现要点

详见 [research/factor/interface.py](../../../../research/factor/interface.py)。

核心约定：
- `__init_subclass__` 读取 `cls.__dict__.get("name")`（防止误继承父类标识），强制匹配 `{type}_{semantic}_{params}` 正则 `_NAME_RE` 与合法 `category`。
- `parse_dependency` 对未识别前缀显式抛出 `ValueError`，禁止静默回退。
- `validate_output` 校验精确三列 `["date", "ticker", factor_name]`，后续若放宽多列宽表与分频率根目录的演进方案见仓库 Issue #87。
