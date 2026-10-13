# registry

> 域：core · 对应代码 core/registry.py · 来源 §62.2 / §68.4（archive/OPEN_QUESTIONS.md）

## 职责与边界

共享注册表机制：`FactorRegistry`（因子类注册与工厂创建）与 `RuleRegistry`（规则注册）。**代码即注册表**——因子存在性由装饰器注册，元数据从 Factor 类属性自动提取，不建独立注册表库。

边界：因子治理流程（入库/退出/去重/生命周期）归 ../research/factor_governance.md；策略侧 alphaType 注册是另一套双表（../research/alpha_type.md），**两套各司其职**——因子侧 FactorRegistry + metadata.json，策略侧 alphaType JSON。

## 接口签名

```python
class FactorRegistry:
    @staticmethod
    def register(cls): ...          # @register_factor 装饰器入口
    @staticmethod
    def list_all() -> list[str]: ...
    @staticmethod
    def get_class(name: str) -> type: ...
    @staticmethod
    def create_instance(name: str, **kwargs): ...

def list_factor_meta() -> list[dict]: ...
```

（`create_instance` / `list_factor_meta` 等工厂函数名为 API 脚手架——源 §62.3 定注册机制，函数名实施时可调。）

元数据两层载体：

| 层 | 载体 | 内容 |
|---|---|---|
| 注册 + 静态元数据 | FactorRegistry + `@register_factor`；Factor 类属性自动提取 | factor_id / version / description / category / frequency / asset_types / depends_on / status / created_at |
| 质量指标 | metadata.json（factor_eval() 后写入） | ic_mean / ic_ir / ic_t / turnover / coverage |

status 三态：`draft → production → deprecated`。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 代码即注册表（FactorRegistry + 类属性 + metadata.json） | 独立 JSON/SQLite 注册表；公司式散在 usignal XML + UFormula + EB | 因子 <100，`list_all()` 遍历汇总即可；独立注册表引入"代码与注册表不一致"同步问题 |
| 注册与发现 = 构造函数注入 | ServiceManager 配置扫描 + 名字查找（公司式） | 依赖在 `__init__` 里给全（Python 对象引用），无需运行时发现（§35.4）；启动顺序=代码书写顺序（§35.5） |
| 元数据从 Factor 类自动提取 | 手工维护元数据表 | 代码即权威定义，提取零成本（§62.2 `META_FIELDS`） |
| status 三态 | 公司 ProductionStatus 5 色灯 | 5 色灯是数据质量传导机制；个人用 depends_hash 变化触发重算替代 |
| 与策略侧 alphaType 双表并存 | 合成一个注册表 | 两套各司其职：因子侧代码即注册表（§62.3），策略侧 alphaType JSON（§65） |
| 不建独立文档文件 | 每因子一个 md | docstring + description 属性即可（compute() 是权威定义；研究笔记写研究日志） |

## 实现要点

```python
META_FIELDS = [
    "name", "version", "description", "category", "frequency",
    "asset_types", "depends_on", "status", "created_at",
]

def list_factor_meta() -> list[dict]:
    """遍历 FactorRegistry 汇总所有因子元数据（因子 <100，遍历足够）。"""
    result = []
    for name in FactorRegistry.list_all():
        cls = FactorRegistry.get_class(name)
        result.append({f: getattr(cls, f, None) for f in META_FIELDS})
    return result
```

血缘不另建存储（§62.5）：`depends_on()` + FactorDAG + depends_hash 三件套已覆盖"依赖了谁/何时算的"，血缘图从 DAG 自动生成（`dag.visualize()`）；per-di 粒度血缘（公司 DependencyLineage）对个人过度。

## 实施时定稿项

- core/ 各文件的最小内容边界——registry.py 放几个 Registry（FactorRegistry / RuleRegistry 之外还要不要，§68.8）
- ~~因子注册表形态~~ 已定 → §62.2（代码即注册表，不建独立库）
