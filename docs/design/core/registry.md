# registry

> 域：core · 对应代码 [core/registry.py](core/registry.py)

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

元数据两层载体：

| 层 | 载体 | 内容 |
|---|---|---|
| 注册 + 静态元数据 | FactorRegistry + `@register_factor`；Factor 类属性自动提取 | factor_id / version / description / category / frequency / asset_types / depends_on / status / created_at |
| 质量指标 | metadata.json（factor_eval() 后写入） | ic_mean / ic_ir / ic_t / turnover / coverage |

status 三态：`draft → production → deprecated`。

## 关键决策

| 决策 | 否决项 | 理由 |
|---|---|---|
| 代码即注册表（FactorRegistry + 类属性 + metadata.json） | 独立 JSON/SQLite 注册表, 遍历汇总即可；独立注册表引入"代码与注册表不一致"同步问题 | 遍历汇总即可，独立注册表可能导致同步问题  | 
| 注册与发现 = 构造函数注入 | ServiceManager 配置扫描 | 依赖在 `__init__` 里给全，无需运行时发现；启动顺序=代码书写顺序 |
| 元数据从 Factor 类自动提取 | 手工维护元数据表 | 代码即权威定义，提取零成本 |
| status 三态 |  5 色灯 | 个人用 depends_hash 变化触发重算替代 |
| 不建独立文档文件 | 每因子一个 md | docstring + description 属性即可 |

## 实现要点

详见[core/registry.py](core/registry.py)

核心约定：
`@register_factor`装饰器注册因子类，`list_factor_meta()`遍历汇总元数据

血缘不另建存储：`depends_on()` + FactorDAG + depends_hash 三件套已覆盖追踪依赖，血缘图从 DAG 自动生成
因子侧FactorRegistry + metadata.json，策略侧 alphaType.JSON，两套并存各司其职

