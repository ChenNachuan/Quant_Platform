"""共享注册机制：代码即注册表（``design/core/registry.md``）。

通过装饰器与类属性提取元数据，无独立持久化注册库且不引入业务域依赖；
仅包含 :class:`FactorRegistry` 与 :class:`RuleRegistry`（alphaType 注册另有 JSON 双表）。
"""

#: 通过 ``getattr`` 提取的类级元数据字段；缺失或为可调用对象（如方法 ``depends_on``）时记为 ``None``。
META_FIELDS = [
    "name",
    "version",
    "description",
    "category",
    "frequency",
    "asset_types",
    "depends_on",
    "status",
    "created_at",
]

# 三态生命周期：draft -> production -> deprecated
VALID_STATUSES = ("draft", "production", "deprecated")


class _Registry:
    """共享类注册基类。

    子类各自持有独立的 ``_store`` 字典，提供 ``register`` / ``list_all`` /
    ``get_class`` / ``create_instance`` 四个类方法接口。
    """

    _store: dict[str, type] = {}

    @classmethod
    def register(cls, klass: type) -> type:
        """注册 *klass* 并返回自身（支持 ``@...register`` 装饰器用法）。

        键名优先取 ``klass.name``，否则回退为 ``klass.__name__``。
        重复注册或显式 ``status`` 不在 :data:`VALID_STATUSES` 内时抛出 ``ValueError``；
        未声明 ``status`` 合法且在元数据中记为 ``None``。
        """
        status = getattr(klass, "status", None)
        if status is not None and status not in VALID_STATUSES:
            raise ValueError(
                f"invalid status {status!r} on {klass.__name__}; "
                f"expected one of {VALID_STATUSES}"
            )
        key = getattr(klass, "name", None) or klass.__name__
        if key in cls._store:
            raise ValueError(f"{key!r} is already registered in {cls.__name__}")
        cls._store[key] = klass
        return klass

    @classmethod
    def list_all(cls) -> list[str]:
        """按注册顺序返回全部已注册键名。"""
        return list(cls._store)

    @classmethod
    def get_class(cls, name: str) -> type:
        """返回 *name* 对应的已注册类；不存在时抛出 ``KeyError``。"""
        return cls._store[name]

    @classmethod
    def create_instance(cls, name: str, **kwargs):
        """使用 *kwargs* 实例化 *name* 对应的已注册类。"""
        return cls.get_class(name)(**kwargs)


class FactorRegistry(_Registry):
    """因子类注册表（基于类属性鸭子类型提取元数据，不强制校验 ``Factor`` 基类）。"""

    _store: dict[str, type] = {}


class RuleRegistry(_Registry):
    """规则类注册表，接口形态与 :class:`FactorRegistry` 一致。"""

    _store: dict[str, type] = {}


def register_factor(cls):
    """``FactorRegistry.register`` 的装饰器别名。"""
    return FactorRegistry.register(cls)


def _meta_value(klass: type, field: str):
    """读取类属性元数据字段；属性缺失或为方法等可调用对象时返回 ``None``。"""
    value = getattr(klass, field, None)
    return None if callable(value) else value


def list_factor_meta() -> list[dict]:
    """遍历 :class:`FactorRegistry` 并汇总所有已注册因子的 :data:`META_FIELDS` 元数据字典列表。"""
    result = []
    for name in FactorRegistry.list_all():
        klass = FactorRegistry.get_class(name)
        result.append({f: _meta_value(klass, f) for f in META_FIELDS})
    return result
