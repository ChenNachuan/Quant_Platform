"""因子抽象基类与 IO 契约。

依赖前缀契约（由 ``FactorDAG`` 解析组装）：
- 裸表名（无冒号）→ 原始数据表，经 ``data_api.load_bars()`` 加载
- ``"factor:xxx"`` → 上游因子，从因子存储读取或上游计算
- 其他前缀 → 显式抛出 ``ValueError``，禁止静默回退

``depends_on()`` 是输入依赖的静态声明（非事件订阅）。``compute()`` 默认返回严格三列长表
``DataFrame[date, ticker, {factor_name}]`` 的原始因子值，声明了 ``output_columns`` 的多列因子
返回 ``DataFrame[date, ticker, *output_columns]``；去极值、填充、标准化与中性化由 ``PostProcessor``
在消费侧完成。元数据直接声明为类属性（代码即注册表）。
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any, Iterable

import pandas as pd
from data.storage_schema import FACTOR_FREQUENCY_CHOICES

# ---------------------------------------------------------------------------
# 因子分类
# ---------------------------------------------------------------------------

#: 四大基础因子类别。
FACTOR_CATEGORIES: tuple[str, ...] = (
    "time_series",
    "cross_section",
    "event",
    "alternative",
)

#: 未归入四大类别时的默认分类。
DEFAULT_CATEGORY: str = "custom"

#: 全部合法的 ``category`` 取值。
VALID_CATEGORIES: frozenset[str] = frozenset(FACTOR_CATEGORIES) | {DEFAULT_CATEGORY}

#: 合法的因子计算/存储频率枚举（来自 storage_schema；``"tick"`` 仅作契约占位）。
FREQUENCY_CHOICES: tuple[str, ...] = FACTOR_FREQUENCY_CHOICES

# ---------------------------------------------------------------------------
# 依赖前缀契约
# ---------------------------------------------------------------------------

#: 标识上游因子依赖的前缀。
FACTOR_DEP_PREFIX: str = "factor:"

#: ``parse_dependency`` 返回类型：原始数据表。
KIND_TABLE: str = "table"
#: ``parse_dependency`` 返回类型：上游因子。
KIND_FACTOR: str = "factor"

# ---------------------------------------------------------------------------
# 因子命名规范：{type}_{semantic}_{params} 全小写 snake_case，至少两段且每段含字母
# ---------------------------------------------------------------------------

# 使用 \Z 而非 $，防止末尾换行符绕过校验并污染注册表键名
_NAME_RE: re.Pattern[str] = re.compile(
    r"^[a-z][a-z0-9]*(?:_[a-z0-9]*[a-z][a-z0-9]*)+\Z"
)

# ---------------------------------------------------------------------------
# 元数据提取
# ---------------------------------------------------------------------------

_META_FIELDS: tuple[str, ...] = (
    "name",
    "description",
    "category",
    "frequency",
    "asset_types",
    "version",
)


# ---------------------------------------------------------------------------
# 依赖解析
# ---------------------------------------------------------------------------


def parse_dependency(dep: str) -> tuple[str, str]:
    """将单条 ``depends_on()`` 依赖解析为 ``(kind, name)``。

    无冒号视为原始数据表 ``(KIND_TABLE, dep)``；``"factor:xxx"`` 解析为
    ``(KIND_FACTOR, "xxx")``。空字符串、空因子名或未识别前缀均抛出 ``ValueError``。
    """
    if not isinstance(dep, str) or not dep.strip():
        raise ValueError(f"dependency must be a non-empty string, got {dep!r}")

    dep = dep.strip()

    if ":" not in dep:
        return KIND_TABLE, dep

    prefix, _, name = dep.partition(":")
    prefix = prefix.strip()
    name = name.strip()

    if prefix == FACTOR_DEP_PREFIX.rstrip(":"):
        if not name:
            raise ValueError(f"factor dependency {dep!r} has an empty factor name")
        return KIND_FACTOR, name

    raise ValueError(
        f"unrecognised dependency prefix {prefix!r} in {dep!r}; "
        f"only 'factor:' is a recognised prefix — bare names are raw data tables"
    )


# ---------------------------------------------------------------------------
# 元数据提取函数
# ---------------------------------------------------------------------------


def factor_meta(cls: type) -> dict[str, Any]:
    """提取 ``Factor`` 子类的类级元数据字典（键为 ``_META_FIELDS``，缺失则为 ``None``）。"""
    return {f: getattr(cls, f, None) for f in _META_FIELDS}


def extract_factor_meta(
    factors: Iterable[type] | None = None,
) -> list[dict[str, Any]]:
    """批量提取因子类元数据；``factors`` 为 ``None`` 时遍历 ``FactorRegistry`` 全部已注册类。"""
    if factors is None:
        from core.registry import FactorRegistry

        factors = (FactorRegistry.get_class(n) for n in FactorRegistry.list_all())
    return [factor_meta(f) for f in factors]


# ---------------------------------------------------------------------------
# 输出 Schema 校验
# ---------------------------------------------------------------------------

_OUTPUT_KEY_COLS: tuple[str, ...] = ("date", "ticker")


def resolved_output_columns(factor: Factor | type[Factor] | str) -> tuple[str, ...]:
    """解析因子的输出值列名元组：``output_columns`` 非空时返回之，否则回退为 ``(factor.name,)``。"""
    if isinstance(factor, str):
        return (factor,)
    cols = getattr(factor, "output_columns", ())
    if cols:
        return tuple(cols)
    return (factor.name,)


def validate_output(factor: Factor | str, df: pd.DataFrame) -> pd.DataFrame:
    """校验 ``df`` 符合因子输出契约（前两列固定为 ``['date', 'ticker']``，其余列集合匹配声明输出列）。"""
    value_cols = resolved_output_columns(factor)
    expected_prefix = list(_OUTPUT_KEY_COLS)
    expected_set = set(_OUTPUT_KEY_COLS) | set(value_cols)
    actual = list(df.columns)
    if (
        actual[:2] != expected_prefix
        or len(actual) != len(expected_set)
        or set(actual) != expected_set
    ):
        expected = [*_OUTPUT_KEY_COLS, *value_cols]
        raise ValueError(f"compute() must return columns {expected}, got {actual}")
    return df


# ---------------------------------------------------------------------------
# Factor 抽象基类
# ---------------------------------------------------------------------------


class Factor(ABC):
    """所有因子的抽象基类。

    元数据以类属性形式声明（代码即注册表）；子类必须自行定义 ``name`` 并实现
    ``depends_on()`` 与 ``compute()``。
    """

    #: 唯一因子名，格式为 ``{type}_{semantic}_{params}``。
    name: str

    #: 一句话因子描述。
    description: str = ""

    #: 因子类别，须属于 ``VALID_CATEGORIES``。
    category: str = DEFAULT_CATEGORY

    #: 计算频率，须属于 ``FREQUENCY_CHOICES``（``"tick"`` 仅作占位）。
    frequency: str = "daily"

    #: 显式多列输出声明；为空元组时默认回退为单列 ``(name,)``。
    output_columns: tuple[str, ...] = ()

    #: 适用的数据表资产类型（不可变元组，避免子类共享可变默认值）。
    asset_types: tuple[str, ...] = ("equity",)

    #: 语义版本号，``compute()`` 逻辑变更时递增。
    version: str = "1.0"

    # -- 类构造期校验 -------------------------------------------------------

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        # 读取 cls.__dict__ 而非 getattr，防止子类未声明 name 时误继承父类标识
        name = cls.__dict__.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(
                f"{cls.__name__} must define its own non-empty 'name' class "
                "attribute (inherited names are not accepted)"
            )
        if not _NAME_RE.match(name):
            raise ValueError(
                f"factor name {name!r} does not match "
                f"{{type}}_{{semantic}}_{{params}} snake_case "
                f"(lowercase, at least two segments, every segment has a letter)"
            )

        category = getattr(cls, "category", DEFAULT_CATEGORY)
        if category not in VALID_CATEGORIES:
            raise ValueError(
                f"invalid category {category!r} on {cls.__name__}; "
                f"expected one of {sorted(VALID_CATEGORIES)}"
            )

        frequency = getattr(cls, "frequency", "daily")
        if not isinstance(frequency, str) or frequency not in FREQUENCY_CHOICES:
            raise ValueError(
                f"invalid frequency {frequency!r} on {cls.__name__}; "
                f"expected one of {FREQUENCY_CHOICES}"
            )

        output_columns = getattr(cls, "output_columns", ())
        if not isinstance(output_columns, (tuple, list)):
            raise ValueError(
                f"output_columns on {cls.__name__} must be a tuple or list, "
                f"got {type(output_columns).__name__}"
            )
        seen_cols: set[str] = set()
        for col in output_columns:
            if not isinstance(col, str) or not col.strip() or col != col.strip():
                raise ValueError(
                    f"invalid output column {col!r} on {cls.__name__}: "
                    "expected non-empty string without leading/trailing whitespace"
                )
            if col in _OUTPUT_KEY_COLS:
                raise ValueError(
                    f"output_columns on {cls.__name__} cannot contain reserved column {col!r}"
                )
            if col in seen_cols:
                raise ValueError(
                    f"duplicate column {col!r} in output_columns on {cls.__name__}"
                )
            seen_cols.add(col)

    # -- 抽象 IO 契约 -------------------------------------------------------

    @abstractmethod
    def depends_on(self) -> list[str]:
        """静态声明输入依赖列表（裸表名或 ``"factor:xxx"``），非事件订阅。"""
        ...

    @abstractmethod
    def compute(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """计算原始因子值。

        输入 ``data`` 为 ``depends_on()`` 各依赖名到对应 DataFrame 的映射；
        输出必须包含前导键列 ``[date, ticker]`` 及 ``output_columns``（默认 ``[{factor_name}]``）
        的原始未清洗值。
        """
        ...
