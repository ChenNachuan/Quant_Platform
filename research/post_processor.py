"""因子后处理层：按固定顺序执行消费侧截面清洗。

四阶段处理契约：
1. ``winsorize`` — MAD 去极值，边界 ``median ± n × 1.4826 × MAD``
2. ``fillna`` — 截面中位数填充
3. ``standardize`` — z-score 标准化（偏态因子可改用秩标准化）
4. ``neutralize``（可选独立调用）— 按日期对行业哑变量与对数市值做 OLS 残差中性化

所有步骤均按日期截面执行（``groupby(date)``），杜绝全样本统计带来的前视偏差；
仅在读取消费时清洗，不修改磁盘上的原始因子数据。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd
from numpy.linalg import lstsq, matrix_rank

__all__ = [
    "MAD_SCALE",
    "MIN_NEUTRALIZE_ROWS",
    "PIPELINE_ETF",
    "PIPELINE_RANK",
    "PIPELINE_RESEARCH",
    "PIPELINE_STOCK_EVAL",
    "PostProcessStep",
    "PostProcessor",
    "SIGMA_RTOL",
    "fillna_cross_section",
    "fillna_median",
    "neutralize",
    "standardize_rank",
    "standardize_zscore",
    "winsorize_mad",
]

#: 正态一致性常数（MAD × 1.4826 ≈ 高斯标准差）。
MAD_SCALE: float = 1.4826

#: neutralize 单日回归所需的最小有效行数，不足时原样透传。
MIN_NEUTRALIZE_ROWS: int = 10

#: 截面标准差相对容差，低于该比例视为浮点舍入噪声。
SIGMA_RTOL: float = 1e-12


# ---------------------------------------------------------------------------
# 序列级单截面处理步骤
# ---------------------------------------------------------------------------


def winsorize_mad(s: pd.Series, n: float = 3.0) -> pd.Series:
    """按 ``median ± n × 1.4826 × MAD`` 对单截面去极值；全 NaN 或 ``MAD == 0`` 截面原样返回，``n <= 0`` 抛出 ``ValueError``。"""
    if not n > 0:
        raise ValueError(f"n must be positive, got {n!r}")

    values = s.astype("float64")
    med = values.median()
    mad = (values - med).abs().median()
    if pd.isna(med) or pd.isna(mad) or mad == 0:
        return values
    half = n * MAD_SCALE * mad
    return values.clip(med - half, med + half)


def fillna_median(s: pd.Series) -> pd.Series:
    """用单截面中位数填充 ``NaN``；全 NaN 截面原样返回。"""
    values = s.astype("float64")
    med = values.median()
    if pd.isna(med):
        return values
    return values.fillna(med)


def _keep_null_key_rows(
    cleaned: pd.Series, original: pd.Series, keyed: pd.Series
) -> pd.Series:
    """还原分组键为空的原始行值，防止 ``groupby.transform`` 丢弃空键导致覆写为 NaN。"""
    return cleaned.where(keyed.notna(), original)


def standardize_zscore(s: pd.Series) -> pd.Series:
    """单截面 z-score 标准化 ``(x - mean) / std``；无有效离散度（``std`` 为 NaN 或低于浮点容差）时非缺失值置为 0。"""
    values = s.astype("float64")
    mu = values.mean()
    sigma = values.std()
    if pd.isna(sigma) or sigma <= SIGMA_RTOL * max(1.0, abs(mu)):
        return values.where(values.isna(), 0.0)
    return (values - mu) / sigma


def standardize_rank(s: pd.Series) -> pd.Series:
    """将单截面映射至 ``(0, 1]`` 百分比秩（保留 NaN），适用于重尾偏态因子。"""
    return s.rank(pct=True)


# ---------------------------------------------------------------------------
# DataFrame 级处理步骤
# ---------------------------------------------------------------------------


def fillna_cross_section(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """按 ``date`` 分组对 ``col`` 执行截面中位数填充，返回新副本。"""
    out = df.copy()
    filled = out.groupby("date")[col].transform(fillna_median)
    out[col] = _keep_null_key_rows(filled, out[col], out["date"])
    return out


def neutralize(
    df: pd.DataFrame,
    factor_col: str,
    industry_col: str = "industry",
    size_col: str = "log_market_cap",
    method: str = "industry_size",
) -> pd.DataFrame:
    """按日截面 OLS 回归中性化并将 ``factor_col`` 替换为残差，返回新副本。

    支持 ``"industry_size"``（截距 + 行业哑变量 + 对数市值）与 ``"industry"``。
    仅因子值、行业与市值（如启用）均有效的行参与拟合，其余行保留原值；
    有效行数不足 ``MIN_NEUTRALIZE_ROWS`` 或设计矩阵满秩（``rank >= n``）的日期跳过处理。
    """
    if method not in {"industry", "industry_size"}:
        raise ValueError(
            f"method must be 'industry' or 'industry_size', got {method!r}"
        )

    result = df.copy()
    # 使用位置索引执行分组写入，避免调用方存在重复索引时 loc 报错
    original_index = result.index
    result = result.reset_index(drop=True)

    for _, group in result.groupby("date"):
        y = group[factor_col].astype("float64").to_numpy()
        size = (
            group[size_col].astype("float64").to_numpy()
            if method == "industry_size"
            else None
        )
        valid = np.isfinite(y) & group[industry_col].notna().to_numpy()
        if size is not None:
            valid &= np.isfinite(size)
        if int(valid.sum()) < MIN_NEUTRALIZE_ROWS:
            continue

        parts: list[np.ndarray] = []
        dummies = pd.get_dummies(group[industry_col], drop_first=True)
        if dummies.shape[1] > 0:
            parts.append(dummies.to_numpy(dtype="float64"))
        if size is not None:
            parts.append(size.reshape(-1, 1))
        rhs = np.hstack(parts) if parts else np.empty((len(y), 0))
        x = np.column_stack([np.ones(len(y)), rhs])

        # 满秩插值会导致残差归零，要求至少保留 1 个自由度（rank < n）
        n_fit = int(valid.sum())
        if int(matrix_rank(x[valid])) >= n_fit:
            continue

        beta, _, _, _ = lstsq(x[valid], y[valid], rcond=None)
        fitted = y - x @ beta
        y[valid] = fitted[valid]
        result.loc[group.index, factor_col] = y

    result.index = original_index
    return result


# ---------------------------------------------------------------------------
# 管线与预设
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PostProcessStep:
    """管线单步定义（按日期分组执行的 ``Series → Series`` 变换）；冻结以防共享预设被篡改。"""

    name: str
    func: Callable[[pd.Series], pd.Series]
    params: dict = field(default_factory=dict)


class PostProcessor:
    """可配置因子清洗管线，按 ``date_col`` 逐日截面依次执行各步骤。"""

    def __init__(self, steps: list[PostProcessStep] | None = None):
        # 深拷贝步骤与参数字典，防止修改实例时污染共享预设常量
        self.steps = [
            PostProcessStep(name=step.name, func=step.func, params=dict(step.params))
            for step in (PIPELINE_RESEARCH if steps is None else steps)
        ]

    def transform(
        self, df: pd.DataFrame, factor_col: str, date_col: str = "date"
    ) -> pd.DataFrame:
        """按 ``date_col`` 分组依次执行清洗步骤，返回 ``df`` 的新副本。"""
        result = df.copy()
        for step in self.steps:
            cleaned = result.groupby(date_col)[factor_col].transform(
                step.func, **step.params
            )
            result[factor_col] = _keep_null_key_rows(
                cleaned, result[factor_col], result[date_col]
            )
        return result


# ---------------------------------------------------------------------------
# 只读预设常量（禁止原地写入，需通过 list(preset) 或 copy() 派生副本）
# ---------------------------------------------------------------------------

_PRESET_WRITE_REFUSED = (
    "preset pipelines are constants — derive a copy and edit that "
    "(list(preset); rebuild a PostProcessStep to change its params)"
)


class _FrozenParams(dict):
    """预设步骤的只读参数字典，禁止原地修改，可通过 ``copy()`` 获取可写副本。"""

    def _refuse(self, *args, **kwargs):
        raise TypeError(_PRESET_WRITE_REFUSED)

    __setitem__ = __delitem__ = __ior__ = _refuse
    clear = pop = popitem = setdefault = update = _refuse

    def copy(self) -> dict:
        """返回可写普通字典副本。"""
        return dict(self)

    def __reduce__(self):
        # 序列化为普通 dict，使 copy/deepcopy/pickle 能够正常重建可写副本
        return (dict, (dict(self),))


class _PresetSteps(list):
    """只读预设步骤列表（保留 ``list`` 子类以兼容签名），可通过 ``copy()`` 或 ``list(preset)`` 派生副本。"""

    def _refuse(self, *args, **kwargs):
        raise TypeError(_PRESET_WRITE_REFUSED)

    __setitem__ = __delitem__ = __iadd__ = __imul__ = _refuse
    append = clear = extend = insert = pop = remove = reverse = sort = _refuse

    def copy(self) -> list:
        """返回可写普通列表副本。"""
        return list(self)

    def __reduce__(self):
        # 序列化为普通 list，使 copy/deepcopy/pickle 能够正常重建可写副本
        return (list, (list(self),))


def _frozen_step(name: str, func: Callable[[pd.Series], pd.Series]) -> PostProcessStep:
    """构造携带只读参数字典的预设步骤。"""
    return PostProcessStep(name, func, _FrozenParams())


#: 研究初筛预设：去极值 → 填充 → z-score 标准化（不含中性化）。
PIPELINE_RESEARCH: list[PostProcessStep] = _PresetSteps(
    [
        _frozen_step("winsorize", winsorize_mad),
        _frozen_step("fillna", fillna_median),
        _frozen_step("standardize", standardize_zscore),
    ]
)

#: 个股评估预设：三步清洗，需在后续显式调用 ``neutralize`` 剔除行业与市值效应。
PIPELINE_STOCK_EVAL: list[PostProcessStep] = _PresetSteps(
    [
        _frozen_step("winsorize", winsorize_mad),
        _frozen_step("fillna", fillna_median),
        _frozen_step("standardize", standardize_zscore),
    ]
)

#: ETF 池预设：三步清洗，不做中性化以保留板块与风格轮动收益。
PIPELINE_ETF: list[PostProcessStep] = _PresetSteps(
    [
        _frozen_step("winsorize", winsorize_mad),
        _frozen_step("fillna", fillna_median),
        _frozen_step("standardize", standardize_zscore),
    ]
)

#: 偏态因子预设：去极值 → 填充 → 百分比秩标准化。
PIPELINE_RANK: list[PostProcessStep] = _PresetSteps(
    [
        _frozen_step("winsorize", winsorize_mad),
        _frozen_step("fillna", fillna_median),
        _frozen_step("standardize", standardize_rank),
    ]
)
