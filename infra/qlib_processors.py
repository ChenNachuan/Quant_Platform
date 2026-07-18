from __future__ import annotations

from typing import Iterable, Literal

import numpy as np
import pandas as pd

from qlib.data.dataset.processor import Processor


LabelMode = Literal[
    "raw_return",
    "standardized_return",
    "return_quantile",
    "quantile_regression",
    "log_return",
    "log_return_zscore",
    "state",
]


def make_future_return_label_expr(
    horizon: int,
    *,
    price_field: str = "$vwap",
    base_lag: int = 1,
    eps: float = 1e-12,
) -> str:
    """生成未来收益率标签表达式。

    口径为 t+base_lag 到 t+base_lag+horizon 的收益，默认保持现有 notebook 的
    Ref($vwap,-h-1) / Ref($vwap,-1) - 1 写法，避免使用当前 bar 价格。
    """
    horizon = int(horizon)
    base_lag = int(base_lag)
    if horizon <= 0:
        raise ValueError("horizon 必须大于 0")
    if base_lag < 0:
        raise ValueError("base_lag 不能小于 0")
    return f"Ref({price_field},-{horizon + base_lag})/(Ref({price_field},-{base_lag})+{float(eps)})-1"


def make_future_log_return_label_expr(
    horizon: int,
    *,
    price_field: str = "$vwap",
    base_lag: int = 1,
    eps: float = 1e-12,
) -> str:
    """生成未来对数收益标签表达式。"""
    horizon = int(horizon)
    base_lag = int(base_lag)
    if horizon <= 0:
        raise ValueError("horizon 必须大于 0")
    if base_lag < 0:
        raise ValueError("base_lag 不能小于 0")
    return f"Log((Ref({price_field},-{horizon + base_lag})+{float(eps)})/(Ref({price_field},-{base_lag})+{float(eps)}))"


def make_return_scale_label_expr(
    horizon: int,
    *,
    price_field: str = "$vwap",
    vol_window: int = 60,
    sqrt_horizon: bool = True,
    eps: float = 1e-12,
) -> str:
    """生成标准化收益还原尺度：历史1步收益波动率 * sqrt(horizon)。"""
    horizon = int(horizon)
    vol_window = int(vol_window)
    if horizon <= 0:
        raise ValueError("horizon 必须大于 0")
    if vol_window <= 1:
        raise ValueError("vol_window 必须大于 1")
    multiplier = float(np.sqrt(horizon)) if sqrt_horizon else 1.0
    return f"(Std({price_field}/(Ref({price_field},1)+{float(eps)})-1,{vol_window})*{multiplier})"


def make_log_return_scale_label_expr(
    horizon: int,
    *,
    price_field: str = "$vwap",
    vol_window: int = 60,
    sqrt_horizon: bool = True,
    eps: float = 1e-12,
) -> str:
    """生成对数收益标准化尺度：历史1步 log return 波动率 * sqrt(horizon)。"""
    horizon = int(horizon)
    vol_window = int(vol_window)
    if horizon <= 0:
        raise ValueError("horizon 必须大于 0")
    if vol_window <= 1:
        raise ValueError("vol_window 必须大于 1")
    multiplier = float(np.sqrt(horizon)) if sqrt_horizon else 1.0
    return f"(Std(Log(({price_field}+{float(eps)})/(Ref({price_field},1)+{float(eps)})),{vol_window})*{multiplier})"


def make_standardized_return_label_expr(
    horizon: int,
    *,
    price_field: str = "$vwap",
    base_lag: int = 1,
    vol_window: int = 60,
    sqrt_horizon: bool = True,
    eps: float = 1e-12,
) -> str:
    """生成波动标准化后的未来收益标签表达式。"""
    raw_expr = make_future_return_label_expr(
        horizon,
        price_field=price_field,
        base_lag=base_lag,
        eps=eps,
    )
    scale_expr = make_return_scale_label_expr(
        horizon,
        price_field=price_field,
        vol_window=vol_window,
        sqrt_horizon=sqrt_horizon,
        eps=eps,
    )
    return f"({raw_expr})/({scale_expr}+{float(eps)})"


def make_standardized_log_return_label_expr(
    horizon: int,
    *,
    price_field: str = "$vwap",
    base_lag: int = 1,
    vol_window: int = 60,
    sqrt_horizon: bool = True,
    eps: float = 1e-12,
) -> str:
    """生成波动标准化后的未来对数收益标签表达式。"""
    raw_expr = make_future_log_return_label_expr(
        horizon,
        price_field=price_field,
        base_lag=base_lag,
        eps=eps,
    )
    scale_expr = make_log_return_scale_label_expr(
        horizon,
        price_field=price_field,
        vol_window=vol_window,
        sqrt_horizon=sqrt_horizon,
        eps=eps,
    )
    return f"({raw_expr})/({scale_expr}+{float(eps)})"


def make_state_label_expr(
    horizon: int,
    *,
    price_field: str = "$vwap",
    base_lag: int = 1,
    up_threshold: float = 0.0002,
    down_threshold: float = -0.0002,
    eps: float = 1e-12,
) -> str:
    """生成三状态标签表达式：0=down，1=flat，2=up。"""
    if float(down_threshold) >= float(up_threshold):
        raise ValueError("down_threshold 必须小于 up_threshold")
    raw_expr = make_future_return_label_expr(
        horizon,
        price_field=price_field,
        base_lag=base_lag,
        eps=eps,
    )
    return f"If(({raw_expr})>{float(up_threshold)},2,If(({raw_expr})<{float(down_threshold)},0,1))"


def make_label_expr(
    *,
    mode: LabelMode = "raw_return",
    horizon: int = 5,
    price_field: str = "$vwap",
    base_lag: int = 1,
    vol_window: int = 60,
    sqrt_horizon: bool = True,
    up_threshold: float = 0.0002,
    down_threshold: float = -0.0002,
    eps: float = 1e-12,
) -> str:
    """按模式生成标签表达式，供 DatasetH 的 QlibDataLoader 使用。"""
    if mode == "raw_return":
        return make_future_return_label_expr(
            horizon,
            price_field=price_field,
            base_lag=base_lag,
            eps=eps,
        )
    if mode == "standardized_return":
        return make_standardized_return_label_expr(
            horizon,
            price_field=price_field,
            base_lag=base_lag,
            vol_window=vol_window,
            sqrt_horizon=sqrt_horizon,
            eps=eps,
        )
    if mode in {"return_quantile", "quantile_regression"}:
        # 分位数映射必须只在训练集拟合，不能在 qlib 表达式层直接计算。
        # 因此这里先返回原始收益率，后续由模型训练流程转换为 train-fitted quantile。
        return make_future_return_label_expr(
            horizon,
            price_field=price_field,
            base_lag=base_lag,
            eps=eps,
        )
    if mode == "log_return":
        return make_future_log_return_label_expr(
            horizon,
            price_field=price_field,
            base_lag=base_lag,
            eps=eps,
        )
    if mode == "log_return_zscore":
        return make_standardized_log_return_label_expr(
            horizon,
            price_field=price_field,
            base_lag=base_lag,
            vol_window=vol_window,
            sqrt_horizon=sqrt_horizon,
            eps=eps,
        )
    if mode == "state":
        return make_state_label_expr(
            horizon,
            price_field=price_field,
            base_lag=base_lag,
            up_threshold=up_threshold,
            down_threshold=down_threshold,
            eps=eps,
        )
    raise ValueError(
        "mode 仅支持 raw_return / standardized_return / return_quantile / quantile_regression / log_return / log_return_zscore / state"
    )


def make_label_learn_processors(
    *,
    label_clip_abs: float | None = 0.2,
    label_mode: LabelMode = "raw_return",
    fields_group: str = "label",
) -> list[dict]:
    """生成标签清洗处理器配置。

    状态标签是离散类别，不做绝对值裁剪；连续收益标签保留可选 clip。
    """
    clip_abs = None if label_mode == "state" else label_clip_abs
    return [
        {
            "class": "LabelInfClip",
            "module_path": "utils.custom_processors",
            "kwargs": {"fields_group": fields_group, "clip_abs": clip_abs},
        },
        {"class": "DropnaLabel"},
    ]


class FixInfAndZeroFFill(Processor):
    """特征清洗：
    1) inf/-inf -> NaN
    2) 指定列 0 -> NaN
    3) 按标的分组前向填充（用上一分钟值替代）"""

    def __init__(
        self,
        fields_group: str | None = "feature",
        zero_as_nan_cols: Iterable[str] | None = ("vwap",),
    ):
        self.fields_group = fields_group
        self.zero_as_nan_cols = (
            list(zero_as_nan_cols) if zero_as_nan_cols is not None else None
        )

    def __call__(self, df: pd.DataFrame):
        if self.fields_group is None:
            sub = df.copy()
        else:
            sub = df[self.fields_group].copy()

        # 无穷值转成缺失值
        sub = sub.replace([np.inf, -np.inf], np.nan)

        # 指定列把 0 当作异常值
        if self.zero_as_nan_cols is not None:
            cols = [c for c in self.zero_as_nan_cols if c in sub.columns]
            if cols:
                sub.loc[:, cols] = sub.loc[:, cols].mask(sub.loc[:, cols] == 0, np.nan)

        # 按时间排序后进行前向填充
        sub = sub.sort_index()
        if isinstance(sub.index, pd.MultiIndex):
            level = "instrument" if "instrument" in (sub.index.names or []) else 0
            sub = sub.groupby(level=level, group_keys=False).ffill()
        else:
            sub = sub.ffill()

        if self.fields_group is None:
            return sub

        df[self.fields_group] = sub
        return df


class LabelInfClip(Processor):
    """标签清洗：
    1) inf/-inf -> NaN
    2) 可选绝对值裁剪，防止极端标签主导训练"""

    def __init__(self, fields_group: str = "label", clip_abs: float | None = 0.2):
        self.fields_group = fields_group
        self.clip_abs = clip_abs

    def __call__(self, df: pd.DataFrame):
        sub = df[self.fields_group].copy()
        sub = sub.replace([np.inf, -np.inf], np.nan)
        if self.clip_abs is not None:
            sub = sub.clip(lower=-float(self.clip_abs), upper=float(self.clip_abs))
        df[self.fields_group] = sub
        return df
