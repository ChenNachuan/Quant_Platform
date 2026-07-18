from __future__ import annotations

import re
from typing import Callable

import numpy as np
import pandas as pd
from qlib.data.dataset import DatasetH

from factor_library.futures.minute_factors import MinuteFactorLibrary

__all__ = [
    "SPREAD_INSTRUMENT",
    "SpreadDataFrameLoader",
    "build_spread_dataset",
    "build_spread_feature_names",
    "compute_minute_factors",
]

SPREAD_INSTRUMENT = "instrument1_instrument2_spread"
RAW_FIELDS = ["open", "high", "low", "close", "volume", "vwap", "total_turnover"]


def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.index, pd.MultiIndex):
        if "datetime" in df.index.names:
            df = df.droplevel([n for n in df.index.names if n != "datetime"])
        else:
            df = df.droplevel(0)
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    return df.sort_index()


def _normalize_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [str(c[-1]).lstrip("$") for c in df.columns]
    else:
        df = df.rename(columns=lambda c: str(c).lstrip("$"))

    if "vwap" not in df.columns:
        if {"total_turnover", "volume"}.issubset(df.columns):
            df["vwap"] = df["total_turnover"] / (df["volume"] + 1e-12)
        elif "close" in df.columns:
            df["vwap"] = df["close"]

    if "total_turnover" not in df.columns and {"vwap", "volume"}.issubset(df.columns):
        df["total_turnover"] = df["vwap"] * df["volume"]

    missing = [c for c in RAW_FIELDS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing raw fields for spread dataset: {missing}")
    return df[RAW_FIELDS].astype("float64")


def _ref(x: pd.Series, n: int | float) -> pd.Series:
    return x.shift(int(n))


def _rolling_func(name: str) -> Callable:
    def func(x: pd.Series, n: int | float) -> pd.Series:
        window = int(n)
        return getattr(x.rolling(window, min_periods=window), name)()

    return func


def _ema(x: pd.Series, n: int | float) -> pd.Series:
    window = int(n)
    return x.ewm(span=window, adjust=False, min_periods=window).mean()


def _max(x, y):
    if isinstance(x, pd.Series) and np.isscalar(y) and int(y) == y and int(y) > 1:
        window = int(y)
        return x.rolling(window, min_periods=window).max()
    return np.maximum(x, y)


def _min(x, y):
    if isinstance(x, pd.Series) and np.isscalar(y) and int(y) == y and int(y) > 1:
        window = int(y)
        return x.rolling(window, min_periods=window).min()
    return np.minimum(x, y)


def _sum(x: pd.Series, n: int | float) -> pd.Series:
    window = int(n)
    return x.rolling(window, min_periods=window).sum()


def _corr(x: pd.Series, y: pd.Series, n: int | float) -> pd.Series:
    window = int(n)
    return x.rolling(window, min_periods=window).corr(y)


def _if(cond, x, y):
    return pd.Series(
        np.where(cond, x, y), index=cond.index if hasattr(cond, "index") else None
    )


def _safe_log(x):
    x = pd.to_numeric(x, errors="coerce")
    x = x.where(np.isfinite(x) & (x > 0))
    return np.log(x)


def _to_python_expr(expr: str) -> str:
    expr = re.sub(r"\$([A-Za-z_][A-Za-z0-9_]*)", r'df["\1"]', expr)
    for qlib_name, py_name in {
        "Ref": "_ref",
        "Mean": "_mean",
        "EMA": "_ema",
        "Std": "_std",
        "Max": "_max",
        "Min": "_min",
        "Abs": "np.abs",
        "Log": "_safe_log",
        "Sum": "_sum",
        "If": "_if",
        "Corr": "_corr",
    }.items():
        expr = re.sub(rf"\b{qlib_name}\s*\(", f"{py_name}(", expr)
    return expr


def compute_minute_factors(
    raw_df: pd.DataFrame,
    selected_factors: list[str] | None = None,
) -> pd.DataFrame:
    """用 pandas 计算 MinuteFactorLibrary 中的 qlib 风格分钟因子。"""
    df = _normalize_feature_frame(_ensure_datetime_index(raw_df))
    factor_names = (
        MinuteFactorLibrary.list_factor_names()
        if not selected_factors
        else list(selected_factors)
    )
    unknown = [name for name in factor_names if name not in MinuteFactorLibrary.FACTORS]
    if unknown:
        raise ValueError(
            f"Unknown factor names: {unknown}. Available factors: {MinuteFactorLibrary.list_factor_names()}"
        )

    env = {
        "df": df,
        "np": np,
        "_ref": _ref,
        "_mean": _rolling_func("mean"),
        "_ema": _ema,
        "_std": _rolling_func("std"),
        "_max": _max,
        "_min": _min,
        "_sum": _sum,
        "_if": _if,
        "_corr": _corr,
        "_safe_log": _safe_log,
    }

    out = {}
    for name in factor_names:
        py_expr = _to_python_expr(MinuteFactorLibrary.FACTORS[name])
        out[name] = eval(py_expr, {"__builtins__": {}}, env)
    return pd.DataFrame(out, index=df.index)


def build_spread_feature_names(
    selected_factors: list[str] | None = None,
    instrument1_prefix: str = "instrument1",
    instrument2_prefix: str = "instrument2",
) -> list[str]:
    names = (
        MinuteFactorLibrary.list_factor_names()
        if not selected_factors
        else list(selected_factors)
    )
    return [
        f"{prefix}_{name}"
        for prefix in (instrument1_prefix, instrument2_prefix, "spread")
        for name in names
    ]


class SpreadDataFrameLoader:
    """DataLoader for one synthetic two-instrument spread instrument.

    It reads two instruments' raw fields from the initialized qlib provider, computes:
    1. instrument1 factors
    2. instrument2 factors
    3. instrument1-instrument2 spread factors
    and returns a DataFrame shaped like a normal qlib feature/label table.
    """

    def __init__(
        self,
        instrument1: str,
        instrument2: str,
        selected_factors: list[str] | None = None,
        label_horizon: int = 6,
        label_base_lag: int = 1,
        label_price_field: str = "vwap",
        freq: str = "1min",
        spread_instrument: str = SPREAD_INSTRUMENT,
        instrument1_prefix: str = "instrument1",
        instrument2_prefix: str = "instrument2",
    ):
        self.instrument1 = instrument1
        self.instrument2 = instrument2
        self.selected_factors = selected_factors
        self.label_horizon = int(label_horizon)
        self.label_base_lag = int(label_base_lag)
        self.label_price_field = label_price_field
        self.freq = freq
        self.spread_instrument = spread_instrument
        self.instrument1_prefix = instrument1_prefix
        self.instrument2_prefix = instrument2_prefix

    def load(self, instruments=None, start_time=None, end_time=None) -> pd.DataFrame:
        from qlib.data import D

        instrument1_raw = self._load_raw_fields(
            D, self.instrument1, start_time, end_time
        )
        instrument2_raw = self._load_raw_fields(
            D, self.instrument2, start_time, end_time
        )

        instrument1_raw = _normalize_feature_frame(
            _ensure_datetime_index(instrument1_raw)
        )
        instrument2_raw = _normalize_feature_frame(
            _ensure_datetime_index(instrument2_raw)
        )
        common_index = instrument1_raw.index.intersection(instrument2_raw.index)
        if common_index.empty:
            raise ValueError(
                f"No overlapping timestamps between {self.instrument1} and {self.instrument2} "
                f"from {start_time} to {end_time}."
            )
        instrument1_raw = instrument1_raw.loc[common_index]
        instrument2_raw = instrument2_raw.loc[common_index]

        spread_raw = pd.DataFrame(index=common_index)
        spread_raw["open"] = instrument1_raw["open"] - instrument2_raw["open"]
        spread_raw["high"] = instrument1_raw["high"] - instrument2_raw["low"]
        spread_raw["low"] = instrument1_raw["low"] - instrument2_raw["high"]
        spread_raw["close"] = instrument1_raw["close"] - instrument2_raw["close"]
        spread_raw["volume"] = instrument1_raw["volume"] + instrument2_raw["volume"]
        spread_raw["vwap"] = instrument1_raw["vwap"] - instrument2_raw["vwap"]
        spread_raw["total_turnover"] = (
            instrument1_raw["total_turnover"] + instrument2_raw["total_turnover"]
        )

        instrument1_factor = compute_minute_factors(
            instrument1_raw, self.selected_factors
        ).add_prefix(f"{self.instrument1_prefix}_")
        instrument2_factor = compute_minute_factors(
            instrument2_raw, self.selected_factors
        ).add_prefix(f"{self.instrument2_prefix}_")
        spread_factor = compute_minute_factors(
            spread_raw, self.selected_factors
        ).add_prefix("spread_")
        feature = pd.concat(
            [instrument1_factor, instrument2_factor, spread_factor], axis=1
        )

        if self.label_price_field not in spread_raw.columns:
            raise ValueError(
                f"label_price_field={self.label_price_field!r} not found in spread fields. "
                f"Available fields: {list(spread_raw.columns)}"
            )
        # spread_price = spread_raw[self.label_price_field]
        # label = (
        #     spread_price.shift(-self.label_horizon)
        #     / (spread_price.shift(-self.label_base_lag) + 1e-12)
        #     - 1
        # ).rename("LABEL0")
        spread_price = spread_raw[self.label_price_field]
        label = (
            (
                spread_price.shift(-self.label_horizon)
                - spread_price.shift(-self.label_base_lag)
            )
            / (
                instrument2_raw[self.label_price_field].shift(-self.label_base_lag)
                + 1e-12
            )
        ).rename("LABEL0")

        out = pd.concat({"feature": feature, "label": label.to_frame()}, axis=1)
        out.index = pd.MultiIndex.from_arrays(
            [out.index, [self.spread_instrument] * len(out)],
            names=["datetime", "instrument"],
        )
        return out.sort_index()

    def _load_raw_fields(
        self, data_api, instrument: str, start_time, end_time
    ) -> pd.DataFrame:
        field_candidates = [
            [f"${f}" for f in RAW_FIELDS],
            ["$open", "$high", "$low", "$close", "$volume", "$vwap"],
            ["$open", "$high", "$low", "$close", "$volume"],
        ]
        last_error: Exception | None = None
        for fields in field_candidates:
            try:
                return data_api.features(
                    [instrument],
                    fields,
                    start_time=start_time,
                    end_time=end_time,
                    freq=self.freq,
                )
            except Exception as exc:
                last_error = exc
        raise RuntimeError(
            f"Failed to load raw fields for {instrument}"
        ) from last_error


def _default_zero_as_nan_cols(
    selected_factors: list[str] | None,
    instrument1_prefix: str = "instrument1",
    instrument2_prefix: str = "instrument2",
) -> list[str]:
    factor_names = (
        MinuteFactorLibrary.list_factor_names()
        if not selected_factors
        else list(selected_factors)
    )
    cols = []
    for raw_name in ("vwap", "total_turnover", "volume"):
        if raw_name in factor_names:
            cols.extend(
                [
                    f"{instrument1_prefix}_{raw_name}",
                    f"{instrument2_prefix}_{raw_name}",
                    f"spread_{raw_name}",
                ]
            )
    return cols


def build_spread_dataset(
    start_time: str,
    end_time: str,
    train_end: str,
    valid_end: str,
    instrument1: str,
    instrument2: str,
    valid_start: str | None = None,
    test_start: str | None = None,
    selected_factors: list[str] | None = None,
    instruments: list[str] | None = None,
    spread_instrument: str | None = None,
    label_expr: str | None = None,
    label_horizon: int = 6,
    label_base_lag: int = 1,
    label_price_field: str = "vwap",
    use_robust_preprocess: bool = True,
    fillna_value: float = 0.0,
    use_process_inf: bool = False,
    zero_as_nan_cols: list[str] | None = None,
    label_clip_abs: float | None = 0.2,
) -> DatasetH:
    """构建双标的价差 ALSTM 数据集。

    特征为 instrument1 因子、instrument2 因子、instrument1-instrument2 价差因子三组拼接；
    标签为未来价差收益率，可用 `label_price_field` 指定用 open/high/low/close/vwap 等价差字段计算。
    用 `instrument1` 和 `instrument2` 显式指定两个标的。
    使用前请先用包含这两个 1min 标的的 qlib provider 初始化 qlib。
    """
    instrument1_prefix = "instrument1"
    instrument2_prefix = "instrument2"
    if spread_instrument is None:
        spread_instrument = f"{instrument1}_{instrument2}_spread"

    if label_expr is not None and label_expr != "spread_raw_return":
        print(
            "label_expr is ignored by build_spread_dataset; use label_horizon/label_base_lag instead."
        )

    infer_processors = []
    if use_process_inf:
        infer_processors.append({"class": "ProcessInf"})

    infer_processors.append(
        {
            "class": "FixInfAndZeroFFill",
            "module_path": "utils.custom_processors",
            "kwargs": {
                "fields_group": "feature",
                "zero_as_nan_cols": _default_zero_as_nan_cols(
                    selected_factors,
                    instrument1_prefix=instrument1_prefix,
                    instrument2_prefix=instrument2_prefix,
                )
                if zero_as_nan_cols is None
                else zero_as_nan_cols,
            },
        }
    )

    if use_robust_preprocess:
        infer_processors.append(
            {
                "class": "RobustZScoreNorm",
                "kwargs": {
                    "fit_start_time": start_time,
                    "fit_end_time": train_end,
                    "fields_group": "feature",
                    "clip_outlier": True,
                },
            }
        )

    infer_processors.append(
        {
            "class": "Fillna",
            "kwargs": {"fields_group": "feature", "fill_value": fillna_value},
        }
    )

    learn_processors = [
        {
            "class": "LabelInfClip",
            "module_path": "utils.custom_processors",
            "kwargs": {"fields_group": "label", "clip_abs": label_clip_abs},
        },
        {"class": "DropnaLabel"},
    ]

    handler_config = {
        "class": "DataHandlerLP",
        "module_path": "qlib.data.dataset.handler",
        "kwargs": {
            "instruments": instruments
            if instruments is not None
            else [spread_instrument],
            "start_time": start_time,
            "end_time": end_time,
            "infer_processors": infer_processors,
            "learn_processors": learn_processors,
            "data_loader": {
                "class": "SpreadDataFrameLoader",
                "module_path": "utils.spread_dataset",
                "kwargs": {
                    "instrument1": instrument1,
                    "instrument2": instrument2,
                    "selected_factors": selected_factors,
                    "label_horizon": label_horizon,
                    "label_base_lag": label_base_lag,
                    "label_price_field": label_price_field,
                    "spread_instrument": spread_instrument,
                    "instrument1_prefix": instrument1_prefix,
                    "instrument2_prefix": instrument2_prefix,
                },
            },
        },
    }

    return DatasetH(
        handler=handler_config,
        segments={
            "train": (start_time, train_end),
            "valid": (valid_start if valid_start is not None else train_end, valid_end),
            "test": (test_start if test_start is not None else valid_end, end_time),
        },
    )
