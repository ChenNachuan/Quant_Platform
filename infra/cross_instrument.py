from __future__ import annotations

import numpy as np
import pandas as pd
from qlib.data.dataset import DatasetH
from qlib.data.storage.file_storage import FileFeatureStorage

from infra.qlib_processors import make_label_learn_processors
from factor_library.futures.minute_factors import MinuteFactorLibrary
from infra.spread_dataset import (
    RAW_FIELDS,
    _ensure_datetime_index,
    _normalize_feature_frame,
    compute_minute_factors,
)


def build_cross_instrument_feature_names(
    selected_factors: list[str] | None = None,
    selected_cross_features: list[str] | None = None,
    target_prefix: str = "IF",
    context_prefix: str = "IC",
    context_prefixes: list[str] | None = None,
) -> list[str]:
    if selected_cross_features:
        return list(selected_cross_features)
    factor_names = (
        MinuteFactorLibrary.list_factor_names()
        if not selected_factors
        else list(selected_factors)
    )
    prefixes = [target_prefix] + (
        list(context_prefixes) if context_prefixes is not None else [context_prefix]
    )
    return [f"{prefix}_{name}" for prefix in prefixes for name in factor_names]


def _as_list(value, fallback=None) -> list:
    if value is None:
        return [] if fallback is None else list(fallback)
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


class CrossInstrumentFeatureLoader:
    """为单个目标标的构建跨标的特征。

    输出索引仍是 target_instrument，因此标签、预测和回测都保持 IF 口径；
    特征中额外拼接 context_instrument 的同名分钟因子。
    """

    def __init__(
        self,
        target_instrument: str = "IF",
        context_instrument: str = "IC",
        context_instruments: list[str] | None = None,
        target_provider_uri: str | None = None,
        context_provider_uri: str | None = None,
        context_provider_uris: list[str | None] | None = None,
        selected_factors: list[str] | None = None,
        selected_cross_features: list[str] | None = None,
        label_mode: str = "raw_return",
        label_horizon: int = 5,
        label_base_lag: int = 1,
        label_vol_window: int = 60,
        label_sqrt_horizon: bool = True,
        label_up_threshold: float = 0.0002,
        label_down_threshold: float = -0.0002,
        label_eps: float = 1e-12,
        label_price_field: str = "vwap",
        freq: str = "1min",
        target_prefix: str = "IF",
        context_prefix: str = "IC",
        context_prefixes: list[str] | None = None,
    ):
        self.target_instrument = target_instrument
        self.context_instruments = _as_list(
            context_instruments, fallback=[context_instrument]
        )
        self.context_instrument = (
            self.context_instruments[0]
            if self.context_instruments
            else context_instrument
        )
        self.target_provider_uri = target_provider_uri
        self.context_provider_uris = _as_list(
            context_provider_uris,
            fallback=[context_provider_uri] * len(self.context_instruments),
        )
        if len(self.context_provider_uris) < len(self.context_instruments):
            self.context_provider_uris.extend(
                [None]
                * (len(self.context_instruments) - len(self.context_provider_uris))
            )
        self.context_provider_uri = (
            self.context_provider_uris[0]
            if self.context_provider_uris
            else context_provider_uri
        )
        self.selected_factors = selected_factors
        self.selected_cross_features = selected_cross_features
        self.label_mode = label_mode
        self.label_horizon = int(label_horizon)
        self.label_base_lag = int(label_base_lag)
        self.label_vol_window = int(label_vol_window)
        self.label_sqrt_horizon = bool(label_sqrt_horizon)
        self.label_up_threshold = float(label_up_threshold)
        self.label_down_threshold = float(label_down_threshold)
        self.label_eps = float(label_eps)
        self.label_price_field = str(label_price_field).lstrip("$")
        self.freq = freq
        self.target_prefix = target_prefix
        self.context_prefixes = _as_list(
            context_prefixes, fallback=self.context_instruments or [context_prefix]
        )
        if len(self.context_prefixes) < len(self.context_instruments):
            self.context_prefixes.extend(
                self.context_instruments[len(self.context_prefixes) :]
            )
        self.context_prefix = (
            self.context_prefixes[0] if self.context_prefixes else context_prefix
        )

    def load(self, instruments=None, start_time=None, end_time=None) -> pd.DataFrame:
        from qlib.data import D

        target_raw = self._load_raw_fields(
            D,
            self.target_instrument,
            start_time,
            end_time,
            provider_uri=self.target_provider_uri,
        )
        target_raw = _normalize_feature_frame(_ensure_datetime_index(target_raw))

        context_raw_map = {}
        common_index = target_raw.index
        for context_instrument, context_provider_uri in zip(
            self.context_instruments, self.context_provider_uris
        ):
            context_raw = self._load_raw_fields(
                D,
                context_instrument,
                start_time,
                end_time,
                provider_uri=context_provider_uri,
            )
            context_raw = _normalize_feature_frame(_ensure_datetime_index(context_raw))
            context_raw_map[context_instrument] = context_raw
            common_index = common_index.intersection(context_raw.index)

        if common_index.empty:
            raise ValueError(
                f"No overlapping timestamps between {self.target_instrument} and {self.context_instruments} "
                f"from {start_time} to {end_time}."
            )
        target_raw = target_raw.loc[common_index]
        context_raw_map = {
            name: raw.loc[common_index] for name, raw in context_raw_map.items()
        }

        target_factor = compute_minute_factors(
            target_raw, self.selected_factors
        ).add_prefix(f"{self.target_prefix}_")
        context_factors = [
            compute_minute_factors(
                context_raw_map[name], self.selected_factors
            ).add_prefix(f"{prefix}_")
            for name, prefix in zip(self.context_instruments, self.context_prefixes)
        ]
        feature = pd.concat([target_factor, *context_factors], axis=1)
        if self.selected_cross_features:
            missing = [
                name
                for name in self.selected_cross_features
                if name not in feature.columns
            ]
            if missing:
                raise ValueError(
                    f"selected_cross_features 中存在未知特征: {missing[:10]}"
                )
            feature = feature.loc[:, list(self.selected_cross_features)]
        label = self._make_label(target_raw).rename("LABEL0")

        out = pd.concat({"feature": feature, "label": label.to_frame()}, axis=1)
        out.index = pd.MultiIndex.from_arrays(
            [out.index, [self.target_instrument] * len(out)],
            names=["datetime", "instrument"],
        )
        return out.sort_index()

    def _make_label(self, raw_df: pd.DataFrame) -> pd.Series:
        if self.label_price_field not in raw_df.columns:
            raise ValueError(
                f"label_price_field={self.label_price_field!r} 不在原始字段中"
            )
        price = raw_df[self.label_price_field]
        raw_return = (
            price.shift(-(self.label_horizon + self.label_base_lag))
            / (price.shift(-self.label_base_lag) + self.label_eps)
            - 1.0
        )

        log_return = np.log(
            (price.shift(-(self.label_horizon + self.label_base_lag)) + self.label_eps)
            / (price.shift(-self.label_base_lag) + self.label_eps)
        )

        if self.label_mode in {"raw_return", "return_quantile", "quantile_regression"}:
            return raw_return
        if self.label_mode == "log_return":
            return log_return
        if self.label_mode == "standardized_return":
            one_step_return = price / (price.shift(1) + self.label_eps) - 1.0
            multiplier = np.sqrt(self.label_horizon) if self.label_sqrt_horizon else 1.0
            scale = (
                one_step_return.rolling(
                    self.label_vol_window, min_periods=self.label_vol_window
                ).std()
                * multiplier
            )
            return raw_return / (scale + self.label_eps)
        if self.label_mode == "log_return_zscore":
            one_step_log_return = np.log(
                (price + self.label_eps) / (price.shift(1) + self.label_eps)
            )
            multiplier = np.sqrt(self.label_horizon) if self.label_sqrt_horizon else 1.0
            scale = (
                one_step_log_return.rolling(
                    self.label_vol_window, min_periods=self.label_vol_window
                ).std()
                * multiplier
            )
            return log_return / (scale + self.label_eps)
        if self.label_mode == "state":
            return pd.Series(
                np.where(
                    raw_return > self.label_up_threshold,
                    2,
                    np.where(raw_return < self.label_down_threshold, 0, 1),
                ),
                index=raw_return.index,
                dtype="float64",
            )
        raise ValueError(
            "label_mode 仅支持 raw_return / standardized_return / return_quantile / quantile_regression / log_return / log_return_zscore / state"
        )

    def _load_raw_fields(
        self,
        data_api,
        instrument: str,
        start_time,
        end_time,
        provider_uri: str | None = None,
    ) -> pd.DataFrame:
        if provider_uri:
            return self._load_raw_fields_from_provider_uri(
                instrument, start_time, end_time, provider_uri
            )
        return data_api.features(
            [instrument],
            [f"${field}" for field in RAW_FIELDS],
            start_time=start_time,
            end_time=end_time,
            freq=self.freq,
        )

    def _load_raw_fields_from_provider_uri(
        self, instrument: str, start_time, end_time, provider_uri: str
    ) -> pd.DataFrame:
        provider_path = pd.io.common.stringify_path(provider_uri)
        calendar_path = pd.io.common.stringify_path(
            f"{provider_path}/calendars/{self.freq}.txt"
        )
        calendar = pd.to_datetime(pd.read_csv(calendar_path, header=None).iloc[:, 0])
        start_ts = (
            pd.Timestamp(start_time) if start_time is not None else calendar.iloc[0]
        )
        end_ts = pd.Timestamp(end_time) if end_time is not None else calendar.iloc[-1]

        fields = {}
        missing = []
        for field in RAW_FIELDS:
            storage = FileFeatureStorage(
                instrument.lower(),
                field,
                self.freq,
                provider_uri={self.freq: provider_uri},
            )
            ser = storage.data
            if ser.empty:
                missing.append(field)
                continue
            idx = ser.index.to_numpy(dtype=int)
            valid = (idx >= 0) & (idx < len(calendar))
            aligned = pd.Series(
                ser.to_numpy(dtype=float)[valid],
                index=calendar.iloc[idx[valid]],
                name=field,
            )
            fields[field] = aligned

        if missing:
            raise ValueError(
                f"{provider_uri} 中 {instrument} 缺少 {self.freq} 字段 {missing}。"
                "请确认 IC provider 是分钟频，并包含 open/high/low/close/volume/vwap/total_turnover。"
            )
        out = pd.concat(fields.values(), axis=1).sort_index()
        return out.loc[(out.index >= start_ts) & (out.index <= end_ts)]


def _default_zero_as_nan_cols(
    selected_factors: list[str] | None,
    target_prefix: str = "IF",
    context_prefix: str = "IC",
    context_prefixes: list[str] | None = None,
) -> list[str]:
    factor_names = (
        MinuteFactorLibrary.list_factor_names()
        if not selected_factors
        else list(selected_factors)
    )
    prefixes = [target_prefix] + (
        list(context_prefixes) if context_prefixes is not None else [context_prefix]
    )
    cols: list[str] = []
    for raw_name in ("vwap", "total_turnover", "volume"):
        if raw_name in factor_names:
            cols.extend([f"{prefix}_{raw_name}" for prefix in prefixes])
    return cols


def build_cross_instrument_dataset(
    start_time: str,
    end_time: str,
    train_end: str,
    valid_end: str,
    valid_start: str | None = None,
    test_start: str | None = None,
    selected_factors: list[str] | None = None,
    selected_cross_features: list[str] | None = None,
    instruments: list[str] | None = None,
    target_instrument: str = "IF",
    context_instrument: str = "IC",
    context_instruments: list[str] | None = None,
    target_provider_uri: str | None = None,
    context_provider_uri: str | None = None,
    context_provider_uris: list[str | None] | None = None,
    label_expr: str | None = None,
    label_mode: str = "raw_return",
    label_horizon: int = 5,
    label_price_field: str = "$vwap",
    label_base_lag: int = 1,
    label_vol_window: int = 60,
    label_sqrt_horizon: bool = True,
    label_up_threshold: float = 0.0002,
    label_down_threshold: float = -0.0002,
    label_eps: float = 1e-12,
    use_robust_preprocess: bool = True,
    fillna_value: float = 0.0,
    use_process_inf: bool = False,
    zero_as_nan_cols: list[str] | None = None,
    label_clip_abs: float | None = 0.2,
) -> DatasetH:
    if label_expr is not None:
        print(
            "label_expr is ignored by build_cross_instrument_dataset; use label_mode/label_horizon instead."
        )

    target_prefix = target_instrument
    context_instruments_list = _as_list(
        context_instruments, fallback=[context_instrument]
    )
    context_provider_uris_list = _as_list(
        context_provider_uris,
        fallback=[context_provider_uri] * len(context_instruments_list),
    )
    if len(context_provider_uris_list) < len(context_instruments_list):
        context_provider_uris_list.extend(
            [None] * (len(context_instruments_list) - len(context_provider_uris_list))
        )
    context_prefix = (
        context_instruments_list[0] if context_instruments_list else context_instrument
    )
    context_prefixes = context_instruments_list
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
                    target_prefix=target_prefix,
                    context_prefix=context_prefix,
                    context_prefixes=context_prefixes,
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

    learn_processors = make_label_learn_processors(
        label_clip_abs=label_clip_abs,
        label_mode=label_mode,
        fields_group="label",
    )

    handler_config = {
        "class": "DataHandlerLP",
        "module_path": "qlib.data.dataset.handler",
        "kwargs": {
            "instruments": instruments
            if instruments is not None
            else [target_instrument],
            "start_time": start_time,
            "end_time": end_time,
            "infer_processors": infer_processors,
            "learn_processors": learn_processors,
            "data_loader": {
                "class": "CrossInstrumentFeatureLoader",
                "module_path": "utils.cross_instrument_dataset",
                "kwargs": {
                    "target_instrument": target_instrument,
                    "context_instrument": context_instrument,
                    "context_instruments": context_instruments_list,
                    "target_provider_uri": target_provider_uri,
                    "context_provider_uri": context_provider_uri,
                    "context_provider_uris": context_provider_uris_list,
                    "selected_factors": selected_factors,
                    "selected_cross_features": selected_cross_features,
                    "label_mode": label_mode,
                    "label_horizon": label_horizon,
                    "label_base_lag": label_base_lag,
                    "label_vol_window": label_vol_window,
                    "label_sqrt_horizon": label_sqrt_horizon,
                    "label_up_threshold": label_up_threshold,
                    "label_down_threshold": label_down_threshold,
                    "label_eps": label_eps,
                    "label_price_field": label_price_field,
                    "target_prefix": target_prefix,
                    "context_prefix": context_prefix,
                    "context_prefixes": context_prefixes,
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
