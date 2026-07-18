from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Mapping

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from qlib.contrib.model.gbdt import LGBModel
from qlib.data.dataset import DatasetH
from qlib.data.dataset.handler import DataHandlerLP


DEFAULT_LGBM_PARAMS: dict[str, Any] = {
    "loss": "mse",
    "colsample_bytree": 0.9,
    "subsample": 0.9,
    "learning_rate": 0.05,
    "max_depth": 6,
    "num_leaves": 31,
    "lambda_l1": 0.5,
    "lambda_l2": 0.5,
    "num_threads": 4,
}

DEFAULT_STATE_LGBM_PARAMS: dict[str, Any] = {
    "objective": "multiclass",
    "metric": "multi_logloss",
    "num_class": 3,
    "learning_rate": 0.03,
    "max_depth": 6,
    "num_leaves": 31,
    "colsample_bytree": 0.9,
    "subsample": 0.9,
    "lambda_l1": 0.5,
    "lambda_l2": 0.5,
    "num_threads": 4,
    "verbosity": -1,
    "num_boost_round": 1000,
    "early_stopping_rounds": 50,
    "verbose_eval": 20,
    "state_class_weight_mode": "sqrt_balanced",
    "state_class_weight_clip": (0.5, 3.0),
}


def build_model(model_params: Mapping[str, Any] | None = None) -> LGBModel:
    """构建 LGBM 回归模型（支持外部参数覆盖默认配置）。"""
    final_params = dict(DEFAULT_LGBM_PARAMS)
    if model_params:
        final_params.update(dict(model_params))
    return LGBModel(**final_params)


def train_and_predict(dataset: DatasetH, model_params: Mapping[str, Any] | None = None):
    """训练回归模型，并在 test 段生成预测值。"""
    model = build_model(model_params=model_params)
    model.fit(dataset)
    pred = model.predict(dataset, segment="test")
    return model, pred


def _pred_to_series(pred_obj, name: str = "pred") -> pd.Series:
    if isinstance(pred_obj, pd.DataFrame):
        if pred_obj.shape[1] == 0:
            return pd.Series(dtype=float, name=name)
        ser = pred_obj.iloc[:, 0]
    else:
        ser = pred_obj
    return pd.to_numeric(pd.Series(ser), errors="coerce").rename(name)


def _label_series(dataset: DatasetH, segment: str, name: str = "label") -> pd.Series:
    label_df = dataset.prepare(segment, col_set="label", data_key=DataHandlerLP.DK_L)
    if label_df.empty:
        return pd.Series(dtype=float, name=name)
    if isinstance(
        label_df.columns, pd.MultiIndex
    ) and "label" in label_df.columns.get_level_values(0):
        label_df = label_df["label"]
    return (
        pd.to_numeric(label_df.iloc[:, 0], errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .rename(name)
    )


def _align_true_pred(y_true: pd.Series, y_pred: pd.Series) -> pd.DataFrame:
    return (
        pd.concat([y_true.rename("y_true"), y_pred.rename("y_pred")], axis=1)
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )


def state_from_return(ret, threshold: float = 0.0002) -> np.ndarray:
    """把原始收益率映射为 0=down, 1=flat, 2=up。"""
    arr = np.asarray(ret, dtype=float)
    out = np.ones(arr.shape, dtype=int)
    out[arr > float(threshold)] = 2
    out[arr < -float(threshold)] = 0
    return out


def _looks_like_state_label(label) -> bool:
    arr = np.asarray(label, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return False
    rounded = np.round(arr)
    return bool(
        np.all(np.isclose(arr, rounded))
        and set(np.unique(rounded).astype(int)).issubset({0, 1, 2})
    )


def state_target_from_label(label, threshold: float = 0.0002) -> np.ndarray:
    if _looks_like_state_label(label):
        return np.asarray(np.round(label), dtype=int).clip(0, 2)
    return state_from_return(label, threshold)


def calc_state_metrics(
    y_true_state,
    y_pred_state,
    *,
    class_names: Mapping[int, str] | None = None,
    prefix: str = "",
) -> dict[str, float | int]:
    """计算三状态分类指标。"""
    class_names = dict(class_names or {0: "down", 1: "flat", 2: "up"})
    y_true_state = np.asarray(y_true_state, dtype=int)
    y_pred_state = np.asarray(y_pred_state, dtype=int)
    n = int(len(y_true_state))
    if n == 0:
        return {prefix + "n": 0}

    out: dict[str, float | int] = {
        prefix + "n": n,
        prefix + "accuracy": float((y_true_state == y_pred_state).mean()),
        prefix + "pred_trade_ratio": float((y_pred_state != 1).mean()),
        prefix + "true_trade_ratio": float((y_true_state != 1).mean()),
    }
    true_trade_mask = y_true_state != 1
    pred_trade_mask = y_pred_state != 1
    out[prefix + "trend_accuracy_true_nonflat"] = (
        float((y_true_state[true_trade_mask] == y_pred_state[true_trade_mask]).mean())
        if true_trade_mask.any()
        else np.nan
    )
    out[prefix + "trade_precision_pred_nonflat"] = (
        float((y_true_state[pred_trade_mask] == y_pred_state[pred_trade_mask]).mean())
        if pred_trade_mask.any()
        else np.nan
    )

    recalls = []
    precisions = []
    f1s = []
    for class_id, class_name in class_names.items():
        true_mask = y_true_state == int(class_id)
        pred_mask = y_pred_state == int(class_id)
        tp = int((true_mask & pred_mask).sum())
        support = int(true_mask.sum())
        pred_count = int(pred_mask.sum())
        recall = tp / support if support else np.nan
        precision = tp / pred_count if pred_count else np.nan
        f1 = (
            2 * precision * recall / (precision + recall)
            if pd.notna(precision) and pd.notna(recall) and (precision + recall) > 0
            else np.nan
        )
        out[prefix + f"{class_name}_support"] = support
        out[prefix + f"{class_name}_pred_count"] = pred_count
        out[prefix + f"{class_name}_recall"] = (
            float(recall) if pd.notna(recall) else np.nan
        )
        out[prefix + f"{class_name}_precision"] = (
            float(precision) if pd.notna(precision) else np.nan
        )
        out[prefix + f"{class_name}_f1"] = float(f1) if pd.notna(f1) else np.nan
        if pd.notna(recall):
            recalls.append(recall)
        if pd.notna(precision):
            precisions.append(precision)
        if pd.notna(f1):
            f1s.append(f1)
    out[prefix + "balanced_accuracy"] = float(np.mean(recalls)) if recalls else np.nan
    out[prefix + "macro_precision"] = (
        float(np.mean(precisions)) if precisions else np.nan
    )
    out[prefix + "macro_f1"] = float(np.mean(f1s)) if f1s else np.nan
    return out


def calc_raw_return_metrics(
    raw_true: pd.Series,
    raw_pred: pd.Series,
    *,
    state_threshold: float = 0.0002,
    class_names: Mapping[int, str] | None = None,
) -> dict[str, float | int]:
    """原始收益率口径下的回归指标和三状态派生指标。"""
    aligned = _align_true_pred(raw_true, raw_pred)
    if aligned.empty:
        return {"n": 0}
    y_true = aligned["y_true"]
    y_pred = aligned["y_pred"]
    out: dict[str, float | int] = {
        "n": int(len(aligned)),
        "mse": float(np.mean((y_pred - y_true) ** 2)),
        "mae": float(np.mean(np.abs(y_pred - y_true))),
        "ic": float(y_pred.corr(y_true, method="pearson")),
        "rank_ic": float(y_pred.corr(y_true, method="spearman")),
        "direction_acc_all": float((np.sign(y_pred) == np.sign(y_true)).mean()),
        "pred_abs_mean": float(y_pred.abs().mean()),
        "true_abs_mean": float(y_true.abs().mean()),
    }
    out.update(
        calc_state_metrics(
            state_from_return(y_true, state_threshold),
            state_from_return(y_pred, state_threshold),
            class_names=class_names,
            prefix="state_",
        )
    )
    return out


def _build_label_dataset(
    dataset_builder,
    dataset_kwargs: Mapping[str, Any],
    label_expr: str,
    *,
    feature_placeholder: bool,
):
    kwargs = dict(dataset_kwargs)
    kwargs["label_expr"] = label_expr
    kwargs["label_clip_abs"] = None
    if feature_placeholder:
        from factor_library.futures.minute_factors import build_dataset

        kwargs["selected_factors"] = ["returns_1min"]
        kwargs["use_robust_preprocess"] = False
        kwargs.pop("target_instrument", None)
        kwargs.pop("context_instrument", None)
        kwargs.pop("context_instruments", None)
        kwargs.pop("target_provider_uri", None)
        kwargs.pop("context_provider_uri", None)
        kwargs.pop("context_provider_uris", None)
        kwargs.pop("context_prefixes", None)
        kwargs.pop("selected_cross_features", None)
        return build_dataset(**kwargs)
    return dataset_builder(**kwargs)


def _prepare_feature_label(
    dataset: DatasetH, segment: str
) -> tuple[pd.DataFrame, pd.Series]:
    df = dataset.prepare(
        segment, col_set=["feature", "label"], data_key=DataHandlerLP.DK_L
    )
    if df.empty:
        raise ValueError(f"{segment} segment 数据为空")
    x = df["feature"]
    y = df["label"]
    if y.values.ndim == 2 and y.values.shape[1] == 1:
        y_ser = pd.Series(np.squeeze(y.values), index=df.index, name="label")
    else:
        raise ValueError("LightGBM 当前只支持单标签训练")
    return x, pd.to_numeric(y_ser, errors="coerce")


def _prepare_feature(dataset: DatasetH, segment: str) -> pd.DataFrame:
    return dataset.prepare(segment, col_set="feature", data_key=DataHandlerLP.DK_I)


def _resolve_regression_params(model_params: Mapping[str, Any] | None = None):
    params = dict(DEFAULT_LGBM_PARAMS)
    if model_params:
        params.update(dict(model_params))

    loss = str(params.pop("loss", params.pop("objective", "mse"))).lower()
    loss_params = dict(params.pop("loss_params", {}) or {})
    num_boost_round = int(params.pop("num_boost_round", 1000))
    early_stopping_rounds = int(params.pop("early_stopping_rounds", 50))
    verbose_eval = int(params.pop("verbose_eval", 20))

    if loss in {"mse", "l2", "regression", "regression_l2"}:
        params["objective"] = "regression_l2"
        params.setdefault("metric", "l2")
    elif loss in {"huber"}:
        params["objective"] = "huber"
        params.setdefault("metric", "l1")
        params.update(loss_params)
    else:
        raise ValueError("LGBM 回归 loss 仅支持 mse / huber")
    params.setdefault("verbosity", -1)
    return params, num_boost_round, early_stopping_rounds, verbose_eval


def train_lgbm_regression(
    dataset: DatasetH,
    *,
    model_params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """训练 LGBM 回归模型，支持 mse / huber。"""
    params, num_boost_round, early_stopping_rounds, verbose_eval = (
        _resolve_regression_params(model_params)
    )
    x_train, y_train = _prepare_feature_label(dataset, "train")
    x_valid, y_valid = _prepare_feature_label(dataset, "valid")

    train_set = lgb.Dataset(x_train.values, label=y_train.values, free_raw_data=False)
    valid_set = lgb.Dataset(
        x_valid.values, label=y_valid.values, reference=train_set, free_raw_data=False
    )
    evals_result: dict[str, dict[str, list[float]]] = {}
    callbacks = [
        lgb.early_stopping(early_stopping_rounds),
        lgb.log_evaluation(period=verbose_eval),
        lgb.record_evaluation(evals_result),
    ]
    model = lgb.train(
        params,
        train_set,
        num_boost_round=num_boost_round,
        valid_sets=[train_set, valid_set],
        valid_names=["train", "valid"],
        callbacks=callbacks,
    )
    metric_name = next(iter(evals_result.get("valid", {"metric": []})))
    valid_scores = evals_result.get("valid", {}).get(metric_name, [])
    best_score = (
        float(valid_scores[model.best_iteration - 1])
        if valid_scores and model.best_iteration
        else np.nan
    )
    return {
        "model": model,
        "evals_result": evals_result,
        "best_epoch": int(model.best_iteration or 0),
        "best_score": best_score,
    }


def predict_lgbm_regression(
    model: lgb.Booster, dataset: DatasetH, *, segment: str = "test"
) -> pd.Series:
    x = _prepare_feature(dataset, segment)
    return pd.Series(
        model.predict(x.values, num_iteration=model.best_iteration),
        index=x.index,
        name="score",
    )


def _build_state_class_weights(
    class_counts: np.ndarray,
    *,
    mode: str = "balanced",
    clip: float | tuple[float, float] | list[float] | None = None,
) -> np.ndarray | None:
    mode = str(mode or "balanced").lower()
    if mode in ("none", "off", "false", "0"):
        return None

    counts = np.asarray(class_counts, dtype=float)
    if mode == "balanced":
        weights = counts.sum() / np.maximum(counts, 1.0)
    elif mode in ("sqrt_balanced", "sqrt"):
        weights = np.sqrt(counts.sum() / np.maximum(counts, 1.0))
    else:
        raise ValueError(
            "state_class_weight_mode 仅支持 balanced / sqrt_balanced / none"
        )

    weights = weights / np.maximum(weights.mean(), 1e-12)
    if clip is not None:
        if isinstance(clip, (tuple, list)):
            lo, hi = float(clip[0]), float(clip[1])
        else:
            lo, hi = 1.0 / float(clip), float(clip)
        weights = np.clip(weights, lo, hi)
    return weights.astype(float)


def train_lgbm_state_classifier(
    dataset: DatasetH,
    *,
    state_return_threshold: float = 0.0002,
    model_params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """训练三状态 LightGBM 分类器。"""
    params = dict(DEFAULT_STATE_LGBM_PARAMS)
    if model_params:
        params.update(dict(model_params))

    num_boost_round = int(params.pop("num_boost_round", 1000))
    early_stopping_rounds = int(params.pop("early_stopping_rounds", 50))
    verbose_eval = int(params.pop("verbose_eval", 20))
    class_weight_mode = params.pop("state_class_weight_mode", "sqrt_balanced")
    class_weight_clip = params.pop("state_class_weight_clip", (0.5, 3.0))

    x_train, raw_y_train = _prepare_feature_label(dataset, "train")
    x_valid, raw_y_valid = _prepare_feature_label(dataset, "valid")
    y_train = pd.Series(
        state_target_from_label(raw_y_train.values, state_return_threshold),
        index=raw_y_train.index,
    )
    y_valid = pd.Series(
        state_target_from_label(raw_y_valid.values, state_return_threshold),
        index=raw_y_valid.index,
    )

    class_counts = np.bincount(y_train.values.astype(int), minlength=3).astype(float)
    class_weights = _build_state_class_weights(
        class_counts,
        mode=str(class_weight_mode),
        clip=class_weight_clip,
    )
    sample_weight = (
        None if class_weights is None else class_weights[y_train.values.astype(int)]
    )

    train_set = lgb.Dataset(
        x_train.values, label=y_train.values, weight=sample_weight, free_raw_data=False
    )
    valid_set = lgb.Dataset(
        x_valid.values, label=y_valid.values, reference=train_set, free_raw_data=False
    )
    evals_result: dict[str, dict[str, list[float]]] = {}
    callbacks = [
        lgb.early_stopping(early_stopping_rounds),
        lgb.log_evaluation(period=verbose_eval),
        lgb.record_evaluation(evals_result),
    ]
    model = lgb.train(
        params,
        train_set,
        num_boost_round=num_boost_round,
        valid_sets=[train_set, valid_set],
        valid_names=["train", "valid"],
        callbacks=callbacks,
    )
    metric_name = next(iter(evals_result.get("valid", {"multi_logloss": []})))
    valid_scores = evals_result.get("valid", {}).get(metric_name, [])
    best_score = (
        float(valid_scores[model.best_iteration - 1])
        if valid_scores and model.best_iteration
        else np.nan
    )
    return {
        "model": model,
        "evals_result": evals_result,
        "best_epoch": int(model.best_iteration or 0),
        "best_score": best_score,
        "class_counts": class_counts,
        "class_weights": class_weights,
    }


def predict_lgbm_state_classifier(
    model: lgb.Booster,
    dataset: DatasetH,
    *,
    segment: str = "test",
    state_return_threshold: float = 0.0002,
) -> tuple[pd.DataFrame, pd.Series, pd.Series, pd.Series]:
    """预测三状态分类概率和类别。"""
    x = _prepare_feature(dataset, segment)
    prob_arr = np.asarray(model.predict(x.values, num_iteration=model.best_iteration))
    if prob_arr.ndim == 1:
        prob_arr = np.column_stack([1.0 - prob_arr, np.zeros_like(prob_arr), prob_arr])
    prob_df = pd.DataFrame(
        prob_arr, index=x.index, columns=["prob_down", "prob_flat", "prob_up"]
    )
    pred_state = pd.Series(prob_arr.argmax(axis=1), index=x.index, name="pred_state")

    raw_label = _label_series(dataset, segment, name="raw_return")
    aligned_idx = prob_df.index.intersection(raw_label.dropna().index)
    prob_df = prob_df.loc[aligned_idx]
    pred_state = pred_state.loc[aligned_idx]
    raw_label = raw_label.loc[aligned_idx]
    true_state = pd.Series(
        state_target_from_label(raw_label.values, state_return_threshold),
        index=aligned_idx,
        name="true_state",
    )
    return prob_df, pred_state, true_state, raw_label


def train_predict_return_and_state_lgbm(
    *,
    base_dataset: DatasetH,
    dataset_builder,
    dataset_kwargs: Mapping[str, Any],
    raw_label_expr: str | None = None,
    label_scale_expr: str | None = None,
    model_task: str = "regression",
    label_is_standardized: bool = False,
    model_params: Mapping[str, Any] | None = None,
    state_model_params: Mapping[str, Any] | None = None,
    state_return_threshold: float = 0.0002,
    state_class_names: Mapping[int, str] | None = None,
    artifact_dir: str | Path | None = "artifacts",
    artifact_prefix: str = "lgbm",
) -> dict[str, Any]:
    """按参数训练收益回归模型、三状态分类模型，或二者都训练。"""
    model_task = (model_task or "regression").lower()
    if model_task not in {"regression", "state", "both"}:
        raise ValueError("model_task 仅支持 regression / state / both")
    run_regression = model_task in {"regression", "both"}
    run_state = model_task in {"state", "both"}
    if label_is_standardized and run_regression and not label_scale_expr:
        raise ValueError("label_is_standardized=True 时必须传入 label_scale_expr")
    if (label_is_standardized and run_regression or run_state) and not raw_label_expr:
        raise ValueError("标准化还原或三状态分类需要传入 raw_label_expr")

    total_start = time.perf_counter()
    artifact_path = Path(artifact_dir) if artifact_dir is not None else None
    if artifact_path is not None:
        artifact_path.mkdir(parents=True, exist_ok=True)

    regression_result = None
    raw_return_dataset = None
    label_scale_dataset = None
    pred_raw_by_segment: dict[str, pd.Series] = {}
    regression_metrics_rows = []

    if run_regression:
        regression_result = train_lgbm_regression(
            base_dataset, model_params=model_params
        )
        model = regression_result["model"]
        regression_result = {
            "model": model,
            "evals_result": regression_result["evals_result"],
            "best_epoch": regression_result["best_epoch"],
            "best_score": regression_result["best_score"],
            "pred_train": predict_lgbm_regression(model, base_dataset, segment="train"),
            "pred_valid": predict_lgbm_regression(model, base_dataset, segment="valid"),
            "pred": predict_lgbm_regression(model, base_dataset, segment="test"),
        }

        if label_is_standardized:
            raw_return_dataset = _build_label_dataset(
                dataset_builder,
                dataset_kwargs,
                raw_label_expr,
                feature_placeholder=True,
            )
            label_scale_dataset = _build_label_dataset(
                dataset_builder,
                dataset_kwargs,
                label_scale_expr,
                feature_placeholder=True,
            )
        for segment, pred_obj in (
            ("train", regression_result["pred_train"]),
            ("valid", regression_result["pred_valid"]),
            ("test", regression_result["pred"]),
        ):
            pred_ser = _pred_to_series(pred_obj, name="pred")
            if label_is_standardized:
                scale_ser = _label_series(label_scale_dataset, segment, name="scale")
                raw_true_ser = _label_series(
                    raw_return_dataset, segment, name="raw_return"
                )
                pred_eval_ser = (pred_ser * scale_ser).rename("pred_raw_return")
                metric_model_name = "regression_restore_raw"
            else:
                raw_true_ser = _label_series(base_dataset, segment, name="raw_return")
                pred_eval_ser = pred_ser.rename("pred_raw_return")
                metric_model_name = "regression_raw"
            pred_raw_by_segment[segment] = pred_eval_ser
            row = {"model": metric_model_name, "segment": segment}
            row.update(
                calc_raw_return_metrics(
                    raw_true_ser,
                    pred_eval_ser,
                    state_threshold=state_return_threshold,
                    class_names=state_class_names,
                )
            )
            regression_metrics_rows.append(row)

    state_result = None
    state_metrics_rows = []
    state_pred_by_segment = {}
    state_prob_by_segment = {}
    state_true_by_segment = {}
    if run_state:
        state_dataset = _build_label_dataset(
            dataset_builder, dataset_kwargs, raw_label_expr, feature_placeholder=False
        )
        state_result = train_lgbm_state_classifier(
            state_dataset,
            state_return_threshold=state_return_threshold,
            model_params=state_model_params,
        )

        for segment in ("train", "valid", "test"):
            prob_df, pred_state_ser, true_state_ser, raw_label_ser = (
                predict_lgbm_state_classifier(
                    state_result["model"],
                    state_dataset,
                    segment=segment,
                    state_return_threshold=state_return_threshold,
                )
            )
            state_prob_by_segment[segment] = prob_df
            state_pred_by_segment[segment] = pred_state_ser
            state_true_by_segment[segment] = true_state_ser
            row = {"model": "state_classifier", "segment": segment}
            row.update(
                calc_state_metrics(
                    true_state_ser.values,
                    pred_state_ser.values,
                    class_names=state_class_names,
                    prefix="state_",
                )
            )
            state_metrics_rows.append(row)

    regression_metrics_df = pd.DataFrame(regression_metrics_rows)
    state_metrics_df = pd.DataFrame(state_metrics_rows)
    model_compare_metrics_df = pd.concat(
        [regression_metrics_df, state_metrics_df], ignore_index=True, sort=False
    )

    pred_train = pred_raw_by_segment["train"].to_frame(0) if run_regression else None
    pred_valid = pred_raw_by_segment["valid"].to_frame(0) if run_regression else None
    pred = pred_raw_by_segment["test"].to_frame(0) if run_regression else None

    result = {
        "model_task": model_task,
        "label_is_standardized": bool(label_is_standardized),
        "regression_result": regression_result,
        "state_result": state_result,
        "model": regression_result["model"] if run_regression else None,
        "state_model": state_result["model"] if run_state else None,
        "evals_result": regression_result["evals_result"] if run_regression else None,
        "state_evals_result": state_result["evals_result"] if run_state else None,
        "best_epoch": regression_result["best_epoch"] if run_regression else None,
        "best_score": regression_result["best_score"] if run_regression else None,
        "state_best_epoch": state_result["best_epoch"] if run_state else None,
        "state_best_score": state_result["best_score"] if run_state else None,
        "state_train_class_counts": state_result["class_counts"] if run_state else None,
        "state_train_class_weights": state_result["class_weights"]
        if run_state
        else None,
        "pred_label_train": regression_result["pred_train"] if run_regression else None,
        "pred_label_valid": regression_result["pred_valid"] if run_regression else None,
        "pred_label": regression_result["pred"] if run_regression else None,
        "pred_std_train": regression_result["pred_train"]
        if run_regression and label_is_standardized
        else None,
        "pred_std_valid": regression_result["pred_valid"]
        if run_regression and label_is_standardized
        else None,
        "pred_std": regression_result["pred"]
        if run_regression and label_is_standardized
        else None,
        "pred_train": pred_train,
        "pred_valid": pred_valid,
        "pred": pred,
        "state_prob_by_segment": state_prob_by_segment,
        "state_pred_by_segment": state_pred_by_segment,
        "state_true_by_segment": state_true_by_segment,
        "regression_metrics_df": regression_metrics_df,
        "state_metrics_df": state_metrics_df,
        "model_compare_metrics_df": model_compare_metrics_df,
        "model_seconds": float(time.perf_counter() - total_start),
    }

    if artifact_path is not None:
        model_obj_path = artifact_path / f"{artifact_prefix}_regression_model.pkl"
        state_model_obj_path = artifact_path / f"{artifact_prefix}_state_model.pkl"
        pred_label_test_path = artifact_path / f"{artifact_prefix}_pred_test_label.pkl"
        pred_raw_test_path = (
            artifact_path / f"{artifact_prefix}_pred_test_raw_return.pkl"
        )
        pred_raw_test_csv = (
            artifact_path / f"{artifact_prefix}_pred_test_raw_return.csv"
        )
        state_prob_test_path = artifact_path / f"{artifact_prefix}_state_prob_test.pkl"
        state_pred_test_path = artifact_path / f"{artifact_prefix}_state_pred_test.pkl"
        metrics_path = artifact_path / f"{artifact_prefix}_return_vs_state_metrics.csv"
        if run_regression:
            try:
                joblib.dump(regression_result["model"], model_obj_path)
                result["model_obj_path"] = str(model_obj_path)
            except Exception as exc:
                result["model_obj_save_error"] = str(exc)
            regression_result["pred"].to_pickle(pred_label_test_path)
            pred.to_pickle(pred_raw_test_path)
            pred.to_csv(pred_raw_test_csv, encoding="utf-8-sig")
            result.update(
                {
                    "pred_label_test_path": str(pred_label_test_path),
                    "pred_raw_test_path": str(pred_raw_test_path),
                    "pred_raw_test_csv": str(pred_raw_test_csv),
                }
            )
        if run_state:
            try:
                joblib.dump(state_result["model"], state_model_obj_path)
                result["state_model_obj_path"] = str(state_model_obj_path)
            except Exception as exc:
                result["state_model_obj_save_error"] = str(exc)
            state_prob_by_segment["test"].to_pickle(state_prob_test_path)
            state_pred_by_segment["test"].to_pickle(state_pred_test_path)
            result.update(
                {
                    "state_prob_test_path": str(state_prob_test_path),
                    "state_pred_test_path": str(state_pred_test_path),
                }
            )
        model_compare_metrics_df.to_csv(metrics_path, index=False, encoding="utf-8-sig")
        result["metrics_path"] = str(metrics_path)

    return result
