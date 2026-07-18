from __future__ import annotations

import itertools
import gc
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

import numpy as np
import pandas as pd

from qlib.data.dataset import TSDatasetH

from engine.models.lgbm import (
    train_predict_return_and_state_lgbm,
)
from engine.models.alstm import (
    train_predict_return_and_state_alstm,
)
from engine.models.distribution_nll import (
    train_predict_distribution_nll_alstm,
)
from engine.backtest.futures_backtest import (
    StrategyRuleConfig,
    run_backtest,
    shift_signal_for_execution,
)


@dataclass
class ExpertConfig:
    """多专家模型配置。

    task:
        up/down/vol 使用二分类机会标签，trend/reversal/quantile/state 使用对应连续或状态标签。
    model_type:
        alstm、lgbm 或 distribution_nll。
    """

    name: str
    task: str
    model_type: str = "alstm"
    enabled: bool = True
    cost_threshold: float = 0.00015
    trend_lookback: int = 20
    reversal_lookback: int = 20
    quantiles: tuple[float, ...] = (0.1, 0.5, 0.9)
    selected_features: list[str] | None = None
    label_multiplier: float = 1.0
    model_params: dict[str, Any] = field(default_factory=dict)
    state_model_params: dict[str, Any] = field(default_factory=dict)


def expert_label_expr(
    task: str,
    *,
    future_return_expr: str,
    price_field: str = "$vwap",
    horizon: int = 5,
    base_lag: int = 1,
    cost_threshold: float = 0.00015,
    trend_lookback: int = 20,
    reversal_lookback: int = 20,
    eps: float = 1e-12,
) -> str:
    """返回专家模型用的 qlib label 表达式。"""
    task = str(task).lower()
    c = float(cost_threshold)
    if task == "up":
        return f"Greater({future_return_expr},{c})"
    if task == "down":
        return f"Greater(-({future_return_expr}),{c})"
    if task == "vol":
        return f"Greater(Abs({future_return_expr}),{c})"
    if task == "trend":
        past_ret = (
            f"({price_field}/(Ref({price_field},{int(trend_lookback)})+{float(eps)})-1)"
        )
        return f"Sign({past_ret})*({future_return_expr})"
    if task == "reversal":
        ma_expr = f"Mean({price_field},{int(reversal_lookback)})"
        std_expr = f"Std({price_field},{int(reversal_lookback)})"
        deviation = f"(({price_field})-({ma_expr}))/(({std_expr})+{float(eps)})"
        return f"-({deviation})*({future_return_expr})"
    if task in {"quantile", "regression", "state", "distribution"}:
        return future_return_expr
    raise ValueError(f"unknown expert task: {task}")


def _series_from_pred(pred_obj, name: str) -> pd.Series:
    if pred_obj is None:
        return pd.Series(dtype=float, name=name)
    if isinstance(pred_obj, pd.DataFrame):
        if pred_obj.shape[1] == 0:
            return pd.Series(dtype=float, name=name)
        ser = pred_obj.iloc[:, 0]
    else:
        ser = pred_obj
    return pd.to_numeric(pd.Series(ser), errors="coerce").rename(name)


def _binary_prob_signal(
    result: Mapping[str, Any], segment: str, positive_state: int = 2
) -> pd.Series:
    prob_by_segment = result.get("state_prob_by_segment") or {}
    pred_by_segment = result.get("state_pred_by_segment") or {}
    prob = prob_by_segment.get(segment)
    if isinstance(prob, pd.DataFrame) and not prob.empty:
        if positive_state == 2 and "prob_up" in prob.columns:
            return pd.to_numeric(prob["prob_up"], errors="coerce").rename("prob_up")
        if positive_state == 0 and "prob_down" in prob.columns:
            return pd.to_numeric(prob["prob_down"], errors="coerce").rename("prob_down")
        col = f"prob_{positive_state}"
        if col in prob.columns:
            return pd.to_numeric(prob[col], errors="coerce").rename(col)
    pred = pred_by_segment.get(segment)
    if pred is None:
        return pd.Series(dtype=float, name="prob")
    return (
        (pd.to_numeric(pred, errors="coerce") == int(positive_state))
        .astype(float)
        .rename("prob")
    )


def _state_direction_signal(result: Mapping[str, Any], segment: str) -> pd.Series:
    prob_by_segment = result.get("state_prob_by_segment") or {}
    pred_by_segment = result.get("state_pred_by_segment") or {}
    prob = prob_by_segment.get(segment)
    if isinstance(prob, pd.DataFrame) and {"prob_up", "prob_down"}.issubset(
        prob.columns
    ):
        return (
            pd.to_numeric(prob["prob_up"], errors="coerce")
            - pd.to_numeric(prob["prob_down"], errors="coerce")
        ).rename("state_dir")
    pred = pred_by_segment.get(segment)
    if pred is None:
        return pd.Series(dtype=float, name="state_dir")
    return (
        pd.to_numeric(pred, errors="coerce")
        .map({0: -1.0, 1: 0.0, 2: 1.0})
        .rename("state_dir")
    )


def _quantile_signal(result: Mapping[str, Any], segment: str) -> pd.Series:
    risk_adj = (result.get("pred_qreg_risk_adjusted_by_segment") or {}).get(segment)
    if risk_adj is not None:
        return pd.to_numeric(risk_adj, errors="coerce").rename("quantile_risk_adjusted")
    risk_key = {
        "train": "pred_qreg_risk_adjusted_train",
        "valid": "pred_qreg_risk_adjusted_valid",
        "test": "pred_qreg_risk_adjusted",
    }.get(segment)
    if risk_key and result.get(risk_key) is not None:
        return pd.to_numeric(result.get(risk_key), errors="coerce").rename(
            "quantile_risk_adjusted"
        )
    qreg = (result.get("pred_qreg_by_segment") or {}).get(segment)
    if isinstance(qreg, pd.DataFrame) and not qreg.empty:
        median_col = (
            "q50" if "q50" in qreg.columns else qreg.columns[len(qreg.columns) // 2]
        )
        return pd.to_numeric(qreg[median_col], errors="coerce").rename(
            "quantile_median"
        )
    qreg_key = {
        "train": "pred_qreg_train",
        "valid": "pred_qreg_valid",
        "test": "pred_qreg",
    }.get(segment)
    qreg = result.get(qreg_key) if qreg_key else None
    if isinstance(qreg, pd.DataFrame) and not qreg.empty:
        median_col = (
            "q50" if "q50" in qreg.columns else qreg.columns[len(qreg.columns) // 2]
        )
        return pd.to_numeric(qreg[median_col], errors="coerce").rename(
            "quantile_median"
        )
    raw = (result.get("pred_raw_by_segment") or {}).get(segment)
    return _series_from_pred(raw, "quantile_median")


def _distribution_signal(result: Mapping[str, Any], segment: str) -> pd.Series:
    signal = (result.get("signal_by_segment") or {}).get(segment)
    if signal is not None:
        return pd.to_numeric(signal, errors="coerce").rename("distribution_confidence")
    pred = (result.get("pred_distribution_by_segment") or {}).get(segment)
    if isinstance(pred, pd.DataFrame) and not pred.empty:
        conf_cols = [c for c in pred.columns if str(c).startswith("pred_confidence_")]
        if conf_cols:
            return pd.to_numeric(pred[conf_cols[0]], errors="coerce").rename(
                "distribution_confidence"
            )
    return pd.Series(dtype=float, name="distribution_confidence")


def _regression_signal(result: Mapping[str, Any], segment: str) -> pd.Series:
    raw = (result.get("pred_raw_by_segment") or {}).get(segment)
    if raw is not None:
        return pd.to_numeric(raw, errors="coerce").rename("regression")
    key = {"train": "pred_train", "valid": "pred_valid", "test": "pred"}.get(
        segment, "pred"
    )
    return _series_from_pred(result.get(key), "regression")


def train_expert_model(
    cfg: ExpertConfig,
    *,
    dataset_builder: Callable[..., Any],
    dataset_kwargs: Mapping[str, Any],
    label_expr_builder: Callable[[ExpertConfig], str],
    d_feat: int,
    step_len: int,
    raw_label_expr: str,
    state_return_threshold: float,
    state_class_names: Mapping[int, str] | None = None,
    artifact_dir: str | Path = "artifacts/if_model_ensemble",
    show_progress: bool = True,
    keep_model_results: bool | None = None,
) -> dict[str, Any]:
    """训练单个专家模型，并返回标准化之前的 train/valid/test alpha。"""
    if not cfg.enabled:
        return {"name": cfg.name, "enabled": False}
    model_type = str(cfg.model_type).lower()
    task = str(cfg.task).lower()
    label_expr = label_expr_builder(cfg)
    label_multiplier = float(getattr(cfg, "label_multiplier", 1.0) or 1.0)
    if label_multiplier != 1.0:
        label_expr = f"({label_multiplier})*({label_expr})"
    expert_kwargs = dict(dataset_kwargs)
    expert_kwargs["label_expr"] = label_expr
    if task in {"up", "down", "vol", "state"}:
        expert_kwargs["label_clip_abs"] = None
    elif model_type == "distribution_nll":
        expert_kwargs["label_clip_abs"] = cfg.model_params.get(
            "label_clip_abs", dataset_kwargs.get("label_clip_abs")
        )
    else:
        expert_kwargs["label_clip_abs"] = cfg.model_params.get(
            "label_clip_abs", dataset_kwargs.get("label_clip_abs")
        )
    if model_type == "distribution_nll":
        expert_kwargs["label_clip_abs"] = cfg.model_params.get(
            "label_clip_abs", expert_kwargs.get("label_clip_abs")
        )
    if cfg.selected_features:
        expert_kwargs["selected_factors"] = None
        expert_kwargs["selected_cross_features"] = list(cfg.selected_features)
    expert_d_feat = len(cfg.selected_features) if cfg.selected_features else int(d_feat)
    base_dataset = dataset_builder(**expert_kwargs)

    artifact_path = Path(artifact_dir) / cfg.name
    artifact_path.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    if task in {"up", "down", "vol", "state"}:
        model_task = "state"
        if model_type == "alstm":
            ts_dataset = TSDatasetH(
                handler=base_dataset.handler,
                segments=base_dataset.segments,
                step_len=int(step_len),
                freq=str(dataset_kwargs.get("freq", "1min")),
            )
            result = train_predict_return_and_state_alstm(
                ts_dataset=ts_dataset,
                base_dataset=base_dataset,
                dataset_builder=dataset_builder,
                dataset_kwargs=expert_kwargs,
                raw_label_expr=label_expr,
                d_feat=expert_d_feat,
                step_len=step_len,
                model_task=model_task,
                label_is_standardized=False,
                model_params=cfg.model_params,
                state_return_threshold=0.5
                if task in {"up", "down", "vol"}
                else state_return_threshold,
                state_class_names=state_class_names,
                state_model_save_path=artifact_path / f"{cfg.name}_state.pt",
                artifact_dir=artifact_path,
                artifact_prefix=cfg.name,
                show_progress=show_progress,
            )
        elif model_type == "lgbm":
            result = train_predict_return_and_state_lgbm(
                base_dataset=base_dataset,
                dataset_builder=dataset_builder,
                dataset_kwargs=expert_kwargs,
                raw_label_expr=label_expr,
                model_task=model_task,
                label_is_standardized=False,
                state_return_threshold=0.5
                if task in {"up", "down", "vol"}
                else state_return_threshold,
                state_class_names=state_class_names,
                state_model_params=cfg.state_model_params or cfg.model_params,
                artifact_dir=artifact_path,
                artifact_prefix=cfg.name,
            )
        elif model_type == "distribution_nll":
            ts_dataset = TSDatasetH(
                handler=base_dataset.handler,
                segments=base_dataset.segments,
                step_len=int(step_len),
                freq=str(dataset_kwargs.get("freq", "1min")),
            )
            result = train_predict_distribution_nll_alstm(
                ts_dataset=ts_dataset,
                d_feat=expert_d_feat,
                step_len=step_len,
                model_params=cfg.model_params,
                artifact_dir=artifact_path,
                artifact_prefix=cfg.name,
                show_progress=show_progress,
            )
        else:
            raise ValueError(
                f"{cfg.name}: model_type 仅支持 alstm / lgbm / distribution_nll"
            )
    else:
        model_task = "regression"
        regression_train_mode = (
            "quantile_regression" if task == "quantile" else "direct"
        )
        regression_label_mode = "raw_return"
        model_params = dict(cfg.model_params)
        if task == "quantile":
            model_params["quantile_regression_quantiles"] = tuple(cfg.quantiles)
        if model_type == "alstm":
            ts_dataset = TSDatasetH(
                handler=base_dataset.handler,
                segments=base_dataset.segments,
                step_len=int(step_len),
                freq=str(dataset_kwargs.get("freq", "1min")),
            )
            result = train_predict_return_and_state_alstm(
                ts_dataset=ts_dataset,
                base_dataset=base_dataset,
                dataset_builder=dataset_builder,
                dataset_kwargs=expert_kwargs,
                raw_label_expr=label_expr,
                d_feat=expert_d_feat,
                step_len=step_len,
                model_task=model_task,
                regression_label_mode=regression_label_mode,
                regression_train_mode=regression_train_mode,
                label_is_standardized=False,
                model_params=model_params,
                state_return_threshold=state_return_threshold,
                state_class_names=state_class_names,
                model_save_path=artifact_path / f"{cfg.name}_regression.pt",
                artifact_dir=artifact_path,
                artifact_prefix=cfg.name,
                show_progress=show_progress,
            )
        elif model_type == "lgbm":
            result = train_predict_return_and_state_lgbm(
                base_dataset=base_dataset,
                dataset_builder=dataset_builder,
                dataset_kwargs=expert_kwargs,
                raw_label_expr=label_expr,
                model_task=model_task,
                label_is_standardized=False,
                model_params=model_params,
                state_return_threshold=state_return_threshold,
                state_class_names=state_class_names,
                artifact_dir=artifact_path,
                artifact_prefix=cfg.name,
            )
        elif model_type == "distribution_nll":
            ts_dataset = TSDatasetH(
                handler=base_dataset.handler,
                segments=base_dataset.segments,
                step_len=int(step_len),
                freq=str(dataset_kwargs.get("freq", "1min")),
            )
            result = train_predict_distribution_nll_alstm(
                ts_dataset=ts_dataset,
                d_feat=expert_d_feat,
                step_len=step_len,
                model_params=model_params,
                artifact_dir=artifact_path,
                artifact_prefix=cfg.name,
                show_progress=show_progress,
            )
        else:
            raise ValueError(
                f"{cfg.name}: model_type 仅支持 alstm / lgbm / distribution_nll"
            )

    signal_by_segment: dict[str, pd.Series] = {}
    for segment in ("train", "valid", "test"):
        if task == "up":
            signal = _binary_prob_signal(result, segment, positive_state=2)
        elif task == "down":
            signal = -_binary_prob_signal(result, segment, positive_state=2)
        elif task == "vol":
            signal = _binary_prob_signal(result, segment, positive_state=2)
        elif task == "state":
            signal = _state_direction_signal(result, segment)
        elif task == "quantile":
            signal = _quantile_signal(result, segment)
        elif task == "distribution":
            signal = _distribution_signal(result, segment)
        else:
            signal = _regression_signal(result, segment)
        signal_by_segment[segment] = pd.to_numeric(signal, errors="coerce").rename(
            cfg.name
        )

    return {
        "name": cfg.name,
        "task": cfg.task,
        "model_type": cfg.model_type,
        "label_expr": label_expr,
        "model_result": result,
        "signal_by_segment": signal_by_segment,
        "seconds": time.perf_counter() - t0,
        "enabled": True,
    }


def train_expert_ensemble(
    configs: list[ExpertConfig],
    keep_model_results: bool = True,
    **kwargs,
) -> dict[str, Any]:
    results = []
    signal_parts = {"train": [], "valid": [], "test": []}
    for cfg in configs:
        if not cfg.enabled:
            continue
        result = train_expert_model(cfg, **kwargs)
        for segment in ("train", "valid", "test"):
            signal_parts[segment].append(result["signal_by_segment"][segment])
        if keep_model_results:
            results.append(result)
        else:
            results.append(
                {
                    "name": result["name"],
                    "task": result["task"],
                    "model_type": result["model_type"],
                    "label_expr": result["label_expr"],
                    "seconds": result["seconds"],
                    "enabled": result["enabled"],
                }
            )
            del result
            gc.collect()
    signals = {
        segment: pd.concat(signal_parts[segment], axis=1)
        for segment in ("train", "valid", "test")
    }
    return {"expert_results": results, "signals": signals}


def fit_signal_standardizer(
    train_signals: pd.DataFrame, eps: float = 1e-12
) -> tuple[pd.Series, pd.Series]:
    cleaned = train_signals.replace([np.inf, -np.inf], np.nan)
    mean = cleaned.mean(axis=0)
    std = cleaned.std(axis=0).replace(0.0, np.nan).fillna(1.0)
    return mean, std + float(eps)


def apply_signal_standardizer(
    signals: pd.DataFrame, mean: pd.Series, std: pd.Series
) -> pd.DataFrame:
    return (
        (signals.replace([np.inf, -np.inf], np.nan) - mean).div(std, axis=1).fillna(0.0)
    )


def combine_signals(
    signals: pd.DataFrame, weights: Mapping[str, float], *, scale: float = 1.0
) -> pd.Series:
    aligned_weights = (
        pd.Series(weights, dtype=float).reindex(signals.columns).fillna(0.0)
    )
    raw = signals.fillna(0.0).mul(aligned_weights, axis=1).sum(axis=1)
    return pd.Series(
        np.tanh(float(scale) * raw), index=signals.index, name="ensemble_signal"
    )


def _metric_from_report(
    report_df: pd.DataFrame, *, account: float, eps: float = 1e-12
) -> dict[str, float]:
    if report_df.empty or "account" not in report_df.columns:
        return {"score": -np.inf}
    curve = pd.to_numeric(report_df["account"], errors="coerce").dropna()
    ret = curve.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    if len(ret) > 1 and ret.std(ddof=1) > eps:
        sharpe = float(ret.mean() / (ret.std(ddof=1) + eps))
        annualized_sharpe = float(sharpe * np.sqrt(240 * 252))
    else:
        sharpe = np.nan
        annualized_sharpe = np.nan
    drawdown = curve / curve.cummax() - 1
    max_drawdown = float(drawdown.min()) if len(drawdown) else np.nan
    trade_count = (
        int((report_df["position_lots"].diff().abs().fillna(0) > 1e-12).sum())
        if "position_lots" in report_df.columns
        else 0
    )
    total_return = (
        float(curve.iloc[-1] / float(account) - 1.0) if len(curve) else np.nan
    )
    score = annualized_sharpe if np.isfinite(annualized_sharpe) else -np.inf
    return {
        "score": score,
        "sharpe_ratio": sharpe,
        "annualized_sharpe": annualized_sharpe,
        "max_drawdown": max_drawdown,
        "trade_count": trade_count,
        "total_return": total_return,
        "final_account": float(curve.iloc[-1]) if len(curve) else np.nan,
    }


def optimize_static_ensemble(
    valid_signals: pd.DataFrame,
    *,
    weight_grid: list[float],
    scale_grid: list[float],
    strategy_rule: StrategyRuleConfig,
    valid_start: str,
    valid_end: str,
    account: float,
    max_abs_weight_sum: float = 3.0,
    min_trade_count: int = 5,
    signal_execution_lag_bars: int = 1,
    top_n: int = 20,
) -> tuple[dict[str, Any], pd.DataFrame]:
    names = list(valid_signals.columns)
    rows = []
    best: dict[str, Any] | None = None
    for values in itertools.product(weight_grid, repeat=len(names)):
        weights = dict(zip(names, values))
        if sum(abs(v) for v in weights.values()) <= 1e-12:
            continue
        if sum(abs(v) for v in weights.values()) > float(max_abs_weight_sum):
            continue
        for scale in scale_grid:
            signal = combine_signals(valid_signals, weights, scale=scale)
            exec_signal = shift_signal_for_execution(
                signal, lag_bars=signal_execution_lag_bars
            )
            report, _analysis, indicator, _positions = run_backtest(
                pred=exec_signal,
                test_start=valid_start,
                end_time=valid_end,
                account=account,
                strategy_rule=strategy_rule,
            )
            metrics = _metric_from_report(report, account=account)
            if int(metrics.get("trade_count", 0)) < int(min_trade_count):
                metrics["score"] = -np.inf
            row = {
                "scale": scale,
                **{f"w_{k}": v for k, v in weights.items()},
                **metrics,
            }
            rows.append(row)
            if best is None or float(row["score"]) > float(best["score"]):
                best = dict(row)
                best["weights"] = weights
                best["scale"] = scale
                best["indicator"] = indicator
    result_df = pd.DataFrame(rows).replace([np.inf, -np.inf], np.nan)
    if not result_df.empty:
        result_df = (
            result_df.sort_values("score", ascending=False, na_position="last")
            .head(int(top_n))
            .reset_index(drop=True)
        )
    return best or {"score": -np.inf, "weights": {}, "scale": 1.0}, result_df


def backtest_ensemble_signal(
    signals: pd.DataFrame,
    *,
    weights: Mapping[str, float],
    scale: float,
    strategy_rule: StrategyRuleConfig,
    test_start: str,
    end_time: str,
    account: float,
    signal_execution_lag_bars: int = 1,
):
    raw_signal = combine_signals(signals, weights, scale=scale).rename(
        "ensemble_signal_raw"
    )
    exec_signal = shift_signal_for_execution(
        raw_signal, lag_bars=signal_execution_lag_bars
    ).rename("ensemble_signal")
    report, analysis, indicator, positions = run_backtest(
        pred=exec_signal,
        test_start=test_start,
        end_time=end_time,
        account=account,
        strategy_rule=strategy_rule,
    )
    metrics = _metric_from_report(report, account=account)
    return {
        "raw_signal": raw_signal,
        "exec_signal": exec_signal,
        "report": report,
        "analysis": analysis,
        "indicator": indicator,
        "positions": positions,
        "metrics": metrics,
    }
