from __future__ import annotations

import copy
import time
from pathlib import Path
from typing import Any, Mapping

import joblib
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from qlib.contrib.model.pytorch_alstm_ts import ALSTM, ConcatDataset, DataLoader
from qlib.data import D
from qlib.data.dataset import TSDatasetH
from qlib.data.dataset.handler import DataHandlerLP
from tqdm.auto import tqdm


DEFAULT_ALSTM_PARAMS: dict[str, Any] = {
    # d_feat 表示“单个时间步的特征维度”（不是 step_len）
    "d_feat": 20,
    "hidden_size": 64,
    "num_layers": 2,
    "dropout": 0.1,
    "n_epochs": 30,
    "lr": 1e-3,
    "metric": "loss",
    "batch_size": 800,
    "early_stop": 10,
    "loss": "mse",
    "loss_params": {},
    "optimizer": "adam",
    "GPU": -1,
    "seed": 42,
}


ALSTM_MODEL_CONTROL_KEYS = {
    "output_dim",
    "output_names",
    "quantile_regression_quantiles",
    "quantile_regression_median_quantile",
    "quantile_regression_risk_lower_quantile",
    "quantile_regression_risk_upper_quantile",
}


def build_alstm_model(model_params: Mapping[str, Any] | None = None) -> ALSTM:
    """
    构建 ALSTM 模型

    说明：
    - 时间窗口由外部的 TSDatasetH(step_len=...) 控制；
    - 本模块只负责模型参数和训练/预测流程。
    """
    final_params = dict(DEFAULT_ALSTM_PARAMS)
    if model_params:
        final_params.update(dict(model_params))

    d_feat = int(final_params.get("d_feat", 0))
    if d_feat <= 0:
        raise ValueError(f"`d_feat` 必须大于 0，当前值为 {d_feat}")

    init_params = {
        k: v for k, v in final_params.items() if k not in ALSTM_MODEL_CONTROL_KEYS
    }
    return ALSTM(**init_params)


def build_alstm_model_config(
    model_params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """返回可用于日志记录/配置管理的模型配置字典。"""
    final_params = dict(DEFAULT_ALSTM_PARAMS)
    if model_params:
        final_params.update(dict(model_params))
    return {
        "model": {
            "class": "ALSTM",
            "module_path": "qlib.contrib.model.pytorch_alstm_ts",
            "kwargs": final_params,
        }
    }


def _q_name(quantile: float) -> str:
    return f"q{int(round(float(quantile) * 100)):02d}"


def _optimizer_for_model(model: ALSTM):
    optimizer_name = str(getattr(model, "optimizer", "adam")).lower()
    lr = float(getattr(model, "lr", DEFAULT_ALSTM_PARAMS["lr"]))
    params = model.ALSTM_model.parameters()
    if optimizer_name == "adam":
        return torch.optim.Adam(params, lr=lr)
    if optimizer_name == "gd":
        return torch.optim.SGD(params, lr=lr)
    raise NotImplementedError(f"optimizer `{optimizer_name}` is not supported")


def _set_alstm_output_dim(
    model: ALSTM, output_dim: int, output_names: list[str] | None = None
) -> ALSTM:
    """把 qlib 单输出 ALSTM 改成多输出，并重建 optimizer 让新输出层参与训练。"""
    output_dim = int(output_dim)
    if output_dim <= 1:
        return model
    fc_out = getattr(model.ALSTM_model, "fc_out", None)
    if not isinstance(fc_out, nn.Linear):
        raise TypeError(
            "当前 ALSTM_model 不含可替换的 nn.Linear fc_out，无法启用多输出分位数回归"
        )
    new_fc_out = nn.Linear(fc_out.in_features, output_dim, bias=fc_out.bias is not None)
    new_fc_out = new_fc_out.to(model.device)
    model.ALSTM_model.fc_out = new_fc_out
    model.output_dim = output_dim
    model.output_names = list(output_names or [str(i) for i in range(output_dim)])
    model.train_optimizer = _optimizer_for_model(model)
    return model


def train_alstm_model(
    model: ALSTM,
    dataset,
    *,
    experiment_name: str = "train_alstm_ts",
    model_config: Mapping[str, Any] | None = None,
    save_model_as: str = "trained_model",
):
    """
    训练模型（数据集由外部传入），并记录实验。

    注意：此函数已弃用，建议使用 train_predict_alstm 代替。
    由于 qlib.workflow.R 可能导致导入问题，此函数不再使用 R 进行实验记录。

    Returns:
        model, None (recorder_id 已废弃)
    """
    model.fit(dataset)
    return model, None


def predict_alstm(model: ALSTM, dataset, segment: str = "test"):
    """在指定数据分段上做预测。"""
    return model.predict(dataset, segment=segment)


def get_cuda_gpu_id(default: int = 0) -> int:
    """返回 qlib ALSTM 可用的 CUDA GPU id；无 CUDA 时返回 -1。"""
    return int(default) if torch.cuda.is_available() else -1


def describe_torch_device(gpu_id: int) -> str:
    """返回当前训练设备描述，方便 notebook 打印。"""
    if gpu_id >= 0 and torch.cuda.is_available():
        return f"cuda:{gpu_id} ({torch.cuda.get_device_name(gpu_id)})"
    return "cpu"


def _scale_label_frame(data_obj, scale: float):
    """缩放 label；兼容 DataFrame 和 TSDatasetH.prepare 返回的 TSDataSampler。"""
    scale = float(scale)
    if abs(scale - 1.0) <= 1e-12:
        return data_obj

    # TSDatasetH.prepare 返回 TSDataSampler；qlib ALSTM 使用 data_arr 最后一列作为 label。
    if hasattr(data_obj, "data_arr"):
        data_obj.data_arr[:, -1] = data_obj.data_arr[:, -1] * scale
        return data_obj

    out = data_obj.copy()
    if isinstance(
        out.columns, pd.MultiIndex
    ) and "label" in out.columns.get_level_values(0):
        label_cols = [c for c in out.columns if c[0] == "label"]
        out.loc[:, label_cols] = out.loc[:, label_cols] * scale
    elif "LABEL0" in out.columns:
        out.loc[:, "LABEL0"] = out["LABEL0"] * scale
    else:
        out.iloc[:, -1] = out.iloc[:, -1] * scale
    return out


def unscale_prediction(pred_obj, label_scale: float):
    """把模型输出从缩放后的 label 口径还原到原始收益率口径。"""
    label_scale = float(label_scale)
    if abs(label_scale) <= 1e-12 or abs(label_scale - 1.0) <= 1e-12:
        return pred_obj
    return pred_obj / label_scale


def _set_label_values(data_obj, values: np.ndarray):
    """替换 DataFrame/TSDataSampler 中的 label 值，保留特征和索引。"""
    values = np.asarray(values, dtype=float)
    if hasattr(data_obj, "data_arr"):
        out = copy.copy(data_obj)
        out.data_arr = np.array(data_obj.data_arr, copy=True)
        out.data_arr[:, -1] = values.reshape(-1)
        return out

    out = data_obj.copy()
    if isinstance(
        out.columns, pd.MultiIndex
    ) and "label" in out.columns.get_level_values(0):
        label_cols = [c for c in out.columns if c[0] == "label"]
        if len(label_cols) != 1:
            raise ValueError("收益率分位数标签当前仅支持单列 label")
        out.loc[:, label_cols[0]] = values.reshape(-1)
    elif "LABEL0" in out.columns:
        out.loc[:, "LABEL0"] = values.reshape(-1)
    else:
        out.iloc[:, -1] = values.reshape(-1)
    return out


def _get_label_values(data_obj) -> np.ndarray:
    """读取 DataFrame/TSDataSampler 中的单列 label 值。"""
    if hasattr(data_obj, "data_arr"):
        return np.asarray(data_obj.data_arr[:, -1], dtype=float)
    if isinstance(
        data_obj.columns, pd.MultiIndex
    ) and "label" in data_obj.columns.get_level_values(0):
        label_cols = [c for c in data_obj.columns if c[0] == "label"]
        if len(label_cols) != 1:
            raise ValueError("收益率分位数标签当前仅支持单列 label")
        return pd.to_numeric(data_obj.loc[:, label_cols[0]], errors="coerce").to_numpy(
            dtype=float
        )
    if "LABEL0" in data_obj.columns:
        return pd.to_numeric(data_obj["LABEL0"], errors="coerce").to_numpy(dtype=float)
    return pd.to_numeric(data_obj.iloc[:, -1], errors="coerce").to_numpy(dtype=float)


def _fit_quantile_reference(dataset_2d, segment: str = "train") -> np.ndarray:
    """只用训练段原始收益率拟合经验分布，避免 valid/test 信息泄露。"""
    y = _label_series(dataset_2d, segment, name="raw_return")
    ref = (
        y.replace([np.inf, -np.inf], np.nan)
        .dropna()
        .sort_values()
        .to_numpy(dtype=float)
    )
    if ref.size == 0:
        raise ValueError("收益率分位数标签无法拟合：训练集 label 为空")
    return ref


def _values_to_train_quantile(values, reference: np.ndarray) -> np.ndarray:
    """把原始收益率映射到训练集经验分位数 [0, 1]。"""
    arr = np.asarray(values, dtype=float)
    ref = np.asarray(reference, dtype=float)
    out = np.full(arr.shape, np.nan, dtype=float)
    mask = np.isfinite(arr)
    if mask.any():
        out[mask] = np.searchsorted(ref, arr[mask], side="right") / float(len(ref))
    return np.clip(out, 0.0, 1.0)


def _train_quantile_to_values(quantile, reference: np.ndarray) -> np.ndarray:
    """把预测分位数按训练集经验分布反变换为原始收益率。"""
    q = np.asarray(quantile, dtype=float)
    ref = np.asarray(reference, dtype=float)
    out = np.full(q.shape, np.nan, dtype=float)
    mask = np.isfinite(q)
    if mask.any():
        out[mask] = np.quantile(ref, np.clip(q[mask], 0.0, 1.0))
    return out


class LabelTransformDataset:
    """对 dataset.prepare 返回的 label 做延迟转换，用于收益率分位数回归。"""

    def __init__(self, dataset, transform):
        self.dataset = dataset
        self.transform = transform
        self.segments = getattr(dataset, "segments", None)

    def prepare(self, *args, **kwargs):
        data = self.dataset.prepare(*args, **kwargs)
        col_set = kwargs.get("col_set")
        if col_set is None and len(args) >= 2:
            col_set = args[1]
        has_label = (
            col_set is None
            or col_set == "label"
            or (isinstance(col_set, (list, tuple, set)) and "label" in col_set)
        )
        if not has_label:
            return data
        values = _get_label_values(data)
        return _set_label_values(data, self.transform(values))


def _resolve_loss_params(
    model: ALSTM, loss_params: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    params = dict(loss_params or {})
    for key in ("huber_delta", "delta", "reduction"):
        if key in params:
            continue
        value = getattr(model, key, None)
        if value is not None:
            params[key] = value
    return params


def _weighted_regression_loss(
    pred: torch.Tensor,
    label: torch.Tensor,
    weight: torch.Tensor | None,
    *,
    loss_type: str,
    loss_params: Mapping[str, Any] | None = None,
) -> torch.Tensor:
    """支持 ALSTM 回归版的 MSE/Huber/Quantile pinball loss，并保留 qlib 的样本权重口径。"""
    loss_type = str(loss_type).lower()
    params = dict(loss_params or {})
    if pred.ndim == 2 and pred.shape[1] > 1:
        if label.ndim == 1:
            label = label.unsqueeze(1).expand_as(pred)
        elif label.ndim == 2 and label.shape[1] == 1:
            label = label.expand_as(pred)
        if weight is not None:
            if weight.ndim == 1:
                weight = weight.unsqueeze(1).expand_as(pred)
            elif weight.ndim == 2 and weight.shape[1] == 1:
                weight = weight.expand_as(pred)
    else:
        pred = pred.reshape(-1)
        label = label.reshape(-1)
        if weight is not None:
            weight = weight.reshape(-1)

    mask = torch.isfinite(label) & torch.isfinite(pred)
    if weight is None:
        weight = torch.ones_like(label)
    weight = weight.to(pred.device)

    if not bool(mask.any()):
        return pred.sum() * 0.0

    err = pred - label
    if loss_type == "mse":
        loss = err.square()
    elif loss_type in ("huber", "smooth_l1"):
        delta = float(params.get("delta", params.get("huber_delta", 1.0)))
        if delta <= 0:
            raise ValueError(f"huber delta 必须大于 0，当前值为 {delta}")
        abs_err = err.abs()
        quadratic = torch.minimum(
            abs_err, torch.as_tensor(delta, dtype=abs_err.dtype, device=abs_err.device)
        )
        linear = abs_err - quadratic
        loss = 0.5 * quadratic.square() + delta * linear
    elif loss_type in ("quantile", "pinball"):
        quantile_value = params.get(
            "quantiles", params.get("quantile", params.get("tau", 0.5))
        )
        if isinstance(quantile_value, (list, tuple, np.ndarray)):
            quantiles = [float(q) for q in quantile_value]
        else:
            quantiles = [float(quantile_value)]
        if any(q <= 0.0 or q >= 1.0 for q in quantiles):
            raise ValueError(
                f"quantile loss 的 quantile/tau 必须在 (0, 1)，当前值为 {quantiles}"
            )
        if pred.ndim == 2 and pred.shape[1] > 1:
            if len(quantiles) != pred.shape[1]:
                raise ValueError(
                    f"多输出 quantile loss 需要 {pred.shape[1]} 个 quantile，当前为 {quantiles}"
                )
            tau = torch.as_tensor(quantiles, dtype=pred.dtype, device=pred.device).view(
                1, -1
            )
        else:
            tau = torch.as_tensor(quantiles[0], dtype=pred.dtype, device=pred.device)
        # pinball loss: max(tau * (y - yhat), (tau - 1) * (y - yhat))
        target_minus_pred = label - pred
        loss = torch.maximum(
            tau * target_minus_pred,
            (tau - 1.0) * target_minus_pred,
        )
    else:
        raise ValueError(f"unknown loss `{loss_type}`，目前支持 mse/huber/quantile")

    loss = weight[mask] * loss[mask]
    reduction = str(params.get("reduction", "mean")).lower()
    if reduction == "mean":
        return torch.mean(loss)
    if reduction == "sum":
        return torch.sum(loss)
    raise ValueError(f"unknown loss reduction `{reduction}`，目前支持 mean/sum")


def _metric_score(
    pred: torch.Tensor,
    label: torch.Tensor,
    weight: torch.Tensor | None,
    *,
    metric: str,
    loss_type: str,
    loss_params: Mapping[str, Any] | None = None,
) -> torch.Tensor:
    metric = str(metric).lower()
    if metric in ("", "loss"):
        return -_weighted_regression_loss(
            pred, label, weight, loss_type=loss_type, loss_params=loss_params
        )
    if metric == "mse":
        return -_weighted_regression_loss(
            pred, label, weight, loss_type="mse", loss_params={"reduction": "mean"}
        )
    if metric in {
        "valid_loss",
        "valid_ic",
        "valid_rank_ic",
        "valid_icir",
        "valid_rank_icir",
        "valid_sharpe_constrained",
    }:
        return -_weighted_regression_loss(
            pred, label, weight, loss_type=loss_type, loss_params=loss_params
        )
    raise ValueError(f"unknown metric `{metric}`")


def _train_epoch_with_loss(
    model: ALSTM, data_loader, *, loss_params: Mapping[str, Any] | None = None
) -> None:
    model.ALSTM_model.train()
    for data, weight in data_loader:
        feature = data[:, :, 0:-1].to(model.device)
        label = data[:, -1, -1].to(model.device)
        weight = weight.to(model.device)

        pred = model.ALSTM_model(feature.float())
        loss = _weighted_regression_loss(
            pred,
            label,
            weight,
            loss_type=str(model.loss),
            loss_params=loss_params,
        )

        model.train_optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_value_(model.ALSTM_model.parameters(), 3.0)
        model.train_optimizer.step()


def _test_epoch_with_loss(
    model: ALSTM,
    data_loader,
    *,
    loss_params: Mapping[str, Any] | None = None,
) -> tuple[float, float]:
    model.ALSTM_model.eval()
    scores = []
    losses = []
    for data, weight in data_loader:
        feature = data[:, :, 0:-1].to(model.device)
        label = data[:, -1, -1].to(model.device)
        weight = weight.to(model.device)

        with torch.no_grad():
            pred = model.ALSTM_model(feature.float())
            loss = _weighted_regression_loss(
                pred,
                label,
                weight,
                loss_type=str(model.loss),
                loss_params=loss_params,
            )
            score = _metric_score(
                pred,
                label,
                weight,
                metric=str(model.metric),
                loss_type=str(model.loss),
                loss_params=loss_params,
            )
        losses.append(float(loss.item()))
        scores.append(float(score.item()))
    return float(np.mean(losses)), float(np.mean(scores))


def _select_prediction_array(
    pred: torch.Tensor | np.ndarray, output_index: int | None = None
) -> np.ndarray:
    arr = (
        pred.detach().cpu().numpy()
        if isinstance(pred, torch.Tensor)
        else np.asarray(pred)
    )
    if arr.ndim == 2 and arr.shape[1] > 1:
        idx = arr.shape[1] // 2 if output_index is None else int(output_index)
        idx = max(0, min(idx, arr.shape[1] - 1))
        return arr[:, idx].reshape(-1)
    return arr.reshape(-1)


def _predict_label_from_loader(
    model: ALSTM, data_loader, *, output_index: int | None = None
) -> tuple[np.ndarray, np.ndarray]:
    model.ALSTM_model.eval()
    pred_list = []
    label_list = []
    for data, _weight in data_loader:
        feature = data[:, :, 0:-1].to(model.device)
        label = data[:, -1, -1].to(model.device)
        with torch.no_grad():
            pred = model.ALSTM_model(feature.float())
        pred_list.append(_select_prediction_array(pred, output_index=output_index))
        label_list.append(label.detach().cpu().numpy().reshape(-1))
    if not pred_list:
        return np.array([], dtype=float), np.array([], dtype=float)
    return np.concatenate(pred_list).astype(float), np.concatenate(label_list).astype(
        float
    )


def _datetime_from_index(index, n: int) -> pd.Series:
    idx = index[:n]
    if isinstance(idx, pd.MultiIndex):
        names = list(idx.names or [])
        level = "datetime" if "datetime" in names else 0
        values = idx.get_level_values(level)
    else:
        values = idx
    return pd.Series(pd.to_datetime(values, errors="coerce"))


def _safe_corr(a, b, *, method: str = "pearson") -> float:
    df = pd.DataFrame({"a": a, "b": b}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(df) < 3 or df["a"].std(ddof=0) <= 0 or df["b"].std(ddof=0) <= 0:
        return np.nan
    return float(df["a"].corr(df["b"], method=method))


def _daily_icir(
    pred: np.ndarray, label: np.ndarray, index, *, method: str = "pearson"
) -> tuple[float, float, int]:
    dt = _datetime_from_index(index, len(pred))
    df = (
        pd.DataFrame({"datetime": dt, "pred": pred, "label": label})
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    if df.empty:
        return np.nan, np.nan, 0
    df["date"] = df["datetime"].dt.normalize()
    ic_values = []
    for _date, group in df.groupby("date"):
        if len(group) < 3:
            continue
        ic = _safe_corr(group["pred"], group["label"], method=method)
        if np.isfinite(ic):
            ic_values.append(ic)
    if not ic_values:
        return np.nan, np.nan, 0
    arr = np.asarray(ic_values, dtype=float)
    mean = float(np.nanmean(arr))
    std = float(np.nanstd(arr, ddof=1)) if len(arr) > 1 else np.nan
    icir = mean / std if np.isfinite(std) and std > 0 else np.nan
    return mean, float(icir), int(len(arr))


def _regression_epoch_selection_metrics(
    pred: np.ndarray,
    label: np.ndarray,
    index,
    params: Mapping[str, Any] | None = None,
) -> dict[str, float | int]:
    params = dict(params or {})
    pred = np.asarray(pred, dtype=float)
    label = np.asarray(label, dtype=float)
    mask = np.isfinite(pred) & np.isfinite(label)
    pred = pred[mask]
    label = label[mask]
    if pred.size == 0:
        return {
            "valid_ic": np.nan,
            "valid_rank_ic": np.nan,
            "valid_icir": np.nan,
            "valid_rank_icir": np.nan,
        }

    idx = index[: len(mask)]
    if len(mask) == len(idx):
        idx = idx[mask]

    ic = _safe_corr(pred, label, method="pearson")
    rank_ic = _safe_corr(pred, label, method="spearman")
    _ic_mean, icir, ic_days = _daily_icir(pred, label, idx, method="pearson")
    _rank_ic_mean, rank_icir, rank_ic_days = _daily_icir(
        pred, label, idx, method="spearman"
    )

    threshold = float(params.get("regression_selection_min_abs_pred", 0.0))
    signal = np.sign(pred)
    if threshold > 0:
        signal = np.where(np.abs(pred) >= threshold, signal, 0.0)
    traded = signal != 0
    trade_ratio = float(np.mean(traded)) if pred.size else 0.0
    min_trade_ratio = float(params.get("regression_selection_min_trade_ratio", 0.05))
    max_trade_ratio = float(params.get("regression_selection_max_trade_ratio", 1.0))
    min_trade_count = int(params.get("regression_selection_min_trade_count", 20))
    eps = float(params.get("regression_selection_sharpe_eps", 1e-12))
    pnl = signal * label
    traded_pnl = pnl[traded]
    if len(traded_pnl) > 1 and np.nanstd(traded_pnl) > eps:
        sharpe = float(np.nanmean(traded_pnl) / (np.nanstd(traded_pnl) + eps))
    else:
        sharpe = np.nan
    constraint_pass = (
        trade_ratio >= min_trade_ratio
        and trade_ratio <= max_trade_ratio
        and int(np.sum(traded)) >= min_trade_count
    )
    return {
        "valid_ic": ic,
        "valid_rank_ic": rank_ic,
        "valid_icir": icir,
        "valid_rank_icir": rank_icir,
        "valid_ic_days": ic_days,
        "valid_rank_ic_days": rank_ic_days,
        "valid_simple_sharpe": sharpe,
        "valid_trade_ratio": trade_ratio,
        "valid_trade_count": int(np.sum(traded)),
        "valid_simple_constraint_pass": int(bool(constraint_pass)),
    }


def _regression_selection_score(
    eval_metrics: Mapping[str, Any],
    *,
    metric: str,
) -> float:
    metric = str(metric or "valid_loss").lower()
    if metric in {"loss", "valid_loss"}:
        value = float(eval_metrics.get("valid_loss", np.nan))
        return -value if np.isfinite(value) else -np.inf
    if metric in {"ic", "valid_ic"}:
        value = float(eval_metrics.get("valid_ic", np.nan))
        return value if np.isfinite(value) else -np.inf
    if metric in {"rank_ic", "valid_rank_ic"}:
        value = float(eval_metrics.get("valid_rank_ic", np.nan))
        return value if np.isfinite(value) else -np.inf
    if metric in {"icir", "valid_icir"}:
        value = float(eval_metrics.get("valid_icir", np.nan))
        return value if np.isfinite(value) else -np.inf
    if metric in {"rank_icir", "valid_rank_icir"}:
        value = float(eval_metrics.get("valid_rank_icir", np.nan))
        return value if np.isfinite(value) else -np.inf
    if metric in {"valid_sharpe_constrained", "sharpe_constrained"}:
        if not bool(eval_metrics.get("valid_simple_constraint_pass", 0)):
            value = float(eval_metrics.get("valid_loss", np.nan))
            return -1e6 - value if np.isfinite(value) else -np.inf
        value = float(eval_metrics.get("valid_simple_sharpe", np.nan))
        return value if np.isfinite(value) else -np.inf
    raise ValueError(
        "regression_selection_metric 仅支持 valid_loss / valid_ic / valid_rank_ic / valid_icir / valid_rank_icir / valid_sharpe_constrained"
    )


def fit_alstm_with_progress(
    model: ALSTM,
    dataset,
    *,
    model_save_path: str | Path = "alstm_best_state.pt",
    label_scale: float = 1.0,
    loss_params: Mapping[str, Any] | None = None,
    selection_params: Mapping[str, Any] | None = None,
    show_progress: bool = True,
) -> tuple[dict[str, list[float]], int, float]:
    """训练 qlib ALSTM，显示 epoch 进度，并支持 label_scale 与可配置回归 loss。"""
    dl_train = dataset.prepare(
        "train", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L
    )
    dl_valid = dataset.prepare(
        "valid", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L
    )
    dl_train = _scale_label_frame(dl_train, label_scale)
    dl_valid = _scale_label_frame(dl_valid, label_scale)
    if dl_train.empty or dl_valid.empty:
        raise ValueError("训练集或验证集为空，请检查数据集切分配置")

    dl_train.config(fillna_type="ffill+bfill")
    dl_valid.config(fillna_type="ffill+bfill")

    wl_train = np.ones(len(dl_train))
    wl_valid = np.ones(len(dl_valid))

    train_loader = DataLoader(
        ConcatDataset(dl_train, wl_train),
        batch_size=model.batch_size,
        shuffle=True,
        num_workers=model.n_jobs,
        drop_last=True,
    )
    valid_loader = DataLoader(
        ConcatDataset(dl_valid, wl_valid),
        batch_size=model.batch_size,
        shuffle=False,
        num_workers=model.n_jobs,
        drop_last=False,
    )

    selection_params = dict(selection_params or {})
    selection_metric = str(
        selection_params.get(
            "regression_selection_metric", getattr(model, "metric", "loss")
        )
    ).lower()
    evals_result = {
        "train": [],
        "valid": [],
        "train_loss": [],
        "valid_loss": [],
        "valid_ic": [],
        "valid_rank_ic": [],
        "valid_icir": [],
        "valid_rank_icir": [],
        "valid_simple_sharpe": [],
        "valid_trade_ratio": [],
        "valid_trade_count": [],
        "valid_simple_constraint_pass": [],
    }
    best_score = -np.inf
    best_epoch = 0
    stop_steps = 0
    best_param = copy.deepcopy(model.ALSTM_model.state_dict())
    resolved_loss_params = _resolve_loss_params(model, loss_params)
    selection_output_index = resolved_loss_params.get("selection_output_index")
    if selection_output_index is not None:
        selection_output_index = int(selection_output_index)

    model.fitted = True
    epoch_iter = tqdm(
        range(model.n_epochs),
        desc="ALSTM Training Epoch",
        dynamic_ncols=True,
        disable=not show_progress,
    )
    for step in epoch_iter:
        _train_epoch_with_loss(model, train_loader, loss_params=resolved_loss_params)
        train_loss, train_score = _test_epoch_with_loss(
            model, train_loader, loss_params=resolved_loss_params
        )
        valid_loss, valid_score = _test_epoch_with_loss(
            model, valid_loader, loss_params=resolved_loss_params
        )
        valid_pred, valid_label = _predict_label_from_loader(
            model, valid_loader, output_index=selection_output_index
        )
        selection_eval = _regression_epoch_selection_metrics(
            valid_pred,
            valid_label,
            dl_valid.get_index(),
            params=selection_params,
        )
        selection_eval["valid_loss"] = float(valid_loss)
        selection_score = _regression_selection_score(
            selection_eval, metric=selection_metric
        )
        if not np.isfinite(selection_score):
            selection_score = -float(valid_loss) if np.isfinite(valid_loss) else -np.inf

        evals_result["train"].append(float(train_score))
        evals_result["valid"].append(float(selection_score))
        evals_result["train_loss"].append(float(train_loss))
        evals_result["valid_loss"].append(float(valid_loss))
        for key in (
            "valid_ic",
            "valid_rank_ic",
            "valid_icir",
            "valid_rank_icir",
            "valid_simple_sharpe",
            "valid_trade_ratio",
            "valid_trade_count",
            "valid_simple_constraint_pass",
        ):
            evals_result[key].append(selection_eval.get(key, np.nan))

        if show_progress:
            postfix = {
                "train_loss": f"{train_loss:.6f}",
                "valid_loss": f"{valid_loss:.6f}",
            }
            if selection_metric not in {"loss", "valid_loss"}:
                postfix["select"] = f"{selection_score:.6f}"
            epoch_iter.set_postfix(postfix)

        if selection_score > best_score:
            best_score = selection_score
            best_epoch = step
            stop_steps = 0
            best_param = copy.deepcopy(model.ALSTM_model.state_dict())
        else:
            stop_steps += 1
            if stop_steps >= model.early_stop:
                if show_progress:
                    print(f"触发早停：连续 {model.early_stop} 轮验证集无提升")
                break

    model.ALSTM_model.load_state_dict(best_param)
    torch.save(best_param, model_save_path)

    if model.use_gpu:
        torch.cuda.empty_cache()

    return evals_result, int(best_epoch), float(best_score)


def _predict_alstm_frame(model: ALSTM, dataset, segment: str = "test") -> pd.DataFrame:
    data = dataset.prepare(
        segment, col_set=["feature", "label"], data_key=DataHandlerLP.DK_L
    )
    if data is None or len(data) == 0:
        return pd.DataFrame()
    data.config(fillna_type="ffill+bfill")
    weights = np.ones(len(data))
    loader = DataLoader(
        ConcatDataset(data, weights),
        batch_size=model.batch_size,
        shuffle=False,
        num_workers=model.n_jobs,
        drop_last=False,
    )
    model.ALSTM_model.eval()
    pred_list = []
    for batch_data, _weight in loader:
        feature = batch_data[:, :, 0:-1].to(model.device)
        with torch.no_grad():
            pred = model.ALSTM_model(feature.float())
        arr = pred.detach().cpu().numpy()
        if arr.ndim == 1:
            arr = arr.reshape(-1, 1)
        pred_list.append(arr)
    if not pred_list:
        return pd.DataFrame(index=data.get_index())
    pred_arr = np.concatenate(pred_list, axis=0).astype(float)
    index = data.get_index()[: pred_arr.shape[0]]
    output_names = getattr(model, "output_names", None)
    if output_names is None or len(output_names) != pred_arr.shape[1]:
        output_names = (
            [0]
            if pred_arr.shape[1] == 1
            else [f"output_{i}" for i in range(pred_arr.shape[1])]
        )
    return pd.DataFrame(pred_arr, index=index, columns=list(output_names))


def predict_alstm_scaled(
    model: ALSTM, dataset, segment: str = "test", label_scale: float = 1.0
):
    """预测并按 label_scale 还原到原始 label 口径。"""
    if int(getattr(model, "output_dim", 1)) > 1:
        return unscale_prediction(
            _predict_alstm_frame(model, dataset, segment=segment), label_scale
        )
    return unscale_prediction(
        predict_alstm(model, dataset, segment=segment), label_scale
    )


def calc_segment_loss(dataset_2d, pred_obj, segment: str) -> dict[str, float | int]:
    """按 segment 计算原始 label 口径下的 MSE。"""
    label_df = dataset_2d.prepare(segment, col_set="label", data_key=DataHandlerLP.DK_L)
    y_true = label_df.iloc[:, 0]
    if isinstance(pred_obj, pd.DataFrame):
        if pred_obj.empty:
            return {"n_samples": 0, "mse": np.nan}
        pred_col = (
            "q50"
            if "q50" in pred_obj.columns
            else pred_obj.columns[len(pred_obj.columns) // 2]
        )
        y_pred = pred_obj[pred_col]
    else:
        y_pred = pred_obj
    aligned = pd.concat(
        [y_true.rename("y_true"), y_pred.rename("y_pred")], axis=1
    ).dropna()
    if aligned.empty:
        return {"n_samples": 0, "mse": np.nan}
    mse = float(np.mean((aligned["y_pred"] - aligned["y_true"]) ** 2))
    return {"n_samples": int(len(aligned)), "mse": mse}


def train_predict_alstm(
    ts_dataset,
    base_dataset,
    *,
    d_feat: int,
    model_params: Mapping[str, Any] | None = None,
    label_scale: float = 1.0,
    model_save_path: str | Path = "alstm_best_state.pt",
    artifact_dir: str | Path | None = None,
    artifact_prefix: str = "alstm",
    show_progress: bool = True,
) -> dict[str, Any]:
    """构建、训练、预测 ALSTM，并返回 notebook 常用结果。"""
    t1 = time.perf_counter()
    final_params = {"d_feat": d_feat}
    if model_params:
        final_params.update(dict(model_params))

    model_config = build_alstm_model_config(model_params=final_params)
    model = build_alstm_model(model_params=final_params)
    output_dim = int(final_params.get("output_dim", 1))
    output_names = final_params.get("output_names")
    if output_dim > 1:
        model = _set_alstm_output_dim(
            model, output_dim, output_names=list(output_names or [])
        )

    evals_result, best_epoch, best_score = fit_alstm_with_progress(
        model,
        ts_dataset,
        model_save_path=model_save_path,
        label_scale=label_scale,
        loss_params=final_params.get("loss_params"),
        selection_params=final_params,
        show_progress=show_progress,
    )

    pred_train = predict_alstm_scaled(
        model, ts_dataset, segment="train", label_scale=label_scale
    )
    pred_valid = predict_alstm_scaled(
        model, ts_dataset, segment="valid", label_scale=label_scale
    )
    pred = predict_alstm_scaled(
        model, ts_dataset, segment="test", label_scale=label_scale
    )

    segment_loss = {
        "train": calc_segment_loss(base_dataset, pred_train, "train"),
        "valid": calc_segment_loss(base_dataset, pred_valid, "valid"),
        "test": calc_segment_loss(base_dataset, pred, "test"),
    }

    model_seconds = time.perf_counter() - t1
    result = {
        "model": model,
        "model_config": model_config,
        "evals_result": evals_result,
        "best_epoch": int(best_epoch),
        "best_score": float(best_score),
        "pred_train": pred_train,
        "pred_valid": pred_valid,
        "pred": pred,
        "segment_loss": segment_loss,
        "model_seconds": float(model_seconds),
    }

    if artifact_dir is not None:
        artifact_path = Path(artifact_dir)
        artifact_path.mkdir(parents=True, exist_ok=True)
        model_obj_path = artifact_path / f"{artifact_prefix}_model.pkl"
        model_state_path = artifact_path / f"{artifact_prefix}_best_state.pt"
        pred_test_path = artifact_path / f"{artifact_prefix}_pred_test.pkl"
        pred_test_csv = artifact_path / f"{artifact_prefix}_pred_test.csv"
        try:
            joblib.dump(model, model_obj_path)
            result["model_obj_path"] = str(model_obj_path)
        except Exception as exc:
            result["model_obj_save_error"] = str(exc)
        torch.save(model.ALSTM_model.state_dict(), model_state_path)
        pred.to_pickle(pred_test_path)
        pred.to_csv(pred_test_csv)
        result.update(
            {
                "model_state_path": str(model_state_path),
                "pred_test_path": str(pred_test_path),
                "pred_test_csv": str(pred_test_csv),
            }
        )

    return result


def _pred_to_series(pred_obj, name: str = "score") -> pd.Series:
    ser = pred_obj.iloc[:, 0] if isinstance(pred_obj, pd.DataFrame) else pred_obj
    return pd.to_numeric(ser, errors="coerce").rename(name)


def _concat_quantile_predictions(pred_by_quantile: Mapping[float, Any]) -> pd.DataFrame:
    parts = []
    for quantile, pred_obj in sorted(
        pred_by_quantile.items(), key=lambda item: float(item[0])
    ):
        parts.append(_pred_to_series(pred_obj, name=_q_name(float(quantile))))
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, axis=1)


def _quantile_risk_adjusted_signal(
    quantile_pred_df: pd.DataFrame,
    *,
    median_quantile: float = 0.5,
    lower_quantile: float | None = None,
    upper_quantile: float | None = None,
    eps: float = 1e-12,
) -> pd.Series:
    if quantile_pred_df.empty:
        return pd.Series(dtype=float, name="quantile_risk_adjusted_signal")
    cols = list(quantile_pred_df.columns)
    median_col = _q_name(median_quantile)
    if median_col not in quantile_pred_df.columns:
        median_col = cols[len(cols) // 2]
    lower_col = _q_name(lower_quantile) if lower_quantile is not None else cols[0]
    upper_col = _q_name(upper_quantile) if upper_quantile is not None else cols[-1]
    if lower_col not in quantile_pred_df.columns:
        lower_col = cols[0]
    if upper_col not in quantile_pred_df.columns:
        upper_col = cols[-1]
    median = pd.to_numeric(quantile_pred_df[median_col], errors="coerce")
    interval_width = (
        pd.to_numeric(quantile_pred_df[upper_col], errors="coerce")
        - pd.to_numeric(quantile_pred_df[lower_col], errors="coerce")
    ).abs()
    return (median / (interval_width + float(eps))).rename(
        "quantile_risk_adjusted_signal"
    )


def _label_series(dataset_2d, segment: str, name: str = "label") -> pd.Series:
    label_df = dataset_2d.prepare(segment, col_set="label", data_key=DataHandlerLP.DK_L)
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


def _looks_like_state_label(label, num_classes: int = 3) -> bool:
    """判断标签是否已经是离散状态类别。"""
    arr = np.asarray(label, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return False
    rounded = np.round(arr)
    return bool(
        np.all(np.isclose(arr, rounded))
        and set(np.unique(rounded).astype(int)).issubset(set(range(int(num_classes))))
    )


def state_target_from_label(
    label, threshold: float = 0.0002, num_classes: int = 3
) -> np.ndarray:
    """兼容两种输入：原始收益率标签，或已经离散化的状态标签。"""
    num_classes = int(num_classes)
    if _looks_like_state_label(label, num_classes=num_classes):
        return np.asarray(np.round(label), dtype=int).clip(0, num_classes - 1)
    if num_classes == 2:
        # 二分类：abs(return) > threshold -> 1 (trade), 否则 0 (no_trade)
        arr = np.asarray(label, dtype=float)
        out = np.zeros(arr.shape, dtype=int)
        out[np.abs(arr) > float(threshold)] = 1
        return out
    if num_classes != 3:
        raise ValueError("多状态分类需要传入已离散化的 label_state 标签")
    return state_from_return(label, threshold)


def state_reward_from_label(
    label,
    target_state: np.ndarray,
    threshold: float = 0.0002,
    num_classes: int = 3,
) -> np.ndarray:
    """用于状态分类早停的简化收益序列。"""
    if _looks_like_state_label(label, num_classes=num_classes):
        flat_idx = int(num_classes) // 2
        return np.sign(np.asarray(target_state, dtype=float) - float(flat_idx))
    return np.asarray(label, dtype=float)


def calc_state_metrics(
    y_true_state,
    y_pred_state,
    *,
    class_names: Mapping[int, str] | None = None,
    prefix: str = "",
) -> dict[str, float | int]:
    """计算状态分类指标，支持三状态或已离散化的多状态标签。"""
    class_names = dict(class_names or {0: "down", 1: "flat", 2: "up"})
    flat_id = next(
        (int(k) for k, v in class_names.items() if str(v).lower() == "flat"),
        len(class_names) // 2,
    )
    y_true_state = np.asarray(y_true_state, dtype=int)
    y_pred_state = np.asarray(y_pred_state, dtype=int)
    n = int(len(y_true_state))
    if n == 0:
        return {prefix + "n": 0}

    out: dict[str, float | int] = {
        prefix + "n": n,
        prefix + "accuracy": float((y_true_state == y_pred_state).mean()),
        prefix + "pred_trade_ratio": float((y_pred_state != flat_id).mean()),
        prefix + "true_trade_ratio": float((y_true_state != flat_id).mean()),
    }
    true_trade_mask = y_true_state != flat_id
    pred_trade_mask = y_pred_state != flat_id
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
    # 回归派生方向指标始终由收益率阈值切成 down/flat/up 三状态；
    # 即使状态分类器使用五状态，也不能复用五状态 class_names。
    out.update(
        calc_state_metrics(
            state_from_return(y_true, state_threshold),
            state_from_return(y_pred, state_threshold),
            prefix="state_",
        )
    )
    return out


class ALSTMStateClassifier(nn.Module):
    """ALSTM 风格的三状态分类器。"""

    def __init__(
        self,
        d_feat: int,
        hidden_size: int = 32,
        num_layers: int = 2,
        dropout: float = 0.3,
        num_classes: int = 3,
        rnn_type: str = "GRU",
    ):
        super().__init__()
        klass = getattr(nn, rnn_type.upper())
        self.net = nn.Sequential(nn.Linear(d_feat, hidden_size), nn.Tanh())
        self.rnn = klass(
            input_size=hidden_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        att_hidden = max(int(hidden_size / 2), 1)
        self.att_net = nn.Sequential(
            nn.Linear(hidden_size, att_hidden),
            nn.Dropout(dropout),
            nn.Tanh(),
            nn.Linear(att_hidden, 1, bias=False),
            nn.Softmax(dim=1),
        )
        self.fc_out = nn.Linear(hidden_size * 2, num_classes)

    def forward(self, inputs):
        rnn_out, _ = self.rnn(self.net(inputs))
        attention_score = self.att_net(rnn_out)
        out_att = torch.sum(rnn_out * attention_score, dim=1)
        return self.fc_out(torch.cat((rnn_out[:, -1, :], out_att), dim=1))


def _make_classifier_loader(
    ts_dataset,
    segment: str,
    batch_size: int,
    *,
    shuffle: bool = False,
    drop_last: bool = False,
    sample_weight: pd.Series | np.ndarray | None = None,
):
    cal = getattr(ts_dataset, "cal", None)
    if cal is not None and len(cal) == 0:
        freq = getattr(ts_dataset, "freq", None)
        segments = getattr(ts_dataset, "segments", None)
        raise ValueError(
            f"TSDatasetH calendar is empty for segment={segment!r}, freq={freq!r}, segments={segments!r}. "
            "Check qlib.init provider_uri/freq and pass freq explicitly when constructing TSDatasetH."
        )
    sampler = ts_dataset.prepare(
        segment, col_set=["feature", "label"], data_key=DataHandlerLP.DK_L
    )
    sampler.config(fillna_type="ffill+bfill")
    if sample_weight is None:
        wl = np.ones(len(sampler), dtype="float32")
    else:
        if isinstance(sample_weight, pd.Series):
            idx = sampler.get_index()
            wl = pd.to_numeric(sample_weight.reindex(idx), errors="coerce").to_numpy(
                dtype="float32"
            )
        else:
            wl = np.asarray(sample_weight, dtype="float32").reshape(-1)
            if len(wl) != len(sampler):
                raise ValueError(
                    f"{segment} sample_weight 长度 {len(wl)} 与 sampler 长度 {len(sampler)} 不一致"
                )
        wl = np.where(np.isfinite(wl) & (wl >= 0), wl, 1.0).astype("float32")
    loader = DataLoader(
        ConcatDataset(sampler, wl),
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=0,
        drop_last=drop_last,
    )
    return sampler, loader


def _build_state_sample_weight(
    raw_return: pd.Series,
    *,
    threshold: float,
    mode: str = "none",
    power: float = 1.0,
    scale: float = 1.0,
    clip: float | tuple[float, float] | list[float] | None = None,
) -> pd.Series | None:
    """根据原始未来收益幅度构造状态分类样本权重，只用于 train/valid loss。"""
    mode = str(mode or "none").lower()
    if mode in {"none", "off", "false", "0"}:
        return None
    if mode not in {"abs_return", "abs", "magnitude"}:
        raise ValueError("state_sample_weight_mode 仅支持 none / abs_return")

    raw = (
        pd.to_numeric(raw_return, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .abs()
    )
    denom = max(abs(float(threshold)), 1e-12)
    strength = (raw / denom).clip(lower=0.0)
    power = float(power)
    if power <= 0:
        raise ValueError(f"state_sample_weight_power 必须大于 0，当前为 {power}")
    weight = 1.0 + float(scale) * np.power(strength, power)
    weight = pd.Series(
        weight, index=raw_return.index, name="state_sample_weight"
    ).replace([np.inf, -np.inf], np.nan)
    if clip is not None:
        if isinstance(clip, (tuple, list)):
            lo, hi = float(clip[0]), float(clip[1])
        else:
            lo, hi = 1.0 / float(clip), float(clip)
        weight = weight.clip(lower=lo, upper=hi)
    mean = float(weight.dropna().mean()) if len(weight.dropna()) else np.nan
    if np.isfinite(mean) and mean > 0:
        weight = weight / mean
    return weight.fillna(1.0).astype("float32")


def _state_classification_loss(
    logits: torch.Tensor,
    target: torch.Tensor,
    *,
    class_weight: torch.Tensor | None = None,
    sample_weight: torch.Tensor | None = None,
    loss_type: str = "cross_entropy",
    focal_gamma: float = 2.0,
) -> torch.Tensor:
    """状态分类 loss：普通 CE 或 focal loss，并支持样本权重。"""
    loss_type = str(loss_type or "cross_entropy").lower()
    if loss_type in {"ce", "cross_entropy", "crossentropyloss"}:
        per_sample = F.cross_entropy(
            logits, target, weight=class_weight, reduction="none"
        )
    elif loss_type in {"focal", "focal_loss"}:
        gamma = float(focal_gamma)
        if gamma < 0:
            raise ValueError(f"state_focal_gamma 必须 >= 0，当前为 {gamma}")
        ce = F.cross_entropy(logits, target, weight=class_weight, reduction="none")
        prob = torch.softmax(logits, dim=1)
        pt = prob.gather(1, target.view(-1, 1)).squeeze(1).clamp(1e-8, 1.0)
        per_sample = torch.pow(1.0 - pt, gamma) * ce
    else:
        raise ValueError("state_loss 仅支持 cross_entropy / focal")

    if sample_weight is not None:
        sw = sample_weight.to(logits.device).reshape(-1).float()
        sw = torch.where(torch.isfinite(sw) & (sw >= 0), sw, torch.ones_like(sw))
        per_sample = per_sample * sw
        denom = sw.sum().clamp_min(1e-12)
        return per_sample.sum() / denom
    return per_sample.mean()


def _ensure_ts_calendar(
    ts_dataset, *, start_time: Any, end_time: Any, freq: str
) -> None:
    """Fill TSDatasetH.cal explicitly for qlib versions that miss custom freq calendars."""
    cal = getattr(ts_dataset, "cal", None)
    if cal is None or len(cal) > 0:
        return

    calendar = list(D.calendar(start_time=start_time, end_time=end_time, freq=freq))
    if not calendar:
        segments = getattr(ts_dataset, "segments", None)
        raise ValueError(
            f"TSDatasetH calendar is empty and D.calendar also returned empty for freq={freq!r}, "
            f"start_time={start_time!r}, end_time={end_time!r}, segments={segments!r}. "
            "Check qlib.init(provider_uri={freq: provider_uri}) and the provider calendar file."
        )
    ts_dataset.cal = calendar
    try:
        ts_dataset.freq = freq
    except Exception:
        pass


def _class_counts_from_loader(
    loader, threshold: float, num_classes: int = 3
) -> np.ndarray:
    counts = np.zeros(int(num_classes), dtype=float)
    for data, _weight in loader:
        label = data[:, -1, -1].cpu().numpy()
        counts += np.bincount(
            state_target_from_label(label, threshold, num_classes=num_classes),
            minlength=int(num_classes),
        ).astype(float)
    return counts


def _build_state_class_weights(
    class_counts: np.ndarray,
    *,
    mode: str = "balanced",
    clip: float | tuple[float, float] | list[float] | None = None,
) -> np.ndarray | None:
    """按类别分布构造状态分类权重。"""
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


def train_state_classifier(
    ts_dataset,
    *,
    d_feat: int,
    model_params: Mapping[str, Any] | None = None,
    state_threshold: float = 0.0002,
    state_sample_weight_by_segment: Mapping[str, pd.Series] | None = None,
    model_save_path: str | Path = "alstm_state_classifier.pt",
    show_progress: bool = True,
) -> dict[str, Any]:
    """训练状态 ALSTM 分类器，支持三状态或已离散化的多状态标签。"""
    params = dict(DEFAULT_ALSTM_PARAMS)
    if model_params:
        params.update(dict(model_params))
    params["d_feat"] = d_feat
    num_classes = int(params.get("state_num_classes", params.get("num_classes", 3)))
    state_class_names = params.get("state_class_names")

    torch.manual_seed(int(params.get("seed", 42)))
    gpu_id = int(params.get("GPU", -1))
    device = torch.device(
        f"cuda:{gpu_id}" if gpu_id >= 0 and torch.cuda.is_available() else "cpu"
    )

    batch_size = int(params.get("state_batch_size", params.get("batch_size", 800)))
    train_drop_last = bool(params.get("state_drop_last", False))
    sample_weight_by_segment = dict(state_sample_weight_by_segment or {})
    train_sampler, train_loader = _make_classifier_loader(
        ts_dataset,
        "train",
        batch_size,
        shuffle=True,
        drop_last=train_drop_last,
        sample_weight=sample_weight_by_segment.get("train"),
    )
    _valid_sampler, valid_loader = _make_classifier_loader(
        ts_dataset,
        "valid",
        batch_size,
        shuffle=False,
        drop_last=False,
        sample_weight=sample_weight_by_segment.get("valid"),
    )

    class_counts = _class_counts_from_loader(
        train_loader, state_threshold, num_classes=num_classes
    )
    class_weights = _build_state_class_weights(
        class_counts,
        mode=str(params.get("state_class_weight_mode", "balanced")),
        clip=params.get("state_class_weight_clip"),
    )

    classifier = ALSTMStateClassifier(
        d_feat=d_feat,
        hidden_size=int(params.get("state_hidden_size", params.get("hidden_size", 32))),
        num_layers=int(params.get("state_num_layers", params.get("num_layers", 2))),
        dropout=float(params.get("state_dropout", params.get("dropout", 0.3))),
        num_classes=num_classes,
    ).to(device)
    criterion_weight = (
        torch.tensor(class_weights, dtype=torch.float32, device=device)
        if class_weights is not None
        else None
    )
    state_loss = str(
        params.get(
            "state_loss", params.get("state_classification_loss", "cross_entropy")
        )
    ).lower()
    state_focal_gamma = float(params.get("state_focal_gamma", 2.0))
    optimizer = torch.optim.Adam(
        classifier.parameters(),
        lr=float(params.get("state_lr", params.get("lr", 1e-3))),
        weight_decay=float(params.get("state_weight_decay", 0.0)),
    )
    grad_clip_norm = float(params.get("state_grad_clip_norm", 3.0))

    def _run_epoch(loader, train: bool = False) -> float:
        classifier.train(train)
        losses = []
        for data, sample_weight in loader:
            feature = data[:, :, 0:-1].to(device)
            label = data[:, -1, -1].cpu().numpy()
            target = torch.tensor(
                state_target_from_label(
                    label, state_threshold, num_classes=num_classes
                ),
                dtype=torch.long,
                device=device,
            )
            if train:
                optimizer.zero_grad()
            logits = classifier(feature.float())
            loss = _state_classification_loss(
                logits,
                target,
                class_weight=criterion_weight,
                sample_weight=sample_weight,
                loss_type=state_loss,
                focal_gamma=state_focal_gamma,
            )
            if train:
                loss.backward()
                if grad_clip_norm > 0:
                    torch.nn.utils.clip_grad_norm_(
                        classifier.parameters(), max_norm=grad_clip_norm
                    )
                optimizer.step()
            losses.append(float(loss.item()))
        return float(np.mean(losses)) if losses else np.nan

    def _eval_classifier(loader) -> dict[str, float | int]:
        classifier.eval()
        losses = []
        pred_state_list = []
        true_state_list = []
        raw_label_list = []
        with torch.no_grad():
            for data, sample_weight in loader:
                feature = data[:, :, 0:-1].to(device)
                label = data[:, -1, -1].cpu().numpy()
                target_np = state_target_from_label(
                    label, state_threshold, num_classes=num_classes
                )
                target = torch.tensor(target_np, dtype=torch.long, device=device)
                logits = classifier(feature.float())
                loss = _state_classification_loss(
                    logits,
                    target,
                    class_weight=criterion_weight,
                    sample_weight=sample_weight,
                    loss_type=state_loss,
                    focal_gamma=state_focal_gamma,
                )
                pred_state = torch.argmax(logits, dim=1).cpu().numpy()

                losses.append(float(loss.item()))
                pred_state_list.append(pred_state)
                true_state_list.append(target_np)
                raw_label_list.append(label)

        if not pred_state_list:
            return {"valid_loss": np.nan}

        pred_state_arr = np.concatenate(pred_state_list).astype(int)
        true_state_arr = np.concatenate(true_state_list).astype(int)
        raw_label_arr = np.concatenate(raw_label_list).astype(float)
        metrics = calc_state_metrics(
            true_state_arr, pred_state_arr, class_names=state_class_names, prefix=""
        )
        flat_idx = num_classes // 2
        signal = np.sign(pred_state_arr.astype(float) - float(flat_idx))
        reward_arr = state_reward_from_label(
            raw_label_arr, true_state_arr, state_threshold, num_classes=num_classes
        )
        strategy_ret = signal * reward_arr
        trade_mask = signal != 0.0
        traded_ret = strategy_ret[trade_mask]
        min_pred_trade_ratio = float(
            params.get("state_selection_min_pred_trade_ratio", 0.05)
        )
        min_trade_count = int(params.get("state_selection_min_trade_count", 1))
        sharpe_eps = float(params.get("state_selection_sharpe_eps", 1e-12))
        constraint_pass = bool(
            metrics.get("pred_trade_ratio", 0.0) >= min_pred_trade_ratio
            and int(trade_mask.sum()) >= min_trade_count
        )
        if len(traded_ret) > 1 and float(np.std(traded_ret)) > sharpe_eps:
            sharpe = float(np.mean(traded_ret) / (np.std(traded_ret) + sharpe_eps))
        else:
            sharpe = np.nan

        out: dict[str, float | int] = {
            "valid_loss": float(np.mean(losses)) if losses else np.nan,
            "valid_simple_return_mean": float(np.mean(traded_ret))
            if len(traded_ret)
            else np.nan,
            "valid_simple_return_sum": float(np.sum(traded_ret))
            if len(traded_ret)
            else 0.0,
            "valid_simple_sharpe": sharpe,
            "valid_simple_trade_count": int(trade_mask.sum()),
            "valid_simple_constraint_pass": int(constraint_pass),
        }
        out.update({f"valid_{k}": v for k, v in metrics.items()})
        return out

    def _selection_score(eval_metrics: Mapping[str, Any]) -> float:
        metric = str(params.get("state_selection_metric", "valid_loss")).lower()
        if metric == "valid_loss":
            value = float(eval_metrics.get("valid_loss", np.nan))
            return -value if np.isfinite(value) else -np.inf
        if metric == "valid_trade_precision_pred_nonflat":
            value = float(
                eval_metrics.get("valid_trade_precision_pred_nonflat", np.nan)
            )
            return value if np.isfinite(value) else -np.inf
        if metric == "valid_macro_f1":
            value = float(eval_metrics.get("valid_macro_f1", np.nan))
            return value if np.isfinite(value) else -np.inf
        if metric in {"valid_sharpe_constrained", "valid_simple_sharpe_constrained"}:
            if not bool(eval_metrics.get("valid_simple_constraint_pass", 0)):
                valid_loss = float(eval_metrics.get("valid_loss", np.nan))
                return -1e12 - valid_loss if np.isfinite(valid_loss) else -1e12
            value = float(eval_metrics.get("valid_simple_sharpe", np.nan))
            return value if np.isfinite(value) else -np.inf
        raise ValueError(
            "state_selection_metric 仅支持 valid_loss / valid_trade_precision_pred_nonflat / "
            "valid_macro_f1 / valid_sharpe_constrained"
        )

    logs = []
    selection_metric = str(params.get("state_selection_metric", "valid_loss"))
    best_selection_score = -np.inf
    best_valid_loss = float("inf")
    best_metric_value = np.nan
    best_epoch = 0
    best_param = copy.deepcopy(classifier.state_dict())
    stop_steps = 0
    epoch_iter = tqdm(
        range(int(params.get("n_epochs", 30))),
        desc="ALSTM State Training",
        dynamic_ncols=True,
        disable=not show_progress,
    )
    for epoch in epoch_iter:
        train_loss = _run_epoch(train_loader, train=True)
        valid_eval = _eval_classifier(valid_loader)
        valid_loss = float(valid_eval.get("valid_loss", np.nan))
        selection_score = _selection_score(valid_eval)
        log_row = {
            "epoch": epoch,
            "train_loss": train_loss,
            **valid_eval,
            "state_selection_metric": selection_metric,
            "state_selection_score": selection_score,
        }
        logs.append(log_row)
        if show_progress:
            epoch_iter.set_postfix(
                {
                    "train_loss": f"{train_loss:.6f}",
                    "valid_loss": f"{valid_loss:.6f}",
                    "sel_score": f"{selection_score:.6f}",
                }
            )
        if selection_score > best_selection_score:
            best_selection_score = float(selection_score)
            best_valid_loss = valid_loss
            best_metric_value = valid_eval.get(selection_metric, np.nan)
            if selection_metric in {
                "valid_sharpe_constrained",
                "valid_simple_sharpe_constrained",
            }:
                best_metric_value = valid_eval.get("valid_simple_sharpe", np.nan)
            best_epoch = epoch
            best_param = copy.deepcopy(classifier.state_dict())
            stop_steps = 0
        else:
            stop_steps += 1
            if stop_steps >= int(params.get("early_stop", 10)):
                if show_progress:
                    print(
                        f"状态分类触发早停：连续 {params.get('early_stop', 10)} 轮验证集无提升"
                    )
                break

    classifier.load_state_dict(best_param)
    torch.save(best_param, model_save_path)
    return {
        "model": classifier,
        "evals_result": pd.DataFrame(logs),
        "best_epoch": int(best_epoch),
        "best_score": float(best_valid_loss),
        "best_selection_metric": selection_metric,
        "best_selection_score": float(best_selection_score),
        "best_selection_metric_value": float(best_metric_value)
        if pd.notna(best_metric_value)
        else np.nan,
        "class_counts": class_counts,
        "class_weights": class_weights,
        "state_loss": state_loss,
        "state_focal_gamma": state_focal_gamma
        if state_loss in {"focal", "focal_loss"}
        else None,
        "state_sample_weight_mode": str(params.get("state_sample_weight_mode", "none")),
        "train_sampler_len": int(len(train_sampler)),
    }


def predict_state_classifier(
    classifier: ALSTMStateClassifier,
    ts_dataset,
    *,
    segment: str = "test",
    batch_size: int = 800,
    state_threshold: float = 0.0002,
    class_names: Mapping[int, str] | None = None,
) -> tuple[pd.DataFrame, pd.Series, pd.Series, pd.Series]:
    """预测状态分类概率和类别。"""
    device = next(classifier.parameters()).device
    fc_out = getattr(classifier, "fc_out", None)
    num_classes = int(getattr(fc_out, "out_features", 3))
    class_names = dict(class_names or {0: "down", 1: "flat", 2: "up"})
    sampler, loader = _make_classifier_loader(
        ts_dataset, segment, batch_size, shuffle=False, drop_last=False
    )
    classifier.eval()
    prob_list = []
    true_state_list = []
    raw_label_list = []
    with torch.no_grad():
        for data, _weight in loader:
            feature = data[:, :, 0:-1].to(device)
            raw_label = data[:, -1, -1].cpu().numpy()
            prob = torch.softmax(classifier(feature.float()), dim=1).cpu().numpy()
            prob_list.append(prob)
            raw_label_list.append(raw_label)
            true_state_list.append(
                state_target_from_label(
                    raw_label, state_threshold, num_classes=num_classes
                )
            )

    prob_arr = np.vstack(prob_list) if prob_list else np.empty((0, num_classes))
    raw_label_arr = np.concatenate(raw_label_list) if raw_label_list else np.array([])
    true_state_arr = (
        np.concatenate(true_state_list) if true_state_list else np.array([], dtype=int)
    )
    idx = sampler.get_index()[: len(prob_arr)]
    prob_cols = [f"prob_{class_names.get(i, f'class_{i}')}" for i in range(num_classes)]
    prob_df = pd.DataFrame(prob_arr, index=idx, columns=prob_cols)
    flat_idx = min(max(num_classes // 2, 0), num_classes - 1)
    if num_classes == 3 and set(prob_cols) == {"prob_down", "prob_flat", "prob_up"}:
        pass
    else:
        down_cols = [prob_cols[i] for i in range(flat_idx)]
        up_cols = [prob_cols[i] for i in range(flat_idx + 1, num_classes)]
        prob_df["prob_down"] = prob_df[down_cols].sum(axis=1) if down_cols else 0.0
        prob_df["prob_flat"] = prob_df[prob_cols[flat_idx]] if prob_cols else np.nan
        prob_df["prob_up"] = prob_df[up_cols].sum(axis=1) if up_cols else 0.0
    pred_state = pd.Series(prob_arr.argmax(axis=1), index=idx, name="pred_state")
    true_state = pd.Series(true_state_arr, index=idx, name="true_state")
    raw_label = pd.Series(raw_label_arr, index=idx, name="raw_return")
    return prob_df, pred_state, true_state, raw_label


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
        # 这里仅需要读取/对齐标签，使用普通单标的数据集即可，避免跨标的自定义 loader 忽略 label_expr。
        from factor_library.futures.minute_factors import build_dataset

        target_instrument = kwargs.get("target_instrument")
        if target_instrument is not None and "instruments" not in kwargs:
            kwargs["instruments"] = [str(target_instrument)]
        kwargs["selected_factors"] = ["returns_1min"]
        kwargs["use_robust_preprocess"] = False
        kwargs.pop("freq", None)
        kwargs.pop("target_instrument", None)
        kwargs.pop("context_instrument", None)
        kwargs.pop("context_instruments", None)
        kwargs.pop("target_provider_uri", None)
        kwargs.pop("context_provider_uri", None)
        kwargs.pop("context_provider_uris", None)
        kwargs.pop("context_prefixes", None)
        kwargs.pop("selected_cross_features", None)
        kwargs.pop("use_cross_instrument_features", None)
        return build_dataset(**kwargs)
    return dataset_builder(**kwargs)


def train_predict_return_and_state_alstm(
    *,
    ts_dataset,
    base_dataset,
    dataset_builder,
    dataset_kwargs: Mapping[str, Any],
    raw_label_expr: str | None = None,
    state_label_expr: str | None = None,
    state_weight_label_expr: str | None = None,
    label_scale_expr: str | None = None,
    d_feat: int,
    step_len: int,
    model_task: str = "regression",
    regression_label_mode: str = "raw_return",
    regression_train_mode: str = "direct",
    label_is_standardized: bool = True,
    label_scale: float = 1.0,
    model_params: Mapping[str, Any] | None = None,
    state_return_threshold: float = 0.0002,
    state_class_names: Mapping[int, str] | None = None,
    model_save_path: str | Path = "artifacts/alstm_std_return_best_state.pt",
    state_model_save_path: str | Path = "artifacts/alstm_state_classifier.pt",
    artifact_dir: str | Path | None = "artifacts",
    artifact_prefix: str = "alstm",
    show_progress: bool = True,
) -> dict[str, Any]:
    """按参数训练收益回归模型、三状态分类模型，或二者都训练。

    Parameters
    ----------
    model_task
        "regression" 只训练回归模型；"state" 只训练三状态分类模型；"both" 两个都训练。
    label_is_standardized
        回归标签是否是波动标准化收益。为 True 时会使用 label_scale_expr 乘回原始收益率口径；
        为 False 时直接把回归预测当作原始收益率口径。
    regression_label_mode
        回归标签口径，支持 raw_return / standardized_return / return_quantile / quantile_regression /
        log_return / log_return_zscore。
        return_quantile 会只用 train 段原始收益率拟合经验分布，再转换 train/valid/test label。
    regression_train_mode
        回归训练方式，支持 direct / empirical_quantile / quantile_regression。
        quantile_regression 会用一个多输出 ALSTM 同时预测多个收益分位数，默认分位数为 0.1/0.5/0.9。
    """
    model_task = (model_task or "regression").lower()
    regression_label_mode = (
        regression_label_mode
        or ("standardized_return" if label_is_standardized else "raw_return")
    ).lower()
    regression_train_mode = (regression_train_mode or "direct").lower()
    if regression_label_mode == "return_quantile":
        regression_train_mode = "empirical_quantile"
    elif regression_label_mode == "quantile_regression":
        regression_train_mode = "quantile_regression"
        regression_label_mode = "raw_return"
    if model_task not in {"regression", "state", "both"}:
        raise ValueError("model_task 仅支持 regression / state / both")
    if regression_label_mode not in {
        "raw_return",
        "standardized_return",
        "return_quantile",
        "quantile_regression",
        "log_return",
        "log_return_zscore",
        "state",
    }:
        raise ValueError(
            "regression_label_mode 仅支持 raw_return / standardized_return / return_quantile / quantile_regression / log_return / log_return_zscore / state"
        )
    if regression_train_mode not in {
        "direct",
        "empirical_quantile",
        "return_quantile",
        "quantile_regression",
    }:
        raise ValueError(
            "regression_train_mode 仅支持 direct / empirical_quantile / return_quantile / quantile_regression"
        )
    if regression_train_mode == "return_quantile":
        regression_train_mode = "empirical_quantile"
    run_regression = model_task in {"regression", "both"}
    run_state = model_task in {"state", "both"}
    if label_is_standardized and run_regression and not label_scale_expr:
        raise ValueError("label_is_standardized=True 时必须传入 label_scale_expr")
    if label_is_standardized and run_regression and not raw_label_expr:
        raise ValueError("标准化还原需要传入 raw_label_expr")
    if run_state and not (state_label_expr or raw_label_expr):
        raise ValueError("状态分类需要传入 state_label_expr 或 raw_label_expr")

    total_start = time.perf_counter()
    artifact_path = Path(artifact_dir) if artifact_dir is not None else None
    if artifact_path is not None:
        artifact_path.mkdir(parents=True, exist_ok=True)

    regression_result = None
    raw_return_dataset = None
    label_scale_dataset = None
    quantile_reference = None
    pred_raw_by_segment: dict[str, pd.Series] = {}
    pred_quantile_by_segment: dict[str, pd.Series] = {}
    pred_qreg_by_segment: dict[str, pd.DataFrame] = {}
    pred_qreg_risk_adjusted_by_segment: dict[str, pd.Series] = {}
    qreg_results: dict[Any, dict[str, Any]] = {}
    regression_metrics_rows = []

    if run_regression:
        regression_ts_dataset = ts_dataset
        regression_base_dataset = base_dataset
        if regression_train_mode == "empirical_quantile":
            if not raw_label_expr:
                raise ValueError("empirical_quantile 回归需要传入 raw_label_expr")
            raw_return_dataset = _build_label_dataset(
                dataset_builder,
                dataset_kwargs,
                raw_label_expr,
                feature_placeholder=True,
            )
            quantile_reference = _fit_quantile_reference(
                raw_return_dataset, segment="train"
            )

            def quantile_transform(values):
                return _values_to_train_quantile(values, quantile_reference)

            regression_ts_dataset = LabelTransformDataset(
                ts_dataset, quantile_transform
            )
            regression_base_dataset = LabelTransformDataset(
                base_dataset, quantile_transform
            )

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

        if regression_train_mode == "quantile_regression":
            params_base = dict(model_params or {})
            quantiles = tuple(
                float(q)
                for q in params_base.get(
                    "quantile_regression_quantiles", (0.1, 0.5, 0.9)
                )
            )
            if not quantiles:
                raise ValueError("quantile_regression_quantiles 不能为空")
            if any(q <= 0.0 or q >= 1.0 for q in quantiles):
                raise ValueError(
                    f"quantile_regression_quantiles 必须都在 (0, 1)，当前值为 {quantiles}"
                )
            quantiles = tuple(sorted(dict.fromkeys(quantiles)))
            median_quantile = float(
                params_base.get("quantile_regression_median_quantile", 0.5)
            )
            risk_lower_quantile = float(
                params_base.get("quantile_regression_risk_lower_quantile", quantiles[0])
            )
            risk_upper_quantile = float(
                params_base.get(
                    "quantile_regression_risk_upper_quantile", quantiles[-1]
                )
            )
            output_names = [_q_name(q) for q in quantiles]
            median_output_index = (
                output_names.index(_q_name(median_quantile))
                if _q_name(median_quantile) in output_names
                else len(output_names) // 2
            )
            q_params = dict(params_base)
            q_params["loss"] = "quantile"
            q_params["output_dim"] = len(quantiles)
            q_params["output_names"] = output_names
            q_loss_params = dict(q_params.get("loss_params") or {})
            q_loss_params.update(
                {
                    "quantiles": quantiles,
                    "reduction": q_loss_params.get("reduction", "mean"),
                    "selection_output_index": median_output_index,
                }
            )
            q_params["loss_params"] = q_loss_params
            regression_result = train_predict_alstm(
                ts_dataset=regression_ts_dataset,
                base_dataset=regression_base_dataset,
                d_feat=d_feat,
                model_params=q_params,
                label_scale=label_scale,
                model_save_path=model_save_path,
                artifact_dir=None,
                show_progress=show_progress,
            )
            qreg_results = {"multi_output": regression_result}

            for segment in ("train", "valid", "test"):
                pred_key = {
                    "train": "pred_train",
                    "valid": "pred_valid",
                    "test": "pred",
                }[segment]
                q_df = regression_result[pred_key].copy()
                q_df = q_df.reindex(columns=output_names)
                if label_is_standardized:
                    scale_ser = _label_series(
                        label_scale_dataset, segment, name="scale"
                    )
                    q_df = q_df.mul(scale_ser, axis=0)
                pred_qreg_by_segment[segment] = q_df
                median_col = _q_name(median_quantile)
                if median_col not in q_df.columns:
                    median_col = q_df.columns[len(q_df.columns) // 2]
                pred_raw_by_segment[segment] = pd.to_numeric(
                    q_df[median_col], errors="coerce"
                ).rename("pred_raw_return")
                pred_qreg_risk_adjusted_by_segment[segment] = (
                    _quantile_risk_adjusted_signal(
                        q_df,
                        median_quantile=median_quantile,
                        lower_quantile=risk_lower_quantile,
                        upper_quantile=risk_upper_quantile,
                    )
                )
                raw_true_ser = (
                    _label_series(raw_return_dataset, segment, name="raw_return")
                    if raw_return_dataset is not None
                    else _label_series(base_dataset, segment, name="raw_return")
                )
                row = {"model": "regression_quantile_pinball", "segment": segment}
                row.update(
                    calc_raw_return_metrics(
                        raw_true_ser,
                        pred_raw_by_segment[segment],
                        state_threshold=state_return_threshold,
                        class_names=state_class_names,
                    )
                )
                row.update(
                    {
                        "quantile_regression_quantiles": ",".join(
                            str(q) for q in quantiles
                        ),
                        "median_quantile": median_quantile,
                        "risk_lower_quantile": risk_lower_quantile,
                        "risk_upper_quantile": risk_upper_quantile,
                    }
                )
                if not q_df.empty:
                    for col in q_df.columns:
                        row[f"pred_{col}_mean"] = float(
                            pd.to_numeric(q_df[col], errors="coerce").mean()
                        )
                    ra = (
                        pred_qreg_risk_adjusted_by_segment[segment]
                        .replace([np.inf, -np.inf], np.nan)
                        .dropna()
                    )
                    row["risk_adjusted_signal_mean"] = (
                        float(ra.mean()) if len(ra) else np.nan
                    )
                    row["risk_adjusted_signal_std"] = (
                        float(ra.std()) if len(ra) else np.nan
                    )
                regression_metrics_rows.append(row)
        else:
            regression_result = train_predict_alstm(
                ts_dataset=regression_ts_dataset,
                base_dataset=regression_base_dataset,
                d_feat=d_feat,
                model_params=model_params,
                label_scale=label_scale,
                model_save_path=model_save_path,
                artifact_dir=None,
                show_progress=show_progress,
            )

        for segment, pred_obj in (
            ("train", regression_result["pred_train"]),
            ("valid", regression_result["pred_valid"]),
            ("test", regression_result["pred"]),
        ):
            if regression_train_mode == "quantile_regression":
                continue
            pred_ser = _pred_to_series(pred_obj, name="pred")
            if regression_train_mode == "empirical_quantile":
                if raw_return_dataset is None or quantile_reference is None:
                    raise ValueError(
                        "empirical_quantile 回归缺少原始收益率数据或训练分位数参考"
                    )
                raw_true_ser = _label_series(
                    raw_return_dataset, segment, name="raw_return"
                )
                pred_quantile_ser = pred_ser.clip(0.0, 1.0).rename(
                    "pred_return_quantile"
                )
                pred_eval_ser = pd.Series(
                    _train_quantile_to_values(
                        pred_quantile_ser.to_numpy(dtype=float), quantile_reference
                    ),
                    index=pred_quantile_ser.index,
                    name="pred_raw_return",
                )
                true_quantile_ser = pd.Series(
                    _values_to_train_quantile(
                        raw_true_ser.to_numpy(dtype=float), quantile_reference
                    ),
                    index=raw_true_ser.index,
                    name="true_return_quantile",
                )
                pred_quantile_by_segment[segment] = pred_quantile_ser
                metric_model_name = "regression_empirical_quantile"
            elif label_is_standardized:
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
            if regression_train_mode == "empirical_quantile":
                quantile_aligned = _align_true_pred(
                    true_quantile_ser, pred_quantile_ser
                )
                if quantile_aligned.empty:
                    row.update({"quantile_n": 0})
                else:
                    q_true = quantile_aligned["y_true"]
                    q_pred = quantile_aligned["y_pred"]
                    row.update(
                        {
                            "quantile_n": int(len(quantile_aligned)),
                            "quantile_mse": float(np.mean((q_pred - q_true) ** 2)),
                            "quantile_mae": float(np.mean(np.abs(q_pred - q_true))),
                            "quantile_ic": float(q_pred.corr(q_true, method="pearson")),
                            "quantile_rank_ic": float(
                                q_pred.corr(q_true, method="spearman")
                            ),
                            "pred_quantile_mean": float(q_pred.mean()),
                            "pred_quantile_std": float(q_pred.std()),
                            "pred_quantile_long_ratio_70": float(
                                (q_pred >= 0.70).mean()
                            ),
                            "pred_quantile_short_ratio_30": float(
                                (q_pred <= 0.30).mean()
                            ),
                        }
                    )
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
            dataset_builder,
            dataset_kwargs,
            state_label_expr or raw_label_expr,
            feature_placeholder=False,
        )
        state_sample_weight_by_segment = None
        state_weight_mode = str(
            (model_params or {}).get("state_sample_weight_mode", "none")
        ).lower()
        if state_weight_mode not in {"none", "off", "false", "0"}:
            weight_expr = state_weight_label_expr or raw_label_expr
            if not weight_expr:
                raise ValueError(
                    "state_sample_weight_mode 非 none 时必须传入 state_weight_label_expr 或 raw_label_expr"
                )
            weight_dataset = _build_label_dataset(
                dataset_builder,
                dataset_kwargs,
                weight_expr,
                feature_placeholder=True,
            )
            state_sample_weight_by_segment = {
                segment: _build_state_sample_weight(
                    _label_series(
                        weight_dataset, segment, name="state_weight_raw_return"
                    ),
                    threshold=state_return_threshold,
                    mode=state_weight_mode,
                    power=float(
                        (model_params or {}).get("state_sample_weight_power", 1.0)
                    ),
                    scale=float(
                        (model_params or {}).get("state_sample_weight_scale", 1.0)
                    ),
                    clip=(model_params or {}).get(
                        "state_sample_weight_clip", (0.5, 5.0)
                    ),
                )
                for segment in ("train", "valid")
            }
        ts_freq = str(dataset_kwargs.get("freq", "1min"))
        state_ts_dataset = TSDatasetH(
            handler=state_dataset.handler,
            segments=state_dataset.segments,
            step_len=int(step_len),
            freq=ts_freq,
        )
        _ensure_ts_calendar(
            state_ts_dataset,
            start_time=dataset_kwargs.get(
                "start_time", state_dataset.segments["train"][0]
            ),
            end_time=dataset_kwargs.get("end_time", state_dataset.segments["test"][1]),
            freq=ts_freq,
        )
        state_result = train_state_classifier(
            state_ts_dataset,
            d_feat=d_feat,
            model_params=model_params,
            state_threshold=state_return_threshold,
            state_sample_weight_by_segment=state_sample_weight_by_segment,
            model_save_path=state_model_save_path,
            show_progress=show_progress,
        )

        for segment in ("train", "valid", "test"):
            prob_df, pred_state_ser, true_state_ser, raw_label_ser = (
                predict_state_classifier(
                    state_result["model"],
                    state_ts_dataset,
                    segment=segment,
                    batch_size=int(
                        (model_params or {}).get(
                            "batch_size", DEFAULT_ALSTM_PARAMS["batch_size"]
                        )
                    ),
                    state_threshold=state_return_threshold,
                    class_names=state_class_names,
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
        "regression_label_mode": regression_label_mode,
        "regression_train_mode": regression_train_mode,
        "label_is_standardized": bool(label_is_standardized),
        "quantile_reference": quantile_reference,
        "quantile_regression_results": qreg_results,
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
        "state_best_selection_metric": state_result["best_selection_metric"]
        if run_state
        else None,
        "state_best_selection_score": state_result["best_selection_score"]
        if run_state
        else None,
        "state_best_selection_metric_value": state_result["best_selection_metric_value"]
        if run_state
        else None,
        "state_train_class_counts": state_result["class_counts"] if run_state else None,
        "state_train_class_weights": state_result["class_weights"]
        if run_state
        else None,
        "pred_label_train": regression_result["pred_train"] if run_regression else None,
        "pred_label_valid": regression_result["pred_valid"] if run_regression else None,
        "pred_label": regression_result["pred"] if run_regression else None,
        "pred_quantile_train": pred_quantile_by_segment.get("train")
        if run_regression
        else None,
        "pred_quantile_valid": pred_quantile_by_segment.get("valid")
        if run_regression
        else None,
        "pred_quantile": pred_quantile_by_segment.get("test")
        if run_regression
        else None,
        "pred_qreg_train": pred_qreg_by_segment.get("train")
        if run_regression
        else None,
        "pred_qreg_valid": pred_qreg_by_segment.get("valid")
        if run_regression
        else None,
        "pred_qreg": pred_qreg_by_segment.get("test") if run_regression else None,
        "pred_qreg_by_segment": pred_qreg_by_segment if run_regression else {},
        "pred_qreg_risk_adjusted_train": pred_qreg_risk_adjusted_by_segment.get("train")
        if run_regression
        else None,
        "pred_qreg_risk_adjusted_valid": pred_qreg_risk_adjusted_by_segment.get("valid")
        if run_regression
        else None,
        "pred_qreg_risk_adjusted": pred_qreg_risk_adjusted_by_segment.get("test")
        if run_regression
        else None,
        "pred_qreg_risk_adjusted_by_segment": pred_qreg_risk_adjusted_by_segment
        if run_regression
        else {},
        "pred_raw_by_segment": pred_raw_by_segment if run_regression else {},
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
        pred_label_test_path = artifact_path / f"{artifact_prefix}_pred_test_label.pkl"
        pred_quantile_test_path = (
            artifact_path / f"{artifact_prefix}_pred_test_return_quantile.pkl"
        )
        pred_quantile_test_csv = (
            artifact_path / f"{artifact_prefix}_pred_test_return_quantile.csv"
        )
        pred_qreg_test_path = (
            artifact_path / f"{artifact_prefix}_pred_test_quantile_regression.pkl"
        )
        pred_qreg_test_csv = (
            artifact_path / f"{artifact_prefix}_pred_test_quantile_regression.csv"
        )
        pred_qreg_risk_test_path = (
            artifact_path / f"{artifact_prefix}_pred_test_quantile_risk_adjusted.pkl"
        )
        pred_qreg_risk_test_csv = (
            artifact_path / f"{artifact_prefix}_pred_test_quantile_risk_adjusted.csv"
        )
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
            if pred_quantile_by_segment.get("test") is not None:
                pred_quantile_by_segment["test"].to_pickle(pred_quantile_test_path)
                pred_quantile_by_segment["test"].to_csv(
                    pred_quantile_test_csv, encoding="utf-8-sig"
                )
            if pred_qreg_by_segment.get("test") is not None:
                pred_qreg_by_segment["test"].to_pickle(pred_qreg_test_path)
                pred_qreg_by_segment["test"].to_csv(
                    pred_qreg_test_csv, encoding="utf-8-sig"
                )
            if pred_qreg_risk_adjusted_by_segment.get("test") is not None:
                pred_qreg_risk_adjusted_by_segment["test"].to_pickle(
                    pred_qreg_risk_test_path
                )
                pred_qreg_risk_adjusted_by_segment["test"].to_csv(
                    pred_qreg_risk_test_csv, encoding="utf-8-sig"
                )
            pred.to_pickle(pred_raw_test_path)
            pred.to_csv(pred_raw_test_csv, encoding="utf-8-sig")
            result.update(
                {
                    "pred_label_test_path": str(pred_label_test_path),
                    "pred_quantile_test_path": str(pred_quantile_test_path)
                    if pred_quantile_by_segment.get("test") is not None
                    else None,
                    "pred_quantile_test_csv": str(pred_quantile_test_csv)
                    if pred_quantile_by_segment.get("test") is not None
                    else None,
                    "pred_qreg_test_path": str(pred_qreg_test_path)
                    if pred_qreg_by_segment.get("test") is not None
                    else None,
                    "pred_qreg_test_csv": str(pred_qreg_test_csv)
                    if pred_qreg_by_segment.get("test") is not None
                    else None,
                    "pred_qreg_risk_test_path": str(pred_qreg_risk_test_path)
                    if pred_qreg_risk_adjusted_by_segment.get("test") is not None
                    else None,
                    "pred_qreg_risk_test_csv": str(pred_qreg_risk_test_csv)
                    if pred_qreg_risk_adjusted_by_segment.get("test") is not None
                    else None,
                    "pred_raw_test_path": str(pred_raw_test_path),
                    "pred_raw_test_csv": str(pred_raw_test_csv),
                }
            )
        if run_state:
            state_prob_by_segment["test"].to_pickle(state_prob_test_path)
            state_pred_by_segment["test"].to_pickle(state_pred_test_path)
            result.update(
                {
                    "state_prob_test_path": str(state_prob_test_path),
                    "state_pred_test_path": str(state_pred_test_path),
                }
            )
        model_compare_metrics_df.to_csv(metrics_path, index=False, encoding="utf-8-sig")
        result.update(
            {
                "metrics_path": str(metrics_path),
            }
        )

    return result
