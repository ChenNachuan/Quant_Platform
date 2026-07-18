from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from qlib.contrib.model.pytorch_alstm_ts import ALSTMModel, ConcatDataset, DataLoader
from qlib.data.dataset.handler import DataHandlerLP

from engine.models.alstm import (
    _regression_epoch_selection_metrics,
    _regression_selection_score,
)


class DistributionALSTMModel(ALSTMModel):
    """ALSTM with Hansen skewed-t distribution output: mu, scale, skew, nu."""

    def __init__(
        self,
        *args,
        scale_floor: float = 1e-4,
        log_scale_min: float = -10.0,
        log_scale_max: float = 5.0,
        skew_max: float = 0.99,
        nu_min: float = 2.05,
        nu_max: float = 80.0,
        **kwargs,
    ):
        self.scale_floor = float(scale_floor)
        self.log_scale_min = float(log_scale_min)
        self.log_scale_max = float(log_scale_max)
        self.skew_max = float(skew_max)
        self.nu_min = float(nu_min)
        self.nu_max = float(nu_max)
        super().__init__(*args, **kwargs)

    def _build_model(self):
        super()._build_model()
        self.fc_out = nn.Linear(in_features=self.hid_size * 2, out_features=4)

    def forward(self, inputs):
        rnn_out, _ = self.rnn(self.net(inputs))
        attention_score = self.att_net(rnn_out)
        out_att = torch.mul(rnn_out, attention_score).sum(dim=1)
        raw = self.fc_out(torch.cat((rnn_out[:, -1, :], out_att), dim=1))
        mu = raw[:, 0]
        log_scale = torch.log(F.softplus(raw[:, 1]) + self.scale_floor).clamp(
            min=self.log_scale_min,
            max=self.log_scale_max,
        )
        skew = torch.tanh(raw[:, 2]) * self.skew_max
        nu = (F.softplus(raw[:, 3]) + self.nu_min).clamp(max=self.nu_max)
        return mu, log_scale, skew, nu


def normalize_qlib_index(index):
    if isinstance(index, pd.MultiIndex):
        names = list(index.names)
        if (
            "datetime" in names
            and "instrument" in names
            and names.index("datetime") > names.index("instrument")
        ):
            return index.reorder_levels(["datetime", "instrument"]).sort_values()
    return index


def hansen_skewt_logpdf(
    x,
    mu,
    log_scale,
    skew,
    nu,
    *,
    scale_floor: float = 1e-4,
    skew_max: float = 0.99,
    nu_min: float = 2.05,
    nu_max: float = 80.0,
):
    scale = torch.exp(log_scale).clamp_min(float(scale_floor))
    z = (x - mu) / scale
    skew = skew.clamp(min=-float(skew_max), max=float(skew_max))
    nu = nu.clamp(min=float(nu_min), max=float(nu_max))

    log_c = (
        torch.lgamma((nu + 1.0) / 2.0)
        - torch.lgamma(nu / 2.0)
        - 0.5
        * torch.log(torch.as_tensor(np.pi, dtype=x.dtype, device=x.device) * (nu - 2.0))
    )
    c = torch.exp(log_c)
    a = 4.0 * skew * c * (nu - 2.0) / (nu - 1.0)
    b_sq = 1.0 + 3.0 * skew.square() - a.square()
    b = torch.sqrt(torch.clamp(b_sq, min=1e-8))

    threshold = -a / b
    left = z < threshold
    denom = torch.where(left, 1.0 - skew, 1.0 + skew).clamp_min(1e-6)
    inner = ((b * z + a) / denom).square()
    log_kernel = -0.5 * (nu + 1.0) * torch.log1p(inner / (nu - 2.0))
    return torch.log(b.clamp_min(1e-8)) + log_c + log_kernel - log_scale


def skewt_nll_loss(
    mu,
    log_scale,
    skew,
    nu,
    target,
    *,
    scale_penalty: float = 0.0,
    weight=None,
    scale_floor: float = 1e-4,
    skew_max: float = 0.99,
    nu_min: float = 2.05,
    nu_max: float = 80.0,
):
    log_pdf = hansen_skewt_logpdf(
        target,
        mu,
        log_scale,
        skew,
        nu,
        scale_floor=scale_floor,
        skew_max=skew_max,
        nu_min=nu_min,
        nu_max=nu_max,
    )
    loss = -log_pdf
    if float(scale_penalty) > 0:
        loss = loss + float(scale_penalty) * torch.exp(log_scale)
    if weight is not None:
        loss = loss * weight
    return loss.mean()


def _make_loader(
    ts_dataset,
    segment: str,
    *,
    batch_size: int,
    num_workers: int,
    shuffle: bool,
    drop_last: bool,
):
    data = ts_dataset.prepare(
        segment, col_set=["feature", "label"], data_key=DataHandlerLP.DK_L
    )
    if data.empty:
        raise ValueError(f"{segment} segment is empty")
    data.config(fillna_type="ffill+bfill")
    weight = np.ones(len(data), dtype=np.float32)
    return DataLoader(
        ConcatDataset(data, weight),
        batch_size=int(batch_size),
        shuffle=bool(shuffle),
        num_workers=int(num_workers),
        drop_last=bool(drop_last),
    )


def _run_epoch(model, loader, optimizer, device, params):
    train = optimizer is not None
    model.train(train)
    losses = []
    for data, weight in loader:
        feature = data[:, :, 0:-1].to(device).float()
        target = data[:, -1, -1].to(device).float() * float(params["label_scale"])
        weight = weight.to(device).float()
        mu, log_scale, skew, nu = model(feature)
        loss = skewt_nll_loss(
            mu,
            log_scale,
            skew,
            nu,
            target,
            scale_penalty=float(params["scale_penalty"]),
            weight=weight,
            scale_floor=float(params["scale_floor"]),
            skew_max=float(params["skew_max"]),
            nu_min=float(params["nu_min"]),
            nu_max=float(params["nu_max"]),
        )
        if train:
            optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_value_(
                model.parameters(), float(params["grad_clip_value"])
            )
            optimizer.step()
        losses.append(float(loss.detach().cpu().item()))
    return float(np.mean(losses)) if losses else np.nan


def _predict_mu_label_for_selection(model, loader, device, label_scale: float):
    model.eval()
    pred_list, label_list = [], []
    with torch.no_grad():
        for data, _weight in loader:
            feature = data[:, :, 0:-1].to(device).float()
            label = data[:, -1, -1].to(device).float()
            mu_scaled, _log_scale, _skew, _nu = model(feature)
            pred_list.append(
                (mu_scaled / float(label_scale)).detach().cpu().numpy().reshape(-1)
            )
            label_list.append(label.detach().cpu().numpy().reshape(-1))
    if not pred_list:
        return np.array([], dtype=float), np.array([], dtype=float)
    return np.concatenate(pred_list).astype(float), np.concatenate(label_list).astype(
        float
    )


def _predict_distribution(
    model,
    ts_dataset,
    segment: str,
    *,
    batch_size: int,
    num_workers: int,
    device,
    label_name: str,
    label_scale: float,
    scale_floor: float,
):
    data = ts_dataset.prepare(
        segment, col_set=["feature", "label"], data_key=DataHandlerLP.DK_I
    )
    data.config(fillna_type="ffill+bfill")
    loader = DataLoader(
        data, batch_size=int(batch_size), num_workers=int(num_workers), shuffle=False
    )
    model.eval()
    mu_list, scale_list, skew_list, nu_list = [], [], [], []
    with torch.no_grad():
        for batch in loader:
            feature = batch[:, :, 0:-1].to(device).float()
            mu_scaled, log_scale, skew, nu = model(feature)
            scale_scaled = torch.exp(log_scale).clamp_min(float(scale_floor))
            mu_list.append((mu_scaled / float(label_scale)).detach().cpu().numpy())
            scale_list.append(
                (scale_scaled / float(label_scale)).detach().cpu().numpy()
            )
            skew_list.append(skew.detach().cpu().numpy())
            nu_list.append(nu.detach().cpu().numpy())
    idx = normalize_qlib_index(data.get_index())
    mu = pd.Series(np.concatenate(mu_list), index=idx, name=f"pred_mu_{label_name}")
    scale = pd.Series(
        np.concatenate(scale_list), index=idx, name=f"pred_scale_{label_name}"
    )
    skew = pd.Series(
        np.concatenate(skew_list), index=idx, name=f"pred_skew_{label_name}"
    )
    nu = pd.Series(np.concatenate(nu_list), index=idx, name=f"pred_nu_{label_name}")
    confidence = (mu / (scale + 1e-12)).rename(f"pred_confidence_{label_name}")
    return pd.concat([mu, scale, skew, nu, confidence], axis=1)


def _selection_params(params: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "regression_selection_min_abs_pred": params.get(
            "distribution_selection_min_abs_pred", 0.0
        ),
        "regression_selection_min_trade_ratio": params.get(
            "distribution_selection_min_trade_ratio", 0.05
        ),
        "regression_selection_max_trade_ratio": params.get(
            "distribution_selection_max_trade_ratio", 1.0
        ),
        "regression_selection_min_trade_count": params.get(
            "distribution_selection_min_trade_count", 20
        ),
    }


def _normalize_selection_metric(metric):
    metric = str(metric or "valid_nll").lower()
    return "valid_loss" if metric in {"valid_nll", "nll"} else metric


def train_predict_distribution_nll_alstm(
    *,
    ts_dataset,
    d_feat: int,
    step_len: int,
    model_params: Mapping[str, Any] | None = None,
    artifact_dir: str | Path | None = None,
    artifact_prefix: str = "distribution_nll",
    show_progress: bool = True,
) -> dict[str, Any]:
    """Train ALSTM skewed-t NLL model and return distribution confidence signals."""
    params = {
        "hidden_size": 32,
        "num_layers": 2,
        "dropout": 0.3,
        "n_epochs": 30,
        "lr": 1e-3,
        "batch_size": 256,
        "early_stop": 8,
        "GPU": 0,
        "seed": 42,
        "rnn_type": "GRU",
        "num_workers": 0,
        "drop_last": True,
        "label_name": "log_return",
        "label_scale": 1.0,
        "scale_floor": 1e-4,
        "log_scale_min": -10.0,
        "log_scale_max": 5.0,
        "nu_min": 2.05,
        "nu_max": 80.0,
        "skew_max": 0.99,
        "scale_penalty": 0.0,
        "grad_clip_value": 3.0,
        "distribution_selection_metric": "valid_sharpe_constrained",
        "distribution_selection_min_abs_pred": 0.0,
        "distribution_selection_min_trade_ratio": 0.05,
        "distribution_selection_max_trade_ratio": 1.0,
        "distribution_selection_min_trade_count": 20,
    }
    params.update(dict(model_params or {}))
    torch.manual_seed(int(params["seed"]))
    np.random.seed(int(params["seed"]))
    device = torch.device(
        f"cuda:{int(params['GPU'])}"
        if int(params["GPU"]) >= 0 and torch.cuda.is_available()
        else "cpu"
    )

    train_loader = _make_loader(
        ts_dataset,
        "train",
        batch_size=int(params["batch_size"]),
        num_workers=int(params["num_workers"]),
        shuffle=True,
        drop_last=bool(params["drop_last"]),
    )
    valid_loader = _make_loader(
        ts_dataset,
        "valid",
        batch_size=int(params["batch_size"]),
        num_workers=int(params["num_workers"]),
        shuffle=False,
        drop_last=False,
    )
    valid_selection_index = ts_dataset.prepare(
        "valid", col_set=["feature", "label"], data_key=DataHandlerLP.DK_L
    ).get_index()
    selection_metric = _normalize_selection_metric(
        params["distribution_selection_metric"]
    )

    model = DistributionALSTMModel(
        d_feat=int(d_feat),
        hidden_size=int(params["hidden_size"]),
        num_layers=int(params["num_layers"]),
        dropout=float(params["dropout"]),
        rnn_type=str(params["rnn_type"]),
        scale_floor=float(params["scale_floor"]),
        log_scale_min=float(params["log_scale_min"]),
        log_scale_max=float(params["log_scale_max"]),
        skew_max=float(params["skew_max"]),
        nu_min=float(params["nu_min"]),
        nu_max=float(params["nu_max"]),
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=float(params["lr"]))

    best_state = copy.deepcopy(model.state_dict())
    best_score = -np.inf
    best_valid_nll = float("inf")
    best_epoch = 0
    bad_epochs = 0
    history_rows = []

    epoch_iter = range(1, int(params["n_epochs"]) + 1)
    if show_progress:
        from tqdm.auto import tqdm

        epoch_iter = tqdm(
            epoch_iter, desc=f"{artifact_prefix} skewed-t NLL", dynamic_ncols=True
        )

    for epoch in epoch_iter:
        train_loss = _run_epoch(model, train_loader, optimizer, device, params)
        valid_loss = _run_epoch(model, valid_loader, None, device, params)
        valid_mu, valid_label = _predict_mu_label_for_selection(
            model, valid_loader, device, float(params["label_scale"])
        )
        selection_eval = _regression_epoch_selection_metrics(
            valid_mu,
            valid_label,
            valid_selection_index,
            params=_selection_params(params),
        )
        selection_eval["valid_loss"] = float(valid_loss)
        score = _regression_selection_score(selection_eval, metric=selection_metric)
        if not np.isfinite(score):
            score = -float(valid_loss) if np.isfinite(valid_loss) else -np.inf
        history_rows.append(
            {
                "epoch": epoch,
                "train_nll": train_loss,
                "valid_nll": valid_loss,
                "valid_selection_score": score,
                "valid_ic": selection_eval.get("valid_ic", np.nan),
                "valid_rank_ic": selection_eval.get("valid_rank_ic", np.nan),
                "valid_icir": selection_eval.get("valid_icir", np.nan),
                "valid_rank_icir": selection_eval.get("valid_rank_icir", np.nan),
                "valid_simple_sharpe": selection_eval.get(
                    "valid_simple_sharpe", np.nan
                ),
                "valid_trade_ratio": selection_eval.get("valid_trade_ratio", np.nan),
                "valid_trade_count": selection_eval.get("valid_trade_count", np.nan),
            }
        )
        if show_progress and hasattr(epoch_iter, "set_postfix"):
            epoch_iter.set_postfix(
                {
                    "train_nll": f"{train_loss:.6f}",
                    "valid_nll": f"{valid_loss:.6f}",
                    "select": f"{score:.6f}",
                }
            )
        if score > best_score:
            best_score = float(score)
            best_valid_nll = float(valid_loss)
            best_epoch = int(epoch)
            best_state = copy.deepcopy(model.state_dict())
            bad_epochs = 0
        else:
            bad_epochs += 1
            if bad_epochs >= int(params["early_stop"]):
                break

    model.load_state_dict(best_state)
    pred_by_segment = {
        seg: _predict_distribution(
            model,
            ts_dataset,
            seg,
            batch_size=int(params["batch_size"]),
            num_workers=int(params["num_workers"]),
            device=device,
            label_name=str(params["label_name"]),
            label_scale=float(params["label_scale"]),
            scale_floor=float(params["scale_floor"]),
        )
        for seg in ("train", "valid", "test")
    }
    signal_by_segment = {
        seg: pred_by_segment[seg][f"pred_confidence_{params['label_name']}"].rename(
            "distribution_confidence"
        )
        for seg in ("train", "valid", "test")
    }
    history_df = pd.DataFrame(history_rows)

    if artifact_dir is not None:
        artifact_path = Path(artifact_dir)
        artifact_path.mkdir(parents=True, exist_ok=True)
        torch.save(best_state, artifact_path / f"{artifact_prefix}_skewt_nll_best.pt")
        history_df.to_csv(
            artifact_path / f"{artifact_prefix}_skewt_nll_history.csv",
            index=False,
            encoding="utf-8-sig",
        )
        for seg, pred_df in pred_by_segment.items():
            pred_df.to_pickle(
                artifact_path / f"{artifact_prefix}_pred_distribution_{seg}.pkl"
            )
            pred_df.to_csv(
                artifact_path / f"{artifact_prefix}_pred_distribution_{seg}.csv",
                encoding="utf-8-sig",
            )

    return {
        "model": model,
        "history": history_df,
        "best_epoch": best_epoch,
        "best_valid_nll": best_valid_nll,
        "best_selection_score": best_score,
        "pred_distribution_by_segment": pred_by_segment,
        "signal_by_segment": signal_by_segment,
        "params": params,
    }
