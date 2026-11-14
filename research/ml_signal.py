"""ML/DL 信号治理层：信号尺度适配、平滑、特征矩阵与版本指纹、落盘、集成与过拟合检验。

核心契约：
- ``MLSignalAdapter`` 将原始分数或概率统一转换为截面 z-score（``[date, ticker, score]``），
  概率先截断至 ``[1e-6, 1-1e-6]`` 再做 logit 变换；零标准差截面输出全 0。
- ``smooth_signal`` 按标的时序做指数加权移动平均（默认 ``halflife=5``）。
- ``features_hash`` 与 ``model_id`` 锚定特征集合与模型版本；``write_model`` 拒绝写入 ``REJECT`` 模型。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from research.alpha_signal import _cross_zscore  # noqa: PLC2701
from research.post_processor import standardize_zscore
from core.config import DATA_ROOT

__all__ = [
    "MLSignalAdapter",
    "VERDICTS",
    "build_feature_matrix",
    "ensemble_models",
    "features_hash",
    "model_id",
    "sanity_check_ml_model",
    "smooth_signal",
    "transform_bars",
    "write_experiment",
    "write_model",
]

#: ``from_probabilities`` 概率截断边界，确保 logit 有限且保序。
PROB_CLIP_LOW: float = 1e-6
PROB_CLIP_HIGH: float = 1.0 - 1e-6

#: ``smooth_signal`` 默认半衰期（5 个交易日）。
DEFAULT_HALFLIFE: int = 5

#: 信号输出标准列契约。
REQUIRED_COLS: tuple[str, ...] = ("date", "ticker", "score")


def _validate_frame(frame: pd.DataFrame, *, score_col: str = "score") -> None:
    """校验输入包含 ``[date, ticker, <score_col>]`` 且无重复 ``(date, ticker)`` 行。"""
    required = ("date", "ticker", score_col)
    missing = [c for c in required if c not in frame.columns]
    if missing:
        raise ValueError(f"frame is missing columns {missing}")
    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("frame has duplicate (date, ticker) rows")


class MLSignalAdapter:
    """将 ML 模型原始输出适配为统一的截面 z-score 信号 ``[date, ticker, score]``。"""

    @staticmethod
    def from_scores(scores: pd.DataFrame) -> pd.DataFrame:
        """将原始分数 ``[date, ticker, score]`` 转为按日截面 z-score，零标准差日期输出全 0。"""
        _validate_frame(scores)
        frame = scores[list(REQUIRED_COLS)].copy()
        return _cross_zscore(frame)

    @staticmethod
    def from_probabilities(probs: pd.DataFrame) -> pd.DataFrame:
        """将分类概率 ``[date, ticker, prob]`` 经截断至 ``[1e-6, 1-1e-6]``、logit 变换后转为按日截面 z-score。"""
        _validate_frame(probs, score_col="prob")
        frame = probs[["date", "ticker", "prob"]].copy()
        p = frame["prob"].astype("float64").clip(PROB_CLIP_LOW, PROB_CLIP_HIGH)
        frame["score"] = np.log(p / (1.0 - p))
        frame = frame[["date", "ticker", "score"]]
        return _cross_zscore(frame)


def smooth_signal(
    frame: pd.DataFrame,
    halflife: int = DEFAULT_HALFLIFE,
) -> pd.DataFrame:
    """按标的时间顺序对 ``score`` 列做指数加权移动平均（EWM），保持原行序与形状不变。"""
    _validate_frame(frame)
    if halflife <= 0:
        raise ValueError(f"halflife must be positive, got {halflife!r}")

    out = frame.copy()
    # 按标的时间顺序计算 EWM 后还原原始行索引顺序
    orig_index = out.index
    sorted_df = out.sort_values(["ticker", "date"])
    smoothed = sorted_df.groupby("ticker")["score"].transform(
        lambda s: s.ewm(halflife=halflife).mean()
    )
    out.loc[sorted_df.index, "score"] = smoothed
    return out.loc[orig_index]


# ---------------------------------------------------------------------------
# transform_bars
# ---------------------------------------------------------------------------

#: 支持的原始行情特征变换类型。
TRANSFORMS: tuple[str, ...] = ("pct_change_N", "zscore_cs")


def transform_bars(
    bars: pd.DataFrame,
    field: str,
    transform: str,
) -> pd.Series:
    """对行情数据 ``bars`` 的指定字段 ``field`` 执行 ``pct_change_N``（N 期涨跌幅）或 ``zscore_cs``（按日截面 z-score）变换。"""
    if field not in bars.columns:
        raise ValueError(f"bars is missing column {field!r}")
    if transform == "zscore_cs":
        return bars.groupby("date")[field].transform(standardize_zscore)
    if transform.startswith("pct_change_"):
        n_str = transform.removeprefix("pct_change_")
        try:
            n = int(n_str)
        except ValueError:
            raise ValueError(f"invalid pct_change transform: {transform!r}") from None
        if n <= 0:
            raise ValueError(f"pct_change period must be positive, got {n}")
        result = (
            bars.sort_values(["ticker", "date"])
            .groupby("ticker")[field]
            .transform(lambda s: s.pct_change(n))
        )
        return result.reindex(bars.index)
    raise ValueError(f"unknown transform: {transform!r} (expected one of {TRANSFORMS})")


# ---------------------------------------------------------------------------
# build_feature_matrix
# ---------------------------------------------------------------------------


def build_feature_matrix(
    specs: list[dict],
    *,
    start,
    end,
    factor_base_dir: str = str(DATA_ROOT / "factors"),
    bars: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """按特征规格列表 ``specs`` 构建外连接特征宽表 ``[date, ticker, feat_0, feat_1, ...]``。

    每条规格为 ``{source: "factor"|"raw", name|field: str, transform: str}``：
    ``factor`` 从因子存储读取并变换；``raw`` 从 ``bars`` 变换并按 ``[start, end]`` 过滤。
    """
    if not specs:
        raise ValueError("specs must be a non-empty list")

    from research.factor_storage import load_factor  # 延迟导入以避免循环依赖

    frames: list[pd.DataFrame] = []
    for i, spec in enumerate(specs):
        source = spec.get("source")
        transform = spec.get("transform")
        if source == "factor":
            name = spec.get("name")
            if not name:
                raise ValueError(f"spec[{i}] source=factor requires 'name'")
            df = load_factor(name, start, end, base_dir=factor_base_dir)
            if df.empty:
                raise ValueError(
                    f"factor {name!r} returned no data for [{start}, {end}]"
                )
            col_name = f"feat_{i}"
            df = df.rename(columns={name: col_name})
            if transform and transform != "none":
                df[col_name] = transform_bars(df, col_name, transform)
            frames.append(df[["date", "ticker", col_name]])
        elif source == "raw":
            field = spec.get("field")
            if not field:
                raise ValueError(f"spec[{i}] source=raw requires 'field'")
            if bars is None:
                raise ValueError("bars is required for source=raw features")
            col_name = f"feat_{i}"
            raw = bars[["date", "ticker", field]].copy()
            if transform and transform != "none":
                raw[col_name] = transform_bars(bars, field, transform)
            else:
                raw[col_name] = raw[field]
            # factor 分支已由 load_factor 过滤区间，raw 分支需显式裁剪
            raw_dates = pd.to_datetime(raw["date"])
            raw = raw[
                (raw_dates >= pd.to_datetime(start))
                & (raw_dates <= pd.to_datetime(end))
            ]
            frames.append(raw[["date", "ticker", col_name]])
        else:
            raise ValueError(f"spec[{i}] has unknown source: {source!r}")

    # 统一日期为 pd.Timestamp，避免 load_factor 的 date32 与 bars 日期类型混用导致合并异常
    frames = [f.assign(date=pd.to_datetime(f["date"])) for f in frames]

    merged = frames[0]
    for f in frames[1:]:
        merged = merged.merge(f, on=["date", "ticker"], how="outer")
    return merged.sort_values(["date", "ticker"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# features_hash
# ---------------------------------------------------------------------------


def _canonical_spec_key(spec: dict) -> tuple:
    """特征规格排序键：``(source, name|field, transform)``。"""
    return (
        spec.get("source", ""),
        spec.get("name", spec.get("field", "")),
        spec.get("transform", ""),
    )


def features_hash(specs: list[dict], dep_hashes: list[str]) -> str:
    """计算特征集 64 位 SHA-256 指纹：对排序后的规范规格 JSON 与去重排序的依赖指纹哈希。"""
    sorted_specs = sorted(specs, key=_canonical_spec_key)
    canonical_json = json.dumps(sorted_specs, sort_keys=True, separators=(",", ":"))
    sorted_deps = sorted(set(dep_hashes))
    raw = canonical_json + "|" + "|".join(sorted_deps)
    return hashlib.sha256(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# model_id
# ---------------------------------------------------------------------------


def model_id(
    name: str,
    major: int,
    minor: int,
    train_date: str,
    train_end: str,
    feat_hash: str,
) -> str:
    """按 ``{name}+v{major}.{minor}({train_date})+{train_end}+{feat_hash}`` 格式构建模型唯一标识。"""
    return f"{name}+v{major}.{minor}({train_date})+{train_end}+{feat_hash}"


# ---------------------------------------------------------------------------
# write_model
# ---------------------------------------------------------------------------


def _jsonable(value):
    """将 NumPy/Pandas 标量及非有限浮点数转为可 JSON 序列化的原生类型。"""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def write_model(
    name: str,
    major: int,
    minor: int,
    model,
    config: dict,
    metadata: dict,
    *,
    verdict: str = "PASS",
    base_dir: str = str(DATA_ROOT / "models"),
) -> Path:
    """将模型（``model.pkl``）、超参（``config.json``）与元数据（``metadata.json``）写入 ``{base_dir}/{name}/v{major}_{minor}/``；拒绝 ``REJECT`` 结论的模型。"""
    verdict = str(verdict).strip().upper()
    if verdict not in VERDICTS:
        raise ValueError(f"verdict must be one of {VERDICTS}, got {verdict!r}")
    if verdict == "REJECT":
        raise ValueError("REJECT models never enter data/models/ (red line)")

    out_dir = Path(base_dir) / name / f"v{major}_{minor}"
    out_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, out_dir / "model.pkl")

    config_path = out_dir / "config.json"
    config_path.write_text(
        json.dumps(_jsonable(config), indent=2, sort_keys=True), encoding="utf-8"
    )

    meta_with_verdict = {**metadata, "verdict": verdict}
    meta_path = out_dir / "metadata.json"
    meta_path.write_text(
        json.dumps(_jsonable(meta_with_verdict), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return out_dir


# ---------------------------------------------------------------------------
# write_experiment
# ---------------------------------------------------------------------------


def write_experiment(
    exp_id: str,
    metrics: dict,
    config: dict,
    *,
    base_dir: str = str(DATA_ROOT / "experiments"),
) -> Path:
    """将实验指标（``metrics.json``）与配置（``config.json``）写入 ``{base_dir}/{exp_id}/``。"""
    out_dir = Path(base_dir) / exp_id
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics_payload = {**metrics, "created_at": datetime.now(timezone.utc).isoformat()}
    (out_dir / "metrics.json").write_text(
        json.dumps(_jsonable(metrics_payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (out_dir / "config.json").write_text(
        json.dumps(_jsonable(config), indent=2, sort_keys=True), encoding="utf-8"
    )

    return out_dir


# ---------------------------------------------------------------------------
# ensemble_models
# ---------------------------------------------------------------------------


def ensemble_models(
    models: list,
    X=None,
    *,
    weights: list[float] | None = None,
) -> pd.DataFrame:
    """按 ``(date, ticker)`` 外连接多个模型输出并计算加权均值（自动忽略缺失单元格，默认等权）。"""
    if not models:
        raise ValueError("models must be a non-empty list")
    if weights is not None and len(weights) != len(models):
        raise ValueError(
            f"weights length {len(weights)} != models length {len(models)}"
        )

    w = [1.0] * len(models) if weights is None else [float(v) for v in weights]
    for i, wi in enumerate(w):
        if not np.isfinite(wi) or wi <= 0:
            raise ValueError(f"weight[{i}] must be finite and positive, got {wi!r}")

    frames: list[pd.DataFrame] = []
    for model in models:
        result = model(X)
        if not isinstance(result, pd.DataFrame):
            raise TypeError(f"model must return DataFrame, got {type(result).__name__}")
        _validate_frame(result)
        frames.append(result[["date", "ticker", "score"]].copy())

    frames = [f.assign(date=pd.to_datetime(f["date"])) for f in frames]

    merged = frames[0].rename(columns={"score": "score_0"})
    for i, f in enumerate(frames[1:], start=1):
        merged = merged.merge(
            f.rename(columns={"score": f"score_{i}"}),
            on=["date", "ticker"],
            how="outer",
        )

    score_cols = [f"score_{i}" for i in range(len(models))]
    vals = merged[score_cols].to_numpy(dtype="float64")
    w_arr = np.array(w, dtype="float64")

    mask = ~np.isnan(vals)
    vals_clean = np.where(mask, vals, 0.0)
    w_masked = np.where(mask, w_arr, 0.0)
    denom = w_masked.sum(axis=1)
    numer = (vals_clean * w_arr).sum(axis=1)
    score = np.where(denom > 0, numer / denom, np.nan)

    merged["score"] = score
    return merged[["date", "ticker", "score"]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# sanity_check_ml_model
# ---------------------------------------------------------------------------

#: 过拟合门禁返回的三种结论。
VERDICTS: tuple[str, ...] = ("PASS", "SUSPECT", "REJECT")


def sanity_check_ml_model(
    test_metrics: dict,
    train_ic: float,
    baseline_sharpe: float,
) -> str:
    """ML 模型过拟合三项门禁（``REJECT`` 优先于 ``SUSPECT``）：

    1. ``sharpe <= 1.1 * baseline_sharpe`` → ``REJECT``（夏普未超等权基线 10%）
    2. ``sharpe > 2.0`` → ``SUSPECT``（排查前视偏差）
    3. ``ic < 0.5 * train_ic`` → ``SUSPECT``（排查数据泄漏）
    任意核心指标缺失或非有限值时抛出 ``ValueError``。
    """
    sharpe = test_metrics.get("sharpe")
    ic = test_metrics.get("ic")
    if sharpe is None:
        raise ValueError("test_metrics must carry 'sharpe'")
    if ic is None:
        raise ValueError("test_metrics must carry 'ic'")
    for label, value in [
        ("sharpe", sharpe),
        ("ic", ic),
        ("train_ic", train_ic),
        ("baseline_sharpe", baseline_sharpe),
    ]:
        if not np.isfinite(value):
            raise ValueError(f"{label} must be a finite number, got {value!r}")

    if sharpe <= 1.1 * baseline_sharpe:
        return "REJECT"

    if sharpe > 2.0 or ic < 0.5 * train_ic:
        return "SUSPECT"

    return "PASS"
