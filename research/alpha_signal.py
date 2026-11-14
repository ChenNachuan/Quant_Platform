"""统一信号层：AlphaSignal 与分数到无约束目标权重的转换。

职责边界：
1. ``AlphaSignal``：不可变信号载体，``frame`` 为按日截面 z-score 后的 ``[date, ticker, score]``。
2. 双路径聚合：``from_multi_factor``（接收 ``[date, ticker, combined_score]``）与
   ``from_ml_factor``（接收 ``[date, ticker, score]``），统一标准化尺度。
3. ``signal_to_weights``：支持 ``top_k``（等权）与 ``proportional``（负分截断为 0；全 0 回退 ``1/n`` 等权），
   输出无约束初始权重并支持归档至 ``cross/`` 与 ``timeseries/``。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from research.post_processor import standardize_zscore
from core.config import DATA_ROOT

__all__ = [
    "AlphaSignal",
    "DEFAULT_K",
    "WEIGHT_METHODS",
    "signal_to_weights",
    "write_signal",
]

# top_k 默认选股数量
DEFAULT_K: int = 20

# 允许的权重生成方法（top_k 为主路径，proportional 为按分加权备选）
WEIGHT_METHODS: tuple[str, ...] = ("top_k", "proportional")

# 信号表必需列契约
REQUIRED_FRAME_COLS: tuple[str, ...] = ("date", "ticker", "score")

# 日终 cross 归档表列契约
CROSS_COLS: tuple[str, ...] = (
    "signal_time",
    "ticker",
    "target_weight",
    "score",
    "strategy_id",
    "trade_date",
)

# 日终 timeseries 归档表列契约
TIMESERIES_COLS: tuple[str, ...] = (
    "datetime",
    "ticker",
    "direction",
    "strength",
    "quantity",
    "factor_snapshot",
    "reason",
)

# 权重和上限容差，用于吸收 1/k 累加浮点误差
WEIGHT_SUM_TOL: float = 1e-9


# ---------------------------------------------------------------------------
# 信号表辅助函数
# ---------------------------------------------------------------------------


def _validate_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """校验 ``[date, ticker, score]`` 列存在且 ``(date, ticker)`` 无重复，原样返回。"""
    missing = [c for c in REQUIRED_FRAME_COLS if c not in frame.columns]
    if missing:
        raise ValueError(f"signal frame is missing columns {missing}")
    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("signal frame has duplicate (date, ticker) rows")
    return frame


def _cross_zscore(frame: pd.DataFrame, col: str = "score") -> pd.DataFrame:
    """按日截面计算 ``col`` 的 z-score，避免引入未来样本统计量。"""
    out = frame.copy()
    out[col] = out.groupby("date")[col].transform(standardize_zscore)
    return out


def _empty_weights() -> pd.DataFrame:
    return pd.DataFrame(columns=["date", "ticker", "target_weight"])


def _date_str(value) -> str:
    """将日期规范化为 ``YYYY-MM-DD`` 字符串。"""
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _jsonable(value):
    """将 numpy 标量与非有限浮点数转换为可 JSON 序列化的 Python 原生类型。"""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


# ---------------------------------------------------------------------------
# AlphaSignal
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AlphaSignal:
    """截面信号不可变载体，包含标准化分数、元数据、归因说明与因子快照。

    ``frame`` 为已做按日截面 z-score 的 ``[date, ticker, score]``；
    ``meta`` 至少需在归档时提供 ``strategy_id``。
    """

    frame: pd.DataFrame
    meta: dict
    reason: str | dict = ""
    factor_snapshot: dict | None = None

    def __post_init__(self) -> None:
        _validate_frame(self.frame)
        # 防御性拷贝，防止外部修改引用对象影响已冻结的信号
        object.__setattr__(self, "frame", self.frame.copy())
        object.__setattr__(self, "meta", dict(self.meta))
        if self.factor_snapshot is not None:
            object.__setattr__(self, "factor_snapshot", dict(self.factor_snapshot))

    # -- 聚合工厂方法 ---------------------------------------------------------

    @classmethod
    def from_multi_factor(
        cls,
        combined: pd.DataFrame,
        *,
        reason: str | dict = "",
        factor_snapshot: dict | None = None,
        **meta,
    ) -> "AlphaSignal":
        """从多因子合成输出 ``[date, ticker, combined_score]`` 构造信号并重做按日 z-score。"""
        if "combined_score" not in combined.columns:
            raise ValueError("combined must carry a 'combined_score' column")
        missing = [c for c in ("date", "ticker") if c not in combined.columns]
        if missing:
            raise ValueError(f"combined is missing columns {missing}")
        renamed = combined.rename(columns={"combined_score": "score"})
        frame = _cross_zscore(renamed[list(REQUIRED_FRAME_COLS)])
        return cls(
            frame=frame,
            meta=meta,
            reason=reason,
            factor_snapshot=factor_snapshot,
        )

    @classmethod
    def from_ml_factor(
        cls,
        ml_scores: pd.DataFrame,
        *,
        reason: str | dict = "",
        factor_snapshot: dict | None = None,
        **meta,
    ) -> "AlphaSignal":
        """从 ML 信号输出 ``[date, ticker, score]`` 构造信号并统一按日 z-score 尺度。"""
        missing = [c for c in REQUIRED_FRAME_COLS if c not in ml_scores.columns]
        if missing:
            raise ValueError(f"ml_scores is missing columns {missing}")
        frame = _cross_zscore(ml_scores[list(REQUIRED_FRAME_COLS)])
        return cls(
            frame=frame,
            meta=meta,
            reason=reason,
            factor_snapshot=factor_snapshot,
        )

    # -- 分数转权重 -----------------------------------------------------------

    def to_weights(
        self, method: str = "top_k", k: int = DEFAULT_K, **kw
    ) -> pd.DataFrame:
        """通过 :func:`signal_to_weights` 将信号分数转换为 ``[date, ticker, target_weight]``。"""
        return signal_to_weights(self.frame, method=method, k=k, **kw)


# ---------------------------------------------------------------------------
# signal_to_weights
# ---------------------------------------------------------------------------


def signal_to_weights(
    signal_frame: pd.DataFrame,
    method: str = "top_k",
    k: int = DEFAULT_K,
    **kw,
) -> pd.DataFrame:
    """将 ``[date, ticker, score]`` 转为无约束目标权重 ``[date, ticker, target_weight]``。

    - ``top_k``（默认）：每日取分数最高的前 *k* 只各配 ``1/k``（不足 *k* 只仍按 ``1/k``，其余为现金）。
    - ``proportional``：截断负分为 0 后按日归一化；含负分且无正分则空仓，全 0 分则回退 ``1/n`` 等权。

    仅返回正权重行；参数或列契约不合法时抛出 ``ValueError``。
    """
    if method not in WEIGHT_METHODS:
        raise ValueError(f"method must be one of {WEIGHT_METHODS}, got {method!r}")
    if kw:
        raise ValueError(f"unexpected keyword arguments: {sorted(kw)}")
    _validate_frame(signal_frame)

    if method == "top_k":
        if k <= 0 or k != int(k):
            raise ValueError(f"k must be a positive integer, got {k!r}")
        return _top_k_weights(signal_frame, int(k))
    return _proportional_weights(signal_frame)


def _top_k_weights(frame: pd.DataFrame, k: int) -> pd.DataFrame:
    """按日取分数最高的前 *k* 只等权（``1/k``），同分按 ``ticker`` 升序确定性排序，跳过 NaN。"""
    valid = frame.dropna(subset=["score"])
    if valid.empty:
        return _empty_weights()
    ranked = valid.sort_values(
        ["date", "score", "ticker"], ascending=[True, False, True]
    )
    selected = ranked.groupby("date", sort=True).head(k)
    out = selected[["date", "ticker"]].copy()
    out["target_weight"] = 1.0 / k
    return out.reset_index(drop=True)


def _proportional_weights(frame: pd.DataFrame) -> pd.DataFrame:
    """按日计算 ``max(score, 0)`` 归一化权重并剔除 0 权重行。

    仅当全截面均为 0 分（无离散度）时回退 ``1/n`` 等权；含负分且无正分则不持仓。
    """
    valid = frame.dropna(subset=["score"]).copy()
    if valid.empty:
        return _empty_weights()

    parts: list[pd.DataFrame] = []
    for _, group in valid.groupby("date", sort=True):
        scores = group["score"].astype("float64")
        positive = scores.clip(lower=0.0)
        total = float(positive.sum())

        if total > 0:
            w = positive / total
            keep = w > 0
            part = group.loc[keep, ["date", "ticker"]].copy()
            part["target_weight"] = w[keep]
            parts.append(part)
        elif (scores == 0).all():
            # 零离散度截面回退等权
            n = len(scores)
            part = group[["date", "ticker"]].copy()
            part["target_weight"] = 1.0 / n
            parts.append(part)
        # 含负分且无正分时不生成持仓

    if not parts:
        return _empty_weights()
    return pd.concat(parts, ignore_index=True)


# ---------------------------------------------------------------------------
# 日终归档
# ---------------------------------------------------------------------------


def write_signal(
    signal: AlphaSignal,
    weights: pd.DataFrame,
    *,
    base_dir: str = str(DATA_ROOT / "signals"),
) -> dict[str, list[Path]]:
    """按交易日将 *signal* 与 *weights* 归档至 ``cross/`` 与 ``timeseries/`` Parquet。

    ``timeseries`` 中 ``quantity`` 恒为 0（具体股数由下游订单生成阶段计算）。
    返回 ``{"cross": [...], "timeseries": [...]}``，按日期升序排列路径。
    """
    _validate_frame(signal.frame)
    strategy_id = signal.meta.get("strategy_id")
    if not strategy_id:
        raise ValueError("signal.meta must carry a non-empty 'strategy_id'")
    missing = [
        c for c in ("date", "ticker", "target_weight") if c not in weights.columns
    ]
    if missing:
        raise ValueError(f"weights is missing columns {missing}")

    signal_time = signal.meta.get("signal_time") or signal.meta.get("trade_date")
    snapshot_json = (
        _jsonify(signal.factor_snapshot) if signal.factor_snapshot is not None else ""
    )
    reason_text = (
        signal.reason if isinstance(signal.reason, str) else _jsonify(signal.reason)
    )

    written: dict[str, list[Path]] = {"cross": [], "timeseries": []}
    frame = signal.frame
    for date, group in frame.groupby("date", sort=True):
        date_key = _date_str(date)

        # 统一格式化为字符串日期键再合并，避免日期类型差异导致匹配失败
        group = group.copy()
        group["_date_key"] = group["date"].map(_date_str)
        day_w = weights.loc[
            pd.to_datetime(weights["date"]) == pd.to_datetime(date)
        ].copy()
        day_w["_date_key"] = day_w["date"].map(_date_str)

        cross = group.merge(
            day_w[["_date_key", "ticker", "target_weight"]],
            on=["_date_key", "ticker"],
            how="left",
        )
        cross["target_weight"] = cross["target_weight"].fillna(0.0)
        cross["signal_time"] = signal_time if signal_time is not None else date_key
        cross["strategy_id"] = strategy_id
        cross["trade_date"] = date_key

        ts = pd.DataFrame(
            {
                "datetime": signal_time if signal_time is not None else date_key,
                "ticker": group["ticker"].to_numpy(),
                "direction": np.sign(group["score"].to_numpy(dtype="float64")),
                "strength": np.abs(group["score"].to_numpy(dtype="float64")),
                "quantity": 0.0,
                "factor_snapshot": snapshot_json,
                "reason": reason_text,
            }
        )

        cross_path = _archive_path(base_dir, "cross", strategy_id, date_key)
        ts_path = _archive_path(base_dir, "timeseries", strategy_id, date_key)
        cross[list(CROSS_COLS)].to_parquet(cross_path, index=False)
        ts[list(TIMESERIES_COLS)].to_parquet(ts_path, index=False)
        written["cross"].append(cross_path)
        written["timeseries"].append(ts_path)
    return written


def _archive_path(base_dir: str, tree: str, strategy_id: str, date_key: str) -> Path:
    out = Path(base_dir) / tree / str(strategy_id)
    out.mkdir(parents=True, exist_ok=True)
    return out / f"{date_key}.parquet"


def _jsonify(value) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)
