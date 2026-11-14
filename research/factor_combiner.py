"""多因子合成与合成质量评估。

职责边界：
1. ``preprocess``：按日截面执行 ``3-MAD 缩尾 → z-score 标准化 → 行业与市值中性化``。
2. ``FactorCombiner``：支持 ``equal_weight``（默认主路径）、``ic_weight`` 与 ``ic_ir_weight``；
   加权方法采用扩张窗口并按月更新，仅使用严格早于当期的历史 IC，杜绝前视偏差。
3. ``FactorCombiner.evaluate`` / ``write_eval_report``：输出 8 项评估指标与因子贡献分解，
   并落盘 ``eval_report.json``。
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from core.config import config_snapshot
from research.post_processor import neutralize as neutralize_residual
from research.post_processor import standardize_zscore, winsorize_mad
from core.config import DATA_ROOT

__all__ = [
    "DEFAULT_LOOKBACK",
    "EVAL_METRIC_KEYS",
    "FactorCombiner",
    "WEIGHT_METHODS",
    "ZERO_SIGMA_IR",
    "factor_contribution",
    "preprocess",
    "write_eval_report",
]

# 加权方法启用所需的最小历史 IC 样本数（扩张窗口的预热期门限）
DEFAULT_LOOKBACK: int = 60

# 允许的合成方法（equal_weight 为基准，ic_ir_weight 惩罚高波动因子）
WEIGHT_METHODS: tuple[str, ...] = ("equal_weight", "ic_weight", "ic_ir_weight")

# 评估报告 8 项核心指标键名顺序
EVAL_METRIC_KEYS: tuple[str, ...] = (
    "ic_mean",
    "ic_ir",
    "ann_return",
    "ann_sharpe",
    "max_drawdown",
    "calmar",
    "turnover_monthly",
    "factor_contribution",
)

# IC 标准差为 0 且均值大于 0 时的有限大替代值，用于归一化胜出
ZERO_SIGMA_IR: float = 1e12


# ---------------------------------------------------------------------------
# 合成前清洗
# ---------------------------------------------------------------------------


def preprocess(
    df: pd.DataFrame,
    factor_col: str,
    *,
    date_col: str = "date",
    industry_col: str = "industry",
    size_col: str = "log_market_cap",
    n_mad: float = 3.0,
    neutralize: bool = True,
) -> pd.DataFrame:
    """返回 *df* 副本，对 *factor_col* 按日截面依次执行缩尾、z-score 与可选中性化。

    不执行缺失值填充（由合成阶段按有效因子重归一化）；日期为空的行保留原值。
    """
    if factor_col not in df.columns:
        raise ValueError(f"factor_col {factor_col!r} not in columns {list(df.columns)}")
    if date_col not in df.columns:
        raise ValueError(f"date_col {date_col!r} not in columns {list(df.columns)}")

    result = df.copy()

    cleaned = result.groupby(date_col)[factor_col].transform(winsorize_mad, n=n_mad)
    result[factor_col] = cleaned.where(result[date_col].notna(), result[factor_col])

    cleaned = result.groupby(date_col)[factor_col].transform(standardize_zscore)
    result[factor_col] = cleaned.where(result[date_col].notna(), result[factor_col])

    if neutralize:
        if date_col != "date":
            # neutralize 内部固定按 "date" 分组，不允许传入其他日期列名
            raise ValueError(
                "neutralize=True requires date_col='date': "
                "research.post_processor.neutralize is hard-wired to 'date'"
            )
        result = neutralize_residual(
            result,
            factor_col,
            industry_col=industry_col,
            size_col=size_col,
        )
    return result


# ---------------------------------------------------------------------------
# 因子贡献分解
# ---------------------------------------------------------------------------


def factor_contribution(
    weights: pd.DataFrame,
    factor_returns: pd.DataFrame,
    factor_exposures: pd.DataFrame,
) -> pd.Series:
    """按 ``Σ_t Σ_j w_{j,t} · exposure_{j,i,t} · factor_return_{i,t}`` 分解各因子对组合的贡献。

    返回按绝对值之和归一化（保留正负号）的 ``pd.Series``，用于识别单一因子主导的伪分散风险。
    """
    for frame, name, cols in (
        (weights, "weights", ("date", "ticker", "weight")),
        (factor_returns, "factor_returns", ("date", "factor_name", "factor_return")),
    ):
        missing = [c for c in cols if c not in frame.columns]
        if missing:
            raise ValueError(f"{name} is missing columns {missing}")

    exposure_cols = [c for c in factor_exposures.columns if c not in ("date", "ticker")]
    if not exposure_cols:
        raise ValueError("factor_exposures has no factor columns")

    names = factor_returns["factor_name"].astype(str)
    unknown = sorted(set(names.unique()) - set(exposure_cols))
    if unknown:
        raise ValueError(
            f"factor_returns names {unknown} not in factor_exposures columns"
        )
    orphan = sorted(set(exposure_cols) - set(names.unique()))
    if orphan:
        # 存在无收益数据的暴露列视为数据缺失，避免剔除后虚高剩余因子占比
        raise ValueError(
            f"factor_exposures columns {orphan} have no factor_returns rows"
        )

    # 转为 (date, ticker, factor_name) 长表
    wide = factor_exposures.melt(
        id_vars=["date", "ticker"],
        value_vars=exposure_cols,
        var_name="factor_name",
        value_name="exposure",
    )
    merged = wide.merge(
        weights[["date", "ticker", "weight"]],
        on=["date", "ticker"],
        how="inner",
    ).merge(
        factor_returns[["date", "factor_name", "factor_return"]],
        on=["date", "factor_name"],
        how="inner",
    )
    if merged.empty:
        raise ValueError("no overlapping (date, ticker) between weights and exposures")

    raw = (
        (merged["weight"] * merged["exposure"] * merged["factor_return"])
        .groupby(merged["factor_name"])
        .sum()
    )
    total = float(raw.abs().sum())
    if total == 0.0:
        # 总贡献为 0 时按等权返回，维持绝对占比和为 1 的契约
        return pd.Series(
            {c: 1.0 / len(exposure_cols) for c in exposure_cols}, dtype="float64"
        )
    return raw / total


# ---------------------------------------------------------------------------
# 评估报告写入
# ---------------------------------------------------------------------------


def write_eval_report(
    name: str,
    metrics: dict,
    config_path: str,
    *,
    out_dir: str = str(DATA_ROOT / "signals"),
    period: tuple | None = None,
    walk_forward_folds: list | None = None,
    combiner: dict | None = None,
) -> Path:
    """将 8 项评估指标、``config_snapshot``（``path@hash``）与折次信息写入 ``eval_report.json`` 并返回路径。"""
    if not name:
        raise ValueError("name must be non-empty")
    missing = [k for k in EVAL_METRIC_KEYS if k not in metrics]
    if missing:
        raise ValueError(f"metrics is missing {missing}")

    out = Path(out_dir) / name
    out.mkdir(parents=True, exist_ok=True)

    if period is not None and len(period) != 2:
        raise ValueError(f"period must be a (start, end) pair, got {period!r}")

    report = {
        "alpha_name": name,
        "config_snapshot": config_snapshot(config_path),
        "period": _jsonable(list(period) if period is not None else None),
        "metrics": {k: _jsonable(metrics[k]) for k in EVAL_METRIC_KEYS},
        "walk_forward_folds": _jsonable(list(walk_forward_folds or [])),
        "combiner": _jsonable(dict(combiner or {})),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    path = out / "eval_report.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path


def _jsonable(value):
    """将 numpy 标量、时间对象与非有限浮点数规范化为可 JSON 序列化值。"""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


# ---------------------------------------------------------------------------
# FactorCombiner
# ---------------------------------------------------------------------------


class FactorCombiner:
    """多因子打分合成器，将预处理后的因子 z-score 合成为 ``combined_score``。

    默认使用 ``equal_weight``；选择 ``ic_weight`` 或 ``ic_ir_weight`` 时需提供宽表格式的历史 Rank IC
    ``[date, f1, f2, ...]``。
    """

    def __init__(self, method: str = "equal_weight", lookback: int = DEFAULT_LOOKBACK):
        if method not in WEIGHT_METHODS:
            raise ValueError(f"method must be one of {WEIGHT_METHODS}, got {method!r}")
        if not lookback > 0:
            raise ValueError(f"lookback must be positive, got {lookback!r}")
        self.method = method
        self.lookback = int(lookback)

    # -- 合成主流程 -----------------------------------------------------------

    def combine(
        self,
        factor_scores: pd.DataFrame,
        ic_history: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """计算 ``[date, ticker, combined_score]``。

        加权方法按月更新权重且仅使用严格早于当月首日的 IC（``ic_date < as_of``）；
        单行部分因子缺失时对有效因子权重重归一化，全缺失保留 NaN。
        """
        id_cols = [c for c in ("date", "ticker") if c in factor_scores.columns]
        if "date" not in id_cols:
            raise ValueError("factor_scores must carry a 'date' column")
        factor_cols = [c for c in factor_scores.columns if c not in id_cols]
        if not factor_cols:
            raise ValueError("factor_scores has no factor columns")

        if self.method == "equal_weight":
            w = self._equal_weights(factor_cols)
            score = _weighted_score(factor_scores, factor_cols, w)
            return factor_scores[id_cols].assign(combined_score=score)

        if ic_history is None:
            raise ValueError(
                f"method {self.method!r} requires ic_history; "
                "equal_weight is the method that needs none"
            )
        if "date" not in ic_history.columns:
            raise ValueError("ic_history must carry a 'date' column")
        if not set(factor_cols) & set(ic_history.columns):
            # 无任何共同因子列时直接报错，防止静默退化为等权
            raise ValueError(
                f"ic_history shares no factor columns with {factor_cols}; "
                f"its columns are {list(ic_history.columns)}"
            )

        schedule = self._weight_schedule(factor_scores, ic_history, factor_cols)
        score = pd.Series(np.nan, index=factor_scores.index, dtype="float64")
        for idx, w in schedule:
            score.loc[idx] = _weighted_score(factor_scores.loc[idx], factor_cols, w)
        return factor_scores[id_cols].assign(combined_score=score)

    def _weight_schedule(
        self,
        factor_scores: pd.DataFrame,
        ic_history: pd.DataFrame,
        factor_cols: list[str],
    ) -> list[tuple[pd.Index, dict]]:
        """按自然月生成权重调度表；第 *M* 月仅使用严格早于该月首日（``<``）的历史 IC。"""
        dates = pd.to_datetime(factor_scores["date"])
        periods = dates.dt.to_period("M")
        ic_dates = pd.to_datetime(ic_history["date"])

        schedule: list[tuple[pd.Index, dict]] = []
        for _, idx in factor_scores.groupby(periods).groups.items():
            as_of = dates.loc[idx].min()
            hist = ic_history.loc[ic_dates < as_of]
            if len(hist) < self.lookback:
                # 预热期不足时直接走等权；逐因子长度门限仍在各权重估计器内强制执行
                w = self._equal_weights(factor_cols)
            elif self.method == "ic_weight":
                w = self._ic_weights(hist, factor_cols)
            else:
                w = self._ic_ir_weights(hist, factor_cols)
            schedule.append((idx, w))
        return schedule

    # -- 权重估计器 -----------------------------------------------------------

    def _equal_weights(self, factor_cols) -> dict:
        """生成等权字典 ``1/n``。"""
        n = len(factor_cols)
        if n == 0:
            raise ValueError("factor_cols is empty")
        return {c: 1.0 / n for c in factor_cols}

    def _ic_weights(self, ic_history, factor_cols) -> dict:
        """基于 ``mean(IC)`` 计算非负归一化权重，样本不足 ``lookback`` 或均值非正置 0，全 0 回退等权。"""
        w = {}
        for c in factor_cols:
            if c not in ic_history.columns:
                w[c] = 0.0
                continue
            s = ic_history[c].astype("float64").dropna()
            if s.size < self.lookback:
                w[c] = 0.0
                continue
            w[c] = max(float(s.mean()), 0.0)
        return self._normalise(w, factor_cols)

    def _ic_ir_weights(self, ic_history, factor_cols) -> dict:
        """基于 ``mean(IC)/std(IC)`` 计算非负归一化权重；逐因子校验 ``lookback`` 与最少 2 个样本，零波动正均值取 ``ZERO_SIGMA_IR``。"""
        w = {}
        for c in factor_cols:
            if c not in ic_history.columns:
                w[c] = 0.0
                continue
            s = ic_history[c].astype("float64").dropna()
            if s.size < self.lookback:
                w[c] = 0.0
                continue
            if len(s) < 2:
                w[c] = 0.0
                continue
            mu = float(s.mean())
            sigma = float(s.std())
            if sigma > 0:
                ir = mu / sigma
            else:
                ir = ZERO_SIGMA_IR if mu > 0 else 0.0
            w[c] = max(ir, 0.0)
        return self._normalise(w, factor_cols)

    def _normalise(self, w: dict, factor_cols) -> dict:
        """将权重字典归一化为和为 1；总和不大于 0 时回退等权。"""
        total = sum(w.values())
        if total <= 0:
            return self._equal_weights(factor_cols)
        return {c: w[c] / total for c in factor_cols}

    # -- 评估入口 -------------------------------------------------------------

    def evaluate(
        self,
        combined: pd.DataFrame,
        forward_returns: pd.DataFrame,
        portfolio_returns: pd.DataFrame,
        *,
        weights: pd.DataFrame | None = None,
        factor_exposures: pd.DataFrame | None = None,
        factor_returns: pd.DataFrame | None = None,
    ) -> dict:
        """计算 8 项合成质量指标并返回字典。

        包含 ``ic_mean``、``ic_ir``、``ann_return``、``ann_sharpe``、``max_drawdown``、``calmar``、
        ``turnover_monthly`` 与 ``factor_contribution``；后两项需同时传入 ``weights``、``factor_exposures`` 与 ``factor_returns``。
        """
        for frame, name, cols in (
            (combined, "combined", ("date", "ticker", "combined_score")),
            (forward_returns, "forward_returns", ("date", "ticker", "ret")),
            (portfolio_returns, "portfolio_returns", ("date", "ret")),
        ):
            missing = [c for c in cols if c not in frame.columns]
            if missing:
                raise ValueError(f"{name} is missing columns {missing}")
        if weights is None or factor_exposures is None or factor_returns is None:
            raise ValueError(
                "weights, factor_exposures and factor_returns are all required "
                "for turnover_monthly and factor_contribution"
            )

        ic = _rank_ic_series(combined, forward_returns)
        ic_mean = float(ic.mean()) if not ic.empty else float("nan")
        ic_std = float(ic.std()) if len(ic) >= 2 else float("nan")
        ic_ir = ic_mean / ic_std if np.isfinite(ic_std) and ic_std > 0 else float("nan")

        rets = portfolio_returns.sort_values("date")["ret"].astype("float64")
        ann_return = _ann_return(rets)
        ann_sharpe = _ann_sharpe(rets)
        max_drawdown = _max_drawdown(rets)
        calmar = (
            ann_return / abs(max_drawdown)
            if np.isfinite(max_drawdown) and max_drawdown != 0
            else float("nan")
        )

        contribution = factor_contribution(weights, factor_returns, factor_exposures)

        return {
            "ic_mean": ic_mean,
            "ic_ir": ic_ir,
            "ann_return": ann_return,
            "ann_sharpe": ann_sharpe,
            "max_drawdown": max_drawdown,
            "calmar": calmar,
            "turnover_monthly": _turnover_monthly(weights),
            "factor_contribution": {str(k): float(v) for k, v in contribution.items()},
        }


# ---------------------------------------------------------------------------
# 指标计算原语
# ---------------------------------------------------------------------------


def _weighted_score(scores: pd.DataFrame, factor_cols: list[str], w: dict) -> pd.Series:
    """按行对非 NaN 有效因子权重重归一化并计算加权均分，全缺失返回 NaN。"""
    values = scores[factor_cols].astype("float64").to_numpy()
    weight_vec = np.array([w[c] for c in factor_cols], dtype="float64")
    finite = np.isfinite(values)
    num = np.where(finite, values * weight_vec, 0.0).sum(axis=1)
    den = np.where(finite, weight_vec, 0.0).sum(axis=1)
    out = np.divide(
        num, den, out=np.full(len(num), np.nan, dtype="float64"), where=den > 0
    )
    return pd.Series(out, index=scores.index, dtype="float64")


def _rank_ic_series(combined: pd.DataFrame, forward_returns: pd.DataFrame) -> pd.Series:
    """计算每日截面 ``combined_score`` 与下期收益的 Spearman 秩相关系数序列。"""
    merged = combined.merge(
        forward_returns[["date", "ticker", "ret"]], on=["date", "ticker"]
    )

    def _one(group: pd.DataFrame) -> float:
        if len(group) < 2:
            return float("nan")
        if group["combined_score"].nunique() < 2 or group["ret"].nunique() < 2:
            return float("nan")
        return float(group["combined_score"].corr(group["ret"], method="spearman"))

    return (
        merged.groupby("date", sort=True)[["combined_score", "ret"]]
        .apply(_one)
        .dropna()
    )


def _ann_return(rets: pd.Series) -> float:
    """按 252 个交易日年化日收益序列。"""
    r = rets.dropna()
    if r.empty:
        return float("nan")
    total = float((1.0 + r).prod() - 1.0)
    return float((1.0 + total) ** (252.0 / len(r)) - 1.0)


def _ann_sharpe(rets: pd.Series) -> float:
    """计算年化夏普比率（``mean / std * sqrt(252)``）。"""
    r = rets.dropna()
    if len(r) < 2:
        return float("nan")
    sigma = float(r.std())
    if sigma <= 0:
        return float("nan")
    return float(r.mean() / sigma * np.sqrt(252.0))


def _max_drawdown(rets: pd.Series) -> float:
    """计算累计净值最大回撤（峰值下限钳位为 1.0 以覆盖首期亏损）。"""
    r = rets.dropna()
    if r.empty:
        return float("nan")
    nav = (1.0 + r).cumprod()
    peak = nav.cummax().clip(lower=1.0)
    return float((nav / peak - 1.0).min())


def _turnover_monthly(weights: pd.DataFrame) -> float:
    """计算相邻月末持仓之间的平均月度单边换手率 ``Σ|Δw|/2``。"""
    if not {"date", "ticker", "weight"} <= set(weights.columns):
        raise ValueError("weights must carry columns ['date', 'ticker', 'weight']")
    frame = weights.copy()
    frame["_t"] = pd.to_datetime(frame["date"])
    frame["_p"] = frame["_t"].dt.to_period("M")

    # 取每月最后一个交易日作为月末持仓
    month_ends = []
    for _, group in frame.groupby("_p", sort=True):
        last = group["_t"].max()
        book = (
            group.loc[group["_t"] == last, ["ticker", "weight"]]
            .set_index("ticker")["weight"]
            .astype("float64")
        )
        month_ends.append(book)

    if len(month_ends) < 2:
        return float("nan")

    turns = []
    for prev, cur in zip(month_ends, month_ends[1:]):
        aligned = pd.concat([prev, cur], axis=1).fillna(0.0)
        aligned.columns = ["prev", "cur"]
        turns.append(float((aligned["cur"] - aligned["prev"]).abs().sum() / 2.0))
    return float(np.mean(turns))
