"""alphalens 集成与因子评估管道。

提供研究侧因子准入流程 ``FactorPipeline``（``compute -> preprocess -> evaluate -> register``）：
- 标量评估结果与尝试记录写入 ``metadata.json``；
- 时序结果落盘至 ``data_platform/factor_eval/{name}/``；
- 完整报告输出至 ``research/reports/{name}_{date}.html``。

准入门限：``rank_ic_mean > 0.04``、``ir > 0.5`` 且 ``ic_t_stat > max(2, sqrt(2*ln(N)))``；
衰减警戒线：``rank_ic_mean < 0.03``。alphalens 仅在方法体内延迟导入。
"""

from __future__ import annotations

import ast
import base64
import io
import json
import math
import operator
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any, Callable, Sequence

import pandas as pd

from research.factor_storage import _safe_component, save_factor_full
from research.post_processor import PIPELINE_STOCK_EVAL, PostProcessor, neutralize
from core.config import DATA_ROOT

__all__ = [
    "AlphalensAdapter",
    "EvalResult",
    "EXIT_IC_THRESHOLD",
    "FactorPipeline",
    "GATE_IC_MIN",
    "GATE_IR_MIN",
    "GATE_T_BASE",
    "check_exit",
    "DEFAULT_PERIODS",
]

# 默认前向收益评估周期（交易日）
DEFAULT_PERIODS: tuple[int, ...] = (1, 5, 10, 20)

# 因子准入门限
GATE_IC_MIN: float = 0.04
GATE_IR_MIN: float = 0.5
GATE_T_BASE: float = 2.0

# 因子衰减退出警戒阈值
EXIT_IC_THRESHOLD: float = 0.03

# alphalens 分层分位数数量
_QUANTILES: int = 5

# 加载价格时的日历日缓冲天数，避免评估窗口尾部缺少前向收益
_PRICE_BUFFER_DAYS: int = 45

# 停牌期间收盘价前向填充最大天数
_FFILL_LIMIT: int = 5

_TEARSHEET_DPI: int = 100


def _noop_show(*_args: Any, **_kwargs: Any) -> None:
    """捕获 tearsheet 图表时用于临时屏蔽 ``plt.show``/``plt.close``。"""


# ---------------------------------------------------------------------------
# 表达式安全求值
# ---------------------------------------------------------------------------

_BIN_OPS: dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
}
_UNARY_OPS: dict[type, Callable[[Any], Any]] = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}


def _rolling_apply(func: str) -> Callable[[pd.Series, int], pd.Series]:
    def _fn(series: pd.Series, window: int) -> pd.Series:
        if not isinstance(window, int) or isinstance(window, bool) or window < 1:
            raise ValueError(f"window must be a positive int, got {window!r}")
        return getattr(series.rolling(window), func)()

    return _fn


_EXPR_FUNCS: dict[str, Callable[..., Any]] = {
    "rolling_mean": _rolling_apply("mean"),
    "rolling_std": _rolling_apply("std"),
    "rolling_sum": _rolling_apply("sum"),
    "rolling_max": _rolling_apply("max"),
    "rolling_min": _rolling_apply("min"),
}


def _positive_periods(periods: int) -> int:
    """校验偏移周期必须为正整数，防止负数周期引入未来数据。"""
    if not isinstance(periods, int) or isinstance(periods, bool) or periods < 1:
        raise ValueError(f"periods must be a positive int, got {periods!r}")
    return periods


def _expr_delta(series: pd.Series, periods: int = 1) -> pd.Series:
    return series.diff(_positive_periods(periods))


def _expr_pct_change(series: pd.Series, periods: int = 1) -> pd.Series:
    return series.pct_change(_positive_periods(periods))


_EXPR_FUNCS["delta"] = _expr_delta
_EXPR_FUNCS["pct_change"] = _expr_pct_change


def _eval_node(node: ast.AST, columns: dict[str, pd.Series]) -> Any:
    if isinstance(node, ast.BinOp) and type(node.op) in _BIN_OPS:
        return _BIN_OPS[type(node.op)](
            _eval_node(node.left, columns), _eval_node(node.right, columns)
        )
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPS:
        return _UNARY_OPS[type(node.op)](_eval_node(node.operand, columns))
    if isinstance(node, ast.Call):
        if node.keywords or not isinstance(node.func, ast.Name):
            raise ValueError(f"unsupported call in expr: {ast.dump(node)}")
        func = _EXPR_FUNCS.get(node.func.id)
        if func is None:
            raise ValueError(f"unknown function {node.func.id!r} in expr")
        return func(*[_eval_node(arg, columns) for arg in node.args])
    if isinstance(node, ast.Name):
        if node.id not in columns:
            raise ValueError(f"unknown column {node.id!r} in expr")
        return columns[node.id]
    if (
        isinstance(node, ast.Constant)
        and isinstance(node.value, (int, float))
        and not isinstance(node.value, bool)
    ):
        return node.value
    raise ValueError(f"unsupported expression element: {ast.dump(node)}")


def _eval_expr(expr: str, columns: dict[str, pd.Series]) -> pd.Series:
    """在白名单算子与窗口函数内安全解析并计算因子表达式，禁止前视操作符。"""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"invalid factor expr {expr!r}: {exc}") from exc
    result = _eval_node(tree.body, columns)
    if not isinstance(result, pd.Series):
        raise ValueError(f"expr must evaluate to a Series, got {type(result).__name__}")
    return result


def _expr_columns(expr: str) -> tuple[str, ...]:
    """提取表达式依赖的行情列名。"""
    tree = ast.parse(expr, mode="eval")
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    funcs = {
        n.func.id
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    }
    return tuple(sorted(names - funcs))


# 中性化所需的元数据列（预处理后剔除，不进入因子存储）
_NEUTRALIZE_COLS: tuple[str, ...] = ("industry", "log_market_cap")


def _value_col(df: pd.DataFrame) -> str:
    cols = [c for c in df.columns if c not in ("date", "ticker")]
    if len(cols) != 1:
        raise ValueError(
            f"expected exactly one value column besides date/ticker, got {cols!r}"
        )
    return cols[0]


def _factor_column(df: pd.DataFrame) -> str:
    """提取唯一因子列名（忽略中性化元数据列）。"""
    cols = [c for c in df.columns if c not in ("date", "ticker", *_NEUTRALIZE_COLS)]
    if len(cols) != 1:
        raise ValueError(f"expected exactly one factor column, got {cols!r}")
    return cols[0]


def _quantile_label(q: Any) -> str:
    """将分位数编码规范化为稳定的整数字符串标签（如 ``1.0 -> "1"``）。"""
    value = float(q)
    return str(int(value)) if value.is_integer() else str(q)


# ---------------------------------------------------------------------------
# 同族因子尝试次数记录
# ---------------------------------------------------------------------------


def factor_family(name: str) -> str:
    """提取因子族名（第一个 ``_`` 前缀）。"""
    return name.split("_", 1)[0] or name


def _attempt_path(base_dir: Path, family: str) -> Path:
    safe = _safe_component(family, "factor family")
    return Path(base_dir) / "factor_eval" / "attempt_log" / f"{safe}.json"


def _load_attempts(base_dir: Path, family: str) -> list[dict]:
    path = _attempt_path(base_dir, family)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        # 尝试日志损坏时禁止静默重置 N，以免放宽准入门限
        raise RuntimeError(
            f"attempt log {path} is unreadable; refusing to reset N"
        ) from exc
    if not isinstance(data, list):
        raise RuntimeError(
            f"attempt log {path} is corrupt (expected a JSON list); refusing to reset N"
        )
    return list(data)


def _record_attempt(base_dir: Path, family: str, entry: dict) -> int:
    entries = _load_attempts(base_dir, family)
    entries.append(entry)
    path = _attempt_path(base_dir, family)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(
        json.dumps(entries, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    tmp.replace(path)  # 原子替换，防止中途崩溃截断日志
    return len(entries)


# ---------------------------------------------------------------------------
# AlphalensAdapter
# ---------------------------------------------------------------------------


class AlphalensAdapter:
    """平台数据格式与 alphalens 接口的无状态适配器。"""

    @staticmethod
    def to_al_factor(factor_df: pd.DataFrame) -> pd.Series:
        """将宽表 ``(date × ticker)`` 转为有序的 ``(date, asset)`` MultiIndex Series（剔除 NaN）。"""
        if not isinstance(factor_df, pd.DataFrame):
            raise TypeError(
                f"factor_df must be a DataFrame, got {type(factor_df).__name__}"
            )
        if factor_df.empty:
            raise ValueError("factor_df is empty")
        wide = factor_df.copy()
        wide.index = pd.to_datetime(wide.index)
        series = wide.stack()
        series.index = series.index.set_names(["date", "asset"])
        series = series.sort_index().astype("float64")
        series.name = "factor"
        return series

    @staticmethod
    def to_al_prices(bars_df: pd.DataFrame, price_col: str = "close") -> pd.DataFrame:
        """将长表行情转为宽表收盘价矩阵，最多前向填充 ``_FFILL_LIMIT`` 个交易日。"""
        required = {"date", "ticker", price_col}
        missing = required - set(bars_df.columns)
        if missing:
            raise ValueError(f"bars_df is missing columns: {sorted(missing)}")
        wide = bars_df.pivot(index="date", columns="ticker", values=price_col)
        wide.index = pd.to_datetime(wide.index)
        wide = wide.sort_index().sort_index(axis=1).astype("float64")
        return wide.ffill(limit=_FFILL_LIMIT)

    @staticmethod
    def extract_metrics(
        factor_data: pd.DataFrame,
        ic_series: pd.DataFrame,
        mean_quant_ret: pd.DataFrame,
        period: str | None = None,
    ) -> dict:
        """从 alphalens 输出提取标准指标（``rank_ic_mean``、``ir``、``ic_t_stat``、单调性、多空收益差、换手率）。"""
        import alphalens.performance as al_perf  # 延迟导入，避免模块加载强依赖 alphalens

        if ic_series.empty:
            raise ValueError("ic_series is empty")
        col = period if period is not None else ic_series.columns[0]
        if col not in ic_series.columns:
            raise ValueError(
                f"period {col!r} not in ic_series {list(ic_series.columns)}"
            )

        ic = ic_series[col].dropna()
        if ic.empty:
            raise ValueError(f"ic_series[{col!r}] has no observations")
        rank_ic_mean = float(ic.mean())
        ic_std = float(ic.std())  # 样本标准差（ddof=1）
        if math.isnan(ic_std):
            ir = math.nan
        elif ic_std == 0.0:
            ir = (
                math.copysign(math.inf, rank_ic_mean)
                if rank_ic_mean != 0.0
                else math.nan
            )
        else:
            ir = rank_ic_mean / ic_std
        n_obs = int(len(ic))
        ic_t_stat = ir * math.sqrt(n_obs)

        def _monotonicity(s: pd.Series) -> float:
            # 计算分位数与收益的 Spearman 单调性，使用统一 RangeIndex 避免标签对齐错位
            x = pd.Series(range(len(s)), dtype="float64")
            y = pd.Series(s.to_numpy(), dtype="float64")
            return x.corr(y, method="spearman")

        quantile_monotonicity = float(
            mean_quant_ret.apply(_monotonicity, axis=0).mean()
        )

        ret_col = (
            mean_quant_ret[col]
            if col in mean_quant_ret.columns
            else mean_quant_ret.iloc[:, 0]
        ).sort_index()
        top_minus_bottom = float(ret_col.iloc[-1] - ret_col.iloc[0])

        quantile_factor = factor_data["factor_quantile"]
        top_q = quantile_factor.max()
        top_turnover = al_perf.quantile_turnover(quantile_factor, top_q, period=1)
        turnover = float(top_turnover.mean())

        return {
            "rank_ic_mean": rank_ic_mean,
            "ir": float(ir),
            "ic_t_stat": float(ic_t_stat),
            "quantile_monotonicity": quantile_monotonicity,
            "top_minus_bottom": top_minus_bottom,
            "turnover": turnover,
        }

    @staticmethod
    def check_gate(metrics: dict, n_attempts: int = 1) -> tuple[bool, dict]:
        """校验准入门限：``IC > 0.04``、``IR > 0.5`` 且 ``t > max(2, sqrt(2*ln(N)))``。"""
        if not isinstance(n_attempts, int) or n_attempts < 1:
            raise ValueError(f"n_attempts must be a positive int, got {n_attempts!r}")
        t_threshold = max(GATE_T_BASE, math.sqrt(2.0 * math.log(n_attempts)))

        def _check(key: str, threshold: float) -> dict:
            value = float(metrics[key])
            return {
                "value": value,
                "threshold": threshold,
                "passed": bool(value > threshold),
            }

        detail = {
            "ic": _check("rank_ic_mean", GATE_IC_MIN),
            "ir": _check("ir", GATE_IR_MIN),
            "t": _check("ic_t_stat", t_threshold),
            "n_attempts": n_attempts,
        }
        passed = all(detail[k]["passed"] for k in ("ic", "ir", "t"))
        return passed, detail


def check_exit(metrics: dict) -> tuple[bool, dict]:
    """校验退出警戒线：``rank_ic_mean < 0.03`` 时标记为需复核。"""
    value = float(metrics["rank_ic_mean"])
    flagged = bool(value < EXIT_IC_THRESHOLD)
    return flagged, {
        "ic": {
            "value": value,
            "threshold": EXIT_IC_THRESHOLD,
            "flagged": flagged,
        }
    }


# ---------------------------------------------------------------------------
# EvalResult
# ---------------------------------------------------------------------------


@dataclass
class EvalResult:
    """单次因子评估的完整产出容器。"""

    summary: dict
    gate_passed: bool
    gate_detail: dict
    ic_series: pd.DataFrame
    quantile_returns: pd.DataFrame
    turnover: pd.DataFrame
    report_path: str | None


# ---------------------------------------------------------------------------
# FactorPipeline
# ---------------------------------------------------------------------------


class _FactorRecord:
    """适配 ``factor_storage`` 接口的轻量因子元信息对象。"""

    def __init__(self, name: str, version: str, depends_on: Sequence[str]) -> None:
        self.name = name
        self.version = str(version)
        self._depends = tuple(depends_on)
        self.description = ""
        self.category = ""
        self.frequency = "daily"
        self.asset_types: tuple[str, ...] = ("equity",)

    def depends_on(self) -> tuple[str, ...]:
        return self._depends


class FactorPipeline:
    """因子研发流水线：``compute -> preprocess -> evaluate -> register``。"""

    def __init__(
        self,
        data_api: Any,
        base_dir: str | Path = str(DATA_ROOT),
        reports_dir: str | Path = "research/reports",
    ) -> None:
        self._data_api = data_api
        self._base_dir = Path(base_dir)
        self._reports_dir = Path(reports_dir)
        # 最近一次 evaluate() 对应的原始因子表，供 register() 落盘未填充原值
        self._last_raw: pd.DataFrame | None = None
        # 待配对的原始因子表（由 compute/preprocess 写入）
        self._pending_raw: pd.DataFrame | None = None

    # -- 阶段 1：计算 ---------------------------------------------------------

    def compute(
        self,
        expr: str,
        universe: Sequence[str],
        start: str,
        end: str,
        name: str = "factor",
    ) -> pd.DataFrame:
        """在 ``[start, end]`` 区间内按标的计算前复权行情上的原始因子值 ``[date, ticker, name]``。"""
        symbols = [str(s) for s in universe]
        if not symbols:
            raise ValueError("universe is empty")
        if name in ("date", "ticker"):
            raise ValueError(f"value column name {name!r} is reserved")
        bars = self._load_bars(symbols, start, end)
        if bars.empty:
            raise ValueError(f"no bars for {symbols} in [{start}, {end}]")

        parts: list[pd.DataFrame] = []
        for ticker, grp in bars.groupby("ticker", sort=True):
            grp = grp.sort_values("date").reset_index(drop=True)
            columns = {c: grp[c] for c in grp.columns if c not in ("date", "ticker")}
            values = _eval_expr(expr, columns)
            parts.append(
                pd.DataFrame(
                    {
                        "date": grp["date"].values,
                        "ticker": str(ticker),
                        name: values.to_numpy(),
                    }
                )
            )
        raw = (
            pd.concat(parts, ignore_index=True)
            .sort_values(["date", "ticker"], kind="mergesort")
            .reset_index(drop=True)
        )
        self._pending_raw = raw
        return raw

    # -- 阶段 2：预处理 -------------------------------------------------------

    def preprocess(
        self, raw: pd.DataFrame, factor_col: str | None = None
    ) -> pd.DataFrame:
        """执行缩尾、中位数填充与 z-score，存在行业/市值列时自动中性化并仅返回 3 列因子表。"""
        col = factor_col or _factor_column(raw)
        processed = PostProcessor(PIPELINE_STOCK_EVAL).transform(
            raw, factor_col=col, date_col="date"
        )
        if {"industry", "log_market_cap"} <= set(processed.columns):
            processed = neutralize(processed, factor_col=col)
        processed = processed[["date", "ticker", col]]
        self._pending_raw = raw
        return processed

    # -- 阶段 3：评估 ---------------------------------------------------------

    def evaluate(
        self,
        processed: pd.DataFrame,
        periods: Sequence[int] = DEFAULT_PERIODS,
        name: str | None = None,
        n_attempts: int | None = None,
    ) -> EvalResult:
        """运行 alphalens 评估、校验多重检验准入门限并生成 tearsheet HTML。

        传入 *name* 时按因子族累计尝试次数 *N*；绘图失败仅发警告并将 ``report_path`` 置为 ``None``。
        """
        import alphalens.performance as al_perf  # 延迟导入 alphalens
        import alphalens.utils as al_utils

        if processed.empty:
            raise ValueError("processed frame is empty")
        col = _value_col(processed)
        if name is not None:
            name = _safe_component(name, "factor name")
        eval_name = name or col
        # 绑定原始因子表供后续 register() 使用，重复评估时不覆盖既有原始数据
        self._last_raw = (
            self._pending_raw if self._pending_raw is not None else processed
        )

        wide = processed.pivot(index="date", columns="ticker", values=col)
        factor = AlphalensAdapter.to_al_factor(wide)

        dates = pd.to_datetime(processed["date"])
        symbols = sorted(processed["ticker"].astype(str).unique())
        bars = self._load_bars(
            symbols,
            str(dates.min().date()),
            str((dates.max() + pd.Timedelta(days=_PRICE_BUFFER_DAYS)).date()),
        )
        prices = AlphalensAdapter.to_al_prices(bars)

        factor_data = al_utils.get_clean_factor_and_forward_returns(
            factor,
            prices,
            quantiles=_QUANTILES,
            periods=tuple(periods),
        )
        ic_series = al_perf.factor_information_coefficient(factor_data)
        mean_quant_ret, _ = al_perf.mean_return_by_quantile(factor_data, by_date=False)

        quantile_levels = sorted(factor_data["factor_quantile"].dropna().unique())
        # 将分位数标签统一为整数字符串，兼容 Parquet 列名与 NaN 导致的浮点转换
        turnover = pd.DataFrame(
            {
                _quantile_label(q): al_perf.quantile_turnover(
                    factor_data["factor_quantile"], q, period=1
                )
                for q in quantile_levels
            }
        )

        summary = AlphalensAdapter.extract_metrics(
            factor_data, ic_series, mean_quant_ret
        )

        family = factor_family(eval_name) if name is not None else None
        if name is not None:
            attempts_n = len(_load_attempts(self._base_dir, family)) + 1
        elif n_attempts is not None:
            attempts_n = n_attempts
        else:
            attempts_n = 1

        gate_passed, gate_detail = AlphalensAdapter.check_gate(
            summary, n_attempts=attempts_n
        )
        if name is not None:
            _record_attempt(
                self._base_dir,
                family,
                {
                    "date": datetime.now(timezone.utc).date().isoformat(),
                    "name": name,
                    "gate_passed": gate_passed,
                    "rank_ic_mean": summary["rank_ic_mean"],
                },
            )

        report_path = self._write_tearsheet(
            factor_data, eval_name, summary, gate_passed
        )

        return EvalResult(
            summary=summary,
            gate_passed=gate_passed,
            gate_detail=gate_detail,
            ic_series=ic_series,
            quantile_returns=mean_quant_ret,
            turnover=turnover,
            report_path=report_path,
        )

    # -- 阶段 4：注册落盘 -----------------------------------------------------

    def register(self, name: str, expr: str, result: EvalResult) -> None:
        """落盘原始因子值、更新 ``metadata.json`` 评估与尝试记录，并保存评估时序 Parquet 文件。"""
        name = _safe_component(name, "factor name")
        if self._last_raw is None:
            raise RuntimeError(
                "register() needs a factor frame from this pipeline "
                "(run compute()/preprocess()/evaluate() first)"
            )
        # 剔除可能残留的中性化辅助列，仅保留单因子列
        col = _factor_column(self._last_raw)
        frame = self._last_raw[["date", "ticker", col]].rename(columns={col: name})

        # 固定使用默认版本号 "1.0"，与 factor_storage 读取契约保持一致
        record = _FactorRecord(name, version="1.0", depends_on=_expr_columns(expr))
        factors_dir = self._base_dir / "factors"
        save_factor_full(record, frame, base_dir=str(factors_dir))

        family = factor_family(name)
        eval_date = datetime.now(timezone.utc).date().isoformat()
        attempts = _load_attempts(self._base_dir, family)
        # 补充记录未命名评估的尝试日志，确保 metadata 中的尝试记录非空
        if not any(
            e.get("name") == name and e.get("date") == eval_date for e in attempts
        ):
            _record_attempt(
                self._base_dir,
                family,
                {
                    "date": eval_date,
                    "name": name,
                    "gate_passed": result.gate_passed,
                    "rank_ic_mean": result.summary["rank_ic_mean"],
                },
            )
            attempts = _load_attempts(self._base_dir, family)
        meta_path = factors_dir / name / f"v{record.version}" / "metadata.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["eval"] = {
            **result.summary,
            "gate_passed": result.gate_passed,
            "eval_date": eval_date,
            "report": result.report_path,
        }
        meta["attempts"] = len(attempts)
        meta["attempt_log"] = attempts
        meta_path.write_text(
            json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

        out_dir = self._base_dir / "factor_eval" / name
        out_dir.mkdir(parents=True, exist_ok=True)
        result.ic_series.to_parquet(out_dir / "ic_series.parquet")
        result.quantile_returns.to_parquet(out_dir / "quantile_returns.parquet")
        result.turnover.to_parquet(out_dir / "turnover.parquet")

    # -- 内部辅助方法 ---------------------------------------------------------

    def _load_bars(self, symbols: Sequence[str], start: str, end: str) -> pd.DataFrame:
        return self._data_api.load_bars(
            list(symbols),
            start,
            end,
            freq="daily",
            adjust="forward",
            asset_type="equity",
        )

    def _write_tearsheet(
        self,
        factor_data: pd.DataFrame,
        eval_name: str,
        summary: dict,
        gate_passed: bool,
    ) -> str | None:
        eval_name = _safe_component(eval_name, "factor name")
        try:
            figures = self._capture_tearsheet(factor_data)
        except Exception as exc:  # 绘图异常不阻断评估主流程
            warnings.warn(f"tearsheet generation failed for {eval_name!r}: {exc}")
            return None
        date_tag = datetime.now(timezone.utc).strftime("%Y%m%d")
        self._reports_dir.mkdir(parents=True, exist_ok=True)
        path = self._reports_dir / f"{eval_name}_{date_tag}.html"
        path.write_text(
            _render_tearsheet_html(eval_name, summary, gate_passed, figures),
            encoding="utf-8",
        )
        return str(path)

    @staticmethod
    def _capture_tearsheet(factor_data: pd.DataFrame) -> list[str]:
        """运行 alphalens 完整 tearsheet 并拦截图形关闭操作以导出 base64 PNG 列表。"""
        import matplotlib.pyplot as plt
        from alphalens import tears as al_tears

        preexisting = set(plt.get_fignums())
        original_show, original_close = plt.show, plt.close
        plt.show = _noop_show
        plt.close = _noop_show
        try:
            al_tears.create_full_tear_sheet(
                factor_data, long_short=True, group_neutral=False, by_group=False
            )
            pngs: list[str] = []
            for num in sorted(set(plt.get_fignums()) - preexisting):
                buffer = io.BytesIO()
                plt.figure(num).savefig(
                    buffer, format="png", dpi=_TEARSHEET_DPI, bbox_inches="tight"
                )
                pngs.append(base64.b64encode(buffer.getvalue()).decode("ascii"))
        finally:
            plt.show, plt.close = original_show, original_close
            for num in set(plt.get_fignums()) - preexisting:
                plt.close(num)
        return pngs


def _render_tearsheet_html(
    eval_name: str, summary: dict, gate_passed: bool, figures: list[str]
) -> str:
    rows = "".join(
        f"<tr><td>{escape(str(k))}</td><td>{escape(str(v))}</td></tr>"
        for k, v in summary.items()
    )
    images = "".join(
        f'<p><img src="data:image/png;base64,{png}" alt="tearsheet figure"/></p>'
        for png in figures
    )
    verdict = "PASS" if gate_passed else "FAIL"
    return (
        '<!DOCTYPE html>\n<html><head><meta charset="utf-8"/>'
        f"<title>{escape(eval_name)} factor evaluation</title></head><body>\n"
        f"<h1>{escape(eval_name)} — gate {verdict}</h1>\n"
        f'<table border="1">{rows}</table>\n{images}\n</body></html>\n'
    )
