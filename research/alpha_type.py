"""策略配方治理：alphaType 注册表、7 维封闭标签、五态生命周期与只增审计日志。

提供配方注册与查询（``create_alpha_type``、``list_alpha_types``）、
状态流转与准入晋升（``transition_alpha_type``、``promote_alpha_type``、``log_audit``），
以及规则管道与日信号编排（``Rule``、``RulePipeline``、``build_portfolio``、``run_daily_signal`` 等）。
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from research.alpha_signal import DEFAULT_K, WEIGHT_METHODS, signal_to_weights
from core.config import DATA_ROOT

__all__ = [
    "REGISTRY_DIR",
    "TAG_DIMENSIONS",
    "Rule",
    "RulePipeline",
    "TopKFilter",
    "build_portfolio",
    "compute_turnover",
    "create_alpha_type",
    "create_rule",
    "list_alpha_types",
    "log_audit",
    "promote_alpha_type",
    "run_daily_signal",
    "send_alert",
    "transition_alpha_type",
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 常量定义
# ---------------------------------------------------------------------------

# 默认注册表目录，辅助函数允许注入 registry_dir 以便测试隔离
REGISTRY_DIR = DATA_ROOT / "registry"

# 7 维封闭枚举标签规范，创建与发布时必须全部填写
TAG_DIMENSIONS: dict[str, set[str]] = {
    "asset_type": {"stock", "etf", "convertible_bond", "futures", "option"},
    "frequency": {"1d", "1W", "1M", "intraday"},
    "holding_period": {"1d", "1W", "2W", "1M", "3M"},
    "style": {
        "momentum",
        "value",
        "low_vol",
        "quality",
        "size",
        "carry",
        "ml",
        "mixed",
    },
    "capacity": {"small", "medium", "large"},
    "turnover": {"low", "medium", "high"},
    "status": {
        "draft",
        "published",
        "production",
        "deprecated",
        "archived",
    },
}

# 生命周期合法流转边（主链顺序推进，任意非草稿态可退回 draft 重做）
_LIFECYCLE_LEGAL: set[tuple[str, str]] = {
    ("draft", "published"),
    ("published", "production"),
    ("production", "deprecated"),
    ("deprecated", "archived"),
    # 退回草稿重做
    ("published", "draft"),
    ("production", "draft"),
    ("deprecated", "draft"),
    ("archived", "draft"),
}

_LIFECYCLE_STATES = frozenset(TAG_DIMENSIONS["status"])

# 审计日志必需字段集合
_AUDIT_KEYS = {
    "action",
    "alpha_type_id",
    "from",
    "to",
    "metrics",
    "ts",
    "actor",
    "reason",
}

# 晋升生产门限阈值
_PROMOTE_IC_MIN = 0.04
_PROMOTE_IR_MIN = 0.5
_PROMOTE_SHARPE_MIN = 1.0

# 注册表元信息标准字段顺序（15 字段）
_META_FIELDS = [
    "alpha_type_id",
    "name",
    "description",
    "version",
    "signal_source",
    "rules",
    "params",
    "asset_pool",
    "frequency",
    "author",
    "created_at",
    "updated_at",
    "tags",
    "status",
    "config_snapshot",
]


# ---------------------------------------------------------------------------
# 标签校验
# ---------------------------------------------------------------------------


def _validate_tags(tags: dict[str, str], *, require_all: bool = True) -> None:
    """按 ``TAG_DIMENSIONS`` 校验标签字典；非法维度、非枚举值或缺少必填维度时抛出 ``ValueError``。"""
    if not isinstance(tags, dict):
        raise ValueError(f"tags must be a dict, got {type(tags).__name__}")
    for dim, value in tags.items():
        if dim not in TAG_DIMENSIONS:
            raise ValueError(
                f"unknown tag dimension: {dim!r}; valid: {sorted(TAG_DIMENSIONS)}"
            )
        if value not in TAG_DIMENSIONS[dim]:
            raise ValueError(
                f"tag {dim}={value!r} not in closed enum {sorted(TAG_DIMENSIONS[dim])}"
            )
    if require_all:
        missing = set(TAG_DIMENSIONS) - set(tags)
        if missing:
            raise ValueError(f"missing required tag dimension(s): {sorted(missing)}")


# ---------------------------------------------------------------------------
# 审计日志
# ---------------------------------------------------------------------------


def log_audit(
    event: dict[str, Any],
    *,
    registry_dir: Path | None = None,
) -> None:
    """向 ``audit_log.jsonl`` 追加写入一条审计日志；键集合必须严格匹配 ``_AUDIT_KEYS``。"""
    missing = _AUDIT_KEYS - set(event)
    if missing:
        raise ValueError(f"audit entry missing keys: {sorted(missing)}")
    extra = set(event) - _AUDIT_KEYS
    if extra:
        raise ValueError(f"audit entry has unknown keys: {sorted(extra)}")
    d = registry_dir or REGISTRY_DIR
    d.mkdir(parents=True, exist_ok=True)
    path = d / "audit_log.jsonl"
    line = json.dumps(event, ensure_ascii=False, default=str)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# 注册表持久化
# ---------------------------------------------------------------------------


def _load_alpha_types(registry_dir: Path) -> list[dict[str, Any]]:
    path = registry_dir / "alpha_types.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def _save_alpha_types(registry_dir: Path, records: list[dict[str, Any]]) -> None:
    registry_dir.mkdir(parents=True, exist_ok=True)
    path = registry_dir / "alpha_types.json"
    path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _load_publishes(registry_dir: Path) -> list[dict[str, Any]]:
    path = registry_dir / "alpha_type_publishes.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def _save_publishes(registry_dir: Path, records: list[dict[str, Any]]) -> None:
    registry_dir.mkdir(parents=True, exist_ok=True)
    path = registry_dir / "alpha_type_publishes.json"
    path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# 配方创建
# ---------------------------------------------------------------------------


def create_alpha_type(
    alpha: dict[str, Any],
    *,
    registry_dir: Path | None = None,
) -> dict[str, Any]:
    """注册新 alphaType 配方（校验配方字段、7 维标签及 ``major.minor`` 版本号），初始化为 ``draft`` 并记录审计。"""
    d = registry_dir or REGISTRY_DIR

    # 校验必填配方字段
    for field in ("signal_source", "rules", "params"):
        if field not in alpha:
            raise ValueError(f"missing required recipe field: {field!r}")

    # 校验 7 维标签完整性
    _validate_tags(alpha.get("tags", {}), require_all=True)

    # 校验版本号格式
    version = str(alpha.get("version", "1.0"))
    parts = version.split(".")
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        raise ValueError(f"version must be 'major.minor', got {version!r}")

    alpha_id = alpha.get("alpha_type_id")
    if not alpha_id:
        raise ValueError("alpha_type_id is required")

    # 校验 ID 唯一性
    records = _load_alpha_types(d)
    if any(r["alpha_type_id"] == alpha_id for r in records):
        raise ValueError(f"duplicate alpha_type_id: {alpha_id!r}")

    # 组装 15 字段元信息记录
    now = _now_iso()
    tags = dict(alpha["tags"])
    tags["status"] = "draft"  # 初始状态固定为 draft
    _defaults = {
        "alpha_type_id": alpha_id,
        "name": alpha.get("name", alpha_id),
        "description": alpha.get("description", ""),
        "version": version,
        "signal_source": alpha["signal_source"],
        "rules": alpha["rules"],
        "params": alpha["params"],
        "asset_pool": alpha.get("asset_pool", []),
        "frequency": alpha.get("frequency", "1d"),
        "author": alpha.get("author", ""),
        "created_at": now,
        "updated_at": now,
        "tags": tags,
        "status": "draft",
        "config_snapshot": alpha.get("config_snapshot", ""),
    }
    record = {f: _defaults[f] for f in _META_FIELDS}

    records.append(record)
    _save_alpha_types(d, records)

    # 记录创建审计日志
    log_audit(
        {
            "action": "create",
            "alpha_type_id": alpha_id,
            "from": None,
            "to": "draft",
            "metrics": None,
            "ts": now,
            "actor": alpha.get("author", "system"),
            "reason": "initial registration",
        },
        registry_dir=d,
    )
    return record


# ---------------------------------------------------------------------------
# 配方查询
# ---------------------------------------------------------------------------


def list_alpha_types(
    filters: dict[str, str] | None = None,
    *,
    registry_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """按标签维度等值过滤已注册配方，未传过滤条件时返回全部记录。"""
    d = registry_dir or REGISTRY_DIR
    records = _load_alpha_types(d)
    if not filters:
        return list(records)
    # 校验过滤条件是否符合枚举定义
    for dim, value in filters.items():
        if dim not in TAG_DIMENSIONS:
            raise ValueError(
                f"unknown filter dimension: {dim!r}; valid: {sorted(TAG_DIMENSIONS)}"
            )
        if value not in TAG_DIMENSIONS[dim]:
            raise ValueError(
                f"filter {dim}={value!r} not in closed enum "
                f"{sorted(TAG_DIMENSIONS[dim])}"
            )
    return [
        r
        for r in records
        if all(r.get("tags", {}).get(k) == v for k, v in filters.items())
    ]


# ---------------------------------------------------------------------------
# 生命周期流转
# ---------------------------------------------------------------------------


def transition_alpha_type(
    alpha: dict[str, Any],
    target: str,
    *,
    registry_dir: Path | None = None,
    actor: str = "system",
    reason: str = "",
) -> dict[str, Any]:
    """执行生命周期状态流转并同步发布记录与审计日志。

    合法流转：``draft→published→production→deprecated→archived`` 及任意非草稿态退回 ``draft``（次版本号 +1）。
    """
    d = registry_dir or REGISTRY_DIR
    current = alpha.get("status", "draft")
    if target not in _LIFECYCLE_STATES:
        raise ValueError(
            f"unknown lifecycle state: {target!r}; valid: {sorted(_LIFECYCLE_STATES)}"
        )
    if (current, target) not in _LIFECYCLE_LEGAL:
        raise ValueError(
            f"illegal lifecycle transition: {current!r} -> {target!r}; "
            f"legal: draft->published, published->production, "
            f"production->deprecated, deprecated->archived, x->draft"
        )

    # draft -> published 前置校验：配方完整且标签齐全
    if current == "draft" and target == "published":
        for field in ("signal_source", "rules", "params"):
            if field not in alpha:
                raise ValueError(f"missing required recipe field: {field!r}")
        _validate_tags(alpha.get("tags", {}), require_all=True)

    now = _now_iso()

    # 退回 draft 重做时递增次版本号
    if target == "draft" and current != "draft":
        ver = str(alpha.get("version", "1.0"))
        major, minor = ver.split(".")
        alpha["version"] = f"{major}.{int(minor) + 1}"

    # 更新状态与标签
    alpha["status"] = target
    alpha.setdefault("tags", {})["status"] = target
    alpha["updated_at"] = now

    # 同步更新发布记录表
    if target == "published":
        # 写入发布记录
        publishes = _load_publishes(d)
        publishes.append(
            {
                "publish_id": f"{alpha['alpha_type_id']}_{now}",
                "alpha_type_id": alpha["alpha_type_id"],
                "config_snapshot": alpha.get("config_snapshot", ""),
                "status": "published",
                "published_at": now,
                "adopted_at": None,
                "retired_at": None,
            }
        )
        _save_publishes(d, publishes)

    if target == "deprecated":
        publishes = _load_publishes(d)
        for p in reversed(publishes):
            if p["alpha_type_id"] == alpha["alpha_type_id"]:
                p["retired_at"] = now
                break
        _save_publishes(d, publishes)

    # 持久化更新后的配方记录
    records = _load_alpha_types(d)
    for i, r in enumerate(records):
        if r["alpha_type_id"] == alpha["alpha_type_id"]:
            records[i] = alpha
            break
    _save_alpha_types(d, records)

    # 写入状态变更审计日志
    log_audit(
        {
            "action": "transition",
            "alpha_type_id": alpha["alpha_type_id"],
            "from": current,
            "to": target,
            "metrics": None,
            "ts": now,
            "actor": actor,
            "reason": reason,
        },
        registry_dir=d,
    )

    return alpha


# ---------------------------------------------------------------------------
# 评估晋升门限
# ---------------------------------------------------------------------------


def promote_alpha_type(
    alpha: dict[str, Any],
    eval_report: dict[str, Any],
    *,
    registry_dir: Path | None = None,
    actor: str = "system",
) -> bool:
    """校验评估报告是否通过生产晋升门限（仅允许 ``published`` 状态）。

    门限要求：``walk_forward_passed``、``beats_equal_weight`` 为 True，且
    ``ic_mean > 0.04``、``ic_ir > 0.5``、``test_sharpe > 1.0``；无论通过与否均写审计日志。
    """
    d = registry_dir or REGISTRY_DIR

    if alpha.get("status") != "published":
        raise ValueError(
            f"promote requires status 'published', got {alpha.get('status')!r}"
        )

    metrics = eval_report.get("metrics", {})
    gate_ok = (
        eval_report.get("walk_forward_passed") is True
        and eval_report.get("beats_equal_weight") is True
        and metrics.get("ic_mean", 0) > _PROMOTE_IC_MIN
        and metrics.get("ic_ir", 0) > _PROMOTE_IR_MIN
        and metrics.get("test_sharpe", 0) > _PROMOTE_SHARPE_MIN
    )

    now = _now_iso()

    if gate_ok:
        # 晋升至 production
        transition_alpha_type(
            alpha,
            "production",
            registry_dir=d,
            actor=actor,
            reason="promote gate passed (§64.5)",
        )
        # 回填发布记录的采用时间
        publishes = _load_publishes(d)
        for p in reversed(publishes):
            if p["alpha_type_id"] == alpha["alpha_type_id"]:
                p["adopted_at"] = now
                p["status"] = "production"
                break
        _save_publishes(d, publishes)
    else:
        # 记录晋升被拒审计日志
        log_audit(
            {
                "action": "promote_rejected",
                "alpha_type_id": alpha["alpha_type_id"],
                "from": "published",
                "to": "published",
                "metrics": metrics,
                "ts": now,
                "actor": actor,
                "reason": "promote gate failed (§64.5)",
            },
            registry_dir=d,
        )

    return gate_ok


# ---------------------------------------------------------------------------
# 告警接口
# ---------------------------------------------------------------------------


def send_alert(
    level: str,
    msg: str,
    alert_type: str = "generic",
    alpha_type_id: str = "",
) -> None:
    """告警抽象接口，V1 通过标准日志输出告警信息。"""
    logger.warning(
        "[%s] %s alert_type=%s alpha_type_id=%s: %s",
        level,
        level,
        alert_type,
        alpha_type_id,
        msg,
    )


# ---------------------------------------------------------------------------
# 规则基类与串行管道
# ---------------------------------------------------------------------------

_RULE_REGISTRY: dict[str, type["Rule"]] = {}

# 合法规则类别
RULE_CATEGORIES = frozenset({"signal_transform", "filter", "position", "risk_gate"})


class Rule(ABC):
    """规则基类，实例化时按 ``param_schema`` 校验参数类型。"""

    rule_id: str = ""
    category: str = ""
    param_schema: dict[str, type] = {}

    def __init__(self, **params: Any) -> None:
        self._validate_params(params)
        self.params = params

    def _validate_params(self, params: dict[str, Any]) -> None:
        for key, expected_type in self.param_schema.items():
            if key in params:
                val = params[key]
                if not isinstance(val, expected_type):
                    raise ValueError(
                        f"rule {self.rule_id!r} param {key!r}: "
                        f"expected {expected_type.__name__}, got {type(val).__name__}"
                    )

    @abstractmethod
    def apply(self, data: pd.DataFrame, **params: Any) -> pd.DataFrame:
        """对输入数据执行规则变换，由子类实现。"""


class TopKFilter(Rule):
    """内置筛选规则：按日保留分数最高的前 *k* 只标的。"""

    rule_id = "top_k_filter"
    category = "filter"
    param_schema = {"k": int}

    def apply(self, data: pd.DataFrame, **params: Any) -> pd.DataFrame:
        k = params.get("k", self.params.get("k", DEFAULT_K))
        valid = data.dropna(subset=["score"])
        if valid.empty:
            return valid
        ranked = valid.sort_values(
            ["date", "score", "ticker"], ascending=[True, False, True]
        )
        return ranked.groupby("date", sort=True).head(k).reset_index(drop=True)


def create_rule(rule_id: str, **params: Any) -> Rule:
    """根据 ``rule_id`` 从注册表实例化规则对象。"""
    if rule_id not in _RULE_REGISTRY:
        raise ValueError(
            f"unknown rule_id: {rule_id!r}; registered: {sorted(_RULE_REGISTRY)}"
        )
    return _RULE_REGISTRY[rule_id](**params)


# 注册内置规则
_RULE_REGISTRY["top_k_filter"] = TopKFilter


class RulePipeline:
    """串行执行规则列表的管道。"""

    def __init__(self, rules: list[Rule]) -> None:
        self.rules = list(rules)

    def run(
        self, data: pd.DataFrame, params: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        """依次执行管道中的规则，将前一步输出作为下一步输入。"""
        result = data
        for rule in self.rules:
            result = rule.apply(result, **(params or {}))
        return result


# ---------------------------------------------------------------------------
# 换手率计算
# ---------------------------------------------------------------------------


def compute_turnover(
    new_weights: pd.DataFrame,
    old_weights: pd.DataFrame,
) -> float:
    """按 ``ticker`` 外连接计算双边半换手率 ``Σ|Δw| / 2``，缺失行权重按 0 处理。"""
    merged = new_weights.merge(
        old_weights,
        on=["ticker"],
        how="outer",
        suffixes=("_new", "_old"),
    )
    merged["target_weight_new"] = merged["target_weight_new"].fillna(0.0)
    merged["target_weight_old"] = merged["target_weight_old"].fillna(0.0)
    delta = merged["target_weight_new"] - merged["target_weight_old"]
    return float(delta.abs().sum() / 2.0)


# ---------------------------------------------------------------------------
# 组合权重构建
# ---------------------------------------------------------------------------


def build_portfolio(
    signal: pd.DataFrame,
    alpha: dict[str, Any],
    prev_weights: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """将信号转为目标权重并执行总仓位缩放及集中度、换手率告警检查。

    求解失败或输出为空时回退按日 ``1/N`` 等权；输入信号为空时抛出 ``ValueError``。
    """
    if signal.empty:
        raise ValueError("empty signal frame: cannot build portfolio")

    params = alpha.get("params", {})
    total_position = params.get("total_position", 1.0)
    method = params.get("weight_method", "top_k")
    if method not in WEIGHT_METHODS:
        method = "top_k"

    # 从 TopKFilter 规则读取 k，未配置则使用默认值
    k = DEFAULT_K
    for rule_entry in alpha.get("rules", []):
        if isinstance(rule_entry, dict) and rule_entry.get("rule_id") == "top_k_filter":
            k = rule_entry.get("params", {}).get("k", DEFAULT_K)
            break

    # 求解权重，异常或空结果时回退等权
    try:
        weights = signal_to_weights(signal, method=method, k=k)
    except Exception:
        weights = _equal_weight_fallback(signal)
    # 防止全 NaN 或非正分导致空仓
    if weights.empty:
        weights = _equal_weight_fallback(signal)

    # 按总仓位比例缩放
    weights["target_weight"] = weights["target_weight"] * total_position

    # 单票集中度检查（仅告警，不裁剪）
    max_single = params.get("max_single_weight", 0.25)
    if not weights.empty:
        per_date_max = weights.groupby("date")["target_weight"].max()
        if (per_date_max > max_single).any():
            send_alert(
                "MEDIUM",
                f"concentration alert: max weight exceeds {max_single}",
                alert_type="concentration",
                alpha_type_id=alpha.get("alpha_type_id", ""),
            )

    # 换手率检查（仅告警，不裁剪）
    if prev_weights is not None and not prev_weights.empty:
        max_turnover = params.get("max_turnover", 1.5)
        turnover = compute_turnover(weights, prev_weights)
        if turnover > max_turnover:
            send_alert(
                "MEDIUM",
                f"turnover alert: {turnover:.2f} exceeds {max_turnover}",
                alert_type="turnover",
                alpha_type_id=alpha.get("alpha_type_id", ""),
            )

    return weights


def _equal_weight_fallback(signal: pd.DataFrame) -> pd.DataFrame:
    """按日对信号内全部标的赋 ``1/N`` 等权。"""
    counts = signal.groupby("date")["ticker"].transform("count")
    out = signal[["date", "ticker"]].copy()
    out["target_weight"] = 1.0 / counts
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 日信号生产
# ---------------------------------------------------------------------------


def run_daily_signal(
    alpha: dict[str, Any],
    date: str,
    prev_portfolio: pd.DataFrame | None = None,
    *,
    signal_fn: Any,
) -> dict[str, Any]:
    """日信号生产编排（失败重试 1 次）。

    成功返回 ``{"status": "SUCCESS", "portfolio": weights}``；两次均失败则发 ``HIGH`` 告警并
    原样返回 ``{"status": "HOLD", "portfolio": prev_portfolio}``。
    """
    alpha_id = alpha.get("alpha_type_id", "unknown")

    for attempt in range(2):  # 最多尝试 2 次（失败重试 1 次）
        try:
            signal = signal_fn(alpha, date)
            weights = build_portfolio(signal, alpha, prev_portfolio)
            return {"status": "SUCCESS", "portfolio": weights}
        except Exception as e:
            if attempt == 0:
                continue  # 首次失败重试
            # 两次均失败则保持原持仓并告警
            send_alert(
                "HIGH",
                f"signal failed {alpha_id}: {e}",
                alert_type="signal_failure",
                alpha_type_id=alpha_id,
            )
            return {"status": "HOLD", "portfolio": prev_portfolio}

    # 兜底返回以满足静态类型检查
    return {"status": "HOLD", "portfolio": prev_portfolio}
