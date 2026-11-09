"""Strategy recipe governance — alphaType registry, 7-dim tags, lifecycle, audit.

Admission, tag validation, five-state lifecycle, and append-only audit for
registered strategy recipes.  An alphaType is a recipe (``signal_source`` +
``rules`` + ``params``), not code.  See ``docs/design/research/alpha_type.md``
and §65 of the archive.

Iteration 1 scope (ac-1, ac-2, ac-4): ``TAG_DIMENSIONS``, ``REGISTRY_DIR``,
``create_alpha_type``, ``list_alpha_types``, ``transition_alpha_type``,
``promote_alpha_type``, ``log_audit``.

Iteration 2 scope (ac-3, ac-5): ``Rule`` ABC + ``RulePipeline``,
``run_daily_signal``, ``build_portfolio``, ``compute_turnover``,
``send_alert`` seam.
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
# Constants
# ---------------------------------------------------------------------------

#: Default registry directory (D5).  Helpers accept ``registry_dir=None``
#: so tests can point at ``tmp_path``.
REGISTRY_DIR = DATA_ROOT / "registry"

#: 7-dim closed-enum tag schema (D2).  Every value must be in its dimension's
#: set; free text is forbidden.  All 7 dims required at create / publish.
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

# Lifecycle legal transitions (D1).
# draft->published->production->deprecated->archived, plus x->draft for rework.
_LIFECYCLE_LEGAL: set[tuple[str, str]] = {
    ("draft", "published"),
    ("published", "production"),
    ("production", "deprecated"),
    ("deprecated", "archived"),
    # any -> draft (rework)
    ("published", "draft"),
    ("production", "draft"),
    ("deprecated", "draft"),
    ("archived", "draft"),
}

_LIFECYCLE_STATES = frozenset(TAG_DIMENSIONS["status"])

# Audit entry keys (D3): the settled item's actor + reason added to the base five.
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

# promote_alpha_type gate thresholds (D4, §64.5).
_PROMOTE_IC_MIN = 0.04
_PROMOTE_IR_MIN = 0.5
_PROMOTE_SHARPE_MIN = 1.0

# Meta-info field order (§65.2, 15 fields).
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
# Tag validation (D2)
# ---------------------------------------------------------------------------


def _validate_tags(tags: dict[str, str], *, require_all: bool = True) -> None:
    """Validate tag dict against TAG_DIMENSIONS (D2).

    Out-of-enum value or free text → ``ValueError``.
    Missing dim at create/publish → ``ValueError`` (require_all=True).
    """
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
# Audit log (D3)
# ---------------------------------------------------------------------------


def log_audit(
    event: dict[str, Any],
    *,
    registry_dir: Path | None = None,
) -> None:
    """Append one JSON line to ``audit_log.jsonl`` (D3).

    Append-only: corrections are new entries; nothing ever rewrites prior bytes.
    Required keys: ``{action, alpha_type_id, from, to, metrics, ts, actor, reason}``.
    """
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
# Registry persistence (D5)
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
# create_alpha_type (D5)
# ---------------------------------------------------------------------------


def create_alpha_type(
    alpha: dict[str, Any],
    *,
    registry_dir: Path | None = None,
) -> dict[str, Any]:
    """Register a new alphaType recipe (D5).

    Validates 7-dim tags (D2) + recipe completeness (``signal_source``,
    ``rules``, ``params``), seeds status ``draft``, appends the 15-field
    meta-info record.  Duplicate ``alpha_type_id`` → ``ValueError``.
    Version string format: ``"major.minor"``.
    """
    d = registry_dir or REGISTRY_DIR

    # Required recipe fields.
    for field in ("signal_source", "rules", "params"):
        if field not in alpha:
            raise ValueError(f"missing required recipe field: {field!r}")

    # Validate tags (D2: all 7 dims required at create).
    _validate_tags(alpha.get("tags", {}), require_all=True)

    # Validate version format.
    version = str(alpha.get("version", "1.0"))
    parts = version.split(".")
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        raise ValueError(f"version must be 'major.minor', got {version!r}")

    alpha_id = alpha.get("alpha_type_id")
    if not alpha_id:
        raise ValueError("alpha_type_id is required")

    # Check for duplicate.
    records = _load_alpha_types(d)
    if any(r["alpha_type_id"] == alpha_id for r in records):
        raise ValueError(f"duplicate alpha_type_id: {alpha_id!r}")

    # Build the 15-field meta-info record (§65.2, field order from _META_FIELDS).
    now = _now_iso()
    tags = dict(alpha["tags"])
    tags["status"] = "draft"  # auto-seed status
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

    # Audit entry (D3): creation is a state change and must be recorded.
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
# list_alpha_types (D2)
# ---------------------------------------------------------------------------


def list_alpha_types(
    filters: dict[str, str] | None = None,
    *,
    registry_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Filter registered alphaTypes by tag dimensions (D2).

    Example::

        list_alpha_types({"style": "momentum", "asset_type": "etf"})
    """
    d = registry_dir or REGISTRY_DIR
    records = _load_alpha_types(d)
    if not filters:
        return list(records)
    # Validate filter keys against TAG_DIMENSIONS.
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
# Lifecycle transition (D1, D6)
# ---------------------------------------------------------------------------


def transition_alpha_type(
    alpha: dict[str, Any],
    target: str,
    *,
    registry_dir: Path | None = None,
    actor: str = "system",
    reason: str = "",
) -> dict[str, Any]:
    """Lifecycle state machine (D1).

    Legal transitions: ``draft→published``, ``published→production``,
    ``production→deprecated``, ``deprecated→archived`` + ``x→draft`` (rework).
    Self-transitions and every other pair raise ``ValueError``.

    Table actions ride the transition (D1):
    - ``draft→published`` writes a publish record (``published_at``).
    - ``production`` stamps ``adopted_at`` (via promote gate D4).
    - ``deprecated`` stamps ``retired_at``.
    - ``tags["status"]`` auto-follows the state.
    - ``any→draft`` rework bumps version minor+1 (D6).
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

    # draft→published precondition: recipe complete + tags full (D1).
    if current == "draft" and target == "published":
        for field in ("signal_source", "rules", "params"):
            if field not in alpha:
                raise ValueError(f"missing required recipe field: {field!r}")
        _validate_tags(alpha.get("tags", {}), require_all=True)

    now = _now_iso()

    # Version bump on rework (D6).
    if target == "draft" and current != "draft":
        ver = str(alpha.get("version", "1.0"))
        major, minor = ver.split(".")
        alpha["version"] = f"{major}.{int(minor) + 1}"

    # Update status and tags.
    alpha["status"] = target
    alpha.setdefault("tags", {})["status"] = target
    alpha["updated_at"] = now

    # Table actions (D1).
    if target == "published":
        # Write publish record.
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

    # Persist updated alpha record.
    records = _load_alpha_types(d)
    for i, r in enumerate(records):
        if r["alpha_type_id"] == alpha["alpha_type_id"]:
            records[i] = alpha
            break
    _save_alpha_types(d, records)

    # Audit entry (D3).
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
# promote_alpha_type (D4)
# ---------------------------------------------------------------------------


def promote_alpha_type(
    alpha: dict[str, Any],
    eval_report: dict[str, Any],
    *,
    registry_dir: Path | None = None,
    actor: str = "system",
) -> bool:
    """Promote to production if the eval gate passes (D4).

    Precondition: ``alpha["status"] == "published"`` (it is the
    published→production transition; else ``ValueError``).

    Gate (§64.5, strict ``>``): ``walk_forward_passed`` AND
    ``beats_equal_weight`` AND ``metrics.ic_mean > 0.04`` AND
    ``metrics.ic_ir > 0.5`` AND ``metrics.test_sharpe > 1.0``.

    Writes a D3 audit entry on both outcomes.  On ok, sets status to
    ``production`` + ``adopted_at``.
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
        # Transition to production (rides D1 table actions).
        transition_alpha_type(
            alpha,
            "production",
            registry_dir=d,
            actor=actor,
            reason="promote gate passed (§64.5)",
        )
        # Stamp adopted_at on publish record.
        publishes = _load_publishes(d)
        for p in reversed(publishes):
            if p["alpha_type_id"] == alpha["alpha_type_id"]:
                p["adopted_at"] = now
                p["status"] = "production"
                break
        _save_publishes(d, publishes)
    else:
        # Audit rejection.
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
# send_alert seam (D10)
# ---------------------------------------------------------------------------


def send_alert(
    level: str,
    msg: str,
    alert_type: str = "generic",
    alpha_type_id: str = "",
) -> None:
    """Alert seam (D10).  V1 body logs via std logging; monkeypatched in tests.

    S12 swaps the body to ``ops/alerting.py`` when it lands.
    Signature matches ``ops/alerting.md`` exactly.
    """
    logger.warning(
        "[%s] %s alert_type=%s alpha_type_id=%s: %s",
        level,
        level,
        alert_type,
        alpha_type_id,
        msg,
    )


# ---------------------------------------------------------------------------
# Rule ABC + RulePipeline (D11)
# ---------------------------------------------------------------------------

_RULE_REGISTRY: dict[str, type["Rule"]] = {}

#: Valid rule categories (D11).
RULE_CATEGORIES = frozenset({"signal_transform", "filter", "position", "risk_gate"})


class Rule(ABC):
    """Base class for all rules (D11).

    Class attrs: ``rule_id``, ``category``, ``param_schema``.
    Params are type-checked against ``param_schema`` at instantiation.
    """

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
        """Transform data.  Must be implemented by subclasses."""


class TopKFilter(Rule):
    """Keep top-k names per date by score (D11: V1 concrete rule)."""

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
    """Factory over the module rule registry (D11)."""
    if rule_id not in _RULE_REGISTRY:
        raise ValueError(
            f"unknown rule_id: {rule_id!r}; registered: {sorted(_RULE_REGISTRY)}"
        )
    return _RULE_REGISTRY[rule_id](**params)


# Register V1 rules.
_RULE_REGISTRY["top_k_filter"] = TopKFilter


class RulePipeline:
    """Run rules serially (D11: no parallel/nested composition).

    Container shape: ``list[Rule]`` — rules carry their own params at
    instantiation (design doc's sketch; the archive sketch ``(rule_id, params)``
    tuples is the rejected alternative).
    """

    def __init__(self, rules: list[Rule]) -> None:
        self.rules = list(rules)

    def run(
        self, data: pd.DataFrame, params: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        """Apply rules in sequence; each receives the prior rule's output."""
        result = data
        for rule in self.rules:
            result = rule.apply(result, **(params or {}))
        return result


# ---------------------------------------------------------------------------
# compute_turnover (D9)
# ---------------------------------------------------------------------------


def compute_turnover(
    new_weights: pd.DataFrame,
    old_weights: pd.DataFrame,
) -> float:
    """Turnover = Σ|Δw| / 2 over the outer join on ticker (D9).

    Joins on ``ticker`` alone (not ``[date, ticker]``) so cross-date
    comparisons work: the turnover question is "how much did the book
    change", which compares weights per ticker across the two frames
    regardless of their date labels.  Missing rows treated as 0.
    """
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
# build_portfolio (D8)
# ---------------------------------------------------------------------------


def build_portfolio(
    signal: pd.DataFrame,
    alpha: dict[str, Any],
    prev_weights: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Signal → target weights with constraint checks (D8).

    Solve = ``signal_to_weights(signal, method, k)``.  method from
    ``alpha["params"].get("weight_method", "top_k")``; k from the recipe's
    TopKFilter rule entry ``params.k``, else ``DEFAULT_K``.  Scale by
    ``params.total_position`` default 1.0.

    Concentration check (alert-only): per-date max weight >
    ``params.max_single_weight`` default 0.25 → ``send_alert("MEDIUM")``,
    no correction.

    Turnover check (alert-only, when *prev_weights* given): turnover >
    ``params.max_turnover`` default 1.5 → ``send_alert("MEDIUM")``, no trim.

    Solve failure → equal-weight fallback (1/N per date, scaled by
    total_position); never an empty book.  Empty signal → ``ValueError``.
    """
    if signal.empty:
        raise ValueError("empty signal frame: cannot build portfolio")

    params = alpha.get("params", {})
    total_position = params.get("total_position", 1.0)
    method = params.get("weight_method", "top_k")
    if method not in WEIGHT_METHODS:
        method = "top_k"

    # Find k from TopKFilter rule entry, else default.
    k = DEFAULT_K
    for rule_entry in alpha.get("rules", []):
        if isinstance(rule_entry, dict) and rule_entry.get("rule_id") == "top_k_filter":
            k = rule_entry.get("params", {}).get("k", DEFAULT_K)
            break

    # Solve (D8: analytical, rules are transforms, weights are solver output).
    try:
        weights = signal_to_weights(signal, method=method, k=k)
    except Exception:
        weights = _equal_weight_fallback(signal)
    # Never an empty book: solver may succeed but yield no rows (e.g. all
    # scores NaN or non-positive under proportional).
    if weights.empty:
        weights = _equal_weight_fallback(signal)

    # Scale by total_position.
    weights["target_weight"] = weights["target_weight"] * total_position

    # Concentration check (alert-only, no correction).
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

    # Turnover check (alert-only, no trim).
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
    """1/N equal weight per date over the signal's tickers (D8 fallback)."""
    counts = signal.groupby("date")["ticker"].transform("count")
    out = signal[["date", "ticker"]].copy()
    out["target_weight"] = 1.0 / counts
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# run_daily_signal (D7)
# ---------------------------------------------------------------------------


def run_daily_signal(
    alpha: dict[str, Any],
    date: str,
    prev_portfolio: pd.DataFrame | None = None,
    *,
    signal_fn: Any,
) -> dict[str, Any]:
    """Daily signal production (D7).  Failure → hold, never liquidate.

    ``signal_fn(alpha, date) -> [date, ticker, score]`` is a required keyword
    (signal computation is the alpha_signal.md boundary; S12 injects the
    producer).

    At most 2 attempts (retry 1).  Success →
    ``{"status": "SUCCESS", "portfolio": weights}``.  Both attempts raise →
    ``send_alert("HIGH", ...)`` and
    ``{"status": "HOLD", "portfolio": prev_portfolio}`` — previous book
    passes through untouched; an empty/liquidating book is never fabricated.
    No persistence inside (S12 owns the pipeline).
    """
    alpha_id = alpha.get("alpha_type_id", "unknown")

    for attempt in range(2):  # retry 1 = 2 attempts total
        try:
            signal = signal_fn(alpha, date)
            weights = build_portfolio(signal, alpha, prev_portfolio)
            return {"status": "SUCCESS", "portfolio": weights}
        except Exception as e:
            if attempt == 0:
                continue  # retry once
            # Both attempts failed: HOLD, never liquidate.
            send_alert(
                "HIGH",
                f"signal failed {alpha_id}: {e}",
                alert_type="signal_failure",
                alpha_type_id=alpha_id,
            )
            return {"status": "HOLD", "portfolio": prev_portfolio}

    # Unreachable, but keeps type checkers happy.
    return {"status": "HOLD", "portfolio": prev_portfolio}
