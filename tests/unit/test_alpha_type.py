"""Tests for research.alpha_type — governance + runtime (S11-T3).

Covers AC1 (7-dim closed-enum tags), AC2 (5-state lifecycle),
AC3 (run_daily_signal failure → HOLD), AC4 (audit append-only),
and AC5 (build_portfolio analytical + alerts + fallback).
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from research.alpha_type import (
    TAG_DIMENSIONS,
    RulePipeline,
    TopKFilter,
    build_portfolio,
    compute_turnover,
    create_alpha_type,
    create_rule,
    list_alpha_types,
    log_audit,
    promote_alpha_type,
    run_daily_signal,
    transition_alpha_type,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_VALID_RECIPE = {
    "alpha_type_id": "mom_20d",
    "name": "Momentum 20D",
    "description": "20-day momentum",
    "version": "1.0",
    "signal_source": {"type": "multi_factor", "config": {}},
    "rules": [{"rule_id": "top_k", "params": {"k": 20}}],
    "params": {"weight_method": "top_k", "total_position": 1.0},
    "asset_pool": ["000001.SZ", "600000.SH"],
    "frequency": "1d",
    "author": "test",
    "tags": {
        "asset_type": "stock",
        "frequency": "1d",
        "holding_period": "1M",
        "style": "momentum",
        "capacity": "medium",
        "turnover": "low",
        "status": "draft",
    },
}

_VALID_EVAL = {
    "walk_forward_passed": True,
    "beats_equal_weight": True,
    "metrics": {"ic_mean": 0.05, "ic_ir": 0.6, "test_sharpe": 1.2},
}


@pytest.fixture()
def registry_dir(tmp_path):
    d = tmp_path / "registry"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# AC1 — 7-dim tags closed enumeration
# ---------------------------------------------------------------------------


class TestTagDimensions:
    """AC1: TAG_DIMENSIONS is 7 dims, closed enums, free text rejected."""

    def test_seven_dimensions(self):
        assert len(TAG_DIMENSIONS) == 7
        expected = {
            "asset_type",
            "frequency",
            "holding_period",
            "style",
            "capacity",
            "turnover",
            "status",
        }
        assert set(TAG_DIMENSIONS) == expected

    def test_out_of_enum_rejected(self, registry_dir):
        alpha = {**_VALID_RECIPE, "tags": {**_VALID_RECIPE["tags"], "style": "trend"}}
        with pytest.raises(ValueError, match="not in closed enum"):
            create_alpha_type(alpha, registry_dir=registry_dir)

    def test_free_text_rejected(self, registry_dir):
        alpha = {
            **_VALID_RECIPE,
            "tags": {**_VALID_RECIPE["tags"], "asset_type": "my_custom_type"},
        }
        with pytest.raises(ValueError, match="not in closed enum"):
            create_alpha_type(alpha, registry_dir=registry_dir)

    def test_missing_dim_rejected_at_create(self, registry_dir):
        tags = {k: v for k, v in _VALID_RECIPE["tags"].items() if k != "capacity"}
        alpha = {**_VALID_RECIPE, "tags": tags}
        with pytest.raises(ValueError, match="missing required tag dimension"):
            create_alpha_type(alpha, registry_dir=registry_dir)

    def test_unknown_dimension_rejected(self, registry_dir):
        alpha = {
            **_VALID_RECIPE,
            "tags": {**_VALID_RECIPE["tags"], "unknown_dim": "value"},
        }
        with pytest.raises(ValueError, match="unknown tag dimension"):
            create_alpha_type(alpha, registry_dir=registry_dir)

    def test_per_dim_rejection_each_dimension(self, registry_dir):
        """Each of the 7 dims rejects an out-of-enum value."""
        bad_values = {
            "asset_type": "crypto",
            "frequency": "tick",
            "holding_period": "1Y",
            "style": "arbitrage",
            "capacity": "huge",
            "turnover": "ultra",
            "status": "active",
        }
        for dim, bad_val in bad_values.items():
            tags = {**_VALID_RECIPE["tags"], dim: bad_val}
            alpha = {**_VALID_RECIPE, "tags": tags, "alpha_type_id": f"test_{dim}"}
            with pytest.raises(ValueError, match="not in closed enum"):
                create_alpha_type(alpha, registry_dir=registry_dir)

    def test_valid_tags_accepted(self, registry_dir):
        record = create_alpha_type(_VALID_RECIPE, registry_dir=registry_dir)
        assert record["alpha_type_id"] == "mom_20d"
        assert record["status"] == "draft"
        assert record["tags"]["status"] == "draft"

    def test_list_alpha_types_filter_by_tag(self, registry_dir):
        create_alpha_type(_VALID_RECIPE, registry_dir=registry_dir)
        results = list_alpha_types({"style": "momentum"}, registry_dir=registry_dir)
        assert len(results) == 1
        assert results[0]["alpha_type_id"] == "mom_20d"

    def test_list_alpha_types_filter_no_match(self, registry_dir):
        create_alpha_type(_VALID_RECIPE, registry_dir=registry_dir)
        results = list_alpha_types({"style": "value"}, registry_dir=registry_dir)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# AC2 — 5-state lifecycle transitions
# ---------------------------------------------------------------------------


class TestLifecycleTransitions:
    """AC2: legal set validated, self/reverse/skip raise ValueError."""

    _counter = 0

    def _make_alpha(self, registry_dir, status="draft"):
        self._counter += 1
        recipe = {
            **_VALID_RECIPE,
            "alpha_type_id": f"test_lifecycle_{self._counter}",
        }
        alpha = create_alpha_type(recipe, registry_dir=registry_dir)
        alpha["status"] = status
        alpha["tags"]["status"] = status
        return alpha

    def test_legal_chain(self, registry_dir):
        """draft→published→production→deprecated→archived."""
        alpha = self._make_alpha(registry_dir, "draft")
        transition_alpha_type(alpha, "published", registry_dir=registry_dir)
        assert alpha["status"] == "published"
        transition_alpha_type(alpha, "production", registry_dir=registry_dir)
        assert alpha["status"] == "production"
        transition_alpha_type(alpha, "deprecated", registry_dir=registry_dir)
        assert alpha["status"] == "deprecated"
        transition_alpha_type(alpha, "archived", registry_dir=registry_dir)
        assert alpha["status"] == "archived"

    def test_any_to_draft_legal(self, registry_dir):
        """x→draft is legal for every x != draft."""
        for state in ("published", "production", "deprecated", "archived"):
            alpha = self._make_alpha(registry_dir, state)
            transition_alpha_type(alpha, "draft", registry_dir=registry_dir)
            assert alpha["status"] == "draft"

    def test_self_transition_rejected(self, registry_dir):
        for state in ("draft", "published", "production", "deprecated", "archived"):
            alpha = self._make_alpha(registry_dir, state)
            with pytest.raises(ValueError, match="illegal lifecycle transition"):
                transition_alpha_type(alpha, state, registry_dir=registry_dir)

    def test_reverse_transitions_rejected(self, registry_dir):
        """Reverse pairs that are not x→draft are illegal."""
        illegal = [
            ("production", "published"),
            ("deprecated", "production"),
            ("archived", "deprecated"),
        ]
        for current, target in illegal:
            alpha = self._make_alpha(registry_dir, current)
            with pytest.raises(ValueError, match="illegal lifecycle transition"):
                transition_alpha_type(alpha, target, registry_dir=registry_dir)

    def test_skip_transitions_rejected(self, registry_dir):
        """Skip-ahead pairs are illegal."""
        illegal = [
            ("draft", "production"),
            ("draft", "deprecated"),
            ("draft", "archived"),
            ("published", "deprecated"),
            ("published", "archived"),
            ("production", "archived"),
        ]
        for current, target in illegal:
            alpha = self._make_alpha(registry_dir, current)
            with pytest.raises(ValueError, match="illegal lifecycle transition"):
                transition_alpha_type(alpha, target, registry_dir=registry_dir)

    def test_tags_status_auto_follows(self, registry_dir):
        alpha = self._make_alpha(registry_dir, "draft")
        transition_alpha_type(alpha, "published", registry_dir=registry_dir)
        assert alpha["tags"]["status"] == "published"

    def test_rework_bumps_version_minor(self, registry_dir):
        """D6: any→draft rework bumps version minor+1."""
        alpha = self._make_alpha(registry_dir, "published")
        alpha["version"] = "1.0"
        transition_alpha_type(alpha, "draft", registry_dir=registry_dir)
        assert alpha["version"] == "1.1"

    def test_unknown_target_rejected(self, registry_dir):
        alpha = self._make_alpha(registry_dir, "draft")
        with pytest.raises(ValueError, match="unknown lifecycle state"):
            transition_alpha_type(alpha, "active", registry_dir=registry_dir)

    def test_publish_revalidates_recipe(self, registry_dir):
        """Review fix 4: draft→published re-checks recipe completeness."""
        alpha = self._make_alpha(registry_dir, "draft")
        del alpha["signal_source"]  # break the recipe after creation
        with pytest.raises(ValueError, match="missing required recipe field"):
            transition_alpha_type(alpha, "published", registry_dir=registry_dir)


# ---------------------------------------------------------------------------
# AC4 — audit_log.jsonl append-only
# ---------------------------------------------------------------------------


class TestAuditLog:
    """AC4: append-only, corrections are new entries, prior bytes unchanged."""

    def test_log_audit_appends(self, registry_dir):
        entry = {
            "action": "create",
            "alpha_type_id": "mom_20d",
            "from": None,
            "to": "draft",
            "metrics": None,
            "ts": "2026-01-01T00:00:00Z",
            "actor": "test",
            "reason": "initial creation",
        }
        log_audit(entry, registry_dir=registry_dir)
        path = registry_dir / "audit_log.jsonl"
        assert path.exists()
        lines = path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["action"] == "create"
        assert parsed["alpha_type_id"] == "mom_20d"

    def test_append_only_prior_bytes_unchanged(self, registry_dir):
        """A correcting append must not rewrite prior bytes."""
        entry1 = {
            "action": "create",
            "alpha_type_id": "mom_20d",
            "from": None,
            "to": "draft",
            "metrics": None,
            "ts": "2026-01-01T00:00:00Z",
            "actor": "test",
            "reason": "initial",
        }
        log_audit(entry1, registry_dir=registry_dir)
        path = registry_dir / "audit_log.jsonl"
        first_bytes = path.read_bytes()

        entry2 = {
            "action": "correct",
            "alpha_type_id": "mom_20d",
            "from": "draft",
            "to": "draft",
            "metrics": {"ic_mean": 0.05},
            "ts": "2026-01-02T00:00:00Z",
            "actor": "test",
            "reason": "corrected metrics",
        }
        log_audit(entry2, registry_dir=registry_dir)
        after_bytes = path.read_bytes()

        # Prior bytes are a prefix of the new file.
        assert after_bytes[: len(first_bytes)] == first_bytes
        assert len(after_bytes) > len(first_bytes)

    def test_missing_keys_rejected(self, registry_dir):
        incomplete = {"action": "test", "alpha_type_id": "x"}
        with pytest.raises(ValueError, match="missing keys"):
            log_audit(incomplete, registry_dir=registry_dir)

    def test_unknown_keys_rejected(self, registry_dir):
        entry = {
            "action": "test",
            "alpha_type_id": "x",
            "from": None,
            "to": "draft",
            "metrics": None,
            "ts": "2026-01-01T00:00:00Z",
            "actor": "test",
            "reason": "",
            "extra_field": "bad",
        }
        with pytest.raises(ValueError, match="unknown keys"):
            log_audit(entry, registry_dir=registry_dir)

    def test_transition_writes_audit(self, registry_dir):
        alpha = create_alpha_type(_VALID_RECIPE, registry_dir=registry_dir)
        transition_alpha_type(alpha, "published", registry_dir=registry_dir)
        path = registry_dir / "audit_log.jsonl"
        lines = path.read_text(encoding="utf-8").strip().split("\n")
        # create_alpha_type writes one entry; transition writes one more.
        assert len(lines) == 2
        last = json.loads(lines[-1])
        assert last["action"] == "transition"
        assert last["from"] == "draft"
        assert last["to"] == "published"

    def test_create_writes_audit(self, registry_dir):
        """Review fix 3: creation is a state change and must be recorded."""
        create_alpha_type(_VALID_RECIPE, registry_dir=registry_dir)
        path = registry_dir / "audit_log.jsonl"
        lines = path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["action"] == "create"
        assert entry["alpha_type_id"] == "mom_20d"
        assert entry["from"] is None
        assert entry["to"] == "draft"


# ---------------------------------------------------------------------------
# Promote gate (D4) — bonus coverage for governance completeness
# ---------------------------------------------------------------------------


class TestPromoteGate:
    """D4: promote gate validates eval_report; audit on both outcomes."""

    def test_promote_passes_gate(self, registry_dir):
        alpha = create_alpha_type(_VALID_RECIPE, registry_dir=registry_dir)
        transition_alpha_type(alpha, "published", registry_dir=registry_dir)
        ok = promote_alpha_type(alpha, _VALID_EVAL, registry_dir=registry_dir)
        assert ok is True
        assert alpha["status"] == "production"

    def test_promote_fails_gate(self, registry_dir):
        alpha = create_alpha_type(_VALID_RECIPE, registry_dir=registry_dir)
        transition_alpha_type(alpha, "published", registry_dir=registry_dir)
        bad_eval = {
            "walk_forward_passed": True,
            "beats_equal_weight": True,
            "metrics": {"ic_mean": 0.03, "ic_ir": 0.6, "test_sharpe": 1.2},
        }
        ok = promote_alpha_type(alpha, bad_eval, registry_dir=registry_dir)
        assert ok is False
        assert alpha["status"] == "published"

    def test_promote_requires_published(self, registry_dir):
        alpha = create_alpha_type(_VALID_RECIPE, registry_dir=registry_dir)
        with pytest.raises(ValueError, match="promote requires status"):
            promote_alpha_type(alpha, _VALID_EVAL, registry_dir=registry_dir)

    def test_promote_boundary_ic(self, registry_dir):
        """ic_mean == 0.04 must NOT pass (strict >)."""
        alpha = create_alpha_type(_VALID_RECIPE, registry_dir=registry_dir)
        transition_alpha_type(alpha, "published", registry_dir=registry_dir)
        boundary_eval = {
            "walk_forward_passed": True,
            "beats_equal_weight": True,
            "metrics": {"ic_mean": 0.04, "ic_ir": 0.6, "test_sharpe": 1.2},
        }
        ok = promote_alpha_type(alpha, boundary_eval, registry_dir=registry_dir)
        assert ok is False


# ---------------------------------------------------------------------------
# AC3 — run_daily_signal failure → HOLD (never liquidate)
# ---------------------------------------------------------------------------


def _make_signal(n=5):
    return pd.DataFrame(
        {
            "date": ["2026-01-05"] * n,
            "ticker": [f"{600000 + i}.SH" for i in range(n)],
            "score": [float(i + 1) for i in range(n)],
        }
    )


def _make_prev_portfolio(n=3):
    return pd.DataFrame(
        {
            "date": ["2026-01-02"] * n,
            "ticker": [f"{600000 + i}.SH" for i in range(n)],
            "target_weight": [1.0 / n] * n,
        }
    )


class TestRunDailySignal:
    """AC3: failure → retry 1 → HOLD prev portfolio + HIGH alert."""

    def test_success_returns_portfolio(self):
        alpha = {"alpha_type_id": "test", "params": {}}
        result = run_daily_signal(
            alpha, "2026-01-05", signal_fn=lambda a, d: _make_signal()
        )
        assert result["status"] == "SUCCESS"
        assert not result["portfolio"].empty

    def test_failure_holds_prev_portfolio(self, monkeypatch):
        """Both attempts fail → HOLD, prev portfolio passes through unchanged."""
        alerts = []
        monkeypatch.setattr(
            "research.alpha_type.send_alert",
            lambda level, msg, **kw: alerts.append((level, msg)),
        )
        alpha = {"alpha_type_id": "test_hold", "params": {}}
        prev = _make_prev_portfolio()
        call_count = 0

        def failing_signal_fn(a, d):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("data source down")

        result = run_daily_signal(
            alpha, "2026-01-05", prev, signal_fn=failing_signal_fn
        )
        assert result["status"] == "HOLD"
        assert result["portfolio"] is prev  # passes through untouched
        assert call_count == 2  # retry 1 = 2 attempts
        assert len(alerts) == 1
        assert alerts[0][0] == "HIGH"
        assert "test_hold" in alerts[0][1]

    def test_failure_no_prev_portfolio_holds_none(self, monkeypatch):
        """No prev portfolio → HOLD returns None (never fabricates empty book)."""
        monkeypatch.setattr("research.alpha_type.send_alert", lambda *a, **kw: None)
        alpha = {"alpha_type_id": "test_none", "params": {}}

        def failing_signal_fn(a, d):
            raise ValueError("bad signal")

        result = run_daily_signal(alpha, "2026-01-05", signal_fn=failing_signal_fn)
        assert result["status"] == "HOLD"
        assert result["portfolio"] is None

    def test_retry_on_transient_failure(self):
        """First attempt fails, second succeeds → SUCCESS."""
        alpha = {"alpha_type_id": "test_retry", "params": {}}
        call_count = 0

        def flaky_signal_fn(a, d):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("transient")
            return _make_signal()

        result = run_daily_signal(alpha, "2026-01-05", signal_fn=flaky_signal_fn)
        assert result["status"] == "SUCCESS"
        assert call_count == 2


# ---------------------------------------------------------------------------
# AC5 — build_portfolio analytical + alerts + fallback
# ---------------------------------------------------------------------------


class TestBuildPortfolio:
    """AC5: analytical solve, concentration/turnover alerts, equal-weight fallback."""

    def test_analytical_top_k(self):
        alpha = {
            "alpha_type_id": "bp_1",
            "params": {"weight_method": "top_k", "total_position": 1.0},
            "rules": [{"rule_id": "top_k_filter", "params": {"k": 3}}],
        }
        signal = _make_signal(n=5)
        weights = build_portfolio(signal, alpha)
        assert not weights.empty
        # top-3 each at 1/3
        assert abs(weights["target_weight"].sum() - 1.0) < 1e-9

    def test_total_position_scaling(self):
        alpha = {
            "alpha_type_id": "bp_2",
            "params": {"weight_method": "top_k", "total_position": 0.5},
            "rules": [{"rule_id": "top_k_filter", "params": {"k": 2}}],
        }
        weights = build_portfolio(_make_signal(n=5), alpha)
        assert abs(weights["target_weight"].sum() - 0.5) < 1e-9

    def test_concentration_alert_no_correction(self, monkeypatch):
        alerts = []
        monkeypatch.setattr(
            "research.alpha_type.send_alert",
            lambda level, msg, **kw: alerts.append((level, msg)),
        )
        alpha = {
            "alpha_type_id": "bp_conc",
            "params": {
                "weight_method": "top_k",
                "total_position": 1.0,
                "max_single_weight": 0.10,
            },
            "rules": [{"rule_id": "top_k_filter", "params": {"k": 2}}],
        }
        weights = build_portfolio(_make_signal(n=5), alpha)
        # Alert fired but weights not trimmed.
        assert any("concentration" in m for _, m in alerts)
        assert abs(weights["target_weight"].sum() - 1.0) < 1e-9  # not trimmed

    def test_turnover_alert_no_trim(self, monkeypatch):
        alerts = []
        monkeypatch.setattr(
            "research.alpha_type.send_alert",
            lambda level, msg, **kw: alerts.append((level, msg)),
        )
        alpha = {
            "alpha_type_id": "bp_to",
            "params": {
                "weight_method": "top_k",
                "total_position": 1.0,
                "max_turnover": 0.1,
            },
            "rules": [{"rule_id": "top_k_filter", "params": {"k": 3}}],
        }
        prev = _make_prev_portfolio(n=3)
        build_portfolio(_make_signal(n=5), alpha, prev)
        assert any("turnover" in m for _, m in alerts)

    def test_equal_weight_fallback_on_solve_failure(self, monkeypatch):
        """Solver exception → 1/N fallback, never empty."""
        monkeypatch.setattr("research.alpha_type.send_alert", lambda *a, **kw: None)
        alpha = {
            "alpha_type_id": "bp_fb",
            "params": {"weight_method": "top_k", "total_position": 1.0},
            "rules": [],
        }
        signal = _make_signal(n=4)
        # Force solver failure by monkeypatching signal_to_weights.
        monkeypatch.setattr(
            "research.alpha_type.signal_to_weights",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("solver fail")),
        )
        weights = build_portfolio(signal, alpha)
        assert not weights.empty
        assert abs(weights["target_weight"].sum() - 1.0) < 1e-9  # 1/N * 4 = 1.0

    def test_empty_result_triggers_fallback(self, monkeypatch):
        """Review fix 1: solver succeeds but yields no rows → fallback."""
        monkeypatch.setattr("research.alpha_type.send_alert", lambda *a, **kw: None)
        alpha = {
            "alpha_type_id": "bp_empty_res",
            "params": {"weight_method": "top_k", "total_position": 1.0},
            "rules": [],
        }
        signal = _make_signal(n=3)
        # Solver returns empty (e.g. all scores NaN under proportional).
        monkeypatch.setattr(
            "research.alpha_type.signal_to_weights",
            lambda *a, **kw: pd.DataFrame(columns=["date", "ticker", "target_weight"]),
        )
        weights = build_portfolio(signal, alpha)
        assert not weights.empty
        assert abs(weights["target_weight"].sum() - 1.0) < 1e-9

    def test_empty_signal_raises(self):
        alpha = {"alpha_type_id": "bp_empty", "params": {}}
        with pytest.raises(ValueError, match="empty signal"):
            build_portfolio(pd.DataFrame(), alpha)


# ---------------------------------------------------------------------------
# D9 — compute_turnover
# ---------------------------------------------------------------------------


class TestComputeTurnover:
    """D9: Σ|Δw|/2 over outer join, missing = 0."""

    def test_identical_weights_zero_turnover(self):
        w = pd.DataFrame(
            {
                "date": ["2026-01-05"] * 3,
                "ticker": ["A", "B", "C"],
                "target_weight": [0.4, 0.3, 0.3],
            }
        )
        assert compute_turnover(w, w) == 0.0

    def test_full_replacement_turnover_one(self):
        old = pd.DataFrame(
            {
                "date": ["2026-01-05"] * 2,
                "ticker": ["A", "B"],
                "target_weight": [0.5, 0.5],
            }
        )
        new = pd.DataFrame(
            {
                "date": ["2026-01-05"] * 2,
                "ticker": ["C", "D"],
                "target_weight": [0.5, 0.5],
            }
        )
        # |Δw| for A=0.5, B=0.5, C=0.5, D=0.5 → Σ=2.0 → /2 = 1.0
        assert abs(compute_turnover(new, old) - 1.0) < 1e-9

    def test_partial_change(self):
        old = pd.DataFrame(
            {
                "date": ["2026-01-05"] * 2,
                "ticker": ["A", "B"],
                "target_weight": [0.6, 0.4],
            }
        )
        new = pd.DataFrame(
            {
                "date": ["2026-01-05"] * 2,
                "ticker": ["A", "B"],
                "target_weight": [0.4, 0.6],
            }
        )
        # |Δw| = 0.2 + 0.2 = 0.4 → /2 = 0.2
        assert abs(compute_turnover(new, old) - 0.2) < 1e-9


# ---------------------------------------------------------------------------
# D11 — Rule / RulePipeline
# ---------------------------------------------------------------------------


class TestRulePipeline:
    """D11: Rule ABC, param validation, serial pipeline."""

    def test_top_k_filter(self):
        rule = TopKFilter(k=2)
        data = _make_signal(n=5)
        result = rule.apply(data)
        assert len(result) == 2

    def test_param_type_checked(self):
        with pytest.raises(ValueError, match="expected int"):
            TopKFilter(k="not_an_int")

    def test_create_rule_factory(self):
        rule = create_rule("top_k_filter", k=3)
        assert rule.rule_id == "top_k_filter"
        assert rule.category == "filter"

    def test_create_rule_unknown(self):
        with pytest.raises(ValueError, match="unknown rule_id"):
            create_rule("nonexistent")

    def test_pipeline_serial(self):
        pipeline = RulePipeline([TopKFilter(k=3), TopKFilter(k=2)])
        data = _make_signal(n=5)
        result = pipeline.run(data)
        assert len(result) == 2  # first filter to 3, then to 2
