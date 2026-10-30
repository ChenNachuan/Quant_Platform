"""Structural and constructive tests for DQC rules — 44 rules across 5 asset YAMLs.

Part 1: structural validation (counts, continuity, applicability, YAML shape).
Part 2: declarative evaluation harness + clean-sample test + 27 HARD trigger cases.

Source of truth: docs/design/data/dqc_rules.md (four dimension tables).
"""

import importlib
import re
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

RULES_DIR = Path(__file__).parent.parent.parent / "data" / "dqc" / "rules"

YAML_FILES = [
    "bars_daily.yaml",
    "bars_minute.yaml",
    "fundamental.yaml",
    "factor.yaml",
    "reference.yaml",
]

# Expected ID ranges per dimension
EXPECTED_IDS = {
    "C": [f"C{i:02d}" for i in range(1, 11)],  # C01-C10
    "L": [f"L{i:02d}" for i in range(1, 15)],  # L01-L14
    "K": [f"K{i:02d}" for i in range(1, 12)],  # K01-K11
    "T": [f"T{i:02d}" for i in range(1, 10)],  # T01-T09
}
ALL_EXPECTED_IDS = sorted([rid for ids in EXPECTED_IDS.values() for rid in ids])

# Expected HARD/SOFT counts per dimension prefix
EXPECTED_HARD = {"C": 6, "L": 9, "K": 7, "T": 5}  # total 27
EXPECTED_SOFT = {"C": 4, "L": 5, "K": 4, "T": 4}  # total 17

# Applicability marks from the design tables: rule_id -> set of marks
EXPECTED_APPLICABILITY = {
    "C01": {"daily", "fund"},
    "C02": {"daily", "fund"},
    "C03": {"minute"},
    "C04": {"daily", "minute"},
    "C05": {"daily", "fund"},
    "C06": {"daily"},
    "C07": {"reference"},
    "C08": {"factor"},
    "C09": {"factor"},
    "C10": {"reference"},
    "L01": {"daily", "minute"},
    "L02": {"daily", "minute"},
    "L03": {"daily", "minute"},
    "L04": {"daily"},
    "L05": {"daily"},  # futures filter within daily
    "L06": {"daily"},
    "L07": {"daily"},
    "L08": {"daily"},
    "L09": {"fund"},
    "L10": {"fund"},
    "L11": {"factor"},
    "L12": {"factor"},
    "L13": {"daily", "minute", "fund", "factor", "reference"},
    "L14": {"daily", "minute", "fund", "factor", "reference"},
    "K01": {"daily"},
    "K02": {"daily", "minute"},
    "K03": {"daily"},
    "K04": {"daily"},
    "K05": {"daily"},
    "K06": {"daily", "minute"},
    "K07": {"daily", "minute"},
    "K08": {"daily", "minute"},
    "K09": {"factor"},
    "K10": {"reference"},
    "K11": {"daily", "fund"},
    "T01": {"daily", "fund"},
    "T02": {"minute"},
    "T03": {"daily", "minute", "fund", "factor", "reference"},
    "T04": {"daily", "minute", "fund"},
    "T05": {"minute"},
    "T06": {"daily"},
    "T07": {"factor"},
    "T08": {"reference"},
    "T09": {"daily", "fund"},
}

REQUIRED_FIELDS = {
    "id",
    "name",
    "severity",
    "type",
    "applicability",
    "threshold",
    "threshold_source",
    "boundary",
    "description",
}
VALID_SEVERITIES = {"HARD", "SOFT"}
VALID_TYPES = {"column_check", "custom_check"}
VALID_CONDITIONS = {
    "gt",
    "ge",
    "lt",
    "le",
    "eq",
    "ne",
    "in",
    "range",
    "regex",
    "notnull",
    "isbool",
    "isbool_notnull",
    "notinf",
    "date_format",
}

# YAML file -> asset mark it represents
YAML_ASSET_MARK = {
    "bars_daily.yaml": "daily",
    "bars_minute.yaml": "minute",
    "fundamental.yaml": "fund",
    "factor.yaml": "factor",
    "reference.yaml": "reference",
}


def _load_all_rules():
    """Load all rules from the 5 YAML files, returning (file, rule) tuples."""
    entries = []
    for fname in YAML_FILES:
        path = RULES_DIR / fname
        with open(path) as f:
            data = yaml.safe_load(f)
        for rule in data["rules"]:
            entries.append((fname, rule))
    return entries


def _unique_rules(entries):
    """Return dict of unique rule_id -> rule (first occurrence wins)."""
    seen = {}
    for _fname, rule in entries:
        rid = rule["id"]
        if rid not in seen:
            seen[rid] = rule
    return seen


# ── Evaluation harness ────────────────────────────────────────────────

_RULES_CACHE: dict | None = None


def _get_rules() -> dict:
    global _RULES_CACHE
    if _RULES_CACHE is None:
        _RULES_CACHE = _unique_rules(_load_all_rules())
    return _RULES_CACHE


def _resolve_function(func_path: str):
    """Resolve 'checks.custom_checks.foo' -> callable from data.dqc.checks."""
    full = "data.dqc." + func_path
    mod_name, fn_name = full.rsplit(".", 1)
    mod = importlib.import_module(mod_name)
    return getattr(mod, fn_name)


def _apply_condition(series: pd.Series, condition: str, value) -> pd.Series:
    """Return a boolean failure mask for a single column."""
    if condition == "gt":
        return series <= value
    if condition == "ge":
        return series < value
    if condition == "lt":
        return series >= value
    if condition == "le":
        return series > value
    if condition == "eq":
        return series != value
    if condition == "ne":
        return series == value
    if condition == "range":
        return (series <= value[0]) | (series >= value[1])
    if condition == "regex":
        return ~series.astype(str).str.match(value, na=False)
    if condition == "notnull":
        return series.isna()
    if condition == "isbool_notnull":
        is_bool = series.map(
            lambda x: isinstance(x, (bool, np.bool_)) if pd.notna(x) else False
        )
        return series.isna() | ~is_bool
    if condition == "notinf":
        if series.dtype.kind in "fc":
            return series.abs() == float("inf")
        return pd.Series(False, index=series.index)
    if condition == "date_format":
        return pd.to_datetime(series, errors="coerce", format="ISO8601").isna()
    raise ValueError(f"Unknown condition: {condition}")


def _apply_filter(df: pd.DataFrame, filter_expr: str) -> pd.Series:
    """Evaluate a boolean filter expression; True = row is in scope."""
    return df.eval(filter_expr).astype(bool)


def evaluate_column_check(df: pd.DataFrame, rule: dict) -> pd.Series:
    """Evaluate a declarative column_check rule. Returns failure mask."""
    failures = pd.Series(False, index=df.index)
    value = rule["value"]
    for col in rule["columns"]:
        if col not in df.columns:
            failures = failures | pd.Series(True, index=df.index)
            continue
        col_value = value[col] if isinstance(value, dict) else value
        failures = failures | _apply_condition(df[col], rule["condition"], col_value)
    if "filter" in rule and rule["filter"]:
        in_scope = _apply_filter(df, rule["filter"])
        failures = failures & in_scope
    return failures


def evaluate_custom_check(df: pd.DataFrame, rule: dict, **overrides) -> pd.Series:
    """Call the rule's custom check function. Returns failure mask."""
    fn = _resolve_function(rule["function"])
    params = dict(rule.get("params") or {})
    params.update(overrides)
    return fn(df, **params)


def evaluate_rule(rule: dict, df: pd.DataFrame, **overrides) -> pd.Series:
    """Evaluate any rule (column or custom) against a DataFrame."""
    if rule["type"] == "column_check":
        return evaluate_column_check(df, rule)
    return evaluate_custom_check(df, rule, **overrides)


def evaluate_cross_rule(
    rule: dict, daily_df: pd.DataFrame, minute_df: pd.DataFrame, **overrides
) -> pd.Series:
    """Evaluate a cross-table rule (K06-K08)."""
    fn = _resolve_function(rule["function"])
    params = dict(rule.get("params") or {})
    params.update(overrides)
    return fn(daily_df, minute_df, **params)


# ── Clean sample fixtures ─────────────────────────────────────────────

TODAY = pd.Timestamp("2024-01-04")
CLEAN_DATES = ["2024-01-02", "2024-01-03"]
CLEAN_TICKERS = ["000001.SZ", "000002.SZ"]


def _make_clean_daily() -> pd.DataFrame:
    rows = []
    prices = {
        "000001.SZ": [(10.0, 10.5, 9.8, 10.2), (10.2, 10.6, 10.0, 10.3)],
        "000002.SZ": [(20.0, 20.5, 19.8, 20.2), (20.2, 20.6, 20.0, 20.3)],
    }
    for ticker in CLEAN_TICKERS:
        for i, dt in enumerate(CLEAN_DATES):
            o, h, lo, c = prices[ticker][i]
            rows.append(
                {
                    "ticker": ticker,
                    "trade_date": dt,
                    "open": o,
                    "high": h,
                    "low": lo,
                    "close": c,
                    "volume": 100000 + i * 10000,
                    "amount": (100000 + i * 10000) * c,
                    "adjfactor": 1.0,
                    "suspend": False,
                    "limit_up": False,
                    "limit_down": False,
                    "prev_close": o,
                }
            )
    return pd.DataFrame(rows)


def _make_clean_minute() -> pd.DataFrame:
    """240 bars per ticker per date, consistent with daily OHLCV."""
    daily = _make_clean_daily()
    rows = []
    for _, drow in daily.iterrows():
        o, c = drow["open"], drow["close"]
        vol_per_bar = drow["volume"] / 240
        for j in range(240):
            price = o if j < 120 else c
            rows.append(
                {
                    "ticker": drow["ticker"],
                    "trade_date": drow["trade_date"],
                    "timestamp": f"{drow['trade_date']} {9 + j // 60:02d}:{j % 60:02d}:00",
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": vol_per_bar,
                    "amount": vol_per_bar * price,
                }
            )
    return pd.DataFrame(rows)


def _make_clean_fundamental() -> pd.DataFrame:
    rows = []
    for ticker in CLEAN_TICKERS:
        for dt in CLEAN_DATES:
            rows.append(
                {
                    "ticker": ticker,
                    "trade_date": dt,
                    "pe": 15.0,
                    "pb": 1.5,
                    "market_cap": 1e9,
                }
            )
    return pd.DataFrame(rows)


def _make_clean_factor() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-02", periods=250).strftime("%Y-%m-%d").tolist()
    rows = []
    for ti, ticker in enumerate(CLEAN_TICKERS):
        for dt in dates:
            rows.append(
                {"ticker": ticker, "trade_date": dt, "factor_value": 0.5 + ti * 0.1}
            )
    return pd.DataFrame(rows)


def _make_clean_reference() -> pd.DataFrame:
    """Industry/classification table for reference rules."""
    rows = [{"ticker": t, "industry": "BANK"} for t in CLEAN_TICKERS]
    return pd.DataFrame(rows)


def _make_clean_calendar() -> pd.DataFrame:
    """Trading calendar covering past + 30 future days from TODAY."""
    dates = pd.bdate_range("2023-01-02", "2025-02-28").strftime("%Y-%m-%d").tolist()
    return pd.DataFrame({"trade_date": dates})


class TestYamlFilesExist:
    def test_all_five_files_present(self):
        for fname in YAML_FILES:
            assert (RULES_DIR / fname).exists(), f"{fname} missing"

    def test_each_file_parses(self):
        for fname in YAML_FILES:
            path = RULES_DIR / fname
            with open(path) as f:
                data = yaml.safe_load(f)
            assert "rules" in data, f"{fname} missing 'rules' key"
            assert isinstance(data["rules"], list)
            assert len(data["rules"]) > 0


class TestRuleCountAndContinuity:
    def test_unique_ids_exactly_44(self):
        entries = _load_all_rules()
        unique = _unique_rules(entries)
        assert len(unique) == 44, f"expected 44 unique ids, got {len(unique)}"

    def test_id_continuity_each_dimension(self):
        entries = _load_all_rules()
        unique = _unique_rules(entries)
        actual_ids = set(unique.keys())
        for prefix, expected_list in EXPECTED_IDS.items():
            for rid in expected_list:
                assert rid in actual_ids, f"{rid} missing from rules"
        assert actual_ids == set(ALL_EXPECTED_IDS), (
            f"unexpected ids: {actual_ids - set(ALL_EXPECTED_IDS)}, "
            f"missing: {set(ALL_EXPECTED_IDS) - actual_ids}"
        )

    def test_dimension_id_counts(self):
        entries = _load_all_rules()
        unique = _unique_rules(entries)
        for prefix, expected_list in EXPECTED_IDS.items():
            actual = [r for r in unique if r.startswith(prefix)]
            assert len(actual) == len(expected_list), (
                f"{prefix}: expected {len(expected_list)}, got {len(actual)}"
            )


class TestHardSoftBreakdown:
    def test_hard_total_27(self):
        entries = _load_all_rules()
        unique = _unique_rules(entries)
        hard = [r for r in unique.values() if r["severity"] == "HARD"]
        assert len(hard) == 27, f"expected 27 HARD, got {len(hard)}"

    def test_soft_total_17(self):
        entries = _load_all_rules()
        unique = _unique_rules(entries)
        soft = [r for r in unique.values() if r["severity"] == "SOFT"]
        assert len(soft) == 17, f"expected 17 SOFT, got {len(soft)}"

    def test_hard_per_dimension(self):
        entries = _load_all_rules()
        unique = _unique_rules(entries)
        for prefix, expected_count in EXPECTED_HARD.items():
            actual = sum(
                1
                for r in unique.values()
                if r["severity"] == "HARD" and r["id"].startswith(prefix)
            )
            assert actual == expected_count, (
                f"{prefix}: expected {expected_count} HARD, got {actual}"
            )

    def test_soft_per_dimension(self):
        entries = _load_all_rules()
        unique = _unique_rules(entries)
        for prefix, expected_count in EXPECTED_SOFT.items():
            actual = sum(
                1
                for r in unique.values()
                if r["severity"] == "SOFT" and r["id"].startswith(prefix)
            )
            assert actual == expected_count, (
                f"{prefix}: expected {expected_count} SOFT, got {actual}"
            )


class TestApplicability:
    def test_each_rule_applicability_matches_design(self):
        entries = _load_all_rules()
        unique = _unique_rules(entries)
        for rid, expected_marks in EXPECTED_APPLICABILITY.items():
            assert rid in unique, f"{rid} not found"
            actual = set(unique[rid]["applicability"])
            assert actual == expected_marks, (
                f"{rid}: expected {expected_marks}, got {actual}"
            )

    def test_rules_appear_in_correct_yaml_files(self):
        entries = _load_all_rules()
        for fname, rule in entries:
            asset = YAML_ASSET_MARK[fname]
            marks = set(rule["applicability"])
            assert asset in marks, (
                f"{rule['id']} in {fname} but applicability={marks} "
                f"does not include '{asset}'"
            )


class TestYamlShape:
    def test_required_fields_present(self):
        entries = _load_all_rules()
        for fname, rule in entries:
            for field in REQUIRED_FIELDS:
                assert field in rule, (
                    f"{rule.get('id', '?')} in {fname}: missing '{field}'"
                )

    def test_severity_valid(self):
        entries = _load_all_rules()
        for _fname, rule in entries:
            assert rule["severity"] in VALID_SEVERITIES, (
                f"{rule['id']}: invalid severity '{rule['severity']}'"
            )

    def test_type_valid(self):
        entries = _load_all_rules()
        for _fname, rule in entries:
            assert rule["type"] in VALID_TYPES, (
                f"{rule['id']}: invalid type '{rule['type']}'"
            )

    def test_column_check_has_required_subfields(self):
        entries = _load_all_rules()
        for fname, rule in entries:
            if rule["type"] == "column_check":
                for field in ("columns", "condition", "value"):
                    assert field in rule, (
                        f"{rule['id']} in {fname}: column_check missing '{field}'"
                    )
                assert rule["condition"] in VALID_CONDITIONS, (
                    f"{rule['id']}: unknown condition '{rule['condition']}'"
                )

    def test_custom_check_has_function(self):
        entries = _load_all_rules()
        for fname, rule in entries:
            if rule["type"] == "custom_check":
                assert "function" in rule, (
                    f"{rule['id']} in {fname}: custom_check missing 'function'"
                )

    def test_threshold_source_valid(self):
        valid_sources = {"fixed", "statistical", "adaptive"}
        entries = _load_all_rules()
        for _fname, rule in entries:
            assert rule["threshold_source"] in valid_sources, (
                f"{rule['id']}: invalid threshold_source '{rule['threshold_source']}'"
            )


class TestHardBoundaryDeclarations:
    def test_every_hard_rule_has_boundary(self):
        entries = _load_all_rules()
        unique = _unique_rules(entries)
        for rid, rule in unique.items():
            if rule["severity"] == "HARD":
                assert "boundary" in rule, f"{rid}: HARD rule missing 'boundary'"
                assert len(rule["boundary"]) > 0, f"{rid}: empty boundary"

    def test_boundary_declares_endpoint_semantics(self):
        """Boundary must contain endpoint language (inclusive/exclusive/gt/ge/le/eq)."""
        endpoint_pattern = re.compile(
            r"(gt|ge|lt|le|eq|inclusive|exclusive|strictly|boundary|FAILS|PASSES|passes|fails)",
            re.IGNORECASE,
        )
        entries = _load_all_rules()
        unique = _unique_rules(entries)
        for rid, rule in unique.items():
            if rule["severity"] == "HARD":
                assert endpoint_pattern.search(rule["boundary"]), (
                    f"{rid}: boundary lacks endpoint semantics: {rule['boundary']}"
                )


class TestUniquenessAcrossFiles:
    def test_multi_asset_rules_consistent_across_files(self):
        """A rule appearing in multiple YAMLs must be an identical copy everywhere.

        Full-body comparison, not just severity/type: _unique_rules is
        first-wins, so a diverging copy silently shadows the corrected one
        (C05 params once diverged exactly this way).
        """
        entries = _load_all_rules()
        by_id = {}
        for fname, rule in entries:
            rid = rule["id"]
            if rid not in by_id:
                by_id[rid] = []
            by_id[rid].append((fname, rule))
        for rid, occurrences in by_id.items():
            if len(occurrences) > 1:
                base_fname, base = occurrences[0]
                for fname, rule in occurrences[1:]:
                    for field in sorted(set(base) | set(rule)):
                        assert base.get(field) == rule.get(field), (
                            f"{rid}: {field} differs between {base_fname} and "
                            f"{fname}: {base.get(field)!r} vs {rule.get(field)!r}"
                        )


# ── Clean sample: all 44 rules pass ───────────────────────────────────


class TestCleanSample:
    """A clean small sample must produce 0 failures across all 44 rules."""

    def _run_all(self, overrides_map=None):
        rules = _get_rules()
        daily = _make_clean_daily()
        minute = _make_clean_minute()
        fund = _make_clean_fundamental()
        factor = _make_clean_factor()
        reference = _make_clean_reference()
        calendar = _make_clean_calendar()

        frame_map = {
            "daily": daily,
            "minute": minute,
            "fund": fund,
            "factor": factor,
            "reference": reference,
        }
        overrides_map = overrides_map or {}
        total_failures = 0
        failures_detail = {}

        for rid, rule in sorted(rules.items()):
            ov = dict(overrides_map.get(rid, {}))
            # Inject external data based on function name
            fn_path = rule.get("function", "")
            if "row_count_vs_calendar" in fn_path:
                ov["calendar_dates"] = set(daily["trade_date"].unique())
            elif "row_count_vs_universe" in fn_path:
                ov["universe"] = set(CLEAN_TICKERS)
            elif "factor_ticker_coverage" in fn_path:
                ov["universe"] = set(CLEAN_TICKERS)
            elif "industry_coverage" in fn_path:
                ov["universe"] = set(CLEAN_TICKERS)
            elif "industry_universe_match" in fn_path:
                ov["universe"] = set(CLEAN_TICKERS)
            elif "fund_vs_daily_tickers" in fn_path:
                ov["daily_tickers"] = set(CLEAN_TICKERS)
            elif "adjfactor_stable" in fn_path:
                ov["ca_dates"] = set()
            elif "open_gap" in fn_path:
                ov["ca_dates"] = set()
            elif "calendar_completeness" in fn_path:
                ov["today"] = TODAY
            elif "no_future_dates" in fn_path or "freshness" in fn_path:
                ov["today"] = TODAY
            elif "calendar_forward" in fn_path:
                ov["today"] = TODAY

            # Pick primary frame — some rules need a different one
            primary = rule["applicability"][0]
            if rid in ("C07", "T08"):
                df = calendar
            else:
                df = frame_map.get(primary, daily)

            if "cross_checks" in fn_path:
                result = evaluate_cross_rule(rule, daily, minute, **ov)
            else:
                result = evaluate_rule(rule, df, **ov)

            n_fail = (
                int(result.sum())
                if isinstance(result, pd.Series)
                else int(bool(result))
            )
            if n_fail > 0:
                failures_detail[rid] = n_fail
            total_failures += n_fail

        return total_failures, failures_detail

    def test_all_44_rules_pass_clean_sample(self):
        total, detail = self._run_all()
        assert total == 0, f"clean sample should have 0 failures, got {total}: {detail}"

    def test_clean_k06_k08_pair(self):
        rules = _get_rules()
        daily = _make_clean_daily()
        minute = _make_clean_minute()
        for rid in ("K06", "K07", "K08"):
            result = evaluate_cross_rule(rules[rid], daily, minute)
            assert not result.any(), f"{rid} should pass on clean pair"


# ── 27 HARD trigger cases ─────────────────────────────────────────────


class TestHardTriggerCases:
    """Each HARD rule gets a minimal dirty sample that trips exactly it."""

    def _rule(self, rid):
        return _get_rules()[rid]

    def _daily(self):
        return _make_clean_daily()

    def _minute(self):
        return _make_clean_minute()

    def _assert_caught(self, rid, failures):
        assert failures.any(), f"{rid} should catch the dirty sample"

    # ── Column-check HARD triggers ──

    def test_C04_null_critical_fields(self):
        df = self._daily()
        df.loc[0, "close"] = np.nan
        self._assert_caught("C04", evaluate_column_check(df, self._rule("C04")))

    def test_L01_price_positive(self):
        df = self._daily()
        df.loc[0, "close"] = 0
        self._assert_caught("L01", evaluate_column_check(df, self._rule("L01")))

    def test_L02_volume_non_negative(self):
        df = self._daily()
        df.loc[0, "volume"] = -1
        self._assert_caught("L02", evaluate_column_check(df, self._rule("L02")))

    def test_L06_adjfactor_positive(self):
        df = self._daily()
        df.loc[0, "adjfactor"] = 0
        self._assert_caught("L06", evaluate_column_check(df, self._rule("L06")))

    def test_L08_boolean_flags(self):
        df = self._daily()
        df["suspend"] = df["suspend"].astype(object)
        df.loc[0, "suspend"] = None
        self._assert_caught("L08", evaluate_column_check(df, self._rule("L08")))

    def test_L10_market_cap_positive(self):
        df = pd.DataFrame({"market_cap": [1e9, 0]})
        self._assert_caught("L10", evaluate_column_check(df, self._rule("L10")))

    def test_L11_factor_finite(self):
        df = pd.DataFrame({"factor_value": [0.5, float("inf")]})
        self._assert_caught("L11", evaluate_column_check(df, self._rule("L11")))

    def test_L13_ticker_format(self):
        df = self._daily()
        df.loc[0, "ticker"] = "999999.XX"
        self._assert_caught("L13", evaluate_column_check(df, self._rule("L13")))

    def test_L14_date_format(self):
        df = self._daily()
        df.loc[0, "trade_date"] = "not-a-date"
        self._assert_caught("L14", evaluate_column_check(df, self._rule("L14")))

    # ── Custom-check HARD triggers ──

    def test_C01_row_count_vs_calendar(self):
        df = self._daily()
        cal = {"2024-01-02", "2024-01-03", "2024-01-04"}
        result = evaluate_custom_check(df, self._rule("C01"), calendar_dates=cal)
        self._assert_caught("C01", result)

    def test_C03_minute_bar_count(self):
        df = self._minute()
        # Drop one bar so count = 239 != 240
        df = df.iloc[:-1]
        result = evaluate_custom_check(df, self._rule("C03"), expected_bars=240)
        self._assert_caught("C03", result)

    def test_C06_adjfactor_present(self):
        df = self._daily()
        df.loc[0, "adjfactor"] = np.nan
        result = evaluate_custom_check(df, self._rule("C06"), max_missing_rate=0.0)
        self._assert_caught("C06", result)

    def test_C07_calendar_completeness(self):
        cal = pd.DataFrame({"trade_date": ["2024-01-02", "2024-01-15"]})  # 13-day gap
        result = evaluate_custom_check(
            cal,
            self._rule("C07"),
            forward_days=30,
            today=pd.Timestamp("2024-01-02"),
        )
        self._assert_caught("C07", result)

    def test_C10_industry_coverage(self):
        ref = pd.DataFrame({"ticker": ["000001.SZ"]})  # missing 000002.SZ
        result = evaluate_custom_check(
            ref, self._rule("C10"), universe=set(CLEAN_TICKERS)
        )
        self._assert_caught("C10", result)

    def test_L03_ohlc_relation(self):
        df = self._daily()
        df.loc[0, "low"] = 11.0  # low > close
        result = evaluate_custom_check(df, self._rule("L03"), tolerance=0.0)
        self._assert_caught("L03", result)

    def test_K02_volume_amount_match(self):
        df = self._daily()
        df.loc[0, "volume"] = 0  # but amount stays > 0
        result = evaluate_custom_check(df, self._rule("K02"))
        self._assert_caught("K02", result)

    def test_K03_suspend_consistency(self):
        df = self._daily()
        df.loc[0, "suspend"] = True  # but volume > 0 and OHLC not flat
        result = evaluate_custom_check(df, self._rule("K03"))
        self._assert_caught("K03", result)

    def test_K05_adjfactor_stable(self):
        df = self._daily()
        df.loc[1, "adjfactor"] = 2.0  # changed on non-CA day
        result = evaluate_custom_check(df, self._rule("K05"), ca_dates=set())
        self._assert_caught("K05", result)

    def test_K10_industry_universe_match(self):
        ref = pd.DataFrame({"ticker": ["000001.SZ"]})  # missing 000002.SZ
        result = evaluate_custom_check(
            ref, self._rule("K10"), universe=set(CLEAN_TICKERS)
        )
        self._assert_caught("K10", result)

    def test_T01_daily_freshness(self):
        df = self._daily()
        result = evaluate_custom_check(
            df,
            self._rule("T01"),
            max_stale_days=1,
            today=pd.Timestamp("2024-03-01"),  # 2 months stale
        )
        self._assert_caught("T01", result)

    def test_T02_minute_freshness(self):
        df = self._minute()
        result = evaluate_custom_check(
            df,
            self._rule("T02"),
            max_stale_days=1,
            today=pd.Timestamp("2024-03-01"),
        )
        self._assert_caught("T02", result)

    def test_T03_no_future_dates(self):
        df = self._daily()
        df.loc[0, "trade_date"] = "2025-12-31"
        result = evaluate_custom_check(df, self._rule("T03"), today=TODAY)
        self._assert_caught("T03", result)

    def test_T04_date_ascending(self):
        df = self._daily()
        # Swap two dates to break ascending order
        df.loc[0, "trade_date"], df.loc[1, "trade_date"] = (
            df.loc[1, "trade_date"],
            df.loc[0, "trade_date"],
        )
        result = evaluate_custom_check(df, self._rule("T04"))
        self._assert_caught("T04", result)

    def test_T05_minute_timestamp_order(self):
        df = self._minute().head(4).copy()
        # Swap timestamps to break ordering
        df.loc[0, "timestamp"], df.loc[1, "timestamp"] = (
            df.loc[1, "timestamp"],
            df.loc[0, "timestamp"],
        )
        result = evaluate_custom_check(df, self._rule("T05"))
        self._assert_caught("T05", result)

    # ── Cross-check HARD triggers (K06-K08) ──

    def test_K06_daily_vs_minute_close(self):
        daily = self._daily()
        minute = self._minute()
        daily.loc[0, "close"] = 99.0  # diverges from last minute close
        result = evaluate_cross_rule(self._rule("K06"), daily, minute, tolerance=0.01)
        self._assert_caught("K06", result)

    def test_K07_daily_vs_minute_volume(self):
        daily = self._daily()
        minute = self._minute()
        daily.loc[0, "volume"] = 999999  # diverges from sum of minute volumes
        result = evaluate_cross_rule(
            self._rule("K07"), daily, minute, max_rel_diff=0.001
        )
        self._assert_caught("K07", result)

    def test_K08_daily_vs_minute_hl(self):
        daily = self._daily()
        minute = self._minute()
        daily.loc[0, "high"] = 0.01  # below minute max high
        result = evaluate_cross_rule(self._rule("K08"), daily, minute)
        self._assert_caught("K08", result)


# ── Review-fix regression cases ───────────────────────────────────────


class TestReviewFixRegression:
    """Guard against the 6 review findings recurring."""

    def test_L04_effective_threshold_is_105_pct(self):
        """Board limit 10% + tolerance 0.5% = 10.5%; 10.8% must fail."""
        df = _make_clean_daily()
        df.loc[0, "prev_close"] = 10.0
        df.loc[0, "close"] = 10.8  # 8% change > 10.5%? no; push higher
        df.loc[0, "close"] = 10.0 * 1.108  # 10.8% > 10.5%
        rule = _get_rules()["L04"]
        result = evaluate_custom_check(df, rule)
        assert result.iloc[0], "10.8% change must exceed effective 10.5% limit"

    def test_L04_boundary_at_105_pct_passes(self):
        """Change just below 10.5% must pass (le semantics)."""
        df = _make_clean_daily()
        df.loc[0, "prev_close"] = 100.0
        df.loc[0, "close"] = 110.49  # 10.49% < 10.5%
        rule = _get_rules()["L04"]
        result = evaluate_custom_check(df, rule)
        assert not result.iloc[0], "change at 10.49% must pass"

    def test_L09_pb_1500_fails(self):
        """PB=1500 exceeds PB range (-1000, 1000); must fail."""
        df = _make_clean_fundamental()
        df.loc[0, "pb"] = 1500.0
        rule = _get_rules()["L09"]
        result = evaluate_column_check(df, rule)
        assert result.iloc[0], "PB=1500 must exceed PB range limit"

    def test_L09_pe_1500_passes(self):
        """PE=1500 is within PE range (-10000, 10000); must pass."""
        df = _make_clean_fundamental()
        df.loc[0, "pe"] = 1500.0
        rule = _get_rules()["L09"]
        result = evaluate_column_check(df, rule)
        assert not result.iloc[0], "PE=1500 is within PE range"

    def test_K01_ca_day_gap_not_flagged(self):
        """Ex-right day gap must be skipped when ca_dates is provided."""
        df = _make_clean_daily()
        df.loc[0, "open"] = 20.0  # 100% gap from prev_close=10.0
        df.loc[0, "trade_date"] = "2024-01-02"
        rule = _get_rules()["K01"]
        result = evaluate_custom_check(df, rule, ca_dates={"2024-01-02"})
        assert not result.iloc[0], "CA-day gap must not be flagged"

    def test_K01_non_ca_day_gap_flagged(self):
        """Non-CA-day gap above threshold must be flagged."""
        df = _make_clean_daily()
        df.loc[0, "open"] = 20.0  # 100% gap from prev_close=10.0
        rule = _get_rules()["K01"]
        result = evaluate_custom_check(df, rule, ca_dates=set())
        assert result.iloc[0], "non-CA-day gap must be flagged"

    def test_C03_returns_aggregate_length_1(self):
        """C03 must return a length-1 __aggregate__ Series, not a multi-row mask."""
        df = _make_clean_minute()
        rule = _get_rules()["C03"]
        result = evaluate_custom_check(df, rule, expected_bars=240)
        assert len(result) == 1
        assert result.index[0] == "__aggregate__"

    def test_T03_returns_aggregate_length_1(self):
        """T03 must return a length-1 __aggregate__ Series."""
        df = _make_clean_daily()
        rule = _get_rules()["T03"]
        result = evaluate_custom_check(df, rule, today=TODAY)
        assert len(result) == 1
        assert result.index[0] == "__aggregate__"

    def test_C05_nan_pe_pb_caught(self):
        """Effective C05 must include pe/pb — NaN in either must trip the rule.

        Regression for the incomplete fix-2: only fundamental.yaml was
        patched while the stale bars_daily.yaml copy shadowed it through
        first-wins dedup, so NaN pe/pb passed unflagged.
        """
        rule = _get_rules()["C05"]
        for col in ("pe", "pb"):
            df = _make_clean_daily().copy()
            df["pe"] = 15.0
            df["pb"] = 1.5
            df.loc[0, col] = np.nan
            result = evaluate_custom_check(df, rule)
            assert result.iloc[0], f"NaN {col} must be caught by effective C05"

    def test_C07_forward_coverage_enforced(self):
        """A 2-date calendar cannot cover forward_days=30 — must fail on coverage.

        Regression: the < forward_days coverage guard used to fire only below
        2 dates, so a 2-date gapless calendar passed vacuously.
        """
        cal = pd.DataFrame({"trade_date": ["2024-01-02", "2024-01-03"]})
        rule = _get_rules()["C07"]
        result = evaluate_custom_check(
            cal, rule, forward_days=30, today=pd.Timestamp("2024-01-02")
        )
        assert result.iloc[0], "2-date calendar must fail forward coverage"
