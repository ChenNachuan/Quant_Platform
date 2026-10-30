"""Structural tests for DQC rules YAML — 44 rules across 5 asset files.

Validates rule count, ID continuity, HARD/SOFT breakdown, applicability marks,
required fields, and per-HARD boundary declarations.
Source of truth: docs/design/data/dqc_rules.md (four dimension tables).
"""

import re
from pathlib import Path

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
        """A rule appearing in multiple YAMLs must have identical severity and type."""
        entries = _load_all_rules()
        by_id = {}
        for fname, rule in entries:
            rid = rule["id"]
            if rid not in by_id:
                by_id[rid] = []
            by_id[rid].append((fname, rule))
        for rid, occurrences in by_id.items():
            if len(occurrences) > 1:
                severities = {r["severity"] for _f, r in occurrences}
                types = {r["type"] for _f, r in occurrences}
                assert len(severities) == 1, (
                    f"{rid}: inconsistent severity across files: {severities}"
                )
                assert len(types) == 1, (
                    f"{rid}: inconsistent type across files: {types}"
                )
