"""Consistency checks binding the QDC migration contract doc to storage_schema.

docs/design/data/qdc_migration_contract.md is the binding M1 spec that M2-M5
execute against. These tests fail when the doc drifts from the schema it claims
to map (columns added/removed, partition paths changed, quality bits renamed).
"""

import re
from pathlib import Path

from data.storage_schema import (
    BARS_DAILY_SCHEMA,
    CALENDAR_SCHEMA,
    CORPORATE_ACTIONS_SCHEMA,
    FUNDAMENTAL_SCHEMA,
    INDUSTRY_SCHEMA,
    PARTITION_PATHS,
    QUALITY_FLAG_BITS,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = REPO_ROOT / "docs" / "design" / "data" / "qdc_migration_contract.md"

BATCH1_SCHEMAS = {
    "bars_daily": BARS_DAILY_SCHEMA,
    "fundamental": FUNDAMENTAL_SCHEMA,
    "calendar": CALENDAR_SCHEMA,
    "industry": INDUSTRY_SCHEMA,
    "corporate_actions": CORPORATE_ACTIONS_SCHEMA,
}

# contract mapping section -> platform table whose columns it must cover
MAPPING_SECTIONS = {
    "### 4.1": ("### 4.2", "bars_daily"),
    "### 4.2": ("### 4.3", "fundamental"),
    "### 4.3": ("### 4.4", "corporate_actions"),
    "### 4.4": ("### 4.5", "industry"),
}


def _contract_text() -> str:
    return CONTRACT_PATH.read_text(encoding="utf-8")


def _section(text: str, start: str, end: str) -> str:
    i = text.index(start)
    j = text.index(end, i)
    return text[i:j]


def _mapping_columns(section: str) -> set[str]:
    """Platform-column tokens from the section's 平台列 mapping table only."""
    cols: set[str] = set()
    in_mapping = False
    for line in section.splitlines():
        if line.startswith("|"):
            if "平台列" in line:
                in_mapping = True
                continue
            if not in_mapping:
                continue
            first = line.split("|")[1].strip()
            if set(first) <= {"-", " ", ":"}:
                continue  # separator row
            for token in first.split(","):
                token = token.strip().strip("`")
                if token:
                    cols.add(token)
        elif in_mapping:
            break  # mapping table ended
    return cols


def test_contract_exists():
    assert CONTRACT_PATH.is_file(), f"missing binding contract at {CONTRACT_PATH}"


def test_mapping_tables_cover_every_schema_column():
    text = _contract_text()
    for start, (end, table) in MAPPING_SECTIONS.items():
        mapped = _mapping_columns(_section(text, start, end))
        schema_cols = set(BATCH1_SCHEMAS[table].columns)
        missing_in_doc = schema_cols - mapped
        unknown_in_doc = mapped - schema_cols
        assert not missing_in_doc, (
            f"{table} ({start}): schema columns absent from contract: "
            f"{sorted(missing_in_doc)}"
        )
        assert not unknown_in_doc, (
            f"{table} ({start}): contract names unknown columns: {sorted(unknown_in_doc)}"
        )


def test_calendar_mapping_section_declares_no_columns():
    section = _section(_contract_text(), "### 4.5", "### 4.6")
    assert _mapping_columns(section) == set(), "calendar must stay empty"
    assert "恒空" in section


def test_partition_paths_match_schema():
    section = _section(_contract_text(), "### 2.1", "### 2.2")
    for table in BATCH1_SCHEMAS:
        expected = PARTITION_PATHS[table]
        rows = [
            ln
            for ln in section.splitlines()
            if ln.startswith("|") and f"`{expected}`" in ln
        ]
        assert rows, f"{table}: contract 2.1 does not carry schema path {expected}"
        assert table in rows[0].split("|")[1], (
            f"path row {expected} does not name {table}"
        )


def test_quality_flag_bits_match_schema():
    section = _section(_contract_text(), "## 6.", "## 7.")
    doc_bits = {
        name: int(value)
        for name, value in re.findall(
            r"\|\s*\d+\s*\|\s*([A-Z0-9_]+)\s*\|\s*(\d+)\s*\|", section
        )
    }
    expected = dict(QUALITY_FLAG_BITS)
    assert doc_bits == expected, (
        f"quality_flag bits drift: doc={doc_bits} schema={expected}"
    )
    assert "CLEAN = 0" in section.replace("`", "")
    assert "不扩位" in section


def test_schema_carries_no_stored_adjusted_price():
    # invariant I-1: qfq/hfq are derived at read time, never stored
    for table, schema in BATCH1_SCHEMAS.items():
        for col in schema.columns:
            low = col.lower()
            assert "qfq" not in low and "hfq" not in low, (
                f"{table}.{col} must not store an adjusted price"
            )
