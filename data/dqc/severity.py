"""DQC severity — disposition of check findings (S9-T3).

Field authority: ``docs/design/data/dqc_severity.md`` (§37).  This module
answers "what to do about a finding"; *what to check* lives in
``data/dqc/rules/`` + ``data/dqc/checks/`` (S9-T2) and execution lives in
:mod:`data.dqc.engine`.

Core mechanisms (design §37):
1. :class:`DQCSeverityConfig` — per-rule ``row_action`` + escalation threshold,
   the 44-rule disposition table of §37.3.
2. Three-level hard-reject granularity: row drop / partition reject / batch
   reject (``granularity`` field).
3. :class:`QualityFlag` — int64 row-level bitmask, re-exported from
   :mod:`core.types` (five bits, ``storage_schema.md`` §47.15).
4. :class:`BatchRejectConfig` — global batch safety valve.

HARD findings go to quarantine, SOFT findings only mark ``quality_flag`` — the
two are never interchanged (issue red line).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from core.types import QualityFlag

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids the import cycle
    from data.dqc.engine import CheckResult

__all__ = [
    "QualityFlag",
    "RULE_FLAG_BITS",
    "DQCSeverityConfig",
    "SEVERITY_CONFIGS",
    "severity_config_for",
    "BatchRejectConfig",
    "DQCBatchReject",
    "compute_quality_flag",
]

# rule_id -> quality_flag bit for the five §47.15 bits.  The other SOFT rules
# have no bit yet (bit5+ reserved; §37 open item "bit assignment outside the
# five").  Rows they flag carry no bit until that ruling lands.
RULE_FLAG_BITS: dict[str, QualityFlag] = {
    "C02": QualityFlag.C02_COVERAGE,  # 1 << 0 — coverage insufficient
    "C05": QualityFlag.C05_NULL_RATE,  # 1 << 1 — null rate high
    "L04": QualityFlag.L04_BEYOND_LIMIT,  # 1 << 2 — beyond price limit
    "L07": QualityFlag.L07_ADJFACTOR_RANGE,  # 1 << 3 — adjfactor range abnormal
    "K01": QualityFlag.K01_OVERNIGHT_GAP,  # 1 << 4 — overnight gap too large
}


@dataclass(frozen=True)
class DQCSeverityConfig:
    """Per-rule disposition (§37.2/§37.3).

    ``row_action`` — what failing rows get: ``"drop"`` removes them,
    ``"flag"`` marks ``quality_flag``, ``"pass"`` touches no row (batch-level
    gates).  ``escalation_threshold`` — failing-row rate above which the rule
    escalates: batch reject for ``drop``/``pass`` rules, an escalated alarm for
    ``flag`` rules (K06–K08 only; ``None`` = never escalate).  ``granularity``
    is the three-level hard-reject scale: ``"row"`` drops the failing rows,
    ``"partition"`` expands to whole ticker×date groups, ``"batch"`` judges
    the batch as a whole.
    """

    row_action: str  # "drop" | "flag" | "pass"
    escalation_threshold: float | None = None
    granularity: str = "row"  # "row" | "partition" | "batch"

    def __post_init__(self) -> None:
        if self.row_action not in ("drop", "flag", "pass"):
            raise ValueError(f"bad row_action {self.row_action!r}")
        if self.granularity not in ("row", "partition", "batch"):
            raise ValueError(f"bad granularity {self.granularity!r}")
        if self.escalation_threshold is not None and not (
            0.0 <= self.escalation_threshold <= 1.0
        ):
            raise ValueError(
                f"escalation_threshold out of [0, 1]: {self.escalation_threshold}"
            )


# §37.3 disposition table, rule by rule.  "flag" rules never escalate except
# K06–K08 (alarm at 10%, rows kept).  C07–C10 are absent from the §37.3 table
# and follow the same regime: HARD aggregate rules are batch gates
# (row_action "pass", escalation 0), SOFT aggregate rules flag the batch.
SEVERITY_CONFIGS: dict[str, DQCSeverityConfig] = {
    # completeness
    "C01": DQCSeverityConfig("pass", 0.0, "batch"),  # date missing -> hard reject
    "C02": DQCSeverityConfig("flag", None, "batch"),  # coverage low -> day flag
    "C03": DQCSeverityConfig("drop", 0.05, "partition"),  # drop ticker×date
    "C04": DQCSeverityConfig("drop", 0.20),  # >20% null critical -> reject
    "C05": DQCSeverityConfig("flag"),  # never escalates
    "C06": DQCSeverityConfig("drop", 0.05),
    "C07": DQCSeverityConfig("pass", 0.0, "batch"),  # gap in calendar -> reject
    "C08": DQCSeverityConfig("flag", None, "batch"),
    "C09": DQCSeverityConfig("flag", None, "batch"),
    "C10": DQCSeverityConfig("pass", 0.0, "batch"),
    # legality
    "L01": DQCSeverityConfig("drop", 0.01),  # negative price = corruption
    "L02": DQCSeverityConfig("drop", 0.01),
    "L03": DQCSeverityConfig("drop", 0.01),
    "L04": DQCSeverityConfig("flag"),  # limit breaches are often legal
    "L05": DQCSeverityConfig("flag"),
    "L06": DQCSeverityConfig("drop", 0.01),
    "L07": DQCSeverityConfig("flag"),
    "L08": DQCSeverityConfig("drop", 0.01),  # schema violation
    "L09": DQCSeverityConfig("flag"),
    "L10": DQCSeverityConfig("drop", 0.05),  # structural missing
    "L11": DQCSeverityConfig("drop", 0.05),
    "L12": DQCSeverityConfig("flag"),
    "L13": DQCSeverityConfig("drop", 0.01),  # format change upstream
    "L14": DQCSeverityConfig("drop", 0.01),
    # consistency
    "K01": DQCSeverityConfig("flag"),
    "K02": DQCSeverityConfig("drop", 0.05),
    "K03": DQCSeverityConfig("drop", 0.05),
    "K04": DQCSeverityConfig("flag"),
    "K05": DQCSeverityConfig("drop", 0.01),  # adjfactor mutation
    "K06": DQCSeverityConfig("flag", 0.10),  # cross-granularity: alarm at 10%
    "K07": DQCSeverityConfig("flag", 0.10),  # (rows kept, §37.3)
    "K08": DQCSeverityConfig("flag", 0.10),
    "K09": DQCSeverityConfig("flag"),
    "K10": DQCSeverityConfig("pass", 0.0, "batch"),
    "K11": DQCSeverityConfig("flag"),
    # timeliness
    "T01": DQCSeverityConfig("pass", 0.0, "batch"),  # stale -> no ingest
    "T02": DQCSeverityConfig("pass", 0.0, "batch"),
    "T03": DQCSeverityConfig("drop", 0.0),  # 1 future row rejects the batch
    "T04": DQCSeverityConfig("drop", None, "partition"),  # time reversal
    "T05": DQCSeverityConfig("drop", None, "partition"),
    "T06": DQCSeverityConfig("flag"),
    "T07": DQCSeverityConfig("flag"),
    "T08": DQCSeverityConfig("flag"),
    "T09": DQCSeverityConfig("flag"),
}


def severity_config_for(rule_id: str) -> DQCSeverityConfig:
    """Return the disposition for *rule_id*.

    Fail-closed: a rule missing from the table must be classified before it
    may run, never silently defaulted.
    """
    try:
        return SEVERITY_CONFIGS[rule_id]
    except KeyError:
        raise KeyError(
            f"no DQCSeverityConfig for rule {rule_id!r}; add it to "
            "data.dqc.severity.SEVERITY_CONFIGS before enabling the rule"
        ) from None


@dataclass(frozen=True)
class BatchRejectConfig:
    """Global batch safety valve (§37.6).

    ``max_hard_drop_rate`` — HARD ``drop`` rows above this share of the batch
    reject the whole batch (the "many rules each failing a little" case).
    ``soft_alarm_rate`` — rows with ``quality_flag != 0`` above this share
    raise an alarm only; SOFT findings never reject.
    """

    max_hard_drop_rate: float = 0.20
    soft_alarm_rate: float = 0.50


class DQCBatchReject(Exception):
    """The batch was refused and saved to quarantine.

    ``detail`` is the rejection metadata written next to the quarantined
    parquet (``data.quarantine.save_quarantine``): data_date, asset_type,
    reject_time, batch_status, total_rows, rows_would_drop, drop_rate,
    hard_rules_triggered, reject_reason.
    """

    def __init__(self, message: str, detail: dict | None = None) -> None:
        super().__init__(message)
        self.detail: dict = detail or {}


def compute_quality_flag(
    df: pd.DataFrame, soft_results: list[CheckResult]
) -> pd.Series:
    """Return the per-row ``quality_flag`` int64 bitmask (§37.4).

    Each SOFT result contributes its bit (``RULE_FLAG_BITS``) on its failing
    rows; rows failing several rules OR the bits together.  SOFT rules without
    a bit contribute nothing (bit5+ reserved).  Masks are reindexed onto
    *df* so this works on the post-drop frame too.
    """
    flags = pd.Series(0, index=df.index, dtype="int64")
    for result in soft_results:
        bit = RULE_FLAG_BITS.get(result.rule_id)
        if bit is None:
            continue
        mask = result.failing_rows.astype(bool).reindex(df.index, fill_value=False)
        flags = flags | (mask.astype("int64") * int(bit))
    return flags
