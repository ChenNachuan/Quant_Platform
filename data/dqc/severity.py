"""DQC 严重级别与处置策略配置。

核心机制：
1. :class:`DQCSeverityConfig`：单规则行处置方式（``drop``/``flag``/``pass``）、升级阈值与粒度。
2. 三级硬拒收粒度：行级剔除（``row``）、分组剔除（``partition``）、整批拦截（``batch``）。
3. :class:`QualityFlag`：int64 行级质量位掩码（目前分配前 5 位，其余预留）。
4. :class:`BatchRejectConfig`：全局批次安全阀。

原则：HARD 违规剔除或进隔离区，SOFT 仅标记 ``quality_flag``，两者互不越界。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from core.types import QualityFlag

if TYPE_CHECKING:  # pragma: no cover - 仅类型检查导入，避免循环依赖
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

# 已分配 quality_flag 比特位的 5 条 SOFT 规则（bit5+ 预留）
RULE_FLAG_BITS: dict[str, QualityFlag] = {
    "C02": QualityFlag.C02_COVERAGE,  # 1 << 0：覆盖率不足
    "C05": QualityFlag.C05_NULL_RATE,  # 1 << 1：软字段空值率偏高
    "L04": QualityFlag.L04_BEYOND_LIMIT,  # 1 << 2：超出涨跌停幅度
    "L07": QualityFlag.L07_ADJFACTOR_RANGE,  # 1 << 3：复权因子范围异常
    "K01": QualityFlag.K01_OVERNIGHT_GAP,  # 1 << 4：隔夜跳空过大
}


@dataclass(frozen=True)
class DQCSeverityConfig:
    """单规则处置策略。

    - ``row_action``：``drop`` 剔除违规行；``flag`` 保留行并记标；``pass`` 不改行（用于整批门禁）。
    - ``escalation_threshold``：触发升级的失败比例阈值；``drop``/``pass`` 超限拒收整批，
      ``flag``（仅 K06–K08）超限仅发升级告警；``None`` 表示不升级。
    - ``granularity``：``row`` 按行；``partition`` 扩展至整个 ``(ticker, trade_date)``；``batch`` 整批判定。
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


# 逐规则处置配置表（flag 规则仅 K06–K08 设 10% 告警阈值且保留数据行）
SEVERITY_CONFIGS: dict[str, DQCSeverityConfig] = {
    # 完整性
    "C01": DQCSeverityConfig("pass", 0.0, "batch"),  # 缺失交易日直接整批拒收
    "C02": DQCSeverityConfig("flag", None, "batch"),  # 覆盖率不足仅打日标
    "C03": DQCSeverityConfig(
        "drop", 0.05, "partition"
    ),  # 分钟线根数异常按 ticker×date 剔除
    "C04": DQCSeverityConfig("drop", 0.20),  # 关键字段空值超 20% 拒收
    "C05": DQCSeverityConfig("flag"),
    "C06": DQCSeverityConfig("drop", 0.05),
    "C07": DQCSeverityConfig("pass", 0.0, "batch"),  # 日历缺口直接拒收
    "C08": DQCSeverityConfig("flag", None, "batch"),
    "C09": DQCSeverityConfig("flag", None, "batch"),
    "C10": DQCSeverityConfig("pass", 0.0, "batch"),
    # 合法性
    "L01": DQCSeverityConfig("drop", 0.01),  # 负价格视为数据损坏
    "L02": DQCSeverityConfig("drop", 0.01),
    "L03": DQCSeverityConfig("drop", 0.01),
    "L04": DQCSeverityConfig("flag"),  # 涨跌停突破多属新股/特殊交易日，仅标记
    "L05": DQCSeverityConfig("flag"),
    "L06": DQCSeverityConfig("drop", 0.01),
    "L07": DQCSeverityConfig("flag"),
    "L08": DQCSeverityConfig("drop", 0.01),
    "L09": DQCSeverityConfig("flag"),
    "L10": DQCSeverityConfig("drop", 0.05),
    "L11": DQCSeverityConfig("drop", 0.05),
    "L12": DQCSeverityConfig("flag"),
    "L13": DQCSeverityConfig("drop", 0.01),  # 上游格式变更
    "L14": DQCSeverityConfig("drop", 0.01),
    # 一致性
    "K01": DQCSeverityConfig("flag"),
    "K02": DQCSeverityConfig("drop", 0.05),
    "K03": DQCSeverityConfig("drop", 0.05),
    "K04": DQCSeverityConfig("flag"),
    "K05": DQCSeverityConfig("drop", 0.01),  # 非除权日复权因子突变
    "K06": DQCSeverityConfig("flag", 0.10),  # 跨粒度偏差超 10% 告警但保留行
    "K07": DQCSeverityConfig("flag", 0.10),
    "K08": DQCSeverityConfig("flag", 0.10),
    "K09": DQCSeverityConfig("flag"),
    "K10": DQCSeverityConfig("pass", 0.0, "batch"),
    "K11": DQCSeverityConfig("flag"),
    # 时效性
    "T01": DQCSeverityConfig("pass", 0.0, "batch"),  # 数据过期拒绝入库
    "T02": DQCSeverityConfig("pass", 0.0, "batch"),
    "T03": DQCSeverityConfig("drop", 0.0),  # 出现 1 条未来日期即整批拒收
    "T04": DQCSeverityConfig("drop", None, "partition"),  # 时间倒流按分区剔除
    "T05": DQCSeverityConfig("drop", None, "partition"),
    "T06": DQCSeverityConfig("flag"),
    "T07": DQCSeverityConfig("flag"),
    "T08": DQCSeverityConfig("flag"),
    "T09": DQCSeverityConfig("flag"),
}


def severity_config_for(rule_id: str) -> DQCSeverityConfig:
    """返回 *rule_id* 的处置配置；未登记规则直接报错（fail-closed）。"""
    try:
        return SEVERITY_CONFIGS[rule_id]
    except KeyError:
        raise KeyError(
            f"no DQCSeverityConfig for rule {rule_id!r}; add it to "
            "data.dqc.severity.SEVERITY_CONFIGS before enabling the rule"
        ) from None


@dataclass(frozen=True)
class BatchRejectConfig:
    """全局批次安全阀：``max_hard_drop_rate`` 超限拒收整批，``soft_alarm_rate`` 超限仅告警。"""

    max_hard_drop_rate: float = 0.20
    soft_alarm_rate: float = 0.50


class DQCBatchReject(Exception):
    """批次被 DQC 拒收并写入隔离区时抛出的异常（``detail`` 含拒绝详情元数据）。"""

    def __init__(self, message: str, detail: dict | None = None) -> None:
        super().__init__(message)
        self.detail: dict = detail or {}


def compute_quality_flag(
    df: pd.DataFrame, soft_results: list[CheckResult]
) -> pd.Series:
    """按 ``RULE_FLAG_BITS`` 计算逐行 ``quality_flag``（int64 按位或掩码，未分配比特的规则跳过）。"""
    flags = pd.Series(0, index=df.index, dtype="int64")
    for result in soft_results:
        bit = RULE_FLAG_BITS.get(result.rule_id)
        if bit is None:
            continue
        mask = result.failing_rows.astype(bool).reindex(df.index, fill_value=False)
        flags = flags | (mask.astype("int64") * int(bit))
    return flags
