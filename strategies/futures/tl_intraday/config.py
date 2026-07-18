"""Single source of parameters for the TL intraday report reproduction."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class TLIntradayConfig:
    start: str = "2023-04-21"
    end_exclusive: str = "2026-01-01"
    in_sample_end: str = "2025-08-31"
    out_of_sample_start: str = "2025-09-01"
    symbol: str = "TL"
    contract_multiplier: float = 10_000.0
    one_way_cost: float = 0.000005
    source_bar_minutes: int = 1
    execution_minutes: int = 5
    force_flat_time: str = "15:10"
    adx_period_minutes: int = 14
    adx_entry_threshold: float = 35.0
    adx_exit_threshold: float = 20.0
    previous_day_low_quantile: float = 0.10
    previous_day_high_quantile: float = 0.90
    adx_stop_quantile: float = 0.10
    adx_stop_days: int = 2
    macd_fast_minutes: int = 12
    macd_slow_minutes: int = 26
    macd_signal_minutes: int = 9
    momentum_rising_minutes: int = 3
    momentum_reduction: float = 0.20
    er_mean_days: int = 20
    er_stop_quantile: float = 0.25
    raw_data_root: Path = PROJECT_ROOT / "data_lake/future/market_data/1min_tl_rebuilt"
    proxy_data_root: Path = PROJECT_ROOT / "data_lake/future/market_data/5min"
    artifact_root: Path = PROJECT_ROOT / "artifacts/futures_tl_intraday"

    @property
    def execution_bars(self) -> int:
        return max(1, round(self.execution_minutes / self.source_bar_minutes))

    def minute_window(self, minutes: int) -> int:
        return max(1, round(minutes / self.source_bar_minutes))

    def to_dict(self) -> dict:
        values = asdict(self)
        for key, value in values.items():
            if isinstance(value, Path):
                values[key] = str(value)
        return values


DEFAULT_CONFIG = TLIntradayConfig()


REPORT_BENCHMARKS = {
    ("adx_stopped", "full"): (0.0361, 1.46, 2.39, 0.4929, 1.34, -0.0151),
    ("adx_stopped", "oos"): (0.1264, 4.05, 16.74, 0.6027, 2.11, -0.0076),
    ("adx_stopped", "2023"): (0.0065, 0.42, 0.43, 0.4444, 1.11, -0.0151),
    ("adx_stopped", "2024"): (0.0329, 1.23, 2.19, 0.4818, 1.25, -0.0150),
    ("adx_stopped", "2025"): (0.0601, 2.17, 4.68, 0.5304, 1.54, -0.0129),
    ("momentum_stopped", "full"): (0.0484, 1.93, 3.27, 0.5458, 1.51, -0.0148),
    ("momentum_stopped", "oos"): (0.0451, 2.58, 11.07, 0.6047, 1.84, -0.0041),
    ("momentum_stopped", "2023"): (0.0252, 1.45, 1.96, 0.5127, 1.30, -0.0128),
    ("momentum_stopped", "2024"): (0.0637, 2.03, 6.43, 0.5641, 1.62, -0.0099),
    ("momentum_stopped", "2025"): (0.0538, 2.19, 4.43, 0.5539, 1.49, -0.0121),
    ("composite", "full"): (0.0431, 2.12, 3.68, 0.5138, 1.53, -0.0117),
    ("composite", "oos"): (0.0855, 3.82, 12.67, 0.5610, 2.02, -0.0068),
    ("composite", "2023"): (0.0159, 1.24, 1.35, 0.5063, 1.26, -0.0117),
    ("composite", "2024"): (0.0483, 1.98, 5.56, 0.5064, 1.52, -0.0087),
    ("composite", "2025"): (0.0570, 2.83, 6.63, 0.5273, 1.67, -0.0086),
}

REPORT_METRIC_ORDER = (
    "annual_return",
    "sharpe",
    "calmar",
    "win_rate",
    "profit_loss_ratio",
    "max_drawdown",
)


__all__ = [
    "DEFAULT_CONFIG",
    "PROJECT_ROOT",
    "REPORT_BENCHMARKS",
    "REPORT_METRIC_ORDER",
    "TLIntradayConfig",
]
