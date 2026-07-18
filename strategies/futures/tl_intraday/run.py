"""CLI entry point for the TL intraday report reproduction."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from engine.backtest.intraday_futures import performance_metrics
from infra.futures_continuous import (
    build_volume_dominant_series,
    load_contract_bars,
)
from strategies.futures.tl_intraday.backtest import (
    run_report_strategies,
    with_source_frequency,
)
from strategies.futures.tl_intraday.config import (
    DEFAULT_CONFIG,
    REPORT_BENCHMARKS,
    REPORT_METRIC_ORDER,
)


def _metrics_frame(results: dict[str, object]) -> pd.DataFrame:
    rows = []
    for name in ["adx_base", "adx_stopped", "momentum_base", "momentum_stopped"]:
        metrics = results[name].metrics
        rows.append({"strategy": name, **metrics})
    rows.append({"strategy": "composite", **results["composite_metrics"]})
    return pd.DataFrame(rows).set_index("strategy")


def _period_metrics_frame(results: dict[str, object], config) -> pd.DataFrame:
    periods = {
        "full": (config.start, "2025-12-31"),
        "oos": (config.out_of_sample_start, "2025-12-31"),
        "2023": ("2023-04-21", "2023-12-31"),
        "2024": ("2024-01-01", "2024-12-31"),
        "2025": ("2025-01-01", "2025-12-31"),
    }
    return_series = {
        "adx_stopped": results["adx_stopped"].daily_returns,
        "momentum_stopped": results["momentum_stopped"].daily_returns,
        "composite": results["composite_daily_returns"],
    }
    rows = []
    for strategy, returns in return_series.items():
        for period, (start, end) in periods.items():
            rows.append(
                {
                    "strategy": strategy,
                    "period": period,
                    **performance_metrics(returns.loc[start:end]),
                }
            )
    return pd.DataFrame(rows).set_index(["strategy", "period"])


def _benchmark_comparison(period_metrics: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (strategy, period), report_values in REPORT_BENCHMARKS.items():
        for metric, report_value in zip(REPORT_METRIC_ORDER, report_values):
            reproduced = float(period_metrics.loc[(strategy, period), metric])
            rows.append(
                {
                    "strategy": strategy,
                    "period": period,
                    "metric": metric,
                    "report": report_value,
                    "reproduced": reproduced,
                    "difference": reproduced - report_value,
                }
            )
    return pd.DataFrame(rows)


def _save_outputs(
    bars: pd.DataFrame,
    results: dict[str, object],
    output_root: Path,
    config,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    metrics = _metrics_frame(results)
    period_metrics = _period_metrics_frame(results, config)
    comparison = _benchmark_comparison(period_metrics)
    metrics.to_csv(output_root / "metrics.csv", encoding="utf-8-sig")
    period_metrics.to_csv(output_root / "period_metrics.csv", encoding="utf-8-sig")
    comparison.to_csv(
        output_root / "benchmark_comparison.csv", index=False, encoding="utf-8-sig"
    )
    pd.DataFrame(
        {
            "adx_base": results["adx_base"].daily_equity,
            "adx_stopped": results["adx_stopped"].daily_equity,
            "momentum_base": results["momentum_base"].daily_equity,
            "momentum_stopped": results["momentum_stopped"].daily_equity,
            "composite": results["composite_equity"],
        }
    ).to_csv(output_root / "daily_equity.csv", encoding="utf-8-sig")
    results["er_diagnostic"].to_csv(output_root / "daily_er.csv", encoding="utf-8-sig")
    for name in ["adx_base", "adx_stopped", "momentum_base", "momentum_stopped"]:
        results[name].executions.to_csv(
            output_root / f"{name}_executions.csv", index=False, encoding="utf-8-sig"
        )
    bars[["dominant_code", "adjustment_factor"]].groupby(
        pd.to_datetime(bars.index).date
    ).first().to_csv(output_root / "dominant_contracts.csv", encoding="utf-8-sig")

    metadata = {
        "config": config.to_dict(),
        "adx_stop_threshold": results["adx_stop_threshold"],
        "er_stop_threshold": results["er_stop_threshold"],
        "blocked_adx_dates": [str(value) for value in results["blocked_adx_dates"]],
        "blocked_momentum_dates": [
            str(value) for value in results["blocked_momentum_dates"]
        ],
    }
    (output_root / "run_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    equity = pd.read_csv(
        output_root / "daily_equity.csv", index_col=0, parse_dates=True
    )
    figure, axis = plt.subplots(figsize=(11, 6))
    equity.plot(ax=axis)
    axis.set_title("TL intraday report reproduction")
    axis.set_xlabel("Date")
    axis.set_ylabel("Net asset value")
    axis.grid(alpha=0.25)
    figure.tight_layout()
    figure.savefig(output_root / "daily_equity.png", dpi=180)
    plt.close(figure)


def _month_keys(start: str, end_exclusive: str) -> list[tuple[int, int]]:
    current = pd.Timestamp(start).replace(day=1)
    final = (pd.Timestamp(end_exclusive) - pd.Timedelta(days=1)).replace(day=1)
    months = []
    while current <= final:
        months.append((current.year, current.month))
        current += pd.offsets.MonthBegin(1)
    return months


def _missing_months(data_root: Path, start: str, end_exclusive: str) -> list[str]:
    missing = []
    for year, month in _month_keys(start, end_exclusive):
        parquet = data_root / f"year={year}" / f"month={month:02d}.parquet"
        success = data_root / f"year={year}" / f"month={month:02d}_SUCCESS"
        if not parquet.exists() or not success.exists():
            missing.append(f"{year}-{month:02d}")
    return missing


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path)
    parser.add_argument(
        "--allow-5min-proxy",
        action="store_true",
        help="Use existing 5-minute data if the report-faithful 1-minute data is absent.",
    )
    parser.add_argument("--output-root", type=Path)
    args = parser.parse_args()

    config = DEFAULT_CONFIG
    data_root = args.data_root or config.raw_data_root
    source_minutes = 1
    missing_months = _missing_months(data_root, config.start, config.end_exclusive)
    if missing_months:
        if not args.allow_5min_proxy:
            raise FileNotFoundError(
                f"TL 1-minute data is incomplete at {data_root}; missing "
                f"{', '.join(missing_months)}. Run "
                "`docker-compose run --rm amazingdata python3 "
                "/scripts/data_fetcher/fetch_tl_min1.py` first, or explicitly use "
                "--allow-5min-proxy."
            )
        data_root = config.proxy_data_root
        source_minutes = 5
    config = with_source_frequency(config, source_minutes)
    output_root = args.output_root or config.artifact_root

    raw = load_contract_bars(
        data_root,
        symbol=config.symbol,
        start=config.start,
        end=config.end_exclusive,
        contract_multiplier=config.contract_multiplier,
    )
    bars = build_volume_dominant_series(raw)
    results = run_report_strategies(bars, config)
    _save_outputs(bars, results, output_root, config)
    print(_metrics_frame(results).round(6).to_string())
    print(f"\nOutputs: {output_root}")


if __name__ == "__main__":
    main()
