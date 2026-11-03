"""Integration tests for ``research.factor_evaluation`` (S11-T1).

Acceptance coverage (QUANT-43):

- ac-1  adapter formats: wide ↔ MultiIndex Series ↔ wide, ffill(limit=5)
- ac-2  IC/IR parity with direct alphalens API calls
- ac-3  gate formula ``t > max(2, sqrt(2*ln(N)))`` at N=1 and N>1
- ac-4  three-layer persistence: metadata.json eval block, factor_eval/
        Parquets, HTML tearsheet

Subject under test is the research-side pipeline; alphalens is exercised for
real (it is the integration being tested).  Data is synthetic — 5 tickers ×
about a year of business days — per docs/design §27.13 validation discipline.
"""

import json
import math
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from research.factor_evaluation import (
    AlphalensAdapter,
    EXIT_IC_THRESHOLD,
    EvalResult,
    FactorPipeline,
    GATE_IC_MIN,
    GATE_IR_MIN,
    GATE_T_BASE,
    _load_attempts,
    check_exit,
    factor_family,
)
from research.factor_storage import load_factor, save_factor_append

pytestmark = pytest.mark.integration

TICKERS = ["000001.SZ", "000002.SZ", "600000.SH", "300750.SZ", "601318.SH"]
N_DAYS = 330
MOMENTUM_EXPR = "close / rolling_mean(close, 20) - 1"
PERIOD_COLS = ["1D", "5D", "10D", "20D"]


# ---------------------------------------------------------------------------
# Fixtures and builders
# ---------------------------------------------------------------------------


@pytest.fixture
def bars() -> pd.DataFrame:
    """Synthetic daily OHLCV: geometric random walk, 5 tickers, no gaps."""
    rng = np.random.default_rng(7)
    dates = pd.date_range("2023-01-02", periods=N_DAYS, freq="B")
    rets = rng.normal(0.0005, 0.02, size=(N_DAYS, len(TICKERS)))
    close = 100.0 * np.exp(np.cumsum(rets, axis=0))
    frames = []
    for j, ticker in enumerate(TICKERS):
        frames.append(
            pd.DataFrame(
                {
                    "date": dates,
                    "ticker": ticker,
                    "open": close[:, j],
                    "high": close[:, j],
                    "low": close[:, j],
                    "close": close[:, j],
                    "volume": 1e6,
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


class _StubDataAPI:
    """``load_bars`` double over a fixed synthetic frame."""

    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame
        self.calls: list = []

    def load_bars(self, symbols, start, end, **kwargs):
        self.calls.append((tuple(symbols), start, end, kwargs))
        df = self._frame[self._frame["ticker"].isin(symbols)]
        lo, hi = pd.to_datetime(start), pd.to_datetime(end)
        dates = pd.to_datetime(df["date"])
        return df.loc[(dates >= lo) & (dates <= hi)].copy().reset_index(drop=True)


@pytest.fixture
def stub(bars) -> _StubDataAPI:
    return _StubDataAPI(bars)


@pytest.fixture
def pipeline(stub, tmp_path) -> FactorPipeline:
    return FactorPipeline(
        stub,
        base_dir=tmp_path / "data_platform",
        reports_dir=tmp_path / "reports",
    )


def make_factor_frame(
    bars: pd.DataFrame, kind: str, noise: float = 0.0
) -> pd.DataFrame:
    """Long ``[date, ticker, factor]`` frame.

    ``kind="next_return"`` is look-ahead ground truth — the factor IS the
    next-day forward return — so its 1D rank IC is ≈ 1.0 by construction.
    ``kind="random"`` is seeded noise with ≈ 0 IC.  Tests build ground truth
    directly because the expr DSL has no lookahead operator on purpose.
    """
    rng = np.random.default_rng(13)
    parts = []
    for ticker, grp in bars.groupby("ticker", sort=True):
        grp = grp.sort_values("date").reset_index(drop=True)
        if kind == "next_return":
            value = grp["close"].shift(-1) / grp["close"] - 1.0
            value = value + noise * rng.normal(size=len(grp))
        else:
            value = pd.Series(rng.normal(size=len(grp)))
        parts.append(
            pd.DataFrame(
                {"date": grp["date"].values, "ticker": ticker, "factor": value}
            )
        )
    out = pd.concat(parts, ignore_index=True)
    return out.dropna(subset=["factor"]).reset_index(drop=True)


def _sample_factor_data() -> pd.DataFrame:
    """Hand-built alphalens-shaped frame: 30 dates × 4 assets, 4 quantiles."""
    dates = pd.date_range("2024-01-01", periods=30, freq="B")
    assets = ["a", "b", "c", "d"]
    index = pd.MultiIndex.from_product([dates, assets], names=["date", "asset"])
    quantiles = np.tile([1.0, 2.0, 3.0, 4.0], len(dates))
    return pd.DataFrame({"factor_quantile": quantiles}, index=index)


def _sample_ic_and_returns():
    """Deterministic ic_series (6 dates) and mean-by-quantile table (4×4)."""
    dates = pd.date_range("2024-01-01", periods=6, freq="B")
    ic = pd.DataFrame(
        {
            "1D": [0.10, 0.20, 0.30, 0.20, 0.40, 0.30],
            "5D": [0.05, 0.15, 0.25, 0.15, 0.35, 0.25],
            "10D": [0.02, 0.12, 0.22, 0.12, 0.32, 0.22],
            "20D": [0.01, 0.11, 0.21, 0.11, 0.31, 0.21],
        },
        index=dates,
    )
    ic.index.name = "date"
    means = pd.DataFrame(
        {
            "1D": [0.001, 0.002, 0.003, 0.004],
            "5D": [0.002, 0.004, 0.006, 0.008],
            "10D": [0.003, 0.006, 0.009, 0.012],
            "20D": [0.004, 0.008, 0.012, 0.016],
        },
        index=pd.Index([1, 2, 3, 4], name="factor_quantile"),
    )
    return ic, means


# ---------------------------------------------------------------------------
# ac-1: AlphalensAdapter formats
# ---------------------------------------------------------------------------


class TestAlphalensAdapterFormats:
    def test_to_al_factor_wide_to_multiindex(self):
        wide = pd.DataFrame(
            {"000001.SZ": [1.0, 2.0, np.nan], "000002.SZ": [3.0, np.nan, 4.0]},
            index=pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
        )
        wide.index.name = "date"

        series = AlphalensAdapter.to_al_factor(wide)

        assert isinstance(series, pd.Series)
        assert list(series.index.names) == ["date", "asset"]
        assert series.name == "factor"
        assert series.index.is_monotonic_increasing
        # NaN cells are not observations and are dropped.
        assert len(series) == 4
        assert series.loc[(pd.Timestamp("2024-01-02"), "000002.SZ")] == 3.0
        assert series.dtype == "float64"

    def test_to_al_factor_rejects_series_input(self):
        with pytest.raises(TypeError, match="DataFrame"):
            AlphalensAdapter.to_al_factor(pd.Series([1.0, 2.0]))

    def test_to_al_factor_rejects_empty(self):
        with pytest.raises(ValueError, match="empty"):
            AlphalensAdapter.to_al_factor(pd.DataFrame(index=pd.to_datetime([])))

    def test_to_al_prices_long_to_wide(self, bars):
        wide = AlphalensAdapter.to_al_prices(bars)

        assert list(wide.columns) == sorted(TICKERS)
        assert wide.index.is_monotonic_increasing
        assert str(wide.index.dtype).startswith("datetime64")
        # pivot round-trip: every cell matches the long table
        for ticker in TICKERS:
            expected = bars.loc[bars["ticker"] == ticker, "close"].to_numpy()
            assert wide[ticker].to_numpy() == pytest.approx(expected)

    def test_to_al_prices_ffills_short_gap_only(self):
        dates = pd.date_range("2024-01-02", periods=12, freq="B")
        rows = []
        # ticker A misses 3 days (2,3,4) — within the 5-day limit → filled
        for i in [0, 1, 5, 6, 7, 8, 9, 10, 11]:
            rows.append({"date": dates[i], "ticker": "A", "close": 10.0 + i})
        # ticker B misses 6 days (5..10) — beyond the limit → tail stays NaN.
        # B covers exactly the days A misses so every date is in the union.
        for i in [0, 1, 2, 3, 4, 11]:
            rows.append({"date": dates[i], "ticker": "B", "close": 20.0 + i})
        long = pd.DataFrame(rows)

        wide = AlphalensAdapter.to_al_prices(long)

        assert len(wide) == 12
        # A: 3-day suspension forward-filled from the 11.0 at position 1
        assert list(wide["A"].iloc[2:5]) == [11.0, 11.0, 11.0]
        assert wide["A"].iloc[5] == 15.0
        # B: 5 filled then 1 remaining NaN (6-day gap, limit=5)
        assert list(wide["B"].iloc[5:10]) == [24.0, 24.0, 24.0, 24.0, 24.0]
        assert math.isnan(wide["B"].iloc[10])
        assert wide["B"].iloc[11] == 31.0

    def test_to_al_prices_missing_column_raises(self, bars):
        with pytest.raises(ValueError, match="missing columns"):
            AlphalensAdapter.to_al_prices(bars.drop(columns=["close"]))

    def test_extract_metrics_values_match_formulas(self):
        import alphalens.performance as al_perf

        ic, means = _sample_ic_and_returns()
        factor_data = _sample_factor_data()

        metrics = AlphalensAdapter.extract_metrics(factor_data, ic, means)

        assert set(metrics) == {
            "rank_ic_mean",
            "ir",
            "ic_t_stat",
            "quantile_monotonicity",
            "top_minus_bottom",
            "turnover",
        }
        col = "1D"
        expected_mean = ic[col].mean()
        expected_ir = expected_mean / ic[col].std()
        assert metrics["rank_ic_mean"] == pytest.approx(expected_mean)
        assert metrics["ir"] == pytest.approx(expected_ir)
        assert metrics["ic_t_stat"] == pytest.approx(
            expected_ir * math.sqrt(len(ic[col]))
        )
        # strictly increasing means per column → monotonicity 1.0
        assert metrics["quantile_monotonicity"] == pytest.approx(1.0)
        assert metrics["top_minus_bottom"] == pytest.approx(
            means[col].iloc[-1] - means[col].iloc[0]
        )
        expected_turnover = al_perf.quantile_turnover(
            factor_data["factor_quantile"], 4.0, period=1
        ).mean()
        assert metrics["turnover"] == pytest.approx(expected_turnover)

    def test_extract_metrics_period_override(self):
        ic, means = _sample_ic_and_returns()
        factor_data = _sample_factor_data()

        metrics = AlphalensAdapter.extract_metrics(factor_data, ic, means, period="5D")

        assert metrics["rank_ic_mean"] == pytest.approx(ic["5D"].mean())

    def test_extract_metrics_rejects_unknown_period(self):
        ic, means = _sample_ic_and_returns()
        with pytest.raises(ValueError, match="not in ic_series"):
            AlphalensAdapter.extract_metrics(
                _sample_factor_data(), ic, means, period="3M"
            )


# ---------------------------------------------------------------------------
# ac-3: gate formula
# ---------------------------------------------------------------------------


class TestGateFormula:
    @staticmethod
    def _metrics(t_stat: float = 2.5, ic: float = 0.05, ir: float = 0.6) -> dict:
        return {"rank_ic_mean": ic, "ir": ir, "ic_t_stat": t_stat}

    def test_gate_passes_strong_metrics(self):
        passed, detail = AlphalensAdapter.check_gate(self._metrics(), n_attempts=1)
        assert passed is True
        assert all(detail[k]["passed"] for k in ("ic", "ir", "t"))

    def test_gate_n1_t_threshold_degenerates_to_two(self):
        _, detail = AlphalensAdapter.check_gate(self._metrics(), n_attempts=1)
        assert detail["t"]["threshold"] == GATE_T_BASE == 2.0
        assert detail["n_attempts"] == 1

    def test_gate_threshold_rises_with_n(self):
        # sqrt(2*ln(10)) = 2.146, sqrt(2*ln(100)) = 3.035
        _, d10 = AlphalensAdapter.check_gate(self._metrics(t_stat=3.5), n_attempts=10)
        _, d100 = AlphalensAdapter.check_gate(self._metrics(t_stat=3.5), n_attempts=100)
        assert d10["t"]["threshold"] == pytest.approx(math.sqrt(2 * math.log(10)))
        assert d100["t"]["threshold"] == pytest.approx(math.sqrt(2 * math.log(100)))
        assert d10["t"]["threshold"] > GATE_T_BASE
        assert d100["t"]["threshold"] > d10["t"]["threshold"]

    def test_gate_t_between_thresholds_fails_at_high_n(self):
        # t=2.1 clears the N=1 floor but not sqrt(2*ln(10))≈2.146
        passed_small, _ = AlphalensAdapter.check_gate(
            self._metrics(t_stat=2.1), n_attempts=1
        )
        passed_big, detail = AlphalensAdapter.check_gate(
            self._metrics(t_stat=2.1), n_attempts=10
        )
        assert passed_small is True
        assert passed_big is False
        assert detail["t"]["passed"] is False

    @pytest.mark.parametrize(
        "ic,ir,t,failing",
        [
            (GATE_IC_MIN, 0.6, 2.5, "ic"),  # IC strictly above 0.04 required
            (0.05, GATE_IR_MIN, 2.5, "ir"),
            (0.05, 0.6, GATE_T_BASE, "t"),
        ],
    )
    def test_gate_fails_each_threshold_exactly_at_the_line(self, ic, ir, t, failing):
        passed, detail = AlphalensAdapter.check_gate(
            self._metrics(ic=ic, ir=ir, t_stat=t), n_attempts=1
        )
        assert passed is False
        assert detail[failing]["passed"] is False

    def test_gate_rejects_bad_n(self):
        with pytest.raises(ValueError, match="n_attempts"):
            AlphalensAdapter.check_gate(self._metrics(), n_attempts=0)

    def test_exit_line_flags_decayed_factor(self):
        flagged, detail = check_exit({"rank_ic_mean": 0.02})
        assert flagged is True
        assert detail["ic"]["threshold"] == EXIT_IC_THRESHOLD

        flagged, _ = check_exit({"rank_ic_mean": 0.05})
        assert flagged is False


# ---------------------------------------------------------------------------
# FactorPipeline: compute / preprocess
# ---------------------------------------------------------------------------


class TestComputePreprocess:
    def test_compute_expr_matches_pandas(self, pipeline, bars):
        raw = pipeline.compute(MOMENTUM_EXPR, TICKERS, "2023-01-02", "2024-12-31")

        assert list(raw.columns) == ["date", "ticker", "factor"]
        assert len(raw) == N_DAYS * len(TICKERS)
        for ticker in TICKERS:
            grp = bars.loc[bars["ticker"] == ticker].sort_values("date")
            expected = grp["close"] / grp["close"].rolling(20).mean() - 1
            got = raw.loc[raw["ticker"] == ticker, "factor"].reset_index(drop=True)
            assert got.to_numpy() == pytest.approx(
                expected.to_numpy(), nan_ok=True, rel=1e-12
            )
        # rolling window of 20 → first 19 rows per ticker are NaN warm-up
        assert raw["factor"].isna().sum() == 19 * len(TICKERS)

    def test_compute_rejects_lookahead_and_unsafe_syntax(self, pipeline):
        for bad in (
            "close.shift(-1)",  # attribute access — and a lookahead shape
            "__import__('os')",  # no arbitrary calls
            "unknown_col + 1",  # no unknown columns
            "rolling_mean(close, 0)",  # window must be positive
        ):
            with pytest.raises(ValueError):
                pipeline.compute(bad, TICKERS, "2023-01-02", "2024-12-31")

    def test_compute_rejects_negative_and_fractional_periods(self, pipeline):
        for bad in (
            "delta(close, -1)",  # negative shift reads the next bar
            "pct_change(close, -2)",  # same lookahead through returns
            "delta(close, 1.9)",  # int() would silently truncate to 1
        ):
            with pytest.raises(ValueError, match="positive int"):
                pipeline.compute(bad, TICKERS, "2023-01-02", "2024-12-31")
        # a bool constant never reaches the operator guard: the expression
        # layer rejects it as a non-numeric constant first
        with pytest.raises(ValueError, match="unsupported expression element"):
            pipeline.compute("delta(close, True)", TICKERS, "2023-01-02", "2024-12-31")

    def test_compute_rejects_empty_universe(self, pipeline):
        with pytest.raises(ValueError, match="universe"):
            pipeline.compute(MOMENTUM_EXPR, [], "2023-01-02", "2024-12-31")

    def test_preprocess_zscores_each_date(self, pipeline, bars):
        raw = pipeline.compute(MOMENTUM_EXPR, TICKERS, "2023-01-02", "2024-12-31")

        processed = pipeline.preprocess(raw)

        assert list(processed.columns) == list(raw.columns)
        grouped = processed.dropna(subset=["factor"]).groupby("date")["factor"]
        assert grouped.mean().abs().max() < 1e-12
        assert grouped.std(ddof=1).to_numpy() == pytest.approx(
            np.ones(len(grouped.std(ddof=1))), abs=1e-12
        )

    def test_preprocess_neutralizes_when_columns_present(self, pipeline):
        # 20 names so the per-date OLS actually runs (MIN_NEUTRALIZE_ROWS=10)
        rng = np.random.default_rng(3)
        dates = pd.date_range("2023-01-02", periods=60, freq="B")
        tickers = [f"T{i:02d}" for i in range(20)]
        raw = pd.DataFrame(
            {
                "date": np.tile(dates, len(tickers)),
                "ticker": np.repeat(tickers, len(dates)),
                "factor": rng.normal(size=len(dates) * len(tickers)),
            }
        )
        with_cols = raw.copy()
        with_cols["industry"] = np.where(with_cols["ticker"] < "T10", "tech", "finance")
        with_cols["log_market_cap"] = 22.0 + rng.normal(scale=0.5, size=len(raw))

        plain = pipeline.preprocess(raw)
        neutral = pipeline.preprocess(with_cols)

        # metadata columns are consumed here; the factor frame stays 3-column
        assert list(neutral.columns) == ["date", "ticker", "factor"]
        assert not neutral["factor"].equals(plain["factor"])


# ---------------------------------------------------------------------------
# ac-2 + gate integration: evaluate
# ---------------------------------------------------------------------------


class TestEvaluate:
    def test_known_ic_factor_passes_gate(self, pipeline, bars):
        factor = make_factor_frame(bars, kind="next_return")

        result = pipeline.evaluate(factor)

        assert isinstance(result, EvalResult)
        assert result.summary["rank_ic_mean"] > 0.5
        assert result.gate_passed is True
        assert list(result.ic_series.columns) == PERIOD_COLS
        assert result.ic_series.index.name == "date"

    def test_random_factor_rejected(self, pipeline, bars):
        factor = make_factor_frame(bars, kind="random")

        result = pipeline.evaluate(factor)

        assert result.gate_passed is False

    def test_ic_ir_parity_with_direct_alphalens(self, pipeline, stub, bars):
        """IC/IR through the pipeline must equal a direct alphalens call."""
        import alphalens.performance as al_perf
        import alphalens.utils as al_utils

        processed = make_factor_frame(bars, kind="random")
        result = pipeline.evaluate(processed)

        # Independent path: hand-rolled alphalens inputs, direct API calls.
        wide = processed.pivot(index="date", columns="ticker", values="factor")
        factor = wide.stack()
        factor.index.names = ["date", "asset"]
        dates = pd.to_datetime(processed["date"])
        symbols = sorted(processed["ticker"].astype(str).unique())
        bars_ext = stub.load_bars(
            symbols,
            str(dates.min().date()),
            str((dates.max() + pd.Timedelta(days=45)).date()),
        )
        prices = bars_ext.pivot(index="date", columns="ticker", values="close")
        prices.index = pd.to_datetime(prices.index)
        prices = prices.sort_index().sort_index(axis=1).astype("float64")

        direct_fd = al_utils.get_clean_factor_and_forward_returns(
            factor, prices, quantiles=5, periods=(1, 5, 10, 20)
        )
        direct_ic = al_perf.factor_information_coefficient(direct_fd)

        pd.testing.assert_frame_equal(result.ic_series, direct_ic)
        expected_mean = direct_ic["1D"].mean()
        expected_ir = expected_mean / direct_ic["1D"].std()
        assert result.summary["rank_ic_mean"] == pytest.approx(expected_mean)
        assert result.summary["ir"] == pytest.approx(expected_ir)
        assert result.summary["ic_t_stat"] == pytest.approx(
            expected_ir * math.sqrt(int(direct_ic["1D"].notna().sum()))
        )

    def test_attempt_n_ratchets_per_family(self, pipeline, bars):
        factor = make_factor_frame(bars, kind="random")

        first = pipeline.evaluate(factor, name="probe_alpha")
        second = pipeline.evaluate(factor, name="probe_beta")

        assert first.gate_detail["n_attempts"] == 1
        assert second.gate_detail["n_attempts"] == 2
        log_path = pipeline._base_dir / "factor_eval" / "attempt_log" / "probe.json"
        entries = json.loads(log_path.read_text(encoding="utf-8"))
        assert [e["name"] for e in entries] == ["probe_alpha", "probe_beta"]
        assert factor_family("probe_alpha") == "probe"

    def test_corrupt_attempt_log_raises_instead_of_resetting_n(self, pipeline, bars):
        """A corrupt log must not silently reset N to 1 (which would loosen the
        admission gate the attempt count exists to tighten)."""
        factor = make_factor_frame(bars, kind="random")
        log_path = pipeline._base_dir / "factor_eval" / "attempt_log" / "probe.json"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        for payload in ("{not json", '{"not": "a list"}'):
            log_path.write_text(payload, encoding="utf-8")
            with pytest.raises(RuntimeError, match="attempt log"):
                pipeline.evaluate(factor, name="probe_alpha")


# ---------------------------------------------------------------------------
# ac-4: three-layer persistence
# ---------------------------------------------------------------------------


class TestThreeLayerPersistence:
    def test_register_lands_all_three_layers(self, pipeline, bars):
        factor = make_factor_frame(bars, kind="next_return", noise=5e-4)
        result = pipeline.evaluate(factor, name="knownic_1d")
        assert result.gate_passed is True

        pipeline.register("knownic_1d", "close - 1", result)

        # Layer 1 — metadata.json eval block (plus attempts siblings)
        meta_path = (
            pipeline._base_dir / "factors" / "knownic_1d" / "v1.0" / "metadata.json"
        )
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert {
            "rank_ic_mean",
            "ir",
            "ic_t_stat",
            "quantile_monotonicity",
            "gate_passed",
            "eval_date",
            "report",
        } <= set(meta["eval"])
        assert meta["eval"]["gate_passed"] is True
        assert meta["eval"]["report"] == result.report_path
        assert meta["attempts"] >= 1
        assert meta["attempt_log"][-1]["name"] == "knownic_1d"

        # Layer 2 — factor_eval/ Parquet trio
        eval_dir = pipeline._base_dir / "factor_eval" / "knownic_1d"
        assert (eval_dir / "ic_series.parquet").exists()
        assert (eval_dir / "quantile_returns.parquet").exists()
        assert (eval_dir / "turnover.parquet").exists()
        ic_back = pd.read_parquet(eval_dir / "ic_series.parquet")
        assert list(ic_back.columns) == PERIOD_COLS
        assert ic_back.index.name == "date"
        pd.testing.assert_frame_equal(ic_back, result.ic_series, check_freq=False)
        # turnover column labels are quantile codes cast to str — Parquet
        # column names must be strings on every engine
        turnover_back = pd.read_parquet(eval_dir / "turnover.parquet")
        # labels pinned integral and dtype-independent: "1", not "1.0"
        assert list(turnover_back.columns) == ["1", "2", "3", "4", "5"]
        pd.testing.assert_frame_equal(turnover_back, result.turnover, check_freq=False)

        # Layer 3 — HTML tearsheet
        assert result.report_path is not None
        report = next(pipeline._reports_dir.glob("knownic_1d_*.html"))
        html = report.read_text(encoding="utf-8")
        assert "gate PASS" in html
        assert "<img" in html

        # Factor values landed under the semantic version directory the
        # ecosystem reads (v1.0) and round-trip through load_factor's *default*
        # version — the reader seam DataAPI.get_factor() wraps.
        version_dir = pipeline._base_dir / "factors" / "knownic_1d" / "v1.0"
        value_files = sorted(version_dir.glob("*.parquet"))
        assert value_files
        stored = pd.concat([pd.read_parquet(p) for p in value_files], ignore_index=True)
        assert set(stored.columns) == {"date", "ticker", "knownic_1d"}
        read_back = load_factor(
            "knownic_1d",
            bars["date"].min(),
            bars["date"].max(),
            base_dir=str(pipeline._base_dir / "factors"),
        )
        assert set(read_back.columns) == {"date", "ticker", "knownic_1d"}
        assert len(read_back) == len(stored)

    def test_eval_block_survives_metadata_rewrite(self, pipeline, bars):
        factor = make_factor_frame(bars, kind="next_return", noise=5e-4)
        result = pipeline.evaluate(factor, name="knownic_1d")
        pipeline.register("knownic_1d", "close - 1", result)

        out_dir = pipeline._base_dir / "factors" / "knownic_1d" / "v1.0"
        record = SimpleNamespace(
            name="knownic_1d",
            version="1.0",
            description="",
            category="",
            frequency="daily",
            asset_types=["equity"],
            depends_on=lambda: ("close",),
        )
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2023-01-03"]),
                "ticker": ["000001.SZ"],
                "knownic_1d": [0.0],
            }
        )
        save_factor_append(record, frame, base_dir=str(pipeline._base_dir / "factors"))

        meta = json.loads((out_dir / "metadata.json").read_text(encoding="utf-8"))
        assert "eval" in meta
        assert meta["eval"]["gate_passed"] is True
        family_log = json.loads(
            (
                pipeline._base_dir / "factor_eval" / "attempt_log" / "knownic.json"
            ).read_text(encoding="utf-8")
        )
        assert meta["attempts"] == len(family_log)
        assert meta["attempt_log"] == family_log

    def test_register_persists_raw_values_not_processed(self, pipeline, bars):
        """factor_storage stores compute() output: NaN is suspension/missing
        and is kept, never filled — the processed frame stays for evaluation."""
        raw = pipeline.compute(
            "rolling_mean(close, 20)", TICKERS, "2023-01-02", "2024-12-31"
        ).copy()
        # sparse suspension-shape gaps: one name missing where its peers quote
        sparse = raw.index[raw["ticker"] == "000001.SZ"][100:105]
        raw.loc[sparse, "factor"] = np.nan
        n_raw_nan = int(raw["factor"].isna().sum())
        assert n_raw_nan > 5  # warm-up NaN plus the sparse gaps

        processed = pipeline.preprocess(raw)
        # the cross-section fill covers the sparse gaps (their dates have a
        # median); the all-name warm-up dates have none and stay NaN
        assert int(processed["factor"].isna().sum()) < n_raw_nan
        assert not processed["factor"].equals(raw["factor"])

        result = pipeline.evaluate(processed, name="roll20_1d")
        # re-evaluate the same pipeline raw: register() must still pair the
        # raw frame, not fall back to the processed one
        result = pipeline.evaluate(processed, name="roll20_1d")
        pipeline.register("roll20_1d", "rolling_mean(close, 20)", result)

        stored = load_factor(
            "roll20_1d",
            bars["date"].min(),
            bars["date"].max(),
            base_dir=str(pipeline._base_dir / "factors"),
        )
        assert len(stored) == len(raw)
        expect = raw.rename(columns={"factor": "roll20_1d"}).sort_values(
            ["ticker", "date"]
        )
        got = stored.sort_values(["ticker", "date"])
        assert list(got["ticker"]) == list(expect["ticker"])
        assert int(got["roll20_1d"].isna().sum()) == n_raw_nan  # raw NaN kept
        assert (
            got["roll20_1d"]
            .isna()
            .reset_index(drop=True)
            .equals(expect["roll20_1d"].isna().reset_index(drop=True))
        )
        np.testing.assert_allclose(
            got["roll20_1d"].to_numpy(),
            expect["roll20_1d"].to_numpy(),
            rtol=1e-12,
            equal_nan=True,
        )

    def test_register_after_neutralise_keeps_raw(self, pipeline, bars):
        """preprocess()'s input may carry industry/log_market_cap (its
        documented shape); register() must persist the factor column and drop
        the metadata instead of dying on a strict value-column check."""
        raw = make_factor_frame(bars, kind="random")
        raw.loc[raw.index[:5], "factor"] = np.nan  # sparse suspension gaps
        raw["industry"] = np.where(raw["ticker"] < "600000.SH", "tech", "finance")
        raw["log_market_cap"] = 22.0
        processed = pipeline.preprocess(raw)

        result = pipeline.evaluate(processed, name="neut_1d")
        pipeline.register("neut_1d", "close - 1", result)

        stored = load_factor(
            "neut_1d",
            bars["date"].min(),
            bars["date"].max(),
            base_dir=str(pipeline._base_dir / "factors"),
        )
        assert set(stored.columns) == {"date", "ticker", "neut_1d"}
        assert int(stored["neut_1d"].isna().sum()) == 5  # raw NaN kept

    def test_rejects_path_traversal_in_names(self, pipeline, bars):
        factor = make_factor_frame(bars, kind="next_return", noise=5e-4)
        result = pipeline.evaluate(factor, name="knownic_1d")

        for bad in ("..", "a/b", "a\\b", ""):
            with pytest.raises(ValueError, match="invalid factor name"):
                pipeline.register(bad, "close - 1", result)
        with pytest.raises(ValueError, match="invalid factor name"):
            pipeline.evaluate(factor, name="evil/../../x")
        # the attempt-log path validates its family component directly
        with pytest.raises(ValueError, match="invalid factor family"):
            _load_attempts(pipeline._base_dir, "evil/../../x")
