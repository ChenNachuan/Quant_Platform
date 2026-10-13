"""Per-ticker golden-sample baseline metrics (QUANT-11 S2-T2).

Computes total_return / sharpe / mdd per ticker from the price fixture.
No portfolio layer — Option A (2026-10-14).  Scope is per-ticker arithmetic only.
"""

import numpy as np
import pandas as pd

SAMPLE_DAYS = 57
TRADING_DAYS = 252


def golden_baseline(prices: pd.DataFrame) -> dict[str, dict[str, float]]:
    sample = prices.loc[prices["phase"] == "sample"]
    warmup = prices.loc[prices["phase"] == "warmup"]
    tickers = [c for c in prices.columns if c not in ("date", "phase")]
    out: dict[str, dict[str, float]] = {"total_return": {}, "sharpe": {}, "mdd": {}}
    for t in tickers:
        closes = sample[t].to_numpy(dtype=float)
        base = float(warmup[t].iloc[-1])
        if base <= 0 or np.any(closes <= 0):
            raise ValueError(f"non-positive close for {t}")
        prev = np.concatenate([[base], closes[:-1]])
        r = closes / prev - 1.0
        if len(r) != SAMPLE_DAYS or np.any(np.isnan(r)):
            raise ValueError(f"bad return series for {t}: n={len(r)}")
        nav = np.concatenate([[1.0], np.cumprod(1.0 + r)])
        if np.any(nav <= 0):
            raise ValueError(f"non-positive NAV for {t}")
        peak = np.maximum.accumulate(nav)
        sd = r.std(ddof=1)
        if sd == 0:
            raise ValueError(f"zero std for {t}")
        out["total_return"][t] = float(np.prod(1.0 + r) - 1.0)
        out["sharpe"][t] = float(r.mean() / sd * np.sqrt(TRADING_DAYS))
        out["mdd"][t] = float((nav / peak - 1.0).min())
    return out
