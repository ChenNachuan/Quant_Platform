"""Portfolio-layer golden-sample baseline (QUANT-30 S8-T1).

Equal-weight top-5 ETF momentum with month-end rebalance.  Weights use the
shift(1) clock — decided at a close, applied to returns strictly after that
close.  Weights drift between rebalances; each month-end resets to 1/5.

Step-2 reference implementation — compared against the hand-calc baseline
(``golden_expected_portfolio.json``) at < 0.01 % relative.
"""

import numpy as np
import pandas as pd

SAMPLE_DAYS = 57
TRADING_DAYS = 252
UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "511010.SH", "159915.SZ"]
REBALANCE_DATES = ["2025-01-27", "2025-02-28", "2025-03-31"]


def portfolio_baseline(prices: pd.DataFrame) -> dict:
    """Compute portfolio metrics and weight schedule from the fixture.

    Returns a dict with:
        total_return, sharpe, mdd : float
        weights_used : (SAMPLE_DAYS, 5) — weights for each day's return
        portfolio_returns : (SAMPLE_DAYS,) — daily r_p
        nav : (SAMPLE_DAYS + 1,) — NAV_0 … NAV_57
    """
    sample = prices[prices["phase"] == "sample"].reset_index(drop=True)
    warmup_last = prices[prices["phase"] == "warmup"].iloc[-1]
    closes = sample[UNIVERSE].to_numpy(dtype=float)
    base = warmup_last[UNIVERSE].to_numpy(dtype=float)
    prev = np.concatenate([base.reshape(1, -1), closes[:-1]], axis=0)
    r = closes / prev - 1.0

    sample_dates = sample["date"].tolist()
    rebalance_si = {sample_dates.index(d) for d in REBALANCE_DATES}

    w = np.full(len(UNIVERSE), 1.0 / len(UNIVERSE))
    nav = 1.0
    nav_series = [1.0]
    r_p = np.zeros(SAMPLE_DAYS)
    weights_used = np.zeros((SAMPLE_DAYS, len(UNIVERSE)))

    for t in range(SAMPLE_DAYS):
        weights_used[t] = w
        r_p[t] = float(np.dot(w, r[t]))
        nav *= 1.0 + r_p[t]
        nav_series.append(nav)
        # Drift: w_i,t = w_i,t-1 * (1 + r_i,t) / (1 + r_p,t)
        w = w * (1.0 + r[t]) / (1.0 + r_p[t])
        # Rebalance at close resets weights for the next period
        if t in rebalance_si:
            w = np.full(len(UNIVERSE), 1.0 / len(UNIVERSE))

    nav_arr = np.array(nav_series)
    peak = np.maximum.accumulate(nav_arr)
    return {
        "total_return": float(np.prod(1.0 + r_p) - 1.0),
        "sharpe": float(r_p.mean() / r_p.std(ddof=1) * np.sqrt(TRADING_DAYS)),
        "mdd": float((nav_arr / peak - 1.0).min()),
        "weights_used": weights_used,
        "portfolio_returns": r_p,
        "nav": nav_arr,
    }
