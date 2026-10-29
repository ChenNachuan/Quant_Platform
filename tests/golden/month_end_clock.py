"""Month-end rebalance clock for the five-year golden sample (QUANT-32 S8-T3).

Decision dates = formation close (last warmup day) + last trading day of each
calendar month.  Matches S8-T1's REBALANCE_DATES rule exactly — the max
trading date per calendar month, never ``_select_rebalance_dates("monthly")``
(which keeps first trading days and moves every fill).
"""

import pandas as pd


def month_end_decision_dates(prices: pd.DataFrame) -> list[str]:
    """Return formation close + month-end trading dates from the fixture.

    The fixture's ``phase`` column marks warmup/sample.  The formation close
    is the last warmup day; month-end dates are the max trading date per
    calendar month in the sample period.
    """
    warmup = prices[prices["phase"] == "warmup"]
    sample = prices[prices["phase"] == "sample"]

    if warmup.empty or sample.empty:
        raise ValueError("fixture must contain both warmup and sample rows")

    formation = str(warmup["date"].iloc[-1]).split()[0]

    sample_ts = pd.to_datetime(sample["date"])
    month_ends = (
        sample_ts.groupby([sample_ts.dt.year, sample_ts.dt.month])
        .max()
        .dt.date  # convert to date
    )
    month_end_strs = [d.isoformat() for d in month_ends]

    return [formation] + month_end_strs
