"""Strategy-facing loaders for the canonical futures data-lake layout."""

from collections.abc import Iterable

import pandas as pd

from strategies.futures.alstm.config import DATA_ROOT


def load_minute_bars(symbol: str, years: Iterable[int] | None = None) -> pd.DataFrame:
    """Load and concatenate partitioned one-minute futures bars for a symbol."""
    symbol = symbol.upper()
    selected_years = set(years) if years is not None else None
    paths = sorted((DATA_ROOT / "1min").glob(f"year=*/{symbol}_1min.parquet"))
    if selected_years is not None:
        paths = [
            path
            for path in paths
            if int(path.parent.name.split("=", 1)[1]) in selected_years
        ]
    if not paths:
        raise FileNotFoundError(
            f"No 1-minute data found for {symbol} under {DATA_ROOT / '1min'}"
        )

    frame = pd.concat((pd.read_parquet(path) for path in paths), ignore_index=True)
    datetime_col = "datetime" if "datetime" in frame.columns else "timestamp"
    if datetime_col not in frame.columns:
        raise ValueError(f"{symbol} minute data has no datetime/timestamp column")
    frame[datetime_col] = pd.to_datetime(frame[datetime_col], errors="coerce")
    if datetime_col != "datetime":
        frame = frame.rename(columns={datetime_col: "datetime"})
    return (
        frame.loc[frame["datetime"].notna()]
        .sort_values("datetime")
        .reset_index(drop=True)
    )


def l2_month_dir(year: int, month: int):
    """Return the canonical directory containing one month of L2 source files."""
    return DATA_ROOT / "tick_l2" / f"year={year}" / f"month={month:02d}"
