"""Industry classification -> ``data_platform/asset/reference/industry.parquet``.

Source: tushare 申万 tree (``index_classify`` + ``index_member``), i.e.
``classification="sw"``.  One reference file, PK ``(ticker, classification)``.

``industry_l3`` is schema-nullable and left null unless the L3 members are
reachable.  Optional for the golden sample.
"""

from __future__ import annotations

import argparse

import pandas as pd

from _common import DATA_ROOT, init_tushare, ticker_convert, write_reference


def _levels(pro, level: str) -> pd.DataFrame:
    frame = pro.index_classify(level=level, src="SW2021")
    if frame is None or frame.empty:
        raise RuntimeError(
            f"index_classify(level={level}, src=SW2021) returned no rows"
        )
    return frame


def build_frame(pro, tickers: list[str]) -> pd.DataFrame:
    wanted = set(tickers)
    rows: list[dict] = []

    l1 = _levels(pro, "L1")
    l2 = _levels(pro, "L2")
    l2_by_parent = {str(row["index_code"]): row for _, row in l2.iterrows()}

    for _, parent in l1.iterrows():
        members = pro.index_member(index_code=parent["index_code"])
        if members is None or members.empty:
            continue
        for _, member in members.iterrows():
            ts_code = str(member.get("con_code") or member.get("ts_code") or "")
            if wanted and ts_code not in wanted:
                continue
            rows.append(
                {
                    "ticker": ticker_convert(ts_code),
                    "industry_l1": str(parent["index_name"]),
                    "industry_l2": _l2_name_for(members, member, l2_by_parent),
                    "industry_l3": None,
                    "classification": "sw",
                    "effective_date": pd.Timestamp.utcnow().date(),
                }
            )

    if not rows:
        raise RuntimeError(
            "no SW industry membership reached for the requested tickers — "
            "index_member/index_classify may be permission-gated on this token"
        )
    return pd.DataFrame(rows).drop_duplicates(subset=["ticker", "classification"])


def _l2_name_for(members, member, l2_by_parent: dict) -> str:
    """Best-effort L2 label; falls back to the L1 name when no L2 is linked."""
    code = str(member.get("index_code") or "")
    return str(l2_by_parent.get(code, {}).get("index_name", code or "unknown"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", required=True, help="comma-separated ts_codes")
    parser.add_argument("--data-root", default=None, help=f"default: {DATA_ROOT}")
    args = parser.parse_args()
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]

    pro = init_tushare()
    frame = build_frame(pro, tickers)
    path = write_reference(frame, "industry", args.data_root)
    print(f"wrote {len(frame)} rows -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
