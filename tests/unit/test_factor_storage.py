"""Tests for research.factor_storage — per-factor-one-table persistence.

Covers every DoD row:

* ``save_factor_append`` concatenates the year file and ``drop_duplicates``
  on ``["date", "ticker"]`` (stored rows win over the incoming batch)
* ``save_factor_full`` overwrites the current version directory — stale year
  files go away, sibling version directories stay; duplicate input keys are
  collapsed to the first row
* ``metadata.json`` carries ``depends_on`` / ``depends_hash`` / ``date_range``
* raw semantics: NaN is kept, nothing is filled

Plus the read path: ``[start, end]`` clipping on ``YYYYMMDD`` strings, the
missing-directory error, and ``load_factor_matrix``'s outer join.

Tests write under ``tmp_path`` only — no ``data_platform/`` in the repo tree.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.factor_interface import Factor
from research.factor_storage import (
    DEFAULT_BASE_DIR,
    METADATA_FIELDS,
    load_factor,
    load_factor_matrix,
    save_factor_append,
    save_factor_full,
    write_metadata,
)


# ---------------------------------------------------------------------------
# Test doubles — real Factor subclasses, not mocks
# ---------------------------------------------------------------------------


class Mom20D(Factor):
    name = "mom_20d"
    description = "20-day momentum"
    category = "time_series"
    frequency = "daily"
    asset_types = ["equity"]
    version = "1.0"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):  # pragma: no cover - storage never calls compute
        raise NotImplementedError


class Vol20D(Factor):
    name = "vol_20d"
    description = "20-day volatility"
    category = "time_series"
    frequency = "daily"
    asset_types = ["equity"]
    version = "1.0"

    def depends_on(self):
        return ["bars_daily", "factor:mom_20d"]

    def compute(self, data):  # pragma: no cover
        raise NotImplementedError


class Mom20DV2(Factor):
    """Same name as :class:`Mom20D`, different version directory."""

    name = "mom_20d"
    description = "20-day momentum (v2 compute)"
    category = "time_series"
    frequency = "daily"
    asset_types = ["equity"]
    version = "2.0"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):  # pragma: no cover
        raise NotImplementedError


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def frame(rows):
    """Build ``DataFrame[date, ticker, {factor}]`` from (date, ticker, value)."""
    return pd.DataFrame(rows, columns=["date", "ticker", "mom_20d"])


def year_path(base: Path, name: str, version: str, year: str) -> Path:
    return base / name / f"v{version}" / f"{year}.parquet"


def read_meta(base: Path, name: str, version: str) -> dict:
    return json.loads(
        (base / name / f"v{version}" / "metadata.json").read_text(encoding="utf-8")
    )


# ---------------------------------------------------------------------------
# save_factor_append
# ---------------------------------------------------------------------------


class TestSaveFactorAppend:
    def test_append_concatenates_year_file_and_drops_duplicate_keys(self, tmp_path):
        """DoD: year file concat + drop_duplicates([date, ticker])."""
        factor = Mom20D()
        first = frame(
            [
                ("20200102", "000001.SZ", 1.0),
                ("20200103", "000001.SZ", 2.0),
            ]
        )
        second = frame(
            [
                ("20200103", "000001.SZ", 99.0),  # duplicate key, stored row wins
                ("20200106", "000002.SZ", 3.0),
            ]
        )

        save_factor_append(factor, first, base_dir=str(tmp_path))
        save_factor_append(factor, second, base_dir=str(tmp_path))

        stored = pd.read_parquet(year_path(tmp_path, "mom_20d", "1.0", "2020"))
        assert len(stored) == 3
        keys = list(zip(stored["date"], stored["ticker"]))
        assert keys == [
            ("20200102", "000001.SZ"),
            ("20200103", "000001.SZ"),
            ("20200106", "000002.SZ"),
        ]
        # drop_duplicates keeps the first occurrence = the already-stored row
        assert stored.loc[stored["date"] == "20200103", "mom_20d"].item() == 2.0

    def test_append_splits_by_year(self, tmp_path):
        factor = Mom20D()
        result = frame(
            [
                ("20191231", "000001.SZ", 1.0),
                ("20200102", "000001.SZ", 2.0),
                ("20210104", "000002.SZ", 3.0),
            ]
        )

        save_factor_append(factor, result, base_dir=str(tmp_path))

        out = tmp_path / "mom_20d" / "v1.0"
        assert sorted(p.name for p in out.glob("*.parquet")) == [
            "2019.parquet",
            "2020.parquet",
            "2021.parquet",
        ]
        assert len(pd.read_parquet(out / "2020.parquet")) == 1

    def test_append_keeps_nan_unfilled(self, tmp_path):
        """Raw values: NaN is suspension/missing and is stored as-is."""
        factor = Mom20D()
        result = frame(
            [
                ("20200102", "000001.SZ", float("nan")),
                ("20200103", "000002.SZ", 1.5),
            ]
        )

        save_factor_append(factor, result, base_dir=str(tmp_path))

        stored = pd.read_parquet(year_path(tmp_path, "mom_20d", "1.0", "2020"))
        assert stored["mom_20d"].isna().tolist() == [True, False]
        assert stored["mom_20d"].dtype == "float64"

    def test_append_does_not_mutate_caller_frame(self, tmp_path):
        factor = Mom20D()
        result = frame([("20200102", "000001.SZ", 1.0)])
        before = result.copy()

        save_factor_append(factor, result, base_dir=str(tmp_path))

        pd.testing.assert_frame_equal(result, before)

    def test_rejects_wrong_columns(self, tmp_path):
        factor = Mom20D()
        bad = pd.DataFrame(
            {"date": ["20200102"], "ticker": ["000001.SZ"], "not_mom": [1.0]}
        )
        with pytest.raises(ValueError, match="columns"):
            save_factor_append(factor, bad, base_dir=str(tmp_path))

    def test_default_base_dir_constant(self):
        assert DEFAULT_BASE_DIR == "data_platform/factors"


# ---------------------------------------------------------------------------
# save_factor_full
# ---------------------------------------------------------------------------


class TestSaveFactorFull:
    def test_full_overwrites_current_version_directory(self, tmp_path):
        """DoD: full overwrites the current version directory."""
        factor = Mom20D()
        wide = frame(
            [
                ("20191231", "000001.SZ", 1.0),
                ("20200102", "000001.SZ", 2.0),
                ("20210104", "000002.SZ", 3.0),
            ]
        )
        narrow = frame(
            [
                ("20200102", "000001.SZ", 20.0),
            ]
        )

        save_factor_full(factor, wide, base_dir=str(tmp_path))
        save_factor_full(factor, narrow, base_dir=str(tmp_path))

        out = tmp_path / "mom_20d" / "v1.0"
        # 2019/2021 stale files are gone — the directory is exactly `narrow`
        assert sorted(p.name for p in out.glob("*.parquet")) == ["2020.parquet"]
        stored = pd.read_parquet(out / "2020.parquet")
        assert len(stored) == 1
        assert stored["mom_20d"].item() == 20.0

    def test_version_directories_coexist_without_overwriting(self, tmp_path):
        """DoD: version directories coexist; one version never clobbers another."""
        v1, v2 = Mom20D(), Mom20DV2()
        save_factor_full(v1, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path))
        save_factor_full(v2, frame([("20200102", "000001.SZ", 2.0)]), str(tmp_path))

        out1 = pd.read_parquet(year_path(tmp_path, "mom_20d", "1.0", "2020"))
        out2 = pd.read_parquet(year_path(tmp_path, "mom_20d", "2.0", "2020"))
        assert out1["mom_20d"].item() == 1.0
        assert out2["mom_20d"].item() == 2.0

        # a later full in v2 leaves v1 alone
        save_factor_full(v2, frame([("20200103", "000001.SZ", 3.0)]), str(tmp_path))
        out1_after = pd.read_parquet(year_path(tmp_path, "mom_20d", "1.0", "2020"))
        assert out1_after["mom_20d"].item() == 1.0

    def test_full_rewrites_values_for_same_key(self, tmp_path):
        """Full is the correction path — unlike append, new values win."""
        factor = Mom20D()
        save_factor_full(factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path))
        save_factor_full(factor, frame([("20200102", "000001.SZ", 9.0)]), str(tmp_path))

        stored = pd.read_parquet(year_path(tmp_path, "mom_20d", "1.0", "2020"))
        assert stored["mom_20d"].item() == 9.0

    def test_full_drops_duplicate_input_keys(self, tmp_path):
        """Duplicate (date, ticker) rows in `result` are not persisted verbatim.

        A buggy compute() returning two rows for one key must not land in the
        year file — load_factor would then hand out ambiguous keys and
        load_factor_matrix a wide table with duplicate index rows.
        """
        factor = Mom20D()
        result = frame(
            [
                ("20200102", "000001.SZ", 1.0),
                ("20200102", "000001.SZ", 99.0),  # duplicate key — first wins
                ("20200103", "000002.SZ", 2.0),
            ]
        )

        save_factor_full(factor, result, base_dir=str(tmp_path))

        stored = pd.read_parquet(year_path(tmp_path, "mom_20d", "1.0", "2020"))
        assert len(stored) == 2
        keys = list(zip(stored["date"], stored["ticker"]))
        assert keys == [("20200102", "000001.SZ"), ("20200103", "000002.SZ")]
        # drop_duplicates keeps the first occurrence of the incoming batch
        assert stored.loc[stored["date"] == "20200102", "mom_20d"].item() == 1.0

    def test_full_with_empty_result_clears_directory(self, tmp_path):
        factor = Mom20D()
        save_factor_full(factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path))
        save_factor_full(factor, frame([]), str(tmp_path))

        out = tmp_path / "mom_20d" / "v1.0"
        assert list(out.glob("*.parquet")) == []
        assert read_meta(tmp_path, "mom_20d", "1.0")["row_count"] == 0


# ---------------------------------------------------------------------------
# metadata.json
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_metadata_has_lineage_and_coverage_fields(self, tmp_path):
        """DoD: metadata.json carries depends_on / depends_hash / date_range."""
        factor = Vol20D()
        result = frame([("20200102", "000001.SZ", 1.0), ("20210304", "000002.SZ", 2.0)])
        result = result.rename(columns={"mom_20d": "vol_20d"})
        save_factor_full(factor, result, str(tmp_path))

        meta = read_meta(tmp_path, "vol_20d", "1.0")
        for field in METADATA_FIELDS:
            assert field in meta, field

        assert meta["factor_name"] == "vol_20d"
        assert meta["version"] == "1.0"
        assert meta["depends_on"] == ["bars_daily", "factor:mom_20d"]
        assert isinstance(meta["depends_hash"], str) and len(meta["depends_hash"]) == 12
        assert meta["date_range"] == ["20200102", "20210304"]
        assert meta["row_count"] == 2
        assert meta["file_count"] == 2
        assert meta["compression"] == "zstd"
        assert meta["category"] == "time_series"
        assert meta["asset_types"] == ["equity"]

    def test_depends_hash_tracks_the_declaration(self, tmp_path):
        mom, vol = Mom20D(), Vol20D()
        save_factor_full(mom, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path))
        vol_result = frame([("20200102", "000001.SZ", 1.0)]).rename(
            columns={"mom_20d": "vol_20d"}
        )
        save_factor_full(vol, vol_result, str(tmp_path))

        mom_meta = read_meta(tmp_path, "mom_20d", "1.0")
        vol_meta = read_meta(tmp_path, "vol_20d", "1.0")
        assert mom_meta["depends_on"] != vol_meta["depends_on"]
        assert mom_meta["depends_hash"] != vol_meta["depends_hash"]

    def test_coverage_describes_directory_not_batch(self, tmp_path):
        """After an append, row_count is the directory's, not the batch's."""
        factor = Mom20D()
        save_factor_append(
            factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path)
        )
        save_factor_append(
            factor, frame([("20200103", "000001.SZ", 2.0)]), str(tmp_path)
        )

        meta = read_meta(tmp_path, "mom_20d", "1.0")
        assert meta["row_count"] == 2
        assert meta["date_range"] == ["20200102", "20200103"]
        assert meta["file_count"] == 1

    def test_created_at_preserved_across_rewrite(self, tmp_path):
        factor = Mom20D()
        save_factor_append(
            factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path)
        )
        created = read_meta(tmp_path, "mom_20d", "1.0")["created_at"]

        save_factor_append(
            factor, frame([("20200103", "000001.SZ", 2.0)]), str(tmp_path)
        )
        meta = read_meta(tmp_path, "mom_20d", "1.0")
        assert meta["created_at"] == created
        assert meta["updated_at"] >= created

    def test_write_metadata_writes_fields_directly(self, tmp_path):
        out_dir = tmp_path / "mom_20d" / "v1.0"
        out_dir.mkdir(parents=True)
        write_metadata(out_dir, Mom20D(), frame([("20200102", "000001.SZ", 1.0)]))

        meta = json.loads((out_dir / "metadata.json").read_text(encoding="utf-8"))
        assert meta["factor_name"] == "mom_20d"
        assert meta["depends_on"] == ["bars_daily"]
        assert len(meta["depends_hash"]) == 12
        assert meta["date_range"] == ["20200102", "20200102"]
        assert meta["row_count"] == 1

    def test_path_components_cannot_escape_base_dir(self, tmp_path):
        """name/version become path segments — separators and .. are refused."""
        with pytest.raises(ValueError, match="factor name"):
            load_factor("../etc", "20200101", "20201231", "1.0", str(tmp_path))
        with pytest.raises(ValueError, match="version"):
            load_factor("mom_20d", "20200101", "20201231", "..", str(tmp_path))


# ---------------------------------------------------------------------------
# load_factor
# ---------------------------------------------------------------------------


class TestLoadFactor:
    def test_clips_to_inclusive_date_window(self, tmp_path):
        factor = Mom20D()
        result = frame(
            [
                ("20191231", "000001.SZ", 1.0),
                ("20200102", "000001.SZ", 2.0),
                ("20200103", "000002.SZ", 3.0),
                ("20210104", "000002.SZ", 4.0),
            ]
        )
        save_factor_full(factor, result, str(tmp_path))

        got = load_factor("mom_20d", "20200102", "20200103", "1.0", str(tmp_path))
        assert got["date"].tolist() == ["20200102", "20200103"]
        assert list(got.columns) == ["date", "ticker", "mom_20d"]

    def test_window_with_no_rows_returns_empty_contract_frame(self, tmp_path):
        factor = Mom20D()
        save_factor_full(factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path))

        got = load_factor("mom_20d", "20300101", "20301231", "1.0", str(tmp_path))
        assert got.empty
        assert list(got.columns) == ["date", "ticker", "mom_20d"]

    def test_missing_version_directory_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="mom_20d"):
            load_factor("mom_20d", "20200101", "20201231", "1.0", str(tmp_path))

    def test_reads_across_years(self, tmp_path):
        factor = Mom20D()
        save_factor_full(
            factor,
            frame(
                [
                    ("20191231", "000001.SZ", 1.0),
                    ("20200102", "000001.SZ", 2.0),
                    ("20210104", "000002.SZ", 3.0),
                ]
            ),
            str(tmp_path),
        )

        got = load_factor("mom_20d", "20191231", "20210104", "1.0", str(tmp_path))
        assert len(got) == 3
        assert got["date"].tolist() == ["20191231", "20200102", "20210104"]

    def test_accepts_datetime_bounds(self, tmp_path):
        import datetime as dt

        factor = Mom20D()
        save_factor_full(factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path))
        got = load_factor(
            "mom_20d", dt.date(2020, 1, 1), dt.date(2020, 1, 31), "1.0", str(tmp_path)
        )
        assert len(got) == 1


# ---------------------------------------------------------------------------
# load_factor_matrix
# ---------------------------------------------------------------------------


class TestLoadFactorMatrix:
    def test_outer_join_wide_table(self, tmp_path):
        mom, vol = Mom20D(), Vol20D()
        save_factor_full(
            mom,
            frame([("20200102", "000001.SZ", 1.0), ("20200103", "000002.SZ", 2.0)]),
            str(tmp_path),
        )
        save_factor_full(
            vol,
            frame(
                [("20200103", "000002.SZ", 3.0), ("20200104", "000003.SZ", 4.0)]
            ).rename(columns={"mom_20d": "vol_20d"}),
            str(tmp_path),
        )

        wide = load_factor_matrix(
            ["mom_20d", "vol_20d"], "20200101", "20200131", "1.0", str(tmp_path)
        )

        assert list(wide.columns) == ["date", "ticker", "mom_20d", "vol_20d"]
        assert len(wide) == 3
        # outer join: keys only one factor has stay NaN — never filled
        assert wide.loc[wide["date"] == "20200102", "vol_20d"].isna().item()
        assert wide.loc[wide["date"] == "20200104", "mom_20d"].isna().item()
        assert wide.loc[wide["date"] == "20200103", "mom_20d"].item() == 2.0
        assert wide.loc[wide["date"] == "20200103", "vol_20d"].item() == 3.0

    def test_empty_names_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="non-empty"):
            load_factor_matrix([], "20200101", "20201231", "1.0", str(tmp_path))

    def test_missing_factor_directory_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_factor_matrix(
                ["mom_20d", "vol_20d"], "20200101", "20201231", "1.0", str(tmp_path)
            )
