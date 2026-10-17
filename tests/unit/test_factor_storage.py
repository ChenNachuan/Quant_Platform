"""Tests for research.factor_storage — per-factor-one-table persistence.

Covers every DoD row:

* ``save_factor_append`` concatenates the year file and ``drop_duplicates``
  on ``["date", "ticker"]`` (stored rows win over the incoming batch)
* ``save_factor_full`` overwrites the current version directory — stale year
  files go away, sibling version directories stay; duplicate input keys are
  collapsed to the first row
* ``metadata.json`` carries ``depends_on`` / ``depends_hash`` / ``date_range``
* raw semantics: NaN is kept, nothing is filled
* a rejected write is side-effect free: no empty version directory, so
  ``load_factor`` reports "not found" rather than a silent empty frame

Plus the date contract (human ruling 2026-10-18, option A) and crash safety:

* ``date`` is Parquet ``DATE`` (``date32``), typed by
  ``data.storage_schema.factor_schema`` — the single source of truth, not a
  private column copy.  ``2024/01/05`` parses to its calendar date; ``NaT``
  is refused by the column type rather than persisted as the string ``"NaT"``
* year partitions come from the parsed date's year (``dt.year``), never from a
  string prefix — ``str[:4]`` is exactly where a bad value used to hide
* every year file is written tmp + ``os.replace``, and ``save_factor_full``
  unlinks stale years only after all replacements exist, so a crash mid-loop
  cannot leave truncated history that ``load_factor`` reports as complete

Plus the read path: ``[start, end]`` clipping, the missing-directory error,
and ``load_factor_matrix``'s outer join.

Tests write under ``tmp_path`` only — no ``data_platform/`` in the repo tree.
"""

from __future__ import annotations

import datetime as dt
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


def on_disk_dates(base: Path, name: str, version: str, year: str) -> list:
    """Date column as persisted — Parquet DATE reads back as ``datetime.date``."""
    return pd.read_parquet(year_path(base, name, version, year))["date"].tolist()


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
            (dt.date(2020, 1, 2), "000001.SZ"),
            (dt.date(2020, 1, 3), "000001.SZ"),
            (dt.date(2020, 1, 6), "000002.SZ"),
        ]
        # drop_duplicates keeps the first occurrence = the already-stored row
        assert (
            stored.loc[stored["date"] == dt.date(2020, 1, 3), "mom_20d"].item() == 2.0
        )

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
        assert keys == [
            (dt.date(2020, 1, 2), "000001.SZ"),
            (dt.date(2020, 1, 3), "000002.SZ"),
        ]
        # drop_duplicates keeps the first occurrence of the incoming batch
        assert (
            stored.loc[stored["date"] == dt.date(2020, 1, 2), "mom_20d"].item() == 1.0
        )

    def test_full_with_empty_result_clears_directory(self, tmp_path):
        factor = Mom20D()
        save_factor_full(factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path))
        save_factor_full(factor, frame([]), str(tmp_path))

        out = tmp_path / "mom_20d" / "v1.0"
        assert list(out.glob("*.parquet")) == []
        assert read_meta(tmp_path, "mom_20d", "1.0")["row_count"] == 0


# ---------------------------------------------------------------------------
# write-path ordering — validate before touching disk
# ---------------------------------------------------------------------------


class TestRejectedWriteLeavesNoDirectory:
    @pytest.mark.parametrize("save", [save_factor_append, save_factor_full])
    def test_rejected_write_on_fresh_version_leaves_no_dir(self, tmp_path, save):
        """A rejected write must leave no empty version directory behind.

        ``mkdir`` before ``_normalise`` created the directory first and only
        then discovered the frame was invalid, leaving a bare version dir on
        disk.  ``load_factor`` then returned an empty frame instead of raising
        FileNotFoundError — silent empty data, the expensive failure mode.
        """
        factor = Mom20D()
        bad = pd.DataFrame(
            {"date": ["20200102"], "ticker": ["000001.SZ"], "not_mom": [1.0]}
        )

        with pytest.raises(ValueError, match="columns"):
            save(factor, bad, base_dir=str(tmp_path))

        assert not (tmp_path / "mom_20d" / "v1.0").exists()
        with pytest.raises(FileNotFoundError):
            load_factor("mom_20d", "20200101", "20201231", base_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# date contract — Parquet DATE, typed by data.storage_schema.factor_schema
# ---------------------------------------------------------------------------


class TestDateContract:
    def test_on_disk_date_column_is_parquet_date(self, tmp_path):
        """Option A: `date` is Parquet DATE (date32), not VARCHAR YYYYMMDD.

        ``data/storage_schema.py`` types every date column through
        ``_date_column() -> pa.Date`` and registers this very file path as
        ``PARTITION_PATHS["factor"]``.  One file path, one type.
        """
        import pyarrow.parquet as pq

        factor = Mom20D()
        save_factor_full(factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path))

        schema = pq.read_schema(year_path(tmp_path, "mom_20d", "1.0", "2020"))
        assert str(schema.field("date").type) == "date32[day]"
        # and it reads back as datetime.date, the in-memory DATE convention
        assert on_disk_dates(tmp_path, "mom_20d", "1.0", "2020") == [
            dt.date(2020, 1, 2)
        ]

    def test_slash_date_normalises_to_calendar_date(self, tmp_path):
        """Under DATE, `2024/01/05` is legal input and means 2024-01-05.

        The VARCHAR contract forbade it and `_as_date_str` persisted it
        verbatim — the row then sat under a date no reader could match.
        """
        factor = Mom20D()
        result = pd.DataFrame(
            {"date": ["2024/01/05"], "ticker": ["000001.SZ"], "mom_20d": [1.0]}
        )

        save_factor_full(factor, result, base_dir=str(tmp_path))

        assert on_disk_dates(tmp_path, "mom_20d", "1.0", "2024") == [
            dt.date(2024, 1, 5)
        ]

    @pytest.mark.parametrize("save", [save_factor_append, save_factor_full])
    def test_nat_date_is_rejected_not_persisted(self, tmp_path, save):
        """NaT is refused by the column type, never written as the string "NaT".

        The VARCHAR path stringified NaT to `"NaT"`, derived a `NaT.parquet`
        year file from `str[:4]`, and hid the row from every later read.
        """
        factor = Mom20D()
        result = pd.DataFrame(
            {"date": [pd.NaT], "ticker": ["000001.SZ"], "mom_20d": [1.0]}
        )

        with pytest.raises(Exception):
            save(factor, result, base_dir=str(tmp_path))

        out = tmp_path / "mom_20d" / "v1.0"
        assert not out.exists() or not list(out.glob("*.parquet"))

    def test_unparseable_date_is_rejected(self, tmp_path):
        """A date the parser cannot read is an error, not a silent bad row."""
        factor = Mom20D()
        result = pd.DataFrame(
            {"date": ["not-a-date"], "ticker": ["000001.SZ"], "mom_20d": [1.0]}
        )

        with pytest.raises(Exception):
            save_factor_full(factor, result, base_dir=str(tmp_path))

        assert not (tmp_path / "mom_20d" / "v1.0").exists() or not list(
            (tmp_path / "mom_20d" / "v1.0").glob("*.parquet")
        )


# ---------------------------------------------------------------------------
# year partitioning — dt.year, not date.str[:4]
# ---------------------------------------------------------------------------


class TestYearPartition:
    def test_year_partition_derives_from_parsed_date(self, tmp_path):
        """The year filename comes from the date's year, not a string prefix.

        ``"31/12/2024"`` is 2024-12-31.  ``str[:4]`` of the raw value is
        ``"31/1"`` — the row vanished into a bogus year file that no read
        would ever open.  ``dt.year`` gets it right.
        """
        factor = Mom20D()
        result = pd.DataFrame(
            {"date": ["31/12/2024"], "ticker": ["000001.SZ"], "mom_20d": [1.0]}
        )

        save_factor_full(factor, result, base_dir=str(tmp_path))

        out = tmp_path / "mom_20d" / "v1.0"
        assert sorted(p.name for p in out.glob("*.parquet")) == ["2024.parquet"]
        assert on_disk_dates(tmp_path, "mom_20d", "1.0", "2024") == [
            dt.date(2024, 12, 31)
        ]

    def test_year_split_for_mixed_representations(self, tmp_path):
        """Date objects, ISO strings and slash strings all split on dt.year."""
        factor = Mom20D()
        result = pd.DataFrame(
            {
                "date": [dt.date(2019, 12, 31), "2020/06/15", "2021-03-01"],
                "ticker": ["000001.SZ", "000002.SZ", "000003.SZ"],
                "mom_20d": [1.0, 2.0, 3.0],
            }
        )

        save_factor_full(factor, result, base_dir=str(tmp_path))

        out = tmp_path / "mom_20d" / "v1.0"
        assert sorted(p.name for p in out.glob("*.parquet")) == [
            "2019.parquet",
            "2020.parquet",
            "2021.parquet",
        ]

    def test_year_split_for_mixed_representations_append(self, tmp_path):
        """Same derivation on the append path."""
        factor = Mom20D()
        result = pd.DataFrame(
            {
                "date": ["31/12/2024", dt.date(2025, 1, 1)],
                "ticker": ["000001.SZ", "000002.SZ"],
                "mom_20d": [1.0, 2.0],
            }
        )

        save_factor_append(factor, result, base_dir=str(tmp_path))

        out = tmp_path / "mom_20d" / "v1.0"
        assert sorted(p.name for p in out.glob("*.parquet")) == [
            "2024.parquet",
            "2025.parquet",
        ]


# ---------------------------------------------------------------------------
# crash safety — tmp + os.replace per year, stale unlinked only after
# ---------------------------------------------------------------------------


def _failing_after(n, real, boom=OSError("disk full")):
    """Return a write_parquet_atomic stand-in that fails after *n* successes."""
    calls = {"n": 0}

    def fake(df, path, **kwargs):
        calls["n"] += 1
        if calls["n"] > n:
            raise boom
        return real(df, path, **kwargs)

    return fake, calls


class TestCrashSafeWrites:
    def test_full_never_unlinks_years_before_replacements_exist(
        self, tmp_path, monkeypatch
    ):
        """A mid-loop failure must leave the previous years on disk intact.

        ``save_factor_full`` used to unlink every year file first and only
        then write.  A crash / disk-full / kill mid-loop left a subset of
        years while ``metadata.json`` still described the old coverage —
        ``load_factor`` then returned truncated history as complete.
        """
        import research.factor_storage as fs

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
        out = tmp_path / "mom_20d" / "v1.0"
        before = {p.name: p.read_bytes() for p in out.glob("*.parquet")}
        assert sorted(before) == ["2019.parquet", "2020.parquet", "2021.parquet"]

        fake, calls = _failing_after(
            1, fs.write_parquet_atomic
        )  # 1st year lands, 2nd blows up
        monkeypatch.setattr(fs, "write_parquet_atomic", fake)

        with pytest.raises(OSError, match="disk full"):
            save_factor_full(
                factor,
                frame(
                    [
                        ("20191231", "000001.SZ", 10.0),
                        ("20200102", "000001.SZ", 20.0),
                        ("20210104", "000002.SZ", 30.0),
                    ]
                ),
                str(tmp_path),
            )
        assert calls["n"] == 2  # second call is the one that raised

        after = {p.name: p.read_bytes() for p in out.glob("*.parquet")}
        # the years that were never reached are still the old ones
        assert after["2021.parquet"] == before["2021.parquet"]
        assert after["2020.parquet"] == before["2020.parquet"]
        # and a load still sees the full pre-failure history
        got = load_factor("mom_20d", "20190101", "20211231", base_dir=str(tmp_path))
        assert len(got) == 3

    def test_full_removes_stale_years_only_after_writes_succeed(self, tmp_path):
        """Years absent from the new result go away — but only on success."""
        factor = Mom20D()
        save_factor_full(
            factor,
            frame(
                [
                    ("20191231", "000001.SZ", 1.0),
                    ("20200102", "000001.SZ", 2.0),
                ]
            ),
            str(tmp_path),
        )
        save_factor_full(
            factor, frame([("20200102", "000001.SZ", 20.0)]), str(tmp_path)
        )

        out = tmp_path / "mom_20d" / "v1.0"
        assert sorted(p.name for p in out.glob("*.parquet")) == ["2020.parquet"]

    def test_failed_write_leaves_no_tmp_and_no_partial_target(
        self, tmp_path, monkeypatch
    ):
        """A failed year write must not leave .tmp or a half-written target."""
        import research.factor_storage as fs

        factor = Mom20D()
        fake, calls = _failing_after(0, fs.write_parquet_atomic)  # first write blows up
        monkeypatch.setattr(fs, "write_parquet_atomic", fake)

        with pytest.raises(OSError, match="disk full"):
            save_factor_full(
                factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path)
            )

        out = tmp_path / "mom_20d" / "v1.0"
        assert not list(out.glob("*.tmp"))
        assert not list(out.glob("*.parquet"))

    def test_append_write_is_atomic(self, tmp_path, monkeypatch):
        """A failed append write leaves the previously stored year intact."""
        import research.factor_storage as fs

        factor = Mom20D()
        save_factor_append(
            factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path)
        )
        before = year_path(tmp_path, "mom_20d", "1.0", "2020").read_bytes()

        fake, calls = _failing_after(0, fs.write_parquet_atomic)
        monkeypatch.setattr(fs, "write_parquet_atomic", fake)
        with pytest.raises(OSError, match="disk full"):
            save_factor_append(
                factor, frame([("20200103", "000001.SZ", 2.0)]), str(tmp_path)
            )

        after = year_path(tmp_path, "mom_20d", "1.0", "2020")
        assert after.read_bytes() == before
        assert not list(after.parent.glob("*.tmp"))

    def test_append_uses_tmp_then_replace(self, tmp_path, monkeypatch):
        """The year file is created by a rename, never by a direct write."""
        import research.factor_storage as fs

        seen = {}
        real = fs.write_parquet_atomic

        def spy(df, path, **kwargs):
            seen["target"] = Path(path)
            seen["existed_before"] = Path(path).exists()
            return real(df, path, **kwargs)

        monkeypatch.setattr(fs, "write_parquet_atomic", spy)
        factor = Mom20D()
        save_factor_append(
            factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path)
        )
        assert seen["existed_before"] is False
        assert not list(seen["target"].parent.glob("*.tmp"))


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
        # date_range is ISO — the DATE column's natural serialisation
        assert meta["date_range"] == ["2020-01-02", "2021-03-04"]
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
        assert meta["date_range"] == ["2020-01-02", "2020-01-03"]
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
        assert meta["date_range"] == ["2020-01-02", "2020-01-02"]
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
        assert got["date"].tolist() == [dt.date(2020, 1, 2), dt.date(2020, 1, 3)]
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
        assert got["date"].tolist() == [
            dt.date(2019, 12, 31),
            dt.date(2020, 1, 2),
            dt.date(2021, 1, 4),
        ]

    def test_accepts_datetime_bounds(self, tmp_path):
        factor = Mom20D()
        save_factor_full(factor, frame([("20200102", "000001.SZ", 1.0)]), str(tmp_path))
        got = load_factor(
            "mom_20d", dt.date(2020, 1, 1), dt.date(2020, 1, 31), "1.0", str(tmp_path)
        )
        assert len(got) == 1

    def test_accepts_slash_and_iso_bounds(self, tmp_path):
        """Bounds in any parseable form clip the same window."""
        factor = Mom20D()
        save_factor_full(
            factor,
            frame([("20200102", "000001.SZ", 1.0), ("20200103", "000001.SZ", 2.0)]),
            str(tmp_path),
        )
        for start, end in [("2020/01/02", "2020/01/03"), ("2020-01-02", "2020-01-03")]:
            got = load_factor("mom_20d", start, end, "1.0", str(tmp_path))
            assert len(got) == 2, (start, end)


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
                [("20200103", "000002.SZ", 20.0), ("20200104", "000003.SZ", 30.0)]
            ).rename(columns={"mom_20d": "vol_20d"}),
            str(tmp_path),
        )

        wide = load_factor_matrix(
            ["mom_20d", "vol_20d"], "20200101", "20201231", "1.0", str(tmp_path)
        )
        assert list(wide.columns) == ["date", "ticker", "mom_20d", "vol_20d"]
        assert len(wide) == 3  # outer join on (date, ticker)
        # each factor is missing exactly the key the other one owns
        assert wide["mom_20d"].isna().sum() == 1
        assert wide["vol_20d"].isna().sum() == 1
        assert wide["date"].tolist() == [
            dt.date(2020, 1, 2),
            dt.date(2020, 1, 3),
            dt.date(2020, 1, 4),
        ]

    def test_empty_names_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="non-empty"):
            load_factor_matrix([], "20200101", "20201231", "1.0", str(tmp_path))

    def test_missing_factor_directory_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_factor_matrix(
                ["mom_20d"], "20200101", "20201231", "1.0", str(tmp_path)
            )
