"""Tests for data.storage_ops — atomic write, locking, lifecycle, capacity.

Covers the QUANT-13 DoD rows that carry risk:

* atomic-write failure injection — no half-written file, no stray ``.tmp``
* ``FileLock`` concurrency — ``ConcurrentWriteError``
* ``cleanup_expired(dry_run=True)`` deletes nothing (file count unchanged)
* ``cleanup_expired(dry_run=False)`` deletes only what the policy says
* schema validation wired — invalid DataFrame is refused and not written
* missing-float normalization — ``None`` no longer poisons a float column to
  ``object`` dtype

``cleanup_*`` is deletion code: every delete branch has its own test.
Price literals use ``10.0`` not ``10`` (T1 Review note for T2).
"""

from __future__ import annotations

import os
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pandera.pandas as pa
import pytest

from data.storage_ops import (
    PARQUET_WRITE_CONFIG,
    ConcurrentWriteError,
    DiskFullError,
    FileLock,
    check_capacity,
    check_daily_growth,
    cleanup_expired,
    cleanup_stale_locks,
    cleanup_stale_tmp,
    load_lifecycle,
    normalize_missing_floats,
    parse_date_from_filename,
    write_parquet_atomic,
)

D = date(2024, 1, 15)


# ---------------------------------------------------------------------------
# Fixtures — one valid bars_daily row, prices as 10.0-style floats
# ---------------------------------------------------------------------------
def _bars_daily_row(**overrides) -> dict:
    row = {
        "ticker": "600519.SH",
        "trade_date": D,
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 1000,
        "amount": 10500.0,
        "adjfactor": 1.0,
        "turnover": 0.5,
        "suspend": False,
        "is_limit_up": False,
        "is_limit_down": False,
        "quality_flag": 0,
    }
    row.update(overrides)
    return row


def _bars_daily_df(**overrides) -> pd.DataFrame:
    return pd.DataFrame([_bars_daily_row(**overrides)])


def _factor_df(value_column: str = "value", value=0.05) -> pd.DataFrame:
    return pd.DataFrame(
        [{"ticker": "600519.SH", "date": D, value_column: value}]
    )


def _data_files(root: Path) -> list[Path]:
    """Files under *root* that are not lock residue.

    The ``.lock`` file is a permanent inode — the mutex is the flock on it —
    so a failed write legitimately leaves it behind.  The atomicity contract
    is about target and ``.tmp``, not the lock file.
    """
    return [p for p in root.rglob("*") if p.is_file() and not p.name.endswith(".lock")]


# ---------------------------------------------------------------------------
# write_parquet_atomic — success path
# ---------------------------------------------------------------------------
class TestWriteParquetAtomic:
    def test_writes_file_and_leaves_no_tmp(self, tmp_path):
        target = tmp_path / "2024-01-15.parquet"
        write_parquet_atomic(_bars_daily_df(), target, table="bars_daily")
        assert target.exists()
        assert not (tmp_path / "2024-01-15.parquet.tmp").exists()
        assert not list(tmp_path.glob("*.tmp"))
        result = pd.read_parquet(target)
        assert len(result) == 1
        assert result["close"].iloc[0] == 10.5

    def test_creates_parent_directories(self, tmp_path):
        target = tmp_path / "asset" / "bars_daily" / "2024-01-15.parquet"
        write_parquet_atomic(_bars_daily_df(), target, table="bars_daily")
        assert target.exists()

    def test_overwrites_existing_target(self, tmp_path):
        target = tmp_path / "2024-01-15.parquet"
        write_parquet_atomic(_bars_daily_df(close=10.5), target, table="bars_daily")
        write_parquet_atomic(_bars_daily_df(close=11.5), target, table="bars_daily")
        assert pd.read_parquet(target)["close"].iloc[0] == 11.5

    def test_factor_named_value_column(self, tmp_path):
        target = tmp_path / "2024.parquet"
        write_parquet_atomic(
            _factor_df("momentum_20d", 0.12),
            target,
            table="factor",
            value_column="momentum_20d",
        )
        result = pd.read_parquet(target)
        assert "momentum_20d" in result.columns

    def test_write_config_applied(self, tmp_path):
        assert PARQUET_WRITE_CONFIG["compression"] == "zstd"
        assert PARQUET_WRITE_CONFIG["row_group_size"] == 65536
        assert PARQUET_WRITE_CONFIG["index"] is False


# ---------------------------------------------------------------------------
# Atomic-write failure injection — no half-written file, ever
# ---------------------------------------------------------------------------
class TestAtomicWriteFailureInjection:
    def test_failure_mid_tmp_write_leaves_nothing(self, tmp_path, monkeypatch):
        """Exception while writing the tmp: no target file, no .tmp."""
        target = tmp_path / "2024-01-15.parquet"

        def boom(self, path, *args, **kwargs):
            # simulate a crash part-way through writing the tmp
            Path(path).write_bytes(b"half-written parquet bytes")
            raise OSError("disk write failed mid-stream")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

        with pytest.raises(OSError, match="mid-stream"):
            write_parquet_atomic(_bars_daily_df(), target, table="bars_daily")

        assert not target.exists(), "target must not exist after a failed write"
        assert not list(tmp_path.glob("*.tmp")), "no .tmp may survive a failed write"
        assert _data_files(tmp_path) == [], "no half-written data file may remain"

    def test_failure_at_rename_leaves_nothing(self, tmp_path, monkeypatch):
        """Exception at the rename step: no target file, no .tmp."""
        target = tmp_path / "2024-01-15.parquet"

        monkeypatch.setattr(
            os, "replace", lambda *a, **k: (_ for _ in ()).throw(OSError("replace died"))
        )

        with pytest.raises(OSError, match="replace died"):
            write_parquet_atomic(_bars_daily_df(), target, table="bars_daily")

        assert not target.exists()
        assert not list(tmp_path.glob("*.tmp"))
        assert _data_files(tmp_path) == [], "no half-written data file may remain"

    def test_failure_before_write_creates_nothing(self, tmp_path):
        """A validation failure means the write never starts."""
        target = tmp_path / "2024-01-15.parquet"
        bad = _bars_daily_df().drop(columns=["adjfactor"])
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            write_parquet_atomic(bad, target, table="bars_daily")
        assert not target.exists()
        assert list(tmp_path.iterdir()) == []

    def test_failed_overwrite_leaves_previous_target_intact(self, tmp_path, monkeypatch):
        """§1: the target is either absent or complete — never half-written.

        A failed *overwrite* must not damage the complete file already at
        the target path.  This is the other half of the atomicity contract:
        the tests above start from an empty tmp_path and only pin the
        "absent" half (Test finding, QUANT-13).
        """
        target = tmp_path / "2024-01-15.parquet"
        write_parquet_atomic(_bars_daily_df(close=11.5), target, table="bars_daily")
        original_bytes = target.read_bytes()

        def boom(self, path, *args, **kwargs):
            Path(path).write_bytes(b"half-written parquet bytes")
            raise OSError("disk write failed mid-stream")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

        with pytest.raises(OSError, match="mid-stream"):
            write_parquet_atomic(_bars_daily_df(close=99.0), target, table="bars_daily")

        assert target.exists(), "the pre-existing complete target must survive"
        assert target.read_bytes() == original_bytes, "target must be byte-identical"
        assert pd.read_parquet(target)["close"].iloc[0] == 11.5
        assert not list(tmp_path.glob("*.tmp")), "no .tmp may survive"

    def test_disk_full_check_refuses_write(self, tmp_path, monkeypatch):
        from data import storage_ops

        monkeypatch.setattr(storage_ops, "check_capacity", lambda *_a, **_k: "CRITICAL")
        target = tmp_path / "2024-01-15.parquet"
        with pytest.raises(DiskFullError):
            write_parquet_atomic(_bars_daily_df(), target, table="bars_daily")
        assert not target.exists()
        assert list(tmp_path.iterdir()) == []


# ---------------------------------------------------------------------------
# Schema validation is wired to every write path
# ---------------------------------------------------------------------------
class TestSchemaValidationWired:
    def test_invalid_dataframe_rejected_and_not_written(self, tmp_path):
        target = tmp_path / "2024-01-15.parquet"
        bad = _bars_daily_df(open=-1.0)  # OHLC must be > 0
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            write_parquet_atomic(bad, target, table="bars_daily")
        assert not target.exists()

    def test_extra_column_rejected_and_not_written(self, tmp_path):
        target = tmp_path / "2024-01-15.parquet"
        bad = _bars_daily_df()
        bad["surprise"] = 1
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            write_parquet_atomic(bad, target, table="bars_daily")
        assert not target.exists()

    def test_float_dtype_in_int_column_rejected(self, tmp_path):
        """Schema-level coerce=False: an int column must not see a float dtype."""
        target = tmp_path / "2024-01-15.parquet"
        bad = _bars_daily_df(volume=10.0)  # 10.0 not 10 — still float dtype
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            write_parquet_atomic(bad, target, table="bars_daily")
        assert not target.exists()

    def test_unknown_table_rejected(self, tmp_path):
        target = tmp_path / "x.parquet"
        with pytest.raises(KeyError):
            write_parquet_atomic(_bars_daily_df(), target, table="no_such_table")
        assert not target.exists()

    def test_factor_value_column_collision_rejected(self, tmp_path):
        """factor_schema('date') silently overwrites the date column — refuse it."""
        target = tmp_path / "2024.parquet"
        with pytest.raises(ValueError, match="collides"):
            write_parquet_atomic(
                _factor_df("date"), target, table="factor", value_column="date"
            )
        assert not target.exists()

    def test_factor_value_column_ticker_collision_rejected(self, tmp_path):
        target = tmp_path / "2024.parquet"
        with pytest.raises(ValueError, match="collides"):
            write_parquet_atomic(
                _factor_df("ticker"), target, table="factor", value_column="ticker"
            )
        assert not target.exists()


# ---------------------------------------------------------------------------
# Missing-float normalization (T1 Review note for T2)
# ---------------------------------------------------------------------------
class TestMissingFloatNormalization:
    def test_all_none_float_column_becomes_nan_not_object(self, tmp_path):
        """A column of all-None must not become object dtype and fail the type check."""
        df = _bars_daily_df(turnover=None)
        assert df["turnover"].dtype == object, "precondition: pandas made it object"

        target = tmp_path / "2024-01-15.parquet"
        write_parquet_atomic(df, target, table="bars_daily")
        result = pd.read_parquet(target)
        assert result["turnover"].dtype == "float64"
        assert result["turnover"].isna().all()

    def test_normalize_missing_floats_helper(self):
        df = _bars_daily_df(turnover=None)
        out = normalize_missing_floats(df, "bars_daily")
        assert out["turnover"].dtype == "float64"
        assert out["turnover"].isna().all()
        # the source frame is untouched
        assert df["turnover"].dtype == object

    def test_normalize_leaves_non_float_columns_alone(self):
        df = _bars_daily_df()
        out = normalize_missing_floats(df, "bars_daily")
        assert out["ticker"].dtype == df["ticker"].dtype
        assert out["volume"].dtype == df["volume"].dtype

    def test_factor_named_value_all_none(self, tmp_path):
        target = tmp_path / "2024.parquet"
        write_parquet_atomic(
            _factor_df("momentum_20d", value=None),
            target,
            table="factor",
            value_column="momentum_20d",
        )
        result = pd.read_parquet(target)
        assert result["momentum_20d"].dtype == "float64"


# ---------------------------------------------------------------------------
# FileLock — concurrency
# ---------------------------------------------------------------------------
class TestFileLock:
    def test_concurrent_lock_raises_concurrent_write_error(self, tmp_path):
        target = tmp_path / "2024-01-15.parquet"
        with FileLock(target):
            with pytest.raises(ConcurrentWriteError):
                with FileLock(target):
                    pass

    def test_concurrent_write_to_same_day_raises(self, tmp_path):
        """Two writers on the same day/table: the second is refused (§2)."""
        target = tmp_path / "2024-01-15.parquet"
        with FileLock(target):
            with pytest.raises(ConcurrentWriteError):
                write_parquet_atomic(_bars_daily_df(), target, table="bars_daily")

    def test_lock_released_after_exit(self, tmp_path):
        target = tmp_path / "2024-01-15.parquet"
        with FileLock(target):
            pass
        # no longer held — a second lock must succeed
        with FileLock(target):
            pass

    def test_lock_file_persists_after_exit(self, tmp_path):
        """The lock file is a permanent inode; the mutex is the flock.

        Unlinking on exit re-opens a two-holders race (second-review finding):
        a second process can flock the old inode after the unlock and before
        the unlink, and a third creates a fresh inode and flocks it too.
        """
        target = tmp_path / "2024-01-15.parquet"
        with FileLock(target) as lock:
            assert lock.lock_path.exists()
        assert lock.lock_path.exists(), "the lock file must not be unlinked"

    def test_lock_file_persists_after_exception(self, tmp_path):
        target = tmp_path / "2024-01-15.parquet"
        lock = FileLock(target)
        with pytest.raises(RuntimeError):
            with lock:
                raise RuntimeError("boom")
        assert lock.lock_path.exists()
        # and the flock was released despite the exception
        with FileLock(target):
            pass

    def test_repeated_locks_reuse_the_same_inode(self, tmp_path):
        """One target must map to one inode, forever — that is what makes the
        flock the only mutex.  Regression for the release-then-unlink race.
        """
        target = tmp_path / "2024-01-15.parquet"
        with FileLock(target) as first:
            inode_before = first.lock_path.stat().st_ino
        with FileLock(target) as second:
            inode_after = second.lock_path.stat().st_ino
        assert inode_before == inode_after

    def test_cleanup_never_deletes_a_held_lock(self, tmp_path):
        """A write longer than max_age_hours must not have its lock swept.

        The lock file's mtime is creation time and never refreshes while
        held, so age alone cannot distinguish residue from a live writer.
        """
        target = tmp_path / "2024-01-15.parquet"
        old_stamp = (datetime.now() - timedelta(hours=5)).timestamp()
        with FileLock(target) as lock:
            os.utime(lock.lock_path, (old_stamp, old_stamp))
            # mtime is 5h old, well past the 1h threshold, but it is held
            removed = cleanup_stale_locks(tmp_path, max_age_hours=1, dry_run=False)
            assert removed == []
            assert lock.lock_path.exists(), "a held lock must never be deleted"

    def test_cleanup_deletes_an_unheld_stale_lock(self, tmp_path):
        lock_path = tmp_path / "2024-01-15.parquet.lock"
        lock_path.write_bytes(b"")
        old_stamp = (datetime.now() - timedelta(hours=5)).timestamp()
        os.utime(lock_path, (old_stamp, old_stamp))

        removed = cleanup_stale_locks(tmp_path, max_age_hours=1, dry_run=False)

        assert removed == [lock_path]
        assert not lock_path.exists()

    def test_cleanup_dry_run_reports_but_keeps_unheld_lock(self, tmp_path):
        lock_path = tmp_path / "2024-01-15.parquet.lock"
        lock_path.write_bytes(b"")
        old_stamp = (datetime.now() - timedelta(hours=5)).timestamp()
        os.utime(lock_path, (old_stamp, old_stamp))

        removed = cleanup_stale_locks(tmp_path, max_age_hours=1)

        assert removed == [lock_path]
        assert lock_path.exists()

    def test_different_targets_do_not_conflict(self, tmp_path):
        a = tmp_path / "2024-01-15.parquet"
        b = tmp_path / "2024-01-16.parquet"
        with FileLock(a):
            with FileLock(b):
                pass

    def test_error_is_os_error_subclass(self):
        assert issubclass(ConcurrentWriteError, OSError)


# ---------------------------------------------------------------------------
# parse_date_from_filename
# ---------------------------------------------------------------------------
class TestParseDateFromFilename:
    def test_parses_iso_date(self):
        assert parse_date_from_filename("2024-01-15.parquet") == date(2024, 1, 15)

    def test_parses_iso_date_under_tmp_suffix(self):
        assert parse_date_from_filename("2024-01-15.parquet.tmp") == date(2024, 1, 15)

    def test_parses_iso_date_under_lock_suffix(self):
        assert parse_date_from_filename("2024-01-15.parquet.lock") == date(2024, 1, 15)

    def test_parses_compact_date(self):
        assert parse_date_from_filename("20240115.parquet") == date(2024, 1, 15)

    def test_parses_year_partition(self):
        assert parse_date_from_filename("2024.parquet") == date(2024, 1, 1)

    def test_parses_embedded_date_in_rejection_name(self):
        assert parse_date_from_filename("batch_2024-01-15_reject.json") == date(
            2024, 1, 15
        )

    def test_contract_specs_has_no_date(self):
        assert parse_date_from_filename("contract_specs.parquet") is None

    def test_industry_has_no_date(self):
        assert parse_date_from_filename("industry.parquet") is None

    def test_corporate_actions_has_no_date(self):
        assert parse_date_from_filename("corporate_actions.parquet") is None

    def test_metadata_has_no_date(self):
        assert parse_date_from_filename("metadata.json") is None

    def test_plain_name_has_no_date(self):
        assert parse_date_from_filename("no-date-here.parquet") is None

    def test_empty_name_has_no_date(self):
        assert parse_date_from_filename("") is None

    def test_invalid_calendar_date_is_not_a_year_guess(self):
        # 2024-13-45 is not a real date — must not fall through to a year
        assert parse_date_from_filename("2024-13-45.parquet") is None


# ---------------------------------------------------------------------------
# cleanup_expired — every delete branch
# ---------------------------------------------------------------------------
class TestCleanupExpired:
    def _make(self, root: Path, rel: str, mtime_days: float = 0) -> Path:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
        if mtime_days:
            stamp = (datetime.now() - timedelta(days=mtime_days)).timestamp()
            os.utime(path, (stamp, stamp))
        return path

    def test_dry_run_default_deletes_nothing(self, tmp_path):
        """A bare call must never delete — this pins the *default*, not the kwarg.

        §3 forbids a 「不传就删」 path, so the unguarded call is the one that
        has to hold.  Passing ``dry_run=True`` here would pass even if the
        default flipped (Review finding 2).
        """
        today = date(2024, 6, 1)
        old = self._make(tmp_path, "raw/src/eq/2020-01-01.parquet", mtime_days=400)
        recent = self._make(tmp_path, "raw/src/eq/2024-05-01.parquet", mtime_days=10)
        before = sorted(p.name for p in tmp_path.rglob("*") if p.is_file())

        removed = cleanup_expired(tmp_path, today=today)

        after = sorted(p.name for p in tmp_path.rglob("*") if p.is_file())
        assert before == after, "a bare cleanup_expired call must not change the file count"
        assert old.exists() and recent.exists()
        # it still reports what it *would* delete
        assert removed == [old]

    def test_explicit_dry_run_true_deletes_nothing(self, tmp_path):
        """DoD row: cleanup_expired(dry_run=True) deletes nothing."""
        today = date(2024, 6, 1)
        old = self._make(tmp_path, "raw/src/eq/2020-01-01.parquet", mtime_days=400)
        before = sorted(p.name for p in tmp_path.rglob("*") if p.is_file())

        removed = cleanup_expired(tmp_path, dry_run=True, today=today)

        after = sorted(p.name for p in tmp_path.rglob("*") if p.is_file())
        assert before == after, "dry_run=True must not change the file count"
        assert old.exists()
        assert removed == [old]

    def test_dry_run_false_deletes_only_expired(self, tmp_path):
        today = date(2024, 6, 1)
        old = self._make(tmp_path, "raw/src/eq/2020-01-01.parquet", mtime_days=400)
        recent = self._make(tmp_path, "raw/src/eq/2024-05-01.parquet", mtime_days=10)

        removed = cleanup_expired(tmp_path, dry_run=False, today=today)

        assert not old.exists(), "expired raw file must be deleted"
        assert recent.exists(), "unexpired raw file must survive"
        assert removed == [old]

    def test_permanent_policy_never_deletes(self, tmp_path):
        today = date(2024, 6, 1)
        ancient = self._make(tmp_path, "asset/bars_daily/1999-01-01.parquet", mtime_days=9000)
        factor = self._make(tmp_path, "factors/mom/v1/1999.parquet", mtime_days=9000)

        removed = cleanup_expired(tmp_path, dry_run=False, today=today)

        assert ancient.exists(), "asset/ is permanent"
        assert factor.exists(), "factors/ is permanent"
        assert removed == []

    def test_quarantine_30_day_window(self, tmp_path):
        today = date(2024, 6, 1)
        expired = self._make(tmp_path, "quarantine/eq/2024-03-01.parquet", mtime_days=100)
        fresh = self._make(tmp_path, "quarantine/eq/2024-05-25.parquet", mtime_days=10)

        removed = cleanup_expired(tmp_path, dry_run=False, today=today)

        assert not expired.exists(), "quarantine beyond 30 days must go"
        assert fresh.exists()
        assert removed == [expired]

    def test_no_date_files_are_kept(self, tmp_path):
        """§5 decision 8: a full-file reference table has no date to age."""
        today = date(2024, 6, 1)
        ref = self._make(tmp_path, "raw/src/eq/contract_specs.parquet", mtime_days=9000)

        removed = cleanup_expired(tmp_path, dry_run=False, today=today)

        assert ref.exists(), "a file with no parseable date is never expired"
        assert removed == []

    def test_skips_tmp_and_lock_residue(self, tmp_path):
        """Residue is cleanup_stale_*'s job, not cleanup_expired's."""
        today = date(2024, 6, 1)
        stale_tmp = self._make(tmp_path, "raw/src/eq/2020-01-01.parquet.tmp", mtime_days=400)
        stale_lock = self._make(tmp_path, "raw/src/eq/2020-01-01.parquet.lock", mtime_days=400)
        stale_data = self._make(tmp_path, "raw/src/eq/2020-01-01.parquet", mtime_days=400)

        removed = cleanup_expired(tmp_path, dry_run=False, today=today)

        assert stale_tmp.exists()
        assert stale_lock.exists()
        assert not stale_data.exists()
        assert removed == [stale_data]

    def test_unlisted_prefix_uses_default_permanent(self, tmp_path):
        today = date(2024, 6, 1)
        odd = self._make(tmp_path, "scratch/whatever/2000-01-01.parquet", mtime_days=9000)

        removed = cleanup_expired(tmp_path, dry_run=False, today=today)

        assert odd.exists(), "unlisted prefixes default to permanent"
        assert removed == []

    def test_boundary_is_exclusive(self, tmp_path):
        """A file exactly `days` old is kept; one day older is deleted."""
        today = date(2024, 6, 1)
        # raw retention = 365 days → cutoff = 2023-06-02
        exactly = self._make(tmp_path, "raw/src/eq/2023-06-02.parquet")
        one_day_over = self._make(tmp_path, "raw/src/eq/2023-06-01.parquet")

        cleanup_expired(tmp_path, dry_run=False, today=today)

        assert exactly.exists(), "cutoff day itself is not expired"
        assert not one_day_over.exists()


# ---------------------------------------------------------------------------
# cleanup_stale_tmp / cleanup_stale_locks — every delete branch
# ---------------------------------------------------------------------------
class TestCleanupStaleResidue:
    def test_stale_tmp_deleted_fresh_tmp_kept(self, tmp_path):
        stale = tmp_path / "2024-01-15.parquet.tmp"
        stale.write_bytes(b"old")
        fresh = tmp_path / "2024-01-16.parquet.tmp"
        fresh.write_bytes(b"new")
        old_stamp = (datetime.now() - timedelta(hours=3)).timestamp()
        os.utime(stale, (old_stamp, old_stamp))

        removed = cleanup_stale_tmp(tmp_path, max_age_hours=1, dry_run=False)

        assert not stale.exists()
        assert fresh.exists(), "a live write's .tmp must not be swept"
        assert removed == [stale]

    def test_stale_tmp_default_deletes_nothing(self, tmp_path):
        """A bare call must never delete — pins the default, not the kwarg."""
        stale = tmp_path / "2024-01-15.parquet.tmp"
        stale.write_bytes(b"old")
        old_stamp = (datetime.now() - timedelta(hours=3)).timestamp()
        os.utime(stale, (old_stamp, old_stamp))

        removed = cleanup_stale_tmp(tmp_path, max_age_hours=1)

        assert stale.exists()
        assert removed == [stale]

    def test_stale_tmp_explicit_dry_run_true_deletes_nothing(self, tmp_path):
        stale = tmp_path / "2024-01-15.parquet.tmp"
        stale.write_bytes(b"old")
        old_stamp = (datetime.now() - timedelta(hours=3)).timestamp()
        os.utime(stale, (old_stamp, old_stamp))

        removed = cleanup_stale_tmp(tmp_path, max_age_hours=1, dry_run=True)

        assert stale.exists()
        assert removed == [stale]

    def test_stale_tmp_ignores_data_files(self, tmp_path):
        data = tmp_path / "2024-01-15.parquet"
        data.write_bytes(b"data")
        old_stamp = (datetime.now() - timedelta(hours=3)).timestamp()
        os.utime(data, (old_stamp, old_stamp))

        removed = cleanup_stale_tmp(tmp_path, max_age_hours=1, dry_run=False)

        assert data.exists(), "only .tmp residue is swept"
        assert removed == []

    def test_stale_lock_deleted_fresh_lock_kept(self, tmp_path):
        stale = tmp_path / "2024-01-15.parquet.lock"
        stale.write_bytes(b"old")
        fresh = tmp_path / "2024-01-16.parquet.lock"
        fresh.write_bytes(b"new")
        old_stamp = (datetime.now() - timedelta(hours=3)).timestamp()
        os.utime(stale, (old_stamp, old_stamp))

        removed = cleanup_stale_locks(tmp_path, max_age_hours=1, dry_run=False)

        assert not stale.exists()
        assert fresh.exists()
        assert removed == [stale]

    def test_stale_lock_default_deletes_nothing(self, tmp_path):
        """A bare call must never delete — pins the default, not the kwarg."""
        stale = tmp_path / "2024-01-15.parquet.lock"
        stale.write_bytes(b"old")
        old_stamp = (datetime.now() - timedelta(hours=3)).timestamp()
        os.utime(stale, (old_stamp, old_stamp))

        removed = cleanup_stale_locks(tmp_path, max_age_hours=1)

        assert stale.exists()
        assert removed == [stale]

    def test_stale_lock_explicit_dry_run_true_deletes_nothing(self, tmp_path):
        stale = tmp_path / "2024-01-15.parquet.lock"
        stale.write_bytes(b"old")
        old_stamp = (datetime.now() - timedelta(hours=3)).timestamp()
        os.utime(stale, (old_stamp, old_stamp))

        removed = cleanup_stale_locks(tmp_path, max_age_hours=1, dry_run=True)

        assert stale.exists()
        assert removed == [stale]

    def test_stale_lock_ignores_data_files(self, tmp_path):
        data = tmp_path / "2024-01-15.parquet"
        data.write_bytes(b"data")
        old_stamp = (datetime.now() - timedelta(hours=3)).timestamp()
        os.utime(data, (old_stamp, old_stamp))

        removed = cleanup_stale_locks(tmp_path, max_age_hours=1, dry_run=False)

        assert data.exists()
        assert removed == []

    def test_missing_root_is_a_noop(self, tmp_path):
        assert cleanup_stale_tmp(tmp_path / "nope") == []
        assert cleanup_stale_locks(tmp_path / "nope") == []


# ---------------------------------------------------------------------------
# lifecycle.yaml
# ---------------------------------------------------------------------------
class TestLifecycleConfig:
    def test_config_loads_and_covers_the_documented_layers(self):
        policy = load_lifecycle()
        prefixes = {p["prefix"]: p for p in policy["policies"]}
        assert prefixes["asset"]["retention"] == "permanent"
        assert prefixes["factors"]["retention"] == "permanent"
        assert prefixes["raw"]["retention"] == "rolling"
        assert prefixes["raw"]["days"] == 365
        assert prefixes["quarantine"]["retention"] == "rolling"
        assert prefixes["quarantine"]["days"] == 30

    def test_default_is_permanent(self):
        policy = load_lifecycle()
        assert policy["default"] == "permanent"

    def test_missing_config_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_lifecycle(tmp_path / "no-such.yaml")

    def test_config_without_policies_raises(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("version: 1\n", encoding="utf-8")
        with pytest.raises(ValueError, match="policies"):
            load_lifecycle(bad)


# ---------------------------------------------------------------------------
# Capacity tiers
# ---------------------------------------------------------------------------
class TestCapacity:
    def _free(self, tmp_path, monkeypatch, free_bytes: int) -> str:
        import data.storage_ops as mod

        class Usage:
            pass

        monkeypatch.setattr(
            mod.shutil,
            "disk_usage",
            lambda _p: __import__("collections").namedtuple("U", "total used free")(
                100 * 1024**3, 100 * 1024**3 - free_bytes, free_bytes
            ),
        )
        return check_capacity(tmp_path)

    def test_well_above_ok_threshold(self, tmp_path, monkeypatch):
        assert self._free(tmp_path, monkeypatch, 60 * 1024**3) == "OK"

    def test_just_above_ok_threshold(self, tmp_path, monkeypatch):
        assert self._free(tmp_path, monkeypatch, 50 * 1024**3 + 1) == "OK"

    def test_exactly_ok_threshold_is_warning(self, tmp_path, monkeypatch):
        assert self._free(tmp_path, monkeypatch, 50 * 1024**3) == "WARNING"

    def test_mid_warning_band(self, tmp_path, monkeypatch):
        assert self._free(tmp_path, monkeypatch, 20 * 1024**3) == "WARNING"

    def test_just_above_critical_threshold(self, tmp_path, monkeypatch):
        assert self._free(tmp_path, monkeypatch, 10 * 1024**3 + 1) == "WARNING"

    def test_exactly_critical_threshold(self, tmp_path, monkeypatch):
        assert self._free(tmp_path, monkeypatch, 10 * 1024**3) == "CRITICAL"

    def test_zero_free_is_critical(self, tmp_path, monkeypatch):
        assert self._free(tmp_path, monkeypatch, 0) == "CRITICAL"

    def test_returns_a_known_tier(self, tmp_path):
        assert check_capacity(tmp_path) in {"OK", "WARNING", "CRITICAL"}

    def test_disk_full_error_is_os_error(self):
        assert issubclass(DiskFullError, OSError)


# ---------------------------------------------------------------------------
# Daily growth
# ---------------------------------------------------------------------------
class TestDailyGrowth:
    def test_within_baseline(self, tmp_path):
        target = tmp_path / "2024-06-01.parquet"
        target.write_bytes(b"x" * 1024)
        report = check_daily_growth(tmp_path, today=date(2024, 6, 1))
        assert report["exceeded"] is False
        assert report["files"] == 1
        assert report["bytes"] == 1024

    def test_over_baseline(self, tmp_path):
        target = tmp_path / "2024-06-01.parquet"
        target.write_bytes(b"x" * 2048)
        report = check_daily_growth(
            tmp_path, today=date(2024, 6, 1), baseline_bytes=1024
        )
        assert report["exceeded"] is True

    def test_counts_only_today(self, tmp_path):
        (tmp_path / "2024-06-01.parquet").write_bytes(b"x" * 100)
        (tmp_path / "2024-05-01.parquet").write_bytes(b"y" * 9999)
        report = check_daily_growth(tmp_path, today=date(2024, 6, 1))
        assert report["files"] == 1
        assert report["bytes"] == 100

    def test_empty_root(self, tmp_path):
        report = check_daily_growth(tmp_path, today=date(2024, 6, 1))
        assert report["files"] == 0
        assert report["exceeded"] is False

    def test_residue_is_not_counted(self, tmp_path):
        """.tmp / .lock are write residue, not data."""
        (tmp_path / "2024-06-01.parquet").write_bytes(b"x" * 100)
        (tmp_path / "2024-06-01.parquet.tmp").write_bytes(b"y" * 9999)
        (tmp_path / "2024-06-01.parquet.lock").write_bytes(b"z" * 9999)
        report = check_daily_growth(tmp_path, today=date(2024, 6, 1))
        assert report["files"] == 1
        assert report["bytes"] == 100


class TestCapacityMissingPath:
    def test_missing_path_measures_nearest_existing_ancestor_not_cwd(self, tmp_path, monkeypatch):
        """Probing cwd would report an unrelated filesystem's free space.

        A path that does not exist yet must be measured on the filesystem it
        would land on — its nearest existing ancestor.
        """
        import data.storage_ops as mod

        probed: list[str] = []
        monkeypatch.setattr(
            mod.shutil,
            "disk_usage",
            lambda p: (probed.append(str(p)) or __import__("collections").namedtuple(
                "U", "total used free"
            )(100 * 1024**3, 0, 60 * 1024**3)),
        )

        missing = tmp_path / "no" / "such" / "dir"
        check_capacity(missing)

        assert probed == [str(tmp_path)], (
            f"expected the nearest existing ancestor {tmp_path}, probed {probed}"
        )
        assert str(Path.cwd()) not in probed, "must never fall back to cwd"


# ---------------------------------------------------------------------------
# scripts/backup.sh — the guards around a destructive rsync --delete
# ---------------------------------------------------------------------------
BACKUP_SH = Path(__file__).resolve().parents[2] / "scripts" / "backup.sh"


def _run_backup(src: Path, dest: Path, **env: str) -> subprocess.CompletedProcess:
    environ = {**os.environ, "BACKUP_ALLOW_LOCAL_DEST": "1", **env}
    return subprocess.run(
        ["bash", str(BACKUP_SH), str(src), str(dest)],
        capture_output=True,
        text=True,
        env=environ,
    )


def _make_data_platform(root: Path) -> Path:
    src = root / "data_platform"
    (src / "asset" / "bars_daily").mkdir(parents=True)
    (src / "raw" / "src" / "eq").mkdir(parents=True)
    (src / "asset" / "bars_daily" / "2024-01-15.parquet").write_bytes(b"a")
    (src / "raw" / "src" / "eq" / "2024-01-15.parquet").write_bytes(b"bb")
    (src / "raw" / "src" / "eq" / "2024-01-15.parquet.tmp").write_bytes(b"junk")
    (src / "raw" / "src" / "eq" / "2024-01-15.parquet.lock").write_bytes(b"junk")
    return src


class TestBackupScript:
    def test_happy_path_copies_and_excludes_residue(self, tmp_path):
        src = _make_data_platform(tmp_path)
        dest = tmp_path / "dest"
        dest.mkdir()

        result = _run_backup(src, dest)

        assert result.returncode == 0, result.stderr
        assert (dest / "asset" / "bars_daily" / "2024-01-15.parquet").read_bytes() == b"a"
        assert (dest / "raw" / "src" / "eq" / "2024-01-15.parquet").read_bytes() == b"bb"
        assert not list(dest.rglob("*.tmp")), "*.tmp must not be backed up"
        assert not list(dest.rglob("*.lock")), "*.lock must not be backed up"
        assert "counts match" in result.stdout

    def test_empty_source_refused_before_destructive_sync(self, tmp_path):
        """HIGH: an empty SRC must not let --delete wipe DEST and print OK."""
        src = tmp_path / "data_platform"
        (src / "asset").mkdir(parents=True)  # marker present, but no files
        dest = tmp_path / "dest"
        dest.mkdir()
        keep = dest / "asset" / "bars_daily" / "2023-01-01.parquet"
        keep.parent.mkdir(parents=True)
        keep.write_bytes(b"last-good-backup")

        result = _run_backup(src, dest)

        assert result.returncode == 2
        assert "no backable files" in result.stderr
        assert keep.exists(), "the last good backup must be untouched"
        assert "OK" not in result.stdout

    def test_wrong_source_refused_and_dest_survives(self, tmp_path):
        """A SRC that is not a data_platform root must be refused outright."""
        src = tmp_path / "not_data_platform"
        src.mkdir()
        (src / "random.txt").write_bytes(b"hello")
        dest = tmp_path / "dest"
        dest.mkdir()
        keep = dest / "asset" / "old.parquet"
        keep.parent.mkdir(parents=True)
        keep.write_bytes(b"last-good-backup")

        result = _run_backup(src, dest)

        assert result.returncode == 2
        assert "does not look like a data_platform root" in result.stderr
        assert keep.exists()
        assert "OK" not in result.stdout

    def test_missing_destination_refused(self, tmp_path):
        """No mkdir -p: an unmounted external disk must not materialise locally."""
        src = _make_data_platform(tmp_path)
        dest = tmp_path / "not_mounted"

        result = _run_backup(src, dest)

        assert result.returncode == 2
        assert "does not exist" in result.stderr
        assert not dest.exists(), "backup.sh must not create the destination"

    def test_foreign_nonempty_destination_refused(self, tmp_path):
        """A mistyped DEST holding an unrelated tree must not be --delete'd."""
        src = _make_data_platform(tmp_path)
        dest = tmp_path / "unrelated"
        dest.mkdir()
        victim = dest / "important" / "notes.txt"
        victim.parent.mkdir(parents=True)
        victim.write_bytes(b"do not delete me")

        result = _run_backup(src, dest)

        assert result.returncode == 2
        assert "does not look like a previous backup" in result.stderr
        assert victim.exists()

    def test_destination_inside_source_refused(self, tmp_path):
        src = _make_data_platform(tmp_path)
        dest = src / "nested_dest"
        dest.mkdir()

        result = _run_backup(src, dest)

        assert result.returncode == 2
        assert "inside source" in result.stderr

    def test_source_inside_destination_refused(self, tmp_path):
        dest = tmp_path / "dest"
        dest.mkdir()
        src = _make_data_platform(dest)

        result = _run_backup(src, dest)

        assert result.returncode == 2
        assert "inside destination" in result.stderr

    def test_same_source_and_destination_refused(self, tmp_path):
        src = _make_data_platform(tmp_path)

        result = _run_backup(src, src)

        assert result.returncode == 2
        assert "same directory" in result.stderr

    def test_count_ignores_residue_directories(self, tmp_path):
        """rsync prunes *.tmp/*.lock directories; find must too."""
        src = _make_data_platform(tmp_path)
        residue_dir = src / "asset" / "leftover.tmp"
        residue_dir.mkdir()
        (residue_dir / "inner.parquet").write_bytes(b"should not count")
        dest = tmp_path / "dest"
        dest.mkdir()

        result = _run_backup(src, dest)

        assert result.returncode == 0, result.stderr
        assert "counts match" in result.stdout
        assert "mismatch" not in result.stdout
