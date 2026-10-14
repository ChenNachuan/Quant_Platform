"""Regression tests for scripts/backup.sh device guards (QUANT-15 #4).

When ``device_of`` returns empty (both ``stat -c`` and ``stat -f`` fail), the
device guards must refuse and exit — not silently skip, which would let
``rsync --delete`` run without a mount-point or same-device check.
"""

import os
import subprocess
from pathlib import Path

BACKUP_SH = Path(__file__).resolve().parents[1] / "scripts" / "backup.sh"


def _make_data_platform(root: Path) -> Path:
    src = root / "data_platform"
    (src / "asset" / "bars_daily").mkdir(parents=True)
    (src / "asset" / "bars_daily" / "2024-01-15.parquet").write_bytes(b"a")
    return src


def _run_backup(src: Path, dest: Path, **env: str) -> subprocess.CompletedProcess:
    environ = {**os.environ, "BACKUP_ALLOW_LOCAL_DEST": "0", **env}
    return subprocess.run(
        ["bash", str(BACKUP_SH), str(src), str(dest)],
        capture_output=True,
        text=True,
        env=environ,
    )


class TestDeviceGuardFailClosed:
    def test_empty_device_of_refuses_and_skips_rsync(self, tmp_path):
        """When stat fails in both forms, the guard must explicitly refuse.

        The guards used to short-circuit on empty device ids and run
        rsync --delete anyway.  A destructive path must fail closed with
        a clear die(), not rely on set -e catching the failed substitution.
        """
        src = _make_data_platform(tmp_path)
        dest = tmp_path / "dest"
        dest.mkdir()
        keep = dest / "asset" / "old_backup.parquet"
        keep.parent.mkdir(parents=True)
        keep.write_bytes(b"last-good-backup")

        # Stub stat so both -c and -f fail, making device_of return empty.
        stub_dir = tmp_path / "bin"
        stub_dir.mkdir()
        stat_stub = stub_dir / "stat"
        stat_stub.write_text("#!/usr/bin/env bash\nexit 1\n")
        stat_stub.chmod(0o755)

        result = _run_backup(src, dest, PATH=f"{stub_dir}:{os.environ['PATH']}")

        assert result.returncode == 2, (
            "backup.sh must die(2) when device_of is empty, "
            f"got exit {result.returncode}\nstdout={result.stdout}\nstderr={result.stderr}"
        )
        assert keep.exists(), "target tree must be preserved when guards fail"
        assert not (dest / "asset" / "bars_daily").exists(), "rsync must not have run"

    def test_empty_device_of_error_names_the_refusal(self, tmp_path):
        """The error must say the device could not be determined."""
        src = _make_data_platform(tmp_path)
        dest = tmp_path / "dest"
        dest.mkdir()

        stub_dir = tmp_path / "bin"
        stub_dir.mkdir()
        stat_stub = stub_dir / "stat"
        stat_stub.write_text("#!/usr/bin/env bash\nexit 1\n")
        stat_stub.chmod(0o755)

        result = _run_backup(src, dest, PATH=f"{stub_dir}:{os.environ['PATH']}")

        assert result.returncode == 2
        combined = result.stdout + result.stderr
        assert "device" in combined.lower(), (
            f"error must name the device refusal, got: {combined}"
        )
