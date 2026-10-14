#!/usr/bin/env bash
# backup.sh — full data_platform rsync to an external disk (docs/design/data/storage_ops.md §51).
#
# Scope: the whole data_platform tree (settled decision §51, superseding the
# older research-only scope).  Cadence is whatever cron/schedule invokes this
# script; the script itself is cadence-agnostic.  rclone cloud backup is NOT
# enabled here — that is a must-ask-back (QUANT-13 §5).
#
# rsync runs with --delete, so every guard below exists to stop a wrong
# argument from erasing the last good backup and still reporting success.
#
# Usage:
#   scripts/backup.sh <data_platform_dir> <backup_dest_dir>
#   DATA_PLATFORM=... BACKUP_DEST=... scripts/backup.sh
#
# DEST is expected to be a mounted external disk.  Backing up onto the same
# filesystem as SRC is refused unless BACKUP_ALLOW_LOCAL_DEST=1 is set
# deliberately — an unmounted disk would otherwise become a plain local
# directory and fill the root filesystem.
#
# Excludes *.tmp / *.lock — write residue is not backup content.
set -euo pipefail

SRC="${1:-${DATA_PLATFORM:-}}"
DEST="${2:-${BACKUP_DEST:-}}"

die() { echo "backup.sh: $*" >&2; exit 2; }

if [[ -z "${SRC}" || -z "${DEST}" ]]; then
    echo "usage: scripts/backup.sh <data_platform_dir> <backup_dest_dir>" >&2
    echo "   or: DATA_PLATFORM=... BACKUP_DEST=... scripts/backup.sh" >&2
    exit 2
fi

[[ -d "${SRC}" ]] || die "source is not a directory: ${SRC}"

# --- SRC must look like a data_platform root -------------------------------
# A wrong or empty SRC is the expensive failure: rsync --delete would erase
# the whole DEST, and with both sides empty the count comparison would print
# OK and exit 0.  Refuse before the destructive sync.
[[ -d "${SRC}/asset" || -d "${SRC}/raw" || -d "${SRC}/factors" ]] \
    || die "source does not look like a data_platform root (no asset/ raw/ or factors/): ${SRC}"

# --- counting must agree with rsync's --exclude semantics -----------------
# rsync prunes matching DIRECTORIES; a plain `find -type f ! -name` descends
# into them and counts the files inside, which turns a complete backup into a
# spurious "file count mismatch".
count_files() {
    find "$1" \
        \( -type d \( -name '*.tmp' -o -name '*.lock' \) -prune \) -o \
        \( -type f ! -name '*.tmp' ! -name '*.lock' -print \) \
        | wc -l | tr -d ' '
}

src_count_before="$(count_files "${SRC}")"
[[ "${src_count_before}" -gt 0 ]] \
    || die "source holds no backable files (after excluding *.tmp/*.lock): ${SRC}"

# --- DEST guards ----------------------------------------------------------
# Never mkdir -p an arbitrary path: an unmounted external disk would silently
# become a local directory and fill the root filesystem.
[[ -e "${DEST}" ]] || die "destination does not exist (mount the external disk first): ${DEST}"
[[ -d "${DEST}" ]] || die "destination exists and is not a directory: ${DEST}"
[[ -w "${DEST}" ]] || die "destination is not writable: ${DEST}"

src_abs="$(cd "${SRC}" && pwd -P)"
dest_abs="$(cd "${DEST}" && pwd -P)"
[[ "${src_abs}" != "${dest_abs}" ]] || die "source and destination are the same directory: ${SRC}"
case "${dest_abs}/" in "${src_abs}/"*) die "destination is inside source: ${DEST}" ;; esac
case "${src_abs}/" in "${dest_abs}/"*) die "source is inside destination: ${SRC}" ;; esac

# A non-empty DEST must already look like a previous backup.  A mistyped DEST
# pointing at an unrelated tree would otherwise be erased by --delete.
# Filesystem metadata is not content: a freshly formatted external disk always
# carries lost+found / .Spotlight-V100 / .fseventsd / .Trashes and friends, and
# the documented deployment is exactly "rsync to a mounted external disk".
# find -name matches the basename literally, so the dollar signs and dots are
# not regex metacharacters here.
dest_real_count="$(find "${DEST}" -mindepth 1 -maxdepth 1 \
    ! -name 'lost+found' \
    ! -name 'System Volume Information' \
    ! -name '$RECYCLE.BIN' \
    ! -name '.Trash' \
    ! -name '.DS_Store' \
    ! -name '.Spotlight-V100' \
    ! -name '.fseventsd' \
    ! -name '.Trashes' \
    ! -name '.TemporaryItems' \
    ! -name '.DocumentRevisions-V100' \
    ! -name '.BC' \
    -print | wc -l | tr -d ' ')"
if [[ "${dest_real_count}" -gt 0 ]]; then
    [[ -d "${DEST}/asset" || -d "${DEST}/raw" || -d "${DEST}/factors" ]] \
        || die "destination has content that does not look like a previous backup: ${DEST}"
fi

# --- DEST must be on a different filesystem than SRC ----------------------
# The design assumes an external disk.  Same-device DEST is the unmounted-
# disk case, so it is refused unless deliberately allowed.
#
# GNU stat first (`-c`), BSD/macOS fallback (`-f`).  The order is load-bearing:
# on Linux `stat -f` means --file-system, so `-f %d` errors but still prints a
# dump to stdout and a trailing fallback appends the real device id onto it —
# the two sides then never compare equal and the guard silently never fires.
device_of() { stat -c %d "$1" 2>/dev/null || stat -f %d "$1" 2>/dev/null; }
if [[ "${BACKUP_ALLOW_LOCAL_DEST:-0}" != "1" ]]; then
    src_dev="$(device_of "${src_abs}")"
    dest_dev="$(device_of "${dest_abs}")"
    if [[ -n "${src_dev}" && -n "${dest_dev}" && "${src_dev}" == "${dest_dev}" ]]; then
        die "destination is on the same filesystem as source (unmounted external disk?); set BACKUP_ALLOW_LOCAL_DEST=1 to override: ${DEST}"
    fi
fi

echo "backup.sh: rsync ${SRC}/ -> ${DEST}/"
rsync -a --delete \
    --exclude='*.tmp' \
    --exclude='*.lock' \
    "${src_abs}/" "${dest_abs}/"

# File-count comparison (§51).  Same exclusion semantics as the rsync above.
#
# SRC is a live Parquet tree, so both sides are sampled AFTER the sync and the
# check is two-sided rather than an equality:
#   dest_count >= src_count_before   nothing that existed at start was lost
#   dest_count <= src_count          nothing appeared on DEST that SRC lacks
# A file landing mid-sync therefore cannot turn a successful backup into a
# FAIL, while a genuinely partial sync still fails the lower bound.
src_count="$(count_files "${src_abs}")"
dest_count="$(count_files "${dest_abs}")"

echo "backup.sh: source files=${src_count} dest files=${dest_count} (pre-sync ${src_count_before})"
if [[ "${dest_count}" -lt "${src_count_before}" ]]; then
    echo "backup.sh: FAIL — destination lost files (source-before=${src_count_before} dest=${dest_count})" >&2
    exit 1
fi
if [[ "${dest_count}" -gt "${src_count}" ]]; then
    echo "backup.sh: FAIL — destination has files source does not (source=${src_count} dest=${dest_count})" >&2
    exit 1
fi

echo "backup.sh: OK — counts match"
