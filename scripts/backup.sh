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
# DEST must be a mount point — the mounted external disk itself.  That is the
# honest form of the guard: a same-filesystem comparison alone does NOT detect
# an unmounted disk, because when data_platform has its own volume an
# unmounted mountpoint falls back to the root filesystem, a *different* device
# from SRC, and a naive comparison would pass.  BACKUP_ALLOW_LOCAL_DEST=1
# deliberately allows a non-mountpoint / same-device DEST for tests and for
# knowingly local backups.
#
# Excludes *.tmp / *.lock and well-known filesystem metadata (lost+found,
# .Spotlight-V100, …) — neither is backup content, and --delete must not
# choke on or remove the latter.
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
[[ -e "${DEST}" ]] || die "destination does not exist (mount the external disk first): ${DEST}"
[[ -d "${DEST}" ]] || die "destination exists and is not a directory: ${DEST}"
[[ -w "${DEST}" ]] || die "destination is not writable: ${DEST}"

# Resolve both sides BEFORE any find(1).  find is -P and will not descend a
# symlinked start, so a symlinked SRC/DEST would report zero files and quietly
# skip the guards that depend on counting.
src_abs="$(cd "${SRC}" && pwd -P)"
dest_abs="$(cd "${DEST}" && pwd -P)"

[[ "${src_abs}" != "${dest_abs}" ]] || die "source and destination are the same directory: ${SRC}"
case "${dest_abs}/" in "${src_abs}/"*) die "destination is inside source: ${DEST}" ;; esac
case "${src_abs}/" in "${dest_abs}/"*) die "source is inside destination: ${SRC}" ;; esac

# --- one list, three consumers --------------------------------------------
# Filesystem metadata a freshly formatted external disk always carries.  Used
# by the DEST pre-check, the counters and rsync alike, so those three can
# never disagree about what counts as content.
FS_METADATA=(
    'lost+found'
    'System Volume Information'
    '$RECYCLE.BIN'
    '.Trash'
    '.DS_Store'
    '.Spotlight-V100'
    '.fseventsd'
    '.Trashes'
    '.TemporaryItems'
    '.DocumentRevisions-V100'
    '.BC'
)

find_skip=()    # -o -name <n> -o -name <m> ...  (joins an existing OR chain)
find_keep=()    # ! -name <n> ! -name <m> ...    (implicit AND)
rsync_excludes=(--exclude='*.tmp' --exclude='*.lock')
for _n in "${FS_METADATA[@]}"; do
    find_skip+=(-o -name "${_n}")
    find_keep+=(! -name "${_n}")
    rsync_excludes+=(--exclude="${_n}")
done
unset _n

# --- counting must agree with rsync's --exclude semantics -----------------
# rsync prunes matching DIRECTORIES; a plain `find -type f ! -name` descends
# into them and counts the files inside, which turns a complete backup into a
# spurious "file count mismatch".  Pruning any matching name covers both.
count_files() {
    find "$1" \
        \( \( -name '*.tmp' -o -name '*.lock' "${find_skip[@]}" \) -prune \) -o \
        \( -type f -print \) \
        | wc -l | tr -d ' '
}

# --- SRC must look like a data_platform root ------------------------------
# A wrong or empty SRC is the expensive failure: rsync --delete would erase
# the whole DEST, and with both sides empty the count comparison would print
# OK and exit 0.  Refuse before the destructive sync.
[[ -d "${src_abs}/asset" || -d "${src_abs}/raw" || -d "${src_abs}/factors" ]] \
    || die "source does not look like a data_platform root (no asset/ raw/ or factors/): ${SRC}"

src_count_before="$(count_files "${src_abs}")"
[[ "${src_count_before}" -gt 0 ]] \
    || die "source holds no backable files (after excluding *.tmp/*.lock): ${SRC}"

# --- DEST must already look like a previous backup ------------------------
# A mistyped DEST pointing at an unrelated tree would otherwise be erased by
# --delete.  Filesystem metadata is not content: a freshly formatted external
# disk always carries it, and the documented deployment is exactly "rsync to a
# mounted external disk".
if [[ "$(find "${dest_abs}" -mindepth 1 -maxdepth 1 "${find_keep[@]}" -print | wc -l | tr -d ' ')" -gt 0 ]]; then
    [[ -d "${dest_abs}/asset" || -d "${dest_abs}/raw" || -d "${dest_abs}/factors" ]] \
        || die "destination has content that does not look like a previous backup: ${DEST}"
fi

# --- DEST must be a mount point, and not the SRC filesystem ---------------
# GNU stat first (`-c`), BSD/macOS fallback (`-f`).  The order is load-bearing:
# on Linux `stat -f` means --file-system, so `-f %d` errors but still prints a
# dump to stdout and a trailing fallback appends the real device id onto it —
# the two sides then never compare equal and the guard silently never fires.
device_of() { stat -c %d "$1" 2>/dev/null || stat -f %d "$1" 2>/dev/null; }

if [[ "${BACKUP_ALLOW_LOCAL_DEST:-0}" != "1" ]]; then
    dest_dev="$(device_of "${dest_abs}")"
    parent_dev="$(device_of "$(dirname "${dest_abs}")")"
    src_dev="$(device_of "${src_abs}")"

    # A same-device comparison against SRC does not mean "the disk is
    # mounted": if data_platform has its own volume and the external disk is
    # not mounted, the mountpoint falls back to the root filesystem — a
    # different device from SRC — and the comparison would pass.  Require
    # DEST to actually be a mount point.
    if [[ -n "${dest_dev}" && -n "${parent_dev}" && "${dest_dev}" == "${parent_dev}" ]]; then
        die "destination is not a mount point (is the external disk mounted there?); set BACKUP_ALLOW_LOCAL_DEST=1 to override: ${DEST}"
    fi
    if [[ -n "${src_dev}" && -n "${dest_dev}" && "${src_dev}" == "${dest_dev}" ]]; then
        die "destination is on the same filesystem as source; set BACKUP_ALLOW_LOCAL_DEST=1 to override: ${DEST}"
    fi
fi

echo "backup.sh: rsync ${src_abs}/ -> ${dest_abs}/"
# Capture rather than letting set -e abort: rsync 23 (partial transfer) and
# 24 (source vanished) are exactly the live-tree churn the two-sided count
# below tolerates — a Parquet file being replaced under us is not a failed
# backup, and must not stop us reaching the count check or the OK line.
rsync_status=0
rsync -a --delete "${rsync_excludes[@]}" "${src_abs}/" "${dest_abs}/" || rsync_status=$?
case "${rsync_status}" in
    0|23|24) ;;
    *)
        echo "backup.sh: FAIL — rsync exited ${rsync_status}" >&2
        exit "${rsync_status}"
        ;;
esac

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
