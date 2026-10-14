#!/usr/bin/env bash
# backup.sh — full data_platform rsync to an external disk (docs/design/data/storage_ops.md §51).
#
# Scope: the whole data_platform tree (settled decision §51, superseding the
# older research-only scope).  Cadence is whatever cron/schedule invokes this
# script; the script itself is cadence-agnostic.  rclone cloud backup is NOT
# enabled here — that is a must-ask-back (QUANT-13 §5).
#
# Verification: file-count comparison between source and destination after
# rsync.  Checksum sampling is deliberately not done (§5 decision 10) — rsync
# already compares size + mtime per file, and the backup target is a
# rebuildable research system (RPO 1 month / RTO 4 hours).
#
# Usage:
#   scripts/backup.sh <data_platform_dir> <backup_dest_dir>
#   DATA_PLATFORM=... BACKUP_DEST=... scripts/backup.sh
#
# Excludes *.tmp / *.lock — write residue is not backup content.
set -euo pipefail

SRC="${1:-${DATA_PLATFORM:-}}"
DEST="${2:-${BACKUP_DEST:-}}"

if [[ -z "${SRC}" || -z "${DEST}" ]]; then
    echo "usage: scripts/backup.sh <data_platform_dir> <backup_dest_dir>" >&2
    echo "   or: DATA_PLATFORM=... BACKUP_DEST=... scripts/backup.sh" >&2
    exit 2
fi

if [[ ! -d "${SRC}" ]]; then
    echo "backup.sh: source is not a directory: ${SRC}" >&2
    exit 2
fi

mkdir -p "${DEST}"

echo "backup.sh: rsync ${SRC}/ -> ${DEST}/"
rsync -a --delete \
    --exclude='*.tmp' \
    --exclude='*.lock' \
    "${SRC}/" "${DEST}/"

# File-count comparison (§51).  Counts every regular file on each side.
src_count="$(find "${SRC}" -type f ! -name '*.tmp' ! -name '*.lock' | wc -l | tr -d ' ')"
dest_count="$(find "${DEST}" -type f ! -name '*.tmp' ! -name '*.lock' | wc -l | tr -d ' ')"

echo "backup.sh: source files=${src_count} dest files=${dest_count}"
if [[ "${src_count}" != "${dest_count}" ]]; then
    echo "backup.sh: FAIL — file count mismatch (source=${src_count} dest=${dest_count})" >&2
    exit 1
fi

echo "backup.sh: OK — counts match"
