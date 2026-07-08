#!/usr/bin/env bash
#
# backup_offhost.sh — copy the local Postgres backups off the host (#192).
#
# A local dump on the same VM as the database does not survive a loss of that
# VM. This script mirrors the backup directory to a REMOTE destination via
# rsync, closing the "copie hors machine reste a faire" gap in INFRA-RUNBOOK.
#
# The destination is provided by the pilot (an rsync/ssh target such as
# user@host:/srv/ootils-backups, or any rsync-writable path) through
# OOTILS_BACKUP_REMOTE — there is no default: the script refuses to run without
# it rather than silently copying nowhere.
#
# Usage:
#   OOTILS_BACKUP_REMOTE=user@host:/srv/ootils-backups scripts/backup_offhost.sh
#
# Environment:
#   OOTILS_BACKUP_REMOTE  REQUIRED. rsync destination (ssh target or path).
#   BACKUP_DIR            Local backup dir (default: ~/ootils-backups/postgres).
#   OOTILS_BACKUP_SSH_OPTS Optional extra ssh options, e.g. "-i ~/.ssh/backup_key -p 2222".
#   RSYNC_EXTRA_OPTS      Optional extra rsync flags.
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
BACKUP_DIR="${BACKUP_DIR:-$HOME/ootils-backups/postgres}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

require_cmd rsync

: "${OOTILS_BACKUP_REMOTE:?OOTILS_BACKUP_REMOTE must be set to the off-host rsync destination, e.g. user@host:/srv/ootils-backups}"

if [[ ! -d "$BACKUP_DIR" ]]; then
  echo "Local backup dir does not exist: $BACKUP_DIR — run scripts/backup_postgres.sh first." >&2
  exit 1
fi

# Build rsync args. -a archive, -z compress, --partial resume, --stats summary.
# A trailing slash on the source mirrors the CONTENTS of BACKUP_DIR into the
# destination (not a nested BACKUP_DIR/ subdir).
rsync_args=(-az --partial --human-readable --stats)

# When the destination is an ssh target, allow custom ssh options (key, port).
if [[ -n "${OOTILS_BACKUP_SSH_OPTS:-}" ]]; then
  require_cmd ssh
  rsync_args+=(-e "ssh ${OOTILS_BACKUP_SSH_OPTS}")
fi

if [[ -n "${RSYNC_EXTRA_OPTS:-}" ]]; then
  # shellcheck disable=SC2206 — intentional word-splitting of caller-provided flags
  rsync_args+=(${RSYNC_EXTRA_OPTS})
fi

echo "Mirroring $BACKUP_DIR/ -> $OOTILS_BACKUP_REMOTE"
rsync "${rsync_args[@]}" "$BACKUP_DIR/" "$OOTILS_BACKUP_REMOTE"
echo "Off-host copy complete."
