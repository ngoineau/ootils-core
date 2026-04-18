#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_SCRIPT="${BACKUP_SCRIPT:-$SCRIPT_DIR/backup_postgres.sh}"
BACKUP_DIR="${BACKUP_DIR:-$HOME/ootils-backups/postgres}"
BACKUP_LOG="${BACKUP_LOG:-$BACKUP_DIR/backup.log}"
BACKUP_CRON_SCHEDULE="${BACKUP_CRON_SCHEDULE:-15 2 * * *}"

mkdir -p "$BACKUP_DIR"
touch "$BACKUP_LOG"

existing_crontab="$(crontab -l 2>/dev/null || true)"
entry="$BACKUP_CRON_SCHEDULE BACKUP_DIR=$BACKUP_DIR $BACKUP_SCRIPT >> $BACKUP_LOG 2>&1"

if printf '%s\n' "$existing_crontab" | grep -F "$BACKUP_SCRIPT" >/dev/null 2>&1; then
  echo "Cron entry already present"
  printf '%s\n' "$entry"
  exit 0
fi

{
  printf '%s\n' "$existing_crontab"
  printf '%s\n' "$entry"
} | crontab -

echo "Installed cron entry:"
printf '%s\n' "$entry"
