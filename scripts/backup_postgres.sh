#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${ENV_FILE:-$REPO_ROOT/.env}"
BACKUP_DIR="${BACKUP_DIR:-$HOME/ootils-backups/postgres}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

compose_cmd() {
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    printf '%s\n' "docker compose"
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    printf '%s\n' "docker-compose"
    return 0
  fi
  echo "Neither 'docker compose' nor 'docker-compose' is available" >&2
  exit 1
}

require_cmd date
require_cmd find
require_cmd sha256sum

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

: "${POSTGRES_USER:?POSTGRES_USER must be set in $ENV_FILE}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set in $ENV_FILE}"
: "${POSTGRES_DB:?POSTGRES_DB must be set in $ENV_FILE}"

mkdir -p "$BACKUP_DIR"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
host_name="$(hostname -s)"
base_name="ootils-postgres-${host_name}-${timestamp}"
tmp_file="$BACKUP_DIR/${base_name}.dump.tmp"
final_file="$BACKUP_DIR/${base_name}.dump"

compose="$(compose_cmd)"

if [[ "$compose" == "docker compose" ]]; then
  docker compose -f "$REPO_ROOT/docker-compose.yml" exec -T postgres \
    sh -lc 'export PGPASSWORD="$POSTGRES_PASSWORD"; pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' \
    > "$tmp_file"
else
  docker-compose -f "$REPO_ROOT/docker-compose.yml" exec -T postgres \
    sh -lc 'export PGPASSWORD="$POSTGRES_PASSWORD"; pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' \
    > "$tmp_file"
fi

mv "$tmp_file" "$final_file"
sha256sum "$final_file" > "$final_file.sha256"

find "$BACKUP_DIR" -type f \( -name 'ootils-postgres-*.dump' -o -name 'ootils-postgres-*.dump.sha256' \) -mtime +"$RETENTION_DAYS" -delete

echo "Backup written: $final_file"
