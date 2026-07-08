#!/usr/bin/env bash
#
# restore_postgres.sh — prove a backup is restorable (#192).
#
# "A backup that was never restored is not a backup." This script takes a dump
# produced by scripts/backup_postgres.sh (pg_dump -Fc custom format, optionally
# gzipped) and restores it into a DISPOSABLE database (default
# ootils_restore_test) that is DROPPED at the end. It then verifies the restore
# is non-empty: a public-schema table count and a SELECT on schema_migrations.
#
# It runs entirely against the docker-compose 'postgres' service, exactly like
# the backup script, so it needs no host-side Postgres client. It NEVER touches
# the live database (POSTGRES_DB) — it refuses if RESTORE_DB collides with it.
#
# Usage:
#   scripts/restore_postgres.sh [DUMP_FILE] [--keep]
#
#   DUMP_FILE   Path to a .dump (or .dump.gz) file. Defaults to the most recent
#               ootils-postgres-*.dump under BACKUP_DIR.
#   --keep      Do not drop the disposable database after verification (leave it
#               for inspection). Default: drop it.
#
# Environment:
#   ENV_FILE     Path to the compose env file (default: <repo>/.env).
#   BACKUP_DIR   Where dumps live (default: ~/ootils-backups/postgres).
#   RESTORE_DB   Disposable database name (default: ootils_restore_test).
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${ENV_FILE:-$REPO_ROOT/.env}"
BACKUP_DIR="${BACKUP_DIR:-$HOME/ootils-backups/postgres}"
RESTORE_DB="${RESTORE_DB:-ootils_restore_test}"

KEEP=0
DUMP_FILE=""
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1 ;;
    -*) echo "Unknown option: $arg" >&2; exit 2 ;;
    *) DUMP_FILE="$arg" ;;
  esac
done

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

require_cmd find

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

if [[ "$RESTORE_DB" == "$POSTGRES_DB" ]]; then
  echo "Refusing to restore over the live database ($POSTGRES_DB). Set RESTORE_DB to a disposable name." >&2
  exit 1
fi

# Pick the most recent dump if none was given.
if [[ -z "$DUMP_FILE" ]]; then
  DUMP_FILE="$(find "$BACKUP_DIR" -type f -name 'ootils-postgres-*.dump' -print0 2>/dev/null \
    | xargs -0 ls -1t 2>/dev/null | head -n1 || true)"
  if [[ -z "$DUMP_FILE" ]]; then
    echo "No dump file found under $BACKUP_DIR and none provided." >&2
    exit 1
  fi
  echo "Using most recent dump: $DUMP_FILE"
fi

if [[ ! -f "$DUMP_FILE" ]]; then
  echo "Dump file not found: $DUMP_FILE" >&2
  exit 1
fi

compose="$(compose_cmd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.yml"

# Helper: run a command inside the postgres service with PGPASSWORD exported.
pg_exec() {
  $compose -f "$COMPOSE_FILE" exec -T postgres \
    sh -lc "export PGPASSWORD=\"\$POSTGRES_PASSWORD\"; $1"
}

cleanup() {
  if [[ "$KEEP" -eq 1 ]]; then
    echo "Keeping disposable database: $RESTORE_DB (--keep)"
    return
  fi
  echo "Dropping disposable database: $RESTORE_DB"
  pg_exec "dropdb -U \"\$POSTGRES_USER\" --if-exists \"$RESTORE_DB\"" || \
    echo "Warning: failed to drop $RESTORE_DB — drop it manually." >&2
}
trap cleanup EXIT

echo "Preparing disposable database: $RESTORE_DB"
pg_exec "dropdb -U \"\$POSTGRES_USER\" --if-exists \"$RESTORE_DB\""
pg_exec "createdb -U \"\$POSTGRES_USER\" \"$RESTORE_DB\""

echo "Restoring dump into $RESTORE_DB ..."
# pg_dump -Fc dumps are read from stdin by pg_restore (custom format). A .gz
# archive is gunzipped on the host first, then streamed in. --no-owner /
# --no-privileges keep the restore independent of the source role grants;
# --exit-on-error makes a genuine restore failure fail this proof loudly.
RESTORE_CMD="pg_restore -U \"\$POSTGRES_USER\" -d \"$RESTORE_DB\" --no-owner --no-privileges --exit-on-error"
case "$DUMP_FILE" in
  *.gz)
    require_cmd gunzip
    gunzip -c "$DUMP_FILE" | pg_exec "$RESTORE_CMD"
    ;;
  *)
    pg_exec "$RESTORE_CMD" < "$DUMP_FILE"
    ;;
esac

echo "Verifying restore ..."
table_count="$(pg_exec "psql -U \"\$POSTGRES_USER\" -d \"$RESTORE_DB\" -tAc \"SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'\"" | tr -d '[:space:]')"
migration_count="$(pg_exec "psql -U \"\$POSTGRES_USER\" -d \"$RESTORE_DB\" -tAc 'SELECT count(*) FROM schema_migrations'" | tr -d '[:space:]')"

echo "  public tables restored : ${table_count:-0}"
echo "  schema_migrations rows : ${migration_count:-0}"

if [[ -z "$table_count" || "$table_count" -eq 0 ]]; then
  echo "RESTORE FAILED: no tables in the restored database." >&2
  exit 1
fi
if [[ -z "$migration_count" || "$migration_count" -eq 0 ]]; then
  echo "RESTORE FAILED: schema_migrations is empty or missing — restore is not trustworthy." >&2
  exit 1
fi

echo "RESTORE OK: $DUMP_FILE restores cleanly into $RESTORE_DB ($table_count tables, $migration_count migrations)."
