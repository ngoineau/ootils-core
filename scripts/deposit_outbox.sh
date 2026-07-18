#!/usr/bin/env bash
#
# deposit_outbox.sh — deposit the local outbox (daily reports, outbound
# export TSVs, ...) into the team's Dropbox (ADR-042 PR-4c/PR-5, "tous les
# echanges equipe ERP via la Dropbox", pilot rule 2026-07-17 — see the
# ERP-canal-Dropbox memory note).
#
# A pure, dumb mirror step: copies ONLY the known outbox artifact files
# (daily_report_*.md from scripts/run_daily_ingest.py --apply's report
# phase; po_drafts_*.tsv/reschedule_messages_*.tsv/transfers_*.tsv from its
# EXPORT phase, ADR-042 decision 4 / engine/reporting/outbound_export.py)
# from the local outbox directory to the configured Dropbox remote via
# `rclone copy` (one-way, source -> destination; never deletes anything on
# either side — `rclone copy`, deliberately NOT `rclone sync`). Mirrors the
# off-host backup pattern (scripts/backup_offhost.sh: a local artifact
# directory copied to a remote target by a thin, cron-friendly shell script,
# destination configured by environment, never hardcoded).
#
# NO SECRET IN THIS FILE. The Dropbox OAuth credential lives in rclone's OWN
# config (`rclone config` on the host, ~/.config/rclone/rclone.conf) under the
# remote name this script references (OOTILS_DROPBOX_REMOTE) — this script
# never reads, writes, or echoes that config.
#
# Usage:
#   scripts/deposit_outbox.sh
#   OOTILS_OUTBOX_DIR=/home/debian/outbox \
#   OOTILS_DROPBOX_REMOTE=dropbox:ootils-outbox \
#   scripts/deposit_outbox.sh
#
# Environment:
#   OOTILS_OUTBOX_DIR      Local outbox dir (default: /home/debian/outbox —
#                          matches scripts/run_daily_ingest.py's --outbox
#                          default).
#   OOTILS_DROPBOX_REMOTE  rclone remote:path destination (default:
#                          dropbox:ootils-outbox). The "dropbox:" prefix must
#                          already exist as a configured rclone remote
#                          (`rclone config`) on this host — this script does
#                          not create or validate that remote.
#   RCLONE_EXTRA_OPTS      Optional extra rclone flags (word-split as-is).
#
set -euo pipefail

OOTILS_OUTBOX_DIR="${OOTILS_OUTBOX_DIR:-/home/debian/outbox}"
OOTILS_DROPBOX_REMOTE="${OOTILS_DROPBOX_REMOTE:-dropbox:ootils-outbox}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

require_cmd rclone

if [[ ! -d "$OOTILS_OUTBOX_DIR" ]]; then
  echo "Local outbox dir does not exist: $OOTILS_OUTBOX_DIR — nothing to deposit." >&2
  exit 1
fi

rclone_args=(
  copy
  --include "daily_report_*.md"
  --include "po_drafts_*.tsv"
  --include "reschedule_messages_*.tsv"
  --include "transfers_*.tsv"
)

if [[ -n "${RCLONE_EXTRA_OPTS:-}" ]]; then
  # shellcheck disable=SC2206 — intentional word-splitting of caller-provided flags
  rclone_args+=(${RCLONE_EXTRA_OPTS})
fi

echo "Depositing $OOTILS_OUTBOX_DIR/{daily_report_*.md,po_drafts_*.tsv,reschedule_messages_*.tsv,transfers_*.tsv} -> $OOTILS_DROPBOX_REMOTE"
rclone "${rclone_args[@]}" "$OOTILS_OUTBOX_DIR" "$OOTILS_DROPBOX_REMOTE"
echo "Dropbox deposit complete."
