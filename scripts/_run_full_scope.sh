#!/bin/bash
# Full scope PI bootstrap + wireup + compute on VM.
# Output redirected to /tmp/full_scope.log
set -e
LOG=/tmp/full_scope.log
echo "=== START $(date) ===" > $LOG

PGCMD="docker exec ootils-core-postgres-1 psql -U ootils -d ootils_pilote_test -c"

# 1. Wipe in correct order
echo "[1/5] Wipe shortages..." | tee -a $LOG
$PGCMD "TRUNCATE shortages;" 2>&1 | tee -a $LOG

echo "[2/5] Wipe dirty_nodes + edges..." | tee -a $LOG
$PGCMD "TRUNCATE dirty_nodes;" 2>&1 | tee -a $LOG
$PGCMD "TRUNCATE edges;" 2>&1 | tee -a $LOG

echo "[3/5] Wipe PI nodes + projection_series..." | tee -a $LOG
$PGCMD "DELETE FROM nodes WHERE node_type='ProjectedInventory' AND scenario_id='00000000-0000-0000-0000-000000000001';" 2>&1 | tee -a $LOG
$PGCMD "DELETE FROM projection_series WHERE scenario_id='00000000-0000-0000-0000-000000000001';" 2>&1 | tee -a $LOG

echo "Wipe state: $(date)" | tee -a $LOG
$PGCMD "SELECT 'pi='||COUNT(*) FROM nodes WHERE node_type='ProjectedInventory';" 2>&1 | tee -a $LOG
$PGCMD "SELECT 'series='||COUNT(*) FROM projection_series;" 2>&1 | tee -a $LOG

echo "[4/5] Bootstrap PI full scope (23K series × 540 days)..." | tee -a $LOG
docker exec -e DATABASE_URL=postgresql://ootils:ootils@postgres:5432/ootils_pilote_test ootils-core-api-1 python -u /tmp/ootils/scripts/bootstrap_pi.py --horizon 540 2>&1 | tee -a $LOG

echo "[4b] Fix wireup..." | tee -a $LOG
docker exec -e DATABASE_URL=postgresql://ootils:ootils@postgres:5432/ootils_pilote_test ootils-core-api-1 python -u /tmp/ootils/scripts/fix_wireup.py 2>&1 | tee -a $LOG

echo "[5/5] Compute PI SQL..." | tee -a $LOG
docker exec -e DATABASE_URL=postgresql://ootils:ootils@postgres:5432/ootils_pilote_test ootils-core-api-1 python -u /tmp/ootils/scripts/compute_pi_sql.py 2>&1 | tee -a $LOG

echo "=== END $(date) ===" | tee -a $LOG
