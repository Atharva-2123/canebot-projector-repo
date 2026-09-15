#!/system/bin/sh
# Drop projector replica rows older than 2 days.
#
# Touches ONLY canebot_replica.db. Does not open config.db or omega state.
# Safe to run while projector/omega are up (SQLite busy timeout).
#
# This Pi has no cron. Projector already prunes once an hour when started
# with -retention 48h. Use this script for a one-shot prune, or call it
# from cron on a Linux host:
#   0 * * * * /path/to/prune-projector-replica.sh
#
# Usage:
#   prune-projector-replica.sh              # 2 days (default)
#   prune-projector-replica.sh 2            # same
#   DAYS=2 prune-projector-replica.sh

set -e

DAYS="${1:-${DAYS:-2}}"
REPLICA="${REPLICA:-/data/local/canebot-projector-repo/projector/canebot_replica.db}"
SQLITE="${SQLITE:-/system/bin/sqlite3}"
LOG="${LOG:-/data/local/canebot/logs/prune-projector-replica.log}"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') $*"
}

if [ ! -x "$SQLITE" ]; then
  echo "sqlite3 not found: $SQLITE" >&2
  exit 1
fi
if [ ! -f "$REPLICA" ]; then
  echo "replica not found: $REPLICA" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOG")"
exec >>"$LOG" 2>&1

# Do not run other statements in this query: sqlite3 prints PRAGMA results too,
# and a numeric prefix would make every ISO event_ts look "older" and wipe the DB.
CUTOFF=$("$SQLITE" "$REPLICA" "SELECT strftime('%Y-%m-%dT%H:%M:%S.000000000Z','now','-$DAYS days');")
if [ -z "$CUTOFF" ]; then
  log "ERROR: empty cutoff; aborting"
  exit 1
fi
case "$CUTOFF" in
  20[0-9][0-9]-[0-1][0-9]-[0-3][0-9]T*) ;;
  *)
    log "ERROR: cutoff is not an ISO timestamp: $CUTOFF"
    exit 1
    ;;
esac
log "pruning $REPLICA older than $DAYS days (cutoff=$CUTOFF)"

"$SQLITE" "$REPLICA" <<EOF
PRAGMA busy_timeout=30000;
BEGIN;
DELETE FROM sensor_toggles     WHERE event_ts < '$CUTOFF';
DELETE FROM config_history     WHERE event_ts < '$CUTOFF';
DELETE FROM state_durations    WHERE event_ts < '$CUTOFF';
DELETE FROM step_dwells        WHERE event_ts < '$CUTOFF';
DELETE FROM fault_events       WHERE event_ts < '$CUTOFF';
DELETE FROM actuator_intervals WHERE event_ts < '$CUTOFF';
DELETE FROM cip_runs           WHERE event_ts < '$CUTOFF';
DELETE FROM door_events        WHERE event_ts < '$CUTOFF';
DELETE FROM rollups            WHERE event_ts < '$CUTOFF';
DELETE FROM gaps               WHERE event_ts < '$CUTOFF';
DELETE FROM cycles             WHERE event_ts < '$CUTOFF';
COMMIT;
EOF

log "done"
