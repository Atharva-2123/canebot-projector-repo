#!/usr/bin/env bash
# Start, stop and inspect both services.
#
#   ./linux/manage-services.sh status|start|stop|restart|logs|check
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICES=(canebot-projector canebot-omega)
REPLICA="$ROOT/projector/canebot_replica.db"

case "${1:-status}" in
  start)   sudo systemctl start   "${SERVICES[@]}" ;;
  stop)    sudo systemctl stop    "${SERVICES[@]}" ;;
  restart) sudo systemctl restart "${SERVICES[@]}" ;;

  status)
    for s in "${SERVICES[@]}"; do
      printf '%-20s %s\n' "$s" "$(systemctl is-active "$s" 2>/dev/null || echo inactive)"
    done
    ;;

  logs)
    journalctl -u canebot-projector -u canebot-omega -f
    ;;

  check)
    echo "== replica contents =="
    for t in cycles step_dwells step_actuators state_durations fault_events \
             door_events actuator_intervals sensor_toggles gaps; do
      printf '%-22s %s\n' "$t" "$(sqlite3 "$REPLICA" "SELECT COUNT(*) FROM $t;" 2>/dev/null || echo '-')"
    done
    echo
    echo "== cycles by outcome =="
    sqlite3 -header -column "$REPLICA" \
      "SELECT result, COUNT(*) AS n, SUM(duration_ms)/1000 AS secs FROM cycles GROUP BY result;"
    echo
    echo "== integrity =="
    if [[ -z "$(sqlite3 "$REPLICA" 'PRAGMA foreign_key_check;')" ]]; then
      echo "foreign keys: clean"
    else
      echo "foreign keys: VIOLATIONS PRESENT"
    fi
    ;;

  *) echo "usage: $0 {status|start|stop|restart|logs|check}" >&2; exit 1 ;;
esac
