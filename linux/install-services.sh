#!/usr/bin/env bash
# Install the CaneBot projector and omega as systemd services.
#
#   sudo ./linux/install-services.sh
#
# Idempotent: re-run after a git pull to pick up new binaries.
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Run with sudo: sudo $0" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_USER="${SUDO_USER:-pi}"

# The controller's database. Read-only — the projector never writes to it.
SOURCE_DB="${SOURCE_DB:-/home/${RUN_USER}/CaneBot_FSM_go/config.db}"

if [[ ! -f "$SOURCE_DB" ]]; then
  echo "Source database not found: $SOURCE_DB" >&2
  echo "Set it explicitly:  SOURCE_DB=/path/to/config.db sudo -E $0" >&2
  exit 1
fi

echo "root        $ROOT"
echo "user        $RUN_USER"
echo "source      $SOURCE_DB"

chmod +x "$ROOT/projector/projector" "$ROOT/omega/omega"

install -m 644 /dev/stdin /etc/systemd/system/canebot-projector.service <<EOF
[Unit]
Description=CaneBot projector — projects the controller's SQLite into the replica
After=network.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${ROOT}/projector
ExecStart=${ROOT}/projector/projector \\
  -source ${SOURCE_DB} \\
  -replica ${ROOT}/projector/canebot_replica.db \\
  -state ${ROOT}/projector/projector_state.db \\
  -interval 5s -batch 2000 -retention 336h
Restart=always
RestartSec=10s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=canebot-projector

[Install]
WantedBy=multi-user.target
EOF

install -m 644 /dev/stdin /etc/systemd/system/canebot-omega.service <<EOF
[Unit]
Description=CaneBot omega — replicates the projector's output to ilyama
After=network-online.target canebot-projector.service
# omega has nothing to ship if the projector is not producing, so tie them together.
Wants=network-online.target
BindsTo=canebot-projector.service

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${ROOT}/omega
EnvironmentFile=${ROOT}/omega/omega.env
ExecStart=${ROOT}/omega/omega -client ${ROOT}/omega/omega-config.yaml
Restart=always
RestartSec=15s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=canebot-omega

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable canebot-projector canebot-omega

echo
echo "Installed. The projector can start now:"
echo "  sudo systemctl start canebot-projector"
echo
echo "omega needs certs in omega/certs/ and omega/omega.env filled in first:"
echo "  cp ${ROOT}/omega/omega.env.example ${ROOT}/omega/omega.env"
echo "  sudo systemctl start canebot-omega"
