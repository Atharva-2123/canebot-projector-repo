#!/usr/bin/env bash
# Copy to omega.env on the device and fill in the two TODOs, then:
#   sudo systemctl start canebot-omega
#
# omega.env is gitignored — it is per-device.

# ── Device identity ──────────────────────────────────────────────────────────
# Must be the device NAME exactly as registered (the console calls it Client ID),
# not its UUID. The broker resolves tenancy from the certificate and matches the
# topic ACL against this — a UUID here connects fine and then ingests nothing.
export OMEGA_DEVICE_ID="CaneBot Vending Machine 1"

# ── Broker ───────────────────────────────────────────────────────────────────
# TLS is the safe default. QUIC is also enabled on this device
# (quic://mqtt.ilyama.golain.io:14567) and gives faster reconnects and no
# head-of-line blocking across lineages — worth trying once TLS is proven,
# since we have 17 lineages sharing the link.
export OMEGA_MQTT_BROKER_URL="ssl://mqtt.ilyama.golain.io:8883"

# ── Topic prefix ─────────────────────────────────────────────────────────────
# TODO: {topic_slug}/{device_name} exactly as provisioned. Find it in the console
# under the device's MQTT details, or via
#   GET /projects/{project_id}/devices/{device_id}/mqtt_connection_details
# A wrong value connects but every publish is rejected by the ACL.
export OMEGA_ROOT_TOPIC="<topic_slug>/CaneBot Vending Machine 1"

# ── Certificates ─────────────────────────────────────────────────────────────
# TODO: confirm these filenames match what the console issued.
_CERTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
export OMEGA_TLS_CA_PATH="${_CERTS}/ca.crt"
export OMEGA_TLS_CERT_PATH="${_CERTS}/device.crt"
export OMEGA_TLS_KEY_PATH="${_CERTS}/device.key"

# ── Databases ────────────────────────────────────────────────────────────────
# omega replicates the PROJECTOR'S OUTPUT, never the machine's own config.db.
export OMEGA_SOURCE_DB_PATH="/home/pi/projector/canebot-projector-repo/projector/canebot_replica.db"

# omega's own cursors — separate from the projector's projector_state.db.
export OMEGA_STATE_DB_PATH="/home/pi/projector/canebot-projector-repo/omega/state.db"
