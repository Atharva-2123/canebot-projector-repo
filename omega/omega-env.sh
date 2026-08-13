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
# {topic_slug}/{device_name}. A wrong value connects but every publish is
# rejected by the broker ACL — healthy-looking, ingests nothing.
# Note the spaces in the device name: quote this everywhere it is used.
export OMEGA_ROOT_TOPIC="6541_b195/CaneBot Vending Machine 1"

# ── Certificates ─────────────────────────────────────────────────────────────
# Rename the console bundle to these three names (see certs/README.md).
_CERTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
export OMEGA_TLS_CA_PATH="${_CERTS}/ca.crt"
export OMEGA_TLS_CERT_PATH="${_CERTS}/device.crt"
export OMEGA_TLS_KEY_PATH="${_CERTS}/device.key"

# ── Databases ────────────────────────────────────────────────────────────────
# omega replicates the PROJECTOR'S OUTPUT, never the machine's own config.db.
export OMEGA_SOURCE_DB_PATH="/home/pi/projector/canebot-projector-repo/projector/canebot_replica.db"

# omega's own cursors — separate from the projector's projector_state.db.
export OMEGA_STATE_DB_PATH="/home/pi/projector/canebot-projector-repo/omega/state.db"

# ── For reference (not read by omega) ────────────────────────────────────────
# Device UUID   5d7f00b1-b4fb-459b-910a-2eb72163232d   <- dashboards filter on THIS
# Fleet ID      49433628-ec6a-439d-80a6-b9bbd4980bce
# Project ID    a3aefb95-81de-4414-a415-635c88a9b195
# Cloud tables  edge_ts_pa3aefb95_<table>
