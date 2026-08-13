#!/usr/bin/env bash
# Source before running omega:  source omega-env.sh
#
# Fill these from the device's MQTT connection details in the Golain console.

# The device NAME as registered — NOT its UUID. Using the UUID is the single most
# common cause of "connects fine, nothing ever ingests".
export OMEGA_DEVICE_ID="canebot-pi-01"

export OMEGA_MQTT_BROKER_URL="ssl://<broker-host>:8883"    # quic://…:8884 if the cert is ECDSA
export OMEGA_ROOT_TOPIC="<topic_slug>/canebot-pi-01"

export OMEGA_TLS_CA_PATH="$(dirname "${BASH_SOURCE[0]}")/certs/ca.crt"
export OMEGA_TLS_CERT_PATH="$(dirname "${BASH_SOURCE[0]}")/certs/device.crt"
export OMEGA_TLS_KEY_PATH="$(dirname "${BASH_SOURCE[0]}")/certs/device.key"

# What omega replicates: the PROJECTOR'S OUTPUT, never the machine's own database.
export OMEGA_SOURCE_DB_PATH="/home/pi/projector/canebot_replica.db"

# omega's own cursors. Separate from the projector's projector_state.db.
export OMEGA_STATE_DB_PATH="/home/pi/projector/omega_state.db"
