#!/usr/bin/env bash
# configure_node.sh — Configure LilyGo T-SIM7080G-S3 as meshnarc capture node
#
# Connect the LilyGo via USB, then run this script.
# Requires: pip install meshtastic
#
# Usage: ./configure_node.sh <mqtt_broker> <mqtt_user> <mqtt_pass> [lat] [lon]

set -euo pipefail

BROKER="${1:?Usage: $0 <broker> <user> <pass> [lat] [lon]}"
USER="${2:?}"
PASS="${3:?}"
LAT="${4:-0.0000}"
LON="${5:-0.0000}"
ALT="${6:-0}"

echo "=== meshnarc node configuration ==="
echo "Broker: ${BROKER}"
echo "Position: ${LAT}, ${LON}"

# Ghost mode — receive only, never rebroadcast
echo "--- Setting role: CLIENT_MUTE (stealth) ---"
meshtastic --set lora.role CLIENT_MUTE

# Always powered (no sleep)
echo "--- Power: always on ---"
meshtastic --set power.is_always_powered true

# MQTT gateway config
echo "--- MQTT gateway ---"
meshtastic --set mqtt.enabled true
meshtastic --set mqtt.address "${BROKER}"
meshtastic --set mqtt.username "${USER}"
meshtastic --set mqtt.password "${PASS}"
meshtastic --set mqtt.encryption_enabled false
meshtastic --set mqtt.json_enabled false
meshtastic --set mqtt.tls_enabled false

# Default channel — enable uplink with the well-known default key
echo "--- Channel 0: LongFast (default key, uplink enabled) ---"
meshtastic --ch-set uplink_enabled true --ch-index 0
meshtastic --ch-set downlink_enabled false --ch-index 0

# SIM7080G cellular APN for Hologram
echo "--- Cellular APN ---"
meshtastic --set network.apn_name "hologram"

# Fixed position
echo "--- Fixed position ---"
meshtastic --setlat "${LAT}" --setlon "${LON}" --setalt "${ALT}"

# Node name (optional but useful for identifying in MQTT topics)
echo "--- Node identity ---"
meshtastic --set-owner "meshnarc"
meshtastic --set-owner-short "NARC"

echo ""
echo "=== Configuration complete ==="
echo ""
echo "Verify with: meshtastic --info"
echo ""
echo "MQTT topics will appear as:"
echo "  msh/US/2/e/LongFast/!<your_node_id>"
echo ""
echo "To add more channels to capture:"
echo "  meshtastic --ch-add 'ChannelName'"
echo "  meshtastic --ch-set psk 'base64key==' --ch-index N"
echo "  meshtastic --ch-set uplink_enabled true --ch-index N"
