#!/usr/bin/env bash
# configure_node_orbic.sh — Configure LilyGo T-SIM7080G-S3 as a meshnarc
# capture node that uplinks over WiFi to an orbic-fusion broker running on
# a co-located Orbic RC400L, instead of this node's own cellular modem.
#
# Use this variant only when the node will physically travel with the
# Orbic (same vehicle/bag/fixed post) -- for a standalone/remote deployment
# with no Orbic nearby, use configure_node.sh's default Hologram-cellular
# path instead. This node's SIM7080G modem is left unconfigured here, not
# disabled outright; nothing stops running both configs on different nodes.
#
# Connect the LilyGo via USB, then run this script.
# Requires: pip install meshtastic
#
# Usage: ./configure_node_orbic.sh <orbic_wifi_ssid> <orbic_wifi_psk> [orbic_ip] [lat] [lon]

set -euo pipefail

WIFI_SSID="${1:?Usage: $0 <orbic_wifi_ssid> <orbic_wifi_psk> [orbic_ip] [lat] [lon]}"
WIFI_PSK="${2:?}"
ORBIC_IP="${3:-192.168.1.1}"
LAT="${4:-0.0000}"
LON="${5:-0.0000}"
ALT="${6:-0}"

echo "=== meshnarc node configuration (orbic-fusion, WiFi uplink) ==="
echo "Orbic AP: ${WIFI_SSID}"
echo "Broker:   ${ORBIC_IP}:1883"
echo "Position: ${LAT}, ${LON}"

# Ghost mode — receive only, never rebroadcast
echo "--- Setting role: CLIENT_MUTE (stealth) ---"
meshtastic --set lora.role CLIENT_MUTE

# Always powered (no sleep)
echo "--- Power: always on ---"
meshtastic --set power.is_always_powered true

# WiFi to the Orbic's own AP, instead of this node's cellular modem
echo "--- WiFi uplink (Orbic AP) ---"
meshtastic --set network.wifi_enabled true
meshtastic --set network.wifi_ssid "${WIFI_SSID}"
meshtastic --set network.wifi_psk "${WIFI_PSK}"

# MQTT gateway config — orbic-fusion's broker has no auth (trust boundary
# is "reachable on the Orbic's own WiFi AP", same posture as its other
# services); leave username/password empty rather than inventing creds
# orbic-fusion doesn't check.
echo "--- MQTT gateway (orbic-fusion) ---"
meshtastic --set mqtt.enabled true
meshtastic --set mqtt.address "${ORBIC_IP}:1883"
meshtastic --set mqtt.username ""
meshtastic --set mqtt.password ""
meshtastic --set mqtt.encryption_enabled false
meshtastic --set mqtt.json_enabled false
meshtastic --set mqtt.tls_enabled false

# Default channel — enable uplink with the well-known default key
echo "--- Channel 0: LongFast (default key, uplink enabled) ---"
meshtastic --ch-set uplink_enabled true --ch-index 0
meshtastic --ch-set downlink_enabled false --ch-index 0

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
echo "Raw Meshtastic ServiceEnvelope protobufs will arrive at orbic-fusion as:"
echo "  topic msh/US/2/e/LongFast/!<your_node_id>"
echo "orbic-fusion doesn't decode them (kind: \"raw\", base64 payload) --"
echo "decoding stays meshnarc_sub.py's job; point it at the same broker if you"
echo "want BQ ingestion to keep working when running this variant."
echo ""
echo "To add more channels to capture:"
echo "  meshtastic --ch-add 'ChannelName'"
echo "  meshtastic --ch-set psk 'base64key==' --ch-index N"
echo "  meshtastic --ch-set uplink_enabled true --ch-index N"
