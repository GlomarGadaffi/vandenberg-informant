# meshtap

passive Meshtastic packet capture to BigQuery. hardware uplinks RF packets over Hologram cellular, subscriber decodes and streams to GCP. designed for long-term RF telemetry archival without node-level storage constraints.

## architecture

**hardware**: LilyGo T-SIM7080G-S3 in CLIENT_MUTE mode (RX-only, no TX)
- receives all Meshtastic mesh traffic on LoRa band
- forwards raw ServiceEnvelope protobufs to MQTT broker over Hologram cellular (LTE-M)
- no local storage, no WiFi required

**subscriber** (meshnarc_sub.py): any internet-connected host
- subscribes to MQTT gateway topics
- decodes Meshtastic ServiceEnvelope messages
- decompresses and validates
- streams structured records to BigQuery via Google Cloud client

**schema** (bq_schema.sql): normalized tables for packets, mesh events, node metadata
- original node ID, timestamp, signal strength, position, message type
- extensible for custom payload types

## deployment

**hardware setup**:
```bash
# On the T-SIM7080G-S3
# 1. flash Meshtastic firmware with MQTT uplink config
# 2. configure Hologram SIM and APN
# 3. set CLIENT_MUTE in firmware config (RX only)
# 4. point MQTT broker to your bridge
```

`configure_node.sh <broker> <user> <pass> [lat] [lon]` does the above over
the node's own Hologram cellular link — the default, for a standalone/
remote deployment with no other network nearby.

**co-located with an Orbic RC400L?** Use `configure_node_orbic.sh
<orbic_wifi_ssid> <orbic_wifi_psk> [orbic_ip] [lat] [lon]` instead: the
T-SIM7080G-S3's own WiFi radio joins the Orbic's AP and publishes to its
[orbic-fusion](https://github.com/GlomarGadaffi/orbic-fusion) broker
directly, one fewer cellular link/SIM in play than running this node's
modem alongside the Orbic's. orbic-fusion doesn't decode Meshtastic
protobufs (`kind: "raw"`, base64 payload) — point `meshnarc_sub.py` at the
same broker if BigQuery ingestion needs to keep working under this
variant. No firmware/subscriber code changes either way, just which
config script you run.

**subscriber on GCP**:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceaccount.json
python meshnarc_sub.py \
    --broker mqtt.example.com \
    --username meshnarc \
    --password secret \
    --topic "msh/US/2/e/LongFast/#" \
    --bq-dataset mesh_logs
```

**deployment scripts**: `deploy_gcp.sh` (Cloud Run Job) or `deploy.sh` (Compute Engine)

## use cases

- **network audit**: passive visibility of all mesh traffic in range
- **forensics**: timestamped, signed packet archive for incident investigation
- **propagation study**: long-term signal strength and position logging for RF modeling

## notes

Meshtastic protobuf decoding via generated stubs; see `requirements.txt` for schema compiler.
