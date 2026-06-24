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
