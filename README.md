# meshnarc

Passive SIGINT platform for LoRa mesh networks. Silently captures all Meshtastic RF traffic via a field-deployable cellular sensor node and warehouses decoded packets in BigQuery for retrospective analysis.

## What This Does

A `CLIENT_MUTE` Meshtastic node receives every packet on the air but never transmits — invisible to the mesh. All captured traffic is uplinked over cellular (Hologram IoT SIM) to an MQTT broker, where a Python subscriber decodes the protobufs and streams structured records into BigQuery.

The result is a queryable archive of every packet seen on the mesh: who transmitted, who they were talking to, what they said, where they were, and when.

### Analytical Capabilities

| Capability | How It Works |
|---|---|
| **Identity Resolution** | Node IDs are correlated with long/short names from `NODEINFO_APP` broadcasts. Over time, builds a complete roster of mesh participants. |
| **Social Graph Mapping** | Every packet records `from_id` → `to_id`. Query patterns reveal who talks to whom, frequency of communication, and group structures. |
| **Geospatial Tracking** | `POSITION_APP` packets yield GPS coordinates, altitude, ground speed, and satellite count. Builds movement track histories per node. |
| **Message Interception** | `TEXT_MESSAGE_APP` payloads are decoded to plaintext. The default Meshtastic encryption key (`AQ==` / `0x01`) is well-known — most users never change it. |
| **RF Fingerprinting** | RSSI and SNR per packet, combined with known capture-node position, enables distance estimation and signal propagation analysis. |
| **Multi-Protocol Fusion** | Schema includes `source_protocol` field. Meshtastic today, MeshCore tomorrow — same analytical pipeline. |

### Field Deployment Model

The capture node is a self-contained cellular dead-drop. No WiFi, no wired connection, no physical access needed after placement. Power it, place it, forget it. Hologram IoT backhaul at ~$1-2/month per node. Multiple sensors across a metro area create a regional mesh surveillance network.

## Architecture

```
┌─────────────────────────────────┐
│  LilyGo T-SIM7080G-S3          │
│  ┌───────────┐  ┌────────────┐ │
│  │ SX1262    │  │ SIM7080G   │ │
│  │ LoRa      │  │ Cat-M1/    │ │
│  │ (capture) │  │ NB-IoT     │ │
│  └─────┬─────┘  └──────┬─────┘ │
│        │  ESP32-S3      │       │
│        │  CLIENT_MUTE   │       │
│        │  MQTT Gateway  │       │
│        └───────┬────────┘       │
└────────────────┼────────────────┘
                 │ Hologram IoT
                 │ cellular
                 ▼
         ┌───────────────┐
         │  MQTT Broker  │
         │  (mosquitto   │
         │   or HiveMQ)  │
         └───────┬───────┘
                 │
                 ▼
    ┌────────────────────────┐
    │  meshnarc-subscriber   │
    │  (Python)              │
    │                        │
    │  • Protobuf decode     │
    │  • AES-256-CTR decrypt │
    │  • Identity correlation│
    │  • BQ streaming insert │
    └────────────┬───────────┘
                 │
                 ▼
         ┌───────────────┐
         │   BigQuery    │
         │  meshnarc.    │
         │  packets      │
         └───────┬───────┘
                 │
                 ▼
         ┌───────────────┐
         │  Analytical   │
         │  Views        │
         │  • recent_nodes│
         │  • messages   │
         │  • positions  │
         └───────────────┘
```

## Components

1. **LilyGo T-SIM7080G-S3** — Meshtastic firmware, `CLIENT_MUTE` role (receive-only, zero RF emissions)
2. **Hologram IoT SIM** — Cat-M1/NB-IoT cellular backhaul, ~$1-2/month
3. **MQTT Broker** — mosquitto (self-hosted) or HiveMQ Cloud (free tier)
4. **meshnarc-subscriber** — Python daemon: protobuf decode, AES decrypt, identity correlation, BQ ingest
5. **BigQuery** — `meshnarc.packets` table with analytical views

## Setup

### 1. Flash Meshtastic Firmware

Use the Meshtastic web flasher: https://flasher.meshtastic.org

- Board: **LilyGo T-SIM7080G-S3**
- Firmware: latest stable

### 2. Hologram SIM

- Get a Hologram IoT SIM: https://hologram.io
- Activate SIM, note ICCID
- Hologram uses auto-APN, but for SIM7080G you may need to set:
  - APN: `hologram` (Meshtastic config → Network → APN)

### 3. Configure Meshtastic Node

Via Meshtastic Python CLI, app, or web UI:

```bash
# Set node role to CLIENT_MUTE (receive-only, don't rebroadcast — stealth)
meshtastic --set lora.role CLIENT_MUTE

# Enable MQTT gateway — uplinks ALL received packets
meshtastic --set mqtt.enabled true
meshtastic --set mqtt.address "your-broker.example.com"
meshtastic --set mqtt.username "meshnarc"
meshtastic --set mqtt.password "your-password"

# Uplink enabled on default channel (index 0)
meshtastic --ch-set uplink_enabled true --ch-index 0

# Encryption key for default channel (the well-known default = AQ==)
# This lets you decode all traffic using the default key
meshtastic --ch-set psk "AQ==" --ch-index 0

# If you want to capture additional channels, add them:
# meshtastic --ch-add "SomeChannel"
# meshtastic --ch-set psk "base64key==" --ch-index 1
# meshtastic --ch-set uplink_enabled true --ch-index 1

# Set fixed position (your capture site)
meshtastic --setlat 0.0000 --setlon 0.0000 --setalt 0

# Power settings — keep alive
meshtastic --set power.is_always_powered true
```

#### Critical Settings Explained

- **CLIENT_MUTE**: The node receives everything but never retransmits.
  Your meshnarc is passive — it doesn't participate in the mesh routing.
  No one sees it rebroadcasting. Ghost mode.

- **mqtt.enabled + uplink_enabled**: Every packet the LoRa radio receives
  gets published to the MQTT broker over the SIM7080G cellular link.

- **Default PSK `AQ==`**: This is the well-known Meshtastic default key
  (0x01). All nodes on "LongFast" use this. Your node decrypts with the
  same key, then uplinks the plaintext to MQTT. Any channel with a
  custom PSK that you don't have stays encrypted (you'll see the packet
  envelope but not the payload).

### 4. MQTT Broker

#### Option A: Self-hosted mosquitto (recommended)

On your server:

```bash
sudo apt install mosquitto mosquitto-clients
sudo systemctl enable mosquitto

# /etc/mosquitto/conf.d/meshnarc.conf
cat << 'EOF' | sudo tee /etc/mosquitto/conf.d/meshnarc.conf
listener 1883
allow_anonymous false
password_file /etc/mosquitto/passwd
EOF

sudo mosquitto_passwd -c /etc/mosquitto/passwd meshnarc
sudo systemctl restart mosquitto
```

If your server is on Tailscale, the LilyGo can't reach it directly (no
Tailscale on ESP32). Options:
- Expose mosquitto on a public IP with TLS + auth
- Use a cloud-hosted broker instead

#### Option B: HiveMQ Cloud (free tier, zero ops)

- https://www.hivemq.com/mqtt-cloud-broker/
- Free tier: 100 connections, 10 GB/month
- Get broker URL, username, password
- Set in Meshtastic MQTT config

### 5. Deploy meshnarc-subscriber

```bash
# On your server or any host
cd meshnarc
pip install -r requirements.txt --break-system-packages

# Set up service account for BigQuery
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
export MESHNARC_PROJECT=your-gcp-project

# Create BQ dataset + table
bq mk --dataset meshnarc
bq query --use_legacy_sql=false < bq_schema.sql

# Run
python meshnarc_sub.py \
  --broker your-broker.example.com \
  --username meshnarc \
  --password your-password \
  --topic "msh/US/2/e/LongFast/#"
```

#### As a systemd service

```bash
sudo cp meshnarc.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now meshnarc
```

#### Deploying to GCP (Buildpacks + e2-micro)

If deploying to Google Cloud, meshnarc-subscriber runs best as a stateless daemon on an `e2-micro` Compute Engine instance using Container-Optimized OS.

You can automatically build and deploy it using Buildpacks:

```powershell
# Using PowerShell
./deploy_gcp.ps1 -Project your-gcp-project

# Or using Bash
./deploy_gcp.sh your-gcp-project
```

Then configure the running container with your MQTT credentials:

```bash
gcloud compute instances update-container meshnarc-sub \
  --zone us-central1-a \
  --project your-gcp-project \
  --container-env="MESHNARC_BROKER=your-broker,MESHNARC_MQTT_USER=user,MESHNARC_MQTT_PASS=pass"
```

### 6. Verify

```bash
# Watch MQTT traffic
mosquitto_sub -h your-broker -u meshnarc -P password -t "msh/#" -v

# Check BQ
bq query 'SELECT * FROM meshnarc.packets ORDER BY rx_timestamp DESC LIMIT 10'
```

## BigQuery Views

The schema includes pre-built analytical views:

### `meshnarc.recent_nodes` — Active Node Roster (24h)

Who's on the air right now. Aggregates packet counts, signal quality, port types, and last-known position per node.

```sql
SELECT * FROM meshnarc.recent_nodes ORDER BY last_seen DESC;
```

### `meshnarc.messages` — Intercepted Text Messages

All decoded `TEXT_MESSAGE_APP` content with sender identity, channel, and signal metadata.

```sql
SELECT rx_timestamp, from_long_name, message_text
FROM meshnarc.messages
WHERE rx_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

### `meshnarc.positions` — GPS Track Histories

Position reports with coordinates, altitude, speed, and satellite count. Feed into mapping tools for movement analysis.

```sql
-- Last known position for every node
SELECT DISTINCT from_id, from_long_name, latitude, longitude
FROM meshnarc.positions
WHERE rx_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
```

## Example Queries

```sql
-- Social graph: who talks to whom (last 7 days)
SELECT from_id, to_id, COUNT(*) AS packet_count
FROM meshnarc.packets
WHERE rx_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND to_id != '!ffffffff'  -- exclude broadcasts
GROUP BY from_id, to_id
ORDER BY packet_count DESC;

-- Movement history for a specific node
SELECT rx_timestamp, latitude, longitude, altitude, ground_speed
FROM meshnarc.positions
WHERE from_id = '!aabbccdd'
ORDER BY rx_timestamp;

-- Signal quality by node (identify nearby vs distant stations)
SELECT from_id, from_long_name,
  COUNT(*) AS packets,
  ROUND(AVG(rx_rssi), 1) AS avg_rssi,
  ROUND(AVG(rx_snr), 1) AS avg_snr
FROM meshnarc.packets
WHERE rx_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY from_id, from_long_name
ORDER BY avg_rssi DESC;
```

## MQTT Topic Structure

Meshtastic publishes to topics like:

```
msh/{region}/{channel_id}/e/{channel_name}/{gateway_node_id}
```

Example: `msh/US/2/e/LongFast/!aabbccdd`

The `/e/` indicates encrypted (default key). Packets are protobuf-encoded
`ServiceEnvelope` messages.

## MeshCore

MeshCore is a separate protocol — no MQTT gateway in its firmware. For
MeshCore capture, you'd need a second radio running MeshCore firmware
with serial output to a companion host. The BQ schema has a
`source_protocol` field ready for it, and `meshnarc_sub.py` has a stub
for MeshCore packet ingestion, but the capture path is different hardware.

## Cost

- **Hologram**: ~$0.40/month device fee + $0.60/MB data.
  MQTT packets are tiny (~100-300 bytes each). Even heavy mesh traffic
  (1000 packets/day) is well under 1 MB/month. Budget ~$1-2/month per node.
- **HiveMQ Cloud**: Free tier sufficient.
- **BigQuery**: Streaming inserts ~$0.01/200MB. Negligible at this scale.
  Storage: $0.02/GB/month. You'd need millions of packets to hit $1.
- **Total**: A single-node deployment runs under $3/month. A multi-node metro deployment scales linearly.
