# meshnarc

Passive Meshtastic packet capture to BigQuery via cellular MQTT gateway.

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
│        │  Meshtastic    │       │
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
    │  (Python, runs on      │
    │   server / GCP e2-micro│
    │   instance / anywhere) │
    │                        │
    │  • MQTT subscribe      │
    │  • Protobuf decode     │
    │  • BQ streaming insert │
    └────────────┬───────────┘
                 │
                 ▼
         ┌───────────────┐
         │   BigQuery    │
         │  meshnarc.    │
         │  packets      │
         └───────────────┘
```

## Components

1. **LilyGo T-SIM7080G-S3** — Meshtastic firmware, MQTT gateway mode
2. **Hologram IoT SIM** — Cat-M1/NB-IoT cellular backhaul
3. **MQTT Broker** — mosquitto (self-hosted) or HiveMQ Cloud (free tier)
4. **meshnarc-subscriber** — Python daemon, decodes + inserts to BQ
5. **BigQuery** — `meshnarc.packets` table

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
  (1000 packets/day) is well under 1 MB/month. Budget ~$1-2/month.
- **HiveMQ Cloud**: Free tier sufficient.
- **BigQuery**: Streaming inserts ~$0.01/200MB. Negligible at this scale.
  Storage: $0.02/GB/month. You'd need millions of packets to hit $1.
