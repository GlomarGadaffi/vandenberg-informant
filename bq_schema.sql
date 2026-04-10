-- meshnarc BigQuery schema
-- Dataset: meshnarc
-- Run: bq mk --dataset meshnarc
--       bq query --use_legacy_sql=false < bq_schema.sql

CREATE TABLE IF NOT EXISTS `meshnarc.packets` (
  packet_id       INT64       NOT NULL,   -- Meshtastic packet ID
  rx_timestamp    TIMESTAMP   NOT NULL,   -- When captured
  source_protocol STRING      NOT NULL,   -- 'meshtastic' | 'meshcore'

  -- Routing
  from_id         STRING,                 -- Source node (hex !aabbccdd)
  from_long_name  STRING,                 -- Node long name if in NodeInfo
  from_short_name STRING,                 -- Node short name
  to_id           STRING,                 -- Destination (!ffffffff = broadcast)
  channel_id      STRING,                 -- Channel name (e.g. LongFast)
  gateway_id      STRING,                 -- MQTT gateway node that uplinked this

  -- Mesh metadata
  hop_limit       INT64,
  hop_start       INT64,
  want_ack        BOOL,
  via_mqtt        BOOL,
  rx_snr          FLOAT64,
  rx_rssi         INT64,

  -- Payload
  port_num        STRING,                 -- TEXT_MESSAGE_APP, POSITION_APP, etc.
  payload_json    STRING,                 -- Decoded payload as JSON
  raw_payload_b64 STRING,                 -- Base64 protobuf bytes

  -- Extracted position (POSITION_APP)
  latitude        FLOAT64,
  longitude       FLOAT64,
  altitude        INT64,
  ground_speed    INT64,
  sats_in_view    INT64,
  precision_bits  INT64,

  -- Capture metadata
  capture_node_id STRING,                 -- Our meshnarc gateway node ID
  capture_lat     FLOAT64,
  capture_lon     FLOAT64,

  ingested_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(rx_timestamp)
CLUSTER BY source_protocol, port_num, from_id
OPTIONS (
  description = 'meshnarc: captured unauthenticated mesh radio packets'
);

-- Recent node activity (24h window)
CREATE OR REPLACE VIEW `meshnarc.recent_nodes` AS
SELECT
  from_id,
  COALESCE(
    ARRAY_AGG(from_long_name IGNORE NULLS ORDER BY rx_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)],
    from_id
  ) AS node_name,
  COUNT(*) AS packet_count,
  ARRAY_AGG(DISTINCT port_num IGNORE NULLS) AS port_nums,
  MIN(rx_timestamp) AS first_seen,
  MAX(rx_timestamp) AS last_seen,
  ROUND(AVG(rx_rssi), 1) AS avg_rssi,
  ROUND(AVG(rx_snr), 1) AS avg_snr,
  ARRAY_AGG(latitude IGNORE NULLS ORDER BY rx_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_lat,
  ARRAY_AGG(longitude IGNORE NULLS ORDER BY rx_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_lon
FROM `meshnarc.packets`
WHERE rx_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY from_id
ORDER BY last_seen DESC;

-- Text messages
CREATE OR REPLACE VIEW `meshnarc.messages` AS
SELECT
  rx_timestamp,
  from_id,
  from_long_name,
  to_id,
  channel_id,
  JSON_VALUE(payload_json, '$.text') AS message_text,
  rx_rssi,
  rx_snr,
  via_mqtt,
  gateway_id
FROM `meshnarc.packets`
WHERE port_num = 'TEXT_MESSAGE_APP'
ORDER BY rx_timestamp DESC;

-- Position history (for mapping)
CREATE OR REPLACE VIEW `meshnarc.positions` AS
SELECT
  rx_timestamp,
  from_id,
  from_long_name,
  latitude,
  longitude,
  altitude,
  ground_speed,
  sats_in_view,
  rx_rssi,
  gateway_id
FROM `meshnarc.packets`
WHERE port_num = 'POSITION_APP'
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
ORDER BY rx_timestamp DESC;
