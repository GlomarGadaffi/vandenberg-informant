#!/usr/bin/env python3
"""
meshnarc_sub.py — MQTT subscriber for Meshtastic packet capture to BigQuery.

Subscribes to Meshtastic MQTT gateway topics, decodes ServiceEnvelope
protobufs, and streams decoded packets to BigQuery.

Designed to run on any host with internet access (Compute Engine, Cloud Run Job, etc).
The LilyGo T-SIM7080G-S3 handles RF capture and MQTT uplink over Hologram
cellular — this subscriber just processes the stream.

Usage:
    python meshnarc_sub.py \
        --broker mqtt.example.com \
        --username meshnarc \
        --password secret \
        --topic "msh/US/2/e/LongFast/#"

Environment:
    GOOGLE_APPLICATION_CREDENTIALS  path to SA key JSON
    MESHNARC_PROJECT                GCP project ID
    MESHNARC_BROKER                 MQTT broker hostname
    MESHNARC_MQTT_USER              MQTT username
    MESHNARC_MQTT_PASS              MQTT password
    MESHNARC_CAPTURE_LAT            fixed capture position latitude
    MESHNARC_CAPTURE_LON            fixed capture position longitude
"""

import argparse
import base64
import collections
import json
import os
import traceback
import signal
import sys
import time
from datetime import datetime, timezone
from queue import Queue, Empty, Full
from threading import Thread, Event, Lock
from typing import Optional

import paho.mqtt.client as mqtt
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from google.cloud import bigquery
from google.protobuf.json_format import MessageToDict

# Meshtastic protobuf imports
from meshtastic.protobuf import (
    mesh_pb2,
    mqtt_pb2,
    portnums_pb2,
    telemetry_pb2,
)


# --- Logging ---

def log_json(level, msg, **kwargs):
    ctx = {"level": level, "msg": str(msg), "ts": datetime.now(timezone.utc).isoformat()}
    if kwargs.pop("exc_info", False):
        ctx["traceback"] = traceback.format_exc()
    if kwargs:
        ctx.update(kwargs)
    print(json.dumps(ctx, default=str), flush=True)


class MeshNarcLogger:
    def __init__(self, verbose=False):
        self.verbose = verbose
    def info(self, msg, **kwargs):
        log_json("INFO", msg, **kwargs)
    def warning(self, msg, **kwargs):
        log_json("WARNING", msg, **kwargs)
    def error(self, msg, **kwargs):
        log_json("ERROR", msg, **kwargs)
    def debug(self, msg, **kwargs):
        if self.verbose:
            log_json("DEBUG", msg, **kwargs)

log = MeshNarcLogger()


# --- Crypto ---

# Meshtastic default channel key: AQ== (0x01), expanded to AES-256
# The firmware expands 1-byte keys by repeating: 0x01 * 32
DEFAULT_KEY = bytes([0x01] * 32)

# Valid portnum range — anything outside this means decrypt produced garbage
_VALID_PORTNUMS = set(portnums_pb2.PortNum.values())


def decrypt_packet(mp: mesh_pb2.MeshPacket, key: bytes = DEFAULT_KEY) -> Optional[bytes]:
    """Decrypt a Meshtastic packet's encrypted payload.

    Meshtastic uses AES-256-CTR. The nonce is constructed from:
    - packet.id (4 bytes LE)
    - packet.from (4 bytes LE)
    - 0x00 * 8

    Returns decrypted bytes, or None on failure.
    """
    if not mp.encrypted:
        return None

    try:
        # Build nonce: packet_id (4 LE) + from_node (4 LE) + 8 zero bytes
        nonce = (
            mp.id.to_bytes(4, "little")
            + mp.from_.to_bytes(4, "little")
            + b"\x00" * 8
        )

        cipher = Cipher(algorithms.AES(key), modes.CTR(nonce))
        decryptor = cipher.decryptor()
        return decryptor.update(mp.encrypted) + decryptor.finalize()

    except Exception as e:
        log.debug(f"Decrypt failed for packet {mp.id}: {e}")
        return None


def try_decrypt_and_parse(
    mp: mesh_pb2.MeshPacket, keys: dict[int, bytes]
) -> Optional[mesh_pb2.Data]:
    """Try each key, validate the decrypted protobuf is sane.

    AES-CTR never "fails" on wrong keys — it just produces garbage.
    We validate by checking that the parsed Data has a known portnum.
    """
    for key in keys.values():
        raw = decrypt_packet(mp, key)
        if not raw:
            continue

        try:
            data = mesh_pb2.Data()
            data.ParseFromString(raw)
            # Sanity: portnum must be a valid enum value and payload non-empty
            if data.portnum in _VALID_PORTNUMS and data.portnum != 0:
                return data
        except Exception:
            continue

    return None


# --- Decode ---

def decode_data(port_num: int, payload: bytes) -> dict:
    """Decode a Meshtastic Data payload based on port number."""
    result = {}

    try:
        if port_num == portnums_pb2.TEXT_MESSAGE_APP:
            result["text"] = payload.decode("utf-8", errors="replace")

        elif port_num == portnums_pb2.POSITION_APP:
            pos = mesh_pb2.Position()
            pos.ParseFromString(payload)
            result = MessageToDict(pos, preserving_proto_field_name=True)

        elif port_num == portnums_pb2.NODEINFO_APP:
            user = mesh_pb2.User()
            user.ParseFromString(payload)
            result = MessageToDict(user, preserving_proto_field_name=True)

        elif port_num == portnums_pb2.TELEMETRY_APP:
            telem = telemetry_pb2.Telemetry()
            telem.ParseFromString(payload)
            result = MessageToDict(telem, preserving_proto_field_name=True)

        elif port_num == portnums_pb2.NEIGHBORINFO_APP:
            ni = mesh_pb2.NeighborInfo()
            ni.ParseFromString(payload)
            result = MessageToDict(ni, preserving_proto_field_name=True)

        elif port_num == portnums_pb2.TRACEROUTE_APP:
            route = mesh_pb2.RouteDiscovery()
            route.ParseFromString(payload)
            result = MessageToDict(route, preserving_proto_field_name=True)

        elif port_num == portnums_pb2.MAP_REPORT_APP:
            try:
                mr = mesh_pb2.MapReport()
                mr.ParseFromString(payload)
                result = MessageToDict(mr, preserving_proto_field_name=True)
            except AttributeError:
                result = {"raw_b64": base64.b64encode(payload).decode()}

        else:
            # Unknown port — store raw
            result = {"raw_b64": base64.b64encode(payload).decode()}

    except Exception as e:
        log.debug(f"Decode error for port {port_num}: {e}")
        result = {
            "decode_error": str(e),
            "raw_b64": base64.b64encode(payload).decode(),
        }

    return result


def portnum_name(port_num: int) -> str:
    """Convert port number int to human-readable name."""
    try:
        return portnums_pb2.PortNum.Name(port_num)
    except ValueError:
        return f"UNKNOWN_{port_num}"


def node_id_hex(num: int) -> str:
    """Format a node number as Meshtastic hex ID."""
    if num == 0xFFFFFFFF:
        return "!ffffffff"
    return f"!{num:08x}"


# --- Dedup ---

class PacketDedup:
    """Bounded dedup set — remembers recent packet fingerprints.

    Meshtastic gateways can publish the same packet multiple times
    (rebroadcasts heard by the same gateway). We dedup on (packet_id, from_id)
    with a bounded window so memory doesn't grow unbounded.
    """

    def __init__(self, maxlen: int = 10000):
        self._seen: collections.OrderedDict = collections.OrderedDict()
        self._maxlen = maxlen

    def is_dup(self, packet_id: int, from_id: int) -> bool:
        key = (packet_id, from_id)
        if key in self._seen:
            return True
        self._seen[key] = None
        # Evict oldest if over capacity
        while len(self._seen) > self._maxlen:
            self._seen.popitem(last=False)
        return False


# --- Subscriber ---

class MeshNarcSubscriber:
    """MQTT subscriber that decodes Meshtastic packets and inserts to BigQuery."""

    def __init__(self, args):
        self.args = args
        self.shutdown = Event()
        self.packet_queue: Queue = Queue(maxsize=50000)
        self._writer_thread: Optional[Thread] = None

        # Thread-safe state
        self._lock = Lock()
        self._node_db: dict = {}  # node_num -> {longName, shortName}
        self._stats = {
            "received": 0,
            "decoded": 0,
            "decrypt_fail": 0,
            "deduped": 0,
            "queue_full": 0,
            "ingested": 0,
            "errors": 0,
            "disconnects": 0,
        }

        self._dedup = PacketDedup()

        # Channel keys: index -> bytes
        # Default channel (index 0) uses the well-known key
        self.channel_keys = {0: DEFAULT_KEY}
        if args.extra_keys:
            for spec in args.extra_keys:
                idx, b64key = spec.split(":", 1)
                raw = base64.b64decode(b64key)
                # Expand short keys the way Meshtastic does
                if len(raw) < 32:
                    raw = raw * (32 // len(raw) + 1)
                    raw = raw[:32]
                self.channel_keys[int(idx)] = raw

        # Capture position — resolve once, not per-packet
        self.capture_lat = (
            args.lat
            or float(os.environ.get("MESHNARC_CAPTURE_LAT", 0))
            or None
        )
        self.capture_lon = (
            args.lon
            or float(os.environ.get("MESHNARC_CAPTURE_LON", 0))
            or None
        )

        # BQ setup
        project = args.project or os.environ.get("MESHNARC_PROJECT", "")
        if not project:
            log.error("No GCP project. Use --project or MESHNARC_PROJECT env var.")
            sys.exit(1)
        dataset = args.dataset or "meshnarc"
        table = args.table or "packets"
        self.table_id = f"{project}.{dataset}.{table}"

    def _inc(self, key: str, n: int = 1):
        """Thread-safe stats increment."""
        with self._lock:
            self._stats[key] += n

    def _get_stats(self) -> dict:
        with self._lock:
            return dict(self._stats)

    def _update_node(self, node_num: int, long_name: str, short_name: str):
        with self._lock:
            self._node_db[node_num] = {"longName": long_name, "shortName": short_name}

    def _get_node(self, node_num: int) -> dict:
        with self._lock:
            return self._node_db.get(node_num, {})

    def start(self):
        """Connect MQTT + BQ and run."""
        log.info("meshnarc subscriber starting")

        # BigQuery
        self.bq_client = bigquery.Client()
        log.info(f"BQ target: {self.table_id}")

        # MQTT
        client_id = f"meshnarc-{int(time.time())}"
        self.mqtt = mqtt.Client(
            client_id=client_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )

        broker = self.args.broker or os.environ.get("MESHNARC_BROKER", "")
        username = self.args.username or os.environ.get("MESHNARC_MQTT_USER", "")
        password = self.args.password or os.environ.get("MESHNARC_MQTT_PASS", "")

        if not broker:
            log.error("No MQTT broker. Use --broker or MESHNARC_BROKER env var.")
            sys.exit(1)

        if username:
            self.mqtt.username_pw_set(username, password)

        port = self.args.mqtt_port or 1883
        if self.args.tls:
            self.mqtt.tls_set()
            port = self.args.mqtt_port or 8883

        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_message = self._on_message
        self.mqtt.on_disconnect = self._on_disconnect

        log.info(f"Connecting to MQTT broker: {broker}:{port}")
        self.mqtt.connect(broker, port, keepalive=60)

        # Start BQ writer thread — NOT daemon, we join it on shutdown
        self._writer_thread = Thread(target=self._bq_writer, name="bq-writer")
        self._writer_thread.start()

        # Signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        # MQTT loop (non-blocking, handles reconnect internally)
        self.mqtt.loop_start()

        # Stats loop
        while not self.shutdown.is_set():
            self.shutdown.wait(60)
            s = self._get_stats()
            with self._lock:
                node_count = len(self._node_db)
            log.info(
                f"stats: rx={s['received']} "
                f"decoded={s['decoded']} "
                f"decrypt_fail={s['decrypt_fail']} "
                f"deduped={s['deduped']} "
                f"queue_full={s['queue_full']} "
                f"ingested={s['ingested']} "
                f"errors={s['errors']} "
                f"disconnects={s['disconnects']} "
                f"queue={self.packet_queue.qsize()} "
                f"nodes={node_count}"
            )

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        topic = self.args.topic or "msh/US/2/e/#"
        log.info(f"MQTT connected (rc={rc}), subscribing to: {topic}")
        client.subscribe(topic)

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        self._inc("disconnects")
        log.warning(f"MQTT disconnected (rc={rc}), will reconnect...")

    def _on_message(self, client, userdata, msg):
        """Process an incoming MQTT message (Meshtastic ServiceEnvelope)."""
        self._inc("received")

        try:
            # Parse ServiceEnvelope
            envelope = mqtt_pb2.ServiceEnvelope()
            envelope.ParseFromString(msg.payload)

            mp = envelope.packet
            if not mp or mp.id == 0:
                return

            # Dedup — same packet heard via rebroadcast
            if self._dedup.is_dup(mp.id, mp.from_):
                self._inc("deduped")
                return

            # Extract topic metadata
            # Topic format: msh/{region}/{channel_id}/e/{channel_name}/{gateway_id}
            topic_parts = msg.topic.split("/")
            channel_name = topic_parts[4] if len(topic_parts) > 4 else ""
            gateway_id = topic_parts[5] if len(topic_parts) > 5 else ""

            # Decrypt / extract payload
            data = None
            if mp.HasField("decoded"):
                data = mp.decoded
            elif mp.encrypted:
                data = try_decrypt_and_parse(mp, self.channel_keys)
                if not data:
                    self._inc("decrypt_fail")
                    return
            else:
                return

            # Decode payload based on port number
            port_name = portnum_name(data.portnum)
            payload_dict = decode_data(data.portnum, data.payload)

            # Update node DB from NODEINFO
            if data.portnum == portnums_pb2.NODEINFO_APP:
                self._update_node(
                    mp.from_,
                    payload_dict.get("long_name", ""),
                    payload_dict.get("short_name", ""),
                )

            # Extract position if present — use `is not None` to handle 0-lat/lon
            lat, lon, alt, speed, sats, precision = (
                None, None, None, None, None, None,
            )
            if data.portnum == portnums_pb2.POSITION_APP:
                # Meshtastic stores lat/lon as int * 1e-7
                lat_i = payload_dict.get("latitude_i")
                lon_i = payload_dict.get("longitude_i")
                if lat_i is not None:
                    lat = lat_i / 1e7
                if lon_i is not None:
                    lon = lon_i / 1e7
                # Some firmware versions use float directly
                if lat is None and "latitude" in payload_dict:
                    lat = payload_dict["latitude"]
                if lon is None and "longitude" in payload_dict:
                    lon = payload_dict["longitude"]
                alt = payload_dict.get("altitude")
                speed = payload_dict.get("ground_speed")
                sats = payload_dict.get("sats_in_view")
                precision = payload_dict.get("precision_bits")

            # Build BQ row
            node_info = self._get_node(mp.from_)

            row = {
                "packet_id": mp.id,
                "rx_timestamp": datetime.fromtimestamp(
                    mp.rx_time if mp.rx_time else time.time(),
                    tz=timezone.utc,
                ).isoformat(),
                "source_protocol": "meshtastic",
                "from_id": node_id_hex(mp.from_),
                "from_long_name": node_info.get("longName"),
                "from_short_name": node_info.get("shortName"),
                "to_id": node_id_hex(mp.to),
                "channel_id": channel_name,
                "gateway_id": gateway_id,
                "hop_limit": mp.hop_limit,
                "hop_start": mp.hop_start if mp.hop_start else None,
                "want_ack": mp.want_ack,
                "via_mqtt": mp.via_mqtt,
                "rx_snr": mp.rx_snr,
                "rx_rssi": mp.rx_rssi,
                "port_num": port_name,
                "payload_json": json.dumps(payload_dict, default=str),
                "raw_payload_b64": base64.b64encode(data.payload).decode()
                    if data.payload else None,
                "latitude": lat,
                "longitude": lon,
                "altitude": alt,
                "ground_speed": speed,
                "sats_in_view": sats,
                "precision_bits": precision,
                "capture_node_id": gateway_id,
                "capture_lat": self.capture_lat,
                "capture_lon": self.capture_lon,
            }

            try:
                self.packet_queue.put_nowait(row)
            except Full:
                self._inc("queue_full")
                log.warning("Packet queue full — dropping packet. BQ writer falling behind.")
                return

            self._inc("decoded")

            if self.args.verbose:
                log.info(
                    f"[{port_name}] {node_id_hex(mp.from_)} → "
                    f"{node_id_hex(mp.to)} | "
                    f"rssi={mp.rx_rssi} snr={mp.rx_snr:.1f} | "
                    f"{json.dumps(payload_dict, default=str)[:120]}"
                )

        except Exception as e:
            log.error(f"Error processing MQTT message: {e}", exc_info=self.args.verbose)
            self._inc("errors")

    def _bq_writer(self):
        """Background thread: batch inserts rows to BigQuery."""
        batch = []
        last_flush = time.time()
        batch_size = 50
        flush_interval = 10  # seconds

        while not self.shutdown.is_set():
            try:
                row = self.packet_queue.get(timeout=1)
                batch.append(row)
            except Empty:
                pass

            now = time.time()
            if len(batch) >= batch_size or (
                batch and now - last_flush >= flush_interval
            ):
                self._flush_batch(batch)
                batch = []
                last_flush = now

        # Drain remaining queue on shutdown
        while not self.packet_queue.empty():
            try:
                batch.append(self.packet_queue.get_nowait())
            except Empty:
                break
        if batch:
            self._flush_batch(batch)

        log.info("BQ writer thread exiting — all batches flushed")

    def _flush_batch(self, batch: list):
        """Insert a batch of rows to BigQuery."""
        if not batch:
            return

        try:
            errors = self.bq_client.insert_rows_json(self.table_id, batch)
            if errors:
                log.error(f"BQ insert errors: {errors[:3]}")
                self._inc("errors", len(errors))
                self._inc("ingested", len(batch) - len(errors))
            else:
                self._inc("ingested", len(batch))
                log.debug(f"Flushed {len(batch)} rows to BQ")
        except Exception as e:
            log.error(f"BQ insert failed: {e}")
            self._inc("errors", len(batch))

    def _handle_signal(self, signum, frame):
        log.info(f"Signal {signum}, shutting down...")
        self.shutdown.set()
        self.mqtt.loop_stop()
        self.mqtt.disconnect()

        # Wait for BQ writer to drain and exit cleanly
        if self._writer_thread and self._writer_thread.is_alive():
            log.info("Waiting for BQ writer to flush...")
            self._writer_thread.join(timeout=15)
            if self._writer_thread.is_alive():
                log.warning("BQ writer didn't finish in time — some data may be lost")

        log.info(f"Final stats: {self._get_stats()}")
        sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description="meshnarc — Meshtastic MQTT to BigQuery subscriber"
    )

    # MQTT
    parser.add_argument("--broker", help="MQTT broker hostname")
    parser.add_argument("--mqtt-port", type=int, help="MQTT port (1883 or 8883)")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--tls", action="store_true", help="Enable MQTT TLS")
    parser.add_argument(
        "--topic",
        default="msh/US/2/e/#",
        help="MQTT topic to subscribe (default: msh/US/2/e/#)",
    )

    # Decryption
    parser.add_argument(
        "--extra-keys",
        nargs="*",
        help="Additional channel keys as channel_index:base64key (e.g. 1:AQEBAQ==)",
    )

    # BigQuery
    parser.add_argument("--project", help="GCP project ID")
    parser.add_argument("--dataset", default="meshnarc", help="BQ dataset")
    parser.add_argument("--table", default="packets", help="BQ table")

    # Capture metadata
    parser.add_argument("--lat", type=float, help="Capture node latitude")
    parser.add_argument("--lon", type=float, help="Capture node longitude")

    # General
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        log.verbose = True

    sub = MeshNarcSubscriber(args)
    sub.start()


if __name__ == "__main__":
    main()
