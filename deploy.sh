#!/usr/bin/env bash
# deploy.sh — set up meshnarc subscriber on a Linux host
# Usage: ./deploy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="${MESHNARC_INSTALL_DIR:-/home/pi/meshnarc}"

echo "=== meshnarc deploy ==="
echo "Install dir: ${INSTALL_DIR}"

# Install Python deps
echo "--- Installing Python dependencies ---"
pip install -r "${SCRIPT_DIR}/requirements.txt" --break-system-packages

# Create install dir if needed
mkdir -p "${INSTALL_DIR}"

# Copy files
echo "--- Copying files ---"
cp "${SCRIPT_DIR}/meshnarc_sub.py" "${INSTALL_DIR}/"
cp "${SCRIPT_DIR}/requirements.txt" "${INSTALL_DIR}/"
cp "${SCRIPT_DIR}/meshnarc.service" "${INSTALL_DIR}/"

# Check for SA key
if [ ! -f "${INSTALL_DIR}/sa-key.json" ]; then
    echo ""
    echo "WARNING: No service account key found at ${INSTALL_DIR}/sa-key.json"
    echo "  Create a SA with roles/bigquery.dataEditor and roles/bigquery.user"
    echo "  Download the key and place it at ${INSTALL_DIR}/sa-key.json"
    echo ""
fi

# BigQuery setup
echo "--- BigQuery schema ---"
if command -v bq &>/dev/null; then
    echo "Creating dataset (if not exists)..."
    bq mk --dataset meshnarc 2>/dev/null || echo "  (dataset already exists)"
    echo "Applying schema..."
    bq query --use_legacy_sql=false < "${SCRIPT_DIR}/bq_schema.sql" || echo "  (tables may already exist)"
else
    echo "  bq CLI not found — apply bq_schema.sql manually"
fi

# systemd
echo "--- systemd service ---"
echo "  Edit ${INSTALL_DIR}/meshnarc.service with your MQTT + GCP settings"
echo "  Then:"
echo "    sudo cp ${INSTALL_DIR}/meshnarc.service /etc/systemd/system/"
echo "    sudo systemctl daemon-reload"
echo "    sudo systemctl enable --now meshnarc"
echo ""
echo "=== deploy complete ==="
