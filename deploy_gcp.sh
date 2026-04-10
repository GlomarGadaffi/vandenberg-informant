#!/usr/bin/env bash
# deploy_gcp.sh — package and deploy meshnarc subscriber to GCE e2-micro
set -euo pipefail

if [ -z "${1:-}" ]; then
  echo "Usage: $0 <gcp-project-id> [zone] [instance-name]"
  exit 1
fi

PROJECT="$1"
ZONE="${2:-us-central1-a}"
INSTANCE_NAME="${3:-meshnarc-sub}"

echo "Deploying $INSTANCE_NAME to $ZONE — hold onto your butts"

IMAGE="gcr.io/${PROJECT}/${INSTANCE_NAME}:latest"

echo "--- Packaging and pushing image via Buildpacks ---"
gcloud builds submit --pack image="$IMAGE" --project "$PROJECT"

echo "--- Checking for existing instance ---"
if gcloud compute instances list --filter="name=($INSTANCE_NAME)" --project "$PROJECT" --format="value(name)" | grep -q "$INSTANCE_NAME"; then
    echo "--- Updating existing instance container ---"
    gcloud compute instances update-container "$INSTANCE_NAME" \
        --zone "$ZONE" \
        --project "$PROJECT" \
        --container-image "$IMAGE"
else
    echo "--- Creating new e2-micro instance (Container-Optimized OS) ---"
    gcloud compute instances create-with-container "$INSTANCE_NAME" \
        --machine-type "e2-micro" \
        --zone "$ZONE" \
        --project "$PROJECT" \
        --container-image "$IMAGE" \
        --container-restart-policy always \
        --scopes "cloud-platform"
fi

echo ""
echo "NOTE: To set MQTT secrets and broker, run:"
echo "gcloud compute instances update-container $INSTANCE_NAME --zone $ZONE --project $PROJECT --container-env=MESHNARC_BROKER=your-broker,MESHNARC_MQTT_USER=user,MESHNARC_MQTT_PASS=pass"
echo ""

echo "=== deploy complete ==="
