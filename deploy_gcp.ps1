param(
    [Parameter(Mandatory=$true)][string]$Project,
    [string]$Region = "us-central1",
    [string]$Zone = "us-central1-a",
    [string]$InstanceName = "meshnarc-sub"
)

$ErrorActionPreference = "Stop"

Write-Host "Deploying $InstanceName to $Zone — hold onto your butts"

# Using Buildpacks to build the container image
$Image = "gcr.io/$Project/$InstanceName:latest"

Write-Host "--- Packaging and pushing image via Buildpacks ---"
gcloud builds submit --pack image=$Image --project $Project

Write-Host "--- Checking for existing instance ---"
$instanceExists = gcloud compute instances list --filter="name=($InstanceName)" --project $Project --format="value(name)"
if ($instanceExists) {
    Write-Host "--- Updating existing instance container ---"
    gcloud compute instances update-container $InstanceName `
        --zone $Zone `
        --project $Project `
        --container-image $Image
} else {
    Write-Host "--- Creating new e2-micro instance (Container-Optimized OS) ---"
    gcloud compute instances create-with-container $InstanceName `
        --machine-type "e2-micro" `
        --zone $Zone `
        --project $Project `
        --container-image $Image `
        --container-restart-policy always `
        --scopes "cloud-platform"
}

Write-Host ""
Write-Host "NOTE: To set MQTT secrets and broker, run:"
Write-Host "gcloud compute instances update-container $InstanceName --zone $Zone --project $Project --container-env=MESHNARC_BROKER=your-broker,MESHNARC_MQTT_USER=user,MESHNARC_MQTT_PASS=pass"
Write-Host ""

Write-Host "=== deploy complete ==="
