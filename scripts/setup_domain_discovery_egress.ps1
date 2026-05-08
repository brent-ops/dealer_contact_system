param(
    [string]$ProjectId = "dealer-contacts-project",
    [string]$Region = "us-central1",
    [string]$Network = "default",
    [string]$RouterName = "dealer-domain-discovery-router",
    [string]$NatName = "dealer-domain-discovery-nat",
    [string]$NatAddressName = "dealer-domain-discovery-egress-ip",
    [string]$ConnectorName = "dealer-discovery-conn",
    [string]$ConnectorSubnetName = "dealer-domain-discovery-subnet",
    [string]$ConnectorSubnetRange = "10.8.0.0/28",
    [int]$ConnectorMinInstances = 2,
    [int]$ConnectorMaxInstances = 3
)

$ErrorActionPreference = "Stop"

Write-Host "Enabling required APIs..."
cmd /c "gcloud services enable compute.googleapis.com vpcaccess.googleapis.com --project=$ProjectId"

Write-Host "Ensuring static NAT IP exists..."
cmd /c "gcloud compute addresses describe $NatAddressName --region $Region --project=$ProjectId >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    cmd /c "gcloud compute addresses create $NatAddressName --region $Region --project=$ProjectId"
}

Write-Host "Ensuring Cloud Router exists..."
cmd /c "gcloud compute routers describe $RouterName --region $Region --project=$ProjectId >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    cmd /c "gcloud compute routers create $RouterName --network $Network --region $Region --project=$ProjectId"
}

Write-Host "Ensuring dedicated connector subnet exists..."
cmd /c "gcloud compute networks subnets describe $ConnectorSubnetName --region $Region --project=$ProjectId >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    cmd /c "gcloud compute networks subnets create $ConnectorSubnetName --network $Network --range $ConnectorSubnetRange --region $Region --project=$ProjectId"
}

Write-Host "Ensuring discovery NAT exists..."
cmd /c "gcloud compute routers nats describe $NatName --router $RouterName --router-region $Region --project=$ProjectId >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    cmd /c "gcloud compute routers nats create $NatName --router=$RouterName --router-region=$Region --nat-custom-subnet-ip-ranges=$ConnectorSubnetName --nat-external-ip-pool=$NatAddressName --project=$ProjectId"
}

Write-Host "Ensuring Serverless VPC Access connector exists..."
cmd /c "gcloud compute networks vpc-access connectors describe $ConnectorName --region $Region --project=$ProjectId >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    cmd /c "gcloud compute networks vpc-access connectors create $ConnectorName --region $Region --subnet $ConnectorSubnetName --subnet-project $ProjectId --min-instances $ConnectorMinInstances --max-instances $ConnectorMaxInstances --project=$ProjectId"
}

Write-Host "Dedicated discovery egress is ready:"
Write-Host "  Network: $Network"
Write-Host "  Router: $RouterName"
Write-Host "  NAT: $NatName"
Write-Host "  Static IP name: $NatAddressName"
Write-Host "  Connector: $ConnectorName"
Write-Host "  Subnet: $ConnectorSubnetName ($ConnectorSubnetRange)"
