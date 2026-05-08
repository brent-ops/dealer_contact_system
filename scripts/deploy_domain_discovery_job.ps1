param(
    [string]$ProjectId = "dealer-contacts-project",
    [string]$Region = "us-central1",
    [string]$Repository = "dealer-contact-system",
    [string]$ImageName = "dealer-contact-system",
    [string]$JobName = "dealer-domain-discovery-worker",
    [string]$ServiceAccountEmail,
    [string]$VpcConnector = "",
    [string]$VpcEgress = "private-ranges-only",
    [int]$DomainDiscoveryBatchSize = 5,
    [int]$DomainDiscoveryQueryBatchSize = 40,
    [int]$DomainDiscoveryCooldownHours = 4,
    [int]$DomainDiscoveryUsSharePercent = 85,
    [bool]$DomainDiscoveryPromotionEnabled = $true,
    [string]$DomainDiscoveryProviderOrder = "gemini_google_search,duckduckgo_html",
    [bool]$GeminiEnabled = $true,
    [string]$GeminiModel = "gemini-2.5-flash",
    [string]$GeminiApiKeySecret = "gemini-api-key:latest",
    [int]$TaskTimeoutSeconds = 1800
)

$ErrorActionPreference = "Stop"

if (-not $ServiceAccountEmail) {
    throw "Provide -ServiceAccountEmail for the discovery Cloud Run job."
}

$imageUri = "$Region-docker.pkg.dev/$ProjectId/$Repository/$ImageName`:latest"
$envVars = @(
    "APP_ENVIRONMENT=cloud",
    "BIGQUERY_PROJECT_ID=$ProjectId",
    "BIGQUERY_DATASET=dealer_data",
    "DOMAIN_DISCOVERY_ENABLED=true",
    "DOMAIN_DISCOVERY_BATCH_SIZE=$DomainDiscoveryBatchSize",
    "DOMAIN_DISCOVERY_QUERY_BATCH_SIZE=$DomainDiscoveryQueryBatchSize",
    "DOMAIN_DISCOVERY_COOLDOWN_HOURS=$DomainDiscoveryCooldownHours",
    "DOMAIN_DISCOVERY_US_SHARE_PERCENT=$DomainDiscoveryUsSharePercent",
    "DOMAIN_DISCOVERY_PROMOTION_ENABLED=$($DomainDiscoveryPromotionEnabled.ToString().ToLower())",
    "DOMAIN_DISCOVERY_PROVIDER_ORDER=$DomainDiscoveryProviderOrder",
    "GEMINI_ENABLED=$($GeminiEnabled.ToString().ToLower())",
    "GEMINI_MODEL=$GeminiModel"
)

if ($env:DOMAIN_DISCOVERY_SEARCH_ENDPOINT) {
    $envVars += "DOMAIN_DISCOVERY_SEARCH_ENDPOINT=$($env:DOMAIN_DISCOVERY_SEARCH_ENDPOINT)"
}
if ($env:DOMAIN_DISCOVERY_SCHEDULE_HINT) {
    $envVars += "DOMAIN_DISCOVERY_SCHEDULE_HINT=$($env:DOMAIN_DISCOVERY_SCHEDULE_HINT)"
}

$envVarMap = @{}
foreach ($entry in $envVars) {
    if (-not $entry) {
        continue
    }
    $parts = $entry -split "=", 2
    if ($parts.Length -eq 2) {
        $envVarMap[$parts[0]] = $parts[1]
    }
}

$envFile = Join-Path $env:TEMP "dealer-domain-discovery-env.yaml"
$envVarMap.GetEnumerator() |
    Sort-Object Name |
    ForEach-Object {
        $value = [string]$_.Value
        $escaped = $value.Replace("'", "''")
        "$($_.Name): '$escaped'"
    } | Set-Content -Path $envFile -Encoding UTF8

$secretMappings = @(
    "GEMINI_API_KEY=$GeminiApiKeySecret"
) -join ","

Write-Host "Ensuring Artifact Registry repository exists..."
cmd /c "gcloud artifacts repositories describe $Repository --location=$Region --project=$ProjectId >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    cmd /c "gcloud artifacts repositories create $Repository --repository-format=docker --location=$Region --description=""Dealer contact system images"" --project=$ProjectId"
}

Write-Host "Building container image..."
cmd /c "gcloud builds submit --tag $imageUri --project=$ProjectId"

Write-Host "Deploying Cloud Run discovery job..."
$deployArgs = @(
    "run jobs deploy $JobName",
    "--image $imageUri",
    "--region $Region",
    "--project=$ProjectId",
    "--service-account $ServiceAccountEmail",
    "--env-vars-file $envFile",
    "--set-secrets $secretMappings",
    "--max-retries 0",
    "--task-timeout ${TaskTimeoutSeconds}s",
    "--args run-domain-discovery-cycle,--seed"
)
if ($VpcConnector) {
    $deployArgs += "--vpc-connector $VpcConnector"
    $deployArgs += "--vpc-egress $VpcEgress"
}
cmd /c ("gcloud " + ($deployArgs -join " "))

Remove-Item -Path $envFile -ErrorAction SilentlyContinue

Write-Host "Cloud Run discovery job deployed:"
Write-Host "  Job: $JobName"
Write-Host "  Region: $Region"
Write-Host "  Image: $imageUri"
