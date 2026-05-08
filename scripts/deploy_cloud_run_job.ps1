param(
    [string]$ProjectId = "dealer-contacts-project",
    [string]$Region = "us-central1",
    [string]$Repository = "dealer-contact-system",
    [string]$ImageName = "dealer-contact-system",
    [string]$JobName = "dealer-contact-worker",
    [string]$ServiceAccountEmail,
    [int]$ValidateBatchSize = 50,
    [int]$EnrichBatchSize = 25,
    [int]$GbpBatchSize = 25,
    [int]$AiRetrievalBatchSize = 1,
    [int]$ExtractBatchSize = 25,
    [int]$RetryBlockedBatchSize = 10,
    [int]$CampaignMonitorSyncBatchSize = 1000,
    [bool]$CampaignMonitorSyncEnabled = $true,
    [bool]$AiRetrievalEnabled = $true,
    [string]$AiRetrievalProviderOrder = "gemini,openai",
    [int]$AiRetrievalMinRunIntervalMinutes = 30,
    [int]$AiRetrievalCooldownHours = 72,
    [int]$AiRetrievalRateLimitCooldownMinutes = 180,
    [double]$AiRetrievalRequestDelaySeconds = 2,
    [string]$AiRetrievalPromptStyle = "simple_staff",
    [bool]$GeminiEnabled = $true,
    [string]$GeminiModel = "gemini-2.5-flash",
    [bool]$OpenAiEnabled = $false,
    [string]$OpenAiModel = "gpt-5.4",
    [string]$CampaignMonitorApiKeySecret = "campaign-monitor-api-key:latest",
    [string]$CampaignMonitorClientIdSecret = "campaign-monitor-client-id:latest",
    [string]$GeminiApiKeySecret = "gemini-api-key:latest",
    [int]$TaskTimeoutSeconds = 3600
)

$ErrorActionPreference = "Stop"

if (-not $ServiceAccountEmail) {
    throw "Provide -ServiceAccountEmail for the Cloud Run job."
}

$imageUri = "$Region-docker.pkg.dev/$ProjectId/$Repository/$ImageName`:latest"
$envVars = @(
    "APP_ENVIRONMENT=cloud",
    "BIGQUERY_PROJECT_ID=$ProjectId",
    "BIGQUERY_DATASET=dealer_data",
    "VALIDATE_WORKER_BATCH_SIZE=$ValidateBatchSize",
    "ENRICH_WORKER_BATCH_SIZE=$EnrichBatchSize",
    "GBP_WORKER_BATCH_SIZE=$GbpBatchSize",
    "AI_RETRIEVAL_BATCH_SIZE=$AiRetrievalBatchSize",
    "AI_RETRIEVAL_ENABLED=$($AiRetrievalEnabled.ToString().ToLower())",
    "AI_RETRIEVAL_PROVIDER_ORDER=$AiRetrievalProviderOrder",
    "AI_RETRIEVAL_MIN_RUN_INTERVAL_MINUTES=$AiRetrievalMinRunIntervalMinutes",
    "AI_RETRIEVAL_COOLDOWN_HOURS=$AiRetrievalCooldownHours",
    "AI_RETRIEVAL_RATE_LIMIT_COOLDOWN_MINUTES=$AiRetrievalRateLimitCooldownMinutes",
    "AI_RETRIEVAL_REQUEST_DELAY_SECONDS=$AiRetrievalRequestDelaySeconds",
    "AI_RETRIEVAL_PROMPT_STYLE=$AiRetrievalPromptStyle",
    "EXTRACT_WORKER_BATCH_SIZE=$ExtractBatchSize",
    "RETRY_BLOCKED_WORKER_BATCH_SIZE=$RetryBlockedBatchSize",
    "CAMPAIGN_MONITOR_SYNC_BATCH_SIZE=$CampaignMonitorSyncBatchSize",
    "CAMPAIGN_MONITOR_SYNC_ENABLED=$($CampaignMonitorSyncEnabled.ToString().ToLower())",
    "GEMINI_ENABLED=$($GeminiEnabled.ToString().ToLower())",
    "GEMINI_MODEL=$GeminiModel",
    "OPENAI_ENABLED=$($OpenAiEnabled.ToString().ToLower())",
    "OPENAI_MODEL=$OpenAiModel"
)

if ($env:CAMPAIGN_MONITOR_MASTER_LIST_NAME) {
    $envVars += "CAMPAIGN_MONITOR_MASTER_LIST_NAME=$($env:CAMPAIGN_MONITOR_MASTER_LIST_NAME)"
}
if ($env:MANAGED_FETCH_ENABLED) {
    $envVars += "MANAGED_FETCH_ENABLED=$($env:MANAGED_FETCH_ENABLED)"
}
if ($env:GBP_ENRICHMENT_ENABLED) {
    $envVars += "GBP_ENRICHMENT_ENABLED=$($env:GBP_ENRICHMENT_ENABLED)"
}
if ($env:GBP_PROVIDER) {
    $envVars += "GBP_PROVIDER=$($env:GBP_PROVIDER)"
}
if ($env:GBP_SEARCH_ENDPOINT) {
    $envVars += "GBP_SEARCH_ENDPOINT=$($env:GBP_SEARCH_ENDPOINT)"
}
if ($env:MANAGED_FETCH_PROVIDER) {
    $envVars += "MANAGED_FETCH_PROVIDER=$($env:MANAGED_FETCH_PROVIDER)"
}
if ($env:MANAGED_FETCH_API_KEY) {
    $envVars += "MANAGED_FETCH_API_KEY=$($env:MANAGED_FETCH_API_KEY)"
}
if ($env:CLIENT_DIM_ENABLED) {
    $envVars += "CLIENT_DIM_ENABLED=$($env:CLIENT_DIM_ENABLED)"
}
if ($env:GLD_ACCOUNTABILITY_PROJECT_ID) {
    $envVars += "GLD_ACCOUNTABILITY_PROJECT_ID=$($env:GLD_ACCOUNTABILITY_PROJECT_ID)"
}
if ($env:CLIENT_DIM_DATASET) {
    $envVars += "CLIENT_DIM_DATASET=$($env:CLIENT_DIM_DATASET)"
}
if ($env:CLIENT_DIM_TABLE) {
    $envVars += "CLIENT_DIM_TABLE=$($env:CLIENT_DIM_TABLE)"
}
if ($env:AI_RETRIEVAL_PROVIDER_ORDER) {
    $envVars += "AI_RETRIEVAL_PROVIDER_ORDER=$($env:AI_RETRIEVAL_PROVIDER_ORDER)"
}
if ($env:AI_RETRIEVAL_PROMPT_STYLE) {
    $envVars += "AI_RETRIEVAL_PROMPT_STYLE=$($env:AI_RETRIEVAL_PROMPT_STYLE)"
}
if ($env:OPENAI_API_KEY) {
    $envVars += "OPENAI_API_KEY=$($env:OPENAI_API_KEY)"
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
$envFile = Join-Path $env:TEMP "dealer-contact-worker-env.yaml"
$envVarMap.GetEnumerator() |
    Sort-Object Name |
    ForEach-Object {
        $value = [string]$_.Value
        $escaped = $value.Replace("'", "''")
        "$($_.Name): '$escaped'"
    } | Set-Content -Path $envFile -Encoding UTF8

$secretMappings = @(
    "CAMPAIGN_MONITOR_API_KEY=$CampaignMonitorApiKeySecret",
    "CAMPAIGN_MONITOR_CLIENT_ID=$CampaignMonitorClientIdSecret",
    "GEMINI_API_KEY=$GeminiApiKeySecret"
) -join ","

Write-Host "Ensuring Artifact Registry repository exists..."
cmd /c "gcloud artifacts repositories describe $Repository --location=$Region --project=$ProjectId >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    cmd /c "gcloud artifacts repositories create $Repository --repository-format=docker --location=$Region --description=""Dealer contact system images"" --project=$ProjectId"
}

Write-Host "Building container image..."
cmd /c "gcloud builds submit --tag $imageUri --project=$ProjectId"

Write-Host "Deploying Cloud Run Job..."
cmd /c "gcloud run jobs deploy $JobName --image $imageUri --region $Region --project=$ProjectId --service-account $ServiceAccountEmail --env-vars-file $envFile --set-secrets $secretMappings --max-retries 0 --task-timeout ${TaskTimeoutSeconds}s --args run-queue-cycle,--seed"

Remove-Item -Path $envFile -ErrorAction SilentlyContinue

Write-Host "Cloud Run Job deployed:"
Write-Host "  Job: $JobName"
Write-Host "  Region: $Region"
Write-Host "  Image: $imageUri"
