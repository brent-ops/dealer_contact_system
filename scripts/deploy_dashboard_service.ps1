param(
  [string]$ProjectId = "dealer-contacts-project",
  [string]$Region = "us-central1",
  [string]$Repository = "dealer-contact-system",
  [string]$ImageName = "dealer-contact-dashboard",
  [string]$ServiceName = "dealer-contact-dashboard",
  [bool]$AiRetrievalEnabled = $true,
  [bool]$ParallelEnrichmentEnabled = $true,
  [bool]$QueueManagerEnabled = $true,
  [bool]$DomainDiscoveryEnabled = $true,
  [bool]$ProcessWatchdogEnabled = $true,
  [bool]$GeminiEnabled = $true,
  [string]$GeminiModel = "gemini-2.5-flash",
  [string]$DomainDiscoveryScheduleHint = "Every 30 minutes",
  [string]$DomainDiscoveryJobName = "dealer-domain-discovery-worker",
  [string]$DomainDiscoverySchedulerName = "dealer-domain-discovery-schedule",
  [string]$DomainDiscoveryVpcConnector = "dealer-discovery-conn",
  [string]$DomainDiscoveryVpcEgress = "all-traffic",
  [string]$DomainDiscoveryEgressIp = "34.45.226.9",
  [string]$QueueManagerJobName = "dealer-queue-manager",
  [string]$ValidateJobName = "dealer-validate-worker",
  [string]$CrawlJobName = "dealer-crawl-worker",
  [string]$GbpJobName = "dealer-gbp-worker",
  [string]$AiJobName = "dealer-ai-worker",
  [string]$ContactExtractJobName = "dealer-contact-extract-worker",
  [string]$BlockedRetryJobName = "dealer-blocked-retry-worker",
  [string]$LeadRefreshJobName = "dealer-lead-refresh-worker",
  [string]$CampaignMonitorApiKeySecret = "campaign-monitor-api-key:latest",
  [string]$CampaignMonitorClientIdSecret = "campaign-monitor-client-id:latest",
  [string]$GeminiApiKeySecret = "gemini-api-key:latest",
  [Parameter(Mandatory = $true)]
  [string]$ServiceAccountEmail
)

$imageUri = "$Region-docker.pkg.dev/$ProjectId/$Repository/$ImageName`:latest"
$envVars = @(
  "APP_ENVIRONMENT=cloud"
  "BIGQUERY_PROJECT_ID=$ProjectId"
  "BIGQUERY_DATASET=dealer_data"
  "AI_RETRIEVAL_ENABLED=$($AiRetrievalEnabled.ToString().ToLower())"
  "PARALLEL_ENRICHMENT_ENABLED=$($ParallelEnrichmentEnabled.ToString().ToLower())"
  "QUEUE_MANAGER_ENABLED=$($QueueManagerEnabled.ToString().ToLower())"
  "DOMAIN_DISCOVERY_ENABLED=$($DomainDiscoveryEnabled.ToString().ToLower())"
  "PROCESS_WATCHDOG_ENABLED=$($ProcessWatchdogEnabled.ToString().ToLower())"
  "GEMINI_ENABLED=$($GeminiEnabled.ToString().ToLower())"
  "GEMINI_MODEL=$GeminiModel"
  "DOMAIN_DISCOVERY_SCHEDULE_HINT=$DomainDiscoveryScheduleHint"
  "DOMAIN_DISCOVERY_JOB_NAME=$DomainDiscoveryJobName"
  "DOMAIN_DISCOVERY_SCHEDULER_NAME=$DomainDiscoverySchedulerName"
  "DOMAIN_DISCOVERY_VPC_CONNECTOR=$DomainDiscoveryVpcConnector"
  "DOMAIN_DISCOVERY_VPC_EGRESS=$DomainDiscoveryVpcEgress"
  "DOMAIN_DISCOVERY_EGRESS_IP=$DomainDiscoveryEgressIp"
  "QUEUE_MANAGER_JOB_NAME=$QueueManagerJobName"
  "VALIDATE_JOB_NAME=$ValidateJobName"
  "CRAWL_JOB_NAME=$CrawlJobName"
  "GBP_JOB_NAME=$GbpJobName"
  "AI_JOB_NAME=$AiJobName"
  "CONTACT_EXTRACT_JOB_NAME=$ContactExtractJobName"
  "BLOCKED_RETRY_JOB_NAME=$BlockedRetryJobName"
  "LEAD_REFRESH_JOB_NAME=$LeadRefreshJobName"
)

if ($env:META_ACCESS_TOKEN) {
  $envVars += "META_ACCESS_TOKEN=$($env:META_ACCESS_TOKEN)"
}
if ($env:META_AD_ACCOUNT_ID) {
  $envVars += "META_AD_ACCOUNT_ID=$($env:META_AD_ACCOUNT_ID)"
}
if ($env:GOOGLE_ADS_DEVELOPER_TOKEN) {
  $envVars += "GOOGLE_ADS_DEVELOPER_TOKEN=$($env:GOOGLE_ADS_DEVELOPER_TOKEN)"
}
if ($env:GOOGLE_ADS_CUSTOMER_ID) {
  $envVars += "GOOGLE_ADS_CUSTOMER_ID=$($env:GOOGLE_ADS_CUSTOMER_ID)"
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
if ($env:MANAGED_FETCH_PROVIDER) {
  $envVars += "MANAGED_FETCH_PROVIDER=$($env:MANAGED_FETCH_PROVIDER)"
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
if ($env:OPENAI_ENABLED) {
  $envVars += "OPENAI_ENABLED=$($env:OPENAI_ENABLED)"
}
if ($env:OPENAI_API_KEY) {
  $envVars += "OPENAI_API_KEY=$($env:OPENAI_API_KEY)"
}
if ($env:OPENAI_MODEL) {
  $envVars += "OPENAI_MODEL=$($env:OPENAI_MODEL)"
}

$envFile = Join-Path $env:TEMP "dealer-contact-dashboard-env.yaml"
$envVars |
  ForEach-Object {
    $parts = $_ -split "=", 2
    $name = $parts[0]
    $value = if ($parts.Length -gt 1) { [string]$parts[1] } else { "" }
    $escaped = $value.Replace("'", "''")
    "${name}: '$escaped'"
  } | Set-Content -Path $envFile -Encoding UTF8

$secretMappings = @(
  "CAMPAIGN_MONITOR_API_KEY=$CampaignMonitorApiKeySecret",
  "CAMPAIGN_MONITOR_CLIENT_ID=$CampaignMonitorClientIdSecret",
  "GEMINI_API_KEY=$GeminiApiKeySecret"
) -join ","

gcloud builds submit `
  --project $ProjectId `
  --config cloudbuild.dashboard.yaml

gcloud run deploy $ServiceName `
  --image $imageUri `
  --region $Region `
  --project $ProjectId `
  --service-account $ServiceAccountEmail `
  --allow-unauthenticated `
  --port 8080 `
  --env-vars-file $envFile `
  --set-secrets $secretMappings

Remove-Item -Path $envFile -ErrorAction SilentlyContinue
