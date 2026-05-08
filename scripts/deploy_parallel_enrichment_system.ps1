param(
    [string]$ProjectId = "dealer-contacts-project",
    [string]$Region = "us-central1",
    [string]$Repository = "dealer-contact-system",
    [string]$ImageName = "dealer-contact-system",
    [string]$ServiceAccountEmail,
    [string]$InvokerServiceAccountEmail,
    [int]$ValidateBatchSize = 75,
    [int]$CrawlBatchSize = 25,
    [int]$GbpBatchSize = 50,
    [int]$AiBatchSize = 7,
    [int]$AiParallelWorkers = 2,
    [int]$ContactExtractBatchSize = 25,
    [int]$BlockedRetryBatchSize = 10,
    [int]$CampaignMonitorSyncBatchSize = 1000,
    [string]$QueueManagerSchedule = "*/30 * * * *",
    [string]$ValidateSchedule = "2,17,32,47 * * * *",
    [string]$CrawlSchedule = "5,20,35,50 * * * *",
    [string]$GbpSchedule = "8,38 * * * *",
    [string]$AiSchedule = "11,41 * * * *",
    [string]$AiSecondarySchedule = "26,56 * * * *",
    [string]$ContactExtractSchedule = "14,29,44,59 * * * *",
    [string]$BlockedRetrySchedule = "17,47 * * * *",
    [string]$LeadRefreshSchedule = "23,53 * * * *",
    [string]$CampaignMonitorApiKeySecret = "campaign-monitor-api-key:latest",
    [string]$CampaignMonitorClientIdSecret = "campaign-monitor-client-id:latest",
    [string]$GeminiApiKeySecret = "gemini-api-key:latest",
    [int]$TaskTimeoutSeconds = 3600
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false
$gcloudBin = (Get-Command gcloud.cmd -ErrorAction SilentlyContinue).Source
if (-not $gcloudBin) {
    $gcloudBin = "gcloud.cmd"
}

if (-not $ServiceAccountEmail) {
    throw "Provide -ServiceAccountEmail for the parallel worker Cloud Run jobs."
}
if (-not $InvokerServiceAccountEmail) {
    throw "Provide -InvokerServiceAccountEmail for the Cloud Scheduler jobs."
}

$imageUri = "$Region-docker.pkg.dev/$ProjectId/$Repository/$ImageName`:latest"
$jobNames = @{
    queue_manager = "dealer-queue-manager"
    validate = "dealer-validate-worker"
    crawl = "dealer-crawl-worker"
    gbp = "dealer-gbp-worker"
    ai = "dealer-ai-worker"
    contact_extract = "dealer-contact-extract-worker"
    blocked_retry = "dealer-blocked-retry-worker"
    lead_refresh = "dealer-lead-refresh-worker"
}
$schedulerNames = @{
    queue_manager = "dealer-queue-manager-schedule"
    validate = "dealer-validate-worker-schedule"
    crawl = "dealer-crawl-worker-schedule"
    gbp = "dealer-gbp-worker-schedule"
    ai = "dealer-ai-worker-schedule"
    ai_secondary = "dealer-ai-worker-secondary-schedule"
    contact_extract = "dealer-contact-extract-worker-schedule"
    blocked_retry = "dealer-blocked-retry-worker-schedule"
    lead_refresh = "dealer-lead-refresh-worker-schedule"
}

function Write-EnvFile {
    param(
        [hashtable]$Values,
        [string]$Path
    )

    $Values.GetEnumerator() |
        Sort-Object Name |
        ForEach-Object {
            $value = [string]$_.Value
            $escaped = $value.Replace("'", "''")
            "$($_.Name): '$escaped'"
        } | Set-Content -Path $Path -Encoding UTF8
}

function Invoke-GcloudCommand {
    param(
        [string[]]$Arguments,
        [switch]$AllowFailure
    )

    & $gcloudBin @Arguments
    $exitCode = $LASTEXITCODE
    if (-not $AllowFailure -and $exitCode -ne 0) {
        throw "gcloud command failed with exit code ${exitCode}: gcloud $($Arguments -join ' ')"
    }
    return $exitCode
}

function Deploy-Job {
    param(
        [string]$JobName,
        [string[]]$JobArgs,
        [string]$EnvFile,
        [string]$SecretMappings
    )

    $argsString = ($JobArgs -join ",")
    Invoke-GcloudCommand -Arguments @(
        "run", "jobs", "deploy", $JobName,
        "--image", $imageUri,
        "--region", $Region,
        "--project", $ProjectId,
        "--service-account", $ServiceAccountEmail,
        "--env-vars-file", $EnvFile,
        "--set-secrets", $SecretMappings,
        "--max-retries", "0",
        "--task-timeout", "${TaskTimeoutSeconds}s",
        "--args", $argsString
    )
}

function Upsert-Scheduler {
    param(
        [string]$SchedulerName,
        [string]$JobName,
        [string]$Schedule
    )

    $uri = "https://run.googleapis.com/v2/projects/$ProjectId/locations/$Region/jobs/${JobName}:run"
    cmd /c "gcloud scheduler jobs describe $SchedulerName --location $Region --project=$ProjectId >nul 2>nul"
    $schedulerExists = ($LASTEXITCODE -eq 0)

    if ($schedulerExists) {
        Invoke-GcloudCommand -Arguments @(
            "scheduler", "jobs", "update", "http", $SchedulerName,
            "--location", $Region,
            "--project", $ProjectId,
            "--schedule", $Schedule,
            "--http-method", "POST",
            "--uri", $uri,
            "--oauth-service-account-email", $InvokerServiceAccountEmail,
            "--oauth-token-scope", "https://www.googleapis.com/auth/cloud-platform"
        )
    } else {
        Invoke-GcloudCommand -Arguments @(
            "scheduler", "jobs", "create", "http", $SchedulerName,
            "--location", $Region,
            "--project", $ProjectId,
            "--schedule", $Schedule,
            "--http-method", "POST",
            "--uri", $uri,
            "--oauth-service-account-email", $InvokerServiceAccountEmail,
            "--oauth-token-scope", "https://www.googleapis.com/auth/cloud-platform"
        )
    }
}

$sharedEnv = @{
    APP_ENVIRONMENT = "cloud"
    BIGQUERY_PROJECT_ID = $ProjectId
    BIGQUERY_DATASET = "dealer_data"
    PARALLEL_ENRICHMENT_ENABLED = "true"
    QUEUE_MANAGER_ENABLED = "true"
    CAMPAIGN_MONITOR_SYNC_ENABLED = "true"
    CAMPAIGN_MONITOR_SYNC_BATCH_SIZE = "$CampaignMonitorSyncBatchSize"
    VALIDATE_WORKER_BATCH_SIZE = "$ValidateBatchSize"
    ENRICH_WORKER_BATCH_SIZE = "$CrawlBatchSize"
    GBP_WORKER_BATCH_SIZE = "$GbpBatchSize"
    AI_RETRIEVAL_BATCH_SIZE = "$AiBatchSize"
    AI_PARALLEL_WORKERS = "$AiParallelWorkers"
    EXTRACT_WORKER_BATCH_SIZE = "$ContactExtractBatchSize"
    RETRY_BLOCKED_WORKER_BATCH_SIZE = "$BlockedRetryBatchSize"
    AI_RETRIEVAL_ENABLED = "true"
    AI_RETRIEVAL_PROVIDER_ORDER = "gemini,openai"
    AI_RETRIEVAL_MIN_RUN_INTERVAL_MINUTES = "30"
    AI_RETRIEVAL_COOLDOWN_HOURS = "72"
    AI_RETRIEVAL_RATE_LIMIT_COOLDOWN_MINUTES = "180"
    AI_RETRIEVAL_REQUEST_DELAY_SECONDS = "2"
    AI_RETRIEVAL_PROMPT_STYLE = "simple_staff"
    GEMINI_ENABLED = "true"
    GEMINI_MODEL = "gemini-2.5-flash"
    OPENAI_ENABLED = "false"
    OPENAI_MODEL = "gpt-5.4"
    GBP_ENRICHMENT_ENABLED = "true"
    GBP_PROVIDER = "duckduckgo_search_fallback"
    DOMAIN_DISCOVERY_ENABLED = "true"
    DOMAIN_DISCOVERY_JOB_NAME = "dealer-domain-discovery-worker"
    PROCESS_WATCHDOG_ENABLED = "true"
    PROCESS_WATCHDOG_JOB_NAME = "dealer-process-watchdog"
    QUEUE_MANAGER_JOB_NAME = $jobNames.queue_manager
    QUEUE_MANAGER_SCHEDULER_NAME = $schedulerNames.queue_manager
    VALIDATE_JOB_NAME = $jobNames.validate
    VALIDATE_SCHEDULER_NAME = $schedulerNames.validate
    CRAWL_JOB_NAME = $jobNames.crawl
    CRAWL_SCHEDULER_NAME = $schedulerNames.crawl
    GBP_JOB_NAME = $jobNames.gbp
    GBP_SCHEDULER_NAME = $schedulerNames.gbp
    AI_JOB_NAME = $jobNames.ai
    AI_SCHEDULER_NAME = $schedulerNames.ai
    CONTACT_EXTRACT_JOB_NAME = $jobNames.contact_extract
    CONTACT_EXTRACT_SCHEDULER_NAME = $schedulerNames.contact_extract
    BLOCKED_RETRY_JOB_NAME = $jobNames.blocked_retry
    BLOCKED_RETRY_SCHEDULER_NAME = $schedulerNames.blocked_retry
    LEAD_REFRESH_JOB_NAME = $jobNames.lead_refresh
    LEAD_REFRESH_SCHEDULER_NAME = $schedulerNames.lead_refresh
    CLIENT_DIM_ENABLED = if ($env:CLIENT_DIM_ENABLED) { $env:CLIENT_DIM_ENABLED } else { "true" }
    GLD_ACCOUNTABILITY_PROJECT_ID = if ($env:GLD_ACCOUNTABILITY_PROJECT_ID) { $env:GLD_ACCOUNTABILITY_PROJECT_ID } else { "productivity-project-491503" }
    CLIENT_DIM_DATASET = if ($env:CLIENT_DIM_DATASET) { $env:CLIENT_DIM_DATASET } else { "accountability_v1" }
    CLIENT_DIM_TABLE = if ($env:CLIENT_DIM_TABLE) { $env:CLIENT_DIM_TABLE } else { "dim_clients" }
}

Write-Host "Ensuring Artifact Registry repository exists..."
cmd /c "gcloud artifacts repositories describe $Repository --location=$Region --project=$ProjectId >nul 2>nul"
if ($LASTEXITCODE -ne 0) {
    cmd /c "gcloud artifacts repositories create $Repository --repository-format=docker --location=$Region --description=""Dealer contact system images"" --project=$ProjectId"
}

Write-Host "Building shared worker image..."
cmd /c "gcloud builds submit --tag $imageUri --project=$ProjectId"

$secretMappings = @(
    "CAMPAIGN_MONITOR_API_KEY=$CampaignMonitorApiKeySecret",
    "CAMPAIGN_MONITOR_CLIENT_ID=$CampaignMonitorClientIdSecret",
    "GEMINI_API_KEY=$GeminiApiKeySecret"
) -join ","

$jobArgs = @{
    queue_manager = @("run-lane-manager")
    validate = @("run-lane-worker", "--lane", "validate")
    crawl = @("run-lane-worker", "--lane", "crawl")
    gbp = @("run-lane-worker", "--lane", "gbp")
    ai = @("run-lane-worker", "--lane", "ai")
    contact_extract = @("run-lane-worker", "--lane", "contact_extract")
    blocked_retry = @("run-lane-worker", "--lane", "blocked_retry")
    lead_refresh = @("run-lane-worker", "--lane", "lead_refresh")
}

foreach ($lane in $jobArgs.Keys) {
    $envFile = Join-Path $env:TEMP "dealer-$lane-worker-env.yaml"
    Write-EnvFile -Values $sharedEnv -Path $envFile
    Write-Host "Deploying $lane job..."
    Deploy-Job -JobName $jobNames[$lane] -JobArgs $jobArgs[$lane] -EnvFile $envFile -SecretMappings $secretMappings
    Remove-Item -Path $envFile -ErrorAction SilentlyContinue
}

Upsert-Scheduler -SchedulerName $schedulerNames.queue_manager -JobName $jobNames.queue_manager -Schedule $QueueManagerSchedule
Upsert-Scheduler -SchedulerName $schedulerNames.validate -JobName $jobNames.validate -Schedule $ValidateSchedule
Upsert-Scheduler -SchedulerName $schedulerNames.crawl -JobName $jobNames.crawl -Schedule $CrawlSchedule
Upsert-Scheduler -SchedulerName $schedulerNames.gbp -JobName $jobNames.gbp -Schedule $GbpSchedule
Upsert-Scheduler -SchedulerName $schedulerNames.ai -JobName $jobNames.ai -Schedule $AiSchedule
Upsert-Scheduler -SchedulerName $schedulerNames.ai_secondary -JobName $jobNames.ai -Schedule $AiSecondarySchedule
Upsert-Scheduler -SchedulerName $schedulerNames.contact_extract -JobName $jobNames.contact_extract -Schedule $ContactExtractSchedule
Upsert-Scheduler -SchedulerName $schedulerNames.blocked_retry -JobName $jobNames.blocked_retry -Schedule $BlockedRetrySchedule
Upsert-Scheduler -SchedulerName $schedulerNames.lead_refresh -JobName $jobNames.lead_refresh -Schedule $LeadRefreshSchedule

Write-Host "Pausing legacy sequential worker scheduler..."
Invoke-GcloudCommand -Arguments @(
    "scheduler", "jobs", "pause", "dealer-contact-worker-schedule",
    "--location", $Region,
    "--project", $ProjectId
) -AllowFailure | Out-Null

Write-Host "Parallel enrichment system deployed."
