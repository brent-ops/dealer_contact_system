param(
    [string]$ProjectId = "dealer-contacts-project",
    [string]$Region = "us-central1",
    [string]$JobName = "dealer-process-watchdog",
    [string]$SchedulerName = "dealer-process-watchdog-schedule",
    [string]$Schedule = "15 2 * * *",
    [string]$TimeZone = "America/Chicago",
    [string]$InvokerServiceAccountEmail
)

$ErrorActionPreference = "Stop"

if (-not $InvokerServiceAccountEmail) {
    throw "Provide -InvokerServiceAccountEmail for the process watchdog Cloud Scheduler OAuth."
}

$uri = "https://run.googleapis.com/v2/projects/$ProjectId/locations/$Region/jobs/${JobName}:run"

Write-Host "Deploying Cloud Scheduler trigger for process watchdog..."
cmd /c "gcloud scheduler jobs describe $SchedulerName --location $Region --project=$ProjectId >nul 2>nul"
$jobExists = $LASTEXITCODE -eq 0

if ($jobExists) {
    cmd /c "gcloud scheduler jobs update http $SchedulerName --location $Region --project=$ProjectId --schedule ""$Schedule"" --time-zone ""$TimeZone"" --http-method POST --uri $uri --oauth-service-account-email $InvokerServiceAccountEmail --oauth-token-scope https://www.googleapis.com/auth/cloud-platform"
} else {
    cmd /c "gcloud scheduler jobs create http $SchedulerName --location $Region --project=$ProjectId --schedule ""$Schedule"" --time-zone ""$TimeZone"" --http-method POST --uri $uri --oauth-service-account-email $InvokerServiceAccountEmail --oauth-token-scope https://www.googleapis.com/auth/cloud-platform"
}
