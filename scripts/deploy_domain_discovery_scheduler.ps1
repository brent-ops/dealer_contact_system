param(
    [string]$ProjectId = "dealer-contacts-project",
    [string]$Region = "us-central1",
    [string]$JobName = "dealer-domain-discovery-worker",
    [string]$SchedulerName = "dealer-domain-discovery-schedule",
    [string]$Schedule = "*/30 * * * *",
    [string]$InvokerServiceAccountEmail
)

$ErrorActionPreference = "Stop"

if (-not $InvokerServiceAccountEmail) {
    throw "Provide -InvokerServiceAccountEmail for the discovery Cloud Scheduler OAuth."
}

$uri = "https://run.googleapis.com/v2/projects/$ProjectId/locations/$Region/jobs/${JobName}:run"

Write-Host "Deploying Cloud Scheduler trigger for discovery..."
cmd /c "gcloud scheduler jobs describe $SchedulerName --location $Region --project=$ProjectId >nul 2>nul"
$jobExists = $LASTEXITCODE -eq 0

if ($jobExists) {
    cmd /c "gcloud scheduler jobs update http $SchedulerName --location $Region --project=$ProjectId --schedule ""$Schedule"" --http-method POST --uri $uri --oauth-service-account-email $InvokerServiceAccountEmail --oauth-token-scope https://www.googleapis.com/auth/cloud-platform"
} else {
    cmd /c "gcloud scheduler jobs create http $SchedulerName --location $Region --project=$ProjectId --schedule ""$Schedule"" --http-method POST --uri $uri --oauth-service-account-email $InvokerServiceAccountEmail --oauth-token-scope https://www.googleapis.com/auth/cloud-platform"
}
