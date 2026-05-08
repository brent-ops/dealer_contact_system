"""Nightly watchdog for stale pipeline processes and self-healing restarts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any

import google.auth
from google.auth.transport.requests import AuthorizedSession

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.dashboard_service import DashboardService
from app.logging_utils import get_logger
from app.services.campaign_monitor import CampaignMonitorService
from app.services.client_dim import ClientDimService
from app.services.prospect_leads import ProspectLeadService


logger = get_logger(__name__)


@dataclass(frozen=True)
class WatchdogCheck:
    """One pipeline-health check outcome."""

    name: str
    status: str
    detail: str
    action: str = "none"


@dataclass(frozen=True)
class ProcessWatchdogResult:
    """Summary from one watchdog pass."""

    status: str
    detail: str
    checks: tuple[WatchdogCheck, ...]
    main_job_triggered: bool
    discovery_job_triggered: bool
    repaired_materializations: bool


class CloudRunJobLauncher:
    """Execute Cloud Run Jobs using Application Default Credentials."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.session = AuthorizedSession(credentials)

    def execute_job(self, job_name: str) -> dict[str, Any]:
        """Execute one Cloud Run job and return the operation payload."""

        url = (
            "https://run.googleapis.com/v2/projects/"
            f"{self.settings.bigquery_project_id}/locations/{self.settings.cloud_run_region}/jobs/{job_name}:run"
        )
        response = self.session.post(url, json={})
        response.raise_for_status()
        payload = response.json()
        logger.info("Triggered Cloud Run job | job_name=%s | operation=%s", job_name, payload.get("name"))
        return payload


class ProcessWatchdogService:
    """Check for stale pipeline lanes and repair or relaunch them."""

    CONNECTION_RECORD_ID = "process_watchdog"
    MAIN_TASKS = ("validate", "enrich", "enrich_gbp", "ai_account_facts", "extract_contacts", "retry_blocked")
    PARALLEL_LANE_RESTART_COOLDOWN_MINUTES = {
        "validate": 45,
        "crawl": 30,
        "gbp": 45,
        "ai": 45,
        "contact_extract": 30,
        "blocked_retry": 45,
        "lead_refresh": 30,
    }

    def __init__(
        self,
        repository: BigQueryRepository,
        settings: Settings,
        prospect_lead_service: ProspectLeadService,
        client_dim_service: ClientDimService,
        dashboard_service: DashboardService,
        campaign_monitor_service: CampaignMonitorService,
    ) -> None:
        self.repository = repository
        self.settings = settings
        self.prospect_lead_service = prospect_lead_service
        self.client_dim_service = client_dim_service
        self.dashboard_service = dashboard_service
        self.campaign_monitor_service = campaign_monitor_service
        self._job_launcher: CloudRunJobLauncher | None = None

    def run(self, dry_run: bool = False) -> ProcessWatchdogResult:
        """Check process freshness, repair stale materializations, and relaunch stale jobs."""

        try:
            if not self.settings.process_watchdog_enabled:
                result = ProcessWatchdogResult(
                    status="skipped",
                    detail="Process watchdog is disabled.",
                    checks=tuple(),
                    main_job_triggered=False,
                    discovery_job_triggered=False,
                    repaired_materializations=False,
                )
                if not dry_run:
                    self._record_status("warning", result.detail)
                return result

            checks: list[WatchdogCheck] = []
            main_job_triggered = False
            discovery_job_triggered = False
            repaired_materializations = False
            lane_restart_specs = (
                ("validate", "Validate", self.settings.validate_job_name),
                ("crawl", "Crawl", self.settings.crawl_job_name),
                ("gbp", "GBP Enrichment", self.settings.gbp_job_name),
                ("ai", "AI Retrieval", self.settings.ai_job_name),
                ("contact_extract", "Contact Extraction", self.settings.contact_extract_job_name),
                ("blocked_retry", "Blocked Retry", self.settings.blocked_retry_job_name),
                ("lead_refresh", "Lead Refresh", self.settings.lead_refresh_job_name),
            ) if self.settings.parallel_enrichment_enabled else tuple()

            health_rows = {
                row["lane_key"]: row
                for row in self.dashboard_service.get_process_health_rows()
            }
            main_check = self._process_row_to_check(
                health_rows.get("main_worker"),
                fallback_name="Main Worker",
                fallback_detail="Main worker health data is unavailable.",
            )
            checks.append(main_check)

            lane_check_indexes: dict[str, int] = {}
            for lane_key, lane_label, _job_name in lane_restart_specs:
                lane_check_indexes[lane_key] = len(checks)
                checks.append(
                    self._process_row_to_check(
                        health_rows.get(lane_key),
                        fallback_name=lane_label,
                        fallback_detail=f"{lane_label} health data is unavailable.",
                    )
                )

            discovery_check = self._process_row_to_check(
                health_rows.get("discovery_search"),
                fallback_name="Discovery Worker",
                fallback_detail="Discovery worker health data is unavailable.",
            )
            checks.append(discovery_check)

            queue_manager_check = self._check_system_target(
                target_system="parallel_cutover",
                name="Queue Manager",
                stale_hours=self.settings.process_watchdog_main_stale_hours,
                disabled_detail="Queue manager is disabled.",
                enabled=self.settings.queue_manager_enabled,
            )
            checks.append(queue_manager_check)

            prospect_check = self._process_row_to_check(
                health_rows.get("prospect_lead_refresh"),
                fallback_name="Prospect Lead Refresh",
                fallback_detail="Prospect lead refresh health data is unavailable.",
            )
            checks.append(prospect_check)

            snapshot_check = self._process_row_to_check(
                health_rows.get("dashboard_snapshot"),
                fallback_name="Dashboard Snapshot",
                fallback_detail="Dashboard snapshot health data is unavailable.",
            )
            checks.append(snapshot_check)

            campaign_monitor_check = self._process_row_to_check(
                health_rows.get("campaign_monitor_sync"),
                fallback_name="Campaign Monitor Sync",
                fallback_detail="Campaign Monitor sync health data is unavailable.",
            )
            checks.append(campaign_monitor_check)

            any_stale = any(check.status in {"stale", "failed"} for check in checks)
            any_warning = any(check.status == "warning" for check in checks)

            if prospect_check.status == "stale":
                if dry_run:
                    checks[-3] = WatchdogCheck(prospect_check.name, "stale", prospect_check.detail, action="dry_run")
                else:
                    self.prospect_lead_service.refresh(dry_run=False)
                    self.client_dim_service.refresh(dry_run=False)
                    self.dashboard_service.capture_snapshot()
                    repaired_materializations = True
                    checks[-3] = WatchdogCheck(
                        prospect_check.name,
                        "repaired",
                        prospect_check.detail,
                        action="refreshed_prospect_leads_and_snapshot",
                    )
                    if snapshot_check.status == "stale":
                        checks[-2] = WatchdogCheck(
                            snapshot_check.name,
                            "repaired",
                            snapshot_check.detail,
                            action="captured_dashboard_snapshot",
                        )

            if snapshot_check.status == "stale" and not repaired_materializations:
                if dry_run:
                    checks[-2] = WatchdogCheck(snapshot_check.name, "stale", snapshot_check.detail, action="dry_run")
                else:
                    self.dashboard_service.capture_snapshot()
                    repaired_materializations = True
                    checks[-2] = WatchdogCheck(
                        snapshot_check.name,
                        "repaired",
                        snapshot_check.detail,
                        action="captured_dashboard_snapshot",
                    )

            if self.settings.parallel_enrichment_enabled:
                for lane_key, _lane_label, job_name in lane_restart_specs:
                    lane_index = lane_check_indexes[lane_key]
                    lane_check = checks[lane_index]
                    if lane_check.status not in {"stale", "failed"}:
                        continue
                    if dry_run:
                        checks[lane_index] = WatchdogCheck(lane_check.name, lane_check.status, lane_check.detail, action="dry_run")
                    else:
                        self._job_launcher_instance().execute_job(job_name)
                        main_job_triggered = True
                        checks[lane_index] = WatchdogCheck(
                            lane_check.name,
                            "restarted",
                            lane_check.detail,
                            action=f"executed_{job_name}",
                        )
            elif main_check.status in {"stale", "failed"}:
                if dry_run:
                    checks[0] = WatchdogCheck(main_check.name, main_check.status, main_check.detail, action="dry_run")
                else:
                    self._job_launcher_instance().execute_job(self.settings.cloud_run_job_name)
                    main_job_triggered = True
                    checks[0] = WatchdogCheck(
                        main_check.name,
                        "restarted",
                        main_check.detail,
                        action=f"executed_{self.settings.cloud_run_job_name}",
                    )

            if discovery_check.status in {"stale", "failed"}:
                if dry_run:
                    checks[1] = WatchdogCheck(discovery_check.name, discovery_check.status, discovery_check.detail, action="dry_run")
                else:
                    self._job_launcher_instance().execute_job(self.settings.domain_discovery_job_name)
                    discovery_job_triggered = True
                    checks[1] = WatchdogCheck(
                        discovery_check.name,
                        "restarted",
                        discovery_check.detail,
                        action=f"executed_{self.settings.domain_discovery_job_name}",
                    )

            if queue_manager_check.status in {"stale", "failed"}:
                if dry_run:
                    checks[2] = WatchdogCheck(queue_manager_check.name, queue_manager_check.status, queue_manager_check.detail, action="dry_run")
                else:
                    self._job_launcher_instance().execute_job(self.settings.queue_manager_job_name)
                    checks[2] = WatchdogCheck(
                        queue_manager_check.name,
                        "restarted",
                        queue_manager_check.detail,
                        action=f"executed_{self.settings.queue_manager_job_name}",
                    )

            if campaign_monitor_check.status in {"stale", "failed"} and self.settings.campaign_monitor_sync_enabled:
                if dry_run:
                    checks[-1] = WatchdogCheck(campaign_monitor_check.name, campaign_monitor_check.status, campaign_monitor_check.detail, action="dry_run")
                elif not main_job_triggered:
                    self._job_launcher_instance().execute_job(self.settings.cloud_run_job_name)
                    main_job_triggered = True
                    checks[-1] = WatchdogCheck(
                        campaign_monitor_check.name,
                        "restarted",
                        campaign_monitor_check.detail,
                        action=f"executed_{self.settings.cloud_run_job_name}_for_campaign_monitor",
                    )

            if any(check.status in {"restarted", "repaired"} for check in checks):
                status = "repaired"
            elif any_stale:
                status = "warning" if dry_run else "stale"
            elif any_warning:
                status = "warning"
            else:
                status = "healthy"

            detail = self._build_detail(checks)
            result = ProcessWatchdogResult(
                status=status,
                detail=detail,
                checks=tuple(checks),
                main_job_triggered=main_job_triggered,
                discovery_job_triggered=discovery_job_triggered,
                repaired_materializations=repaired_materializations,
            )
            if not dry_run:
                sync_status = "healthy" if status in {"healthy", "repaired"} else "warning"
                self._record_status(sync_status, detail)
            return result
        except Exception as exc:
            if not dry_run:
                self._record_status("error", f"Process watchdog failed: {exc}")
            raise

    def _check_main_pipeline(self) -> WatchdogCheck:
        """Return whether the main worker tasks look fresh."""

        tasks_sql = ", ".join(f"'{task}'" for task in self.MAIN_TASKS)
        query = f"""
        WITH per_task AS (
          SELECT
            task_type,
            MAX(IF(run_status = 'completed', completed_at, NULL)) AS last_completed_at
          FROM `{self.settings.pipeline_runs_table_fqn}`
          WHERE task_type IN ({tasks_sql})
          GROUP BY task_type
        ),
        running_state AS (
          SELECT
            MAX(started_at) AS last_running_started_at
          FROM `{self.settings.pipeline_runs_table_fqn}`
          WHERE run_status = 'running'
        )
        SELECT
          ARRAY_AGG(
            STRUCT(task_type AS task_type, last_completed_at AS last_completed_at)
            ORDER BY task_type
          ) AS task_rows,
          (SELECT last_running_started_at FROM running_state) AS last_running_started_at
        FROM per_task
        """
        row = self.repository.fetch_one(query)
        task_rows = row.get("task_rows") or []
        stale_tasks: list[str] = []
        seen_tasks = {str(task_row.get("task_type")) for task_row in task_rows}
        for task_type in self.MAIN_TASKS:
            if task_type not in seen_tasks:
                stale_tasks.append(task_type)
        for task_row in task_rows:
            last_completed_at = task_row.get("last_completed_at")
            if self._is_timestamp_stale(last_completed_at, self.settings.process_watchdog_main_stale_hours):
                stale_tasks.append(str(task_row.get("task_type")))

        last_running_started_at = row.get("last_running_started_at")
        if stale_tasks and self._has_recent_running_job(last_running_started_at):
            return WatchdogCheck(
                "Main Worker",
                "warning",
                "Main worker has stale task timestamps, but a current execution is still within the running grace window.",
            )
        if stale_tasks:
            return WatchdogCheck(
                "Main Worker",
                "stale",
                f"Stale task lanes: {', '.join(stale_tasks)}.",
            )
        return WatchdogCheck("Main Worker", "healthy", "Recent queue-backed task runs are fresh.")

    def _check_discovery_pipeline(self) -> WatchdogCheck:
        """Return whether discovery looks fresh."""

        query = f"""
        SELECT
          MAX(IF(run_status IN ('completed', 'partial_failure'), completed_at, NULL)) AS last_completed_at,
          MAX(IF(run_status = 'running', started_at, NULL)) AS last_running_started_at
        FROM `{self.settings.domain_discovery_runs_table_fqn}`
        WHERE run_type = 'search'
        """
        row = self.repository.fetch_one(query)
        last_completed_at = row.get("last_completed_at")
        if self._is_timestamp_stale(last_completed_at, self.settings.process_watchdog_discovery_stale_hours):
            if self._has_recent_running_job(row.get("last_running_started_at")):
                return WatchdogCheck(
                    "Discovery Worker",
                    "warning",
                    "Discovery timestamps are stale, but a discovery run is still within the running grace window.",
                )
            return WatchdogCheck(
                "Discovery Worker",
                "stale",
                "No recent successful discovery search run was recorded within the expected window.",
            )
        return WatchdogCheck("Discovery Worker", "healthy", "Discovery runs are arriving within the expected window.")

    def _check_prospect_leads(self) -> WatchdogCheck:
        """Return whether prospect lead materialization looks fresh."""

        query = f"""
        SELECT
          MAX(updated_at) AS last_updated_at
        FROM `{self.settings.prospect_leads_table_fqn}`
        """
        row = self.repository.fetch_one(query)
        last_updated_at = row.get("last_updated_at")
        if self._is_timestamp_stale(last_updated_at, self.settings.process_watchdog_prospect_leads_stale_hours):
            return WatchdogCheck(
                "Prospect Lead Refresh",
                "stale",
                "Prospect leads look stale, which can leave sales-ready and marketing-ready views behind.",
            )
        return WatchdogCheck("Prospect Lead Refresh", "healthy", "Prospect leads were refreshed recently.")

    def _check_dashboard_snapshot(self) -> WatchdogCheck:
        """Return whether dashboard snapshots are being captured."""

        query = f"""
        SELECT
          MAX(snapshot_at) AS last_snapshot_at
        FROM `{self.settings.dashboard_snapshots_table_fqn}`
        """
        row = self.repository.fetch_one(query)
        last_snapshot_at = row.get("last_snapshot_at")
        if self._is_timestamp_stale(last_snapshot_at, self.settings.process_watchdog_snapshot_stale_hours):
            return WatchdogCheck(
                "Dashboard Snapshot",
                "stale",
                "Dashboard snapshots have not been captured in the expected time window.",
            )
        return WatchdogCheck("Dashboard Snapshot", "healthy", "Dashboard snapshots are current.")

    def _check_campaign_monitor(self) -> WatchdogCheck:
        """Return whether Campaign Monitor sync freshness looks healthy."""

        if not self.settings.campaign_monitor_sync_enabled:
            return WatchdogCheck("Campaign Monitor Sync", "warning", "Campaign Monitor sync is disabled.")

        query = f"""
        SELECT
          MAX(last_synced_at) AS last_synced_at
        FROM `{self.settings.sync_targets_table_fqn}`
        WHERE target_system = 'campaign_monitor'
        """
        row = self.repository.fetch_one(query)
        last_synced_at = row.get("last_synced_at")
        if self._is_timestamp_stale(last_synced_at, self.settings.process_watchdog_campaign_monitor_stale_hours):
            return WatchdogCheck(
                "Campaign Monitor Sync",
                "stale",
                "Campaign Monitor has not recorded a fresh sync within the expected window.",
            )
        return WatchdogCheck("Campaign Monitor Sync", "healthy", "Campaign Monitor sync activity is current.")

    def _check_system_target(
        self,
        *,
        target_system: str,
        name: str,
        stale_hours: int,
        disabled_detail: str,
        enabled: bool,
    ) -> WatchdogCheck:
        """Return freshness for a sync-target heartbeat-based system row."""

        if not enabled:
            return WatchdogCheck(name, "healthy", disabled_detail)
        query = f"""
        SELECT
          MAX(last_synced_at) AS last_synced_at,
          ANY_VALUE(sync_status) AS sync_status
        FROM `{self.settings.sync_targets_table_fqn}`
        WHERE target_system = '{self._escape_sql(target_system)}'
        """
        row = self.repository.fetch_one(query)
        last_synced_at = row.get("last_synced_at")
        sync_status = str(row.get("sync_status") or "").lower()
        if sync_status in {"failed", "error"}:
            return WatchdogCheck(name, "failed", f"{name} reported {sync_status}.")
        if self._is_timestamp_stale(last_synced_at, stale_hours):
            return WatchdogCheck(name, "stale", f"{name} has not reported recently.")
        return WatchdogCheck(name, "healthy", f"{name} is reporting within the expected window.")

    def _has_recent_running_job(self, timestamp_value: Any) -> bool:
        """Return whether a running job started recently enough to avoid duplicate restarts."""

        if not timestamp_value:
            return False
        timestamp = self._coerce_datetime(timestamp_value)
        if not timestamp:
            return False
        elapsed_seconds = (datetime.now(timezone.utc) - timestamp).total_seconds()
        return elapsed_seconds <= (self.settings.process_watchdog_running_grace_minutes * 60)

    def _is_timestamp_stale(self, timestamp_value: Any, stale_hours: int) -> bool:
        """Return whether the given timestamp is older than the configured stale window."""

        if stale_hours <= 0:
            return False
        timestamp = self._coerce_datetime(timestamp_value)
        if not timestamp:
            return True
        elapsed_seconds = (datetime.now(timezone.utc) - timestamp).total_seconds()
        return elapsed_seconds > (stale_hours * 3600)

    def _coerce_datetime(self, value: Any) -> datetime | None:
        """Normalize a BigQuery timestamp-like value into an aware datetime."""

        if not value:
            return None
        if isinstance(value, datetime):
            timestamp = value
        else:
            try:
                timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except ValueError:
                return None
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc)

    def _build_detail(self, checks: list[WatchdogCheck]) -> str:
        """Build one compact JSON-like detail string for logs and CLI output."""

        summary = [
            {
                "name": check.name,
                "status": check.status,
                "action": check.action,
            }
            for check in checks
        ]
        return json.dumps(summary, separators=(",", ":"))

    def _process_row_to_check(
        self,
        row: dict[str, Any] | None,
        *,
        fallback_name: str,
        fallback_detail: str,
    ) -> WatchdogCheck:
        """Translate one shared process-health row into a watchdog check."""

        if not row:
            return WatchdogCheck(fallback_name, "warning", fallback_detail)
        status = str(row.get("status") or "warning").lower()
        detail = str(row.get("detail") or fallback_detail)
        if status in {"stale", "failed"} and self._has_recent_lane_start(row):
            status = "warning"
            detail = f"{detail} Recent lane activity was detected, so watchdog restart is deferred."
        if status == "backlogged":
            status = "warning"
        if status == "flat":
            status = "healthy"
        return WatchdogCheck(str(row.get("label") or fallback_name), status, detail)

    def _has_recent_lane_start(self, row: dict[str, Any]) -> bool:
        """Return whether the lane has started recently enough to avoid forced relaunch."""

        lane_key = str(row.get("lane_key") or "").strip()
        if not lane_key:
            return False
        cooldown_minutes = self.PARALLEL_LANE_RESTART_COOLDOWN_MINUTES.get(
            lane_key,
            self.settings.process_watchdog_running_grace_minutes,
        )
        last_started_at = row.get("last_started_at")
        if last_started_at and self._started_within_minutes(last_started_at, cooldown_minutes):
            return True
        in_progress_count = int(row.get("in_progress_count", 0) or 0)
        running_count = int(row.get("running_count", 0) or 0)
        return (in_progress_count > 0 or running_count > 0) and self._started_within_minutes(
            last_started_at,
            cooldown_minutes,
        )

    def _started_within_minutes(self, timestamp_value: Any, minutes: int) -> bool:
        """Return whether the given timestamp is within the requested minute window."""

        if minutes <= 0:
            return False
        timestamp = self._coerce_datetime(timestamp_value)
        if not timestamp:
            return False
        elapsed_seconds = (datetime.now(timezone.utc) - timestamp).total_seconds()
        return elapsed_seconds <= (minutes * 60)

    def _record_status(self, sync_status: str, sync_detail: str) -> None:
        """Upsert the latest watchdog status into sync_targets for dashboard visibility."""

        query = f"""
        MERGE `{self.settings.sync_targets_table_fqn}` AS target
        USING (
          SELECT
            'process_watchdog' AS target_system,
            'system_watchdog' AS target_entity_type,
            '{self._escape_sql(self.settings.process_watchdog_job_name)}' AS target_entity_id,
            'system' AS source_record_type,
            '{self.CONNECTION_RECORD_ID}' AS source_record_id,
            '{self._escape_sql(sync_status)}' AS sync_status,
            '{self._escape_sql(sync_detail)}' AS sync_detail
        ) AS source
        ON target.target_system = source.target_system
           AND target.source_record_type = source.source_record_type
           AND target.source_record_id = source.source_record_id
        WHEN MATCHED THEN
          UPDATE SET
            target_entity_type = source.target_entity_type,
            target_entity_id = source.target_entity_id,
            sync_status = source.sync_status,
            sync_detail = source.sync_detail,
            last_synced_at = CURRENT_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (
            sync_target_id,
            target_system,
            target_entity_type,
            target_entity_id,
            source_record_type,
            source_record_id,
            sync_status,
            sync_detail,
            last_synced_at,
            created_at,
            updated_at
          )
        VALUES (
            GENERATE_UUID(),
            source.target_system,
            source.target_entity_type,
            source.target_entity_id,
            source.source_record_type,
            source.source_record_id,
            source.sync_status,
            source.sync_detail,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(query)

    def _job_launcher_instance(self) -> CloudRunJobLauncher:
        """Create the Cloud Run launcher only when a restart is actually needed."""

        if self._job_launcher is None:
            self._job_launcher = CloudRunJobLauncher(self.settings)
        return self._job_launcher

    def _escape_sql(self, value: str) -> str:
        """Escape a string for BigQuery literals."""

        return (
            value.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        )
