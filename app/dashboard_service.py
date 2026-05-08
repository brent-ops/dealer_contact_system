"""Dashboard data queries for the one-page system status UI."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class DashboardConnectionStatus:
    """Simple health row for one important system dependency."""

    name: str
    status: str
    detail: str


class DashboardService:
    """Read one-page dashboard metrics from BigQuery."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def get_dashboard_data(self) -> dict[str, Any]:
        """Return all dashboard data needed by the UI."""

        overview = self._get_overview()
        latest_snapshot = self._get_latest_snapshot()
        previous_snapshot = self._get_previous_snapshot()
        trend_rows = self._get_recent_snapshots()
        process_health_rows = self.get_process_health_rows()
        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %I:%M %p"),
            "environment": self.settings.environment,
            "project_id": self.settings.bigquery_project_id,
            "dataset": self.settings.bigquery_dataset,
            "overview": overview,
            "snapshot_warning": self._build_snapshot_warning(process_health_rows),
            "process_health_rows": process_health_rows,
            "system_health_rows": self._build_system_health_rows(process_health_rows),
            "cutover_health_rows": self._build_cutover_health_rows(),
            "discovery_overview": self._get_discovery_overview(),
            "discovery_runtime": self._get_discovery_runtime_details(),
            "snapshot_summary": self._build_snapshot_summary(latest_snapshot, previous_snapshot),
            "trend_rows": trend_rows,
            "trend_cards": self._build_trend_cards(trend_rows),
            "connections": self._get_connections(overview),
            "classification_counts": self._get_classification_counts(),
            "fetch_status_counts": self._get_fetch_status_counts(),
            "brand_counts": self._get_brand_counts(),
            "role_family_counts": self._get_role_family_counts(),
            "ai_provider_counts": self._get_ai_provider_counts(),
            "queue_rows": self._get_queue_rows(),
            "frontier_rows": self._get_frontier_rows(),
            "recent_runs": self._get_recent_runs(),
            "recent_discovery_runs": self._get_recent_discovery_runs(),
            # This panel is operationally useful, but the live query can contend
            # with heavy account-table writes and make the whole dashboard time out.
            "blocked_accounts": [],
            "integration_connections": self._get_integration_connections(),
        }

    def get_process_health_rows(self) -> list[dict[str, Any]]:
        """Return shared lane-health rows for both the dashboard and watchdog."""

        run_rollups = self._get_run_rollups(self.settings.pipeline_runs_table_fqn, "task_type")
        sync_rollups = self._get_sync_rollups()
        if self.settings.parallel_enrichment_enabled:
            rows = [
                self._build_queue_lane_row(
                    lane_key="validate",
                    label="Validate",
                    queue_row=self._get_single_queue_rollup(self.settings.validate_queue_table_fqn, "validate"),
                    run_row=run_rollups.get("validate"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="crawl",
                    label="Crawl",
                    queue_row=self._get_single_queue_rollup(self.settings.crawl_queue_table_fqn, "crawl"),
                    run_row=run_rollups.get("crawl"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="gbp",
                    label="GBP Enrichment",
                    queue_row=self._get_single_queue_rollup(self.settings.gbp_queue_table_fqn, "gbp"),
                    run_row=run_rollups.get("gbp"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="ai",
                    label="AI Retrieval",
                    queue_row=self._get_single_queue_rollup(self.settings.ai_queue_table_fqn, "ai"),
                    run_row=run_rollups.get("ai"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="contact_extract",
                    label="Contact Extraction",
                    queue_row=self._get_single_queue_rollup(
                        self.settings.contact_extract_queue_table_fqn,
                        "contact_extract",
                    ),
                    run_row=run_rollups.get("contact_extract"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="blocked_retry",
                    label="Blocked Retry",
                    queue_row=self._get_single_queue_rollup(
                        self.settings.blocked_retry_queue_table_fqn,
                        "blocked_retry",
                    ),
                    run_row=run_rollups.get("blocked_retry"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="lead_refresh",
                    label="Lead Refresh",
                    queue_row=self._get_single_queue_rollup(self.settings.lead_refresh_queue_table_fqn, "lead_refresh"),
                    run_row=run_rollups.get("lead_refresh"),
                    stale_hours=self.settings.process_watchdog_prospect_leads_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="discovery_search",
                    label="Discovery Search",
                    queue_row=self._get_single_queue_rollup(
                        self.settings.domain_discovery_queue_table_fqn,
                        "search",
                    ),
                    run_row=self._get_run_rollups(self.settings.domain_discovery_runs_table_fqn, "'search'").get("search"),
                    stale_hours=self.settings.process_watchdog_discovery_stale_hours,
                ),
                self._build_freshness_lane_row(
                    lane_key="prospect_lead_refresh",
                    label="Prospect Lead Materialization",
                    timestamp=self._get_max_timestamp(self.settings.prospect_leads_table_fqn, "updated_at"),
                    stale_hours=self.settings.process_watchdog_prospect_leads_stale_hours,
                    processed_24h=self._count_recent_rows(
                        self.settings.prospect_leads_table_fqn,
                        "updated_at",
                        24,
                    ),
                    total_count=self._count_rows(self.settings.prospect_leads_table_fqn),
                    detail_prefix="Prospect leads materialization",
                ),
                self._build_freshness_lane_row(
                    lane_key="dashboard_snapshot",
                    label="Dashboard Snapshot",
                    timestamp=self._get_max_timestamp(self.settings.dashboard_snapshots_table_fqn, "snapshot_at"),
                    stale_hours=self.settings.process_watchdog_snapshot_stale_hours,
                    processed_24h=self._count_recent_rows(
                        self.settings.dashboard_snapshots_table_fqn,
                        "snapshot_at",
                        24,
                    ),
                    total_count=self._count_rows(self.settings.dashboard_snapshots_table_fqn),
                    detail_prefix="Dashboard snapshots",
                ),
                self._build_sync_lane_row(
                    lane_key="campaign_monitor_sync",
                    label="Campaign Monitor Sync",
                    sync_row=sync_rollups.get("campaign_monitor"),
                    stale_hours=self.settings.process_watchdog_campaign_monitor_stale_hours,
                ),
            ]
        else:
            queue_rollups = self._get_queue_rollups(self.settings.account_work_queue_table_fqn, "task_type")
            discovery_queue_rollups = self._get_queue_rollups(
                self.settings.domain_discovery_queue_table_fqn,
                "'search'",
            )
            discovery_run_rollups = self._get_run_rollups(
                self.settings.domain_discovery_runs_table_fqn,
                "'search'",
            )
            rows = [
                self._build_queue_lane_row(
                    lane_key="validate",
                    label="Validate",
                    queue_row=queue_rollups.get("validate"),
                    run_row=run_rollups.get("validate"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="enrich",
                    label="Enrich",
                    queue_row=queue_rollups.get("enrich"),
                    run_row=run_rollups.get("enrich"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="enrich_gbp",
                    label="GBP Enrichment",
                    queue_row=queue_rollups.get("enrich_gbp"),
                    run_row=run_rollups.get("enrich_gbp"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="ai_account_facts",
                    label="AI Retrieval",
                    queue_row=queue_rollups.get("ai_account_facts"),
                    run_row=run_rollups.get("ai_account_facts"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="extract_contacts",
                    label="Extract Contacts",
                    queue_row=queue_rollups.get("extract_contacts"),
                    run_row=run_rollups.get("extract_contacts"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="retry_blocked",
                    label="Retry Blocked",
                    queue_row=queue_rollups.get("retry_blocked"),
                    run_row=run_rollups.get("retry_blocked"),
                    stale_hours=self.settings.process_watchdog_main_stale_hours,
                ),
                self._build_queue_lane_row(
                    lane_key="discovery_search",
                    label="Discovery Search",
                    queue_row=discovery_queue_rollups.get("search"),
                    run_row=discovery_run_rollups.get("search"),
                    stale_hours=self.settings.process_watchdog_discovery_stale_hours,
                ),
                self._build_freshness_lane_row(
                    lane_key="prospect_lead_refresh",
                    label="Prospect Lead Refresh",
                    timestamp=self._get_max_timestamp(self.settings.prospect_leads_table_fqn, "updated_at"),
                    stale_hours=self.settings.process_watchdog_prospect_leads_stale_hours,
                    processed_24h=self._count_recent_rows(
                        self.settings.prospect_leads_table_fqn,
                        "updated_at",
                        24,
                    ),
                    total_count=self._count_rows(self.settings.prospect_leads_table_fqn),
                    detail_prefix="Prospect leads materialization",
                ),
                self._build_freshness_lane_row(
                    lane_key="dashboard_snapshot",
                    label="Dashboard Snapshot",
                    timestamp=self._get_max_timestamp(self.settings.dashboard_snapshots_table_fqn, "snapshot_at"),
                    stale_hours=self.settings.process_watchdog_snapshot_stale_hours,
                    processed_24h=self._count_recent_rows(
                        self.settings.dashboard_snapshots_table_fqn,
                        "snapshot_at",
                        24,
                    ),
                    total_count=self._count_rows(self.settings.dashboard_snapshots_table_fqn),
                    detail_prefix="Dashboard snapshots",
                ),
                self._build_sync_lane_row(
                    lane_key="campaign_monitor_sync",
                    label="Campaign Monitor Sync",
                    sync_row=sync_rollups.get("campaign_monitor"),
                    stale_hours=self.settings.process_watchdog_campaign_monitor_stale_hours,
                ),
            ]
        rows.insert(0, self._build_main_worker_row(rows))
        return rows

    def get_client_records_page(self, page: int = 1, page_size: int = 100) -> dict[str, Any]:
        """Return paginated prospect lead rows for the read-only client records UI."""

        safe_page = max(page, 1)
        safe_page_size = min(max(page_size, 25), 250)
        offset = (safe_page - 1) * safe_page_size

        count_query = f"""
        SELECT COUNT(*) AS total_rows
        FROM `{self.settings.prospect_leads_table_fqn}`
        """
        total_rows = int(self.repository.fetch_one(count_query).get("total_rows", 0))

        column_query = f"""
        SELECT column_name
        FROM `{self.settings.bigquery_project_id}.{self.settings.bigquery_dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{self.settings.prospect_leads_table}'
        ORDER BY ordinal_position
        """
        columns = [row["column_name"] for row in self.repository.fetch_all(column_query)]

        data_query = f"""
        SELECT *
        FROM `{self.settings.prospect_leads_table_fqn}`
        ORDER BY updated_at DESC NULLS LAST, first_seen_at DESC NULLS LAST, email
        LIMIT {safe_page_size}
        OFFSET {offset}
        """
        rows = [
            {key: self._serialize_table_value(value) for key, value in row.items()}
            for row in self.repository.fetch_all(data_query)
        ]

        total_pages = max((total_rows + safe_page_size - 1) // safe_page_size, 1)
        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %I:%M %p"),
            "environment": self.settings.environment,
            "project_id": self.settings.bigquery_project_id,
            "dataset": self.settings.bigquery_dataset,
            "table_name": self.settings.prospect_leads_table_fqn,
            "columns": columns,
            "rows": rows,
            "page": safe_page,
            "page_size": safe_page_size,
            "total_rows": total_rows,
            "total_pages": total_pages,
            "has_previous": safe_page > 1,
            "has_next": safe_page < total_pages,
            "previous_page": max(safe_page - 1, 1),
            "next_page": min(safe_page + 1, total_pages),
        }

    def get_discovery_candidates_page(self, page: int = 1, page_size: int = 100) -> dict[str, Any]:
        """Return paginated discovery candidate rows for read-only UI review."""

        safe_page = max(page, 1)
        safe_page_size = min(max(page_size, 25), 250)
        offset = (safe_page - 1) * safe_page_size

        count_query = f"""
        SELECT COUNT(*) AS total_rows
        FROM `{self.settings.discovered_domain_candidates_table_fqn}`
        WHERE COALESCE(promotion_status, '') IN ('new', 'promoted_to_main_pipeline')
        """
        total_rows = int(self.repository.fetch_one(count_query).get("total_rows", 0))

        hidden_rejected_query = f"""
        SELECT COUNT(*) AS hidden_rejected_rows
        FROM `{self.settings.discovered_domain_candidates_table_fqn}`
        WHERE STARTS_WITH(COALESCE(promotion_status, ''), 'rejected')
        """
        hidden_rejected_rows = int(
            self.repository.fetch_one(hidden_rejected_query).get("hidden_rejected_rows", 0)
        )

        hidden_duplicate_query = f"""
        SELECT COUNT(*) AS hidden_duplicate_rows
        FROM `{self.settings.discovered_domain_candidates_table_fqn}`
        WHERE COALESCE(promotion_status, '') = 'duplicate_existing'
        """
        hidden_duplicate_rows = int(
            self.repository.fetch_one(hidden_duplicate_query).get("hidden_duplicate_rows", 0)
        )

        data_query = f"""
        WITH candidate_base AS (
          SELECT
            candidate_id,
            COALESCE(
              NULLIF(TRIM(candidate_account_name), ''),
              NULLIF(TRIM(matched.account_name), ''),
              NULLIF(TRIM(promoted.account_name), ''),
              candidate_domain
            ) AS dealer_name,
            COALESCE(
              NULLIF(TRIM(matched.inferred_brand), ''),
              NULLIF(TRIM(promoted.inferred_brand), ''),
              NULLIF(TRIM(candidate.brand_hint), '')
            ) AS brand_hint,
            candidate.market,
            candidate.country,
            COALESCE(
              NULLIF(TRIM(matched.account_state), ''),
              NULLIF(TRIM(promoted.account_state), ''),
              NULLIF(TRIM(candidate.state_or_province), '')
            ) AS state_or_province,
            COALESCE(
              NULLIF(TRIM(matched.account_city), ''),
              NULLIF(TRIM(promoted.account_city), ''),
              NULLIF(TRIM(candidate.city), '')
            ) AS city,
            candidate.candidate_domain,
            candidate.candidate_website_url,
            candidate.promotion_status,
            candidate.promotion_reason,
            COALESCE(
              NULLIF(TRIM(candidate.promoted_account_key), ''),
              NULLIF(TRIM(matched.account_key), ''),
              NULLIF(TRIM(promoted.account_key), '')
            ) AS canonical_account_key,
            ROUND(candidate.confidence_score, 2) AS confidence_score,
            candidate.source_url,
            candidate.search_term,
            candidate.discovered_at,
            candidate.last_seen_at,
            candidate.updated_at,
            (
              SELECT STRING_AGG(contact_name, ', ' ORDER BY contact_name LIMIT 3)
              FROM (
                SELECT DISTINCT NULLIF(TRIM(full_name), '') AS contact_name
                FROM `{self.settings.prospect_leads_table_fqn}` AS lead
                WHERE lead.account_key = COALESCE(
                  NULLIF(TRIM(candidate.promoted_account_key), ''),
                  NULLIF(TRIM(matched.account_key), ''),
                  NULLIF(TRIM(promoted.account_key), '')
                )
                  AND NULLIF(TRIM(full_name), '') IS NOT NULL
              )
            ) AS contact_names
          FROM `{self.settings.discovered_domain_candidates_table_fqn}` AS candidate
          LEFT JOIN `{self.settings.dealer_accounts_table_fqn}` AS matched
            ON matched.account_key = candidate.candidate_domain
            OR REGEXP_REPLACE(LOWER(COALESCE(matched.website_url, '')), r'^https?://(www\\.)?', '') = candidate.candidate_domain
          LEFT JOIN `{self.settings.dealer_accounts_table_fqn}` AS promoted
            ON promoted.account_key = candidate.promoted_account_key
          WHERE COALESCE(candidate.promotion_status, '') IN ('new', 'promoted_to_main_pipeline')
        )
        SELECT
          candidate_id,
          dealer_name,
          brand_hint,
          contact_names,
          candidate_domain,
          candidate_website_url,
          promotion_status,
          promotion_reason,
          canonical_account_key,
          market,
          country,
          state_or_province,
          city,
          confidence_score,
          source_url,
          search_term,
          discovered_at,
          last_seen_at,
          updated_at
        FROM candidate_base
        ORDER BY
          CASE promotion_status
            WHEN 'new' THEN 0
            WHEN 'promoted_to_main_pipeline' THEN 1
            WHEN 'duplicate_existing' THEN 2
            ELSE 3
          END,
          updated_at DESC NULLS LAST,
          discovered_at DESC NULLS LAST,
          candidate_domain
        LIMIT {safe_page_size}
        OFFSET {offset}
        """
        rows = [
            {key: self._serialize_table_value(value) for key, value in row.items()}
            for row in self.repository.fetch_all(data_query)
        ]

        columns = [
            "dealer_name",
            "brand_hint",
            "contact_names",
            "candidate_domain",
            "candidate_website_url",
            "promotion_status",
            "promotion_reason",
            "canonical_account_key",
            "market",
            "country",
            "state_or_province",
            "city",
            "confidence_score",
            "source_url",
            "search_term",
            "discovered_at",
            "last_seen_at",
            "updated_at",
            "candidate_id",
        ]

        activity_query = f"""
        SELECT
          MAX(started_at) AS last_run_at,
          ARRAY_AGG(run_notes IGNORE NULLS ORDER BY started_at DESC LIMIT 1)[OFFSET(0)] AS last_run_notes,
          COUNTIF(started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) AS runs_last_24h,
          COUNTIF(
            started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            AND REGEXP_CONTAINS(COALESCE(run_notes, ''), r'wrote [1-9][0-9]* candidate row')
          ) AS productive_runs_last_24h
        FROM `{self.settings.domain_discovery_runs_table_fqn}`
        WHERE run_type = 'search'
        """
        activity = self.repository.fetch_one(activity_query)

        total_pages = max((total_rows + safe_page_size - 1) // safe_page_size, 1)
        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %I:%M %p"),
            "environment": self.settings.environment,
            "project_id": self.settings.bigquery_project_id,
            "dataset": self.settings.bigquery_dataset,
            "table_name": self.settings.discovered_domain_candidates_table_fqn,
            "columns": columns,
            "rows": rows,
            "page": safe_page,
            "page_size": safe_page_size,
            "total_rows": total_rows,
            "hidden_rejected_rows": hidden_rejected_rows,
            "hidden_duplicate_rows": hidden_duplicate_rows,
            "total_pages": total_pages,
            "has_previous": safe_page > 1,
            "has_next": safe_page < total_pages,
            "previous_page": max(safe_page - 1, 1),
            "next_page": min(safe_page + 1, total_pages),
            "last_run_at": self._format_timestamp(activity.get("last_run_at")),
            "last_run_notes": str(activity.get("last_run_notes") or "").strip(),
            "runs_last_24h": int(activity.get("runs_last_24h", 0) or 0),
            "productive_runs_last_24h": int(activity.get("productive_runs_last_24h", 0) or 0),
        }

    def _serialize_table_value(self, value: Any) -> str:
        """Normalize BigQuery values for safe HTML table rendering."""

        if value is None:
            return ""
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, default=str)
        return str(value)

    def capture_snapshot(self) -> None:
        """Store one point-in-time dashboard snapshot for trend reporting."""

        query = f"""
        INSERT INTO `{self.settings.dashboard_snapshots_table_fqn}` (
          snapshot_id,
          snapshot_at,
          source_contacts,
          dealer_accounts,
          validated_dealers,
          validated_dealer_groups,
          accounts_with_phone,
          accounts_with_website_phone,
          accounts_with_gbp_phone,
          accounts_with_ai_phone,
          accounts_with_gbp_address,
          accounts_with_ai_address,
          accounts_with_best_phone_from_gbp,
          accounts_with_best_phone_from_ai,
          activation_ready_accounts,
          marketing_ready_contacts,
          sales_ready_leads,
          validated_websites,
          enriched_websites,
          prospect_contacts,
          prospect_leads,
          activation_ready_contacts,
          current_client_contacts,
          canada_contacts,
          dim_matched_leads,
          dim_suppressed_leads,
          website_extracted_contacts,
          blocked_fetch_accounts,
          managed_fetch_eligible_accounts,
          queued_ai_account_facts,
          queued_validate,
          queued_enrich,
          queued_extract,
          queued_retry_blocked,
          created_at
        )
        SELECT
          GENERATE_UUID(),
          CURRENT_TIMESTAMP(),
          (SELECT COUNT(*) FROM `{self.settings.source_contact_table_fqn}`),
          (SELECT COUNT(*) FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(dealer_classification = 'dealer') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(dealer_classification = 'dealer_group') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(account_phone IS NOT NULL AND TRIM(account_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(website_phone IS NOT NULL AND TRIM(website_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(gbp_phone IS NOT NULL AND TRIM(gbp_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(ai_phone IS NOT NULL AND TRIM(ai_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(gbp_address_line IS NOT NULL AND TRIM(gbp_address_line) != '') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(ai_address_line IS NOT NULL AND TRIM(ai_address_line) != '') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(best_phone_source = 'gbp' AND best_phone IS NOT NULL AND TRIM(best_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(best_phone_source = 'ai' AND best_phone IS NOT NULL AND TRIM(best_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(activation_status = 'activation_ready') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNT(*) FROM `{self.settings.marketing_ready_contacts_view_fqn}`),
          (SELECT COUNT(*) FROM `{self.settings.sales_ready_leads_view_fqn}`),
          (SELECT COUNTIF(website_url IS NOT NULL AND TRIM(website_url) != '' AND dealer_classification IN ('dealer', 'dealer_group')) FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(website_url IS NOT NULL AND TRIM(website_url) != '') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNT(*) FROM `{self.settings.prospect_contacts_table_fqn}`),
          (SELECT COUNT(*) FROM `{self.settings.prospect_leads_table_fqn}`),
          (SELECT COUNT(*) FROM `{self.settings.activation_ready_contacts_view_fqn}`),
          (SELECT COUNTIF(audience_type = 'current_client') FROM `{self.settings.prospect_contacts_table_fqn}`),
          (SELECT COUNTIF(is_canada) FROM `{self.settings.prospect_leads_table_fqn}`),
          (SELECT COUNTIF(dim_client_match_flag) FROM `{self.settings.prospect_leads_table_fqn}`),
          (SELECT COUNTIF(NOT prospecting_allowed_flag) FROM `{self.settings.prospect_leads_table_fqn}`),
          (SELECT COUNTIF(source_type = 'website_contact_extraction') FROM `{self.settings.prospect_contacts_table_fqn}`),
          (SELECT COUNTIF(fetch_status = 'blocked' AND dealer_classification IN ('dealer', 'dealer_group')) FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(managed_fetch_status = 'eligible') FROM `{self.settings.dealer_accounts_table_fqn}`),
          (SELECT COUNTIF(task_type = 'ai_account_facts' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`),
          (SELECT COUNTIF(task_type = 'validate' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`),
          (SELECT COUNTIF(task_type = 'enrich' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`),
          (SELECT COUNTIF(task_type = 'extract_contacts' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`),
          (SELECT COUNTIF(task_type = 'retry_blocked' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`),
          CURRENT_TIMESTAMP()
        """
        self.repository.execute_statement(query)

    def _get_overview(self) -> dict[str, Any]:
        """Return the top-line counts shown in summary cards."""

        query = f"""
        SELECT
          (SELECT COUNT(*) FROM `{self.settings.source_contact_table_fqn}`) AS source_contacts,
          (SELECT COUNT(*) FROM `{self.settings.dealer_accounts_table_fqn}`) AS dealer_accounts,
          (SELECT COUNTIF(dealer_classification = 'dealer') FROM `{self.settings.dealer_accounts_table_fqn}`) AS validated_dealers,
          (SELECT COUNTIF(dealer_classification = 'dealer_group') FROM `{self.settings.dealer_accounts_table_fqn}`) AS validated_dealer_groups,
          (SELECT COUNTIF(account_phone IS NOT NULL AND TRIM(account_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`) AS accounts_with_phone,
          (SELECT COUNTIF(website_phone IS NOT NULL AND TRIM(website_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`) AS accounts_with_website_phone,
          (SELECT COUNTIF(gbp_phone IS NOT NULL AND TRIM(gbp_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`) AS accounts_with_gbp_phone,
          (SELECT COUNTIF(ai_phone IS NOT NULL AND TRIM(ai_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`) AS accounts_with_ai_phone,
          (SELECT COUNTIF(gbp_address_line IS NOT NULL AND TRIM(gbp_address_line) != '') FROM `{self.settings.dealer_accounts_table_fqn}`) AS accounts_with_gbp_address,
          (SELECT COUNTIF(ai_address_line IS NOT NULL AND TRIM(ai_address_line) != '') FROM `{self.settings.dealer_accounts_table_fqn}`) AS accounts_with_ai_address,
          (SELECT COUNTIF(gbp_last_verified_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) FROM `{self.settings.dealer_accounts_table_fqn}`) AS gbp_verified_last_24h,
          (SELECT COUNTIF(ai_last_verified_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) FROM `{self.settings.dealer_accounts_table_fqn}`) AS ai_verified_last_24h,
          (SELECT MAX(gbp_last_verified_at) FROM `{self.settings.dealer_accounts_table_fqn}`) AS last_gbp_verified_at,
          (SELECT MAX(ai_last_verified_at) FROM `{self.settings.dealer_accounts_table_fqn}`) AS last_ai_verified_at,
          (SELECT COUNTIF(best_phone_source = 'gbp' AND best_phone IS NOT NULL AND TRIM(best_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`) AS accounts_with_best_phone_from_gbp,
          (SELECT COUNTIF(best_phone_source = 'ai' AND best_phone IS NOT NULL AND TRIM(best_phone) != '') FROM `{self.settings.dealer_accounts_table_fqn}`) AS accounts_with_best_phone_from_ai,
          (SELECT COUNTIF(activation_status = 'activation_ready') FROM `{self.settings.dealer_accounts_table_fqn}`) AS activation_ready_accounts,
          (SELECT COUNT(*) FROM `{self.settings.marketing_ready_contacts_view_fqn}`) AS marketing_ready_contacts,
          (SELECT COUNT(*) FROM `{self.settings.sales_ready_leads_view_fqn}`) AS sales_ready_leads,
          (SELECT COUNTIF(website_url IS NOT NULL AND TRIM(website_url) != '') FROM `{self.settings.dealer_accounts_table_fqn}`) AS enriched_websites,
          (SELECT COUNTIF(website_url IS NOT NULL AND TRIM(website_url) != '' AND dealer_classification IN ('dealer', 'dealer_group')) FROM `{self.settings.dealer_accounts_table_fqn}`) AS validated_websites,
          (SELECT COUNT(*) FROM `{self.settings.prospect_contacts_table_fqn}`) AS prospect_contacts,
          (SELECT COUNT(*) FROM `{self.settings.prospect_leads_table_fqn}`) AS prospect_leads,
          (SELECT COUNT(*) FROM `{self.settings.activation_ready_contacts_view_fqn}`) AS activation_ready_contacts,
          (SELECT COUNTIF(audience_type = 'current_client') FROM `{self.settings.prospect_contacts_table_fqn}`) AS current_client_contacts,
          (SELECT COUNTIF(is_canada) FROM `{self.settings.prospect_leads_table_fqn}`) AS canada_contacts,
          (SELECT COUNTIF(dim_client_match_flag) FROM `{self.settings.prospect_leads_table_fqn}`) AS dim_matched_leads,
          (SELECT COUNTIF(NOT prospecting_allowed_flag) FROM `{self.settings.prospect_leads_table_fqn}`) AS dim_suppressed_leads,
          (SELECT COUNTIF(source_type = 'website_contact_extraction') FROM `{self.settings.prospect_contacts_table_fqn}`) AS website_extracted_contacts,
          (SELECT COUNT(*) FROM `{self.settings.pipeline_runs_table_fqn}`) AS pipeline_runs,
            (SELECT COUNTIF(task_type = 'ai_account_facts' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`) AS queued_ai_account_facts,
            (SELECT COUNTIF(task_type = 'enrich_gbp' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`) AS queued_gbp_enrich,
            (SELECT COUNTIF(task_type = 'validate' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`) AS queued_validate,
          (SELECT COUNTIF(task_type = 'enrich' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`) AS queued_enrich,
          (SELECT COUNTIF(task_type = 'extract_contacts' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`) AS queued_extract,
          (SELECT COUNTIF(task_type = 'retry_blocked' AND status IN ('pending', 'retry')) FROM `{self.settings.account_work_queue_table_fqn}`) AS queued_retry_blocked,
          (SELECT COUNTIF(status = 'in_progress') FROM `{self.settings.account_work_queue_table_fqn}`) AS queue_in_progress,
          (SELECT COUNTIF(fetch_status = 'blocked' AND dealer_classification IN ('dealer', 'dealer_group')) FROM `{self.settings.dealer_accounts_table_fqn}`) AS blocked_fetch_accounts,
          (SELECT COUNTIF(managed_fetch_status = 'eligible') FROM `{self.settings.dealer_accounts_table_fqn}`) AS managed_fetch_eligible_accounts,
          (SELECT COUNTIF(status IN ('pending', 'retry')) FROM `{self.settings.domain_discovery_queue_table_fqn}`) AS queued_discovery_tasks,
          (SELECT COUNT(*) FROM `{self.settings.discovered_domain_candidates_table_fqn}`) AS discovery_candidates,
          (SELECT COUNTIF(promotion_status = 'duplicate_existing') FROM `{self.settings.discovered_domain_candidates_table_fqn}`) AS discovery_duplicate_existing,
          (SELECT COUNTIF(promotion_status = 'promoted_to_main_pipeline') FROM `{self.settings.discovered_domain_candidates_table_fqn}`) AS discovery_promoted_candidates,
          (SELECT COUNTIF(STARTS_WITH(promotion_status, 'rejected')) FROM `{self.settings.discovered_domain_candidates_table_fqn}`) AS discovery_rejected_candidates
        """
        return self.repository.fetch_one(query)

    def _get_discovery_overview(self) -> dict[str, Any]:
        """Return a separate top-line summary for the discovery-only subsystem."""

        query = f"""
        SELECT
          (SELECT COUNTIF(status IN ('pending', 'retry')) FROM `{self.settings.domain_discovery_queue_table_fqn}`) AS queued_tasks,
          (SELECT COUNT(*) FROM `{self.settings.discovered_domain_candidates_table_fqn}`) AS total_candidates,
          (SELECT COUNTIF(promotion_status = 'new') FROM `{self.settings.discovered_domain_candidates_table_fqn}`) AS new_candidates,
          (SELECT COUNTIF(promotion_status = 'duplicate_existing') FROM `{self.settings.discovered_domain_candidates_table_fqn}`) AS duplicate_existing_candidates,
          (SELECT COUNTIF(promotion_status = 'promoted_to_main_pipeline') FROM `{self.settings.discovered_domain_candidates_table_fqn}`) AS promoted_candidates,
          (SELECT COUNTIF(STARTS_WITH(promotion_status, 'rejected')) FROM `{self.settings.discovered_domain_candidates_table_fqn}`) AS rejected_candidates,
          (SELECT COUNT(*) FROM `{self.settings.domain_discovery_runs_table_fqn}` WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) AS runs_last_24h,
          (
            SELECT MAX(started_at)
            FROM `{self.settings.domain_discovery_runs_table_fqn}`
          ) AS last_run_at
        """
        return self.repository.fetch_one(query)

    def _get_discovery_runtime_details(self) -> list[dict[str, str]]:
        """Return compact runtime details for the separate discovery worker."""

        return [
            {
                "label": "Cloud Run Job",
                "value": self.settings.domain_discovery_job_name,
            },
            {
                "label": "Scheduler",
                "value": self.settings.domain_discovery_scheduler_name,
            },
            {
                "label": "Cadence",
                "value": self.settings.domain_discovery_schedule_hint,
            },
            {
                "label": "VPC Connector",
                "value": self.settings.domain_discovery_vpc_connector or "-",
            },
            {
                "label": "Egress Mode",
                "value": self.settings.domain_discovery_vpc_egress or "-",
            },
            {
                "label": "Dedicated Egress IP",
                "value": self.settings.domain_discovery_egress_ip or "-",
            },
        ]

    def _get_latest_snapshot(self) -> dict[str, Any]:
        """Return the newest stored dashboard snapshot."""

        query = f"""
        SELECT
          snapshot_at,
          source_contacts,
          dealer_accounts,
          validated_dealers,
          validated_dealer_groups,
          accounts_with_phone,
          accounts_with_website_phone,
          accounts_with_gbp_phone,
          accounts_with_ai_phone,
          accounts_with_gbp_address,
          accounts_with_ai_address,
          accounts_with_best_phone_from_gbp,
          accounts_with_best_phone_from_ai,
          activation_ready_accounts,
          marketing_ready_contacts,
          sales_ready_leads,
          validated_websites,
          enriched_websites,
          prospect_contacts,
          prospect_leads,
          activation_ready_contacts,
          current_client_contacts,
          canada_contacts,
          dim_matched_leads,
          dim_suppressed_leads,
          website_extracted_contacts,
          blocked_fetch_accounts,
          managed_fetch_eligible_accounts,
          queued_ai_account_facts,
          queued_validate,
          queued_enrich,
          queued_extract,
          queued_retry_blocked
        FROM `{self.settings.dashboard_snapshots_table_fqn}`
        ORDER BY snapshot_at DESC
        LIMIT 1
        """
        return self.repository.fetch_one(query)

    def _get_previous_snapshot(self) -> dict[str, Any]:
        """Return the prior dashboard snapshot for before/after comparisons."""

        query = f"""
        SELECT
          snapshot_at,
          source_contacts,
          dealer_accounts,
          validated_dealers,
          validated_dealer_groups,
          accounts_with_phone,
          accounts_with_website_phone,
          accounts_with_gbp_phone,
          accounts_with_ai_phone,
          accounts_with_gbp_address,
          accounts_with_ai_address,
          accounts_with_best_phone_from_gbp,
          accounts_with_best_phone_from_ai,
          activation_ready_accounts,
          marketing_ready_contacts,
          sales_ready_leads,
          validated_websites,
          enriched_websites,
          prospect_contacts,
          prospect_leads,
          activation_ready_contacts,
          current_client_contacts,
          canada_contacts,
          dim_matched_leads,
          dim_suppressed_leads,
          website_extracted_contacts,
          blocked_fetch_accounts,
          managed_fetch_eligible_accounts,
          queued_ai_account_facts,
          queued_validate,
          queued_enrich,
          queued_extract,
          queued_retry_blocked
        FROM `{self.settings.dashboard_snapshots_table_fqn}`
        ORDER BY snapshot_at DESC
        LIMIT 1
        OFFSET 1
        """
        return self.repository.fetch_one(query)

    def _get_recent_snapshots(self) -> list[dict[str, Any]]:
        """Return recent snapshots for trend rows on the dashboard."""

        query = f"""
        SELECT
          snapshot_at,
          validated_dealers,
          validated_dealer_groups,
          accounts_with_phone,
          accounts_with_website_phone,
          accounts_with_gbp_phone,
          accounts_with_ai_phone,
          accounts_with_gbp_address,
          accounts_with_ai_address,
          accounts_with_best_phone_from_gbp,
          accounts_with_best_phone_from_ai,
          activation_ready_accounts,
          marketing_ready_contacts,
          sales_ready_leads,
          validated_websites,
          enriched_websites,
          prospect_leads,
          activation_ready_contacts,
          current_client_contacts,
          canada_contacts,
          dim_matched_leads,
          dim_suppressed_leads,
          website_extracted_contacts,
          blocked_fetch_accounts,
          managed_fetch_eligible_accounts
        FROM `{self.settings.dashboard_snapshots_table_fqn}`
        ORDER BY snapshot_at DESC
        LIMIT 10
        """
        return self.repository.fetch_all(query)

    def _build_snapshot_summary(
        self,
        latest_snapshot: dict[str, Any],
        previous_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        """Return dashboard cards showing before/after changes."""

        labels = {
            "validated_dealers": "Validated Dealers",
            "validated_dealer_groups": "Dealer Groups",
            "accounts_with_phone": "Accounts With Phone",
            "accounts_with_website_phone": "Accounts With Website Phone",
            "accounts_with_gbp_phone": "Accounts With GBP Phone",
            "accounts_with_ai_phone": "Accounts With AI Phone",
            "accounts_with_gbp_address": "Accounts With GBP Address",
            "accounts_with_ai_address": "Accounts With AI Address",
            "accounts_with_best_phone_from_gbp": "Best Phone From GBP",
            "accounts_with_best_phone_from_ai": "Best Phone From AI",
            "activation_ready_accounts": "Activation-Ready Accounts",
            "prospect_leads": "Prospect Leads",
            "marketing_ready_contacts": "Marketing-Ready Contacts",
            "sales_ready_leads": "Sales-Ready Leads",
            "validated_websites": "Website-Ready Dealers",
            "enriched_websites": "Resolved Websites",
            "activation_ready_contacts": "Activation-Ready Contacts",
            "current_client_contacts": "Current Clients",
            "canada_contacts": "Canada Contacts",
            "dim_matched_leads": "DIM Matched Leads",
            "dim_suppressed_leads": "Suppressed Leads",
            "website_extracted_contacts": "Website Contacts",
            "blocked_fetch_accounts": "Blocked Dealer Sites",
            "managed_fetch_eligible_accounts": "Managed Fetch Eligible",
        }
        if not latest_snapshot:
            return {
                "available": False,
                "latest_snapshot_at": None,
                "previous_snapshot_at": None,
                "cards": [],
            }

        cards: list[dict[str, Any]] = []
        for key, label in labels.items():
            latest_value = int(latest_snapshot.get(key, 0) or 0)
            previous_value = int(previous_snapshot.get(key, 0) or 0)
            delta = latest_value - previous_value
            cards.append(
                {
                    "label": label,
                    "value": latest_value,
                    "delta": delta,
                    "direction": self._delta_direction(delta),
                }
            )

        return {
            "available": True,
            "latest_snapshot_at": latest_snapshot.get("snapshot_at"),
            "previous_snapshot_at": previous_snapshot.get("snapshot_at"),
            "cards": cards,
        }

    def _build_trend_cards(self, trend_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Build compact sparkline cards from recent snapshot rows."""

        metrics = [
            ("validated_dealers", "Validated Dealers"),
            ("validated_websites", "Website-Ready Dealers"),
            ("accounts_with_phone", "Accounts With Phone"),
            ("accounts_with_website_phone", "Website Phone Coverage"),
            ("accounts_with_gbp_phone", "GBP Phone Coverage"),
            ("accounts_with_ai_phone", "AI Phone Coverage"),
            ("accounts_with_gbp_address", "GBP Address Coverage"),
            ("accounts_with_ai_address", "AI Address Coverage"),
            ("accounts_with_best_phone_from_gbp", "Best Phone From GBP"),
            ("accounts_with_best_phone_from_ai", "Best Phone From AI"),
            ("marketing_ready_contacts", "Marketing-Ready Contacts"),
            ("sales_ready_leads", "Sales-Ready Leads"),
            ("prospect_leads", "Prospect Leads"),
            ("activation_ready_contacts", "Activation-Ready Contacts"),
            ("current_client_contacts", "Current Clients"),
            ("canada_contacts", "Canada Contacts"),
            ("dim_matched_leads", "DIM Matched Leads"),
            ("dim_suppressed_leads", "Suppressed Leads"),
            ("website_extracted_contacts", "Website Contacts"),
            ("blocked_fetch_accounts", "Blocked Dealer Sites"),
            ("managed_fetch_eligible_accounts", "Managed Fetch Eligible"),
        ]
        if not trend_rows:
            return []

        ordered_rows = list(reversed(trend_rows))
        cards: list[dict[str, Any]] = []
        for key, label in metrics:
            values = [int(row.get(key, 0) or 0) for row in ordered_rows]
            cards.append(
                {
                    "label": label,
                    "current_value": values[-1] if values else 0,
                    "delta": (values[-1] - values[0]) if len(values) > 1 else 0,
                    "direction": self._delta_direction((values[-1] - values[0]) if len(values) > 1 else 0),
                    "sparkline_points": self._build_sparkline_points(values),
                }
            )
        return cards

    @staticmethod
    def _delta_direction(delta: int) -> str:
        """Return a CSS-friendly delta direction."""

        if delta > 0:
            return "up"
        if delta < 0:
            return "down"
        return "flat"

    @staticmethod
    def _build_sparkline_points(values: list[int]) -> str:
        """Convert metric values into a compact SVG polyline string."""

        if not values:
            return ""
        if len(values) == 1:
            return "0,30 100,30"

        min_value = min(values)
        max_value = max(values)
        spread = max(max_value - min_value, 1)
        x_step = 100 / (len(values) - 1)
        points: list[str] = []
        for index, value in enumerate(values):
            x = round(index * x_step, 2)
            normalized = (value - min_value) / spread
            y = round(30 - (normalized * 24), 2)
            points.append(f"{x},{y}")
        return " ".join(points)

    def _build_snapshot_warning(self, process_health_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Return a visible snapshot warning when dashboard trend data is stale."""

        snapshot_row = next(
            (row for row in process_health_rows if row.get("lane_key") == "dashboard_snapshot"),
            None,
        )
        if not snapshot_row or snapshot_row.get("status") not in {"stale", "failed"}:
            return None
        return {
            "title": "Snapshot Freshness Warning",
            "detail": str(snapshot_row.get("detail") or "Dashboard snapshots are stale."),
            "last_success_at": snapshot_row.get("last_success_at") or "-",
        }

    def _build_system_health_rows(
        self,
        process_health_rows: list[dict[str, Any]],
    ) -> list[DashboardConnectionStatus]:
        """Build the top-level system health rows shown above the dashboard details."""

        by_key = {str(row.get("lane_key")): row for row in process_health_rows}
        system_statuses = self._get_named_system_statuses(
            ("client_dim", "managed_fetch", "gbp_enrichment", "ai_retrieval", "process_watchdog", "parallel_cutover")
        )
        rows = [
            self._process_health_connection(by_key.get("main_worker"), "Main Worker"),
            self._process_health_connection(by_key.get("discovery_search"), "Discovery Worker"),
            self._system_status_row(
                name="Queue Manager",
                sync_row=system_statuses.get("parallel_cutover"),
                fallback_status="warning" if self.settings.queue_manager_enabled else "healthy",
                fallback_detail=(
                    "Queue manager is enabled but has not reported recently."
                    if self.settings.queue_manager_enabled
                    else "Queue manager is disabled."
                ),
            ),
            self._system_status_row(
                name="Process Watchdog",
                sync_row=system_statuses.get("process_watchdog"),
                fallback_status="warning" if self.settings.process_watchdog_enabled else "healthy",
                fallback_detail=(
                    "Watchdog is enabled but has not reported recently."
                    if self.settings.process_watchdog_enabled
                    else "Watchdog is disabled."
                ),
            ),
            self._process_health_connection(by_key.get("campaign_monitor_sync"), "Campaign Monitor"),
            self._process_health_connection(by_key.get("ai"), "AI Retrieval")
            if self.settings.parallel_enrichment_enabled
            else self._process_health_connection(by_key.get("ai_account_facts"), "AI Retrieval"),
            self._process_health_connection(by_key.get("gbp"), "GBP Enrichment")
            if self.settings.parallel_enrichment_enabled
            else self._process_health_connection(by_key.get("enrich_gbp"), "GBP Enrichment"),
        ]
        return rows

    def _build_cutover_health_rows(self) -> list[DashboardConnectionStatus]:
        """Return cutover-specific health rows for the distributed-worker rollout."""

        system_statuses = self._get_named_system_statuses(("parallel_cutover",))
        return [
            self._system_status_row(
                name="Parallel Cutover",
                sync_row=system_statuses.get("parallel_cutover"),
                fallback_status="warning" if self.settings.parallel_enrichment_enabled else "healthy",
                fallback_detail=(
                    "Parallel worker cutover is enabled but has not been audited recently."
                    if self.settings.parallel_enrichment_enabled
                    else "Legacy sequential worker mode is still active."
                ),
            )
        ]

    def _process_health_connection(
        self,
        row: dict[str, Any] | None,
        fallback_name: str,
    ) -> DashboardConnectionStatus:
        """Convert one process-health row into a connection-style dashboard badge."""

        if not row:
            return DashboardConnectionStatus(
                name=fallback_name,
                status="warning",
                detail="Health data is unavailable.",
            )
        return DashboardConnectionStatus(
            name=str(row.get("label") or fallback_name),
            status=str(row.get("status") or "warning"),
            detail=str(row.get("detail") or "Health data is unavailable."),
        )

    def _get_queue_rollups(self, table_fqn: str, lane_key_value: str) -> dict[str, dict[str, Any]]:
        """Return queued and due counts keyed by task or run type."""

        query = f"""
        SELECT
          {lane_key_value} AS lane_key,
          COUNTIF(status IN ('pending', 'retry')) AS queued_count,
          COUNTIF(status IN ('pending', 'retry') AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP())) AS due_count,
          COUNTIF(status = 'in_progress') AS in_progress_count,
          COUNTIF(status = 'completed') AS completed_count,
          COUNTIF(status = 'failed') AS failed_count
        FROM `{table_fqn}`
        GROUP BY lane_key
        """
        return {
            str(row["lane_key"]): row
            for row in self.repository.fetch_all(query)
        }

    def _get_single_queue_rollup(self, table_fqn: str, lane_key: str) -> dict[str, Any]:
        """Return one queue rollup row for a dedicated single-lane queue table."""

        if lane_key == "search" or table_fqn == self.settings.domain_discovery_queue_table_fqn:
            query = f"""
            SELECT
              '{lane_key}' AS lane_key,
              COUNTIF(status IN ('pending', 'retry')) AS queued_count,
              COUNTIF(status IN ('pending', 'retry') AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP())) AS due_count,
              0 AS discovery_first_pass_count,
              0 AS main_list_first_pass_count,
              0 AS follow_up_count,
              0 AS retry_count,
              COUNTIF(status = 'in_progress') AS in_progress_count,
              COUNTIF(status = 'completed') AS completed_count,
              COUNTIF(status = 'failed') AS failed_count
            FROM `{table_fqn}`
            """
        else:
            query = f"""
            SELECT
              '{lane_key}' AS lane_key,
              COUNTIF(status IN ('pending', 'retry')) AS queued_count,
              COUNTIF(status IN ('pending', 'retry') AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP())) AS due_count,
              COUNTIF(status IN ('pending', 'retry') AND work_phase = 'new_discovery_first_pass') AS discovery_first_pass_count,
              COUNTIF(status IN ('pending', 'retry') AND work_phase = 'main_list_first_pass') AS main_list_first_pass_count,
              COUNTIF(status IN ('pending', 'retry') AND work_phase = 'follow_up') AS follow_up_count,
              COUNTIF(status IN ('pending', 'retry') AND work_phase = 'retry') AS retry_count,
              COUNTIF(status = 'in_progress') AS in_progress_count,
              COUNTIF(status = 'completed') AS completed_count,
              COUNTIF(status = 'failed') AS failed_count
            FROM `{table_fqn}`
            """
        return self.repository.fetch_one(query)

    def _get_run_rollups(self, table_fqn: str, lane_key_value: str) -> dict[str, dict[str, Any]]:
        """Return recent throughput and latest completion stats keyed by lane."""

        query = f"""
        SELECT
          {lane_key_value} AS lane_key,
          COUNTIF(started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) AS runs_24h,
          SUM(IF(started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR), IFNULL(claimed_count, 0), 0)) AS processed_24h,
          SUM(IF(started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR), IFNULL(succeeded_count, 0), 0)) AS succeeded_24h,
          SUM(IF(started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR), IFNULL(failed_count, 0), 0)) AS failed_24h,
          MAX(IF(run_status = 'completed', completed_at, NULL)) AS last_success_at,
          MAX(started_at) AS last_started_at,
          COUNTIF(run_status = 'running') AS running_count
        FROM `{table_fqn}`
        GROUP BY lane_key
        """
        return {
            str(row["lane_key"]): row
            for row in self.repository.fetch_all(query)
        }

    def _get_sync_rollups(self) -> dict[str, dict[str, Any]]:
        """Return one latest sync row per target system plus recent sync volume."""

        query = f"""
        WITH latest_rows AS (
          SELECT
            target_system,
            sync_status,
            sync_detail,
            last_synced_at,
            ROW_NUMBER() OVER (
              PARTITION BY target_system
              ORDER BY last_synced_at DESC NULLS LAST, updated_at DESC NULLS LAST, created_at DESC
            ) AS row_number
          FROM `{self.settings.sync_targets_table_fqn}`
        ),
        recent_counts AS (
          SELECT
            target_system,
            COUNTIF(last_synced_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) AS synced_24h
          FROM `{self.settings.sync_targets_table_fqn}`
          GROUP BY target_system
        )
        SELECT
          latest_rows.target_system,
          latest_rows.sync_status,
          latest_rows.sync_detail,
          latest_rows.last_synced_at,
          IFNULL(recent_counts.synced_24h, 0) AS synced_24h
        FROM latest_rows
        LEFT JOIN recent_counts
          ON recent_counts.target_system = latest_rows.target_system
        WHERE latest_rows.row_number = 1
        """
        return {
            str(row["target_system"]): row
            for row in self.repository.fetch_all(query)
        }

    def _count_rows(self, table_fqn: str) -> int:
        """Return one table row count."""

        row = self.repository.fetch_one(f"SELECT COUNT(*) AS row_count FROM `{table_fqn}`")
        return int(row.get("row_count", 0) or 0)

    def _count_recent_rows(self, table_fqn: str, timestamp_column: str, hours: int) -> int:
        """Return rows updated within the requested freshness window."""

        query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{table_fqn}`
        WHERE {timestamp_column} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        """
        row = self.repository.fetch_one(query)
        return int(row.get("row_count", 0) or 0)

    def _get_max_timestamp(self, table_fqn: str, timestamp_column: str) -> Any:
        """Return one latest timestamp value from a table."""

        row = self.repository.fetch_one(
            f"SELECT MAX({timestamp_column}) AS latest_timestamp FROM `{table_fqn}`"
        )
        return row.get("latest_timestamp")

    def _build_queue_lane_row(
        self,
        *,
        lane_key: str,
        label: str,
        queue_row: dict[str, Any] | None,
        run_row: dict[str, Any] | None,
        stale_hours: int,
    ) -> dict[str, Any]:
        """Build one process-health row for a queue-backed pipeline lane."""

        queue_row = queue_row or {}
        run_row = run_row or {}
        queued_count = int(queue_row.get("queued_count", 0) or 0)
        due_count = int(queue_row.get("due_count", 0) or 0)
        in_progress_count = int(queue_row.get("in_progress_count", 0) or 0)
        discovery_first_pass_count = int(queue_row.get("discovery_first_pass_count", 0) or 0)
        main_list_first_pass_count = int(queue_row.get("main_list_first_pass_count", 0) or 0)
        follow_up_count = int(queue_row.get("follow_up_count", 0) or 0)
        retry_count = int(queue_row.get("retry_count", 0) or 0)
        runs_24h = int(run_row.get("runs_24h", 0) or 0)
        processed_24h = int(run_row.get("processed_24h", 0) or 0)
        succeeded_24h = int(run_row.get("succeeded_24h", 0) or 0)
        failed_24h = int(run_row.get("failed_24h", 0) or 0)
        last_success_at = run_row.get("last_success_at")
        last_started_at = run_row.get("last_started_at")
        running_count = int(run_row.get("running_count", 0) or 0)

        status = self._classify_queue_lane_status(
            last_success_at=last_success_at,
            last_started_at=last_started_at,
            stale_hours=stale_hours,
            queued_count=queued_count,
            due_count=due_count,
            in_progress_count=in_progress_count,
            running_count=running_count,
            succeeded_24h=succeeded_24h,
            failed_24h=failed_24h,
        )
        detail = (
            f"{queued_count:,} queued, {due_count:,} due, {processed_24h:,} processed in the last 24 hours, "
            f"{failed_24h:,} failed, {in_progress_count:,} in progress. "
            f"Discovery first-pass {discovery_first_pass_count:,}, main-list first-pass {main_list_first_pass_count:,}, "
            f"follow-up {follow_up_count:,}, retry {retry_count:,}. "
            f"Last success {self._format_timestamp(last_success_at)}."
        )
        return {
            "lane_key": lane_key,
            "label": label,
            "status": status,
            "last_success_at": self._format_timestamp(last_success_at),
            "last_started_at": self._format_timestamp(last_started_at),
            "runs_24h": runs_24h,
            "processed_24h": processed_24h,
            "succeeded_24h": succeeded_24h,
            "failed_24h": failed_24h,
            "queued_count": queued_count,
            "due_count": due_count,
            "discovery_first_pass_count": discovery_first_pass_count,
            "main_list_first_pass_count": main_list_first_pass_count,
            "follow_up_count": follow_up_count,
            "retry_count": retry_count,
            "in_progress_count": in_progress_count,
            "running_count": running_count,
            "detail": detail,
        }

    def _build_freshness_lane_row(
        self,
        *,
        lane_key: str,
        label: str,
        timestamp: Any,
        stale_hours: int,
        processed_24h: int,
        total_count: int,
        detail_prefix: str,
    ) -> dict[str, Any]:
        """Build one process-health row for a freshness-driven lane."""

        status = "stale" if self._is_timestamp_stale(timestamp, stale_hours) else "healthy"
        detail = (
            f"{detail_prefix} last updated {self._format_timestamp(timestamp)}. "
            f"{processed_24h:,} update(s) in the last 24 hours across {total_count:,} total row(s)."
        )
        return {
            "lane_key": lane_key,
            "label": label,
            "status": status,
            "last_success_at": self._format_timestamp(timestamp),
            "runs_24h": processed_24h,
            "processed_24h": processed_24h,
            "succeeded_24h": processed_24h,
            "failed_24h": 0,
            "queued_count": 0,
            "due_count": 0,
            "in_progress_count": 0,
            "detail": detail,
        }

    def _build_sync_lane_row(
        self,
        *,
        lane_key: str,
        label: str,
        sync_row: dict[str, Any] | None,
        stale_hours: int,
    ) -> dict[str, Any]:
        """Build one process-health row for sync-driven lanes."""

        sync_row = sync_row or {}
        last_success_at = sync_row.get("last_synced_at")
        synced_24h = int(sync_row.get("synced_24h", 0) or 0)
        sync_status = str(sync_row.get("sync_status") or "unknown").lower()
        sync_detail = str(sync_row.get("sync_detail") or "").strip()
        if sync_status in {"failed", "error"}:
            status = "failed"
        elif self._is_timestamp_stale(last_success_at, stale_hours):
            status = "stale"
        else:
            status = "healthy"
        detail = f"Latest sync status {sync_status} at {self._format_timestamp(last_success_at)}."
        if sync_detail:
            detail = f"{detail} {sync_detail}"
        return {
            "lane_key": lane_key,
            "label": label,
            "status": status,
            "last_success_at": self._format_timestamp(last_success_at),
            "runs_24h": synced_24h,
            "processed_24h": synced_24h,
            "succeeded_24h": synced_24h,
            "failed_24h": 0 if status != "failed" else 1,
            "queued_count": 0,
            "due_count": 0,
            "in_progress_count": 0,
            "detail": detail,
        }

    def _build_main_worker_row(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        """Build one summary row for the overall main worker health."""

        if self.settings.parallel_enrichment_enabled:
            main_keys = {"validate", "crawl", "gbp", "ai", "contact_extract", "blocked_retry", "lead_refresh"}
            label = "Parallel Enrichment Workers"
        else:
            main_keys = {"validate", "enrich", "enrich_gbp", "ai_account_facts", "extract_contacts", "retry_blocked"}
            label = "Main Worker"
        main_rows = [row for row in rows if row.get("lane_key") in main_keys]
        if not main_rows:
            return {
                "lane_key": "main_worker",
                "label": label,
                "status": "warning",
                "last_success_at": "-",
                "runs_24h": 0,
                "processed_24h": 0,
                "succeeded_24h": 0,
                "failed_24h": 0,
                "queued_count": 0,
                "due_count": 0,
                "in_progress_count": 0,
                "detail": "No main-worker health rows are available.",
            }

        status = "healthy"
        if any(row["status"] == "failed" for row in main_rows):
            status = "failed"
        elif any(row["status"] == "stale" for row in main_rows):
            status = "stale"
        elif any(row["status"] == "backlogged" for row in main_rows):
            status = "backlogged"

        non_healthy = [row["label"] for row in main_rows if row["status"] in {"failed", "stale", "backlogged"}]
        detail = (
            f"{sum(int(row.get('queued_count', 0) or 0) for row in main_rows):,} queued across main lanes, "
            f"{sum(int(row.get('processed_24h', 0) or 0) for row in main_rows):,} processed in the last 24 hours."
        )
        if non_healthy:
            detail += f" Attention needed for: {', '.join(non_healthy)}."
        else:
            detail += " All main lanes are current or idle."
        return {
            "lane_key": "main_worker",
            "label": label,
            "status": status,
            "last_success_at": max((row.get("last_success_at") or "-" for row in main_rows), default="-"),
            "runs_24h": sum(int(row.get("runs_24h", 0) or 0) for row in main_rows),
            "processed_24h": sum(int(row.get("processed_24h", 0) or 0) for row in main_rows),
            "succeeded_24h": sum(int(row.get("succeeded_24h", 0) or 0) for row in main_rows),
            "failed_24h": sum(int(row.get("failed_24h", 0) or 0) for row in main_rows),
            "queued_count": sum(int(row.get("queued_count", 0) or 0) for row in main_rows),
            "due_count": sum(int(row.get("due_count", 0) or 0) for row in main_rows),
            "in_progress_count": sum(int(row.get("in_progress_count", 0) or 0) for row in main_rows),
            "detail": detail,
        }

    def _classify_queue_lane_status(
        self,
        *,
        last_success_at: Any,
        last_started_at: Any,
        stale_hours: int,
        queued_count: int,
        due_count: int,
        in_progress_count: int,
        running_count: int,
        succeeded_24h: int,
        failed_24h: int,
    ) -> str:
        """Return one normalized process-health state for a queue-backed lane."""

        if due_count == 0:
            return "healthy" if in_progress_count > 0 or running_count > 0 else "flat"
        if due_count > 0 and failed_24h > 0 and succeeded_24h == 0 and not self._has_recent_running_activity(last_started_at, in_progress_count, running_count):
            return "failed"
        if due_count > 0 and self._is_timestamp_stale(last_success_at, stale_hours):
            if self._has_recent_running_activity(last_started_at, in_progress_count, running_count):
                return "backlogged"
            return "stale"
        if queued_count > 0 and (succeeded_24h == 0 or due_count > max(succeeded_24h * 10, 25)):
            return "backlogged"
        return "healthy"

    def _has_recent_running_activity(
        self,
        timestamp_value: Any,
        in_progress_count: int,
        running_count: int,
    ) -> bool:
        """Return whether a lane appears to have an active run inside the grace window."""

        if in_progress_count <= 0 and running_count <= 0:
            return False
        timestamp = self._coerce_datetime(timestamp_value)
        if not timestamp:
            return False
        elapsed_seconds = (datetime.utcnow() - timestamp.replace(tzinfo=None)).total_seconds()
        return elapsed_seconds <= (self.settings.process_watchdog_running_grace_minutes * 60)

    def _is_timestamp_stale(self, timestamp_value: Any, stale_hours: int) -> bool:
        """Return whether the timestamp is older than the allowed window."""

        if stale_hours <= 0:
            return False
        timestamp = self._coerce_datetime(timestamp_value)
        if not timestamp:
            return True
        elapsed_seconds = (datetime.utcnow() - timestamp.replace(tzinfo=None)).total_seconds()
        return elapsed_seconds > (stale_hours * 3600)

    def _coerce_datetime(self, value: Any) -> datetime | None:
        """Normalize a timestamp-like value for stale checks."""

        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

    def _get_connections(self, overview: dict[str, Any]) -> list[DashboardConnectionStatus]:
        """Create human-readable system health rows."""

        system_statuses = self._get_named_system_statuses(
            ("client_dim", "managed_fetch", "gbp_enrichment", "ai_retrieval", "process_watchdog")
        )
        discovery_overview = self._get_discovery_overview()
        ai_configured = bool(
            (self.settings.gemini_enabled and self.settings.gemini_api_key)
            or (self.settings.openai_enabled and self.settings.openai_api_key)
        )
        return [
            DashboardConnectionStatus(
                name="BigQuery",
                status="healthy",
                detail=f"Connected to {self.settings.bigquery_project_id}.{self.settings.bigquery_dataset}",
            ),
            DashboardConnectionStatus(
                name="Source Table",
                status="healthy" if int(overview.get("source_contacts", 0)) > 0 else "warning",
                detail=f"{int(overview.get('source_contacts', 0)):,} source contacts available",
            ),
            DashboardConnectionStatus(
                name="Worker Queue",
                status="healthy" if int(overview.get("pipeline_runs", 0)) > 0 else "warning",
                detail=f"{int(overview.get('queue_in_progress', 0)):,} items in progress across queue workers",
            ),
            DashboardConnectionStatus(
                name="Discovery Worker",
                status="healthy" if self.settings.domain_discovery_enabled else "warning",
                detail=(
                    f"{int(discovery_overview.get('queued_tasks', 0)):,} queued search tasks and "
                    f"{int(discovery_overview.get('runs_last_24h', 0)):,} run(s) logged in the last 24 hours"
                ),
            ),
            DashboardConnectionStatus(
                name="Blocked-Site Retry",
                status="warning" if int(overview.get("blocked_fetch_accounts", 0)) > 0 else "healthy",
                detail=f"{int(overview.get('blocked_fetch_accounts', 0)):,} dealer sites still blocked or challenged",
            ),
            self._system_status_row(
                name="Client DIM",
                sync_row=system_statuses.get("client_dim"),
                fallback_status="warning",
                fallback_detail=(
                    "Configured for future client-suppression matching."
                    if self.settings.client_dim_enabled
                    else "Client DIM integration is not configured yet."
                ),
            ),
            self._system_status_row(
                name="Managed Fetch",
                sync_row=system_statuses.get("managed_fetch"),
                fallback_status="warning" if int(overview.get("managed_fetch_eligible_accounts", 0)) > 0 else "healthy",
                fallback_detail=(
                    f"{int(overview.get('managed_fetch_eligible_accounts', 0)):,} hard blocked sites are eligible for escalation."
                ),
            ),
            self._system_status_row(
                name="GBP Enrichment",
                sync_row=system_statuses.get("gbp_enrichment"),
                fallback_status="healthy" if self.settings.gbp_enrichment_enabled else "warning",
                fallback_detail=self._build_gbp_connection_detail(overview),
            ),
            self._system_status_row(
                name="AI Retrieval",
                sync_row=system_statuses.get("ai_retrieval"),
                fallback_status="healthy" if self.settings.ai_retrieval_enabled and ai_configured else "warning",
                fallback_detail=self._build_ai_connection_detail(overview),
            ),
            self._system_status_row(
                name="Process Watchdog",
                sync_row=system_statuses.get("process_watchdog"),
                fallback_status="warning" if self.settings.process_watchdog_enabled else "healthy",
                fallback_detail=(
                    "Nightly watchdog has not reported yet."
                    if self.settings.process_watchdog_enabled
                    else "Nightly watchdog is disabled."
                ),
            ),
        ]

    def _build_gbp_connection_detail(self, overview: dict[str, Any]) -> str:
        """Describe GBP lane coverage and recent verification activity."""

        return (
            f"{int(overview.get('accounts_with_gbp_phone', 0)):,} accounts have GBP phone coverage, "
            f"{int(overview.get('accounts_with_gbp_address', 0)):,} have GBP address coverage, "
            f"{int(overview.get('gbp_verified_last_24h', 0)):,} were verified in the last 24 hours, "
            f"and {int(overview.get('queued_gbp_enrich', 0)):,} are still queued. "
            f"Last GBP verification: {self._format_timestamp(overview.get('last_gbp_verified_at'))}."
        )

    def _build_ai_connection_detail(self, overview: dict[str, Any]) -> str:
        """Describe AI lane coverage and recent verification activity."""

        return (
            f"{int(overview.get('accounts_with_ai_phone', 0)):,} accounts have AI phone coverage, "
            f"{int(overview.get('accounts_with_ai_address', 0)):,} have AI address coverage, "
            f"{int(overview.get('ai_verified_last_24h', 0)):,} were verified in the last 24 hours, "
            f"and {int(overview.get('queued_ai_account_facts', 0)):,} are still queued. "
            f"Last AI verification: {self._format_timestamp(overview.get('last_ai_verified_at'))}."
        )

    def _format_timestamp(self, value: Any) -> str:
        """Format a BigQuery timestamp into a compact dashboard string."""

        if not value:
            return "-"
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %I:%M %p UTC")
        return str(value)

    def _get_integration_connections(self) -> list[DashboardConnectionStatus]:
        """Return status rows for downstream platform integrations."""

        latest_syncs = self._get_latest_sync_statuses()

        return [
            self._build_integration_status(
                name="Campaign Monitor",
                configured=bool(
                    self.settings.campaign_monitor_api_key
                    and self.settings.campaign_monitor_client_id
                ),
                sync_row=latest_syncs.get("campaign_monitor"),
                missing_detail="Missing API key or client ID.",
            ),
            self._build_integration_status(
                name="Meta",
                configured=bool(
                    self.settings.meta_access_token
                    and self.settings.meta_ad_account_id
                ),
                sync_row=latest_syncs.get("meta"),
                missing_detail="Missing access token or ad account ID.",
            ),
            self._build_integration_status(
                name="Google Ads",
                configured=bool(
                    self.settings.google_ads_developer_token
                    and self.settings.google_ads_customer_id
                ),
                sync_row=latest_syncs.get("google_ads"),
                missing_detail="Missing developer token or customer ID.",
            ),
        ]

    def _build_integration_status(
        self,
        name: str,
        configured: bool,
        sync_row: dict[str, Any] | None,
        missing_detail: str,
    ) -> DashboardConnectionStatus:
        """Map configuration and sync history into one dashboard status row."""

        if not configured:
            return DashboardConnectionStatus(
                name=name,
                status="error",
                detail=f"Not connected. {missing_detail}",
            )

        if not sync_row:
            return DashboardConnectionStatus(
                name=name,
                status="warning",
                detail="Configured, but no sync attempts have been recorded yet.",
            )

        sync_status = (sync_row.get("sync_status") or "unknown").lower()
        last_synced_at = sync_row.get("last_synced_at")
        sync_detail = str(sync_row.get("sync_detail") or "").strip()
        detail = f"Latest sync status: {sync_status}"
        if last_synced_at:
            detail += f" at {last_synced_at}"
        if sync_detail:
            detail += f". {sync_detail}"

        if sync_status in {"synced", "success", "completed"}:
            status = "healthy"
        elif sync_status in {"failed", "error"}:
            status = "error"
        else:
            status = "warning"

        return DashboardConnectionStatus(
            name=name,
            status=status,
            detail=detail,
        )

    def _get_latest_sync_statuses(self) -> dict[str, dict[str, Any]]:
        """Return the latest sync row for each target system."""

        query = f"""
        SELECT
          target_system,
          sync_status,
          sync_detail,
          last_synced_at
        FROM (
          SELECT
            target_system,
            sync_status,
            sync_detail,
            last_synced_at,
            ROW_NUMBER() OVER (
              PARTITION BY target_system
              ORDER BY last_synced_at DESC NULLS LAST, updated_at DESC NULLS LAST, created_at DESC
            ) AS row_number
          FROM `{self.settings.sync_targets_table_fqn}`
          WHERE target_system IN ('campaign_monitor', 'meta', 'google_ads')
        )
        WHERE row_number = 1
        """
        rows = self.repository.fetch_all(query)
        return {str(row["target_system"]): row for row in rows}

    def _get_named_system_statuses(self, systems: tuple[str, ...]) -> dict[str, dict[str, Any]]:
        """Return the latest sync row for each requested system."""

        if not systems:
            return {}
        systems_sql = ", ".join(f"'{system}'" for system in systems)
        query = f"""
        SELECT
          target_system,
          sync_status,
          sync_detail,
          last_synced_at
        FROM (
          SELECT
            target_system,
            sync_status,
            sync_detail,
            last_synced_at,
            ROW_NUMBER() OVER (
              PARTITION BY target_system
              ORDER BY last_synced_at DESC NULLS LAST, updated_at DESC NULLS LAST, created_at DESC
            ) AS row_number
          FROM `{self.settings.sync_targets_table_fqn}`
          WHERE target_system IN ({systems_sql})
        )
        WHERE row_number = 1
        """
        rows = self.repository.fetch_all(query)
        return {str(row["target_system"]): row for row in rows}

    def _system_status_row(
        self,
        name: str,
        sync_row: dict[str, Any] | None,
        fallback_status: str,
        fallback_detail: str,
    ) -> DashboardConnectionStatus:
        """Convert one latest-system-status row into a dashboard connection row."""

        if not sync_row:
            return DashboardConnectionStatus(name=name, status=fallback_status, detail=fallback_detail)

        sync_status = str(sync_row.get("sync_status") or "unknown").lower()
        last_synced_at = sync_row.get("last_synced_at")
        sync_detail = str(sync_row.get("sync_detail") or "").strip()
        detail = f"Latest status: {sync_status}"
        if last_synced_at:
            detail += f" at {last_synced_at}"
        if sync_detail:
            detail += f". {sync_detail}"
        if sync_status in {"synced", "success", "completed", "configured", "healthy", "repaired"}:
            status = "healthy"
        elif sync_status in {"failed", "error"}:
            status = "error"
        elif sync_status in {"warning"}:
            status = "warning"
        else:
            status = fallback_status
        return DashboardConnectionStatus(name=name, status=status, detail=detail)

    def _get_classification_counts(self) -> list[dict[str, Any]]:
        """Return account counts by dealer classification."""

        query = f"""
        SELECT
          COALESCE(dealer_classification, 'unclassified') AS label,
          COUNT(*) AS total_accounts
        FROM `{self.settings.dealer_accounts_table_fqn}`
        GROUP BY label
        ORDER BY total_accounts DESC, label ASC
        """
        return self.repository.fetch_all(query)

    def _get_fetch_status_counts(self) -> list[dict[str, Any]]:
        """Return fetch counts for dealer and dealer_group websites."""

        query = f"""
        SELECT
          COALESCE(fetch_status, 'not_attempted') AS label,
          COUNT(*) AS total_accounts
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE dealer_classification IN ('dealer', 'dealer_group')
        GROUP BY label
        ORDER BY total_accounts DESC, label ASC
        """
        return self.repository.fetch_all(query)

    def _get_brand_counts(self) -> list[dict[str, Any]]:
        """Return the leading validated brands for dashboard charts."""

        query = f"""
        SELECT
          inferred_brand AS label,
          COUNT(*) AS total_accounts
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE dealer_classification IN ('dealer', 'dealer_group')
          AND inferred_brand IS NOT NULL
          AND TRIM(inferred_brand) != ''
        GROUP BY inferred_brand
        ORDER BY total_accounts DESC, label ASC
        LIMIT 15
        """
        return self.repository.fetch_all(query)

    def _get_role_family_counts(self) -> list[dict[str, Any]]:
        """Return extracted contact counts by role family."""

        query = f"""
        SELECT
          COALESCE(role_family, 'unclassified') AS label,
          COUNT(*) AS total_contacts
        FROM `{self.settings.prospect_contacts_table_fqn}`
        WHERE source_type = 'website_contact_extraction'
        GROUP BY label
        ORDER BY total_contacts DESC, label ASC
        LIMIT 12
        """
        return self.repository.fetch_all(query)

    def _get_ai_provider_counts(self) -> list[dict[str, Any]]:
        """Return recent AI retrieval success/failure counts by provider."""

        query = f"""
        SELECT
          provider AS label,
          COUNTIF(provider_status = 'success') AS successful_retrievals,
          COUNTIF(provider_status = 'failed') AS failed_retrievals,
          COUNTIF(provider_status = 'rate_limited') AS rate_limited_retrievals,
          COUNT(*) AS total_attempts
        FROM `{self.settings.ai_retrieval_results_table_fqn}`
        WHERE retrieved_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY provider
        ORDER BY total_attempts DESC, provider ASC
        """
        return self.repository.fetch_all(query)

    def _get_queue_rows(self) -> list[dict[str, Any]]:
        """Return queue status counts for each worker type."""

        if self.settings.parallel_enrichment_enabled:
            return [
                self._parallel_queue_row("validate", self.settings.validate_queue_table_fqn),
                self._parallel_queue_row("crawl", self.settings.crawl_queue_table_fqn),
                self._parallel_queue_row("gbp", self.settings.gbp_queue_table_fqn),
                self._parallel_queue_row("ai", self.settings.ai_queue_table_fqn),
                self._parallel_queue_row("contact_extract", self.settings.contact_extract_queue_table_fqn),
                self._parallel_queue_row("blocked_retry", self.settings.blocked_retry_queue_table_fqn),
                self._parallel_queue_row("lead_refresh", self.settings.lead_refresh_queue_table_fqn),
            ]
        query = f"""
        SELECT
          task_type,
          COUNTIF(status IN ('pending', 'retry')) AS queued_count,
          COUNTIF(status = 'in_progress') AS in_progress_count,
          COUNTIF(status = 'completed') AS completed_count,
          COUNTIF(status = 'failed') AS failed_count
        FROM `{self.settings.account_work_queue_table_fqn}`
        GROUP BY task_type
        ORDER BY task_type ASC
        """
        return self.repository.fetch_all(query)

    def _parallel_queue_row(self, lane_key: str, table_fqn: str) -> dict[str, Any]:
        """Return one queue throughput row for a dedicated parallel lane table."""

        row = self.repository.fetch_one(
            f"""
            SELECT
              '{lane_key}' AS task_type,
              COUNTIF(status IN ('pending', 'retry')) AS queued_count,
              COUNTIF(status = 'in_progress') AS in_progress_count,
              COUNTIF(status = 'completed') AS completed_count,
              COUNTIF(status = 'failed') AS failed_count
            FROM `{table_fqn}`
            """
        )
        return row

    def _get_frontier_rows(self) -> list[dict[str, Any]]:
        """Return phase-aware queue counts so first-pass vs retry is visible."""

        if not self.settings.parallel_enrichment_enabled:
            return []
        rows = [
            ("Validate", self.settings.validate_queue_table_fqn),
            ("Crawl", self.settings.crawl_queue_table_fqn),
            ("GBP Enrichment", self.settings.gbp_queue_table_fqn),
            ("AI Retrieval", self.settings.ai_queue_table_fqn),
            ("Contact Extraction", self.settings.contact_extract_queue_table_fqn),
            ("Blocked Retry", self.settings.blocked_retry_queue_table_fqn),
            ("Lead Refresh", self.settings.lead_refresh_queue_table_fqn),
        ]
        output: list[dict[str, Any]] = []
        for label, table_fqn in rows:
            output.append(
                self.repository.fetch_one(
                    f"""
                    SELECT
                      '{label}' AS lane_label,
                      COUNTIF(status IN ('pending', 'retry') AND work_phase = 'new_discovery_first_pass') AS discovery_first_pass_count,
                      COUNTIF(status IN ('pending', 'retry') AND work_phase = 'main_list_first_pass') AS main_list_first_pass_count,
                      COUNTIF(status IN ('pending', 'retry') AND work_phase = 'follow_up') AS follow_up_count,
                      COUNTIF(status IN ('pending', 'retry') AND work_phase = 'retry') AS retry_count
                    FROM `{table_fqn}`
                    """
                )
            )
        return output

    def _get_recent_runs(self) -> list[dict[str, Any]]:
        """Return the most recent worker runs."""

        query = f"""
        SELECT
          task_type,
          run_status,
          requested_batch_size,
          claimed_count,
          succeeded_count,
          failed_count,
          worker_id,
          started_at,
          completed_at,
          run_notes
        FROM `{self.settings.pipeline_runs_table_fqn}`
        ORDER BY started_at DESC
        LIMIT 12
        """
        return self.repository.fetch_all(query)

    def _get_recent_discovery_runs(self) -> list[dict[str, Any]]:
        """Return the most recent discovery-only runs."""

        query = f"""
        SELECT
          run_type,
          run_status,
          requested_batch_size,
          claimed_count,
          succeeded_count,
          failed_count,
          worker_id,
          started_at,
          completed_at,
          run_notes
        FROM `{self.settings.domain_discovery_runs_table_fqn}`
        ORDER BY started_at DESC
        LIMIT 8
        """
        return self.repository.fetch_all(query)

    def _get_blocked_accounts(self) -> list[dict[str, Any]]:
        """Return a small list of blocked dealer sites for operational review."""

        query = f"""
        SELECT
          account_key,
          website_url,
          inferred_brand,
          blocked_reason,
          blocked_attempt_count,
          managed_fetch_status,
          last_fetch_attempt_at
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE dealer_classification IN ('dealer', 'dealer_group')
          AND fetch_status = 'blocked'
        ORDER BY blocked_attempt_count DESC, last_fetch_attempt_at DESC
        LIMIT 12
        """
        try:
            return self.repository.fetch_all(query)
        except Exception as exc:
            logger.warning("Blocked accounts dashboard query failed; returning empty list | error=%s", exc)
            return []
