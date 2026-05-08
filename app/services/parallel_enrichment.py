"""Parallel lane queue manager and worker services."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import uuid

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger
from app.services.account_enrichment import AccountEnrichmentService
from app.services.ai_retrieval import AiRetrievalService
from app.services.browser_retry import BrowserRetryService
from app.services.campaign_monitor import CampaignMonitorService
from app.services.client_dim import ClientDimService
from app.services.contact_extraction import ContactExtractionService
from app.services.dealer_validation import DealerValidationService
from app.services.gbp_enrichment import GbpEnrichmentService
from app.services.prospect_leads import ProspectLeadService
from app.services.work_queue import PipelineRunLogger
from app.dashboard_service import DashboardService


logger = get_logger(__name__)

LANE_VALIDATE = "validate"
LANE_CRAWL = "crawl"
LANE_GBP = "gbp"
LANE_AI = "ai"
LANE_CONTACT_EXTRACT = "contact_extract"
LANE_BLOCKED_RETRY = "blocked_retry"
LANE_LEAD_REFRESH = "lead_refresh"
PHASE_DISCOVERY_FIRST_PASS = "new_discovery_first_pass"
PHASE_MAIN_LIST_FIRST_PASS = "main_list_first_pass"
PHASE_FOLLOW_UP = "follow_up"
PHASE_RETRY = "retry"
LANES = (
    LANE_VALIDATE,
    LANE_CRAWL,
    LANE_GBP,
    LANE_AI,
    LANE_CONTACT_EXTRACT,
    LANE_BLOCKED_RETRY,
    LANE_LEAD_REFRESH,
)
ACTIVE_QUEUE_STATUSES = ("pending", "retry", "in_progress")
MANAGER_SEED_LIMIT_PER_LANE = 250


@dataclass(frozen=True)
class LaneManagerResult:
    """Summary from one queue manager pass."""

    status: str
    detail: str
    seeded_counts: dict[str, int]
    duplicate_active_items: int


class ParallelLaneManagerService:
    """Own lane queues, event handoff, and dedupe logic."""

    CONNECTION_RECORD_ID = "parallel_cutover"
    LOCK_RECORD_ID = "parallel_cutover_lock"

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def queue_table_fqn(self, lane: str) -> str:
        """Return the queue table for one lane."""

        mapping = {
            LANE_VALIDATE: self.settings.validate_queue_table_fqn,
            LANE_CRAWL: self.settings.crawl_queue_table_fqn,
            LANE_GBP: self.settings.gbp_queue_table_fqn,
            LANE_AI: self.settings.ai_queue_table_fqn,
            LANE_CONTACT_EXTRACT: self.settings.contact_extract_queue_table_fqn,
            LANE_BLOCKED_RETRY: self.settings.blocked_retry_queue_table_fqn,
            LANE_LEAD_REFRESH: self.settings.lead_refresh_queue_table_fqn,
        }
        if lane not in mapping:
            raise ValueError(f"Unsupported lane: {lane}")
        return mapping[lane]

    def batch_size_for_lane(self, lane: str) -> int:
        """Return the configured batch size for one lane."""

        return {
            LANE_VALIDATE: self.settings.validate_worker_batch_size,
            LANE_CRAWL: self.settings.enrich_worker_batch_size,
            LANE_GBP: self.settings.gbp_worker_batch_size,
            LANE_AI: self.settings.ai_retrieval_batch_size,
            LANE_CONTACT_EXTRACT: self.settings.extract_worker_batch_size,
            LANE_BLOCKED_RETRY: self.settings.retry_blocked_worker_batch_size,
            LANE_LEAD_REFRESH: max(self.settings.campaign_monitor_sync_batch_size, 250),
        }[lane]

    def seed_all(self, dry_run: bool = False) -> dict[str, int]:
        """Seed all lanes from canonical state."""

        counts: dict[str, int] = {}
        for lane in LANES:
            counts[lane] = self.seed_lane(lane, dry_run=dry_run)
        return counts

    def seed_lane(self, lane: str, dry_run: bool = False) -> int:
        """Seed one lane queue from canonical state."""

        rows = self.repository.fetch_all(
            f"""
            SELECT account_key, priority, reason, work_phase, intake_source, intake_rank
            FROM ({self._seed_source_query(lane)})
            LIMIT {MANAGER_SEED_LIMIT_PER_LANE}
            """
        )
        if dry_run or not rows:
            return len(rows)

        self.enqueue_many(
            lane=lane,
            items=[
                {
                    "account_key": str(row["account_key"]),
                    "reason": str(row["reason"]),
                    "source_lane": "seed",
                    "parent_lane": None,
                    "priority": int(row["priority"]),
                    "work_phase": str(row.get("work_phase") or PHASE_MAIN_LIST_FIRST_PASS),
                    "intake_source": str(row.get("intake_source") or "primary_import"),
                    "intake_rank": int(row.get("intake_rank") or 0),
                }
                for row in rows
            ],
        )
        return len(rows)

    def enqueue(
        self,
        lane: str,
        account_key: str,
        reason: str,
        source_lane: str,
        parent_lane: str | None,
        priority: int | None = None,
    ) -> None:
        """Create or reopen one lane queue item."""

        self.enqueue_many(
            lane=lane,
            items=[
                {
                    "account_key": account_key,
                    "reason": reason,
                    "source_lane": source_lane,
                    "parent_lane": parent_lane,
                    "priority": priority,
                    "work_phase": PHASE_FOLLOW_UP,
                    "intake_source": "other",
                    "intake_rank": 0,
                }
            ],
        )

    def enqueue_many(self, lane: str, items: list[dict[str, object]]) -> None:
        """Create or reopen many queue items for one lane in one statement."""

        if not items:
            return

        chunk_size = 100
        for start in range(0, len(items), chunk_size):
            self._enqueue_many_chunk(lane=lane, items=items[start : start + chunk_size])

    def _enqueue_many_chunk(self, lane: str, items: list[dict[str, object]]) -> None:
        """Create or reopen one chunk of queue items for one lane."""

        queue_table = self.queue_table_fqn(lane)
        source_rows = []
        for item in items:
            account_key = str(item["account_key"])
            reason = str(item["reason"])
            source_lane = str(item["source_lane"])
            parent_lane = item.get("parent_lane")
            priority = item.get("priority")
            priority_value = int(priority) if priority is not None else self._default_priority(lane)
            work_phase = str(item.get("work_phase") or PHASE_FOLLOW_UP)
            intake_source = str(item.get("intake_source") or "other")
            intake_rank = int(item.get("intake_rank") or 0)
            parent_sql = "CAST(NULL AS STRING)" if not parent_lane else f"CAST('{self._escape_sql(str(parent_lane))}' AS STRING)"
            source_rows.append(
                "SELECT "
                f"CAST('{self._escape_sql(account_key)}' AS STRING) AS account_key, "
                f"CAST('{self._escape_sql(self._dedupe_key(account_key, reason))}' AS STRING) AS dedupe_key, "
                f"CAST('{self._escape_sql(reason)}' AS STRING) AS reason, "
                f"CAST('{self._escape_sql(source_lane)}' AS STRING) AS source_lane, "
                f"{parent_sql} AS parent_lane, "
                f"CAST({priority_value} AS INT64) AS priority, "
                f"CAST('{self._escape_sql(work_phase)}' AS STRING) AS work_phase, "
                f"CAST('{self._escape_sql(intake_source)}' AS STRING) AS intake_source, "
                f"CAST({intake_rank} AS INT64) AS intake_rank"
            )
        query = f"""
        MERGE `{queue_table}` AS target
        USING (
          {' UNION ALL '.join(source_rows)}
        ) AS source
        ON target.account_key = source.account_key
           AND target.dedupe_key = source.dedupe_key
        WHEN MATCHED AND target.status IN ('completed', 'failed') THEN
          UPDATE SET
            status = 'pending',
            priority = GREATEST(IFNULL(target.priority, 0), source.priority),
            source_lane = source.source_lane,
            parent_lane = source.parent_lane,
            work_phase = source.work_phase,
            intake_source = source.intake_source,
            intake_rank = source.intake_rank,
            next_attempt_at = CURRENT_TIMESTAMP(),
            retry_due_at = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = NULL,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP()
        WHEN MATCHED AND target.status IN ('pending', 'retry', 'in_progress') THEN
          UPDATE SET
            priority = GREATEST(IFNULL(target.priority, 0), source.priority),
            source_lane = source.source_lane,
            parent_lane = source.parent_lane,
            work_phase = CASE
              WHEN target.work_phase = '{PHASE_RETRY}' AND source.work_phase != '{PHASE_RETRY}' THEN target.work_phase
              ELSE source.work_phase
            END,
            intake_source = COALESCE(NULLIF(target.intake_source, ''), source.intake_source),
            intake_rank = CASE
              WHEN IFNULL(target.intake_rank, 0) = 0 THEN source.intake_rank
              ELSE target.intake_rank
            END,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (
            work_item_id,
            account_key,
            status,
            priority,
            attempt_count,
            reason,
            dedupe_key,
            parent_lane,
            source_lane,
            work_phase,
            intake_source,
            intake_rank,
            lease_owner,
            lease_expires_at,
            last_attempt_at,
            first_attempt_at,
            next_attempt_at,
            retry_due_at,
            completed_at,
            last_error,
            created_at,
            updated_at
          )
          VALUES (
            GENERATE_UUID(),
            source.account_key,
            'pending',
            source.priority,
            0,
            source.reason,
            source.dedupe_key,
            source.parent_lane,
            source.source_lane,
            source.work_phase,
            source.intake_source,
            source.intake_rank,
            NULL,
            NULL,
            NULL,
            NULL,
            CURRENT_TIMESTAMP(),
            NULL,
            NULL,
            NULL,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(query)

    def claim_batch(self, lane: str, batch_size: int, worker_id: str) -> list[str]:
        """Lease a batch of account keys for one lane."""

        queue_table = self.queue_table_fqn(lane)
        eligibility_sql = self._claim_eligibility_sql(lane)
        reclaim_query = f"""
        UPDATE `{queue_table}`
        SET
          status = 'retry',
          work_phase = '{PHASE_RETRY}',
          lease_owner = NULL,
          lease_expires_at = NULL,
          next_attempt_at = CURRENT_TIMESTAMP(),
          retry_due_at = CURRENT_TIMESTAMP(),
          last_error = COALESCE(last_error, 'Lane worker lease expired before completion.'),
          updated_at = CURRENT_TIMESTAMP()
        WHERE status = 'in_progress'
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at <= CURRENT_TIMESTAMP()
        """
        self.repository.execute_statement(reclaim_query)
        update_query = f"""
        UPDATE `{queue_table}`
        SET
          status = 'in_progress',
          lease_owner = '{self._escape_sql(worker_id)}',
          lease_expires_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {self.settings.worker_lease_minutes} MINUTE),
          last_attempt_at = CURRENT_TIMESTAMP(),
          first_attempt_at = COALESCE(first_attempt_at, CURRENT_TIMESTAMP()),
          attempt_count = IFNULL(attempt_count, 0) + 1,
          updated_at = CURRENT_TIMESTAMP()
        WHERE work_item_id IN (
          SELECT work_item_id
          FROM `{queue_table}`
          WHERE status IN ('pending', 'retry')
            AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP())
            AND (lease_expires_at IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP())
            {eligibility_sql}
          ORDER BY
            CASE work_phase
              WHEN '{PHASE_DISCOVERY_FIRST_PASS}' THEN 1
              WHEN '{PHASE_MAIN_LIST_FIRST_PASS}' THEN 2
              WHEN '{PHASE_FOLLOW_UP}' THEN 3
              ELSE 4
            END ASC,
            priority DESC,
            CASE
              WHEN work_phase = '{PHASE_DISCOVERY_FIRST_PASS}' THEN COALESCE(intake_rank, 0)
              ELSE COALESCE(intake_rank, 9223372036854775807)
            END ASC,
            created_at ASC
          LIMIT {batch_size}
        )
        """
        self.repository.execute_statement(update_query)
        query = f"""
        SELECT account_key
        FROM `{queue_table}`
        WHERE lease_owner = '{self._escape_sql(worker_id)}'
          AND status = 'in_progress'
        ORDER BY updated_at ASC
        """
        return [str(row["account_key"]) for row in self.repository.fetch_all(query)]

    def mark_completed(self, lane: str, account_keys: list[str]) -> None:
        """Mark queue items complete for one lane."""

        if not account_keys:
            return
        queue_table = self.queue_table_fqn(lane)
        account_keys_sql = ", ".join(f"'{self._escape_sql(value)}'" for value in account_keys)
        query = f"""
        UPDATE `{queue_table}`
        SET
          status = 'completed',
          lease_owner = NULL,
          lease_expires_at = NULL,
          completed_at = CURRENT_TIMESTAMP(),
          retry_due_at = NULL,
          last_error = NULL,
          updated_at = CURRENT_TIMESTAMP()
        WHERE account_key IN ({account_keys_sql})
          AND status = 'in_progress'
        """
        self.repository.execute_statement(query)

    def mark_failed(self, lane: str, account_keys: list[str], error_message: str) -> None:
        """Return queue items to retry for one lane."""

        if not account_keys:
            return
        queue_table = self.queue_table_fqn(lane)
        account_keys_sql = ", ".join(f"'{self._escape_sql(value)}'" for value in account_keys)
        retry_minutes = self._retry_delay_minutes(lane, error_message)
        escaped_error = self._escape_sql(error_message[:4000])
        query = f"""
        UPDATE `{queue_table}`
        SET
          status = 'retry',
          work_phase = '{PHASE_RETRY}',
          lease_owner = NULL,
          lease_expires_at = NULL,
          next_attempt_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {retry_minutes} MINUTE),
          retry_due_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {retry_minutes} MINUTE),
          last_error = '{escaped_error}',
          updated_at = CURRENT_TIMESTAMP()
        WHERE account_key IN ({account_keys_sql})
          AND status = 'in_progress'
        """
        self.repository.execute_statement(query)

    def update_lane_state(
        self,
        lane: str,
        account_keys: list[str],
        state_status: str,
        upstream_lane: str | None,
        detail: str,
    ) -> None:
        """Upsert operational state for a lane/account pair."""

        if not account_keys:
            return
        chunk_size = 100
        for start in range(0, len(account_keys), chunk_size):
            self._update_lane_state_chunk(
                lane=lane,
                account_keys=account_keys[start : start + chunk_size],
                state_status=state_status,
                upstream_lane=upstream_lane,
                detail=detail,
            )

    def route_after_lane(self, lane: str, account_keys: list[str]) -> None:
        """Enqueue downstream lanes based on the latest canonical state."""

        if not account_keys:
            return

        snapshots = self._load_account_snapshots(account_keys)
        lane_items: dict[str, list[dict[str, object]]] = {lane_name: [] for lane_name in LANES}
        seen_items: set[tuple[str, str, str]] = set()

        def queue_downstream(
            downstream_lane: str,
            account_key: str,
            reason: str,
            priority: int,
        ) -> None:
            dedupe_tuple = (downstream_lane, account_key, reason)
            if dedupe_tuple in seen_items:
                return
            seen_items.add(dedupe_tuple)
            lane_items[downstream_lane].append(
                {
                    "account_key": account_key,
                    "reason": reason,
                    "source_lane": lane,
                    "parent_lane": lane,
                    "priority": priority,
                    "work_phase": PHASE_FOLLOW_UP,
                    "intake_source": str(snapshot.get("intake_source") or "other"),
                    "intake_rank": int(snapshot.get("intake_rank") or 0),
                }
            )

        for snapshot in snapshots:
            account_key = snapshot["account_key"]
            website_exists = bool(snapshot.get("website_url"))
            blocked = str(snapshot.get("fetch_status") or "").lower() == "blocked"
            missing_core = self._missing_core_details(snapshot)
            has_contacts = int(snapshot.get("contact_count", 0) or 0) > 0
            is_dealer = str(snapshot.get("dealer_classification") or "") in {"dealer", "dealer_group"}

            if lane == LANE_VALIDATE:
                if is_dealer:
                    queue_downstream(LANE_CRAWL, account_key, "validated_dealer", 100)
                    queue_downstream(LANE_LEAD_REFRESH, account_key, "validated_state_changed", 30)
                continue

            if lane == LANE_CRAWL:
                if blocked:
                    queue_downstream(LANE_BLOCKED_RETRY, account_key, "crawl_blocked", 70)
                if missing_core or blocked:
                    queue_downstream(LANE_AI, account_key, "crawl_incomplete", 65)
                    queue_downstream(LANE_GBP, account_key, "crawl_missing_phone_or_location", 60)
                if website_exists and not has_contacts:
                    queue_downstream(LANE_CONTACT_EXTRACT, account_key, "crawl_has_website", 75)
                queue_downstream(LANE_LEAD_REFRESH, account_key, "crawl_completed", 40)
                continue

            if lane == LANE_AI:
                if missing_core:
                    queue_downstream(LANE_GBP, account_key, "ai_incomplete", 60)
                if website_exists and not has_contacts:
                    queue_downstream(LANE_CONTACT_EXTRACT, account_key, "ai_has_website", 70)
                queue_downstream(LANE_LEAD_REFRESH, account_key, "ai_completed", 40)
                continue

            if lane == LANE_GBP:
                if website_exists and not has_contacts:
                    queue_downstream(LANE_CONTACT_EXTRACT, account_key, "gbp_has_website", 70)
                queue_downstream(LANE_LEAD_REFRESH, account_key, "gbp_completed", 40)
                continue

            if lane == LANE_BLOCKED_RETRY:
                if blocked:
                    queue_downstream(LANE_AI, account_key, "retry_still_blocked", 65)
                else:
                    queue_downstream(LANE_CRAWL, account_key, "retry_recovered_site", 80)
                    if website_exists and not has_contacts:
                        queue_downstream(LANE_CONTACT_EXTRACT, account_key, "retry_has_website", 70)
                queue_downstream(LANE_LEAD_REFRESH, account_key, "blocked_retry_completed", 40)
                continue

            if lane == LANE_CONTACT_EXTRACT:
                queue_downstream(LANE_LEAD_REFRESH, account_key, "contacts_extracted", 40)

        for downstream_lane, items in lane_items.items():
            if items:
                self.enqueue_many(downstream_lane, items)

    def run_manager(self, dry_run: bool = False) -> LaneManagerResult:
        """Seed all lanes and record cutover health."""

        execution_id = str(uuid.uuid4())
        if not dry_run and not self._acquire_manager_lock(execution_id):
            detail = "Another queue-manager execution is already active; skipped reseed."
            return LaneManagerResult(
                status="skipped",
                detail=detail,
                seeded_counts={lane: 0 for lane in LANES},
                duplicate_active_items=self._count_duplicate_active_items(),
            )

        if not dry_run:
            self.backfill_frontier_metadata()
        seeded_counts = self.seed_all(dry_run=dry_run)
        duplicate_active_items = self._count_duplicate_active_items()
        detail = (
            f"Seeded lanes: {json.dumps(seeded_counts, sort_keys=True)}. "
            f"Duplicate active items: {duplicate_active_items}."
        )
        status = "warning" if duplicate_active_items else "healthy"
        if not dry_run:
            self._record_status(status, detail)
        return LaneManagerResult(status, detail, seeded_counts, duplicate_active_items)

    def backfill_frontier_metadata(self) -> None:
        """Classify legacy queue rows into discovery/main-list/follow-up/retry phases."""

        self.repository.execute_statement(
            f"""
            UPDATE `{self.settings.dealer_accounts_table_fqn}`
            SET
              intake_source = 'discovery',
              promoted_at = COALESCE(promoted_at, updated_at, created_at, CURRENT_TIMESTAMP()),
              intake_rank = COALESCE(intake_rank, CAST(-UNIX_SECONDS(COALESCE(promoted_at, updated_at, created_at, CURRENT_TIMESTAMP())) AS INT64)),
              updated_at = CURRENT_TIMESTAMP()
            WHERE COALESCE(NULLIF(TRIM(intake_source), ''), '') = ''
              AND (
                source_type = 'domain_discovery'
                OR enrichment_stage = 'discovery_seed'
                OR source_table = '{self.settings.discovered_domain_candidates_table}'
              )
            """
        )
        self.repository.execute_statement(
            f"""
            UPDATE `{self.settings.dealer_accounts_table_fqn}`
            SET
              intake_source = COALESCE(NULLIF(TRIM(intake_source), ''), 'primary_import'),
              intake_rank = COALESCE(intake_rank, CAST(UNIX_SECONDS(COALESCE(first_seen_at, created_at, CURRENT_TIMESTAMP())) AS INT64)),
              updated_at = CURRENT_TIMESTAMP()
            WHERE intake_source IS NULL
               OR intake_rank IS NULL
            """
        )
        for lane in LANES:
            queue_table = self.queue_table_fqn(lane)
            query = f"""
            UPDATE `{queue_table}` AS q
            SET
              work_phase = CASE
                WHEN q.status = 'retry' THEN '{PHASE_RETRY}'
                WHEN q.source_lane = 'seed' THEN CASE
                  WHEN COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import') = 'discovery'
                    THEN '{PHASE_DISCOVERY_FIRST_PASS}'
                  ELSE '{PHASE_MAIN_LIST_FIRST_PASS}'
                END
                WHEN q.parent_lane IS NOT NULL OR q.source_lane != 'seed' THEN '{PHASE_FOLLOW_UP}'
                ELSE '{PHASE_MAIN_LIST_FIRST_PASS}'
              END,
              intake_source = CASE
                WHEN q.source_lane = 'seed' THEN COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import')
                ELSE COALESCE(q.intake_source, da.intake_source, 'primary_import')
              END,
              intake_rank = CASE
                WHEN q.source_lane = 'seed' THEN COALESCE(
                  da.intake_rank,
                  CASE
                    WHEN COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import') = 'discovery'
                      THEN COALESCE(UNIX_SECONDS(da.promoted_at) * -1, UNIX_SECONDS(da.created_at) * -1, 0)
                    ELSE COALESCE(UNIX_SECONDS(da.first_seen_at), UNIX_SECONDS(da.created_at), 0)
                  END
                )
                ELSE COALESCE(
                  q.intake_rank,
                  da.intake_rank,
                  CASE
                    WHEN COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import') = 'discovery'
                      THEN COALESCE(UNIX_SECONDS(da.promoted_at) * -1, UNIX_SECONDS(da.created_at) * -1, 0)
                    ELSE COALESCE(UNIX_SECONDS(da.first_seen_at), UNIX_SECONDS(da.created_at), 0)
                  END
                )
              END,
              retry_due_at = CASE
                WHEN q.status = 'retry' THEN COALESCE(q.retry_due_at, q.next_attempt_at, CURRENT_TIMESTAMP())
                ELSE q.retry_due_at
              END,
              updated_at = CURRENT_TIMESTAMP()
            FROM `{self.settings.dealer_accounts_table_fqn}` AS da
            WHERE da.account_key = q.account_key
              AND (
                q.work_phase IS NULL
                OR q.intake_source IS NULL
                OR q.intake_rank IS NULL
                OR (q.status = 'retry' AND q.retry_due_at IS NULL)
                OR (
                  q.source_lane = 'seed'
                  AND COALESCE(NULLIF(TRIM(q.intake_source), ''), 'primary_import') != COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import')
                )
              )
            """
            self.repository.execute_statement(query)

    def _seed_source_query(self, lane: str) -> str:
        """Return the canonical source query for one lane safety-net seed."""

        def first_pass_select(priority: int, reason: str, dealer_filter: str) -> str:
            queue_table = self.queue_table_fqn(lane)
            return f"""
            SELECT
              da.account_key,
              {priority} AS priority,
              '{reason}' AS reason,
              CASE
                WHEN COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import') = 'discovery'
                  THEN '{PHASE_DISCOVERY_FIRST_PASS}'
                ELSE '{PHASE_MAIN_LIST_FIRST_PASS}'
              END AS work_phase,
              COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import') AS intake_source,
              CASE
                WHEN COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import') = 'discovery'
                  THEN COALESCE(UNIX_SECONDS(da.promoted_at) * -1, UNIX_SECONDS(da.created_at) * -1, 0)
                ELSE COALESCE(UNIX_SECONDS(da.first_seen_at), UNIX_SECONDS(da.created_at), 0)
              END AS intake_rank
            FROM `{self.settings.dealer_accounts_table_fqn}` AS da
            WHERE {dealer_filter}
              AND NOT EXISTS (
                SELECT 1
                FROM `{queue_table}` AS q
                WHERE q.account_key = da.account_key
                  AND q.first_attempt_at IS NOT NULL
              )
            """

        if lane == LANE_VALIDATE:
            return first_pass_select(
                priority=100,
                reason="validate_account",
                dealer_filter="IFNULL(da.is_personal_domain, FALSE) = FALSE",
            )
        if lane == LANE_CRAWL:
            return first_pass_select(
                priority=80,
                reason="crawl_first_pass",
                dealer_filter="da.dealer_classification IN ('dealer', 'dealer_group', 'unknown')",
            )
        if lane == LANE_GBP:
            return first_pass_select(
                priority=60,
                reason="gbp_first_pass",
                dealer_filter="da.dealer_classification IN ('dealer', 'dealer_group')",
            )
        if lane == LANE_AI:
            return first_pass_select(
                priority=65,
                reason="ai_first_pass",
                dealer_filter=(
                    "da.dealer_classification IN ('dealer', 'dealer_group') "
                    "AND (da.next_ai_retrieval_at IS NULL OR da.next_ai_retrieval_at <= CURRENT_TIMESTAMP())"
                ),
            )
        if lane == LANE_CONTACT_EXTRACT:
            return first_pass_select(
                priority=70,
                reason="contact_extract_first_pass",
                dealer_filter="da.dealer_classification IN ('dealer', 'dealer_group')",
            )
        if lane == LANE_BLOCKED_RETRY:
            return f"""
            SELECT
              da.account_key,
              70 AS priority,
              'blocked_site_retry_due' AS reason,
              '{PHASE_RETRY}' AS work_phase,
              COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import') AS intake_source,
              CASE
                WHEN COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import') = 'discovery'
                  THEN COALESCE(UNIX_SECONDS(da.promoted_at) * -1, UNIX_SECONDS(da.created_at) * -1, 0)
                ELSE COALESCE(UNIX_SECONDS(da.first_seen_at), UNIX_SECONDS(da.created_at), 0)
              END AS intake_rank
            FROM `{self.settings.dealer_accounts_table_fqn}` AS da
            WHERE da.dealer_classification IN ('dealer', 'dealer_group')
              AND da.fetch_status = 'blocked'
              AND {self._blocked_retry_due_sql('da')}
            """
        if lane == LANE_LEAD_REFRESH:
            return f"""
            WITH changed_accounts AS (
              SELECT DISTINCT
                da.account_key,
                da.intake_source,
                da.promoted_at,
                da.first_seen_at,
                da.created_at
              FROM `{self.settings.dealer_accounts_table_fqn}` AS da
              WHERE da.updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
              UNION DISTINCT
              SELECT DISTINCT
                da.account_key,
                da.intake_source,
                da.promoted_at,
                da.first_seen_at,
                da.created_at
              FROM `{self.settings.account_relationships_table_fqn}` AS ar
              JOIN `{self.settings.dealer_accounts_table_fqn}` AS da
                ON da.dealer_account_id = ar.dealer_account_id
              JOIN `{self.settings.prospect_contacts_table_fqn}` AS pc
                ON pc.prospect_contact_id = ar.prospect_contact_id
              WHERE ar.updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
                 OR pc.updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            )
            SELECT
              account_key,
              40 AS priority,
              'materialization_refresh_needed' AS reason,
              '{PHASE_FOLLOW_UP}' AS work_phase,
              COALESCE(NULLIF(TRIM(intake_source), ''), 'primary_import') AS intake_source,
              CASE
                WHEN COALESCE(NULLIF(TRIM(intake_source), ''), 'primary_import') = 'discovery'
                  THEN COALESCE(UNIX_SECONDS(promoted_at) * -1, UNIX_SECONDS(created_at) * -1, 0)
                ELSE COALESCE(UNIX_SECONDS(first_seen_at), UNIX_SECONDS(created_at), 0)
              END AS intake_rank
            FROM changed_accounts
            """
        raise ValueError(f"Unsupported lane: {lane}")

    def _load_account_snapshots(self, account_keys: list[str]) -> list[dict[str, object]]:
        """Load the canonical account/contact state used for routing."""

        account_keys_sql = ", ".join(f"'{self._escape_sql(value)}'" for value in account_keys)
        query = f"""
        SELECT
          da.account_key,
          da.dealer_classification,
          NULLIF(TRIM(da.website_url), '') AS website_url,
          da.fetch_status,
          da.best_phone,
          da.account_city,
          da.account_state,
          da.gbp_address_line,
          da.ai_address_line,
          da.managed_fetch_status,
          COALESCE(NULLIF(TRIM(da.intake_source), ''), 'primary_import') AS intake_source,
          COALESCE(da.intake_rank, 0) AS intake_rank,
          COUNT(DISTINCT ar.relationship_id) AS contact_count,
          COUNTIF(ar.source_type = 'website_contact_extraction') AS website_contact_count
        FROM `{self.settings.dealer_accounts_table_fqn}` AS da
        LEFT JOIN `{self.settings.account_relationships_table_fqn}` AS ar
          ON ar.dealer_account_id = da.dealer_account_id
        WHERE da.account_key IN ({account_keys_sql})
        GROUP BY
          da.account_key,
          da.dealer_classification,
          da.website_url,
          da.fetch_status,
          da.best_phone,
          da.account_city,
          da.account_state,
          da.gbp_address_line,
          da.ai_address_line,
          da.managed_fetch_status,
          da.intake_source,
          da.intake_rank
        """
        return self.repository.fetch_all(query)

    def _update_lane_state_chunk(
        self,
        *,
        lane: str,
        account_keys: list[str],
        state_status: str,
        upstream_lane: str | None,
        detail: str,
    ) -> None:
        """Upsert one chunk of lane state rows in a single statement."""

        if not account_keys:
            return
        escaped_lane = self._escape_sql(lane)
        escaped_status = self._escape_sql(state_status)
        escaped_detail = self._escape_sql(detail[:4000])
        escaped_reason = self._escape_sql(detail[:255])
        upstream_sql = "CAST(NULL AS STRING)" if not upstream_lane else f"CAST('{self._escape_sql(upstream_lane)}' AS STRING)"
        source_rows = [
            "SELECT "
            f"CAST('{escaped_lane}' AS STRING) AS lane_name, "
            f"CAST('{self._escape_sql(account_key)}' AS STRING) AS account_key, "
            f"{upstream_sql} AS upstream_lane"
            for account_key in account_keys
        ]
        query = f"""
        MERGE `{self.settings.lane_execution_state_table_fqn}` AS target
        USING (
          {' UNION ALL '.join(source_rows)}
        ) AS source
        ON target.lane_name = source.lane_name
           AND target.account_key = source.account_key
        WHEN MATCHED THEN
          UPDATE SET
            state_status = '{escaped_status}',
            freshness_status = '{escaped_status}',
            last_attempt_at = CURRENT_TIMESTAMP(),
            last_success_at = CASE
              WHEN '{escaped_status}' = 'success' THEN CURRENT_TIMESTAMP()
              ELSE target.last_success_at
            END,
            next_eligible_at = CURRENT_TIMESTAMP(),
            stale_after_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {self._stale_hours_for_lane(lane)} HOUR),
            upstream_lane = source.upstream_lane,
            last_handoff_reason = '{escaped_reason}',
            detail = '{escaped_detail}',
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (
            lane_state_id,
            lane_name,
            account_key,
            state_status,
            freshness_status,
            last_attempt_at,
            last_success_at,
            next_eligible_at,
            stale_after_at,
            upstream_lane,
            last_handoff_reason,
            detail,
            created_at,
            updated_at
          )
          VALUES (
            GENERATE_UUID(),
            source.lane_name,
            source.account_key,
            '{escaped_status}',
            '{escaped_status}',
            CURRENT_TIMESTAMP(),
            CASE WHEN '{escaped_status}' = 'success' THEN CURRENT_TIMESTAMP() ELSE NULL END,
            CURRENT_TIMESTAMP(),
            TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {self._stale_hours_for_lane(lane)} HOUR),
            source.upstream_lane,
            '{escaped_reason}',
            '{escaped_detail}',
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(query)

    def _missing_core_details(self, snapshot: dict[str, object]) -> bool:
        """Return whether phone or location still looks incomplete."""

        best_phone = str(snapshot.get("best_phone") or "").strip()
        city = str(snapshot.get("account_city") or "").strip()
        state = str(snapshot.get("account_state") or "").strip()
        return not best_phone or not city or not state

    def _count_duplicate_active_items(self) -> int:
        """Count duplicate active queue items across lane queues."""

        total = 0
        for lane in LANES:
            queue_table = self.queue_table_fqn(lane)
            query = f"""
            SELECT COUNT(*) AS duplicate_rows
            FROM (
              SELECT account_key, dedupe_key
              FROM `{queue_table}`
              WHERE status IN ('pending', 'retry', 'in_progress')
              GROUP BY account_key, dedupe_key
              HAVING COUNT(*) > 1
            )
            """
            row = self.repository.fetch_one(query)
            total += int(row.get("duplicate_rows", 0))
        return total

    def _record_status(self, sync_status: str, sync_detail: str) -> None:
        """Record queue-manager/cutover health into sync targets."""

        escaped_detail = self._escape_sql(sync_detail[:4000])
        query = f"""
        MERGE `{self.settings.sync_targets_table_fqn}` AS target
        USING (
          SELECT
            '{self.CONNECTION_RECORD_ID}' AS sync_target_id,
            'parallel_cutover' AS target_system,
            'system' AS target_entity_type,
            'parallel_workers' AS target_entity_id,
            'system' AS source_record_type,
            '{self.CONNECTION_RECORD_ID}' AS source_record_id
        ) AS source
        ON target.sync_target_id = source.sync_target_id
        WHEN MATCHED THEN
          UPDATE SET
            sync_status = '{self._escape_sql(sync_status)}',
            sync_detail = '{escaped_detail}',
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
            source.sync_target_id,
            source.target_system,
            source.target_entity_type,
            source.target_entity_id,
            source.source_record_type,
            source.source_record_id,
            '{self._escape_sql(sync_status)}',
            '{escaped_detail}',
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(query)

    def _acquire_manager_lock(self, execution_id: str) -> bool:
        """Acquire a coarse queue-manager lock so reseed passes do not overlap."""

        detail = self._escape_sql(f"execution:{execution_id}")
        query = f"""
        MERGE `{self.settings.sync_targets_table_fqn}` AS target
        USING (
          SELECT
            '{self.LOCK_RECORD_ID}' AS sync_target_id,
            'parallel_cutover_lock' AS target_system,
            'system' AS target_entity_type,
            'parallel_workers' AS target_entity_id,
            'system' AS source_record_type,
            '{self.LOCK_RECORD_ID}' AS source_record_id
        ) AS source
        ON target.sync_target_id = source.sync_target_id
        WHEN MATCHED
          AND (
            target.sync_status != 'running'
            OR target.updated_at IS NULL
            OR target.updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 MINUTE)
          )
        THEN
          UPDATE SET
            sync_status = 'running',
            sync_detail = '{detail}',
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
            source.sync_target_id,
            source.target_system,
            source.target_entity_type,
            source.target_entity_id,
            source.source_record_type,
            source.source_record_id,
            'running',
            '{detail}',
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(query)
        row = self.repository.fetch_one(
            f"""
            SELECT sync_status, sync_detail
            FROM `{self.settings.sync_targets_table_fqn}`
            WHERE sync_target_id = '{self.LOCK_RECORD_ID}'
            """
        )
        return (
            str(row.get("sync_status") or "") == "running"
            and str(row.get("sync_detail") or "") == f"execution:{execution_id}"
        )

    def _claim_eligibility_sql(self, lane: str) -> str:
        """Return additional claim-time eligibility checks for one lane."""

        queue_table = self.queue_table_fqn(lane)
        frontier_query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{queue_table}`
        WHERE status IN ('pending', 'retry')
          AND work_phase IN ('{PHASE_DISCOVERY_FIRST_PASS}', '{PHASE_MAIN_LIST_FIRST_PASS}')
          AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP())
          AND (lease_expires_at IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP())
        """
        frontier_count = int(self.repository.fetch_one(frontier_query).get("row_count", 0) or 0)
        clauses = []
        if frontier_count > 0:
            clauses.append(
                f"work_phase IN ('{PHASE_DISCOVERY_FIRST_PASS}', '{PHASE_MAIN_LIST_FIRST_PASS}')"
            )
        if lane == LANE_AI:
            clauses.append(
                f"""account_key IN (
                  SELECT account_key
                  FROM `{self.settings.dealer_accounts_table_fqn}`
                  WHERE dealer_classification IN ('dealer', 'dealer_group')
                    AND (
                      next_ai_retrieval_at IS NULL
                      OR next_ai_retrieval_at <= CURRENT_TIMESTAMP()
                    )
                )"""
            )
        if not clauses:
            return ""
        return "\n          AND " + "\n          AND ".join(clauses)

    def _default_priority(self, lane: str) -> int:
        return {
            LANE_VALIDATE: 100,
            LANE_CRAWL: 80,
            LANE_GBP: 60,
            LANE_AI: 65,
            LANE_CONTACT_EXTRACT: 70,
            LANE_BLOCKED_RETRY: 70,
            LANE_LEAD_REFRESH: 40,
        }[lane]

    def _retry_delay_minutes(self, lane: str, error_message: str) -> int:
        error_text = error_message.lower()
        if lane == LANE_AI and "rate limit" in error_text:
            return self.settings.ai_retrieval_rate_limit_cooldown_minutes
        return 60

    def _stale_hours_for_lane(self, lane: str) -> int:
        if lane == LANE_LEAD_REFRESH:
            return self.settings.process_watchdog_prospect_leads_stale_hours
        return self.settings.process_watchdog_main_stale_hours

    def _dedupe_key(self, account_key: str, reason: str) -> str:
        return f"{account_key.lower()}::{reason.lower()}"

    def _blocked_retry_due_sql(self, alias: str) -> str:
        """Return SQL for when a blocked retry is eligible."""

        prefix = f"{alias}." if alias else ""
        short_hours = self.settings.blocked_retry_short_cooldown_hours
        medium_hours = self.settings.blocked_retry_medium_cooldown_hours
        long_hours = self.settings.blocked_retry_long_cooldown_hours
        return f"""
        (
          {prefix}last_fetch_attempt_at IS NULL
          OR CURRENT_TIMESTAMP() >= CASE
            WHEN IFNULL({prefix}blocked_attempt_count, 0) <= 1 THEN TIMESTAMP_ADD({prefix}last_fetch_attempt_at, INTERVAL {short_hours} HOUR)
            WHEN IFNULL({prefix}blocked_attempt_count, 0) <= 3 THEN TIMESTAMP_ADD({prefix}last_fetch_attempt_at, INTERVAL {medium_hours} HOUR)
            ELSE TIMESTAMP_ADD({prefix}last_fetch_attempt_at, INTERVAL {long_hours} HOUR)
          END
        )
        """

    def _escape_sql(self, value: str) -> str:
        return (
            value.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        )


class ParallelLaneWorkerService:
    """Run one parallel enrichment lane at a time."""

    LANE_LOCK_PREFIX = "lane_worker_lock"

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.manager = ParallelLaneManagerService(repository, settings)
        self.run_logger = PipelineRunLogger(repository, settings)
        self.dealer_validation = DealerValidationService(repository, settings)
        self.account_enrichment = AccountEnrichmentService(repository, settings)
        self.gbp_enrichment = GbpEnrichmentService(repository, settings)
        self.ai_retrieval = AiRetrievalService(repository, settings)
        self.contact_extraction = ContactExtractionService(repository, settings)
        self.browser_retry = BrowserRetryService(repository, settings)
        self.prospect_lead_service = ProspectLeadService(repository, settings)
        self.client_dim_service = ClientDimService(repository, settings)
        self.dashboard_service = DashboardService(repository, settings)
        self.campaign_monitor_service = CampaignMonitorService(repository, settings)

    def run_lane(self, lane: str, dry_run: bool = False, seed: bool = False) -> dict[str, object]:
        """Run one lane queue loop."""

        if seed:
            self.manager.seed_lane(lane, dry_run=dry_run)
        batch_size = self.manager.batch_size_for_lane(lane)
        if dry_run:
            return {
                "lane": lane,
                "status": "preview",
                "claimed_count": 0,
                "succeeded_count": 0,
                "failed_count": 0,
                "detail": "Dry run did not claim any queue items.",
            }

        worker_id = str(uuid.uuid4())
        lock_id = self._acquire_lane_lock(lane, worker_id)
        if not lock_id:
            return {
                "lane": lane,
                "status": "skipped",
                "claimed_count": 0,
                "succeeded_count": 0,
                "failed_count": 0,
                "detail": f"Skipped {lane} because another execution is still active.",
            }

        pipeline_run_id = self.run_logger.start_run(
            task_type=lane,
            worker_id=worker_id,
            requested_batch_size=batch_size,
        )
        claimed_account_keys = self.manager.claim_batch(lane, batch_size, worker_id)
        account_keys = list(dict.fromkeys(claimed_account_keys))
        logger.info(
            "Lane worker claimed queue items",
            extra={
                "lane": lane,
                "claimed_count": len(claimed_account_keys),
                "unique_account_count": len(account_keys),
            },
        )
        if not account_keys:
            self.run_logger.finish_run(
                pipeline_run_id=pipeline_run_id,
                run_status="completed",
                claimed_count=0,
                succeeded_count=0,
                failed_count=0,
                run_notes="No lane queue items were available to claim.",
            )
            return {
                "lane": lane,
                "status": "completed",
                "claimed_count": 0,
                "succeeded_count": 0,
                "failed_count": 0,
                "detail": "No queue items were available to claim.",
            }

        try:
            logger.info("Lane worker dispatch starting", extra={"lane": lane, "claimed_count": len(account_keys)})
            self._dispatch_lane(lane, account_keys)
            logger.info("Lane worker dispatch finished", extra={"lane": lane, "claimed_count": len(account_keys)})
            self.manager.mark_completed(lane, account_keys)
            self.manager.update_lane_state(lane, account_keys, "success", None, f"{lane} completed")
            self.manager.route_after_lane(lane, account_keys)
            self.run_logger.finish_run(
                pipeline_run_id=pipeline_run_id,
                run_status="completed",
                claimed_count=len(account_keys),
                succeeded_count=len(account_keys),
                failed_count=0,
                run_notes=None,
            )
            return {
                "lane": lane,
                "status": "completed",
                "claimed_count": len(account_keys),
                "succeeded_count": len(account_keys),
                "failed_count": 0,
                "detail": f"Processed {len(account_keys)} account(s).",
            }
        except Exception as exc:
            error_message = str(exc)
            self.manager.mark_failed(lane, account_keys, error_message)
            self.manager.update_lane_state(lane, account_keys, "failed", None, error_message)
            self.run_logger.finish_run(
                pipeline_run_id=pipeline_run_id,
                run_status="failed",
                claimed_count=len(account_keys),
                succeeded_count=0,
                failed_count=len(account_keys),
                run_notes=error_message,
            )
            raise
        finally:
            self._record_lane_lock_status(lock_id=lock_id, status="idle", detail=f"worker:{worker_id}")

    def _dispatch_lane(self, lane: str, account_keys: list[str]) -> None:
        """Dispatch one lane to the underlying enrichment service."""

        limit = len(account_keys)
        if lane == LANE_VALIDATE:
            self.dealer_validation.validate(dry_run=False, limit=limit, account_keys=account_keys)
            return
        if lane == LANE_CRAWL:
            self.account_enrichment.enrich(dry_run=False, limit=limit, account_keys=account_keys)
            return
        if lane == LANE_GBP:
            result = self.gbp_enrichment.enrich(dry_run=False, limit=limit, account_keys=account_keys)
            if result.status not in {"success", "preview"}:
                raise RuntimeError(result.detail)
            return
        if lane == LANE_AI:
            result = self.ai_retrieval.refresh_account_facts(dry_run=False, limit=limit, account_keys=account_keys)
            if result.status in {"warning", "failed"}:
                raise RuntimeError(result.detail)
            return
        if lane == LANE_CONTACT_EXTRACT:
            self.contact_extraction.extract(dry_run=False, limit=limit, account_keys=account_keys)
            return
        if lane == LANE_BLOCKED_RETRY:
            self.browser_retry.retry(dry_run=False, limit=limit, account_keys=account_keys)
            return
        if lane == LANE_LEAD_REFRESH:
            self.prospect_lead_service.refresh(dry_run=False)
            self.client_dim_service.refresh(dry_run=False)
            self.dashboard_service.capture_snapshot()
            self.campaign_monitor_service.check_connection(dry_run=False)
            if self.settings.campaign_monitor_sync_enabled:
                self.campaign_monitor_service.sync_subscribers(
                    dry_run=False,
                    limit=self.settings.campaign_monitor_sync_batch_size,
                )
            return
        raise ValueError(f"Unsupported lane: {lane}")

    def _acquire_lane_lock(self, lane: str, worker_id: str) -> str | None:
        """Acquire one available per-lane lock slot and return its lock id."""

        detail = self._escape_sql(f"worker:{worker_id}")
        grace_minutes = max(self.settings.worker_lease_minutes, 15)
        for slot in range(1, self._max_parallel_workers_for_lane(lane) + 1):
            lock_id = self._lane_lock_id(lane, slot)
            query = f"""
            MERGE `{self.settings.sync_targets_table_fqn}` AS target
            USING (
              SELECT
                '{lock_id}' AS sync_target_id,
                '{self._escape_sql(lock_id)}' AS target_system,
                'lane_worker' AS target_entity_type,
                '{self._escape_sql(lane)}' AS target_entity_id,
                'system' AS source_record_type,
                '{lock_id}' AS source_record_id
            ) AS source
            ON target.sync_target_id = source.sync_target_id
            WHEN MATCHED
              AND (
                target.sync_status != 'running'
                OR target.updated_at IS NULL
                OR target.updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {grace_minutes} MINUTE)
              )
            THEN
              UPDATE SET
                sync_status = 'running',
                sync_detail = '{detail}',
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
                source.sync_target_id,
                source.target_system,
                source.target_entity_type,
                source.target_entity_id,
                source.source_record_type,
                source.source_record_id,
                'running',
                '{detail}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
              )
            """
            self.repository.execute_statement(query)
            row = self.repository.fetch_one(
                f"""
                SELECT sync_status, sync_detail
                FROM `{self.settings.sync_targets_table_fqn}`
                WHERE sync_target_id = '{lock_id}'
                """
            )
            if (
                str(row.get("sync_status") or "") == "running"
                and str(row.get("sync_detail") or "") == f"worker:{worker_id}"
            ):
                return lock_id
        return None

    def _record_lane_lock_status(self, lock_id: str, status: str, detail: str) -> None:
        """Persist the latest per-lane worker heartbeat so health checks stay truthful."""

        escaped_detail = self._escape_sql(detail[:4000])
        query = f"""
        UPDATE `{self.settings.sync_targets_table_fqn}`
        SET
          sync_status = '{self._escape_sql(status)}',
          sync_detail = '{escaped_detail}',
          last_synced_at = CURRENT_TIMESTAMP(),
          updated_at = CURRENT_TIMESTAMP()
        WHERE sync_target_id = '{lock_id}'
        """
        self.repository.execute_statement(query)

    def _lane_lock_id(self, lane: str, slot: int = 1) -> str:
        """Return the sync-target key used as a coarse lock for one lane slot."""

        return f"{self.LANE_LOCK_PREFIX}_{lane}_{slot}"

    def _max_parallel_workers_for_lane(self, lane: str) -> int:
        """Return the configured concurrency slots for one lane."""

        if lane == LANE_AI:
            return max(1, self.settings.ai_parallel_workers)
        return 1

    def _escape_sql(self, value: str) -> str:
        """Escape one string for a BigQuery string literal."""

        return (
            value.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        )
