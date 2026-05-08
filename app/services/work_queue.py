"""Queue-backed worker orchestration for brute-force pipeline runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import uuid

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger
from app.services.account_enrichment import AccountEnrichmentService
from app.services.ai_retrieval import AiRetrievalService
from app.services.browser_retry import BrowserRetryService
from app.services.contact_extraction import ContactExtractionService
from app.services.dealer_validation import DealerValidationService
from app.services.gbp_enrichment import GbpEnrichmentService


logger = get_logger(__name__)

TASK_VALIDATE = "validate"
TASK_ENRICH = "enrich"
TASK_ENRICH_GBP = "enrich_gbp"
TASK_AI_ACCOUNT_FACTS = "ai_account_facts"
TASK_EXTRACT_CONTACTS = "extract_contacts"
TASK_RETRY_BLOCKED = "retry_blocked"
TASK_TYPES = [
    TASK_VALIDATE,
    TASK_ENRICH,
    TASK_ENRICH_GBP,
    TASK_AI_ACCOUNT_FACTS,
    TASK_EXTRACT_CONTACTS,
    TASK_RETRY_BLOCKED,
]


@dataclass(frozen=True)
class WorkQueuePreview:
    """Summary of queued work for one task type."""

    task_type: str
    pending_count: int
    in_progress_count: int
    completed_count: int
    failed_count: int


class PipelineRunLogger:
    """Track one worker run in BigQuery for later monitoring."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def start_run(self, task_type: str, worker_id: str, requested_batch_size: int) -> str:
        """Create a new run-log row and return its ID."""

        pipeline_run_id = str(uuid.uuid4())
        query = f"""
        INSERT INTO `{self.settings.pipeline_runs_table_fqn}` (
          pipeline_run_id,
          task_type,
          run_status,
          worker_id,
          requested_batch_size,
          claimed_count,
          succeeded_count,
          failed_count,
          run_notes,
          started_at,
          completed_at,
          created_at,
          updated_at
        )
        VALUES (
          '{pipeline_run_id}',
          '{task_type}',
          'running',
          '{worker_id}',
          {requested_batch_size},
          0,
          0,
          0,
          NULL,
          CURRENT_TIMESTAMP(),
          NULL,
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP()
        )
        """
        self.repository.execute_statement(query)
        return pipeline_run_id

    def finish_run(
        self,
        pipeline_run_id: str,
        run_status: str,
        claimed_count: int,
        succeeded_count: int,
        failed_count: int,
        run_notes: str | None = None,
    ) -> None:
        """Update the final run status and counts."""

        notes_sql = "NULL"
        if run_notes:
            escaped_notes = (
                run_notes.replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
            )
            notes_sql = f"'{escaped_notes}'"
        query = f"""
        UPDATE `{self.settings.pipeline_runs_table_fqn}`
        SET
          run_status = '{run_status}',
          claimed_count = {claimed_count},
          succeeded_count = {succeeded_count},
          failed_count = {failed_count},
          run_notes = {notes_sql},
          completed_at = CURRENT_TIMESTAMP(),
          updated_at = CURRENT_TIMESTAMP()
        WHERE pipeline_run_id = '{pipeline_run_id}'
        """
        self.repository.execute_statement(query)


class WorkQueueService:
    """Seed, claim, and process queue-backed account work."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.run_logger = PipelineRunLogger(repository, settings)
        self.dealer_validation = DealerValidationService(repository, settings)
        self.account_enrichment = AccountEnrichmentService(repository, settings)
        self.gbp_enrichment = GbpEnrichmentService(repository, settings)
        self.ai_retrieval = AiRetrievalService(repository, settings)
        self.contact_extraction = ContactExtractionService(repository, settings)
        self.browser_retry = BrowserRetryService(repository, settings)

    def preview(self, task_type: str) -> WorkQueuePreview:
        """Return queue counts for one task type."""

        query = f"""
        SELECT
          COUNTIF(status IN ('pending', 'retry')) AS pending_count,
          COUNTIF(status = 'in_progress') AS in_progress_count,
          COUNTIF(status = 'completed') AS completed_count,
          COUNTIF(status = 'failed') AS failed_count
        FROM `{self.settings.account_work_queue_table_fqn}`
        WHERE task_type = '{task_type}'
        """
        row = self.repository.fetch_one(query)
        return WorkQueuePreview(
            task_type=task_type,
            pending_count=int(row.get("pending_count", 0)),
            in_progress_count=int(row.get("in_progress_count", 0)),
            completed_count=int(row.get("completed_count", 0)),
            failed_count=int(row.get("failed_count", 0)),
        )

    def seed(self, task_type: str) -> None:
        """Insert any missing queue rows for the requested task type."""

        if task_type == "all":
            for item in TASK_TYPES:
                self.seed(item)
            return

        if task_type == TASK_RETRY_BLOCKED:
            self._seed_retry_blocked_queue()
            logger.info("Seeded work queue | task_type=%s", task_type)
            return
        if task_type == TASK_AI_ACCOUNT_FACTS and (
            not self.settings.ai_retrieval_enabled or not self.ai_retrieval._configured_providers()
        ):
            logger.info("Skipped AI queue seed because AI retrieval is not configured.")
            return

        source_query = self._seed_source_query(task_type)
        merge_query = f"""
        MERGE `{self.settings.account_work_queue_table_fqn}` AS target
        USING (
          {source_query}
        ) AS source
        ON target.task_type = source.task_type
           AND target.account_key = source.account_key
        WHEN NOT MATCHED THEN
          INSERT (
            work_item_id,
            task_type,
            account_key,
            status,
            priority,
            attempt_count,
            lease_owner,
            lease_expires_at,
            last_attempt_at,
            next_attempt_at,
            completed_at,
            last_error,
            created_at,
            updated_at
          )
          VALUES (
            GENERATE_UUID(),
            source.task_type,
            source.account_key,
            'pending',
            source.priority,
            0,
            NULL,
            NULL,
            NULL,
            CURRENT_TIMESTAMP(),
            NULL,
            NULL,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(merge_query)
        logger.info("Seeded work queue | task_type=%s", task_type)

    def _seed_retry_blocked_queue(self) -> None:
        """Seed or reopen blocked-site retry work items only when cooldown has expired."""

        source_query = f"""
        SELECT
          '{TASK_RETRY_BLOCKED}' AS task_type,
          account_key,
          40 AS priority
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE dealer_classification IN ('dealer', 'dealer_group')
          AND fetch_status = 'blocked'
          AND {self._blocked_retry_due_sql('')}
        """
        merge_query = f"""
        MERGE `{self.settings.account_work_queue_table_fqn}` AS target
        USING (
          {source_query}
        ) AS source
        ON target.task_type = source.task_type
           AND target.account_key = source.account_key
        WHEN MATCHED AND target.status IN ('completed', 'failed') THEN
          UPDATE SET
            status = 'pending',
            priority = source.priority,
            lease_owner = NULL,
            lease_expires_at = NULL,
            next_attempt_at = CURRENT_TIMESTAMP(),
            completed_at = NULL,
            updated_at = CURRENT_TIMESTAMP()
        WHEN MATCHED AND target.status = 'retry' THEN
          UPDATE SET
            priority = source.priority,
            next_attempt_at = CURRENT_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (
            work_item_id,
            task_type,
            account_key,
            status,
            priority,
            attempt_count,
            lease_owner,
            lease_expires_at,
            last_attempt_at,
            next_attempt_at,
            completed_at,
            last_error,
            created_at,
            updated_at
          )
          VALUES (
            GENERATE_UUID(),
            source.task_type,
            source.account_key,
            'pending',
            source.priority,
            0,
            NULL,
            NULL,
            NULL,
            CURRENT_TIMESTAMP(),
            NULL,
            NULL,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(merge_query)

    def run_worker(
        self,
        task_type: str,
        batch_size: int | None = None,
        dry_run: bool = False,
    ) -> None:
        """Claim a batch of queued work and process it with the right service."""

        requested_batch_size = batch_size or self.settings.worker_batch_size
        preview = self.preview(task_type)
        logger.info(
            "Worker preview | task_type=%s | pending=%s | in_progress=%s | completed=%s | failed=%s | dry_run=%s",
            preview.task_type,
            preview.pending_count,
            preview.in_progress_count,
            preview.completed_count,
            preview.failed_count,
            dry_run,
        )
        if dry_run:
            return

        if not self._should_run_task_now(task_type):
            logger.info("Skipped worker run because task is cooling down | task_type=%s", task_type)
            return

        worker_id = str(uuid.uuid4())
        pipeline_run_id = self.run_logger.start_run(
            task_type=task_type,
            worker_id=worker_id,
            requested_batch_size=requested_batch_size,
        )
        claimed_items = self._claim_batch(
            task_type=task_type,
            batch_size=requested_batch_size,
            worker_id=worker_id,
        )
        claimed_account_keys = [item["account_key"] for item in claimed_items]
        if not claimed_account_keys:
            self.run_logger.finish_run(
                pipeline_run_id=pipeline_run_id,
                run_status="completed",
                claimed_count=0,
                succeeded_count=0,
                failed_count=0,
                run_notes="No queue items were available to claim.",
            )
            logger.info("No queued work was available | task_type=%s", task_type)
            return

        logger.info(
            "Claimed work queue batch | task_type=%s | worker_id=%s | items=%s",
            task_type,
            worker_id,
            len(claimed_account_keys),
        )

        try:
            self._dispatch_task(task_type=task_type, account_keys=claimed_account_keys)
        except Exception as exc:
            self._mark_failed(
                task_type=task_type,
                account_keys=claimed_account_keys,
                error_message=str(exc),
            )
            self.run_logger.finish_run(
                pipeline_run_id=pipeline_run_id,
                run_status="failed",
                claimed_count=len(claimed_account_keys),
                succeeded_count=0,
                failed_count=len(claimed_account_keys),
                run_notes=str(exc),
            )
            raise

        self._mark_completed(task_type=task_type, account_keys=claimed_account_keys)
        self.run_logger.finish_run(
            pipeline_run_id=pipeline_run_id,
            run_status="completed",
            claimed_count=len(claimed_account_keys),
            succeeded_count=len(claimed_account_keys),
            failed_count=0,
        )

    def _should_run_task_now(self, task_type: str) -> bool:
        """Return True when the task should run on this cycle."""

        if task_type != TASK_AI_ACCOUNT_FACTS:
            return True

        minimum_interval = max(self.settings.ai_retrieval_min_run_interval_minutes, 0)
        if minimum_interval <= 0:
            return True

        query = f"""
        SELECT
          MAX(started_at) AS last_started_at
        FROM `{self.settings.pipeline_runs_table_fqn}`
        WHERE task_type = '{TASK_AI_ACCOUNT_FACTS}'
          AND run_status IN ('running', 'completed')
        """
        row = self.repository.fetch_one(query)
        last_started_at = row.get("last_started_at")
        if not last_started_at:
            return True
        if isinstance(last_started_at, str):
            try:
                last_started_at = datetime.fromisoformat(last_started_at.replace("Z", "+00:00"))
            except ValueError:
                return True
        if not isinstance(last_started_at, datetime):
            return True
        if last_started_at.tzinfo is None:
            last_started_at = last_started_at.replace(tzinfo=timezone.utc)
        elapsed_seconds = (datetime.now(timezone.utc) - last_started_at.astimezone(timezone.utc)).total_seconds()
        return elapsed_seconds >= (minimum_interval * 60)

    def _seed_source_query(self, task_type: str) -> str:
        """Return the source query used to seed queue rows."""

        if task_type == TASK_VALIDATE:
            return f"""
            SELECT
              '{TASK_VALIDATE}' AS task_type,
              account_key,
              100 AS priority
            FROM `{self.settings.dealer_accounts_table_fqn}`
            WHERE IFNULL(is_personal_domain, FALSE) = FALSE
            """
        if task_type == TASK_ENRICH:
            return f"""
            SELECT
              '{TASK_ENRICH}' AS task_type,
              account_key,
              80 AS priority
            FROM `{self.settings.dealer_accounts_table_fqn}`
            WHERE dealer_classification IN ('dealer', 'dealer_group')
              AND (
                website_url IS NULL
                OR TRIM(website_url) = ''
                OR account_name IS NULL
                OR TRIM(account_name) = ''
                OR inferred_brand IS NULL
                OR TRIM(inferred_brand) = ''
                OR account_city IS NULL
                OR account_state IS NULL
              )
            """
        if task_type == TASK_ENRICH_GBP:
            return f"""
            SELECT
              '{TASK_ENRICH_GBP}' AS task_type,
              account_key,
              70 AS priority
            FROM `{self.settings.dealer_accounts_table_fqn}`
            WHERE dealer_classification IN ('dealer', 'dealer_group')
              AND (
                best_phone IS NULL
                OR TRIM(best_phone) = ''
                OR gbp_address_line IS NULL
                OR TRIM(gbp_address_line) = ''
                OR account_city IS NULL
                OR account_state IS NULL
                OR fetch_status = 'blocked'
              )
            """
        if task_type == TASK_AI_ACCOUNT_FACTS:
            return f"""
            SELECT
              '{TASK_AI_ACCOUNT_FACTS}' AS task_type,
              account_key,
              65 AS priority
            FROM `{self.settings.dealer_accounts_table_fqn}`
            WHERE dealer_classification IN ('dealer', 'dealer_group')
              AND (
                fetch_status = 'blocked'
                OR managed_fetch_status = 'eligible'
                OR website_url IS NULL
                OR TRIM(website_url) = ''
                OR best_phone IS NULL
                OR TRIM(best_phone) = ''
                OR account_city IS NULL
                OR account_state IS NULL
              )
              AND (
                next_ai_retrieval_at IS NULL
                OR next_ai_retrieval_at <= CURRENT_TIMESTAMP()
              )
            """
        if task_type == TASK_EXTRACT_CONTACTS:
            return f"""
            SELECT
              '{TASK_EXTRACT_CONTACTS}' AS task_type,
              account_key,
              60 AS priority
            FROM `{self.settings.dealer_accounts_table_fqn}`
            WHERE dealer_classification IN ('dealer', 'dealer_group')
              AND website_url IS NOT NULL
              AND TRIM(website_url) != ''
            """
        if task_type == TASK_RETRY_BLOCKED:
            return f"""
            SELECT
              '{TASK_RETRY_BLOCKED}' AS task_type,
              account_key,
              40 AS priority
            FROM `{self.settings.dealer_accounts_table_fqn}`
            WHERE dealer_classification IN ('dealer', 'dealer_group')
              AND fetch_status = 'blocked'
              AND {self._blocked_retry_due_sql('')}
            """
        raise ValueError(f"Unsupported task type: {task_type}")

    def _blocked_retry_due_sql(self, alias: str) -> str:
        """Return SQL that enforces progressively slower blocked-site retries."""

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

    def _claim_batch(
        self,
        task_type: str,
        batch_size: int,
        worker_id: str,
    ) -> list[dict[str, str]]:
        """Lease a batch of queue rows to this worker."""

        reclaim_query = f"""
        UPDATE `{self.settings.account_work_queue_table_fqn}`
        SET
          status = 'retry',
          lease_owner = NULL,
          lease_expires_at = NULL,
          next_attempt_at = CURRENT_TIMESTAMP(),
          last_error = COALESCE(last_error, 'Worker lease expired before completion.'),
          updated_at = CURRENT_TIMESTAMP()
        WHERE task_type = '{task_type}'
          AND status = 'in_progress'
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at <= CURRENT_TIMESTAMP()
        """
        self.repository.execute_statement(reclaim_query)

        task_specific_eligibility_sql = self._claim_eligibility_sql(task_type)
        update_query = f"""
        UPDATE `{self.settings.account_work_queue_table_fqn}`
        SET
          status = 'in_progress',
          lease_owner = '{worker_id}',
          lease_expires_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {self.settings.worker_lease_minutes} MINUTE),
          last_attempt_at = CURRENT_TIMESTAMP(),
          attempt_count = IFNULL(attempt_count, 0) + 1,
          updated_at = CURRENT_TIMESTAMP()
        WHERE work_item_id IN (
          SELECT work_item_id
          FROM `{self.settings.account_work_queue_table_fqn}`
          WHERE task_type = '{task_type}'
            AND status IN ('pending', 'retry')
            AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP())
            AND (lease_expires_at IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP())
            {task_specific_eligibility_sql}
          ORDER BY priority DESC, created_at ASC
          LIMIT {batch_size}
        )
        """
        self.repository.execute_statement(update_query)

        fetch_query = f"""
        SELECT work_item_id, account_key
        FROM `{self.settings.account_work_queue_table_fqn}`
        WHERE task_type = '{task_type}'
          AND status = 'in_progress'
          AND lease_owner = '{worker_id}'
        ORDER BY priority DESC, created_at ASC
        """
        return [dict(row.items()) for row in self.repository.run_query(fetch_query)]

    def _claim_eligibility_sql(self, task_type: str) -> str:
        """Return extra claim-time eligibility filters for special queue types."""

        if task_type != TASK_AI_ACCOUNT_FACTS:
            return ""

        return f"""
            AND account_key IN (
              SELECT account_key
              FROM `{self.settings.dealer_accounts_table_fqn}`
              WHERE dealer_classification IN ('dealer', 'dealer_group')
                AND (
                  next_ai_retrieval_at IS NULL
                  OR next_ai_retrieval_at <= CURRENT_TIMESTAMP()
                )
            )
        """

    def _mark_completed(self, task_type: str, account_keys: list[str]) -> None:
        """Mark queue rows complete after a successful worker batch."""

        account_keys_sql = ", ".join(f"'{self._escape_sql(value)}'" for value in account_keys)
        query = f"""
        UPDATE `{self.settings.account_work_queue_table_fqn}`
        SET
          status = 'completed',
          lease_owner = NULL,
          lease_expires_at = NULL,
          completed_at = CURRENT_TIMESTAMP(),
          last_error = NULL,
          updated_at = CURRENT_TIMESTAMP()
        WHERE task_type = '{task_type}'
          AND account_key IN ({account_keys_sql})
        """
        self.repository.execute_statement(query)

    def _mark_failed(self, task_type: str, account_keys: list[str], error_message: str) -> None:
        """Requeue failed worker items with an error message."""

        account_keys_sql = ", ".join(f"'{self._escape_sql(value)}'" for value in account_keys)
        error_sql = self._escape_sql(error_message)
        query = f"""
        UPDATE `{self.settings.account_work_queue_table_fqn}`
        SET
          status = 'retry',
          lease_owner = NULL,
          lease_expires_at = NULL,
          next_attempt_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE),
          last_error = '{error_sql}',
          updated_at = CURRENT_TIMESTAMP()
        WHERE task_type = '{task_type}'
          AND account_key IN ({account_keys_sql})
        """
        self.repository.execute_statement(query)

    def _dispatch_task(self, task_type: str, account_keys: list[str]) -> None:
        """Run the existing pipeline service for this queue batch."""

        limit = len(account_keys)
        if task_type == TASK_VALIDATE:
            self.dealer_validation.validate(
                dry_run=False,
                limit=limit,
                account_keys=account_keys,
            )
            return
        if task_type == TASK_ENRICH:
            self.account_enrichment.enrich(
                dry_run=False,
                limit=limit,
                account_keys=account_keys,
            )
            return
        if task_type == TASK_ENRICH_GBP:
            self.gbp_enrichment.enrich(
                dry_run=False,
                limit=limit,
                account_keys=account_keys,
            )
            return
        if task_type == TASK_AI_ACCOUNT_FACTS:
            result = self.ai_retrieval.refresh_account_facts(
                dry_run=False,
                limit=limit,
                account_keys=account_keys,
            )
            if result.status in {"warning", "failed"}:
                raise RuntimeError(result.detail)
            return
        if task_type == TASK_EXTRACT_CONTACTS:
            self.contact_extraction.extract(
                dry_run=False,
                limit=limit,
                account_keys=account_keys,
            )
            return
        if task_type == TASK_RETRY_BLOCKED:
            self.browser_retry.retry(
                dry_run=False,
                limit=limit,
                account_keys=account_keys,
            )
            return
        raise ValueError(f"Unsupported task type: {task_type}")

    def _escape_sql(self, value: str) -> str:
        """Escape one string for a BigQuery string literal."""

        return (
            value.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        )
