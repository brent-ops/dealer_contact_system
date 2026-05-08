"""Track and stage hard blocked sites for managed anti-bot escalation."""

from __future__ import annotations

from dataclasses import dataclass

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class ManagedFetchRefreshResult:
    """Summary from one managed fetch eligibility refresh."""

    status: str
    detail: str
    eligible_accounts: int


class ManagedFetchService:
    """Mark hard blocked dealer sites for later managed-service escalation."""

    CONNECTION_RECORD_ID = "managed_fetch_lane"

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def preview(self) -> dict[str, int]:
        """Return the number of accounts eligible for managed escalation."""

        query = f"""
        SELECT
          COUNT(*) AS eligible_accounts
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE dealer_classification IN ('dealer', 'dealer_group')
          AND fetch_status = 'blocked'
          AND IFNULL(blocked_attempt_count, 0) >= {self.settings.managed_fetch_min_blocked_attempts}
          AND (
            next_managed_fetch_at IS NULL
            OR next_managed_fetch_at <= CURRENT_TIMESTAMP()
          )
        """
        row = self.repository.fetch_one(query)
        return {"eligible_accounts": int(row.get("eligible_accounts", 0))}

    def refresh(self, dry_run: bool = False) -> ManagedFetchRefreshResult:
        """Mark eligible blocked accounts for managed fetch escalation."""

        preview = self.preview()
        eligible_accounts = preview["eligible_accounts"]
        if dry_run:
            detail = f"Would mark {eligible_accounts:,} hard blocked dealer sites for managed fetch escalation."
            return ManagedFetchRefreshResult("preview", detail, eligible_accounts)

        query = f"""
        UPDATE `{self.settings.dealer_accounts_table_fqn}`
        SET
          best_fetch_method = COALESCE(NULLIF(TRIM(best_fetch_method), ''), NULLIF(TRIM(fetch_method), ''), 'browser'),
          managed_fetch_status = 'eligible',
          managed_fetch_provider = '{self._escape_sql(self.settings.managed_fetch_provider)}',
          managed_fetch_notes = CASE
            WHEN {str(self.settings.managed_fetch_enabled).upper()} THEN 'Eligible for managed anti-bot fetch lane.'
            ELSE 'Managed anti-bot fetch lane is not enabled yet.'
          END,
          next_managed_fetch_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {self.settings.managed_fetch_cooldown_hours} HOUR),
          updated_at = CURRENT_TIMESTAMP()
        WHERE dealer_classification IN ('dealer', 'dealer_group')
          AND fetch_status = 'blocked'
          AND IFNULL(blocked_attempt_count, 0) >= {self.settings.managed_fetch_min_blocked_attempts}
          AND (
            next_managed_fetch_at IS NULL
            OR next_managed_fetch_at <= CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(query)

        if self.settings.managed_fetch_enabled and self.settings.managed_fetch_api_key:
            status = "configured"
            detail = (
                f"Marked {eligible_accounts:,} hard blocked dealer sites as eligible for provider "
                f"'{self.settings.managed_fetch_provider}'."
            )
        else:
            status = "warning"
            detail = (
                f"Marked {eligible_accounts:,} hard blocked dealer sites as eligible, "
                "but the managed anti-bot provider is not configured yet."
            )

        self._record_status(status)
        logger.info(detail)
        return ManagedFetchRefreshResult(status, detail, eligible_accounts)

    def _record_status(self, sync_status: str) -> None:
        """Upsert the managed fetch lane status into sync_targets."""

        query = f"""
        MERGE `{self.settings.sync_targets_table_fqn}` AS target
        USING (
          SELECT
            'managed_fetch' AS target_system,
            'blocked_site_escalation' AS target_entity_type,
            '{self._escape_sql(self.settings.managed_fetch_provider)}' AS target_entity_id,
            'system' AS source_record_type,
            '{self.CONNECTION_RECORD_ID}' AS source_record_id,
            '{sync_status}' AS sync_status
        ) AS source
        ON target.target_system = source.target_system
           AND target.source_record_type = source.source_record_type
           AND target.source_record_id = source.source_record_id
        WHEN MATCHED THEN
          UPDATE SET
            target_entity_type = source.target_entity_type,
            target_entity_id = source.target_entity_id,
            sync_status = source.sync_status,
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
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(query)

    def _escape_sql(self, value: str) -> str:
        """Escape a string value for BigQuery SQL literals."""

        return (
            value.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        )
