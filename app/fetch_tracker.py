"""Track fetch outcomes for website enrichment and extraction."""

from __future__ import annotations

from dataclasses import dataclass
import time

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class FetchTrackingUpdate:
    """Represents one fetch attempt outcome for a dealer account."""

    dealer_account_id: str
    fetch_status: str
    fetch_method: str
    blocked_reason: str | None = None


class FetchStatusTracker:
    """Write fetch attempt outcomes back into dealer_accounts."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def record(self, update: FetchTrackingUpdate) -> None:
        """Persist the latest fetch outcome and retry metadata."""

        blocked_reason_sql = "NULL"
        if update.blocked_reason:
            escaped_reason = update.blocked_reason.replace("'", "''")
            blocked_reason_sql = f"'{escaped_reason}'"
        query = f"""
        UPDATE `{self.settings.dealer_accounts_table_fqn}`
        SET
          fetch_status = '{update.fetch_status}',
          fetch_method = '{update.fetch_method}',
          blocked_reason = CASE
            WHEN '{update.fetch_status}' = 'blocked' THEN {blocked_reason_sql}
            WHEN '{update.fetch_status}' = 'success' THEN NULL
            ELSE blocked_reason
          END,
          blocked_attempt_count = CASE
            WHEN '{update.fetch_status}' = 'blocked' THEN IFNULL(blocked_attempt_count, 0) + 1
            ELSE IFNULL(blocked_attempt_count, 0)
          END,
          last_fetch_attempt_at = CURRENT_TIMESTAMP(),
          last_fetch_success_at = CASE
            WHEN '{update.fetch_status}' = 'success' THEN CURRENT_TIMESTAMP()
            ELSE last_fetch_success_at
          END,
          updated_at = CURRENT_TIMESTAMP()
        WHERE dealer_account_id = '{update.dealer_account_id}'
        """
        self._execute_with_retry(query, update)

    def _execute_with_retry(self, query: str, update: FetchTrackingUpdate) -> None:
        """Retry transient dealer-account write conflicts for fetch tracking."""

        max_attempts = 5
        delay_seconds = 1.0
        for attempt in range(1, max_attempts + 1):
            try:
                self.repository.execute_statement(query)
                return
            except Exception as exc:
                message = str(exc)
                is_retryable = "Could not serialize access to table" in message
                if not is_retryable or attempt >= max_attempts:
                    raise
                logger.warning(
                    "Retrying fetch status write after transient serialization conflict | dealer_account_id=%s | fetch_status=%s | attempt=%s | error=%s",
                    update.dealer_account_id,
                    update.fetch_status,
                    attempt,
                    message,
                )
                time.sleep(delay_seconds)
                delay_seconds *= 2
