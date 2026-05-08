"""Integrate the client DIM table as suppression and ownership authority."""

from __future__ import annotations

from dataclasses import dataclass

from google.api_core.exceptions import BadRequest, Forbidden, NotFound

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class ClientDimRefreshResult:
    """Summary from one client DIM refresh attempt."""

    status: str
    detail: str
    matched_leads: int
    suppressed_leads: int


class ClientDimService:
    """Refresh client DIM matches into the prospect lead table."""

    CONNECTION_RECORD_ID = "client_dim_connection"

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def refresh(self, dry_run: bool = False) -> ClientDimRefreshResult:
        """Refresh client DIM mappings into the prospect lead table."""

        if not self.settings.client_dim_enabled:
            detail = "Client DIM integration is disabled. Prospect leads keep default suppression values."
            if not dry_run:
                self._record_status("warning", detail)
            return ClientDimRefreshResult("warning", detail, 0, 0)

        client_dim_table = self.settings.client_dim_table_fqn
        if not client_dim_table:
            detail = "Client DIM integration is enabled, but dataset/table configuration is incomplete."
            if not dry_run:
                self._record_status("failed", detail)
            return ClientDimRefreshResult("failed", detail, 0, 0)

        try:
            self.repository.fetch_one(f"SELECT COUNT(*) AS total_rows FROM `{client_dim_table}` LIMIT 1")
        except (BadRequest, Forbidden, NotFound) as error:
            detail = f"Client DIM table is not accessible yet: {error}"
            logger.warning(detail)
            if not dry_run:
                self._record_status("failed", detail)
            return ClientDimRefreshResult("failed", detail, 0, 0)

        preview_query = self._build_match_preview_query(client_dim_table)
        preview = self.repository.fetch_one(preview_query)
        matched_leads = int(preview.get("matched_leads", 0))
        suppressed_leads = int(preview.get("suppressed_leads", 0))

        if dry_run:
            detail = (
                "Would refresh client DIM matches. "
                f"Matched leads: {matched_leads:,}. Suppressed leads: {suppressed_leads:,}."
            )
            return ClientDimRefreshResult("preview", detail, matched_leads, suppressed_leads)

        reset_query = f"""
        UPDATE `{self.settings.prospect_leads_table_fqn}`
        SET
          dim_client_match_flag = FALSE,
          dim_client_id = NULL,
          dim_account_owner = NULL,
          prospecting_allowed_flag = TRUE,
          suppression_reason = NULL,
          current_client_override_flag = FALSE,
          updated_at = CURRENT_TIMESTAMP()
        WHERE TRUE
        """
        self.repository.execute_statement(reset_query)

        merge_query = self._build_match_merge_query(client_dim_table)
        self.repository.execute_statement(merge_query)
        detail = (
            f"Refreshed client DIM matches. Matched leads: {matched_leads:,}. "
            f"Suppressed leads: {suppressed_leads:,}."
        )
        self._record_status("synced", detail)
        logger.info(detail)
        return ClientDimRefreshResult("success", detail, matched_leads, suppressed_leads)

    def _build_match_preview_query(self, client_dim_table: str) -> str:
        """Return a count query for lead/DIM matches."""

        return f"""
        WITH client_dim AS (
          SELECT
            LOWER(TRIM(client_key)) AS dim_client_id,
            NULLIF(TRIM(client_name), '') AS client_name,
            NULLIF(LOWER(TRIM(normalized_client_name)), '') AS normalized_client_name,
            active_flag,
            source_aliases,
            COALESCE(
              (
                SELECT REGEXP_EXTRACT(alias, r'^assignment:account_manager:(.+)$')
                FROM UNNEST(source_aliases) AS alias
                WHERE STARTS_WITH(alias, 'assignment:account_manager:')
                LIMIT 1
              ),
              (
                SELECT REGEXP_EXTRACT(alias, r'^assignment:account_coordinator:(.+)$')
                FROM UNNEST(source_aliases) AS alias
                WHERE STARTS_WITH(alias, 'assignment:account_coordinator:')
                LIMIT 1
              )
            ) AS dim_account_owner
          FROM `{client_dim_table}`
        ),
        client_dim_match_keys AS (
          SELECT
            dim_client_id,
            dim_account_owner,
            active_flag,
            normalized_client_name AS match_key,
            1 AS match_rank
          FROM client_dim
          WHERE normalized_client_name IS NOT NULL
          UNION ALL
          SELECT
            dim_client_id,
            dim_account_owner,
            active_flag,
            REGEXP_EXTRACT(alias, r'^airtable_name:(.+)$') AS match_key,
            2 AS match_rank
          FROM client_dim, UNNEST(source_aliases) AS alias
          WHERE STARTS_WITH(alias, 'airtable_name:')
          UNION ALL
          SELECT
            dim_client_id,
            dim_account_owner,
            active_flag,
            REGEXP_EXTRACT(alias, r'^observed_name:(.+)$') AS match_key,
            3 AS match_rank
          FROM client_dim, UNNEST(source_aliases) AS alias
          WHERE STARTS_WITH(alias, 'observed_name:')
        ),
        lead_candidates AS (
          SELECT
            prospect_lead_id,
            email,
            email_domain,
            account_key,
            dealer_name,
            LOWER(
              TRIM(
                REGEXP_REPLACE(
                  COALESCE(dealer_name, ''),
                  r'[^a-zA-Z0-9]+',
                  ' '
                )
              )
            ) AS normalized_dealer_name
          FROM `{self.settings.prospect_leads_table_fqn}`
        ),
        ranked_matches AS (
          SELECT
            lc.prospect_lead_id,
            ck.active_flag,
            ROW_NUMBER() OVER (
              PARTITION BY lc.prospect_lead_id
              ORDER BY
                ck.match_rank,
                ck.dim_client_id
            ) AS row_number
          FROM lead_candidates AS lc
          JOIN client_dim_match_keys AS ck
            ON ck.match_key = lc.normalized_dealer_name
        )
        SELECT
          COUNTIF(row_number = 1) AS matched_leads,
          COUNTIF(row_number = 1 AND active_flag) AS suppressed_leads
        FROM ranked_matches
        """

    def _build_match_merge_query(self, client_dim_table: str) -> str:
        """Return the merge query for client DIM mappings."""

        return f"""
        MERGE `{self.settings.prospect_leads_table_fqn}` AS target
        USING (
          WITH client_dim AS (
            SELECT
              LOWER(TRIM(client_key)) AS dim_client_id,
              NULLIF(TRIM(client_name), '') AS client_name,
              NULLIF(LOWER(TRIM(normalized_client_name)), '') AS normalized_client_name,
              active_flag,
              source_aliases,
              COALESCE(
                (
                  SELECT REGEXP_EXTRACT(alias, r'^assignment:account_manager:(.+)$')
                  FROM UNNEST(source_aliases) AS alias
                  WHERE STARTS_WITH(alias, 'assignment:account_manager:')
                  LIMIT 1
                ),
                (
                  SELECT REGEXP_EXTRACT(alias, r'^assignment:account_coordinator:(.+)$')
                  FROM UNNEST(source_aliases) AS alias
                  WHERE STARTS_WITH(alias, 'assignment:account_coordinator:')
                  LIMIT 1
                )
              ) AS dim_account_owner
            FROM `{client_dim_table}`
          ),
          client_dim_match_keys AS (
            SELECT
              dim_client_id,
              dim_account_owner,
              active_flag,
              normalized_client_name AS match_key,
              1 AS match_rank
            FROM client_dim
            WHERE normalized_client_name IS NOT NULL
            UNION ALL
            SELECT
              dim_client_id,
              dim_account_owner,
              active_flag,
              REGEXP_EXTRACT(alias, r'^airtable_name:(.+)$') AS match_key,
              2 AS match_rank
            FROM client_dim, UNNEST(source_aliases) AS alias
            WHERE STARTS_WITH(alias, 'airtable_name:')
            UNION ALL
            SELECT
              dim_client_id,
              dim_account_owner,
              active_flag,
              REGEXP_EXTRACT(alias, r'^observed_name:(.+)$') AS match_key,
              3 AS match_rank
            FROM client_dim, UNNEST(source_aliases) AS alias
            WHERE STARTS_WITH(alias, 'observed_name:')
          ),
          lead_candidates AS (
            SELECT
              prospect_lead_id,
              dealer_name,
              LOWER(
                TRIM(
                  REGEXP_REPLACE(
                    COALESCE(dealer_name, ''),
                    r'[^a-zA-Z0-9]+',
                    ' '
                  )
                )
              ) AS normalized_dealer_name
            FROM `{self.settings.prospect_leads_table_fqn}`
          ),
          ranked_matches AS (
            SELECT
              lc.prospect_lead_id,
              ck.dim_client_id,
              ck.dim_account_owner,
              ck.active_flag,
              ROW_NUMBER() OVER (
                PARTITION BY lc.prospect_lead_id
                ORDER BY
                  ck.match_rank,
                  ck.dim_client_id
              ) AS row_number
            FROM lead_candidates AS lc
            JOIN client_dim_match_keys AS ck
              ON ck.match_key = lc.normalized_dealer_name
          )
          SELECT
            prospect_lead_id,
            dim_client_id,
            dim_account_owner,
            active_flag
          FROM ranked_matches
          WHERE row_number = 1
        ) AS source
        ON target.prospect_lead_id = source.prospect_lead_id
        WHEN MATCHED THEN
          UPDATE SET
            dim_client_match_flag = TRUE,
            dim_client_id = source.dim_client_id,
            dim_account_owner = source.dim_account_owner,
            prospecting_allowed_flag = NOT source.active_flag,
            suppression_reason = CASE
              WHEN source.active_flag THEN 'client_dim_active_client'
              ELSE NULL
            END,
            current_client_override_flag = source.active_flag,
            updated_at = CURRENT_TIMESTAMP()
        """

    def _record_status(self, sync_status: str, detail: str) -> None:
        """Upsert the latest client DIM integration state into sync_targets."""

        safe_detail = (
            detail.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        )
        query = f"""
        MERGE `{self.settings.sync_targets_table_fqn}` AS target
        USING (
          SELECT
            'client_dim' AS target_system,
            'system_connection' AS target_entity_type,
            '{self.settings.client_dim_table_fqn or "not_configured"}' AS target_entity_id,
            'system' AS source_record_type,
            '{self.CONNECTION_RECORD_ID}' AS source_record_id,
            '{sync_status}' AS sync_status,
            '{safe_detail}' AS sync_detail
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
