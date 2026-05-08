"""Refresh the materialized prospect lead table from canonical records."""

from __future__ import annotations

from dataclasses import dataclass

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class ProspectLeadRefreshResult:
    """Summary from one prospect lead table refresh."""

    status: str
    detail: str
    lead_count: int


class ProspectLeadService:
    """Build the materialized prospect lead table from canonical tables."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def preview(self) -> dict[str, int]:
        """Return basic source counts before refreshing the lead table."""

        query = f"""
        SELECT
          (SELECT COUNT(*) FROM `{self.settings.prospect_contacts_table_fqn}`) AS prospect_contacts,
          (SELECT COUNT(*) FROM `{self.settings.account_relationships_table_fqn}`) AS account_relationships,
          (SELECT COUNT(*) FROM `{self.settings.dealer_accounts_table_fqn}`) AS dealer_accounts,
          (
            SELECT COUNT(*)
            FROM `{self.settings.account_relationships_table_fqn}` AS ar
            JOIN `{self.settings.prospect_contacts_table_fqn}` AS pc
              ON pc.prospect_contact_id = ar.prospect_contact_id
            JOIN `{self.settings.dealer_accounts_table_fqn}` AS da
              ON da.dealer_account_id = ar.dealer_account_id
            WHERE pc.email IS NOT NULL
              AND TRIM(pc.email) != ''
          ) AS candidate_leads
        """
        row = self.repository.fetch_one(query)
        return {key: int(value or 0) for key, value in row.items()}

    def refresh(self, dry_run: bool = False) -> ProspectLeadRefreshResult:
        """Refresh the prospect lead table from canonical data."""

        preview = self.preview()
        if dry_run:
            detail = (
                "Would refresh the prospect lead table from canonical records. "
                f"Candidate lead rows: {preview['candidate_leads']:,}."
            )
            logger.info("Prospect lead refresh preview | %s", detail)
            return ProspectLeadRefreshResult(
                status="preview",
                detail=detail,
                lead_count=preview["candidate_leads"],
            )

        query = f"""
        CREATE OR REPLACE TABLE `{self.settings.prospect_leads_table_fqn}` AS
        WITH base_rows AS (
          SELECT
            TO_HEX(SHA256(CONCAT(ar.dealer_account_id, '|', pc.prospect_contact_id))) AS prospect_lead_id,
            ar.relationship_id,
            pc.prospect_contact_id,
            da.dealer_account_id,
            COALESCE(
              NULLIF(TRIM(pc.full_name), ''),
              NULLIF(TRIM(CONCAT(COALESCE(pc.first_name, ''), ' ', COALESCE(pc.last_name, ''))), '')
            ) AS full_name,
            NULLIF(TRIM(pc.first_name), '') AS first_name,
            NULLIF(TRIM(pc.last_name), '') AS last_name,
            LOWER(TRIM(pc.email)) AS email,
            LOWER(TRIM(pc.email_domain)) AS email_domain,
            COALESCE(
              NULLIF(TRIM(pc.phone_number), ''),
              NULLIF(TRIM(da.best_phone), ''),
              NULLIF(TRIM(da.account_phone), '')
            ) AS phone_number,
            COALESCE(NULLIF(TRIM(da.account_name), ''), da.account_key) AS dealer_name,
            da.account_key,
            COALESCE(NULLIF(TRIM(da.website_url), ''), NULLIF(TRIM(da.ai_website_url), '')) AS website_url,
            COALESCE(NULLIF(TRIM(da.account_city), ''), NULLIF(TRIM(da.ai_city), ''), NULLIF(TRIM(da.gbp_city), '')) AS city,
            COALESCE(NULLIF(TRIM(da.account_state), ''), NULLIF(TRIM(da.ai_state_or_province), ''), NULLIF(TRIM(da.gbp_state_or_province), '')) AS state,
            COALESCE(NULLIF(TRIM(da.ai_address_line), ''), NULLIF(TRIM(da.gbp_address_line), '')) AS address_line,
            COALESCE(NULLIF(TRIM(da.ai_postal_code), ''), NULLIF(TRIM(da.gbp_postal_code), '')) AS postal_code,
            COALESCE(NULLIF(TRIM(pc.country), ''), NULLIF(TRIM(da.ai_country), ''), NULLIF(TRIM(da.gbp_country), ''), 'United States') AS country,
            COALESCE(
              NULLIF(TRIM(pc.market), ''),
              CASE
                WHEN COALESCE(NULLIF(TRIM(pc.country), ''), NULLIF(TRIM(da.ai_country), ''), NULLIF(TRIM(da.gbp_country), ''), 'United States') = 'Canada' THEN 'Canada'
                ELSE 'US'
              END
            ) AS market,
            COALESCE(NULLIF(TRIM(da.inferred_brand), ''), 'Unknown') AS oem,
            COALESCE(NULLIF(TRIM(da.dealer_classification), ''), 'unclassified') AS dealer_classification,
            COALESCE(NULLIF(TRIM(pc.role_family), ''), 'unclassified') AS role_family,
            NULLIF(TRIM(pc.role_title), '') AS role_title,
            COALESCE(NULLIF(TRIM(pc.email_quality), ''), 'unknown') AS email_quality,
            COALESCE(NULLIF(TRIM(pc.email_source_type), ''), NULLIF(TRIM(pc.source_type), ''), 'unknown') AS email_source_type,
            COALESCE(NULLIF(TRIM(pc.email_source_url), ''), NULLIF(TRIM(pc.source_url), '')) AS email_source_url,
            COALESCE(pc.email_confidence_score, pc.confidence_score, 0.0) AS email_confidence_score,
            (
              REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(pc.email_domain), ''), '')), r'(law|legal|attorney|attorneys|counsel|esq|esquire)\\.')
              OR REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(da.account_name), ''), '')), r'\b(law|legal|attorney|attorneys|counsel|esq|esquire)\b')
              OR REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(pc.role_title), ''), '')), r'\b(attorney|attorneys|counsel|esq|esquire|lawyer)\b')
            ) AS legal_contact_flag,
            CASE
              WHEN LOWER(COALESCE(pc.contact_status, 'active')) IN ('inactive', 'suppressed', 'invalid') THEN 'hold'
              WHEN LOWER(COALESCE(da.account_status, 'active')) IN ('inactive', 'suppressed') THEN 'hold'
              WHEN COALESCE(pc.activation_status, da.activation_status, 'enrichment_needed') = 'activation_ready' THEN 'activation_ready'
              ELSE 'enrichment_needed'
            END AS activation_status,
            COALESCE(NULLIF(TRIM(pc.audience_type), ''), 'prospect') AS audience_type,
            COALESCE(NULLIF(TRIM(pc.source_file_name), ''), pc.source_table, 'contact_master') AS source_list,
            COALESCE(NULLIF(TRIM(da.website_phone), ''), NULLIF(TRIM(da.account_phone), '')) AS website_phone,
            NULLIF(TRIM(da.gbp_phone), '') AS gbp_phone,
            NULLIF(TRIM(da.ai_phone), '') AS ai_phone,
            NULLIF(TRIM(da.ai_address_line), '') AS ai_address_line,
            NULLIF(TRIM(da.ai_city), '') AS ai_city,
            NULLIF(TRIM(da.ai_state_or_province), '') AS ai_state_or_province,
            NULLIF(TRIM(da.ai_postal_code), '') AS ai_postal_code,
            NULLIF(TRIM(da.ai_country), '') AS ai_country,
            NULLIF(TRIM(da.ai_source_provider), '') AS ai_source_provider,
            NULLIF(TRIM(da.ai_source_url), '') AS ai_source_url,
            COALESCE(NULLIF(TRIM(da.best_phone), ''), NULLIF(TRIM(da.website_phone), ''), NULLIF(TRIM(da.ai_phone), ''), NULLIF(TRIM(da.gbp_phone), ''), NULLIF(TRIM(da.account_phone), '')) AS best_phone,
            COALESCE(NULLIF(TRIM(da.best_phone_source), ''), CASE
              WHEN NULLIF(TRIM(da.website_phone), '') IS NOT NULL OR NULLIF(TRIM(da.account_phone), '') IS NOT NULL THEN 'website'
              WHEN NULLIF(TRIM(da.ai_phone), '') IS NOT NULL THEN 'ai'
              WHEN NULLIF(TRIM(da.gbp_phone), '') IS NOT NULL THEN 'gbp'
              ELSE NULL
            END) AS best_phone_source,
            CASE
              WHEN NULLIF(TRIM(da.account_city), '') IS NOT NULL OR NULLIF(TRIM(da.account_state), '') IS NOT NULL THEN 'website'
              WHEN NULLIF(TRIM(da.ai_address_line), '') IS NOT NULL OR NULLIF(TRIM(da.ai_city), '') IS NOT NULL THEN 'ai'
              WHEN NULLIF(TRIM(da.gbp_address_line), '') IS NOT NULL OR NULLIF(TRIM(da.gbp_city), '') IS NOT NULL THEN 'gbp'
              ELSE NULL
            END AS best_location_source,
            COALESCE(pc.confidence_score, 0.0) AS contact_confidence_score,
            COALESCE(da.confidence_score, 0.0) AS account_confidence_score,
            FALSE AS dim_client_match_flag,
            CAST(NULL AS STRING) AS dim_client_id,
            CAST(NULL AS STRING) AS dim_account_owner,
            NOT (
              REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(pc.email_domain), ''), '')), r'(law|legal|attorney|attorneys|counsel|esq|esquire)\\.')
              OR REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(da.account_name), ''), '')), r'\b(law|legal|attorney|attorneys|counsel|esq|esquire)\b')
              OR REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(pc.role_title), ''), '')), r'\b(attorney|attorneys|counsel|esq|esquire|lawyer)\b')
            ) AS prospecting_allowed_flag,
            CASE
              WHEN (
                REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(pc.email_domain), ''), '')), r'(law|legal|attorney|attorneys|counsel|esq|esquire)\\.')
                OR REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(da.account_name), ''), '')), r'\b(law|legal|attorney|attorneys|counsel|esq|esquire)\b')
                OR REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(pc.role_title), ''), '')), r'\b(attorney|attorneys|counsel|esq|esquire|lawyer)\b')
              ) THEN 'legal_contact'
              ELSE NULL
            END AS suppression_reason,
            FALSE AS current_client_override_flag,
            LEAST(
              COALESCE(pc.first_seen_at, CURRENT_TIMESTAMP()),
              COALESCE(da.first_seen_at, CURRENT_TIMESTAMP()),
              COALESCE(ar.first_seen_at, CURRENT_TIMESTAMP())
            ) AS first_seen_at,
            GREATEST(
              COALESCE(pc.last_seen_at, pc.updated_at, CURRENT_TIMESTAMP()),
              COALESCE(da.last_seen_at, da.updated_at, CURRENT_TIMESTAMP()),
              COALESCE(ar.last_seen_at, ar.updated_at, CURRENT_TIMESTAMP())
            ) AS last_seen_at,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
          FROM `{self.settings.account_relationships_table_fqn}` AS ar
          JOIN `{self.settings.prospect_contacts_table_fqn}` AS pc
            ON pc.prospect_contact_id = ar.prospect_contact_id
          JOIN `{self.settings.dealer_accounts_table_fqn}` AS da
            ON da.dealer_account_id = ar.dealer_account_id
          WHERE pc.email IS NOT NULL
            AND TRIM(pc.email) != ''
        )
        SELECT
          prospect_lead_id,
          relationship_id,
          prospect_contact_id,
          dealer_account_id,
          full_name,
          first_name,
          last_name,
          email,
          email_domain,
          phone_number,
          dealer_name,
          account_key,
          website_url,
          city,
          state,
          address_line,
          postal_code,
          country,
          market,
          oem,
          dealer_classification,
          role_family,
          role_title,
          email_quality,
          email_source_type,
          email_source_url,
          email_confidence_score,
          legal_contact_flag,
          activation_status,
          (
            activation_status = 'activation_ready'
            AND NOT legal_contact_flag
            AND COALESCE(TRIM(full_name), '') != ''
            AND COALESCE(TRIM(dealer_name), '') != ''
            AND COALESCE(TRIM(best_phone), '') != ''
          ) AS marketing_ready_flag,
          (
            activation_status = 'activation_ready'
            AND NOT legal_contact_flag
            AND COALESCE(TRIM(full_name), '') != ''
            AND COALESCE(TRIM(dealer_name), '') != ''
            AND COALESCE(TRIM(best_phone), '') != ''
            AND audience_type != 'current_client'
            AND dealer_classification IN ('dealer', 'dealer_group')
          ) AS sales_ready_flag,
          audience_type,
          source_list,
          audience_type = 'current_client' AS is_current_client,
          country = 'Canada' AS is_canada,
          website_phone,
          gbp_phone,
          ai_phone,
          best_location_source,
          ai_address_line,
          ai_city,
          ai_state_or_province,
          ai_postal_code,
          ai_country,
          ai_source_provider,
          ai_source_url,
          best_phone,
          best_phone_source,
          contact_confidence_score,
          account_confidence_score,
          dim_client_match_flag,
          dim_client_id,
          dim_account_owner,
          prospecting_allowed_flag,
          suppression_reason,
          current_client_override_flag,
          first_seen_at,
          last_seen_at,
          created_at,
          updated_at
        FROM base_rows
        """
        self.repository.execute_statement(query)
        count_query = f"SELECT COUNT(*) AS total_leads FROM `{self.settings.prospect_leads_table_fqn}`"
        lead_count = int(self.repository.fetch_one(count_query).get("total_leads", 0))
        detail = f"Refreshed prospect lead table with {lead_count:,} rows."
        logger.info(detail)
        return ProspectLeadRefreshResult(
            status="success",
            detail=detail,
            lead_count=lead_count,
        )
