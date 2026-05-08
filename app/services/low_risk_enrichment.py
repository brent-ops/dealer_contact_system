"""Low-risk staged enrichment for accounts and contacts."""

from __future__ import annotations

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger


logger = get_logger(__name__)


class LowRiskEnrichmentService:
    """Apply cheap, parallel-safe enrichment and readiness labels."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def run(self, dry_run: bool = False) -> None:
        """Apply low-risk enrichment tiers to canonical accounts and contacts."""

        if dry_run:
            preview = self.preview()
            logger.info(
                "Low-risk enrichment preview | activation_ready_contacts=%s | activation_ready_accounts=%s | external_seed_contacts=%s | external_seed_accounts=%s",
                preview["activation_ready_contacts"],
                preview["activation_ready_accounts"],
                preview["external_seed_contacts"],
                preview["external_seed_accounts"],
            )
            return

        self._update_accounts()
        self._update_contacts()

    def preview(self) -> dict[str, int]:
        """Return basic counts for staged enrichment readiness."""

        query = f"""
        SELECT
          (SELECT COUNT(*) FROM `{self.settings.prospect_contacts_table_fqn}` WHERE source_table = '{self.settings.external_seed_contacts_table}') AS external_seed_contacts,
          (SELECT COUNT(*) FROM `{self.settings.dealer_accounts_table_fqn}` WHERE source_table = '{self.settings.external_seed_contacts_table}') AS external_seed_accounts,
          (SELECT COUNT(*) FROM `{self.settings.prospect_contacts_table_fqn}` WHERE activation_status = 'activation_ready') AS activation_ready_contacts,
          (SELECT COUNT(*) FROM `{self.settings.dealer_accounts_table_fqn}` WHERE activation_status = 'activation_ready') AS activation_ready_accounts
        """
        row = self.repository.fetch_one(query)
        return {key: int(value or 0) for key, value in row.items()}

    def _update_accounts(self) -> None:
        """Apply low-risk readiness labels to accounts."""

        query = f"""
        UPDATE `{self.settings.dealer_accounts_table_fqn}` AS da
        SET
          enrichment_stage = CASE
            WHEN da.website_url IS NOT NULL
                 AND TRIM(da.website_url) != ''
                 AND da.account_name IS NOT NULL
                 AND TRIM(da.account_name) != ''
                 AND da.inferred_brand IS NOT NULL
                 AND TRIM(da.inferred_brand) != ''
                 AND da.account_city IS NOT NULL
                 AND TRIM(da.account_city) != ''
                 AND da.account_state IS NOT NULL
                 AND TRIM(da.account_state) != ''
              THEN 'high_confidence_enriched'
            WHEN da.website_url IS NOT NULL
                 AND TRIM(da.website_url) != ''
                 OR da.inferred_brand IS NOT NULL
                 AND TRIM(da.inferred_brand) != ''
                 OR da.dealer_classification IS NOT NULL
                 AND TRIM(da.dealer_classification) != ''
              THEN 'low_risk_enriched'
            ELSE 'seed_only'
          END,
          website_phone = COALESCE(
            NULLIF(TRIM(website_phone), ''),
            NULLIF(TRIM(account_phone), '')
          ),
          website_phone_confidence_score = CASE
            WHEN NULLIF(TRIM(website_phone), '') IS NOT NULL THEN COALESCE(website_phone_confidence_score, account_phone_confidence_score, 0.0)
            WHEN NULLIF(TRIM(account_phone), '') IS NOT NULL THEN COALESCE(account_phone_confidence_score, 0.72)
            ELSE website_phone_confidence_score
          END,
          website_phone_source_url = COALESCE(
            NULLIF(TRIM(website_phone_source_url), ''),
            NULLIF(TRIM(account_phone_source_url), '')
          ),
          best_phone = COALESCE(
            NULLIF(TRIM(best_phone), ''),
            NULLIF(TRIM(website_phone), ''),
            NULLIF(TRIM(gbp_phone), ''),
            NULLIF(TRIM(account_phone), '')
          ),
          best_phone_source = COALESCE(
            NULLIF(TRIM(best_phone_source), ''),
            CASE
              WHEN NULLIF(TRIM(website_phone), '') IS NOT NULL OR NULLIF(TRIM(account_phone), '') IS NOT NULL THEN 'website'
              WHEN NULLIF(TRIM(gbp_phone), '') IS NOT NULL THEN 'gbp'
              ELSE NULL
            END
          ),
          best_phone_confidence_score = COALESCE(
            best_phone_confidence_score,
            website_phone_confidence_score,
            gbp_phone_confidence_score,
            account_phone_confidence_score
          ),
          best_phone_source_url = COALESCE(
            NULLIF(TRIM(best_phone_source_url), ''),
            NULLIF(TRIM(website_phone_source_url), ''),
            NULLIF(TRIM(account_phone_source_url), ''),
            NULLIF(TRIM(gbp_phone_source_url), '')
          ),
          activation_status = CASE
            WHEN LOWER(COALESCE(da.account_status, 'active')) IN ('inactive', 'suppressed') THEN 'hold'
            WHEN da.dealer_classification IN ('dealer', 'dealer_group')
                 AND (
                   (da.inferred_brand IS NOT NULL AND TRIM(da.inferred_brand) != '')
                   OR (da.website_url IS NOT NULL AND TRIM(da.website_url) != '')
                 )
              THEN 'activation_ready'
            WHEN da.source_table = '{self.settings.external_seed_contacts_table}'
                 AND da.inferred_brand IS NOT NULL
                 AND TRIM(da.inferred_brand) != ''
              THEN 'activation_ready'
            ELSE 'enrichment_needed'
          END,
          updated_at = CURRENT_TIMESTAMP()
        WHERE TRUE
        """
        job = self.repository.execute_statement(query)
        logger.info(
            "Completed low-risk account enrichment | target=%s | affected_rows=%s",
            self.settings.dealer_accounts_table_fqn,
            job.num_dml_affected_rows,
        )

    def _update_contacts(self) -> None:
        """Apply low-risk readiness labels to contacts."""

        query = f"""
        UPDATE `{self.settings.prospect_contacts_table_fqn}` AS pc
        SET
          enrichment_stage = CASE
            WHEN pc.source_type = 'website_contact_extraction'
                 AND pc.role_title IS NOT NULL
                 AND TRIM(pc.role_title) != ''
              THEN 'high_confidence_enriched'
            WHEN pc.source_table = '{self.settings.external_seed_contacts_table}'
                 OR (pc.country IS NOT NULL AND TRIM(pc.country) != '')
                 OR (pc.audience_type IS NOT NULL AND TRIM(pc.audience_type) != '')
              THEN 'low_risk_enriched'
            ELSE 'seed_only'
          END,
          activation_status = CASE
            WHEN LOWER(COALESCE(pc.contact_status, 'active')) IN ('inactive', 'suppressed', 'invalid') THEN 'hold'
            WHEN pc.source_type = 'website_contact_extraction'
                 AND pc.role_title IS NOT NULL
                 AND TRIM(pc.role_title) != ''
              THEN 'activation_ready'
            WHEN pc.source_table = '{self.settings.external_seed_contacts_table}'
                 AND (
                   pc.audience_type = 'current_client'
                   OR pc.country = 'Canada'
                   OR (pc.source_file_name IS NOT NULL AND TRIM(pc.source_file_name) != '')
                 )
              THEN 'activation_ready'
            WHEN pc.email_domain IS NOT NULL
                 AND TRIM(pc.email_domain) != ''
                 AND pc.audience_type IS NOT NULL
                 AND TRIM(pc.audience_type) != ''
              THEN 'activation_ready'
            WHEN COALESCE(pc.is_personal_email, FALSE) = TRUE
                 AND (
                   pc.audience_type = 'current_client'
                   OR pc.country = 'Canada'
                   OR pc.source_table = '{self.settings.external_seed_contacts_table}'
                 )
              THEN 'activation_ready'
            ELSE 'enrichment_needed'
          END,
          updated_at = CURRENT_TIMESTAMP()
        WHERE TRUE
        """
        job = self.repository.execute_statement(query)
        logger.info(
            "Completed low-risk contact enrichment | target=%s | affected_rows=%s",
            self.settings.prospect_contacts_table_fqn,
            job.num_dml_affected_rows,
        )
