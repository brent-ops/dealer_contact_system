"""Promote one-time external seed rows into canonical account/contact tables."""

from __future__ import annotations

from dataclasses import dataclass

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class ExternalSeedPromotionPreview:
    """Read-only summary of what will be promoted from raw external seeds."""

    raw_rows: int
    distinct_emails: int
    distinct_accounts: int
    current_clients: int
    canada_rows: int
    personal_email_rows: int


class ExternalSeedPromotionService:
    """Promote raw external seed contacts into canonical BigQuery tables."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def preview(self) -> ExternalSeedPromotionPreview:
        """Return a read-only summary of the raw external seed table."""

        query = f"""
        SELECT
          COUNT(*) AS raw_rows,
          COUNT(DISTINCT normalized_email) AS distinct_emails,
          COUNT(DISTINCT account_key) AS distinct_accounts,
          COUNTIF(audience_type = 'current_client') AS current_clients,
          COUNTIF(country = 'Canada') AS canada_rows,
          COUNTIF(is_personal_email) AS personal_email_rows
        FROM `{self.settings.external_seed_contacts_table_fqn}`
        """
        row = self.repository.fetch_one(query)
        return ExternalSeedPromotionPreview(
            raw_rows=int(row.get("raw_rows", 0)),
            distinct_emails=int(row.get("distinct_emails", 0)),
            distinct_accounts=int(row.get("distinct_accounts", 0)),
            current_clients=int(row.get("current_clients", 0)),
            canada_rows=int(row.get("canada_rows", 0)),
            personal_email_rows=int(row.get("personal_email_rows", 0)),
        )

    def promote(self, dry_run: bool = False) -> None:
        """Promote the raw external seed table into canonical tables."""

        preview = self.preview()
        logger.info(
            "External seed promotion preview | raw_rows=%s | distinct_emails=%s | distinct_accounts=%s | current_clients=%s | canada_rows=%s | personal_emails=%s | dry_run=%s",
            preview.raw_rows,
            preview.distinct_emails,
            preview.distinct_accounts,
            preview.current_clients,
            preview.canada_rows,
            preview.personal_email_rows,
            dry_run,
        )

        if dry_run:
            logger.info("Dry run enabled. No canonical promotion writes were executed.")
            return

        self._merge_dealer_accounts()
        self._merge_prospect_contacts()
        self._merge_account_relationships()

    def _merge_dealer_accounts(self) -> None:
        """Create or update canonical dealer account seed rows from external imports."""

        query = f"""
        MERGE `{self.settings.dealer_accounts_table_fqn}` AS target
        USING (
          SELECT
            TO_HEX(SHA256(account_key)) AS dealer_account_id,
            account_key,
            ANY_VALUE(email_domain) AS email_domain,
            CAST(NULL AS STRING) AS account_name,
            ANY_VALUE(NULLIF(inferred_brand_from_source, '')) AS inferred_brand,
            CAST(NULL AS STRING) AS website_url,
            CASE
              WHEN LOGICAL_OR(is_personal_email) THEN 'external_seed_personal_domain'
              ELSE 'external_seed_domain'
            END AS account_type,
            'active' AS account_status,
            MAX(
              CASE source_group
                WHEN 'current_clients' THEN 0.95
                WHEN 'engagement_list' THEN 0.85
                WHEN 'oem_seed' THEN 0.80
                WHEN 'canada_oem_seed' THEN 0.80
                WHEN 'industry_seed' THEN 0.70
                WHEN 'combined_subscribers' THEN 0.60
                WHEN 'automotive_services' THEN 0.55
                ELSE 0.50
              END
            ) AS confidence_score,
            'external_seed_promotion' AS source_type,
            '{self.settings.external_seed_contacts_table}' AS source_table,
            LOGICAL_OR(is_personal_email) AS is_personal_domain,
            MIN(imported_at) AS first_seen_at,
            MAX(imported_at) AS last_seen_at,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
          FROM `{self.settings.external_seed_contacts_table_fqn}`
          WHERE account_key IS NOT NULL
            AND TRIM(account_key) != ''
          GROUP BY account_key
        ) AS source
        ON target.dealer_account_id = source.dealer_account_id
        WHEN MATCHED THEN
          UPDATE SET
            target.email_domain = COALESCE(target.email_domain, source.email_domain),
            target.inferred_brand = COALESCE(target.inferred_brand, source.inferred_brand),
            target.account_type = COALESCE(target.account_type, source.account_type),
            target.account_status = COALESCE(target.account_status, source.account_status),
            target.confidence_score = GREATEST(IFNULL(target.confidence_score, 0.0), IFNULL(source.confidence_score, 0.0)),
            target.source_type = source.source_type,
            target.source_table = source.source_table,
            target.is_personal_domain = IFNULL(target.is_personal_domain, FALSE) OR source.is_personal_domain,
            target.first_seen_at = LEAST(IFNULL(target.first_seen_at, source.first_seen_at), source.first_seen_at),
            target.last_seen_at = GREATEST(IFNULL(target.last_seen_at, source.last_seen_at), source.last_seen_at),
            target.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (
            dealer_account_id,
            account_key,
            email_domain,
            account_name,
            inferred_brand,
            website_url,
            account_type,
            account_status,
            confidence_score,
            source_type,
            source_table,
            is_personal_domain,
            first_seen_at,
            last_seen_at,
            created_at,
            updated_at
          )
          VALUES (
            source.dealer_account_id,
            source.account_key,
            source.email_domain,
            source.account_name,
            source.inferred_brand,
            source.website_url,
            source.account_type,
            source.account_status,
            source.confidence_score,
            source.source_type,
            source.source_table,
            source.is_personal_domain,
            source.first_seen_at,
            source.last_seen_at,
            source.created_at,
            source.updated_at
          )
        """
        self._execute_logged_merge(
            query,
            self.settings.external_seed_contacts_table_fqn,
            self.settings.dealer_accounts_table_fqn,
        )

    def _merge_prospect_contacts(self) -> None:
        """Create or update canonical contact seed rows from external imports."""

        query = f"""
        MERGE `{self.settings.prospect_contacts_table_fqn}` AS target
        USING (
          SELECT
            TO_HEX(SHA256(normalized_email)) AS prospect_contact_id,
            ANY_VALUE(external_seed_contact_id) AS source_contact_id,
            ANY_VALUE(normalized_full_name) AS full_name,
            ANY_VALUE(first_name) AS first_name,
            ANY_VALUE(last_name) AS last_name,
            normalized_email AS email,
            ANY_VALUE(email_domain) AS email_domain,
            LOGICAL_OR(is_personal_email) AS is_personal_email,
            CASE
              WHEN LOGICAL_OR(is_personal_email) THEN 'personal'
              ELSE 'business'
            END AS domain_type,
            CAST(NULL AS STRING) AS role_type,
            CAST(NULL AS STRING) AS role_title,
            CAST(NULL AS STRING) AS role_family,
            'active' AS contact_status,
            MAX(
              CASE source_group
                WHEN 'current_clients' THEN 0.95
                WHEN 'engagement_list' THEN 0.85
                WHEN 'oem_seed' THEN 0.80
                WHEN 'canada_oem_seed' THEN 0.80
                WHEN 'industry_seed' THEN 0.70
                WHEN 'combined_subscribers' THEN 0.60
                WHEN 'automotive_services' THEN 0.55
                ELSE 0.50
              END
            ) AS confidence_score,
            'external_seed_promotion' AS source_type,
            '{self.settings.external_seed_contacts_table}' AS source_table,
            ANY_VALUE(source_file_name) AS source_file_name,
            CASE
              WHEN COUNTIF(audience_type = 'current_client') > 0 THEN 'current_client'
              ELSE 'prospect'
            END AS audience_type,
            CASE
              WHEN COUNTIF(country = 'Canada') > 0 THEN 'Canada'
              ELSE ANY_VALUE(market)
            END AS market,
            CASE
              WHEN COUNTIF(country = 'Canada') > 0 THEN 'Canada'
              ELSE ANY_VALUE(country)
            END AS country,
            MIN(imported_at) AS first_seen_at,
            MAX(imported_at) AS last_seen_at,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
          FROM `{self.settings.external_seed_contacts_table_fqn}`
          WHERE normalized_email IS NOT NULL
            AND TRIM(normalized_email) != ''
          GROUP BY normalized_email
        ) AS source
        ON target.prospect_contact_id = source.prospect_contact_id
        WHEN MATCHED THEN
          UPDATE SET
            target.source_contact_id = COALESCE(target.source_contact_id, source.source_contact_id),
            target.full_name = COALESCE(target.full_name, source.full_name),
            target.first_name = COALESCE(target.first_name, source.first_name),
            target.last_name = COALESCE(target.last_name, source.last_name),
            target.email_domain = COALESCE(target.email_domain, source.email_domain),
            target.is_personal_email = IFNULL(target.is_personal_email, FALSE) OR source.is_personal_email,
            target.domain_type = COALESCE(target.domain_type, source.domain_type),
            target.contact_status = COALESCE(target.contact_status, source.contact_status),
            target.confidence_score = GREATEST(IFNULL(target.confidence_score, 0.0), IFNULL(source.confidence_score, 0.0)),
            target.source_type = source.source_type,
            target.source_table = source.source_table,
            target.source_file_name = COALESCE(target.source_file_name, source.source_file_name),
            target.audience_type = CASE
              WHEN source.audience_type = 'current_client' THEN 'current_client'
              ELSE COALESCE(target.audience_type, source.audience_type)
            END,
            target.market = CASE
              WHEN source.country = 'Canada' THEN 'Canada'
              ELSE COALESCE(target.market, source.market)
            END,
            target.country = CASE
              WHEN source.country = 'Canada' THEN 'Canada'
              ELSE COALESCE(target.country, source.country)
            END,
            target.first_seen_at = LEAST(IFNULL(target.first_seen_at, source.first_seen_at), source.first_seen_at),
            target.last_seen_at = GREATEST(IFNULL(target.last_seen_at, source.last_seen_at), source.last_seen_at),
            target.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (
            prospect_contact_id,
            source_contact_id,
            full_name,
            first_name,
            last_name,
            email,
            email_domain,
            is_personal_email,
            domain_type,
            role_type,
            role_title,
            role_family,
            contact_status,
            confidence_score,
            source_type,
            source_table,
            source_file_name,
            audience_type,
            market,
            country,
            first_seen_at,
            last_seen_at,
            created_at,
            updated_at
          )
          VALUES (
            source.prospect_contact_id,
            source.source_contact_id,
            source.full_name,
            source.first_name,
            source.last_name,
            source.email,
            source.email_domain,
            source.is_personal_email,
            source.domain_type,
            source.role_type,
            source.role_title,
            source.role_family,
            source.contact_status,
            source.confidence_score,
            source.source_type,
            source.source_table,
            source.source_file_name,
            source.audience_type,
            source.market,
            source.country,
            source.first_seen_at,
            source.last_seen_at,
            source.created_at,
            source.updated_at
          )
        """
        self._execute_logged_merge(
            query,
            self.settings.external_seed_contacts_table_fqn,
            self.settings.prospect_contacts_table_fqn,
        )

    def _merge_account_relationships(self) -> None:
        """Create or update canonical account-to-contact relationships from external imports."""

        query = f"""
        MERGE `{self.settings.account_relationships_table_fqn}` AS target
        USING (
          SELECT
            TO_HEX(SHA256(CONCAT(account_key, '|', normalized_email))) AS relationship_id,
            TO_HEX(SHA256(account_key)) AS dealer_account_id,
            TO_HEX(SHA256(normalized_email)) AS prospect_contact_id,
            'external_seed_membership' AS relationship_type,
            TRUE AS is_primary,
            'active' AS relationship_status,
            MAX(
              CASE source_group
                WHEN 'current_clients' THEN 0.95
                WHEN 'engagement_list' THEN 0.85
                WHEN 'oem_seed' THEN 0.80
                WHEN 'canada_oem_seed' THEN 0.80
                WHEN 'industry_seed' THEN 0.70
                WHEN 'combined_subscribers' THEN 0.60
                WHEN 'automotive_services' THEN 0.55
                ELSE 0.50
              END
            ) AS confidence_score,
            'external_seed_promotion' AS source_type,
            '{self.settings.external_seed_contacts_table}' AS source_table,
            MIN(imported_at) AS first_seen_at,
            MAX(imported_at) AS last_seen_at,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at
          FROM `{self.settings.external_seed_contacts_table_fqn}`
          WHERE account_key IS NOT NULL
            AND TRIM(account_key) != ''
            AND normalized_email IS NOT NULL
            AND TRIM(normalized_email) != ''
          GROUP BY account_key, normalized_email
        ) AS source
        ON target.relationship_id = source.relationship_id
        WHEN MATCHED THEN
          UPDATE SET
            target.relationship_status = COALESCE(target.relationship_status, source.relationship_status),
            target.confidence_score = GREATEST(IFNULL(target.confidence_score, 0.0), IFNULL(source.confidence_score, 0.0)),
            target.last_seen_at = GREATEST(IFNULL(target.last_seen_at, source.last_seen_at), source.last_seen_at),
            target.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (
            relationship_id,
            dealer_account_id,
            prospect_contact_id,
            relationship_type,
            is_primary,
            relationship_status,
            confidence_score,
            source_type,
            source_table,
            first_seen_at,
            last_seen_at,
            created_at,
            updated_at
          )
          VALUES (
            source.relationship_id,
            source.dealer_account_id,
            source.prospect_contact_id,
            source.relationship_type,
            source.is_primary,
            source.relationship_status,
            source.confidence_score,
            source.source_type,
            source.source_table,
            source.first_seen_at,
            source.last_seen_at,
            source.created_at,
            source.updated_at
          )
        """
        self._execute_logged_merge(
            query,
            self.settings.external_seed_contacts_table_fqn,
            self.settings.account_relationships_table_fqn,
        )

    def _execute_logged_merge(self, query: str, source_table: str, target_table: str) -> None:
        """Execute a merge statement and log the result."""

        logger.info("Running live write | source=%s | target=%s", source_table, target_table)
        job = self.repository.execute_statement(query)
        logger.info(
            "Completed live write | source=%s | target=%s | affected_rows=%s",
            source_table,
            target_table,
            job.num_dml_affected_rows,
        )
