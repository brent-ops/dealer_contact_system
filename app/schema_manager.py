"""Create and verify BigQuery tables used by the pipeline."""

from __future__ import annotations

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger


logger = get_logger(__name__)


class SchemaManager:
    """Ensure required canonical tables exist in BigQuery."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def ensure_tables(self) -> None:
        """Create canonical tables if they are missing."""

        lane_queue_schema = """
            (
              work_item_id STRING NOT NULL,
              account_key STRING NOT NULL,
              status STRING NOT NULL,
              priority INT64,
              attempt_count INT64,
              reason STRING,
              dedupe_key STRING,
              parent_lane STRING,
              source_lane STRING,
              work_phase STRING,
              intake_source STRING,
              intake_rank INT64,
              lease_owner STRING,
              lease_expires_at TIMESTAMP,
              last_attempt_at TIMESTAMP,
              first_attempt_at TIMESTAMP,
              next_attempt_at TIMESTAMP,
              retry_due_at TIMESTAMP,
              completed_at TIMESTAMP,
              last_error STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
        """
        statements = {
            self.settings.dealer_accounts_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.dealer_accounts_table_fqn}` (
              dealer_account_id STRING NOT NULL,
              account_key STRING NOT NULL,
              email_domain STRING,
              account_name STRING,
              inferred_brand STRING,
              dealer_classification STRING,
              account_city STRING,
              account_state STRING,
              account_phone STRING,
              website_phone STRING,
              gbp_phone STRING,
              ai_phone STRING,
              gbp_display_name STRING,
              ai_display_name STRING,
              gbp_address_line STRING,
              ai_address_line STRING,
              gbp_city STRING,
              ai_city STRING,
              gbp_state_or_province STRING,
              ai_state_or_province STRING,
              gbp_postal_code STRING,
              ai_postal_code STRING,
              gbp_country STRING,
              ai_country STRING,
              gbp_source STRING,
              ai_website_url STRING,
              ai_staff_page_url STRING,
              ai_staff_hints_json STRING,
              ai_staff_hint_count INT64,
              ai_source_provider STRING,
              ai_source_url STRING,
              best_phone STRING,
              best_phone_source STRING,
              best_location_source STRING,
              website_url STRING,
              account_type STRING,
              account_status STRING,
              enrichment_stage STRING,
              activation_status STRING,
              confidence_score FLOAT64,
              dealer_classification_confidence_score FLOAT64,
              website_confidence_score FLOAT64,
              account_name_confidence_score FLOAT64,
              brand_confidence_score FLOAT64,
              location_confidence_score FLOAT64,
              account_phone_confidence_score FLOAT64,
              website_phone_confidence_score FLOAT64,
              gbp_phone_confidence_score FLOAT64,
              gbp_address_confidence_score FLOAT64,
              ai_confidence_score FLOAT64,
              best_phone_confidence_score FLOAT64,
              source_type STRING,
              source_table STRING,
              intake_source STRING,
              intake_rank INT64,
              promoted_at TIMESTAMP,
              website_source_url STRING,
              account_name_source_url STRING,
              brand_source_url STRING,
              dealer_classification_source_url STRING,
              dealer_validation_checked_at TIMESTAMP,
              location_source_url STRING,
              account_phone_source_url STRING,
              website_phone_source_url STRING,
              gbp_phone_source_url STRING,
              gbp_last_verified_at TIMESTAMP,
              best_phone_source_url STRING,
              ai_request_id STRING,
              ai_retrieval_status STRING,
              ai_retrieval_last_error STRING,
              ai_retrieval_last_attempt_at TIMESTAMP,
              next_ai_retrieval_at TIMESTAMP,
              ai_last_verified_at TIMESTAMP,
              fetch_status STRING,
              fetch_method STRING,
              blocked_reason STRING,
              blocked_attempt_count INT64,
              last_fetch_attempt_at TIMESTAMP,
              last_fetch_success_at TIMESTAMP,
              is_personal_domain BOOL,
              last_verified_at TIMESTAMP,
              first_seen_at TIMESTAMP,
              last_seen_at TIMESTAMP,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.prospect_contacts_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.prospect_contacts_table_fqn}` (
              prospect_contact_id STRING NOT NULL,
              source_contact_id STRING,
              full_name STRING,
              first_name STRING,
              last_name STRING,
              email STRING NOT NULL,
              email_domain STRING,
              is_personal_email BOOL,
              domain_type STRING,
              role_type STRING,
              role_title STRING,
              role_family STRING,
              phone_number STRING,
              email_quality STRING,
              email_source_type STRING,
              email_source_url STRING,
              email_confidence_score FLOAT64,
              contact_status STRING,
              enrichment_stage STRING,
              activation_status STRING,
              confidence_score FLOAT64,
              source_type STRING,
              source_table STRING,
              source_file_name STRING,
              audience_type STRING,
              market STRING,
              country STRING,
              source_url STRING,
              first_seen_at TIMESTAMP,
              last_seen_at TIMESTAMP,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.account_relationships_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.account_relationships_table_fqn}` (
              relationship_id STRING NOT NULL,
              dealer_account_id STRING NOT NULL,
              prospect_contact_id STRING NOT NULL,
              relationship_type STRING,
              is_primary BOOL,
              relationship_status STRING,
              confidence_score FLOAT64,
              source_type STRING,
              source_table STRING,
              source_url STRING,
              first_seen_at TIMESTAMP,
              last_seen_at TIMESTAMP,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.sync_targets_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.sync_targets_table_fqn}` (
              sync_target_id STRING NOT NULL,
              target_system STRING NOT NULL,
              target_entity_type STRING NOT NULL,
              target_entity_id STRING,
              source_record_type STRING NOT NULL,
              source_record_id STRING NOT NULL,
              sync_status STRING,
              sync_detail STRING,
              last_synced_at TIMESTAMP,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.account_work_queue_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.account_work_queue_table_fqn}` (
              work_item_id STRING NOT NULL,
              task_type STRING NOT NULL,
              account_key STRING NOT NULL,
              status STRING NOT NULL,
              priority INT64,
              attempt_count INT64,
              lease_owner STRING,
              lease_expires_at TIMESTAMP,
              last_attempt_at TIMESTAMP,
              next_attempt_at TIMESTAMP,
              completed_at TIMESTAMP,
              last_error STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.validate_queue_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.validate_queue_table_fqn}` {lane_queue_schema}
            """,
            self.settings.crawl_queue_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.crawl_queue_table_fqn}` {lane_queue_schema}
            """,
            self.settings.gbp_queue_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.gbp_queue_table_fqn}` {lane_queue_schema}
            """,
            self.settings.ai_queue_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.ai_queue_table_fqn}` {lane_queue_schema}
            """,
            self.settings.contact_extract_queue_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.contact_extract_queue_table_fqn}` {lane_queue_schema}
            """,
            self.settings.blocked_retry_queue_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.blocked_retry_queue_table_fqn}` {lane_queue_schema}
            """,
            self.settings.lead_refresh_queue_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.lead_refresh_queue_table_fqn}` {lane_queue_schema}
            """,
            self.settings.lane_execution_state_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.lane_execution_state_table_fqn}` (
              lane_state_id STRING NOT NULL,
              lane_name STRING NOT NULL,
              account_key STRING NOT NULL,
              state_status STRING NOT NULL,
              freshness_status STRING,
              last_attempt_at TIMESTAMP,
              last_success_at TIMESTAMP,
              next_eligible_at TIMESTAMP,
              stale_after_at TIMESTAMP,
              upstream_lane STRING,
              last_handoff_reason STRING,
              detail STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.pipeline_runs_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.pipeline_runs_table_fqn}` (
              pipeline_run_id STRING NOT NULL,
              task_type STRING NOT NULL,
              run_status STRING NOT NULL,
              worker_id STRING,
              requested_batch_size INT64,
              claimed_count INT64,
              succeeded_count INT64,
              failed_count INT64,
              run_notes STRING,
              started_at TIMESTAMP,
              completed_at TIMESTAMP,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.domain_discovery_queue_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.domain_discovery_queue_table_fqn}` (
              work_item_id STRING NOT NULL,
              dedupe_key STRING NOT NULL,
              search_term STRING NOT NULL,
              brand_hint STRING,
              market STRING,
              country STRING,
              state_or_province STRING,
              city STRING,
              status STRING NOT NULL,
              priority INT64,
              attempt_count INT64,
              lease_owner STRING,
              lease_expires_at TIMESTAMP,
              last_attempt_at TIMESTAMP,
              next_attempt_at TIMESTAMP,
              completed_at TIMESTAMP,
              last_error STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.domain_discovery_runs_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.domain_discovery_runs_table_fqn}` (
              discovery_run_id STRING NOT NULL,
              run_type STRING NOT NULL,
              run_status STRING NOT NULL,
              worker_id STRING,
              requested_batch_size INT64,
              claimed_count INT64,
              succeeded_count INT64,
              failed_count INT64,
              run_notes STRING,
              started_at TIMESTAMP,
              completed_at TIMESTAMP,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.discovered_domain_candidates_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.discovered_domain_candidates_table_fqn}` (
              candidate_id STRING NOT NULL,
              search_term STRING NOT NULL,
              brand_hint STRING,
              market STRING,
              country STRING,
              state_or_province STRING,
              city STRING,
              candidate_domain STRING NOT NULL,
              candidate_website_url STRING,
              candidate_account_name STRING,
              source_engine STRING,
              source_url STRING,
              discovered_at TIMESTAMP,
              confidence_score FLOAT64,
              dedupe_key STRING NOT NULL,
              promotion_status STRING,
              promotion_reason STRING,
              promoted_account_key STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP,
              last_seen_at TIMESTAMP
            )
            """,
            self.settings.metro_discovery_targets_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.metro_discovery_targets_table_fqn}` (
              metro_target_id STRING NOT NULL,
              metro_key STRING NOT NULL,
              metro_name STRING NOT NULL,
              city STRING NOT NULL,
              state_or_province STRING NOT NULL,
              country STRING NOT NULL,
              market STRING NOT NULL,
              population_rank INT64,
              active BOOL,
              last_seeded_at TIMESTAMP,
              last_discovered_at TIMESTAMP,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.ai_retrieval_results_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.ai_retrieval_results_table_fqn}` (
              ai_retrieval_result_id STRING NOT NULL,
              dealer_account_id STRING NOT NULL,
              account_key STRING NOT NULL,
              retrieval_intent STRING NOT NULL,
              provider STRING NOT NULL,
              provider_status STRING NOT NULL,
              request_id STRING,
              facts_json STRING,
              citations_json STRING,
              confidence_score FLOAT64,
              detail STRING,
              error_message STRING,
              retrieved_at TIMESTAMP,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
            self.settings.dashboard_snapshots_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.dashboard_snapshots_table_fqn}` (
              snapshot_id STRING NOT NULL,
              snapshot_at TIMESTAMP NOT NULL,
              source_contacts INT64,
              dealer_accounts INT64,
              validated_dealers INT64,
              validated_dealer_groups INT64,
              accounts_with_phone INT64,
              accounts_with_website_phone INT64,
              accounts_with_gbp_phone INT64,
              accounts_with_ai_phone INT64,
              accounts_with_gbp_address INT64,
              accounts_with_ai_address INT64,
              accounts_with_best_phone_from_gbp INT64,
              accounts_with_best_phone_from_ai INT64,
              activation_ready_accounts INT64,
              marketing_ready_contacts INT64,
              sales_ready_leads INT64,
              validated_websites INT64,
              enriched_websites INT64,
              prospect_contacts INT64,
              activation_ready_contacts INT64,
              current_client_contacts INT64,
              canada_contacts INT64,
              website_extracted_contacts INT64,
              blocked_fetch_accounts INT64,
              queued_ai_account_facts INT64,
              queued_validate INT64,
              queued_enrich INT64,
              queued_extract INT64,
              queued_retry_blocked INT64,
              created_at TIMESTAMP
            )
            """,
            self.settings.external_seed_contacts_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.external_seed_contacts_table_fqn}` (
              external_seed_contact_id STRING NOT NULL,
              import_batch_id STRING NOT NULL,
              source_file_name STRING NOT NULL,
              source_path STRING,
              source_group STRING,
              audience_type STRING,
              market STRING,
              country STRING,
              inferred_brand_from_source STRING,
              row_number INT64,
              raw_name STRING,
              raw_email STRING,
              normalized_full_name STRING,
              first_name STRING,
              last_name STRING,
              normalized_email STRING,
              email_domain STRING,
              account_key STRING,
              is_personal_email BOOL,
              import_notes STRING,
              imported_at TIMESTAMP,
              created_at TIMESTAMP
            )
            """,
            self.settings.prospect_leads_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.prospect_leads_table_fqn}` (
              prospect_lead_id STRING NOT NULL,
              relationship_id STRING,
              prospect_contact_id STRING NOT NULL,
              dealer_account_id STRING NOT NULL,
              full_name STRING,
              first_name STRING,
              last_name STRING,
              email STRING NOT NULL,
              email_domain STRING,
              phone_number STRING,
              dealer_name STRING,
              account_key STRING,
              website_url STRING,
              city STRING,
              state STRING,
              country STRING,
              market STRING,
              oem STRING,
              dealer_classification STRING,
              role_family STRING,
              role_title STRING,
              email_quality STRING,
              email_source_type STRING,
              email_source_url STRING,
              email_confidence_score FLOAT64,
              legal_contact_flag BOOL,
              activation_status STRING,
              marketing_ready_flag BOOL,
              sales_ready_flag BOOL,
              audience_type STRING,
              source_list STRING,
              is_current_client BOOL,
              is_canada BOOL,
              website_phone STRING,
              gbp_phone STRING,
              ai_phone STRING,
              address_line STRING,
              postal_code STRING,
              ai_address_line STRING,
              ai_city STRING,
              ai_state_or_province STRING,
              ai_postal_code STRING,
              ai_country STRING,
              ai_source_provider STRING,
              ai_source_url STRING,
              best_location_source STRING,
              best_phone STRING,
              best_phone_source STRING,
              contact_confidence_score FLOAT64,
              account_confidence_score FLOAT64,
              dim_client_match_flag BOOL,
              dim_client_id STRING,
              dim_account_owner STRING,
              prospecting_allowed_flag BOOL,
              suppression_reason STRING,
              current_client_override_flag BOOL,
              first_seen_at TIMESTAMP,
              last_seen_at TIMESTAMP,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
        }

        for table_name, statement in statements.items():
            logger.info("Ensuring table exists: %s", table_name)
            self.repository.execute_statement(statement)

        self._ensure_optional_columns()
        self._ensure_views()

    def ensure_parallel_worker_tables(self) -> None:
        """Create only the distributed-worker operational tables."""

        lane_queue_schema = """
            (
              work_item_id STRING NOT NULL,
              account_key STRING NOT NULL,
              status STRING NOT NULL,
              priority INT64,
              attempt_count INT64,
              reason STRING,
              dedupe_key STRING,
              parent_lane STRING,
              source_lane STRING,
              work_phase STRING,
              intake_source STRING,
              intake_rank INT64,
              lease_owner STRING,
              lease_expires_at TIMESTAMP,
              last_attempt_at TIMESTAMP,
              first_attempt_at TIMESTAMP,
              next_attempt_at TIMESTAMP,
              retry_due_at TIMESTAMP,
              completed_at TIMESTAMP,
              last_error STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
        """
        statements = {
            self.settings.validate_queue_table_fqn: f"CREATE TABLE IF NOT EXISTS `{self.settings.validate_queue_table_fqn}` {lane_queue_schema}",
            self.settings.crawl_queue_table_fqn: f"CREATE TABLE IF NOT EXISTS `{self.settings.crawl_queue_table_fqn}` {lane_queue_schema}",
            self.settings.gbp_queue_table_fqn: f"CREATE TABLE IF NOT EXISTS `{self.settings.gbp_queue_table_fqn}` {lane_queue_schema}",
            self.settings.ai_queue_table_fqn: f"CREATE TABLE IF NOT EXISTS `{self.settings.ai_queue_table_fqn}` {lane_queue_schema}",
            self.settings.contact_extract_queue_table_fqn: f"CREATE TABLE IF NOT EXISTS `{self.settings.contact_extract_queue_table_fqn}` {lane_queue_schema}",
            self.settings.blocked_retry_queue_table_fqn: f"CREATE TABLE IF NOT EXISTS `{self.settings.blocked_retry_queue_table_fqn}` {lane_queue_schema}",
            self.settings.lead_refresh_queue_table_fqn: f"CREATE TABLE IF NOT EXISTS `{self.settings.lead_refresh_queue_table_fqn}` {lane_queue_schema}",
            self.settings.lane_execution_state_table_fqn: f"""
            CREATE TABLE IF NOT EXISTS `{self.settings.lane_execution_state_table_fqn}` (
              lane_state_id STRING NOT NULL,
              lane_name STRING NOT NULL,
              account_key STRING NOT NULL,
              state_status STRING NOT NULL,
              freshness_status STRING,
              last_attempt_at TIMESTAMP,
              last_success_at TIMESTAMP,
              next_eligible_at TIMESTAMP,
              stale_after_at TIMESTAMP,
              upstream_lane STRING,
              last_handoff_reason STRING,
              detail STRING,
              created_at TIMESTAMP,
              updated_at TIMESTAMP
            )
            """,
        }
        for table_name, statement in statements.items():
            logger.info("Ensuring parallel worker table exists: %s", table_name)
            self.repository.execute_statement(statement)

    def _ensure_optional_columns(self) -> None:
        """Add newly introduced columns without rewriting any tables."""

        alter_statements = [
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS account_city STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS account_state STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS account_phone STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS website_phone STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_phone STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_phone STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_display_name STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_display_name STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_address_line STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_address_line STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_city STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_city STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_state_or_province STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_state_or_province STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_postal_code STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_postal_code STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_country STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_country STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_source STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_website_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_staff_page_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_staff_hints_json STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_staff_hint_count INT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_source_provider STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_source_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS best_phone STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS best_phone_source STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS best_location_source STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS dealer_classification STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS enrichment_stage STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS activation_status STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS dealer_classification_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS website_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS account_name_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS brand_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS location_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS account_phone_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS website_phone_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_phone_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_address_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS best_phone_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS website_source_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS account_name_source_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS brand_source_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS dealer_classification_source_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS dealer_validation_checked_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS location_source_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS account_phone_source_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS website_phone_source_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_phone_source_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_last_verified_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS best_phone_source_url STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_request_id STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_retrieval_status STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_retrieval_last_error STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_retrieval_last_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS next_ai_retrieval_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS ai_last_verified_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS fetch_status STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS fetch_method STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS blocked_reason STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS blocked_attempt_count INT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS last_fetch_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS last_fetch_success_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS best_fetch_method STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS managed_fetch_status STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS managed_fetch_provider STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS managed_fetch_notes STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS managed_fetch_last_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS managed_fetch_last_success_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS next_managed_fetch_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS fallback_source_status STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS last_verified_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS intake_source STRING",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS intake_rank INT64",
            f"ALTER TABLE `{self.settings.dealer_accounts_table_fqn}` ADD COLUMN IF NOT EXISTS promoted_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS role_family STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS phone_number STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS email_quality STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS email_source_type STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS email_source_url STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS email_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS enrichment_stage STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS activation_status STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS source_file_name STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS audience_type STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS market STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS country STRING",
            f"ALTER TABLE `{self.settings.prospect_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS source_url STRING",
            f"ALTER TABLE `{self.settings.account_relationships_table_fqn}` ADD COLUMN IF NOT EXISTS source_url STRING",
            f"ALTER TABLE `{self.settings.sync_targets_table_fqn}` ADD COLUMN IF NOT EXISTS sync_detail STRING",
            f"ALTER TABLE `{self.settings.account_work_queue_table_fqn}` ADD COLUMN IF NOT EXISTS priority INT64",
            f"ALTER TABLE `{self.settings.account_work_queue_table_fqn}` ADD COLUMN IF NOT EXISTS attempt_count INT64",
            f"ALTER TABLE `{self.settings.account_work_queue_table_fqn}` ADD COLUMN IF NOT EXISTS lease_owner STRING",
            f"ALTER TABLE `{self.settings.account_work_queue_table_fqn}` ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.account_work_queue_table_fqn}` ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.account_work_queue_table_fqn}` ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.account_work_queue_table_fqn}` ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.account_work_queue_table_fqn}` ADD COLUMN IF NOT EXISTS last_error STRING",
            f"ALTER TABLE `{self.settings.validate_queue_table_fqn}` ADD COLUMN IF NOT EXISTS work_phase STRING",
            f"ALTER TABLE `{self.settings.validate_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_source STRING",
            f"ALTER TABLE `{self.settings.validate_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_rank INT64",
            f"ALTER TABLE `{self.settings.validate_queue_table_fqn}` ADD COLUMN IF NOT EXISTS first_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.validate_queue_table_fqn}` ADD COLUMN IF NOT EXISTS retry_due_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.crawl_queue_table_fqn}` ADD COLUMN IF NOT EXISTS work_phase STRING",
            f"ALTER TABLE `{self.settings.crawl_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_source STRING",
            f"ALTER TABLE `{self.settings.crawl_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_rank INT64",
            f"ALTER TABLE `{self.settings.crawl_queue_table_fqn}` ADD COLUMN IF NOT EXISTS first_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.crawl_queue_table_fqn}` ADD COLUMN IF NOT EXISTS retry_due_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.gbp_queue_table_fqn}` ADD COLUMN IF NOT EXISTS work_phase STRING",
            f"ALTER TABLE `{self.settings.gbp_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_source STRING",
            f"ALTER TABLE `{self.settings.gbp_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_rank INT64",
            f"ALTER TABLE `{self.settings.gbp_queue_table_fqn}` ADD COLUMN IF NOT EXISTS first_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.gbp_queue_table_fqn}` ADD COLUMN IF NOT EXISTS retry_due_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.ai_queue_table_fqn}` ADD COLUMN IF NOT EXISTS work_phase STRING",
            f"ALTER TABLE `{self.settings.ai_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_source STRING",
            f"ALTER TABLE `{self.settings.ai_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_rank INT64",
            f"ALTER TABLE `{self.settings.ai_queue_table_fqn}` ADD COLUMN IF NOT EXISTS first_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.ai_queue_table_fqn}` ADD COLUMN IF NOT EXISTS retry_due_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.contact_extract_queue_table_fqn}` ADD COLUMN IF NOT EXISTS work_phase STRING",
            f"ALTER TABLE `{self.settings.contact_extract_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_source STRING",
            f"ALTER TABLE `{self.settings.contact_extract_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_rank INT64",
            f"ALTER TABLE `{self.settings.contact_extract_queue_table_fqn}` ADD COLUMN IF NOT EXISTS first_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.contact_extract_queue_table_fqn}` ADD COLUMN IF NOT EXISTS retry_due_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.blocked_retry_queue_table_fqn}` ADD COLUMN IF NOT EXISTS work_phase STRING",
            f"ALTER TABLE `{self.settings.blocked_retry_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_source STRING",
            f"ALTER TABLE `{self.settings.blocked_retry_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_rank INT64",
            f"ALTER TABLE `{self.settings.blocked_retry_queue_table_fqn}` ADD COLUMN IF NOT EXISTS first_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.blocked_retry_queue_table_fqn}` ADD COLUMN IF NOT EXISTS retry_due_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.lead_refresh_queue_table_fqn}` ADD COLUMN IF NOT EXISTS work_phase STRING",
            f"ALTER TABLE `{self.settings.lead_refresh_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_source STRING",
            f"ALTER TABLE `{self.settings.lead_refresh_queue_table_fqn}` ADD COLUMN IF NOT EXISTS intake_rank INT64",
            f"ALTER TABLE `{self.settings.lead_refresh_queue_table_fqn}` ADD COLUMN IF NOT EXISTS first_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.lead_refresh_queue_table_fqn}` ADD COLUMN IF NOT EXISTS retry_due_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.pipeline_runs_table_fqn}` ADD COLUMN IF NOT EXISTS worker_id STRING",
            f"ALTER TABLE `{self.settings.pipeline_runs_table_fqn}` ADD COLUMN IF NOT EXISTS requested_batch_size INT64",
            f"ALTER TABLE `{self.settings.pipeline_runs_table_fqn}` ADD COLUMN IF NOT EXISTS claimed_count INT64",
            f"ALTER TABLE `{self.settings.pipeline_runs_table_fqn}` ADD COLUMN IF NOT EXISTS succeeded_count INT64",
            f"ALTER TABLE `{self.settings.pipeline_runs_table_fqn}` ADD COLUMN IF NOT EXISTS failed_count INT64",
            f"ALTER TABLE `{self.settings.pipeline_runs_table_fqn}` ADD COLUMN IF NOT EXISTS run_notes STRING",
            f"ALTER TABLE `{self.settings.pipeline_runs_table_fqn}` ADD COLUMN IF NOT EXISTS started_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.pipeline_runs_table_fqn}` ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS dedupe_key STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS search_term STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS brand_hint STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS market STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS country STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS state_or_province STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS city STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS priority INT64",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS attempt_count INT64",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS lease_owner STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.domain_discovery_queue_table_fqn}` ADD COLUMN IF NOT EXISTS last_error STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_runs_table_fqn}` ADD COLUMN IF NOT EXISTS worker_id STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_runs_table_fqn}` ADD COLUMN IF NOT EXISTS requested_batch_size INT64",
            f"ALTER TABLE `{self.settings.domain_discovery_runs_table_fqn}` ADD COLUMN IF NOT EXISTS claimed_count INT64",
            f"ALTER TABLE `{self.settings.domain_discovery_runs_table_fqn}` ADD COLUMN IF NOT EXISTS succeeded_count INT64",
            f"ALTER TABLE `{self.settings.domain_discovery_runs_table_fqn}` ADD COLUMN IF NOT EXISTS failed_count INT64",
            f"ALTER TABLE `{self.settings.domain_discovery_runs_table_fqn}` ADD COLUMN IF NOT EXISTS run_notes STRING",
            f"ALTER TABLE `{self.settings.domain_discovery_runs_table_fqn}` ADD COLUMN IF NOT EXISTS started_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.domain_discovery_runs_table_fqn}` ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS brand_hint STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS market STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS country STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS state_or_province STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS city STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS candidate_domain STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS candidate_website_url STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS candidate_account_name STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS source_engine STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS source_url STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS discovered_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS dedupe_key STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS promotion_status STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS promotion_reason STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS promoted_account_key STRING",
            f"ALTER TABLE `{self.settings.discovered_domain_candidates_table_fqn}` ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.metro_discovery_targets_table_fqn}` ADD COLUMN IF NOT EXISTS metro_key STRING",
            f"ALTER TABLE `{self.settings.metro_discovery_targets_table_fqn}` ADD COLUMN IF NOT EXISTS metro_name STRING",
            f"ALTER TABLE `{self.settings.metro_discovery_targets_table_fqn}` ADD COLUMN IF NOT EXISTS city STRING",
            f"ALTER TABLE `{self.settings.metro_discovery_targets_table_fqn}` ADD COLUMN IF NOT EXISTS state_or_province STRING",
            f"ALTER TABLE `{self.settings.metro_discovery_targets_table_fqn}` ADD COLUMN IF NOT EXISTS country STRING",
            f"ALTER TABLE `{self.settings.metro_discovery_targets_table_fqn}` ADD COLUMN IF NOT EXISTS market STRING",
            f"ALTER TABLE `{self.settings.metro_discovery_targets_table_fqn}` ADD COLUMN IF NOT EXISTS population_rank INT64",
            f"ALTER TABLE `{self.settings.metro_discovery_targets_table_fqn}` ADD COLUMN IF NOT EXISTS active BOOL",
            f"ALTER TABLE `{self.settings.metro_discovery_targets_table_fqn}` ADD COLUMN IF NOT EXISTS last_seeded_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.metro_discovery_targets_table_fqn}` ADD COLUMN IF NOT EXISTS last_discovered_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS accounts_with_phone INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS accounts_with_website_phone INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS accounts_with_gbp_phone INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS accounts_with_ai_phone INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS accounts_with_gbp_address INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS accounts_with_ai_address INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS accounts_with_best_phone_from_gbp INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS accounts_with_best_phone_from_ai INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS activation_ready_accounts INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS marketing_ready_contacts INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS sales_ready_leads INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS activation_ready_contacts INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS current_client_contacts INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS canada_contacts INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS prospect_leads INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS dim_matched_leads INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS dim_suppressed_leads INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS managed_fetch_eligible_accounts INT64",
            f"ALTER TABLE `{self.settings.dashboard_snapshots_table_fqn}` ADD COLUMN IF NOT EXISTS queued_ai_account_facts INT64",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS source_path STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS source_group STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS audience_type STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS market STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS country STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS inferred_brand_from_source STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS row_number INT64",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS raw_name STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS raw_email STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS normalized_full_name STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS first_name STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS last_name STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS normalized_email STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS email_domain STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS account_key STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS is_personal_email BOOL",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS import_notes STRING",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS imported_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.external_seed_contacts_table_fqn}` ADD COLUMN IF NOT EXISTS created_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS relationship_id STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS prospect_contact_id STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS dealer_account_id STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS full_name STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS first_name STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS last_name STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS email STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS email_domain STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS phone_number STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS dealer_name STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS account_key STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS website_url STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS city STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS state STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS country STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS market STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS oem STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS dealer_classification STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS role_family STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS role_title STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS email_quality STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS email_source_type STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS email_source_url STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS email_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS legal_contact_flag BOOL",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS activation_status STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS marketing_ready_flag BOOL",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS sales_ready_flag BOOL",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS audience_type STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS source_list STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS is_current_client BOOL",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS is_canada BOOL",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS website_phone STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS gbp_phone STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS ai_phone STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS address_line STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS postal_code STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS ai_address_line STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS ai_city STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS ai_state_or_province STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS ai_postal_code STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS ai_country STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS ai_source_provider STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS ai_source_url STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS best_location_source STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS best_phone STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS best_phone_source STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS contact_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS account_confidence_score FLOAT64",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS dim_client_match_flag BOOL",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS dim_client_id STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS dim_account_owner STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS prospecting_allowed_flag BOOL",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS suppression_reason STRING",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS current_client_override_flag BOOL",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS created_at TIMESTAMP",
            f"ALTER TABLE `{self.settings.prospect_leads_table_fqn}` ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP",
        ]

        for statement in alter_statements:
            self.repository.execute_statement(statement)

    def _ensure_views(self) -> None:
        """Create or replace activation views used by downstream syncs."""

        activation_ready_view = f"""
        CREATE OR REPLACE VIEW `{self.settings.activation_ready_contacts_view_fqn}` AS
        WITH ranked_contacts AS (
          SELECT
            pc.prospect_contact_id,
            LOWER(pc.email) AS email,
            COALESCE(NULLIF(TRIM(pc.full_name), ''), TRIM(CONCAT(COALESCE(pc.first_name, ''), ' ', COALESCE(pc.last_name, '')))) AS full_name,
            COALESCE(NULLIF(TRIM(pc.first_name), ''), '') AS first_name,
            COALESCE(NULLIF(TRIM(pc.last_name), ''), '') AS last_name,
            COALESCE(NULLIF(TRIM(pc.phone_number), ''), NULLIF(TRIM(da.best_phone), ''), NULLIF(TRIM(da.account_phone), ''), '') AS phone_number,
            COALESCE(pc.is_personal_email, FALSE) AS is_personal_email,
            COALESCE(NULLIF(TRIM(pc.role_family), ''), 'unclassified') AS role_family,
            COALESCE(NULLIF(TRIM(pc.role_title), ''), '') AS role_title,
            COALESCE(NULLIF(TRIM(pc.audience_type), ''), 'prospect') AS audience_type,
            COALESCE(
              NULLIF(TRIM(pc.market), ''),
              CASE
                WHEN COALESCE(NULLIF(TRIM(pc.country), ''), NULLIF(TRIM(da.gbp_country), ''), 'United States') = 'Canada' THEN 'Canada'
                ELSE 'US'
              END
            ) AS market,
            COALESCE(NULLIF(TRIM(pc.country), ''), NULLIF(TRIM(da.gbp_country), ''), 'United States') AS country,
            COALESCE(NULLIF(TRIM(pc.source_file_name), ''), pc.source_table, 'contact_master') AS source_list,
            COALESCE(NULLIF(TRIM(da.account_name), ''), da.account_key, '') AS dealer_name,
            COALESCE(NULLIF(TRIM(da.inferred_brand), ''), 'Unknown') AS oem,
            COALESCE(NULLIF(TRIM(da.account_city), ''), '') AS city,
            COALESCE(NULLIF(TRIM(da.account_state), ''), '') AS state,
            COALESCE(NULLIF(TRIM(da.dealer_classification), ''), 'unclassified') AS dealer_classification,
            COALESCE(NULLIF(TRIM(da.website_url), ''), '') AS website_url,
            COALESCE(pc.confidence_score, 0.0) AS contact_confidence_score,
            COALESCE(da.confidence_score, 0.0) AS account_confidence_score,
            CASE
              WHEN pc.source_type = 'website_contact_extraction' THEN 'website_extracted'
              WHEN pc.country = 'Canada' THEN 'canada_seed'
              WHEN pc.audience_type = 'current_client' THEN 'current_client_seed'
              WHEN pc.source_table = '{self.settings.external_seed_contacts_table}' THEN 'external_seed'
              ELSE 'canonical'
            END AS readiness_source,
            pc.source_type,
            pc.source_url,
            da.source_type AS account_source_type,
            da.source_table AS account_source_table,
            (
              REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(pc.email_domain), ''), '')), r'(law|legal|attorney|attorneys|counsel|esq|esquire)\\.')
              OR REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(da.account_name), ''), '')), r'\b(law|legal|attorney|attorneys|counsel|esq|esquire)\b')
              OR REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(pc.role_title), ''), '')), r'\b(attorney|attorneys|counsel|esq|esquire|lawyer)\b')
            ) AS legal_contact_flag,
            ROW_NUMBER() OVER (
              PARTITION BY LOWER(pc.email)
              ORDER BY
                CASE
                  WHEN pc.source_type = 'website_contact_extraction' THEN 5
                  WHEN pc.audience_type = 'current_client' THEN 4
                  WHEN pc.source_file_name LIKE '%Openers%' THEN 3
                  WHEN pc.source_file_name LIKE '%OpenEmailList%' THEN 3
                  WHEN pc.country = 'Canada' THEN 3
                  WHEN da.dealer_classification IN ('dealer', 'dealer_group') THEN 2
                  ELSE 1
                END DESC,
                COALESCE(pc.confidence_score, 0) DESC,
                pc.last_seen_at DESC NULLS LAST,
                pc.created_at DESC
            ) AS row_number
          FROM `{self.settings.prospect_contacts_table_fqn}` AS pc
          JOIN `{self.settings.account_relationships_table_fqn}` AS ar
            ON ar.prospect_contact_id = pc.prospect_contact_id
          JOIN `{self.settings.dealer_accounts_table_fqn}` AS da
            ON da.dealer_account_id = ar.dealer_account_id
          WHERE pc.email IS NOT NULL
            AND TRIM(pc.email) != ''
            AND LOWER(COALESCE(pc.contact_status, 'active')) NOT IN ('inactive', 'suppressed', 'invalid')
            AND COALESCE(pc.activation_status, 'enrichment_needed') = 'activation_ready'
            AND NOT (
              REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(pc.email_domain), ''), '')), r'(law|legal|attorney|attorneys|counsel|esq|esquire)\\.')
              OR REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(da.account_name), ''), '')), r'\b(law|legal|attorney|attorneys|counsel|esq|esquire)\b')
              OR REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(TRIM(pc.role_title), ''), '')), r'\b(attorney|attorneys|counsel|esq|esquire|lawyer)\b')
            )
        )
        SELECT
          prospect_contact_id,
          email,
            full_name,
            first_name,
            last_name,
            phone_number,
            is_personal_email,
            role_family,
            role_title,
          audience_type,
          market,
          country,
          source_list,
          dealer_name,
          oem,
          city,
          state,
          dealer_classification,
          website_url,
          contact_confidence_score,
          account_confidence_score,
          readiness_source,
          source_type,
          source_url,
          account_source_type,
          account_source_table
        FROM ranked_contacts
        WHERE row_number = 1
        """

        marketing_ready_view = f"""
        CREATE OR REPLACE VIEW `{self.settings.marketing_ready_contacts_view_fqn}` AS
        SELECT
          *
        FROM `{self.settings.activation_ready_contacts_view_fqn}`
        WHERE TRIM(full_name) != ''
          AND TRIM(dealer_name) != ''
          AND TRIM(phone_number) != ''
        """

        sales_ready_view = f"""
        CREATE OR REPLACE VIEW `{self.settings.sales_ready_leads_view_fqn}` AS
        SELECT
          *
        FROM `{self.settings.marketing_ready_contacts_view_fqn}`
        WHERE audience_type != 'current_client'
          AND dealer_classification IN ('dealer', 'dealer_group')
          AND TRIM(full_name) != ''
          AND TRIM(dealer_name) != ''
        """

        self.repository.execute_statement(activation_ready_view)
        self.repository.execute_statement(marketing_ready_view)
        self.repository.execute_statement(sales_ready_view)
