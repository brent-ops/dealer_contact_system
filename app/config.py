"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


# Load variables from a local .env file if one exists.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    """Simple container for runtime configuration."""

    environment: str
    bigquery_project_id: str
    bigquery_dataset: str
    source_contact_table: str
    dealer_accounts_table: str
    prospect_contacts_table: str
    account_relationships_table: str
    sync_targets_table: str
    account_work_queue_table: str
    validate_queue_table: str
    crawl_queue_table: str
    gbp_queue_table: str
    ai_queue_table: str
    contact_extract_queue_table: str
    blocked_retry_queue_table: str
    lead_refresh_queue_table: str
    lane_execution_state_table: str
    pipeline_runs_table: str
    domain_discovery_queue_table: str
    domain_discovery_runs_table: str
    discovered_domain_candidates_table: str
    metro_discovery_targets_table: str
    dashboard_snapshots_table: str
    external_seed_contacts_table: str
    prospect_leads_table: str
    ai_retrieval_results_table: str
    activation_ready_contacts_view: str
    marketing_ready_contacts_view: str
    sales_ready_leads_view: str
    external_seed_directory: str
    enrichment_batch_size: int
    worker_batch_size: int
    worker_lease_minutes: int
    validate_worker_batch_size: int
    enrich_worker_batch_size: int
    gbp_worker_batch_size: int
    ai_retrieval_batch_size: int
    ai_parallel_workers: int
    extract_worker_batch_size: int
    retry_blocked_worker_batch_size: int
    campaign_monitor_sync_batch_size: int
    campaign_monitor_sync_enabled: bool
    parallel_enrichment_enabled: bool
    queue_manager_enabled: bool
    domain_discovery_enabled: bool
    domain_discovery_batch_size: int
    domain_discovery_query_batch_size: int
    domain_discovery_cooldown_hours: int
    domain_discovery_promotion_enabled: bool
    domain_discovery_schedule_hint: str
    domain_discovery_us_share_percent: int
    domain_discovery_provider_order: str
    domain_discovery_search_endpoint: str
    domain_discovery_job_name: str
    domain_discovery_scheduler_name: str
    domain_discovery_vpc_connector: str
    domain_discovery_vpc_egress: str
    domain_discovery_egress_ip: str
    process_watchdog_enabled: bool
    process_watchdog_main_stale_hours: int
    process_watchdog_discovery_stale_hours: int
    process_watchdog_prospect_leads_stale_hours: int
    process_watchdog_snapshot_stale_hours: int
    process_watchdog_campaign_monitor_stale_hours: int
    process_watchdog_running_grace_minutes: int
    process_watchdog_job_name: str
    process_watchdog_scheduler_name: str
    queue_manager_job_name: str
    queue_manager_scheduler_name: str
    validate_job_name: str
    validate_scheduler_name: str
    crawl_job_name: str
    crawl_scheduler_name: str
    gbp_job_name: str
    gbp_scheduler_name: str
    ai_job_name: str
    ai_scheduler_name: str
    contact_extract_job_name: str
    contact_extract_scheduler_name: str
    blocked_retry_job_name: str
    blocked_retry_scheduler_name: str
    lead_refresh_job_name: str
    lead_refresh_scheduler_name: str
    gbp_enrichment_enabled: bool
    gbp_provider: str
    gbp_search_endpoint: str
    managed_fetch_enabled: bool
    managed_fetch_provider: str
    managed_fetch_api_key: str | None
    managed_fetch_batch_size: int
    managed_fetch_min_blocked_attempts: int
    managed_fetch_cooldown_hours: int
    ai_retrieval_enabled: bool
    ai_retrieval_provider_order: str
    ai_retrieval_min_run_interval_minutes: int
    ai_retrieval_cooldown_hours: int
    ai_retrieval_rate_limit_cooldown_minutes: int
    ai_retrieval_request_delay_seconds: float
    ai_retrieval_prompt_style: str
    gemini_enabled: bool
    gemini_api_key: str | None
    gemini_model: str
    openai_enabled: bool
    openai_api_key: str | None
    openai_model: str
    cloud_run_region: str
    cloud_run_job_name: str
    request_timeout_seconds: int
    browser_timeout_seconds: int
    blocked_retry_short_cooldown_hours: int
    blocked_retry_medium_cooldown_hours: int
    blocked_retry_long_cooldown_hours: int
    campaign_monitor_api_key: str | None
    campaign_monitor_client_id: str | None
    campaign_monitor_master_list_name: str
    meta_access_token: str | None
    meta_ad_account_id: str | None
    google_ads_developer_token: str | None
    google_ads_customer_id: str | None
    google_application_credentials: str | None
    gld_accountability_project_id: str
    client_dim_dataset: str
    client_dim_table: str
    client_dim_enabled: bool
    client_dim_client_id_column: str
    client_dim_account_owner_column: str
    client_dim_account_key_column: str
    client_dim_domain_column: str
    client_dim_name_column: str
    client_dim_city_column: str
    client_dim_state_column: str
    client_dim_email_domain_column: str
    client_dim_email_column: str
    client_dim_current_client_flag_column: str

    @property
    def source_contact_table_fqn(self) -> str:
        """Return the fully qualified source table name."""

        return self.table_fqn(self.source_contact_table)

    @property
    def dealer_accounts_table_fqn(self) -> str:
        """Return the fully qualified dealer accounts table name."""

        return self.table_fqn(self.dealer_accounts_table)

    @property
    def prospect_contacts_table_fqn(self) -> str:
        """Return the fully qualified prospect contacts table name."""

        return self.table_fqn(self.prospect_contacts_table)

    @property
    def account_relationships_table_fqn(self) -> str:
        """Return the fully qualified account relationships table name."""

        return self.table_fqn(self.account_relationships_table)

    @property
    def sync_targets_table_fqn(self) -> str:
        """Return the fully qualified sync targets table name."""

        return self.table_fqn(self.sync_targets_table)

    @property
    def account_work_queue_table_fqn(self) -> str:
        """Return the fully qualified account work queue table name."""

        return self.table_fqn(self.account_work_queue_table)

    @property
    def validate_queue_table_fqn(self) -> str:
        """Return the fully qualified validate queue table name."""

        return self.table_fqn(self.validate_queue_table)

    @property
    def crawl_queue_table_fqn(self) -> str:
        """Return the fully qualified crawl queue table name."""

        return self.table_fqn(self.crawl_queue_table)

    @property
    def gbp_queue_table_fqn(self) -> str:
        """Return the fully qualified GBP queue table name."""

        return self.table_fqn(self.gbp_queue_table)

    @property
    def ai_queue_table_fqn(self) -> str:
        """Return the fully qualified AI queue table name."""

        return self.table_fqn(self.ai_queue_table)

    @property
    def contact_extract_queue_table_fqn(self) -> str:
        """Return the fully qualified contact extraction queue table name."""

        return self.table_fqn(self.contact_extract_queue_table)

    @property
    def blocked_retry_queue_table_fqn(self) -> str:
        """Return the fully qualified blocked retry queue table name."""

        return self.table_fqn(self.blocked_retry_queue_table)

    @property
    def lead_refresh_queue_table_fqn(self) -> str:
        """Return the fully qualified lead refresh queue table name."""

        return self.table_fqn(self.lead_refresh_queue_table)

    @property
    def lane_execution_state_table_fqn(self) -> str:
        """Return the fully qualified lane execution state table name."""

        return self.table_fqn(self.lane_execution_state_table)

    @property
    def pipeline_runs_table_fqn(self) -> str:
        """Return the fully qualified pipeline runs table name."""

        return self.table_fqn(self.pipeline_runs_table)

    @property
    def domain_discovery_queue_table_fqn(self) -> str:
        """Return the fully qualified domain discovery queue table name."""

        return self.table_fqn(self.domain_discovery_queue_table)

    @property
    def domain_discovery_runs_table_fqn(self) -> str:
        """Return the fully qualified domain discovery runs table name."""

        return self.table_fqn(self.domain_discovery_runs_table)

    @property
    def discovered_domain_candidates_table_fqn(self) -> str:
        """Return the fully qualified discovered domain candidates table name."""

        return self.table_fqn(self.discovered_domain_candidates_table)

    @property
    def metro_discovery_targets_table_fqn(self) -> str:
        """Return the fully qualified metro discovery targets table name."""

        return self.table_fqn(self.metro_discovery_targets_table)

    @property
    def dashboard_snapshots_table_fqn(self) -> str:
        """Return the fully qualified dashboard snapshots table name."""

        return self.table_fqn(self.dashboard_snapshots_table)

    @property
    def external_seed_contacts_table_fqn(self) -> str:
        """Return the fully qualified external seed contacts table name."""

        return self.table_fqn(self.external_seed_contacts_table)

    @property
    def prospect_leads_table_fqn(self) -> str:
        """Return the fully qualified prospect leads table name."""

        return self.table_fqn(self.prospect_leads_table)

    @property
    def ai_retrieval_results_table_fqn(self) -> str:
        """Return the fully qualified AI retrieval provenance table name."""

        return self.table_fqn(self.ai_retrieval_results_table)

    @property
    def activation_ready_contacts_view_fqn(self) -> str:
        """Return the fully qualified activation-ready contacts view name."""

        return self.table_fqn(self.activation_ready_contacts_view)

    @property
    def marketing_ready_contacts_view_fqn(self) -> str:
        """Return the fully qualified marketing-ready contacts view name."""

        return self.table_fqn(self.marketing_ready_contacts_view)

    @property
    def sales_ready_leads_view_fqn(self) -> str:
        """Return the fully qualified sales-ready leads view name."""

        return self.table_fqn(self.sales_ready_leads_view)

    @property
    def client_dim_table_fqn(self) -> str | None:
        """Return the fully qualified client DIM table name when configured."""

        if not self.client_dim_dataset or not self.client_dim_table:
            return None
        return f"{self.gld_accountability_project_id}.{self.client_dim_dataset}.{self.client_dim_table}"

    def table_fqn(self, table_name: str) -> str:
        """Build a fully qualified BigQuery table name."""

        return f"{self.bigquery_project_id}.{self.bigquery_dataset}.{table_name}"


def get_settings() -> Settings:
    """Read settings from environment variables with safe defaults."""

    return Settings(
        environment=os.getenv("APP_ENVIRONMENT", "local"),
        bigquery_project_id=os.getenv("BIGQUERY_PROJECT_ID", "dealer-contacts-project"),
        bigquery_dataset=os.getenv("BIGQUERY_DATASET", "dealer_data"),
        source_contact_table=os.getenv("SOURCE_CONTACT_TABLE", "contact_master"),
        dealer_accounts_table=os.getenv("DEALER_ACCOUNTS_TABLE", "dealer_accounts"),
        prospect_contacts_table=os.getenv("PROSPECT_CONTACTS_TABLE", "prospect_contacts"),
        account_relationships_table=os.getenv(
            "ACCOUNT_RELATIONSHIPS_TABLE",
            "account_relationships",
        ),
        sync_targets_table=os.getenv("SYNC_TARGETS_TABLE", "sync_targets"),
        account_work_queue_table=os.getenv("ACCOUNT_WORK_QUEUE_TABLE", "account_work_queue"),
        validate_queue_table=os.getenv("VALIDATE_QUEUE_TABLE", "validate_queue"),
        crawl_queue_table=os.getenv("CRAWL_QUEUE_TABLE", "crawl_queue"),
        gbp_queue_table=os.getenv("GBP_QUEUE_TABLE", "gbp_queue"),
        ai_queue_table=os.getenv("AI_QUEUE_TABLE", "ai_queue"),
        contact_extract_queue_table=os.getenv("CONTACT_EXTRACT_QUEUE_TABLE", "contact_extract_queue"),
        blocked_retry_queue_table=os.getenv("BLOCKED_RETRY_QUEUE_TABLE", "blocked_retry_queue"),
        lead_refresh_queue_table=os.getenv("LEAD_REFRESH_QUEUE_TABLE", "lead_refresh_queue"),
        lane_execution_state_table=os.getenv("LANE_EXECUTION_STATE_TABLE", "lane_execution_state"),
        pipeline_runs_table=os.getenv("PIPELINE_RUNS_TABLE", "pipeline_runs"),
        domain_discovery_queue_table=os.getenv("DOMAIN_DISCOVERY_QUEUE_TABLE", "domain_discovery_queue"),
        domain_discovery_runs_table=os.getenv("DOMAIN_DISCOVERY_RUNS_TABLE", "domain_discovery_runs"),
        discovered_domain_candidates_table=os.getenv(
            "DISCOVERED_DOMAIN_CANDIDATES_TABLE",
            "discovered_domain_candidates",
        ),
        metro_discovery_targets_table=os.getenv(
            "METRO_DISCOVERY_TARGETS_TABLE",
            "metro_discovery_targets",
        ),
        dashboard_snapshots_table=os.getenv("DASHBOARD_SNAPSHOTS_TABLE", "dashboard_snapshots"),
        external_seed_contacts_table=os.getenv("EXTERNAL_SEED_CONTACTS_TABLE", "external_seed_contacts"),
        prospect_leads_table=os.getenv("PROSPECT_LEADS_TABLE", "prospect_leads"),
        ai_retrieval_results_table=os.getenv("AI_RETRIEVAL_RESULTS_TABLE", "ai_retrieval_results"),
        activation_ready_contacts_view=os.getenv("ACTIVATION_READY_CONTACTS_VIEW", "activation_ready_contacts"),
        marketing_ready_contacts_view=os.getenv("MARKETING_READY_CONTACTS_VIEW", "marketing_ready_contacts"),
        sales_ready_leads_view=os.getenv("SALES_READY_LEADS_VIEW", "sales_ready_leads"),
        external_seed_directory=os.getenv(
            "EXTERNAL_SEED_DIRECTORY",
            str(Path.home() / "Downloads"),
        ),
        enrichment_batch_size=int(os.getenv("ENRICHMENT_BATCH_SIZE", "25")),
        worker_batch_size=int(os.getenv("WORKER_BATCH_SIZE", "50")),
        worker_lease_minutes=int(os.getenv("WORKER_LEASE_MINUTES", "30")),
        validate_worker_batch_size=int(os.getenv("VALIDATE_WORKER_BATCH_SIZE", "50")),
        enrich_worker_batch_size=int(os.getenv("ENRICH_WORKER_BATCH_SIZE", "25")),
        gbp_worker_batch_size=int(os.getenv("GBP_WORKER_BATCH_SIZE", "25")),
        ai_retrieval_batch_size=int(os.getenv("AI_RETRIEVAL_BATCH_SIZE", "1")),
        ai_parallel_workers=int(os.getenv("AI_PARALLEL_WORKERS", "1")),
        extract_worker_batch_size=int(os.getenv("EXTRACT_WORKER_BATCH_SIZE", "25")),
        retry_blocked_worker_batch_size=int(os.getenv("RETRY_BLOCKED_WORKER_BATCH_SIZE", "10")),
        campaign_monitor_sync_batch_size=int(os.getenv("CAMPAIGN_MONITOR_SYNC_BATCH_SIZE", "1000")),
        campaign_monitor_sync_enabled=os.getenv("CAMPAIGN_MONITOR_SYNC_ENABLED", "false").lower() == "true",
        parallel_enrichment_enabled=os.getenv("PARALLEL_ENRICHMENT_ENABLED", "true").lower() == "true",
        queue_manager_enabled=os.getenv("QUEUE_MANAGER_ENABLED", "true").lower() == "true",
        domain_discovery_enabled=os.getenv("DOMAIN_DISCOVERY_ENABLED", "true").lower() == "true",
        domain_discovery_batch_size=int(os.getenv("DOMAIN_DISCOVERY_BATCH_SIZE", "5")),
        domain_discovery_query_batch_size=int(os.getenv("DOMAIN_DISCOVERY_QUERY_BATCH_SIZE", "40")),
        domain_discovery_cooldown_hours=int(os.getenv("DOMAIN_DISCOVERY_COOLDOWN_HOURS", "4")),
        domain_discovery_promotion_enabled=os.getenv("DOMAIN_DISCOVERY_PROMOTION_ENABLED", "true").lower() == "true",
        domain_discovery_schedule_hint=os.getenv("DOMAIN_DISCOVERY_SCHEDULE_HINT", "*/30 * * * *"),
        domain_discovery_us_share_percent=int(os.getenv("DOMAIN_DISCOVERY_US_SHARE_PERCENT", "85")),
        domain_discovery_provider_order=os.getenv(
            "DOMAIN_DISCOVERY_PROVIDER_ORDER",
            "gemini_google_search,duckduckgo_html",
        ),
        domain_discovery_search_endpoint=os.getenv(
            "DOMAIN_DISCOVERY_SEARCH_ENDPOINT",
            "https://html.duckduckgo.com/html/",
        ),
        domain_discovery_job_name=os.getenv("DOMAIN_DISCOVERY_JOB_NAME", "dealer-domain-discovery-worker"),
        domain_discovery_scheduler_name=os.getenv("DOMAIN_DISCOVERY_SCHEDULER_NAME", "dealer-domain-discovery-schedule"),
        domain_discovery_vpc_connector=os.getenv("DOMAIN_DISCOVERY_VPC_CONNECTOR", "dealer-discovery-conn"),
        domain_discovery_vpc_egress=os.getenv("DOMAIN_DISCOVERY_VPC_EGRESS", "all-traffic"),
        domain_discovery_egress_ip=os.getenv("DOMAIN_DISCOVERY_EGRESS_IP", "34.45.226.9"),
        process_watchdog_enabled=os.getenv("PROCESS_WATCHDOG_ENABLED", "true").lower() == "true",
        process_watchdog_main_stale_hours=int(os.getenv("PROCESS_WATCHDOG_MAIN_STALE_HOURS", "6")),
        process_watchdog_discovery_stale_hours=int(os.getenv("PROCESS_WATCHDOG_DISCOVERY_STALE_HOURS", "6")),
        process_watchdog_prospect_leads_stale_hours=int(os.getenv("PROCESS_WATCHDOG_PROSPECT_LEADS_STALE_HOURS", "6")),
        process_watchdog_snapshot_stale_hours=int(os.getenv("PROCESS_WATCHDOG_SNAPSHOT_STALE_HOURS", "8")),
        process_watchdog_campaign_monitor_stale_hours=int(os.getenv("PROCESS_WATCHDOG_CAMPAIGN_MONITOR_STALE_HOURS", "8")),
        process_watchdog_running_grace_minutes=int(os.getenv("PROCESS_WATCHDOG_RUNNING_GRACE_MINUTES", "90")),
        process_watchdog_job_name=os.getenv("PROCESS_WATCHDOG_JOB_NAME", "dealer-process-watchdog"),
        process_watchdog_scheduler_name=os.getenv("PROCESS_WATCHDOG_SCHEDULER_NAME", "dealer-process-watchdog-schedule"),
        queue_manager_job_name=os.getenv("QUEUE_MANAGER_JOB_NAME", "dealer-queue-manager"),
        queue_manager_scheduler_name=os.getenv("QUEUE_MANAGER_SCHEDULER_NAME", "dealer-queue-manager-schedule"),
        validate_job_name=os.getenv("VALIDATE_JOB_NAME", "dealer-validate-worker"),
        validate_scheduler_name=os.getenv("VALIDATE_SCHEDULER_NAME", "dealer-validate-worker-schedule"),
        crawl_job_name=os.getenv("CRAWL_JOB_NAME", "dealer-crawl-worker"),
        crawl_scheduler_name=os.getenv("CRAWL_SCHEDULER_NAME", "dealer-crawl-worker-schedule"),
        gbp_job_name=os.getenv("GBP_JOB_NAME", "dealer-gbp-worker"),
        gbp_scheduler_name=os.getenv("GBP_SCHEDULER_NAME", "dealer-gbp-worker-schedule"),
        ai_job_name=os.getenv("AI_JOB_NAME", "dealer-ai-worker"),
        ai_scheduler_name=os.getenv("AI_SCHEDULER_NAME", "dealer-ai-worker-schedule"),
        contact_extract_job_name=os.getenv("CONTACT_EXTRACT_JOB_NAME", "dealer-contact-extract-worker"),
        contact_extract_scheduler_name=os.getenv(
            "CONTACT_EXTRACT_SCHEDULER_NAME",
            "dealer-contact-extract-worker-schedule",
        ),
        blocked_retry_job_name=os.getenv("BLOCKED_RETRY_JOB_NAME", "dealer-blocked-retry-worker"),
        blocked_retry_scheduler_name=os.getenv(
            "BLOCKED_RETRY_SCHEDULER_NAME",
            "dealer-blocked-retry-worker-schedule",
        ),
        lead_refresh_job_name=os.getenv("LEAD_REFRESH_JOB_NAME", "dealer-lead-refresh-worker"),
        lead_refresh_scheduler_name=os.getenv("LEAD_REFRESH_SCHEDULER_NAME", "dealer-lead-refresh-worker-schedule"),
        gbp_enrichment_enabled=os.getenv("GBP_ENRICHMENT_ENABLED", "true").lower() == "true",
        gbp_provider=os.getenv("GBP_PROVIDER", "duckduckgo_search_fallback"),
        gbp_search_endpoint=os.getenv("GBP_SEARCH_ENDPOINT", "https://html.duckduckgo.com/html/"),
        managed_fetch_enabled=os.getenv("MANAGED_FETCH_ENABLED", "false").lower() == "true",
        managed_fetch_provider=os.getenv("MANAGED_FETCH_PROVIDER", "none"),
        managed_fetch_api_key=os.getenv("MANAGED_FETCH_API_KEY"),
        managed_fetch_batch_size=int(os.getenv("MANAGED_FETCH_BATCH_SIZE", "25")),
        managed_fetch_min_blocked_attempts=int(os.getenv("MANAGED_FETCH_MIN_BLOCKED_ATTEMPTS", "3")),
        managed_fetch_cooldown_hours=int(os.getenv("MANAGED_FETCH_COOLDOWN_HOURS", "72")),
        ai_retrieval_enabled=os.getenv("AI_RETRIEVAL_ENABLED", "true").lower() == "true",
        ai_retrieval_provider_order=os.getenv("AI_RETRIEVAL_PROVIDER_ORDER", "gemini,openai"),
        ai_retrieval_min_run_interval_minutes=int(os.getenv("AI_RETRIEVAL_MIN_RUN_INTERVAL_MINUTES", "30")),
        ai_retrieval_cooldown_hours=int(os.getenv("AI_RETRIEVAL_COOLDOWN_HOURS", "72")),
        ai_retrieval_rate_limit_cooldown_minutes=int(os.getenv("AI_RETRIEVAL_RATE_LIMIT_COOLDOWN_MINUTES", "180")),
        ai_retrieval_request_delay_seconds=float(os.getenv("AI_RETRIEVAL_REQUEST_DELAY_SECONDS", "2")),
        ai_retrieval_prompt_style=os.getenv("AI_RETRIEVAL_PROMPT_STYLE", "simple_staff").strip().lower() or "simple_staff",
        gemini_enabled=os.getenv("GEMINI_ENABLED", "false").lower() == "true",
        gemini_api_key=os.getenv("GEMINI_API_KEY"),
        gemini_model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        openai_enabled=os.getenv("OPENAI_ENABLED", "false").lower() == "true",
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5.4"),
        cloud_run_region=os.getenv("CLOUD_RUN_REGION", "us-central1"),
        cloud_run_job_name=os.getenv("CLOUD_RUN_JOB_NAME", "dealer-contact-worker"),
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "12")),
        browser_timeout_seconds=int(os.getenv("BROWSER_TIMEOUT_SECONDS", "30")),
        blocked_retry_short_cooldown_hours=int(os.getenv("BLOCKED_RETRY_SHORT_COOLDOWN_HOURS", "6")),
        blocked_retry_medium_cooldown_hours=int(os.getenv("BLOCKED_RETRY_MEDIUM_COOLDOWN_HOURS", "24")),
        blocked_retry_long_cooldown_hours=int(os.getenv("BLOCKED_RETRY_LONG_COOLDOWN_HOURS", "72")),
        campaign_monitor_api_key=os.getenv("CAMPAIGN_MONITOR_API_KEY"),
        campaign_monitor_client_id=os.getenv("CAMPAIGN_MONITOR_CLIENT_ID"),
        campaign_monitor_master_list_name=os.getenv(
            "CAMPAIGN_MONITOR_MASTER_LIST_NAME",
            "Automation All Subscribers",
        ),
        meta_access_token=os.getenv("META_ACCESS_TOKEN"),
        meta_ad_account_id=os.getenv("META_AD_ACCOUNT_ID"),
        google_ads_developer_token=os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
        google_ads_customer_id=os.getenv("GOOGLE_ADS_CUSTOMER_ID"),
        google_application_credentials=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        gld_accountability_project_id=os.getenv("GLD_ACCOUNTABILITY_PROJECT_ID", "productivity-project-491503"),
        client_dim_dataset=os.getenv("CLIENT_DIM_DATASET", "accountability_v1"),
        client_dim_table=os.getenv("CLIENT_DIM_TABLE", "dim_clients"),
        client_dim_enabled=os.getenv("CLIENT_DIM_ENABLED", "false").lower() == "true",
        client_dim_client_id_column=os.getenv("CLIENT_DIM_CLIENT_ID_COLUMN", "client_key"),
        client_dim_account_owner_column=os.getenv("CLIENT_DIM_ACCOUNT_OWNER_COLUMN", ""),
        client_dim_account_key_column=os.getenv("CLIENT_DIM_ACCOUNT_KEY_COLUMN", "account_key"),
        client_dim_domain_column=os.getenv("CLIENT_DIM_DOMAIN_COLUMN", ""),
        client_dim_name_column=os.getenv("CLIENT_DIM_NAME_COLUMN", "normalized_client_name"),
        client_dim_city_column=os.getenv("CLIENT_DIM_CITY_COLUMN", ""),
        client_dim_state_column=os.getenv("CLIENT_DIM_STATE_COLUMN", ""),
        client_dim_email_domain_column=os.getenv("CLIENT_DIM_EMAIL_DOMAIN_COLUMN", ""),
        client_dim_email_column=os.getenv("CLIENT_DIM_EMAIL_COLUMN", ""),
        client_dim_current_client_flag_column=os.getenv("CLIENT_DIM_CURRENT_CLIENT_FLAG_COLUMN", "active_flag"),
    )
