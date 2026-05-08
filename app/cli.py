"""Command line interface for the dealer contact system pipeline."""

from __future__ import annotations

import argparse
import json

from app.bigquery_client import get_bigquery_client
from app.bigquery_repository import BigQueryRepository
from app.config import Settings, get_settings
from app.dashboard_service import DashboardService
from app.dashboard_web import run_dashboard
from app.logging_utils import configure_logging, get_logger
from app.schema_manager import SchemaManager
from app.source_catalog import SOURCE_FEEDS
from app.services.account_enrichment import AccountEnrichmentService
from app.services.ai_retrieval import AiRetrievalService
from app.services.browser_retry import BrowserRetryService
from app.services.campaign_monitor import CampaignMonitorService
from app.services.client_dim import ClientDimService
from app.services.contact_extraction import ContactExtractionService
from app.services.dealer_validation import DealerValidationService
from app.services.domain_discovery import DomainDiscoveryService
from app.services.external_seed_import import ExternalSeedImportService
from app.services.external_seed_promotion import ExternalSeedPromotionService
from app.services.gbp_enrichment import GbpEnrichmentService
from app.services.low_risk_enrichment import LowRiskEnrichmentService
from app.services.managed_fetch import ManagedFetchService
from app.services.normalization import NormalizationService
from app.services.process_watchdog import ProcessWatchdogService
from app.services.prospect_leads import ProspectLeadService
from app.services.parallel_enrichment import (
    LANE_AI,
    LANE_BLOCKED_RETRY,
    LANE_CONTACT_EXTRACT,
    LANE_CRAWL,
    LANE_GBP,
    LANE_LEAD_REFRESH,
    LANE_VALIDATE,
    LANES,
    ParallelLaneManagerService,
    ParallelLaneWorkerService,
)
from app.services.work_queue import TASK_TYPES, WorkQueueService


logger = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI parser."""

    parser = argparse.ArgumentParser(
        description="Dealer Contact System pipeline CLI.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "setup",
        help="Create or verify canonical BigQuery tables.",
    )

    normalize_parser = subparsers.add_parser(
        "normalize",
        help="Normalize contact_master into canonical BigQuery tables.",
    )
    normalize_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the normalization inputs without writing to BigQuery.",
    )

    enrich_parser = subparsers.add_parser(
        "enrich-accounts",
        help="Confirm dealer websites and enrich dealer account fields from public pages.",
    )
    enrich_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the account enrichment batch without writing to BigQuery.",
    )
    enrich_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Override the default enrichment batch size for a single run.",
    )
    enrich_parser.add_argument(
        "--account-key",
        action="append",
        default=None,
        help="Re-enrich one or more specific account_key values.",
    )

    extract_parser = subparsers.add_parser(
        "extract-contacts",
        help="Extract manager, operations, and executive contacts from confirmed dealer websites.",
    )
    extract_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the contact extraction batch without writing to BigQuery.",
    )
    extract_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Override the default extraction batch size for a single run.",
    )
    extract_parser.add_argument(
        "--account-key",
        action="append",
        default=None,
        help="Extract contacts for one or more specific account_key values.",
    )

    validate_parser = subparsers.add_parser(
        "validate-dealers",
        help="Classify accounts as dealers, dealer groups, vendors, OEMs, non-dealers, or unknown.",
    )
    validate_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the dealer validation batch without writing to BigQuery.",
    )
    validate_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Override the default validation batch size for a single run.",
    )
    validate_parser.add_argument(
        "--account-key",
        action="append",
        default=None,
        help="Validate one or more specific account_key values.",
    )

    browser_retry_parser = subparsers.add_parser(
        "retry-blocked-sites",
        help="Retry blocked dealer websites with a browser-backed fetch lane.",
    )
    browser_retry_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the blocked-site browser retry queue without writing to BigQuery.",
    )
    browser_retry_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Override the default blocked-site retry batch size for a single run.",
    )
    browser_retry_parser.add_argument(
        "--account-key",
        action="append",
        default=None,
        help="Retry one or more specific blocked account_key values.",
    )

    seed_queue_parser = subparsers.add_parser(
        "seed-work-queue",
        help="Seed queue rows for one task or all worker task types.",
    )
    seed_queue_parser.add_argument(
        "--task-type",
        choices=TASK_TYPES + ["all"],
        default="all",
        help="Which queue to seed. Defaults to all worker task types.",
    )

    worker_parser = subparsers.add_parser(
        "run-worker",
        help="Claim a batch from the BigQuery work queue and process it.",
    )
    worker_parser.add_argument(
        "--task-type",
        required=True,
        choices=TASK_TYPES,
        help="Which queue-backed worker to run.",
    )
    worker_parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Override the default queue worker batch size for this run.",
    )
    worker_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview queue counts without claiming or processing work.",
    )

    cycle_parser = subparsers.add_parser(
        "run-queue-cycle",
        help="Run one full queue-driven worker cycle for scheduled Cloud Run jobs.",
    )
    cycle_parser.add_argument(
        "--seed",
        action="store_true",
        help="Seed all queue task types before running the cycle.",
    )
    cycle_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the cycle without processing queue items.",
    )

    discovery_seed_parser = subparsers.add_parser(
        "seed-domain-discovery",
        help="Seed discovery-only search tasks into the separate domain discovery queue.",
    )
    discovery_seed_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview discovery queue seeding without writing queue rows.",
    )

    discovery_worker_parser = subparsers.add_parser(
        "run-domain-discovery-worker",
        help="Claim and process one batch from the separate domain discovery queue.",
    )
    discovery_worker_parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Override the default domain discovery worker batch size for this run.",
    )
    discovery_worker_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the discovery worker without processing queued tasks.",
    )

    discovery_cycle_parser = subparsers.add_parser(
        "run-domain-discovery-cycle",
        help="Run one full discovery-only cycle for the separate Cloud Run discovery job.",
    )
    discovery_cycle_parser.add_argument(
        "--seed",
        action="store_true",
        help="Seed the discovery queue before running the discovery cycle.",
    )
    discovery_cycle_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the discovery cycle without processing queued tasks or promotions.",
    )
    lane_worker_parser = subparsers.add_parser(
        "run-lane-worker",
        help="Run one parallel enrichment lane worker against its dedicated queue.",
    )
    lane_worker_parser.add_argument(
        "--lane",
        choices=list(LANES),
        required=True,
        help="Lane name to process.",
    )
    lane_worker_parser.add_argument(
        "--seed",
        action="store_true",
        help="Seed the lane queue before claiming work.",
    )
    lane_worker_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the lane worker without processing queue items.",
    )
    lane_seed_parser = subparsers.add_parser(
        "seed-lane-queues",
        help="Seed one or all parallel enrichment lane queues from canonical state.",
    )
    lane_seed_parser.add_argument(
        "--lane",
        choices=["all", *LANES],
        default="all",
        help="Lane queue to seed.",
    )
    lane_seed_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview seed counts without writing queue rows.",
    )
    lane_manager_parser = subparsers.add_parser(
        "run-lane-manager",
        help="Run the parallel lane manager safety-net seed and cutover audit.",
    )
    lane_manager_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview lane manager actions without mutating queue state.",
    )
    parallel_setup_parser = subparsers.add_parser(
        "setup-parallel-enrichment",
        help="Create only the distributed-worker queue and lane-state tables.",
    )

    discovery_promote_parser = subparsers.add_parser(
        "promote-discovered-domains",
        help="Promote truly new discovered domain candidates into the canonical intake path.",
    )
    discovery_promote_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview domain candidate promotion without writing canonical rows.",
    )
    discovery_promote_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Override the default discovered-domain promotion batch size for this run.",
    )

    subparsers.add_parser(
        "report-domain-discovery",
        help="Print queue and candidate counts for the separate domain discovery system.",
    )

    subparsers.add_parser(
        "report",
        help="Print source and canonical BigQuery table counts.",
    )

    subparsers.add_parser(
        "serve-dashboard",
        help="Run the one-page dashboard UI locally.",
    )

    subparsers.add_parser(
        "capture-dashboard-snapshot",
        help="Store one dashboard metrics snapshot for before/after reporting.",
    )
    subparsers.add_parser(
        "list-source-feeds",
        help="Show the catalog of external CSV seed feeds and their intended segmentation metadata.",
    )
    import_seeds_parser = subparsers.add_parser(
        "import-external-seeds",
        help="One-time import of external CSV seed files into the raw external_seed_contacts table.",
    )
    import_seeds_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the external CSV seed import without writing any BigQuery rows.",
    )
    promote_seeds_parser = subparsers.add_parser(
        "promote-external-seeds",
        help="Promote raw external seed contacts into canonical account/contact tables.",
    )
    promote_seeds_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the external seed promotion without writing canonical rows.",
    )
    low_risk_parser = subparsers.add_parser(
        "run-low-risk-enrichment",
        help="Apply cheap, staged enrichment and activation readiness labels to canonical records.",
    )
    low_risk_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview low-risk enrichment counts without writing any updates.",
    )
    gbp_parser = subparsers.add_parser(
        "refresh-gbp-enrichment",
        help="Enrich dealer accounts with fallback GBP-style phone and address signals.",
    )
    gbp_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview GBP enrichment without writing any updates.",
    )
    gbp_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Override the default GBP enrichment batch size for this run.",
    )
    gbp_parser.add_argument(
        "--account-key",
        action="append",
        default=None,
        help="Enrich one or more specific account_key values with GBP fallback data.",
    )
    ai_parser = subparsers.add_parser(
        "refresh-ai-account-facts",
        help="Use configured AI providers to retrieve structured account facts for protected dealer sites.",
    )
    ai_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview AI account-facts retrieval without writing any updates.",
    )
    ai_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Override the default AI retrieval batch size for this run.",
    )
    ai_parser.add_argument(
        "--account-key",
        action="append",
        default=None,
        help="Retrieve AI account facts for one or more specific account_key values.",
    )
    ai_parser.add_argument(
        "--prompt-style",
        choices=["structured", "simple_staff"],
        default=None,
        help="Override the AI prompt style for this run.",
    )
    prospect_leads_parser = subparsers.add_parser(
        "refresh-prospect-leads",
        help="Refresh the materialized prospect lead table from canonical data.",
    )
    prospect_leads_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the prospect lead refresh without rebuilding the table.",
    )
    client_dim_parser = subparsers.add_parser(
        "refresh-client-dim",
        help="Refresh client DIM matches and suppression flags into the prospect lead table.",
    )
    client_dim_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the client DIM refresh without writing suppression updates.",
    )
    managed_fetch_parser = subparsers.add_parser(
        "refresh-managed-fetch",
        help="Mark hard blocked dealer sites as eligible for managed anti-bot escalation.",
    )
    managed_fetch_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview managed-fetch eligibility without writing escalation state.",
    )
    watchdog_parser = subparsers.add_parser(
        "run-process-watchdog",
        help="Check nightly process freshness and auto-restart stale pipeline jobs when needed.",
    )
    watchdog_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview watchdog actions without refreshing materializations or triggering Cloud Run jobs.",
    )

    campaign_monitor_parser = subparsers.add_parser(
        "check-campaign-monitor",
        help="Check Campaign Monitor API connectivity and record the latest status.",
    )
    campaign_monitor_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test the connection logic without writing the result to BigQuery.",
    )

    campaign_monitor_structure_parser = subparsers.add_parser(
        "ensure-campaign-monitor-structure",
        help="Create the master Campaign Monitor list, required custom fields, and OEM segments.",
    )
    campaign_monitor_structure_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the Campaign Monitor structure changes without creating anything.",
    )

    campaign_monitor_sync_parser = subparsers.add_parser(
        "sync-campaign-monitor",
        help="Sync a deduped batch of subscribers into the Campaign Monitor master list.",
    )
    campaign_monitor_sync_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the Campaign Monitor subscriber sync without sending data.",
    )
    campaign_monitor_sync_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of subscribers to sync in one batch.",
    )

    return parser


def main() -> None:
    """Run the requested CLI command."""

    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    settings = get_settings()
    repository = BigQueryRepository(get_bigquery_client())
    schema_manager = SchemaManager(repository, settings)
    normalization_service = NormalizationService(repository, settings)
    account_enrichment_service = AccountEnrichmentService(repository, settings)
    contact_extraction_service = ContactExtractionService(repository, settings)
    dealer_validation_service = DealerValidationService(repository, settings)
    browser_retry_service = BrowserRetryService(repository, settings)
    work_queue_service = WorkQueueService(repository, settings)
    dashboard_service = DashboardService(repository, settings)
    campaign_monitor_service = CampaignMonitorService(repository, settings)
    external_seed_import_service = ExternalSeedImportService(repository, settings)
    external_seed_promotion_service = ExternalSeedPromotionService(repository, settings)
    low_risk_enrichment_service = LowRiskEnrichmentService(repository, settings)
    gbp_enrichment_service = GbpEnrichmentService(repository, settings)
    ai_retrieval_service = AiRetrievalService(repository, settings)
    prospect_lead_service = ProspectLeadService(repository, settings)
    client_dim_service = ClientDimService(repository, settings)
    managed_fetch_service = ManagedFetchService(repository, settings)
    domain_discovery_service = DomainDiscoveryService(repository, settings)
    parallel_lane_manager = ParallelLaneManagerService(repository, settings)
    parallel_lane_worker = ParallelLaneWorkerService(repository, settings)
    process_watchdog_service = ProcessWatchdogService(
        repository,
        settings,
        prospect_lead_service,
        client_dim_service,
        dashboard_service,
        campaign_monitor_service,
    )

    logger.info(
        "Starting command | environment=%s | project=%s | dataset=%s | command=%s",
        settings.environment,
        settings.bigquery_project_id,
        settings.bigquery_dataset,
        args.command,
    )

    if args.command == "setup":
        schema_manager.ensure_tables()
        logger.info("Schema setup complete.")
        return

    if args.command == "normalize":
        schema_manager.ensure_tables()
        normalization_service.normalize(dry_run=args.dry_run)
        logger.info("Normalization command complete.")
        return

    if args.command == "enrich-accounts":
        schema_manager.ensure_tables()
        account_enrichment_service.enrich(
            dry_run=args.dry_run,
            limit=args.limit,
            account_keys=args.account_key,
        )
        logger.info("Account enrichment command complete.")
        return

    if args.command == "extract-contacts":
        schema_manager.ensure_tables()
        contact_extraction_service.extract(
            dry_run=args.dry_run,
            limit=args.limit,
            account_keys=args.account_key,
        )
        logger.info("Contact extraction command complete.")
        return

    if args.command == "validate-dealers":
        schema_manager.ensure_tables()
        dealer_validation_service.validate(
            dry_run=args.dry_run,
            limit=args.limit,
            account_keys=args.account_key,
        )
        logger.info("Dealer validation command complete.")
        return

    if args.command == "retry-blocked-sites":
        schema_manager.ensure_tables()
        browser_retry_service.retry(
            dry_run=args.dry_run,
            limit=args.limit,
            account_keys=args.account_key,
        )
        logger.info("Browser retry command complete.")
        return

    if args.command == "seed-work-queue":
        schema_manager.ensure_tables()
        work_queue_service.seed(task_type=args.task_type)
        logger.info("Work queue seed command complete.")
        return

    if args.command == "run-worker":
        work_queue_service.run_worker(
            task_type=args.task_type,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )
        logger.info("Queue worker command complete.")
        return

    if args.command == "run-queue-cycle":
        if args.seed:
            work_queue_service.seed(task_type="all")
        work_queue_service.run_worker(
            task_type="validate",
            batch_size=settings.validate_worker_batch_size,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            low_risk_enrichment_service.run(dry_run=False)
        work_queue_service.run_worker(
            task_type="enrich",
            batch_size=settings.enrich_worker_batch_size,
            dry_run=args.dry_run,
        )
        work_queue_service.run_worker(
            task_type="enrich_gbp",
            batch_size=settings.gbp_worker_batch_size,
            dry_run=args.dry_run,
        )
        work_queue_service.run_worker(
            task_type="retry_blocked",
            batch_size=settings.retry_blocked_worker_batch_size,
            dry_run=args.dry_run,
        )
        work_queue_service.run_worker(
            task_type="ai_account_facts",
            batch_size=settings.ai_retrieval_batch_size,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            managed_fetch_service.refresh(dry_run=False)
        work_queue_service.run_worker(
            task_type="extract_contacts",
            batch_size=settings.extract_worker_batch_size,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            prospect_lead_service.refresh(dry_run=False)
            client_dim_service.refresh(dry_run=False)
            dashboard_service.capture_snapshot()
            campaign_monitor_service.check_connection(dry_run=False)
            if settings.campaign_monitor_sync_enabled:
                campaign_monitor_service.sync_subscribers(
                    dry_run=False,
                    limit=settings.campaign_monitor_sync_batch_size,
                )
        logger.info("Queue cycle command complete.")
        return

    if args.command == "seed-domain-discovery":
        seeded = domain_discovery_service.seed_queue(dry_run=args.dry_run)
        print(f"Discovery tasks {'would be seeded' if args.dry_run else 'seeded'}: {seeded}")
        logger.info("Domain discovery seed command complete.")
        return

    if args.command == "run-domain-discovery-worker":
        result = domain_discovery_service.run_worker(
            dry_run=args.dry_run,
            batch_size=args.batch_size,
        )
        print(f"Discovery worker status: {result.status}")
        print(result.detail)
        print(f"Processed tasks: {result.processed_tasks}")
        print(f"Candidates written: {result.candidates_written}")
        logger.info("Domain discovery worker command complete.")
        return

    if args.command == "run-domain-discovery-cycle":
        result = domain_discovery_service.run_cycle(seed=args.seed, dry_run=args.dry_run)
        print(f"Discovery cycle status: {result.status}")
        print(result.detail)
        print(f"Processed tasks: {result.processed_tasks}")
        print(f"Candidates written: {result.candidates_written}")
        print(f"Promoted candidates: {result.promoted_candidates}")
        logger.info("Domain discovery cycle command complete.")
        return

    if args.command == "seed-lane-queues":
        if args.lane == "all":
            counts = parallel_lane_manager.seed_all(dry_run=args.dry_run)
            print(json.dumps(counts, indent=2, sort_keys=True))
        else:
            count = parallel_lane_manager.seed_lane(args.lane, dry_run=args.dry_run)
            print(f"{args.lane}: {count}")
        logger.info("Lane queue seed command complete.")
        return

    if args.command == "run-lane-manager":
        result = parallel_lane_manager.run_manager(dry_run=args.dry_run)
        logger.info("Lane manager complete | status=%s | detail=%s", result.status, result.detail)
        print(f"Lane manager status: {result.status}")
        print(result.detail)
        print(json.dumps(result.seeded_counts, indent=2, sort_keys=True))
        print(f"Duplicate active items: {result.duplicate_active_items}")
        return

    if args.command == "run-lane-worker":
        result = parallel_lane_worker.run_lane(lane=args.lane, dry_run=args.dry_run, seed=args.seed)
        logger.info("Lane worker complete | lane=%s | status=%s | detail=%s", args.lane, result["status"], result["detail"])
        print(f"Lane worker status: {result['status']}")
        print(result["detail"])
        print(f"Lane: {result['lane']}")
        print(f"Claimed: {result['claimed_count']}")
        print(f"Succeeded: {result['succeeded_count']}")
        print(f"Failed: {result['failed_count']}")
        return

    if args.command == "setup-parallel-enrichment":
        schema_manager.ensure_parallel_worker_tables()
        logger.info("Parallel enrichment table setup complete.")
        return

    if args.command == "promote-discovered-domains":
        result = domain_discovery_service.promote_candidates(dry_run=args.dry_run, limit=args.limit)
        print(f"Domain promotion status: {result.status}")
        print(result.detail)
        print(f"Promoted candidates: {result.promoted_candidates}")
        logger.info("Domain discovery promotion command complete.")
        return

    if args.command == "report-domain-discovery":
        report = domain_discovery_service.report()
        print(f"Queued discovery tasks: {report.get('queued_tasks', 0)}")
        print(f"Total discovery candidates: {report.get('total_candidates', 0)}")
        print(f"New candidates: {report.get('new_candidates', 0)}")
        print(f"Duplicate existing candidates: {report.get('duplicate_existing_candidates', 0)}")
        print(f"Promoted candidates: {report.get('promoted_candidates', 0)}")
        print(f"Rejected candidates: {report.get('rejected_candidates', 0)}")
        logger.info("Domain discovery report command complete.")
        return

    if args.command == "report":
        print_report(repository, settings)
        return

    if args.command == "serve-dashboard":
        run_dashboard()
        return

    if args.command == "capture-dashboard-snapshot":
        schema_manager.ensure_tables()
        dashboard_service.capture_snapshot()
        logger.info("Dashboard snapshot capture complete.")
        return

    if args.command == "list-source-feeds":
        logger.info("Configured external source feeds: %s", len(SOURCE_FEEDS))
        for feed in SOURCE_FEEDS:
            print(
                " | ".join(
                    [
                        feed.file_name,
                        f"audience={feed.audience_type}",
                        f"market={feed.market}",
                        f"country={feed.country}",
                        f"brand={feed.inferred_brand or 'mixed'}",
                        f"group={feed.source_group}",
                        f"notes={feed.notes}",
                    ]
                )
            )
        return

    if args.command == "import-external-seeds":
        schema_manager.ensure_tables()
        summaries = external_seed_import_service.import_catalog(dry_run=args.dry_run)
        print("External seed import summary:")
        for summary in summaries:
            print(
                " | ".join(
                    [
                        summary.file_name,
                        f"total_rows={summary.total_rows}",
                        f"valid_email_rows={summary.valid_email_rows}",
                        f"personal_email_rows={summary.personal_email_rows}",
                        f"existing_raw_rows={summary.existing_raw_rows}",
                        f"would_insert_rows={summary.would_insert_rows}",
                        f"audience={summary.audience_type}",
                        f"market={summary.market}",
                        f"country={summary.country}",
                        f"brand={summary.inferred_brand or 'mixed'}",
                    ]
                )
            )
        return

    if args.command == "promote-external-seeds":
        schema_manager.ensure_tables()
        external_seed_promotion_service.promote(dry_run=args.dry_run)
        logger.info("External seed promotion command complete.")
        return

    if args.command == "run-low-risk-enrichment":
        schema_manager.ensure_tables()
        low_risk_enrichment_service.run(dry_run=args.dry_run)
        logger.info("Low-risk enrichment command complete.")
        return

    if args.command == "refresh-gbp-enrichment":
        schema_manager.ensure_tables()
        result = gbp_enrichment_service.enrich(
            dry_run=args.dry_run,
            limit=args.limit,
            account_keys=args.account_key,
        )
        logger.info("GBP enrichment command complete | status=%s | detail=%s", result.status, result.detail)
        print(f"GBP enrichment status: {result.status}")
        print(result.detail)
        print(f"Enriched accounts: {result.enriched_accounts}")
        return

    if args.command == "refresh-ai-account-facts":
        schema_manager.ensure_tables()
        result = ai_retrieval_service.refresh_account_facts(
            dry_run=args.dry_run,
            limit=args.limit,
            account_keys=args.account_key,
            prompt_style=args.prompt_style,
        )
        logger.info("AI account-facts command complete | status=%s | detail=%s", result.status, result.detail)
        print(f"AI account-facts status: {result.status}")
        print(result.detail)
        print(f"Processed accounts: {result.processed_accounts}")
        print(f"Enriched accounts: {result.enriched_accounts}")
        return

    if args.command == "refresh-prospect-leads":
        schema_manager.ensure_tables()
        result = prospect_lead_service.refresh(dry_run=args.dry_run)
        logger.info("Prospect lead refresh complete | status=%s | detail=%s", result.status, result.detail)
        print(f"Prospect leads status: {result.status}")
        print(result.detail)
        print(f"Lead rows: {result.lead_count}")
        return

    if args.command == "refresh-client-dim":
        schema_manager.ensure_tables()
        result = client_dim_service.refresh(dry_run=args.dry_run)
        logger.info(
            "Client DIM refresh complete | status=%s | matched=%s | suppressed=%s | detail=%s",
            result.status,
            result.matched_leads,
            result.suppressed_leads,
            result.detail,
        )
        print(f"Client DIM status: {result.status}")
        print(result.detail)
        print(f"Matched leads: {result.matched_leads}")
        print(f"Suppressed leads: {result.suppressed_leads}")
        return

    if args.command == "refresh-managed-fetch":
        schema_manager.ensure_tables()
        result = managed_fetch_service.refresh(dry_run=args.dry_run)
        logger.info(
            "Managed fetch refresh complete | status=%s | eligible=%s | detail=%s",
            result.status,
            result.eligible_accounts,
            result.detail,
        )
        print(f"Managed fetch status: {result.status}")
        print(result.detail)
        print(f"Eligible accounts: {result.eligible_accounts}")
        return

    if args.command == "run-process-watchdog":
        result = process_watchdog_service.run(dry_run=args.dry_run)
        logger.info("Process watchdog complete | status=%s | detail=%s", result.status, result.detail)
        print(f"Process watchdog status: {result.status}")
        print(result.detail)
        print(f"Main job triggered: {result.main_job_triggered}")
        print(f"Discovery job triggered: {result.discovery_job_triggered}")
        print(f"Materializations repaired: {result.repaired_materializations}")
        for check in result.checks:
            print(f"- {check.name}: {check.status} | {check.detail} | action={check.action}")
        return

    if args.command == "check-campaign-monitor":
        schema_manager.ensure_tables()
        result = campaign_monitor_service.check_connection(dry_run=args.dry_run)
        logger.info("Campaign Monitor check complete | status=%s | detail=%s", result.status, result.detail)
        print(f"Campaign Monitor status: {result.status}")
        print(result.detail)
        return

    if args.command == "ensure-campaign-monitor-structure":
        schema_manager.ensure_tables()
        result = campaign_monitor_service.ensure_master_list_and_segments(dry_run=args.dry_run)
        logger.info(
            "Campaign Monitor structure check complete | status=%s | list_id=%s | created_segments=%s | existing_segments=%s",
            result.status,
            result.list_id,
            result.created_segments,
            result.existing_segments,
        )
        print(f"Campaign Monitor structure status: {result.status}")
        print(result.detail)
        if result.list_id:
            print(f"Master list ID: {result.list_id}")
        print(f"Created segments: {result.created_segments}")
        print(f"Existing segments: {result.existing_segments}")
        return

    if args.command == "sync-campaign-monitor":
        schema_manager.ensure_tables()
        result = campaign_monitor_service.sync_subscribers(
            dry_run=args.dry_run,
            limit=args.limit,
        )
        logger.info(
            "Campaign Monitor subscriber sync complete | status=%s | list_id=%s | submitted=%s | new=%s | existing=%s | failed=%s",
            result.status,
            result.list_id,
            result.submitted_count,
            result.new_subscribers,
            result.existing_subscribers,
            result.failed_count,
        )
        print(f"Campaign Monitor subscriber sync status: {result.status}")
        print(result.detail)
        if result.list_id:
            print(f"Master list ID: {result.list_id}")
        print(f"Submitted: {result.submitted_count}")
        print(f"New subscribers: {result.new_subscribers}")
        print(f"Existing subscribers updated: {result.existing_subscribers}")
        print(f"Failures: {result.failed_count}")
        return


def print_report(repository: BigQueryRepository, settings: Settings) -> None:
    """Print health checks for the source and canonical tables."""

    query = f"""
    SELECT
      (SELECT COUNT(*) FROM `{settings.source_contact_table_fqn}`) AS source_contacts,
      (SELECT COUNT(DISTINCT account_key) FROM `{settings.source_contact_table_fqn}`) AS source_distinct_accounts,
      (SELECT COUNTIF(is_personal_email) FROM `{settings.source_contact_table_fqn}`) AS source_personal_emails,
      (SELECT COUNT(*) FROM `{settings.dealer_accounts_table_fqn}`) AS dealer_accounts,
      (SELECT COUNTIF(website_url IS NOT NULL AND TRIM(website_url) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS enriched_websites,
      (SELECT COUNTIF(website_url IS NOT NULL AND TRIM(website_url) != '' AND dealer_classification IN ('dealer', 'dealer_group')) FROM `{settings.dealer_accounts_table_fqn}`) AS validated_websites,
      (SELECT COUNTIF(source_type = 'website_resolution') FROM `{settings.dealer_accounts_table_fqn}`) AS resolution_only_websites,
      (SELECT COUNTIF(fetch_status = 'success' AND dealer_classification IN ('dealer', 'dealer_group')) FROM `{settings.dealer_accounts_table_fqn}`) AS successful_fetch_accounts,
      (SELECT COUNTIF(fetch_status = 'blocked' AND dealer_classification IN ('dealer', 'dealer_group')) FROM `{settings.dealer_accounts_table_fqn}`) AS blocked_fetch_accounts,
      (SELECT COUNTIF(fetch_status = 'unavailable' AND dealer_classification IN ('dealer', 'dealer_group')) FROM `{settings.dealer_accounts_table_fqn}`) AS unavailable_fetch_accounts,
      (SELECT COUNTIF(
        website_url IS NOT NULL
        AND TRIM(website_url) != ''
        AND dealer_classification IN ('dealer', 'dealer_group')
        AND source_type = 'website_resolution'
        AND IFNULL(account_name, '') = ''
        AND IFNULL(inferred_brand, '') = ''
        AND (IFNULL(account_city, '') = '' OR IFNULL(account_state, '') = '')
      ) FROM `{settings.dealer_accounts_table_fqn}`) AS blocked_or_unparsed_websites,
      (SELECT COUNTIF(account_name IS NOT NULL AND TRIM(account_name) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS enriched_names,
      (SELECT COUNTIF(inferred_brand IS NOT NULL AND TRIM(inferred_brand) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS enriched_brands,
      (SELECT COUNTIF(account_city IS NOT NULL AND account_state IS NOT NULL) FROM `{settings.dealer_accounts_table_fqn}`) AS enriched_locations,
      (SELECT COUNTIF(dealer_classification = 'dealer') FROM `{settings.dealer_accounts_table_fqn}`) AS validated_dealers,
      (SELECT COUNTIF(dealer_classification = 'dealer_group') FROM `{settings.dealer_accounts_table_fqn}`) AS validated_dealer_groups,
      (SELECT COUNTIF(account_phone IS NOT NULL AND TRIM(account_phone) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS accounts_with_phone,
      (SELECT COUNTIF(website_phone IS NOT NULL AND TRIM(website_phone) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS accounts_with_website_phone,
      (SELECT COUNTIF(gbp_phone IS NOT NULL AND TRIM(gbp_phone) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS accounts_with_gbp_phone,
      (SELECT COUNTIF(ai_phone IS NOT NULL AND TRIM(ai_phone) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS accounts_with_ai_phone,
      (SELECT COUNTIF(gbp_address_line IS NOT NULL AND TRIM(gbp_address_line) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS accounts_with_gbp_address,
      (SELECT COUNTIF(ai_address_line IS NOT NULL AND TRIM(ai_address_line) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS accounts_with_ai_address,
      (SELECT COUNTIF(best_phone_source = 'gbp' AND best_phone IS NOT NULL AND TRIM(best_phone) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS accounts_with_best_phone_from_gbp,
      (SELECT COUNTIF(best_phone_source = 'ai' AND best_phone IS NOT NULL AND TRIM(best_phone) != '') FROM `{settings.dealer_accounts_table_fqn}`) AS accounts_with_best_phone_from_ai,
      (SELECT COUNTIF(activation_status = 'activation_ready') FROM `{settings.dealer_accounts_table_fqn}`) AS activation_ready_accounts,
      (SELECT COUNT(*) FROM `{settings.marketing_ready_contacts_view_fqn}`) AS marketing_ready_contacts,
      (SELECT COUNT(*) FROM `{settings.sales_ready_leads_view_fqn}`) AS sales_ready_leads,
      (SELECT COUNTIF(dealer_classification = 'vendor') FROM `{settings.dealer_accounts_table_fqn}`) AS validated_vendors,
      (SELECT COUNTIF(dealer_classification = 'non_dealer') FROM `{settings.dealer_accounts_table_fqn}`) AS validated_non_dealers,
      (SELECT COUNTIF(dealer_classification = 'oem') FROM `{settings.dealer_accounts_table_fqn}`) AS validated_oems,
      (SELECT COUNTIF(dealer_classification = 'unknown') FROM `{settings.dealer_accounts_table_fqn}`) AS validated_unknowns,
      (SELECT COUNT(*) FROM `{settings.prospect_contacts_table_fqn}`) AS prospect_contacts,
      (SELECT COUNTIF(email_quality = 'website_observed') FROM `{settings.prospect_contacts_table_fqn}`) AS observed_email_contacts,
      (SELECT COUNTIF(email_quality = 'ai_inferred') FROM `{settings.prospect_contacts_table_fqn}`) AS inferred_email_contacts,
      (SELECT COUNT(*) FROM `{settings.prospect_leads_table_fqn}`) AS prospect_leads,
      (SELECT COUNT(*) FROM `{settings.activation_ready_contacts_view_fqn}`) AS activation_ready_contacts,
      (SELECT COUNTIF(audience_type = 'current_client') FROM `{settings.prospect_contacts_table_fqn}`) AS current_client_contacts,
      (SELECT COUNTIF(country = 'Canada') FROM `{settings.prospect_contacts_table_fqn}`) AS canada_contacts,
      (SELECT COUNTIF(dim_client_match_flag) FROM `{settings.prospect_leads_table_fqn}`) AS dim_matched_leads,
      (SELECT COUNTIF(NOT prospecting_allowed_flag) FROM `{settings.prospect_leads_table_fqn}`) AS dim_suppressed_leads,
      (SELECT COUNTIF(source_type = 'website_contact_extraction') FROM `{settings.prospect_contacts_table_fqn}`) AS website_extracted_contacts,
      (SELECT COUNTIF(managed_fetch_status = 'eligible') FROM `{settings.dealer_accounts_table_fqn}`) AS managed_fetch_eligible_accounts,
      (SELECT COUNT(*) FROM `{settings.ai_retrieval_results_table_fqn}`) AS ai_retrieval_results,
      (SELECT COUNTIF(provider_status = 'success') FROM `{settings.ai_retrieval_results_table_fqn}`) AS ai_successful_results,
      (SELECT COUNTIF(provider_status = 'failed') FROM `{settings.ai_retrieval_results_table_fqn}`) AS ai_failed_results,
      (SELECT COUNT(*) FROM `{settings.account_relationships_table_fqn}`) AS account_relationships,
      (SELECT COUNT(*) FROM `{settings.sync_targets_table_fqn}`) AS sync_targets,
      (SELECT COUNTIF(task_type = 'validate' AND status IN ('pending', 'retry')) FROM `{settings.account_work_queue_table_fqn}`) AS queued_validate,
      (SELECT COUNTIF(task_type = 'enrich' AND status IN ('pending', 'retry')) FROM `{settings.account_work_queue_table_fqn}`) AS queued_enrich,
      (SELECT COUNTIF(task_type = 'enrich_gbp' AND status IN ('pending', 'retry')) FROM `{settings.account_work_queue_table_fqn}`) AS queued_enrich_gbp,
      (SELECT COUNTIF(task_type = 'ai_account_facts' AND status IN ('pending', 'retry')) FROM `{settings.account_work_queue_table_fqn}`) AS queued_ai_account_facts,
      (SELECT COUNTIF(task_type = 'extract_contacts' AND status IN ('pending', 'retry')) FROM `{settings.account_work_queue_table_fqn}`) AS queued_extract_contacts,
      (SELECT COUNTIF(task_type = 'retry_blocked' AND status IN ('pending', 'retry')) FROM `{settings.account_work_queue_table_fqn}`) AS queued_retry_blocked,
      (SELECT COUNTIF(status = 'in_progress') FROM `{settings.account_work_queue_table_fqn}`) AS queue_in_progress,
      (SELECT COUNT(*) FROM `{settings.pipeline_runs_table_fqn}`) AS pipeline_runs,
      (SELECT COUNTIF(status IN ('pending', 'retry')) FROM `{settings.domain_discovery_queue_table_fqn}`) AS queued_discovery_tasks,
      (SELECT COUNT(*) FROM `{settings.discovered_domain_candidates_table_fqn}`) AS discovery_candidates,
      (SELECT COUNTIF(promotion_status = 'duplicate_existing') FROM `{settings.discovered_domain_candidates_table_fqn}`) AS discovery_duplicate_existing,
      (SELECT COUNTIF(promotion_status = 'promoted_to_main_pipeline') FROM `{settings.discovered_domain_candidates_table_fqn}`) AS discovery_promoted_candidates,
      (SELECT COUNTIF(STARTS_WITH(promotion_status, 'rejected')) FROM `{settings.discovered_domain_candidates_table_fqn}`) AS discovery_rejected_candidates,
      (SELECT COUNT(*) FROM `{settings.domain_discovery_runs_table_fqn}`) AS discovery_runs
    """
    report = repository.fetch_one(query)
    logger.info("Read-only report | source=%s", settings.source_contact_table_fqn)
    print("Dealer Contact System Report")
    print(f"Source contacts: {report.get('source_contacts', 0)}")
    print(f"Source distinct account keys: {report.get('source_distinct_accounts', 0)}")
    print(f"Source personal emails: {report.get('source_personal_emails', 0)}")
    print(f"Dealer accounts: {report.get('dealer_accounts', 0)}")
    print(f"Enriched websites: {report.get('enriched_websites', 0)}")
    print(f"Validated dealer/group websites: {report.get('validated_websites', 0)}")
    print(f"Resolution-only websites: {report.get('resolution_only_websites', 0)}")
    print(f"Successful dealer/group fetches: {report.get('successful_fetch_accounts', 0)}")
    print(f"Blocked dealer/group fetches: {report.get('blocked_fetch_accounts', 0)}")
    print(f"Unavailable dealer/group fetches: {report.get('unavailable_fetch_accounts', 0)}")
    print(f"Blocked or unparsed dealer websites: {report.get('blocked_or_unparsed_websites', 0)}")
    print(f"Enriched account names: {report.get('enriched_names', 0)}")
    print(f"Enriched brands: {report.get('enriched_brands', 0)}")
    print(f"Enriched locations: {report.get('enriched_locations', 0)}")
    print(f"Validated dealers: {report.get('validated_dealers', 0)}")
    print(f"Validated dealer groups: {report.get('validated_dealer_groups', 0)}")
    print(f"Accounts with phone: {report.get('accounts_with_phone', 0)}")
    print(f"Accounts with website phone: {report.get('accounts_with_website_phone', 0)}")
    print(f"Accounts with GBP phone: {report.get('accounts_with_gbp_phone', 0)}")
    print(f"Accounts with AI phone: {report.get('accounts_with_ai_phone', 0)}")
    print(f"Accounts with GBP address: {report.get('accounts_with_gbp_address', 0)}")
    print(f"Accounts with AI address: {report.get('accounts_with_ai_address', 0)}")
    print(f"Accounts with best phone from GBP: {report.get('accounts_with_best_phone_from_gbp', 0)}")
    print(f"Accounts with best phone from AI: {report.get('accounts_with_best_phone_from_ai', 0)}")
    print(f"Activation-ready accounts: {report.get('activation_ready_accounts', 0)}")
    print(f"Marketing-ready contacts: {report.get('marketing_ready_contacts', 0)}")
    print(f"Sales-ready leads: {report.get('sales_ready_leads', 0)}")
    print(f"Validated vendors: {report.get('validated_vendors', 0)}")
    print(f"Validated non-dealers: {report.get('validated_non_dealers', 0)}")
    print(f"Validated OEMs: {report.get('validated_oems', 0)}")
    print(f"Validated unknowns: {report.get('validated_unknowns', 0)}")
    print(f"Prospect contacts: {report.get('prospect_contacts', 0)}")
    print(f"Observed-email contacts: {report.get('observed_email_contacts', 0)}")
    print(f"Inferred-email contacts: {report.get('inferred_email_contacts', 0)}")
    print(f"Prospect leads: {report.get('prospect_leads', 0)}")
    print(f"Activation-ready contacts: {report.get('activation_ready_contacts', 0)}")
    print(f"Current-client contacts: {report.get('current_client_contacts', 0)}")
    print(f"Canada contacts: {report.get('canada_contacts', 0)}")
    print(f"Client DIM matched leads: {report.get('dim_matched_leads', 0)}")
    print(f"Suppressed leads: {report.get('dim_suppressed_leads', 0)}")
    print(f"Website-extracted contacts: {report.get('website_extracted_contacts', 0)}")
    print(f"Managed fetch eligible accounts: {report.get('managed_fetch_eligible_accounts', 0)}")
    print(f"AI retrieval results: {report.get('ai_retrieval_results', 0)}")
    print(f"AI successful retrievals: {report.get('ai_successful_results', 0)}")
    print(f"AI failed retrievals: {report.get('ai_failed_results', 0)}")
    print(f"Account relationships: {report.get('account_relationships', 0)}")
    print(f"Sync targets: {report.get('sync_targets', 0)}")
    print(f"Queued validate tasks: {report.get('queued_validate', 0)}")
    print(f"Queued enrich tasks: {report.get('queued_enrich', 0)}")
    print(f"Queued GBP enrich tasks: {report.get('queued_enrich_gbp', 0)}")
    print(f"Queued AI account-facts tasks: {report.get('queued_ai_account_facts', 0)}")
    print(f"Queued extract tasks: {report.get('queued_extract_contacts', 0)}")
    print(f"Queued blocked-site retry tasks: {report.get('queued_retry_blocked', 0)}")
    print(f"Queue items in progress: {report.get('queue_in_progress', 0)}")
    print(f"Pipeline runs logged: {report.get('pipeline_runs', 0)}")
    print(f"Queued discovery tasks: {report.get('queued_discovery_tasks', 0)}")
    print(f"Discovery candidates: {report.get('discovery_candidates', 0)}")
    print(f"Discovery duplicate existing: {report.get('discovery_duplicate_existing', 0)}")
    print(f"Discovery promoted candidates: {report.get('discovery_promoted_candidates', 0)}")
    print(f"Discovery rejected candidates: {report.get('discovery_rejected_candidates', 0)}")
    print(f"Discovery runs logged: {report.get('discovery_runs', 0)}")


if __name__ == "__main__":
    main()
