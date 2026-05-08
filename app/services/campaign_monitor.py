"""Campaign Monitor connection checks and structure helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class SegmentDefinition:
    """Definition for one Campaign Monitor segment and its rules."""

    title: str
    rules: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class CampaignMonitorHealthResult:
    """Simple result from one Campaign Monitor health check."""

    status: str
    detail: str


@dataclass(frozen=True)
class CampaignMonitorStructureResult:
    """Summary from one structure sync into Campaign Monitor."""

    status: str
    detail: str
    list_id: str | None
    created_segments: int
    existing_segments: int


@dataclass(frozen=True)
class CampaignMonitorSyncResult:
    """Summary from one subscriber sync batch."""

    status: str
    detail: str
    list_id: str | None
    submitted_count: int
    new_subscribers: int
    existing_subscribers: int
    failed_count: int


class CampaignMonitorService:
    """Run Campaign Monitor health checks and record them in BigQuery."""

    BASE_URL = "https://api.createsend.com/api/v3.3"
    CONNECTION_RECORD_ID = "campaign_monitor_connection"
    MASTER_LIST_RECORD_ID = "campaign_monitor_master_list"
    SEGMENT_CUSTOM_FIELDS = {
        "OEM": "Text",
        "Dealer Name": "Text",
        "City": "Text",
        "State": "Text",
        "Phone": "Text",
        "Country": "Text",
        "Market": "Text",
        "Audience Type": "Text",
        "Source List": "Text",
        "Role Family": "Text",
        "Dealer Classification": "Text",
    }
    STATIC_SEGMENT_DEFINITIONS = (
        SegmentDefinition(
            title="Automation Canada Subscribers",
            rules=(("Country", "Canada"),),
        ),
        SegmentDefinition(
            title="Automation United States Subscribers",
            rules=(("Country", "United States"),),
        ),
        SegmentDefinition(
            title="Automation Canada Current Clients",
            rules=(("Country", "Canada"), ("Audience Type", "current_client")),
        ),
        SegmentDefinition(
            title="Automation United States Current Clients",
            rules=(("Country", "United States"), ("Audience Type", "current_client")),
        ),
        SegmentDefinition(
            title="Automation Canada Prospects",
            rules=(("Country", "Canada"), ("Audience Type", "prospect")),
        ),
        SegmentDefinition(
            title="Automation United States Prospects",
            rules=(("Country", "United States"), ("Audience Type", "prospect")),
        ),
    )
    LEGACY_SEGMENT_TITLES = (
        "Automation Current Clients",
        "Automation Prospects",
    )

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings

    def check_connection(self, dry_run: bool = False) -> CampaignMonitorHealthResult:
        """Check Campaign Monitor connectivity and optionally store the result."""

        if not self.settings.campaign_monitor_api_key or not self.settings.campaign_monitor_client_id:
            result = CampaignMonitorHealthResult(
                status="failed",
                detail="Campaign Monitor credentials are missing.",
            )
            if not dry_run:
                self._record_status(result.status)
            return result

        url = (
            f"{self.BASE_URL}/clients/"
            f"{self.settings.campaign_monitor_client_id}/lists.json"
        )
        try:
            response = requests.get(
                url,
                auth=(self.settings.campaign_monitor_api_key, "x"),
                timeout=self.settings.request_timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            list_count = len(payload) if isinstance(payload, list) else 0
            result = CampaignMonitorHealthResult(
                status="success",
                detail=f"Connected successfully. Found {list_count} subscriber lists.",
            )
        except requests.RequestException as error:
            logger.warning("Campaign Monitor connection check failed: %s", error)
            result = CampaignMonitorHealthResult(
                status="failed",
                detail=f"Connection failed: {error}",
            )

        if not dry_run:
            self._record_status(result.status)
        return result

    def ensure_master_list_and_segments(
        self,
        dry_run: bool = False,
    ) -> CampaignMonitorStructureResult:
        """Ensure the master list, custom fields, and OEM segments exist."""

        if not self.settings.campaign_monitor_api_key or not self.settings.campaign_monitor_client_id:
            return CampaignMonitorStructureResult(
                status="failed",
                detail="Campaign Monitor credentials are missing.",
                list_id=None,
                created_segments=0,
                existing_segments=0,
            )

        brands = self._get_brand_values()
        if not brands:
            return CampaignMonitorStructureResult(
                status="failed",
                detail="No validated OEM brands were found in BigQuery.",
                list_id=None,
                created_segments=0,
                existing_segments=0,
            )

        lists = self._api_get(f"/clients/{self.settings.campaign_monitor_client_id}/lists.json")
        master_list = next(
            (
                item
                for item in lists
                if str(item.get("Name", "")).strip() == self.settings.campaign_monitor_master_list_name
            ),
            None,
        )

        created_list = False
        if not master_list:
            if dry_run:
                list_id = None
            else:
                payload = {
                    "Title": self.settings.campaign_monitor_master_list_name,
                    "UnsubscribePage": "",
                    "ConfirmedOptIn": False,
                    "ConfirmationSuccessPage": "",
                    "UnsubscribeSetting": "AllClientLists",
                }
                list_id = str(
                    self._api_post(
                        f"/lists/{self.settings.campaign_monitor_client_id}.json",
                        payload,
                    )
                )
                created_list = True
                master_list = {
                    "ListID": list_id,
                    "Name": self.settings.campaign_monitor_master_list_name,
                }
        else:
            list_id = str(master_list["ListID"])

        if dry_run and not master_list:
            return CampaignMonitorStructureResult(
                status="success",
                detail=(
                    f"Would create master list '{self.settings.campaign_monitor_master_list_name}' "
                    f"and {len(brands) + len(self.STATIC_SEGMENT_DEFINITIONS)} total segments."
                ),
                list_id=None,
                created_segments=len(brands) + len(self.STATIC_SEGMENT_DEFINITIONS),
                existing_segments=0,
            )

        if list_id is None:
            return CampaignMonitorStructureResult(
                status="failed",
                detail="Unable to determine a Campaign Monitor list ID.",
                list_id=None,
                created_segments=0,
                existing_segments=0,
            )

        field_map = self._ensure_custom_fields(list_id, dry_run=dry_run)
        existing_segments = self._api_get(f"/lists/{list_id}/segments.json") if not dry_run else []
        if not dry_run:
            self._remove_legacy_segments(list_id=list_id, segments=existing_segments)
            existing_segments = self._api_get(f"/lists/{list_id}/segments.json")
        existing_names = {str(item.get("Title", "")).strip() for item in existing_segments}
        created_segments = 0

        for definition in self._build_segment_definitions(
            brands=brands,
            field_map=field_map,
        ):
            segment_name = definition.title
            if segment_name in existing_names:
                continue
            created_segments += 1
            if dry_run:
                continue
            payload = {
                "Title": segment_name,
                "RuleGroups": [
                    {
                        "Rules": [
                            {
                                "RuleType": rule_type,
                                "Clause": clause,
                            }
                            for rule_type, clause in definition.rules
                        ]
                    }
                ],
            }
            self._api_post(f"/segments/{list_id}.json", payload)

        if not dry_run:
            self._record_structure_status(list_id=list_id)

        existing_segment_count = len(existing_names)
        action_bits = []
        if created_list:
            action_bits.append("created the master list")
        if created_segments:
            action_bits.append(f"created {created_segments} OEM segments")
        if not action_bits:
            action_bits.append("found the master list and all OEM segments already in place")

        return CampaignMonitorStructureResult(
            status="success",
            detail=(
                f"Campaign Monitor structure is ready: {', '.join(action_bits)}. "
                f"Current OEM brand count: {len(brands)}."
            ),
            list_id=list_id,
            created_segments=created_segments,
            existing_segments=existing_segment_count,
        )

    def sync_subscribers(
        self,
        dry_run: bool = False,
        limit: int = 100,
    ) -> CampaignMonitorSyncResult:
        """Sync a deduped batch of activation-ready subscribers into the master list."""

        if not self.settings.campaign_monitor_api_key or not self.settings.campaign_monitor_client_id:
            return CampaignMonitorSyncResult(
                status="failed",
                detail="Campaign Monitor credentials are missing.",
                list_id=None,
                submitted_count=0,
                new_subscribers=0,
                existing_subscribers=0,
                failed_count=0,
            )

        structure = self.ensure_master_list_and_segments(dry_run=False)
        if structure.status != "success" or not structure.list_id:
            return CampaignMonitorSyncResult(
                status="failed",
                detail="Campaign Monitor master list is not ready.",
                list_id=structure.list_id,
                submitted_count=0,
                new_subscribers=0,
                existing_subscribers=0,
                failed_count=0,
            )

        field_map = self._ensure_custom_fields(structure.list_id, dry_run=False)
        subscribers = self._load_sync_candidates(limit=limit)
        if not subscribers:
            return CampaignMonitorSyncResult(
                status="success",
                detail="No eligible subscribers were found for this sync batch.",
                list_id=structure.list_id,
                submitted_count=0,
                new_subscribers=0,
                existing_subscribers=0,
                failed_count=0,
            )

        if dry_run:
            return CampaignMonitorSyncResult(
                status="success",
                detail=f"Would sync {len(subscribers)} subscribers into {self.settings.campaign_monitor_master_list_name}.",
                list_id=structure.list_id,
                submitted_count=len(subscribers),
                new_subscribers=0,
                existing_subscribers=0,
                failed_count=0,
            )

        payload = {
            "Subscribers": [
                {
                    "EmailAddress": subscriber["email"],
                    "Name": subscriber["full_name"] or subscriber["email"],
                    "CustomFields": [
                        {"Key": field_map["OEM"], "Value": subscriber["oem"]},
                        {"Key": field_map["Dealer Name"], "Value": subscriber["dealer_name"]},
                        {"Key": field_map["City"], "Value": subscriber["city"]},
                        {"Key": field_map["State"], "Value": subscriber["state"]},
                        {"Key": field_map["Phone"], "Value": subscriber["phone_number"]},
                        {"Key": field_map["Country"], "Value": subscriber["country"]},
                        {"Key": field_map["Market"], "Value": subscriber["market"]},
                        {"Key": field_map["Audience Type"], "Value": subscriber["audience_type"]},
                        {"Key": field_map["Source List"], "Value": subscriber["source_list"]},
                        {"Key": field_map["Role Family"], "Value": subscriber["role_family"]},
                        {"Key": field_map["Dealer Classification"], "Value": subscriber["dealer_classification"]},
                    ],
                    "ConsentToTrack": "Unchanged",
                }
                for subscriber in subscribers
            ],
            "Resubscribe": True,
            "QueueSubscriptionBasedAutoResponders": False,
            "RestartSubscriptionBasedAutoresponders": False,
        }
        result = self._api_post(
            f"/subscribers/{structure.list_id}/import.json",
            payload,
            expected_statuses=(201, 400),
        )

        failure_details = result.get("FailureDetails", []) if isinstance(result, dict) else []
        failure_count = len(failure_details)
        new_subscribers = int(result.get("TotalNewSubscribers", 0)) if isinstance(result, dict) else 0
        existing_subscribers = int(result.get("TotalExistingSubscribers", 0)) if isinstance(result, dict) else 0
        failed_emails = {
            str(item.get("EmailAddress") or item.get("email") or "").strip().lower()
            for item in failure_details
            if isinstance(item, dict)
        }
        successful_emails = [
            subscriber["email"]
            for subscriber in subscribers
            if subscriber["email"].strip().lower() not in failed_emails
        ]

        self._record_subscriber_sync_status(
            list_id=structure.list_id,
            submitted_count=len(subscribers),
            failed_count=failure_count,
        )
        self._record_individual_subscriber_sync_status(
            list_id=structure.list_id,
            emails=successful_emails,
        )

        return CampaignMonitorSyncResult(
            status="success" if failure_count == 0 else "warning",
            detail=(
                f"Submitted {len(subscribers)} subscribers to {self.settings.campaign_monitor_master_list_name}. "
                f"New: {new_subscribers}. Existing updated: {existing_subscribers}. Failures: {failure_count}."
            ),
            list_id=structure.list_id,
            submitted_count=len(subscribers),
            new_subscribers=new_subscribers,
            existing_subscribers=existing_subscribers,
            failed_count=failure_count,
        )

    def _record_status(self, sync_status: str) -> None:
        """Upsert the latest Campaign Monitor connection state into sync_targets."""

        query = f"""
        MERGE `{self.settings.sync_targets_table_fqn}` AS target
        USING (
          SELECT
            'campaign_monitor' AS target_system,
            'system_connection' AS target_entity_type,
            '{self.settings.campaign_monitor_client_id or "missing_client_id"}' AS target_entity_id,
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

    def _record_structure_status(self, list_id: str) -> None:
        """Upsert the Campaign Monitor master list status into sync_targets."""

        query = f"""
        MERGE `{self.settings.sync_targets_table_fqn}` AS target
        USING (
          SELECT
            'campaign_monitor' AS target_system,
            'subscriber_list' AS target_entity_type,
            '{list_id}' AS target_entity_id,
            'system' AS source_record_type,
            '{self.MASTER_LIST_RECORD_ID}' AS source_record_id,
            'synced' AS sync_status
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

    def _record_subscriber_sync_status(
        self,
        list_id: str,
        submitted_count: int,
        failed_count: int,
    ) -> None:
        """Upsert subscriber sync status into sync_targets for dashboard visibility."""

        sync_status = "synced" if failed_count == 0 else "warning"
        query = f"""
        MERGE `{self.settings.sync_targets_table_fqn}` AS target
        USING (
          SELECT
            'campaign_monitor' AS target_system,
            'subscriber_sync' AS target_entity_type,
            '{list_id}' AS target_entity_id,
            'system' AS source_record_type,
            'campaign_monitor_subscriber_sync' AS source_record_id,
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

    def _load_sync_candidates(self, limit: int) -> list[dict[str, str]]:
        """Load deduped subscribers that are unsynced or changed since last Campaign Monitor sync."""

        query = f"""
        WITH deduped AS (
          SELECT
            email,
            full_name,
            role_family,
            dealer_name,
            oem,
            city,
            state,
            phone_number,
            country,
            market,
            audience_type,
            source_list,
            dealer_classification,
            updated_at,
            ROW_NUMBER() OVER (
              PARTITION BY LOWER(email)
              ORDER BY
                marketing_ready_flag DESC,
                current_client_override_flag DESC,
                account_confidence_score DESC,
                contact_confidence_score DESC,
                updated_at DESC
            ) AS row_number
          FROM `{self.settings.prospect_leads_table_fqn}`
          WHERE activation_status = 'activation_ready'
            AND COALESCE(prospecting_allowed_flag, TRUE)
            AND email IS NOT NULL
            AND TRIM(email) != ''
        ),
        ranked AS (
          SELECT
            email,
            TRIM(full_name) AS full_name,
            role_family,
            dealer_name,
            oem,
            city,
            state,
            phone_number,
            country,
            market,
            audience_type,
            source_list,
            dealer_classification,
            updated_at
          FROM deduped
          WHERE row_number = 1
        )
        SELECT
          ranked.email,
          ranked.full_name,
          ranked.role_family,
          ranked.dealer_name,
          ranked.oem,
          ranked.city,
          ranked.state,
          ranked.phone_number,
          ranked.country,
          ranked.market,
          ranked.audience_type,
          ranked.source_list,
          ranked.dealer_classification
        FROM ranked
        LEFT JOIN `{self.settings.sync_targets_table_fqn}` AS sync_state
          ON sync_state.target_system = 'campaign_monitor'
         AND sync_state.target_entity_type = 'subscriber'
         AND LOWER(sync_state.target_entity_id) = LOWER(ranked.email)
        WHERE sync_state.last_synced_at IS NULL
           OR ranked.updated_at > sync_state.last_synced_at
        ORDER BY
          sync_state.last_synced_at ASC NULLS FIRST,
          ranked.updated_at DESC,
          ranked.oem ASC,
          ranked.dealer_name ASC,
          ranked.email ASC
        LIMIT {int(limit)}
        """
        rows = self.repository.fetch_all(query)
        return [
            {
                "email": str(row.get("email", "")).strip(),
                "full_name": str(row.get("full_name", "") or "").strip(),
                "role_family": str(row.get("role_family", "") or "").strip() or "unclassified",
                "dealer_name": str(row.get("dealer_name", "") or "").strip(),
                "oem": str(row.get("oem", "") or "").strip() or "Unknown",
                "city": str(row.get("city", "") or "").strip(),
                "state": str(row.get("state", "") or "").strip(),
                "phone_number": str(row.get("phone_number", "") or "").strip(),
                "country": str(row.get("country", "") or "").strip() or "United States",
                "market": str(row.get("market", "") or "").strip() or "US",
                "audience_type": str(row.get("audience_type", "") or "").strip() or "prospect",
                "source_list": str(row.get("source_list", "") or "").strip() or "contact_master",
                "dealer_classification": str(row.get("dealer_classification", "") or "").strip(),
            }
            for row in rows
            if row.get("email")
        ]

    def _record_individual_subscriber_sync_status(self, list_id: str, emails: list[str]) -> None:
        """Persist per-email Campaign Monitor sync progress so later runs advance through the list."""

        cleaned_emails = sorted({str(email).strip().lower() for email in emails if str(email).strip()})
        if not cleaned_emails:
            return
        source_sql = "\nUNION ALL\n".join(
            (
                "SELECT "
                "'campaign_monitor' AS target_system, "
                "'subscriber' AS target_entity_type, "
                f"'{self._escape_sql_literal(email)}' AS target_entity_id, "
                "'prospect_lead' AS source_record_type, "
                f"'{self._escape_sql_literal(email)}' AS source_record_id, "
                "'synced' AS sync_status"
            )
            for email in cleaned_emails
        )
        query = f"""
        MERGE `{self.settings.sync_targets_table_fqn}` AS target
        USING (
          {source_sql}
        ) AS source
        ON target.target_system = source.target_system
           AND target.target_entity_type = source.target_entity_type
           AND LOWER(target.target_entity_id) = LOWER(source.target_entity_id)
           AND target.source_record_type = source.source_record_type
           AND target.source_record_id = source.source_record_id
        WHEN MATCHED THEN
          UPDATE SET
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

    def _escape_sql_literal(self, value: str) -> str:
        """Escape a value for direct interpolation into BigQuery string literals."""

        return (
            str(value)
            .replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        )

    def _ensure_custom_fields(self, list_id: str, dry_run: bool) -> dict[str, str]:
        """Ensure required custom fields exist on the Campaign Monitor list."""

        existing_fields = self._api_get(f"/lists/{list_id}/customfields.json") if not dry_run else []
        field_map = {
            str(item.get("FieldName", "")).strip(): str(item.get("Key", "")).strip()
            for item in existing_fields
        }
        for field_name, data_type in self.SEGMENT_CUSTOM_FIELDS.items():
            if field_name in field_map:
                continue
            field_map[field_name] = f"[{field_name}]"
            if dry_run:
                continue
            payload = {
                "FieldName": field_name,
                "DataType": data_type,
                "VisibleInPreferenceCenter": True,
            }
            created = self._api_post(f"/lists/{list_id}/customfields.json", payload)
            field_map[field_name] = str(created) if created else field_map[field_name]
        return field_map

    def _get_brand_values(self) -> list[str]:
        """Return validated OEM brands from dealer accounts."""

        query = f"""
        SELECT DISTINCT inferred_brand
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE dealer_classification IN ('dealer', 'dealer_group')
          AND inferred_brand IS NOT NULL
          AND TRIM(inferred_brand) != ''
        ORDER BY inferred_brand ASC
        """
        rows = self.repository.fetch_all(query)
        return [str(row["inferred_brand"]).strip() for row in rows if row.get("inferred_brand")]

    def _build_segment_definitions(
        self,
        brands: list[str],
        field_map: dict[str, str],
    ) -> list[SegmentDefinition]:
        """Build OEM plus geography/audience segment definitions."""

        definitions: list[SegmentDefinition] = []
        oem_key = field_map.get("OEM", "[OEM]")

        for brand in brands:
            definitions.append(
                SegmentDefinition(
                    title=f"Automation {brand} Subscribers",
                    rules=((oem_key, f"EQUALS {brand}"),),
                )
            )

        for definition in self.STATIC_SEGMENT_DEFINITIONS:
            definitions.append(
                SegmentDefinition(
                    title=definition.title,
                    rules=tuple(
                        (
                            field_map.get(field_name, f"[{field_name}]"),
                            f"EQUALS {value}",
                        )
                        for field_name, value in definition.rules
                    ),
                )
            )

        return definitions

    def _remove_legacy_segments(self, list_id: str, segments: list[dict[str, Any]]) -> None:
        """Delete legacy generic audience segments that overlap geo-specific ones."""

        for segment in segments:
            title = str(segment.get("Title", "")).strip()
            segment_id = str(segment.get("SegmentID", "")).strip()
            if title not in self.LEGACY_SEGMENT_TITLES or not segment_id:
                continue
            self._api_delete(f"/segments/{segment_id}.json")

    def _api_get(self, path: str) -> Any:
        """Send a GET request to Campaign Monitor and return the decoded JSON."""

        response = requests.get(
            f"{self.BASE_URL}{path}",
            auth=(self.settings.campaign_monitor_api_key, "x"),
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        if not response.text.strip():
            return None
        return response.json()

    def _api_post(
        self,
        path: str,
        payload: dict[str, Any],
        expected_statuses: tuple[int, ...] = (200, 201),
    ) -> Any:
        """Send a POST request to Campaign Monitor and return the decoded response."""

        response = requests.post(
            f"{self.BASE_URL}{path}",
            auth=(self.settings.campaign_monitor_api_key, "x"),
            json=payload,
            timeout=self.settings.request_timeout_seconds,
        )
        if response.status_code not in expected_statuses:
            response.raise_for_status()
        if not response.text.strip():
            return None
        if response.headers.get("content-type", "").startswith("application/json"):
            return response.json()
        return response.text.strip().strip('"')

    def _api_delete(
        self,
        path: str,
        expected_statuses: tuple[int, ...] = (200, 202),
    ) -> None:
        """Send a DELETE request to Campaign Monitor."""

        response = requests.delete(
            f"{self.BASE_URL}{path}",
            auth=(self.settings.campaign_monitor_api_key, "x"),
            timeout=self.settings.request_timeout_seconds,
        )
        if response.status_code not in expected_statuses:
            response.raise_for_status()
