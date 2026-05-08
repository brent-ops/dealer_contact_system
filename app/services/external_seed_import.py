"""One-time import service for external CSV seed files."""

from __future__ import annotations

import csv
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger
from app.source_catalog import SOURCE_FEEDS, SourceFeed


logger = get_logger(__name__)

PERSONAL_EMAIL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "aol.com",
    "msn.com",
    "live.com",
    "me.com",
    "mac.com",
    "comcast.net",
}


@dataclass(frozen=True)
class FileImportSummary:
    """Summary for one CSV file during import preview or live import."""

    file_name: str
    total_rows: int
    valid_email_rows: int
    personal_email_rows: int
    existing_raw_rows: int
    would_insert_rows: int
    audience_type: str
    market: str
    country: str
    inferred_brand: str | None


class ExternalSeedImportService:
    """Import one-time external CSV seed files into a raw BigQuery table."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.seed_directory = Path(settings.external_seed_directory)

    def import_catalog(self, dry_run: bool = False) -> list[FileImportSummary]:
        """Import all cataloged seed CSVs or preview the landing counts."""

        import_batch_id = datetime.now(timezone.utc).strftime("seed-import-%Y%m%d-%H%M%S")
        summaries: list[FileImportSummary] = []

        for feed in SOURCE_FEEDS:
            summary, rows = self._prepare_file(feed=feed, import_batch_id=import_batch_id)
            summaries.append(summary)
            logger.info(
                "External seed file | file=%s | total_rows=%s | valid_email_rows=%s | personal_email_rows=%s | existing_raw_rows=%s | would_insert_rows=%s | dry_run=%s",
                summary.file_name,
                summary.total_rows,
                summary.valid_email_rows,
                summary.personal_email_rows,
                summary.existing_raw_rows,
                summary.would_insert_rows,
                dry_run,
            )
            if not dry_run and rows:
                self._insert_rows(rows)

        return summaries

    def _prepare_file(
        self,
        feed: SourceFeed,
        import_batch_id: str,
    ) -> tuple[FileImportSummary, list[dict[str, object]]]:
        """Read one CSV file, normalize rows, and build a summary."""

        path = self.seed_directory / feed.file_name
        if not path.exists():
            logger.warning("Seed file missing from directory: %s", path)
            return (
                FileImportSummary(
                    file_name=feed.file_name,
                    total_rows=0,
                    valid_email_rows=0,
                    personal_email_rows=0,
                    existing_raw_rows=0,
                    would_insert_rows=0,
                    audience_type=feed.audience_type,
                    market=feed.market,
                    country=feed.country,
                    inferred_brand=feed.inferred_brand,
                ),
                [],
            )

        rows_to_insert: list[dict[str, object]] = []
        total_rows = 0
        valid_email_rows = 0
        personal_email_rows = 0
        existing_raw_rows = 0
        imported_at = datetime.now(timezone.utc).isoformat()
        existing_ids = self._existing_ids_for_file(feed.file_name)

        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_number, row in enumerate(reader, start=1):
                total_rows += 1
                raw_name = str(row.get("Name", "") or "").strip()
                raw_email = str(row.get("Email Address", "") or "").strip()
                normalized_email = raw_email.lower()
                if not self._is_valid_email(normalized_email):
                    continue

                valid_email_rows += 1
                email_domain = normalized_email.split("@", 1)[1]
                is_personal_email = email_domain in PERSONAL_EMAIL_DOMAINS
                if is_personal_email:
                    personal_email_rows += 1

                external_seed_contact_id = self._build_seed_id(feed.file_name, row_number, normalized_email)
                if external_seed_contact_id in existing_ids:
                    existing_raw_rows += 1
                    continue

                first_name, last_name = self._split_name(raw_name)
                rows_to_insert.append(
                    {
                        "external_seed_contact_id": external_seed_contact_id,
                        "import_batch_id": import_batch_id,
                        "source_file_name": feed.file_name,
                        "source_path": str(path),
                        "source_group": feed.source_group,
                        "audience_type": feed.audience_type,
                        "market": feed.market,
                        "country": feed.country,
                        "inferred_brand_from_source": feed.inferred_brand,
                        "row_number": row_number,
                        "raw_name": raw_name,
                        "raw_email": raw_email,
                        "normalized_full_name": raw_name,
                        "first_name": first_name,
                        "last_name": last_name,
                        "normalized_email": normalized_email,
                        "email_domain": email_domain,
                        "account_key": email_domain,
                        "is_personal_email": is_personal_email,
                        "import_notes": feed.notes,
                        "imported_at": imported_at,
                        "created_at": imported_at,
                    }
                )

        summary = FileImportSummary(
            file_name=feed.file_name,
            total_rows=total_rows,
            valid_email_rows=valid_email_rows,
            personal_email_rows=personal_email_rows,
            existing_raw_rows=existing_raw_rows,
            would_insert_rows=len(rows_to_insert),
            audience_type=feed.audience_type,
            market=feed.market,
            country=feed.country,
            inferred_brand=feed.inferred_brand,
        )
        return summary, rows_to_insert

    def _insert_rows(self, rows: list[dict[str, object]]) -> None:
        """Insert one chunk of raw seed rows into BigQuery."""

        chunk_size = 500
        for start_index in range(0, len(rows), chunk_size):
            chunk = rows[start_index : start_index + chunk_size]
            errors = self.repository.insert_rows_json(
                self.settings.external_seed_contacts_table_fqn,
                chunk,
            )
            if errors:
                raise RuntimeError(f"BigQuery insert failed for external seed rows: {errors}")

    def _existing_ids_for_file(self, file_name: str) -> set[str]:
        """Fetch existing raw row IDs for one source file."""

        query = f"""
        SELECT external_seed_contact_id
        FROM `{self.settings.external_seed_contacts_table_fqn}`
        WHERE source_file_name = '{file_name}'
        """
        rows = self.repository.fetch_all(query)
        return {
            str(row["external_seed_contact_id"])
            for row in rows
            if row.get("external_seed_contact_id")
        }

    @staticmethod
    def _build_seed_id(file_name: str, row_number: int, normalized_email: str) -> str:
        """Build a stable raw row identifier."""

        digest = hashlib.sha256(f"{file_name}|{row_number}|{normalized_email}".encode("utf-8")).hexdigest()
        return digest

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        """Perform a simple email sanity check."""

        return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email))

    @staticmethod
    def _split_name(full_name: str) -> tuple[str, str]:
        """Split a full name into first and last components."""

        name = full_name.strip()
        if not name:
            return "", ""
        parts = name.split()
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], " ".join(parts[1:])
