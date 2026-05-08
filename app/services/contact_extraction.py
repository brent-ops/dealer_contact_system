"""Leadership contact extraction from confirmed dealer websites."""

from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.fetch_tracker import FetchStatusTracker, FetchTrackingUpdate
from app.logging_utils import get_logger
from app.web_fetcher import FetchResult, WebFetcher


logger = get_logger(__name__)

CONTACT_PAGE_HINTS = [
    "/staff",
    "/staff.aspx",
    "/staff.html",
    "/meet-our-staff",
    "/meet-our-staff.aspx",
    "/our-team",
    "/team",
    "/leadership",
    "/leadership.aspx",
    "/about-us",
    "/aboutus.aspx",
    "/contact-us",
    "/contactus.aspx",
    "/sales",
    "/service",
]

ROLE_PATTERNS = [
    ("general_manager", "executive", re.compile(r"\b(general manager|gm)\b", re.IGNORECASE)),
    ("dealer_principal", "executive", re.compile(r"\b(co-owner|dealer principal|principal|owner|managing partner|partner)\b", re.IGNORECASE)),
    ("president", "executive", re.compile(r"\b(president|ceo|chief executive officer)\b", re.IGNORECASE)),
    ("operations", "operations", re.compile(r"\b(coo|chief operating officer|operations manager|operations director|director of operations|fixed operations director|fixed ops director)\b", re.IGNORECASE)),
    ("sales", "sales", re.compile(r"\b(general sales manager|internet sales manager|sales manager|sales director|director of sales|new car manager|used car manager)\b", re.IGNORECASE)),
    ("service", "service", re.compile(r"\b(service manager|service director|fixed ops manager|parts manager|parts director)\b", re.IGNORECASE)),
    ("marketing", "marketing", re.compile(r"\b(marketing manager|marketing director|digital marketing|e-?commerce director)\b", re.IGNORECASE)),
    ("finance", "executive", re.compile(r"\b(cfo|chief financial officer|controller|finance director|business manager|finance manager)\b", re.IGNORECASE)),
]

EMAIL_PATTERN = re.compile(r"mailto:([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})", re.IGNORECASE)
PLAIN_EMAIL_PATTERN = re.compile(r"\b([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})\b", re.IGNORECASE)


@dataclass(frozen=True)
class ExtractionPreview:
    """Read-only summary of the extraction target pool."""

    candidate_accounts: int
    accounts_with_websites: int


@dataclass(frozen=True)
class ContactCandidate:
    """Candidate leadership contact extracted from a public page."""

    dealer_account_id: str
    account_key: str
    full_name: str
    first_name: str
    last_name: str
    email: str
    email_domain: str
    role_type: str
    role_family: str
    role_title: str
    source_url: str
    confidence_score: float


class ContactExtractionService:
    """Extract manager, operations, and executive contacts from dealer sites."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.fetcher = WebFetcher(timeout_seconds=settings.request_timeout_seconds)
        self.fetch_tracker = FetchStatusTracker(repository, settings)

    def preview(self) -> ExtractionPreview:
        """Return a summary of how many websites are ready for extraction."""

        query = f"""
        SELECT
          COUNT(*) AS candidate_accounts,
          COUNTIF(website_url IS NOT NULL AND TRIM(website_url) != '') AS accounts_with_websites
        FROM `{self.settings.dealer_accounts_table_fqn}` AS dealer_accounts
        WHERE IFNULL(is_personal_domain, FALSE) = FALSE
          AND dealer_classification IN ('dealer', 'dealer_group')
        """
        row = self.repository.fetch_one(query)
        return ExtractionPreview(
            candidate_accounts=int(row.get("candidate_accounts", 0)),
            accounts_with_websites=int(row.get("accounts_with_websites", 0)),
        )

    def extract(
        self,
        dry_run: bool = False,
        limit: int | None = None,
        account_keys: list[str] | None = None,
    ) -> None:
        """Extract leadership contacts and write them into canonical tables."""

        preview = self.preview()
        logger.info(
            "Contact extraction preview | candidates=%s | websites=%s | dry_run=%s",
            preview.candidate_accounts,
            preview.accounts_with_websites,
            dry_run,
        )

        accounts = self._load_accounts(limit=limit, account_keys=account_keys)
        logger.info("Loaded contact extraction batch | accounts=%s", len(accounts))

        candidates: list[ContactCandidate] = []
        for account in accounts:
            candidates.extend(self._extract_for_account(account))

        deduped = self._dedupe_candidates(candidates)
        logger.info(
            "Prepared leadership contact candidates | raw=%s | deduped=%s | dry_run=%s",
            len(candidates),
            len(deduped),
            dry_run,
        )

        if dry_run:
            return

        for candidate in deduped:
            self._upsert_contact(candidate)
            self._upsert_relationship(candidate)
            self._supersede_ai_inferred_contacts(candidate)

    def _load_accounts(self, limit: int | None, account_keys: list[str] | None) -> list[dict[str, str]]:
        """Load account websites that are ready for contact extraction."""

        row_limit = limit or self.settings.enrichment_batch_size
        account_filter = ""
        if account_keys:
            quoted = ", ".join("'" + account_key.lower().replace("'", "''") + "'" for account_key in account_keys)
            account_filter = f" AND account_key IN ({quoted})"
        query = f"""
        SELECT
          dealer_account_id,
          account_key,
          website_url,
          source_type
        FROM `{self.settings.dealer_accounts_table_fqn}` AS dealer_accounts
        WHERE website_url IS NOT NULL
          AND TRIM(website_url) != ''
          AND dealer_classification IN ('dealer', 'dealer_group')
          AND NOT EXISTS (
            SELECT 1
            FROM `{self.settings.account_relationships_table_fqn}` AS relationships
            WHERE relationships.dealer_account_id = dealer_accounts.dealer_account_id
              AND relationships.source_type = 'website_contact_extraction'
          )
          {account_filter}
        ORDER BY
          CASE
            WHEN source_type = 'website_enrichment' THEN 0
            WHEN source_type = 'website_resolution' THEN 1
            ELSE 2
          END ASC,
          IFNULL(last_verified_at, TIMESTAMP('1970-01-01')) ASC,
          account_key ASC
        LIMIT {row_limit}
        """
        return [dict(row.items()) for row in self.repository.run_query(query)]

    def _extract_for_account(self, account: dict[str, str]) -> list[ContactCandidate]:
        """Extract contact candidates from one account website."""

        homepage = self.fetcher.fetch(account["website_url"])
        blocked_reason = homepage.blocked_reason if homepage and homepage.blocked else None
        pages: list[FetchResult]
        if homepage and homepage.ok:
            pages = self._collect_pages(homepage)
        else:
            canonical_url = self._resolve_website_url(account["website_url"])
            pages = self._probe_known_pages(canonical_url)

        if not pages:
            self._record_fetch_status(
                dealer_account_id=account["dealer_account_id"],
                fetch_status="blocked" if blocked_reason else "unavailable",
                blocked_reason=blocked_reason or "no_parseable_people_pages",
            )
            logger.info("Website unavailable for contact extraction | account_key=%s", account["account_key"])
            return []

        self._record_fetch_status(
            dealer_account_id=account["dealer_account_id"],
            fetch_status="success",
        )
        candidates: list[ContactCandidate] = []
        for page in pages:
            candidates.extend(self._extract_page_candidates(account, page))
        return candidates

    def _collect_pages(self, homepage: FetchResult) -> list[FetchResult]:
        """Fetch a small set of likely leadership pages."""

        pages = [homepage]
        for page_url in self._discover_page_urls(homepage):
            result = self.fetcher.fetch(page_url)
            if result and result.ok and all(existing.final_url != result.final_url for existing in pages):
                pages.append(result)
        return pages

    def _probe_known_pages(self, website_url: str) -> list[FetchResult]:
        """Probe likely people pages directly when homepage fetching is blocked."""

        pages: list[FetchResult] = []
        for path in CONTACT_PAGE_HINTS:
            page_url = urljoin(website_url.rstrip("/") + "/", path.lstrip("/"))
            result = self.fetcher.fetch(page_url)
            if result and result.ok and all(existing.final_url != result.final_url for existing in pages):
                pages.append(result)
        return pages

    def _resolve_website_url(self, website_url: str) -> str:
        """Resolve a stored website to its best current final URL."""

        _, final_url = self.fetcher.resolve(website_url)
        return final_url or website_url

    def _discover_page_urls(self, homepage: FetchResult) -> list[str]:
        """Discover likely people/contact pages from homepage navigation."""

        soup = BeautifulSoup(homepage.html, "html.parser")
        discovered: list[tuple[int, str]] = []
        seen: set[str] = set()
        base_netloc = urlparse(homepage.final_url).netloc

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            try:
                full_url = urljoin(homepage.final_url, href)
                parsed = urlparse(full_url)
            except ValueError:
                logger.debug("Skipping malformed page link during contact extraction | href=%s", href)
                continue
            if parsed.netloc != base_netloc:
                continue

            haystack = f"{href} {anchor.get_text(' ', strip=True)}".lower()
            if any(token in haystack for token in ["staff", "team", "leadership", "about", "contact", "finance", "service", "sales", "manager"]):
                clean_url = full_url.split("#", 1)[0]
                if clean_url not in seen:
                    seen.add(clean_url)
                    discovered.append((self._page_priority(haystack), clean_url))

        for path in CONTACT_PAGE_HINTS:
            guessed = urljoin(homepage.final_url + "/", path.lstrip("/"))
            if guessed not in seen:
                seen.add(guessed)
                discovered.append((self._page_priority(path.lower()), guessed))
        discovered.sort(key=lambda item: (item[0], item[1]))
        return [url for _, url in discovered[:16]]

    def _page_priority(self, haystack: str) -> int:
        """Return a stable priority for likely people/contact pages."""

        if "staff" in haystack or "team" in haystack:
            return 0
        if "leadership" in haystack or "manager" in haystack:
            return 1
        if "about" in haystack or "contact" in haystack:
            return 2
        if "sales" in haystack or "service" in haystack or "finance" in haystack:
            return 3
        return 4

    def _extract_page_candidates(self, account: dict[str, str], page: FetchResult) -> list[ContactCandidate]:
        """Extract leadership contacts with emails from a page."""

        soup = BeautifulSoup(page.html, "html.parser")
        candidates: list[ContactCandidate] = []
        allowed_domains = self._allowed_email_domains(account, page)
        for email, text_window in self._email_contexts(soup):
            if not self._email_matches_account(email, allowed_domains):
                continue
            title_match = self._classify_role(text_window)
            if not title_match:
                continue
            full_name = self._extract_name_near_email(text_window, email, title_match[2])
            if not full_name:
                continue
            first_name, last_name = self._split_name(full_name)
            confidence = self._score_candidate(
                email=email,
                full_name=full_name,
                role_title=title_match[2],
                source_url=page.final_url,
            )
            candidates.append(
                ContactCandidate(
                    dealer_account_id=account["dealer_account_id"],
                    account_key=account["account_key"],
                    full_name=full_name,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    email_domain=email.split("@", 1)[1],
                    role_type=title_match[0],
                    role_family=title_match[1],
                    role_title=title_match[2],
                    source_url=page.final_url,
                    confidence_score=confidence,
                )
            )
        return candidates

    def _email_contexts(self, soup: BeautifulSoup) -> list[tuple[str, str]]:
        """Return mailto and plain-text email contexts from a page."""

        contexts: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()

        for anchor in soup.find_all("a", href=True):
            match = EMAIL_PATTERN.search(anchor["href"])
            if not match:
                continue
            email = match.group(1).lower()
            text_window = self._anchor_context_text(anchor)
            key = (email, text_window)
            if key not in seen:
                seen.add(key)
                contexts.append(key)

        for text_node in soup.find_all(string=PLAIN_EMAIL_PATTERN):
            text = str(text_node).strip()
            if not text:
                continue
            match = PLAIN_EMAIL_PATTERN.search(text)
            if not match:
                continue
            email = match.group(1).lower()
            parent = text_node.parent
            text_window = text
            current = parent
            for _ in range(5):
                if current is None:
                    break
                candidate = current.get_text(" ", strip=True)
                if len(candidate) >= 40:
                    text_window = candidate
                    break
                current = getattr(current, "parent", None)
            key = (email, text_window)
            if key not in seen:
                seen.add(key)
                contexts.append(key)

        return contexts

    def _allowed_email_domains(self, account: dict[str, str], page: FetchResult) -> set[str]:
        """Return acceptable domains for contacts tied to this account/site."""

        domains = set()
        for value in [account.get("account_key"), account.get("website_url"), page.final_url]:
            if not value:
                continue
            try:
                host = urlparse(value).netloc or value
            except ValueError:
                logger.debug("Skipping malformed host while building allowed email domains | value=%s", value)
                continue
            host = host.lower().strip()
            host = host.removeprefix("www.")
            if host:
                domains.add(host)
        return domains

    def _email_matches_account(self, email: str, allowed_domains: set[str]) -> bool:
        """Return True when an extracted email fits the confirmed account domain."""

        domain = email.split("@", 1)[1].lower()
        domain = domain.removeprefix("www.")
        for allowed in allowed_domains:
            if domain == allowed or domain.endswith("." + allowed) or allowed.endswith("." + domain):
                return True
        return False

    def _anchor_context_text(self, anchor) -> str:
        """Take nearby text around a mailto anchor for parsing."""

        current = anchor
        for _ in range(5):
            current = getattr(current, "parent", None)
            if current is None:
                break
            text = current.get_text(" ", strip=True)
            if len(text) >= 40:
                return text
        return anchor.get_text(" ", strip=True)

    def _classify_role(self, text: str) -> tuple[str, str, str] | None:
        """Map a nearby title to a role type and family."""

        for role_type, role_family, pattern in ROLE_PATTERNS:
            match = pattern.search(text)
            if match:
                return role_type, role_family, match.group(0).strip()
        return None

    def _extract_name_near_email(self, text: str, email: str, role_title: str) -> str | None:
        """Find a likely full name near an email address."""

        cleaned = text.replace(email, " ").replace("Read my Bio", " ")
        role_index = cleaned.lower().find(role_title.lower())
        if role_index > 0:
            cleaned = cleaned[:role_index]
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        name_matches = re.findall(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b", cleaned)
        for candidate in name_matches:
            if not self._looks_like_title(candidate):
                return self._trim_name_suffix(candidate.strip())
        return None

    def _looks_like_title(self, candidate: str) -> bool:
        """Return True for title-like phrases rather than person names."""

        lowered = candidate.lower()
        stop_words = ["manager", "director", "service", "sales", "operations", "marketing", "executive", "principal"]
        return any(word in lowered for word in stop_words)

    def _split_name(self, full_name: str) -> tuple[str, str]:
        """Split a full name into first and last names."""

        parts = full_name.split()
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], parts[-1]

    def _trim_name_suffix(self, full_name: str) -> str:
        """Remove trailing title fragments that leaked into the name parse."""

        parts = full_name.split()
        title_fragments = {"Co", "General", "Internet", "Business", "New", "Used", "Service", "Parts", "Finance"}
        while len(parts) > 1 and parts[-1] in title_fragments:
            parts.pop()
        return " ".join(parts)

    def _score_candidate(self, email: str, full_name: str, role_title: str, source_url: str) -> float:
        """Assign a simple confidence score to a public contact candidate."""

        score = 0.7
        if full_name and len(full_name.split()) >= 2:
            score += 0.1
        if role_title:
            score += 0.1
        if any(path in source_url.lower() for path in ["staff", "team", "leadership"]):
            score += 0.07
        if not email.startswith("info@") and not email.startswith("sales@"):
            score += 0.03
        return min(score, 0.98)

    def _record_fetch_status(
        self,
        dealer_account_id: str,
        fetch_status: str,
        blocked_reason: str | None = None,
    ) -> None:
        """Persist the latest basic-fetch outcome for this account."""

        self.fetch_tracker.record(
            FetchTrackingUpdate(
                dealer_account_id=dealer_account_id,
                fetch_status=fetch_status,
                fetch_method="basic_http",
                blocked_reason=blocked_reason,
            )
        )

    def _dedupe_candidates(self, candidates: list[ContactCandidate]) -> list[ContactCandidate]:
        """Keep the best candidate per account/email pair."""

        best: dict[tuple[str, str], ContactCandidate] = {}
        for candidate in candidates:
            key = (candidate.dealer_account_id, candidate.email)
            if key not in best or candidate.confidence_score > best[key].confidence_score:
                best[key] = candidate
        return list(best.values())

    def _upsert_contact(self, candidate: ContactCandidate) -> None:
        """Insert or update one extracted contact."""

        email = candidate.email.replace("'", "''")
        query = f"""
        MERGE `{self.settings.prospect_contacts_table_fqn}` AS target
        USING (
          SELECT
            TO_HEX(SHA256('{email}')) AS prospect_contact_id,
            '{candidate.full_name.replace("'", "''")}' AS full_name,
            '{candidate.first_name.replace("'", "''")}' AS first_name,
            '{candidate.last_name.replace("'", "''")}' AS last_name,
            '{email}' AS email,
            '{candidate.email_domain.replace("'", "''")}' AS email_domain,
            FALSE AS is_personal_email,
            'business' AS domain_type,
            '{candidate.role_type}' AS role_type,
            '{candidate.role_family}' AS role_family,
            '{candidate.role_title.replace("'", "''")}' AS role_title,
            'website_observed' AS email_quality,
            'website_contact_extraction' AS email_source_type,
            '{candidate.source_url.replace("'", "''")}' AS email_source_url,
            {candidate.confidence_score} AS email_confidence_score,
            'active' AS contact_status,
            {candidate.confidence_score} AS confidence_score,
            'website_contact_extraction' AS source_type,
            '{self.settings.dealer_accounts_table}' AS source_table,
            '{candidate.source_url.replace("'", "''")}' AS source_url,
            CURRENT_TIMESTAMP() AS observed_at
        ) AS source
        ON target.prospect_contact_id = source.prospect_contact_id
        WHEN MATCHED THEN
          UPDATE SET
            target.full_name = IF(source.confidence_score >= IFNULL(target.confidence_score, 0.0), source.full_name, target.full_name),
            target.first_name = IF(source.confidence_score >= IFNULL(target.confidence_score, 0.0), source.first_name, target.first_name),
            target.last_name = IF(source.confidence_score >= IFNULL(target.confidence_score, 0.0), source.last_name, target.last_name),
            target.email_domain = COALESCE(target.email_domain, source.email_domain),
            target.domain_type = COALESCE(target.domain_type, source.domain_type),
            target.role_type = IF(source.confidence_score >= IFNULL(target.confidence_score, 0.0), source.role_type, target.role_type),
            target.role_family = IF(source.confidence_score >= IFNULL(target.confidence_score, 0.0), source.role_family, target.role_family),
            target.role_title = IF(source.confidence_score >= IFNULL(target.confidence_score, 0.0), source.role_title, target.role_title),
            target.email_quality = 'website_observed',
            target.email_source_type = source.email_source_type,
            target.email_source_url = source.email_source_url,
            target.email_confidence_score = GREATEST(IFNULL(target.email_confidence_score, 0.0), source.email_confidence_score),
            target.contact_status = source.contact_status,
            target.confidence_score = GREATEST(IFNULL(target.confidence_score, 0.0), source.confidence_score),
            target.source_type = source.source_type,
            target.source_table = source.source_table,
            target.source_url = IF(source.confidence_score >= IFNULL(target.confidence_score, 0.0), source.source_url, target.source_url),
            target.first_seen_at = LEAST(IFNULL(target.first_seen_at, source.observed_at), source.observed_at),
            target.last_seen_at = GREATEST(IFNULL(target.last_seen_at, source.observed_at), source.observed_at),
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
            email_quality,
            email_source_type,
            email_source_url,
            email_confidence_score,
            contact_status,
            confidence_score,
            source_type,
            source_table,
            source_url,
            first_seen_at,
            last_seen_at,
            created_at,
            updated_at
          )
          VALUES (
            source.prospect_contact_id,
            NULL,
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
            source.email_quality,
            source.email_source_type,
            source.email_source_url,
            source.email_confidence_score,
            source.contact_status,
            source.confidence_score,
            source.source_type,
            source.source_table,
            source.source_url,
            source.observed_at,
            source.observed_at,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(query)

    def _upsert_relationship(self, candidate: ContactCandidate) -> None:
        """Insert or update the account relationship for an extracted contact."""

        account_key = candidate.account_key.replace("'", "''")
        email = candidate.email.replace("'", "''")
        source_url = candidate.source_url.replace("'", "''")
        query = f"""
        MERGE `{self.settings.account_relationships_table_fqn}` AS target
        USING (
          SELECT
            TO_HEX(SHA256(CONCAT('{account_key}', '|', '{email}'))) AS relationship_id,
            '{candidate.dealer_account_id}' AS dealer_account_id,
            TO_HEX(SHA256('{email}')) AS prospect_contact_id,
            'website_role_match' AS relationship_type,
            TRUE AS is_primary,
            'active' AS relationship_status,
            {candidate.confidence_score} AS confidence_score,
            'website_contact_extraction' AS source_type,
            '{self.settings.dealer_accounts_table}' AS source_table,
            '{source_url}' AS source_url,
            CURRENT_TIMESTAMP() AS observed_at
        ) AS source
        ON target.relationship_id = source.relationship_id
        WHEN MATCHED THEN
          UPDATE SET
            target.relationship_type = source.relationship_type,
            target.relationship_status = source.relationship_status,
            target.confidence_score = GREATEST(IFNULL(target.confidence_score, 0.0), source.confidence_score),
            target.source_type = source.source_type,
            target.source_table = source.source_table,
            target.source_url = source.source_url,
            target.first_seen_at = LEAST(IFNULL(target.first_seen_at, source.observed_at), source.observed_at),
            target.last_seen_at = GREATEST(IFNULL(target.last_seen_at, source.observed_at), source.observed_at),
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
            source_url,
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
            source.source_url,
            source.observed_at,
            source.observed_at,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          )
        """
        self.repository.execute_statement(query)

    def _supersede_ai_inferred_contacts(self, candidate: ContactCandidate) -> None:
        """Mark weaker AI-inferred contacts as superseded when a website-observed email exists."""

        full_name_sql = candidate.full_name.replace("'", "''").lower()
        email_sql = candidate.email.replace("'", "''").lower()
        dealer_account_id_sql = candidate.dealer_account_id.replace("'", "''")
        contact_query = f"""
        UPDATE `{self.settings.prospect_contacts_table_fqn}` AS pc
        SET
          contact_status = 'superseded',
          activation_status = 'hold',
          enrichment_stage = 'superseded',
          updated_at = CURRENT_TIMESTAMP()
        WHERE pc.source_type = 'ai_inferred_directory_email'
          AND LOWER(IFNULL(pc.full_name, '')) = '{full_name_sql}'
          AND LOWER(pc.email) != '{email_sql}'
          AND EXISTS (
            SELECT 1
            FROM `{self.settings.account_relationships_table_fqn}` AS ar
            WHERE ar.prospect_contact_id = pc.prospect_contact_id
              AND ar.dealer_account_id = '{dealer_account_id_sql}'
          )
        """
        relationship_query = f"""
        UPDATE `{self.settings.account_relationships_table_fqn}` AS ar
        SET
          relationship_status = 'superseded',
          updated_at = CURRENT_TIMESTAMP()
        WHERE ar.source_type = 'ai_inferred_directory_email'
          AND ar.dealer_account_id = '{dealer_account_id_sql}'
          AND ar.prospect_contact_id IN (
            SELECT pc.prospect_contact_id
            FROM `{self.settings.prospect_contacts_table_fqn}` AS pc
            WHERE pc.source_type = 'ai_inferred_directory_email'
              AND LOWER(IFNULL(pc.full_name, '')) = '{full_name_sql}'
              AND LOWER(pc.email) != '{email_sql}'
          )
        """
        self.repository.execute_statement(contact_query)
        self.repository.execute_statement(relationship_query)
