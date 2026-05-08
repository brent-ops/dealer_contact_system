"""Account enrichment from public website signals."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.fetch_tracker import FetchStatusTracker, FetchTrackingUpdate
from app.logging_utils import get_logger
from app.web_fetcher import FetchResult, WebFetcher


logger = get_logger(__name__)

US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
}

BRAND_PATTERNS = {
    "acura": "Acura",
    "audi": "Audi",
    "bmw": "BMW",
    "buick": "Buick",
    "cadillac": "Cadillac",
    "chevrolet": "Chevrolet",
    "chevy": "Chevrolet",
    "chrysler": "Chrysler",
    "dodge": "Dodge",
    "ford": "Ford",
    "gmc": "GMC",
    "honda": "Honda",
    "hyundai": "Hyundai",
    "infiniti": "Infiniti",
    "jeep": "Jeep",
    "kia": "Kia",
    "lexus": "Lexus",
    "lincoln": "Lincoln",
    "mazda": "Mazda",
    "mercedes": "Mercedes-Benz",
    "mercedes-benz": "Mercedes-Benz",
    "mini": "MINI",
    "mitsubishi": "Mitsubishi",
    "nissan": "Nissan",
    "porsche": "Porsche",
    "ram": "Ram",
    "subaru": "Subaru",
    "toyota": "Toyota",
    "volkswagen": "Volkswagen",
    "vw": "Volkswagen",
    "volvo": "Volvo",
}
PHONE_REGEX = re.compile(
    r"(?:\+?1[\s.\-]?)?(?:\(?\d{3}\)?[\s.\-]?)\d{3}[\s.\-]?\d{4}"
)

STAFF_PAGE_HINTS = [
    "/",
    "/about-us",
    "/contact-us",
    "/staff",
    "/meet-our-staff",
    "/our-team",
    "/leadership",
]


@dataclass(frozen=True)
class AccountRecord:
    """Subset of account fields needed for enrichment."""

    dealer_account_id: str
    account_key: str
    email_domain: str | None
    website_url: str | None
    website_confidence_score: float
    account_name_confidence_score: float
    brand_confidence_score: float
    location_confidence_score: float
    account_phone_confidence_score: float


@dataclass(frozen=True)
class EnrichmentPreview:
    """Read-only snapshot of the enrichment target pool."""

    candidate_accounts: int
    accounts_with_websites: int


class AccountEnrichmentService:
    """Enrich dealer accounts using public website signals."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.fetcher = WebFetcher(timeout_seconds=settings.request_timeout_seconds)
        self.fetch_tracker = FetchStatusTracker(repository, settings)

    def preview(self) -> EnrichmentPreview:
        """Return a summary of how many accounts will be considered."""

        query = f"""
        SELECT
          COUNT(*) AS candidate_accounts,
          COUNTIF(website_url IS NOT NULL AND TRIM(website_url) != '') AS accounts_with_websites
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE IFNULL(is_personal_domain, FALSE) = FALSE
          AND dealer_classification IN ('dealer', 'dealer_group')
          AND {self._needs_enrichment_sql()}
        """
        row = self.repository.fetch_one(query)
        return EnrichmentPreview(
            candidate_accounts=int(row.get("candidate_accounts", 0)),
            accounts_with_websites=int(row.get("accounts_with_websites", 0)),
        )

    def enrich(
        self,
        dry_run: bool = False,
        limit: int | None = None,
        account_keys: list[str] | None = None,
    ) -> None:
        """Enrich accounts and write the best current values back to BigQuery."""

        preview = self.preview()
        logger.info(
            "Account enrichment preview | candidates=%s | existing_websites=%s | dry_run=%s",
            preview.candidate_accounts,
            preview.accounts_with_websites,
            dry_run,
        )

        accounts = self._load_accounts(limit=limit, account_keys=account_keys)
        logger.info("Loaded enrichment batch | accounts=%s", len(accounts))

        updates: list[dict[str, object]] = []
        for account in accounts:
            update = self._enrich_account(account)
            if update:
                updates.append(update)

        logger.info("Prepared account enrichment updates | updates=%s | dry_run=%s", len(updates), dry_run)
        if dry_run:
            return

        if updates:
            self._apply_updates(updates)
            logger.info("Applied account enrichment updates | updates=%s", len(updates))
        else:
            logger.info("No account enrichment updates were generated.")

    def _load_accounts(
        self,
        limit: int | None,
        account_keys: list[str] | None,
    ) -> list[AccountRecord]:
        """Load the next batch of accounts to enrich."""

        row_limit = limit or self.settings.enrichment_batch_size
        account_filter = ""
        if account_keys:
            quoted = ", ".join(
                "'" + account_key.lower().replace("'", "''") + "'"
                for account_key in account_keys
            )
            account_filter = f" AND account_key IN ({quoted})"
        query = f"""
        SELECT
          dealer_account_id,
          account_key,
          email_domain,
          website_url,
          IFNULL(website_confidence_score, 0.0) AS website_confidence_score,
          IFNULL(account_name_confidence_score, 0.0) AS account_name_confidence_score,
          IFNULL(brand_confidence_score, 0.0) AS brand_confidence_score,
          IFNULL(location_confidence_score, 0.0) AS location_confidence_score,
          IFNULL(account_phone_confidence_score, 0.0) AS account_phone_confidence_score
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE IFNULL(is_personal_domain, FALSE) = FALSE
          AND dealer_classification IN ('dealer', 'dealer_group')
          AND {self._needs_enrichment_sql()}
          {account_filter}
        ORDER BY
          CASE
            WHEN website_url IS NULL OR TRIM(website_url) = '' THEN 0
            WHEN IFNULL(account_name, '') = '' OR IFNULL(inferred_brand, '') = '' THEN 1
            WHEN IFNULL(account_city, '') = '' OR IFNULL(account_state, '') = '' THEN 2
            ELSE 3
          END ASC,
          IFNULL(last_verified_at, TIMESTAMP('1970-01-01')) ASC,
          account_key ASC
        LIMIT {row_limit}
        """
        rows = list(self.repository.run_query(query))
        return [
            AccountRecord(
                dealer_account_id=row["dealer_account_id"],
                account_key=row["account_key"],
                email_domain=row["email_domain"],
                website_url=row["website_url"],
                website_confidence_score=float(row["website_confidence_score"] or 0.0),
                account_name_confidence_score=float(row["account_name_confidence_score"] or 0.0),
                brand_confidence_score=float(row["brand_confidence_score"] or 0.0),
                location_confidence_score=float(row["location_confidence_score"] or 0.0),
                account_phone_confidence_score=float(row["account_phone_confidence_score"] or 0.0),
            )
            for row in rows
        ]

    def _needs_enrichment_sql(self) -> str:
        """Return the SQL predicate for accounts missing key enrichment fields."""

        return """
        (
          website_url IS NULL
          OR TRIM(website_url) = ''
          OR account_name IS NULL
          OR TRIM(account_name) = ''
          OR inferred_brand IS NULL
          OR TRIM(inferred_brand) = ''
          OR account_city IS NULL
          OR account_state IS NULL
          OR account_phone IS NULL
          OR TRIM(account_phone) = ''
        )
        """

    def _enrich_account(self, account: AccountRecord) -> dict[str, object] | None:
        """Enrich a single account from its public website."""

        candidate_urls = self._candidate_homepages(account)
        homepage, blocked_result = self._find_homepage(candidate_urls)
        pages: list[FetchResult] = []
        fetch_status = "unavailable"
        blocked_reason: str | None = blocked_result.blocked_reason if blocked_result else None
        if not homepage:
            resolved_url = self._resolve_homepage(candidate_urls)
            probe_url = resolved_url or (candidate_urls[0] if candidate_urls else None)
            if probe_url:
                pages = self._probe_known_pages(probe_url)
                if pages:
                    fetch_status = "success"

            if not resolved_url and not pages:
                self._record_fetch_status(
                    dealer_account_id=account.dealer_account_id,
                    fetch_status="blocked" if blocked_reason else "unavailable",
                    blocked_reason=blocked_reason or "no_public_html",
                )
                logger.info("No public homepage found | account_key=%s", account.account_key)
                return None

            if pages:
                homepage = pages[0]
            else:
                homepage = None
        else:
            pages = self._collect_pages(homepage)
            fetch_status = "success"

        if not homepage and not pages:
            self._record_fetch_status(
                dealer_account_id=account.dealer_account_id,
                fetch_status="blocked" if blocked_reason else "unavailable",
                blocked_reason=blocked_reason or "resolved_without_parseable_html",
            )
            logger.info("No parseable enrichment pages found | account_key=%s", account.account_key)
            return None

        if not homepage:
            self._record_fetch_status(
                dealer_account_id=account.dealer_account_id,
                fetch_status=fetch_status,
            )
            # Save the best resolved website even when the site blocks fetches.
            return {
                "dealer_account_id": account.dealer_account_id,
                "website_url": resolved_url,
                "website_source_url": resolved_url,
                "website_confidence_score": max(0.82, account.website_confidence_score),
                "confidence_score": max(0.82, account.website_confidence_score),
                "source_type": "website_resolution",
                "source_table": self.settings.dealer_accounts_table,
                "last_verified_at": "CURRENT_TIMESTAMP()",
            }

        derived = self._extract_best_values(pages, homepage)
        self._record_fetch_status(
            dealer_account_id=account.dealer_account_id,
            fetch_status="success",
        )
        if not derived:
            return None

        update = {
            "dealer_account_id": account.dealer_account_id,
            "website_url": homepage.final_url,
            "website_source_url": homepage.final_url,
            "website_confidence_score": max(derived.get("website_confidence_score", 0.0), account.website_confidence_score),
            "confidence_score": max(
                derived.get("website_confidence_score", 0.0),
                derived.get("account_name_confidence_score", 0.0),
                derived.get("brand_confidence_score", 0.0),
                derived.get("location_confidence_score", 0.0),
            ),
            "source_type": "website_enrichment",
            "source_table": self.settings.dealer_accounts_table,
            "last_verified_at": "CURRENT_TIMESTAMP()",
        }

        if derived.get("account_name") and float(derived.get("account_name_confidence_score", 0.0)) >= account.account_name_confidence_score:
            update["account_name"] = derived["account_name"]
            update["account_name_source_url"] = derived.get("account_name_source_url", homepage.final_url)
            update["account_name_confidence_score"] = derived["account_name_confidence_score"]

        if derived.get("inferred_brand") and float(derived.get("brand_confidence_score", 0.0)) >= account.brand_confidence_score:
            update["inferred_brand"] = derived["inferred_brand"]
            update["brand_source_url"] = derived.get("brand_source_url", homepage.final_url)
            update["brand_confidence_score"] = derived["brand_confidence_score"]

        if (
            derived.get("account_city")
            and derived.get("account_state")
            and float(derived.get("location_confidence_score", 0.0)) >= account.location_confidence_score
        ):
            update["account_city"] = derived["account_city"]
            update["account_state"] = derived["account_state"]
            update["location_source_url"] = derived.get("location_source_url", homepage.final_url)
            update["location_confidence_score"] = derived["location_confidence_score"]

        if derived.get("account_phone") and float(derived.get("account_phone_confidence_score", 0.0)) >= account.account_phone_confidence_score:
            update["account_phone"] = derived["account_phone"]
            update["account_phone_source_url"] = derived.get("account_phone_source_url", homepage.final_url)
            update["account_phone_confidence_score"] = derived["account_phone_confidence_score"]
            update["website_phone"] = derived["account_phone"]
            update["website_phone_source_url"] = derived.get("account_phone_source_url", homepage.final_url)
            update["website_phone_confidence_score"] = derived["account_phone_confidence_score"]
            update["best_phone"] = derived["account_phone"]
            update["best_phone_source"] = "website"
            update["best_phone_source_url"] = derived.get("account_phone_source_url", homepage.final_url)
            update["best_phone_confidence_score"] = derived["account_phone_confidence_score"]

        return update

    def _candidate_homepages(self, account: AccountRecord) -> list[str]:
        """Build likely homepage URLs from the account domain."""

        domains = [account.website_url] if account.website_url else []
        base_domain = account.account_key or account.email_domain
        if base_domain:
            domains.extend(
                [
                    f"https://{base_domain}",
                    f"https://www.{base_domain}",
                    f"http://{base_domain}",
                    f"http://www.{base_domain}",
                ]
            )

        unique_urls: list[str] = []
        for url in domains:
            if not url:
                continue
            cleaned = url.strip().rstrip("/")
            if cleaned and cleaned not in unique_urls:
                unique_urls.append(cleaned)
        return unique_urls

    def _find_homepage(self, candidate_urls: list[str]) -> tuple[FetchResult | None, FetchResult | None]:
        """Fetch candidate homepages and return the best usable page plus blocked fallback."""

        blocked_result = None
        for url in candidate_urls:
            result = self.fetcher.fetch(url)
            if not result:
                continue
            if result.ok:
                return result, blocked_result
            if result.blocked and blocked_result is None:
                blocked_result = result
        return None, blocked_result

    def _resolve_homepage(self, candidate_urls: list[str]) -> str | None:
        """Resolve candidate homepages to a final URL when HTML fetching fails."""

        for url in candidate_urls:
            status_code, final_url = self.fetcher.resolve(url)
            if status_code and final_url and final_url != url:
                return final_url
            if status_code and final_url:
                return final_url
        return None

    def _collect_pages(self, homepage: FetchResult) -> list[FetchResult]:
        """Fetch a small set of high-signal pages from a confirmed homepage."""

        pages = [homepage]
        for path in STAFF_PAGE_HINTS[1:]:
            result = self.fetcher.fetch(urljoin(homepage.final_url + "/", path.lstrip("/")))
            if result and result.ok:
                pages.append(result)
        return pages

    def _probe_known_pages(self, base_url: str) -> list[FetchResult]:
        """Probe a few high-signal pages when the homepage itself is blocked."""

        pages: list[FetchResult] = []
        for path in STAFF_PAGE_HINTS:
            result = self.fetcher.fetch(urljoin(base_url.rstrip("/") + "/", path.lstrip("/")))
            if result and result.ok and all(existing.final_url != result.final_url for existing in pages):
                pages.append(result)
        return pages

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

    def _extract_best_values(self, pages: list[FetchResult], homepage: FetchResult) -> dict[str, object]:
        """Extract best current account values from fetched pages."""

        name_candidate: tuple[str, float, str] | None = None
        brand_candidate: tuple[str, float, str] | None = None
        location_candidate: tuple[str, str, float, str] | None = None
        phone_candidate: tuple[str, float, str] | None = None

        for page in pages:
            soup = BeautifulSoup(page.html, "html.parser")

            page_name = self._extract_name(soup)
            if page_name and (not name_candidate or page_name[1] > name_candidate[1]):
                name_candidate = (page_name[0], page_name[1], page.final_url)

            page_brand = self._extract_brand(
                soup=soup,
                page_url=page.final_url,
                account_name=page_name[0] if page_name else None,
            )
            if page_brand and (not brand_candidate or page_brand[1] > brand_candidate[1]):
                brand_candidate = (page_brand[0], page_brand[1], page.final_url)

            page_location = self._extract_location(soup, page.html)
            if page_location and (not location_candidate or page_location[2] > location_candidate[2]):
                location_candidate = (
                    page_location[0],
                    page_location[1],
                    page_location[2],
                    page.final_url,
                )

            page_phone = self._extract_phone(soup, page.html)
            if page_phone and (not phone_candidate or page_phone[1] > phone_candidate[1]):
                phone_candidate = (
                    page_phone[0],
                    page_phone[1],
                    page.final_url,
                )

        result: dict[str, object] = {
            "website_confidence_score": 0.95,
        }

        if name_candidate:
            result["account_name"] = name_candidate[0]
            result["account_name_confidence_score"] = name_candidate[1]
            result["account_name_source_url"] = name_candidate[2]

        if brand_candidate:
            result["inferred_brand"] = brand_candidate[0]
            result["brand_confidence_score"] = brand_candidate[1]
            result["brand_source_url"] = brand_candidate[2]

        if location_candidate:
            result["account_city"] = location_candidate[0]
            result["account_state"] = location_candidate[1]
            result["location_confidence_score"] = location_candidate[2]
            result["location_source_url"] = location_candidate[3]

        if phone_candidate:
            result["account_phone"] = phone_candidate[0]
            result["account_phone_confidence_score"] = phone_candidate[1]
            result["account_phone_source_url"] = phone_candidate[2]

        return result

    def _extract_name(self, soup: BeautifulSoup) -> tuple[str, float] | None:
        """Extract a dealership name from title, meta, or JSON-LD."""

        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            text = script.string or script.get_text(strip=True)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue

            candidates = payload if isinstance(payload, list) else [payload]
            for item in candidates:
                if isinstance(item, dict) and isinstance(item.get("name"), str):
                    cleaned = self._clean_name(item["name"])
                    if cleaned:
                        return cleaned, 0.95

        og_site_name = soup.find("meta", attrs={"property": "og:site_name"})
        if og_site_name and og_site_name.get("content"):
            cleaned = self._clean_name(og_site_name["content"])
            if cleaned:
                return cleaned, 0.9

        if soup.title and soup.title.string:
            cleaned = self._clean_name(soup.title.string)
            if cleaned:
                return cleaned, 0.78
        return None

    def _extract_brand(
        self,
        soup: BeautifulSoup,
        page_url: str,
        account_name: str | None,
    ) -> tuple[str, float] | None:
        """Infer the dealership brand from stronger, account-level signals."""

        candidate_texts: list[tuple[str, float]] = []
        if account_name:
            candidate_texts.append((account_name, 0.98))

        if soup.title and soup.title.string:
            candidate_texts.append((soup.title.string, 0.9))

        og_site_name = soup.find("meta", attrs={"property": "og:site_name"})
        if og_site_name and og_site_name.get("content"):
            candidate_texts.append((og_site_name["content"], 0.94))

        meta_description = soup.find("meta", attrs={"name": "description"})
        if meta_description and meta_description.get("content"):
            candidate_texts.append((meta_description["content"], 0.72))

        candidate_texts.append((page_url, 0.68))

        for text, confidence in candidate_texts:
            lowered = text.lower()
            for pattern, brand in BRAND_PATTERNS.items():
                if re.search(rf"\b{re.escape(pattern)}\b", lowered):
                    return brand, confidence
        return None

    def _extract_location(self, soup: BeautifulSoup, html: str) -> tuple[str, str, float] | None:
        """Extract city and state from JSON-LD or page text."""

        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            text = script.string or script.get_text(strip=True)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue

            candidates = payload if isinstance(payload, list) else [payload]
            for item in candidates:
                if not isinstance(item, dict):
                    continue
                address = item.get("address")
                if isinstance(address, dict):
                    city = self._normalize_city(address.get("addressLocality"))
                    state = self._normalize_state(address.get("addressRegion"))
                    if city and state:
                        return city, state, 0.95

        match = re.search(
            r"\b([A-Z][A-Za-z.\- ]+?),\s*(" + "|".join(sorted(US_STATE_CODES)) + r")\b",
            html,
        )
        if match:
            city = self._normalize_city(match.group(1))
            state = self._normalize_state(match.group(2))
            if city and state:
                return city, state, 0.72
        return None

    def _extract_phone(self, soup: BeautifulSoup, html: str) -> tuple[str, float] | None:
        """Extract a primary dealer phone number from JSON-LD, tel links, or page text."""

        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            text = script.string or script.get_text(strip=True)
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue

            candidates = payload if isinstance(payload, list) else [payload]
            for item in candidates:
                if isinstance(item, dict):
                    phone = self._normalize_phone(item.get("telephone"))
                    if phone:
                        return phone, 0.95

        for link in soup.find_all("a", href=True):
            href = str(link.get("href") or "")
            if href.lower().startswith("tel:"):
                phone = self._normalize_phone(href[4:])
                if phone:
                    return phone, 0.9

        match = PHONE_REGEX.search(html)
        if match:
            phone = self._normalize_phone(match.group(0))
            if phone:
                return phone, 0.72
        return None

    def _apply_updates(self, updates: list[dict[str, object]]) -> None:
        """Apply enrichment updates one row at a time for readable merge logic."""

        for update in updates:
            query = self._build_update_query(update)
            self.repository.execute_statement(query)

    def _build_update_query(self, update: dict[str, object]) -> str:
        """Build a safe update statement for one dealer account."""

        assignments: list[str] = []
        for field, value in update.items():
            if field == "dealer_account_id":
                continue
            if value == "CURRENT_TIMESTAMP()":
                assignments.append(f"{field} = CURRENT_TIMESTAMP()")
                continue
            if value is None:
                continue
            assignments.append(f"{field} = {self._sql_literal(value)}")

        assignments.append("updated_at = CURRENT_TIMESTAMP()")
        return f"""
        UPDATE `{self.settings.dealer_accounts_table_fqn}`
        SET
          {", ".join(assignments)}
        WHERE dealer_account_id = '{update["dealer_account_id"]}'
        """

    def _clean_name(self, raw_name: str) -> str | None:
        """Clean a site or organization name into a dealership-style label."""

        cleaned = re.sub(r"\s+", " ", raw_name).strip(" |-:\t\r\n")
        cleaned = re.sub(r"\b(Home|Homepage|New Vehicles|Used Vehicles|Service|Parts)\b.*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = cleaned.strip(" |-:\t\r\n")
        return cleaned or None

    def _normalize_city(self, city: object) -> str | None:
        """Normalize a city field."""

        if not isinstance(city, str):
            return None
        cleaned = re.sub(r"\s+", " ", city).strip(" ,")
        return cleaned.title() if cleaned else None

    def _normalize_state(self, state: object) -> str | None:
        """Normalize a state field."""

        if not isinstance(state, str):
            return None
        cleaned = state.strip().upper()
        return cleaned if cleaned in US_STATE_CODES else None

    def _normalize_phone(self, phone: object) -> str | None:
        """Normalize a phone value into a simple North American display format."""

        if not isinstance(phone, str):
            return None
        digits = re.sub(r"\D", "", phone)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) != 10:
            return None
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"

    def _sql_literal(self, value: object) -> str:
        """Format a Python value as a BigQuery SQL literal."""

        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        # BigQuery string literals use backslash escaping; doubled single quotes
        # can be parsed as adjacent literals in generated statements.
        escaped = (
            str(value)
            .replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        )
        return f"'{escaped}'"
