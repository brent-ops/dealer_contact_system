"""Dealer account validation and classification."""

from __future__ import annotations

from dataclasses import dataclass
import re
import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger
from app.web_fetcher import WebFetcher


logger = get_logger(__name__)
MIN_WRITE_CONFIDENCE = 0.8
SERIALIZATION_RETRY_ATTEMPTS = 5
SERIALIZATION_RETRY_BASE_SECONDS = 1.0

OEM_PATTERNS = [
    r"\bford motor company\b",
    r"\btoyota motor\b",
    r"\bgeneral motors\b",
    r"\bhonda motor\b",
    r"\bnissan motor\b",
]

OEM_ROOT_HOSTS = {
    "audi.com",
    "ford.com",
    "toyota.com",
    "honda.com",
    "chevrolet.com",
    "nissanusa.com",
}

BRAND_ALIAS_PATTERN = re.compile(
    r"^(?:\d+|[a-z])(?:[a-z0-9-]*)(acura|audi|bmw|buick|cadillac|chevrolet|chevy|chrysler|dodge|ford|gmc|honda|hyundai|jeep|kia|lexus|lincoln|mazda|nissan|porsche|ram|subaru|toyota|volkswagen|vw)\.[a-z]{2,}$",
    re.IGNORECASE,
)

AUDIOF_AUDI_PATTERN = re.compile(
    r"^audiof[a-z0-9-]+\.[a-z]{2,}$",
    re.IGNORECASE,
)

AUDIOFTYPO_AUDI_PATTERN = re.compile(
    r"^audi[a-z0-9-]{3,}\.[a-z]{2,}$",
    re.IGNORECASE,
)

BRAND_KEYWORDS = [
    "acura",
    "audi",
    "bmw",
    "buick",
    "cadillac",
    "chevrolet",
    "chevy",
    "chrysler",
    "dodge",
    "ford",
    "gmc",
    "honda",
    "hyundai",
    "infiniti",
    "jeep",
    "kia",
    "lexus",
    "lincoln",
    "mazda",
    "mercedes",
    "mini",
    "mitsubishi",
    "nissan",
    "porsche",
    "ram",
    "subaru",
    "toyota",
    "volkswagen",
    "volvo",
]

VENDOR_PATTERNS = [
    r"\bcox automotive\b",
    r"\breynolds (?:and|&) reynolds\b",
    r"\bdealertrack\b",
    r"\bautomotive software\b",
    r"\bdealer crm\b",
    r"\bautomotive retail technology\b",
    r"\btitle and registration processing\b",
    r"\bdealership title\b",
    r"\btitle clerk education\b",
    r"\bnationwide auto dealership\b",
    r"\bcdp\b",
    r"\blead platform\b",
    r"\bintent-based marketing\b",
    r"\bautomotive systems and components manufacturer\b",
    r"\bcomponents manufacturers?\b",
    r"\bdigital marketing\b",
    r"\binternet marketing agency\b",
    r"\bauction(s)?\b",
    r"\bwholesale auction\b",
    r"\bdealer marketing\b",
    r"\bdealer services\b",
    r"\bautomotive customer data\b",
    r"\bautomotive audience\b",
    r"\bautomotive data platform\b",
    r"\bdealer solutions\b",
    r"\bf&i selling system\b",
    r"\bfinance and insurance\b",
    r"\baged inventory\b",
    r"\bmobility data\b",
    r"\bdata and analytics company\b",
    r"\bmarketing platform\b",
    r"\btelematics\b",
    r"\btrucking industry\b",
    r"\btrailer needs\b",
    r"\bheavy-haul distributor\b",
    r"\btruck bodies\b",
]

VENDOR_HOST_HINTS = [
    "activator.ai",
    "mazdaleads.com",
    "affinitiv.com",
    "acvauctions.com",
    "adesa.com",
    "askpatty.com",
    "autotrader.com",
    "autonation.com",
    "dealer.com",
    "dealerspace.com",
    "cmdlr.com",
]

OEM_SUBDOMAIN_HINTS = [
    ".ford.com",
    ".toyota.com",
    ".honda.com",
    ".chevrolet.com",
    ".nissanusa.com",
]

OEM_HOST_HINTS = [
    "hondaweb.com",
]

NON_DEALER_PATTERNS = [
    r"\bhome care\b",
    r"\bhealth care\b",
    r"\bmedical\b",
    r"\bclinic\b",
    r"\bdental\b",
    r"\bcharity cars\b",
    r"\bvehicle donation\b",
    r"\blaw firm\b",
    r"\binsurance agency\b",
    r"\bmarketing agency\b",
    r"\bconsulting services\b",
    r"\bstrategy\b",
    r"\badvisory\b",
    r"\bmanaged services\b",
    r"\bconstruction\b",
    r"\broofing\b",
    r"\buniversity\b",
    r"\bstaffing\b",
    r"\belectricity\b",
    r"\bpower company\b",
    r"\baviation\b",
    r"\baircraft\b",
    r"\bit services\b",
    r"\bcyber security\b",
    r"\bmicrosoft certified solutions partner\b",
    r"\berp\b",
    r"\bcloud\b",
    r"\bofficial website\b",
    r"\bmoving beyond smoking\b",
    r"\bsmoking\b",
    r"\btobacco\b",
    r"\bforest products\b",
    r"\bgravel mining\b",
    r"\btree farm\b",
    r"\bhardware\b",
    r"\bdesign studio\b",
    r"\bgraphic design\b",
    r"\bui/ux\b",
]

AUTOMOTIVE_INDUSTRY_PATTERNS = [
    r"\bautomotive accessories\b",
    r"\bvehicle wraps\b",
    r"\bwindow tint(?:ing)?\b",
    r"\bpaint protection\b",
    r"\bceramic coatings\b",
    r"\blift kits\b",
    r"\bleveling kits\b",
    r"\bbed covers\b",
    r"\bcamper shells\b",
    r"\boffroad\b",
    r"\bstyle & performance\b",
    r"\bdetailing\b",
    r"\bwheel(?:s)? and tire(?:s)?\b",
    r"\bsuspension\b",
    r"\bautomotive services\b",
    r"\bautomotive styling\b",
    r"\bupfitting\b",
    r"\btrailer hitches\b",
]

DEALER_GROUP_PATTERNS = [
    r"\bauto group\b",
    r"\bautomotive group\b",
    r"\bdealer group\b",
    r"\bautoplex\b",
    r"\bmotors group\b",
]

DEALER_PATTERNS = [
    r"\bnew inventory\b",
    r"\bused inventory\b",
    r"\bschedule test drive\b",
    r"\bvalue my trade\b",
    r"\bservice department\b",
    r"\bcertified pre-owned\b",
    r"\bparts department\b",
    r"\bfinance department\b",
    r"\bshop new\b",
    r"\bshop used\b",
    r"\bnew vehicles\b",
    r"\bused vehicles\b",
    r"\btrade-in\b",
    r"\bapply for financing\b",
    r"\bschedule service\b",
    r"\boil change\b",
    r"\bcar dealership\b",
    r"\bauto dealership\b",
    r"\bbuy your next vehicle\b",
]

INVENTORY_URL_HINTS = [
    "inventory",
    "new-inventory",
    "used-inventory",
    "vehicle-details",
    "new vehicles",
    "used vehicles",
    "cars-for-sale",
    "certified",
    "pre-owned",
]

INVENTORY_PATH_PROBES = [
    "/new-inventory/index.htm",
    "/used-inventory/index.htm",
    "/inventory/index.htm",
    "/new-inventory",
    "/used-inventory",
    "/cars-for-sale",
    "/search/",
    "/search",
]

INDEPENDENT_DEALER_KEYWORDS = [
    "auto",
    "autos",
    "autoland",
    "automall",
    "autoplex",
    "motors",
    "cars",
    "autosales",
    "sales",
    "used",
]

ROOFTOP_BRAND_ALIASES = [
    "benz",
    "gm",
    "vw",
]


@dataclass(frozen=True)
class ValidationPreview:
    """Summary of current dealer classification coverage."""

    total_accounts: int
    validated_accounts: int


@dataclass(frozen=True)
class ValidationResult:
    """Best current dealer classification for one account."""

    dealer_account_id: str
    account_key: str
    classification: str
    confidence_score: float
    source_url: str
    evidence_summary: str


class DealerValidationService:
    """Classify accounts as dealers, non-dealers, vendors, OEMs, or unknown."""

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.fetcher = WebFetcher(timeout_seconds=settings.request_timeout_seconds)

    def preview(self) -> ValidationPreview:
        """Return current validation coverage."""

        query = f"""
        SELECT
          COUNT(*) AS total_accounts,
          COUNTIF(dealer_classification IS NOT NULL AND TRIM(dealer_classification) != '') AS validated_accounts
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE IFNULL(is_personal_domain, FALSE) = FALSE
        """
        row = self.repository.fetch_one(query)
        return ValidationPreview(
            total_accounts=int(row.get("total_accounts", 0)),
            validated_accounts=int(row.get("validated_accounts", 0)),
        )

    def validate(
        self,
        dry_run: bool = False,
        limit: int | None = None,
        account_keys: list[str] | None = None,
    ) -> None:
        """Validate a batch of accounts and write best current classifications."""

        preview = self.preview()
        logger.info(
            "Dealer validation preview | total_accounts=%s | validated_accounts=%s | dry_run=%s",
            preview.total_accounts,
            preview.validated_accounts,
            dry_run,
        )

        accounts = self._load_accounts(limit=limit, account_keys=account_keys)
        logger.info("Loaded dealer validation batch | accounts=%s", len(accounts))

        results: list[ValidationResult] = []
        for account in accounts:
            result = self._validate_account(account)
            if result:
                results.append(result)

        logger.info(
            "Prepared dealer validation updates | updates=%s | dry_run=%s",
            len(results),
            dry_run,
        )

        for result in results:
            logger.info(
                "Dealer validation result | account_key=%s | classification=%s | confidence=%.2f | source_url=%s | evidence=%s",
                result.account_key,
                result.classification,
                result.confidence_score,
                result.source_url,
                result.evidence_summary,
            )

        if dry_run:
            return

        for result in results:
            if not self._should_write_result(result):
                logger.info(
                    "Skipped low-confidence dealer validation write | account_key=%s | classification=%s | confidence=%.2f",
                    result.account_key,
                    result.classification,
                    result.confidence_score,
                )
                self._mark_checked(result.dealer_account_id)
                continue
            self._apply_result(result)

    def _load_accounts(self, limit: int | None, account_keys: list[str] | None) -> list[dict[str, str]]:
        """Load accounts ready for validation."""

        row_limit = limit or self.settings.enrichment_batch_size
        account_filter = ""
        if account_keys:
            quoted = ", ".join("'" + account_key.lower().replace("'", "''") + "'" for account_key in account_keys)
            account_filter = f" AND account_key IN ({quoted})"
        query = f"""
        SELECT
          dealer_account_id,
          account_key,
          account_name,
          inferred_brand,
          website_url
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE IFNULL(is_personal_domain, FALSE) = FALSE
          {account_filter}
        ORDER BY
          IFNULL(dealer_validation_checked_at, TIMESTAMP('1970-01-01')) ASC,
          account_key ASC
        LIMIT {row_limit}
        """
        return [dict(row.items()) for row in self.repository.run_query(query)]

    def _validate_account(self, account: dict[str, str]) -> ValidationResult | None:
        """Classify one account from its best current website/account signals."""

        candidate_urls = self._candidate_urls(account)
        source_url = candidate_urls[0]
        page = self._fetch_first_candidate(candidate_urls)
        page_text = ""
        inventory_signal = False
        page_status = None
        resolved_url = source_url
        if page:
            page_status = page.status_code
            source_url = page.final_url
            resolved_url = page.final_url
        if page:
            soup = BeautifulSoup(page.html, "html.parser")
            title_text = soup.title.get_text(" ", strip=True) if soup.title else ""
            meta_description = ""
            meta_tag = soup.find("meta", attrs={"name": "description"})
            if meta_tag and meta_tag.get("content"):
                meta_description = meta_tag["content"]
            og_site_name = ""
            og_tag = soup.find("meta", attrs={"property": "og:site_name"})
            if og_tag and og_tag.get("content"):
                og_site_name = og_tag["content"]
            page_text = " ".join(
                [
                    title_text,
                    meta_description,
                    og_site_name,
                    soup.get_text(" ", strip=True),
                ]
            )
            inventory_signal = self._has_inventory_signal(soup, page.final_url)
            if not inventory_signal:
                inventory_signal = self._probe_inventory_paths(account["account_key"], source_url)
        else:
            page_status, resolved_url = self._resolve_first_candidate(candidate_urls)
            source_url = resolved_url
            inventory_signal = self._probe_inventory_paths(account["account_key"], resolved_url)

        evidence = " ".join(
            [
                account.get("account_key") or "",
                account.get("account_name") or "",
                account.get("inferred_brand") or "",
                resolved_url,
                page_text[:12000],
                "account_key_brand_hint" if self._contains_brand_keyword(account.get("account_key") or "") else "",
                "inventory_page_detected" if inventory_signal else "",
                "blocked_site" if page_status == 403 else "",
            ]
        ).lower()

        classification, confidence = self._score_classification(evidence, source_url)
        evidence_summary = self._build_evidence_summary(evidence, inventory_signal)
        return ValidationResult(
            dealer_account_id=account["dealer_account_id"],
            account_key=account["account_key"],
            classification=classification,
            confidence_score=confidence,
            source_url=source_url,
            evidence_summary=evidence_summary,
        )

    def _score_classification(self, evidence: str, source_url: str) -> tuple[str, float]:
        """Choose the best dealer classification from website evidence."""

        host = self._normalized_host(source_url)

        if self._is_vendor_host(host):
            return "vendor", 0.96

        if self._is_oem_host(host):
            return "oem", 0.95

        if self._matches_any(evidence, VENDOR_PATTERNS):
            return "vendor", 0.92

        if self._matches_any(evidence, NON_DEALER_PATTERNS):
            return "non_dealer", 0.9

        if self._matches_any(evidence, DEALER_GROUP_PATTERNS) and (
            self._looks_like_brand_dealer_domain(evidence)
            or self._looks_like_brand_alias_dealer(evidence)
            or "account_key_brand_hint" in evidence
        ):
            return "dealer", 0.93

        if self._matches_any(evidence, DEALER_GROUP_PATTERNS) and self._matches_any(evidence, DEALER_PATTERNS):
            return "dealer_group", 0.92

        if self._looks_like_brand_dealer_domain(evidence):
            return "dealer", 0.9

        if self._looks_like_brand_alias_dealer(evidence):
            return "dealer", 0.89

        if self._looks_like_rooftop_brand_domain(evidence):
            return "dealer", 0.86

        if "blocked_site" in evidence and "inventory_page_detected" in evidence and self._contains_brand_keyword(evidence) and self._has_automotive_context(evidence):
            return "dealer", 0.96

        if "blocked_site" in evidence and "inventory_page_detected" in evidence and self._contains_independent_dealer_keyword(evidence) and self._has_automotive_context(evidence):
            return "dealer", 0.93

        if "inventory_page_detected" in evidence and self._contains_brand_keyword(evidence) and self._has_automotive_context(evidence):
            return "dealer", 0.95

        if "inventory_page_detected" in evidence and self._contains_independent_dealer_keyword(evidence) and self._has_automotive_context(evidence):
            return "dealer", 0.91

        if self._matches_any(evidence, AUTOMOTIVE_INDUSTRY_PATTERNS):
            return "vendor", 0.88

        if self._matches_any(evidence, DEALER_PATTERNS) and self._contains_brand_keyword(evidence) and self._has_automotive_context(evidence):
            return "dealer", 0.93

        if self._matches_any(evidence, DEALER_PATTERNS) and self._has_automotive_context(evidence):
            return "dealer", 0.9

        if self._matches_any(evidence, OEM_PATTERNS) and not self._contains_independent_dealer_keyword(evidence):
            return "oem", 0.88

        if self._contains_brand_keyword(evidence) and self._has_automotive_context(evidence) and any(keyword in evidence for keyword in ["inventory", "service", "finance", "test drive", "trade"]):
            return "dealer", 0.84

        if any(keyword in host for keyword in ["software", "consulting", "agency", "vendor", "crm", "marketing"]):
            return "vendor", 0.82

        if any(keyword in evidence for keyword in ["dmv", "title processing", "registration processing"]) and "dealership" in evidence:
            return "vendor", 0.84

        if any(keyword in evidence for keyword in ["accessories", "tint", "wraps", "detailing", "offroad", "suspension", "wheels and tires"]):
            return "vendor", 0.82

        if any(keyword in host for keyword in ["care", "health", "clinic", "hospital", "dental", "medical"]):
            return "non_dealer", 0.82

        return "unknown", 0.45

    def _build_evidence_summary(self, evidence: str, inventory_signal: bool) -> str:
        """Return a compact explanation of which signal family matched."""

        source_url = self._extract_first_url(evidence)
        host = self._normalized_host(source_url)

        if self._is_vendor_host(host):
            return "Matched a known automotive platform or vendor host."

        if self._is_oem_host(host):
            return "Matched an OEM manufacturer host."

        if self._matches_any(evidence, DEALER_GROUP_PATTERNS) and (
            self._looks_like_brand_dealer_domain(evidence)
            or self._looks_like_brand_alias_dealer(evidence)
            or "account_key_brand_hint" in evidence
        ):
            return "Matched dealer-group signals, but the domain itself looks like a specific rooftop dealer."

        if self._matches_any(evidence, DEALER_GROUP_PATTERNS) and self._matches_any(evidence, DEALER_PATTERNS):
            return "Matched both dealership and dealer-group patterns."

        if self._matches_any(evidence, VENDOR_PATTERNS):
            return "Matched automotive vendor patterns."

        if self._matches_any(evidence, AUTOMOTIVE_INDUSTRY_PATTERNS):
            return "Matched automotive industry or specialty-shop patterns."

        if self._looks_like_brand_dealer_domain(evidence):
            return "Matched a brand-containing dealer domain pattern."

        if self._looks_like_brand_alias_dealer(evidence):
            return "Matched a brand-heavy alias domain pattern that strongly suggests a dealer."

        if self._looks_like_rooftop_brand_domain(evidence):
            return "Matched a rooftop-style domain that includes an automotive brand."

        if "blocked_site" in evidence and inventory_signal and self._contains_brand_keyword(evidence):
            return "Blocked site, but inventory paths and brand cues strongly indicate a dealer."

        if "blocked_site" in evidence and inventory_signal and self._contains_independent_dealer_keyword(evidence):
            return "Blocked site, but inventory paths and independent-dealer cues strongly indicate a dealer."

        if inventory_signal and self._contains_brand_keyword(evidence):
            return "Detected inventory pages with automotive brand cues."

        if inventory_signal and self._contains_independent_dealer_keyword(evidence):
            return "Detected inventory pages with independent-dealer cues."

        if inventory_signal:
            return "Detected inventory or vehicle-listing pages."

        if self._matches_any(evidence, DEALER_PATTERNS) and self._contains_brand_keyword(evidence):
            return "Matched dealership inventory/service patterns with automotive brand cues."

        if self._matches_any(evidence, DEALER_PATTERNS):
            return "Matched dealership inventory/service patterns."

        if self._matches_any(evidence, NON_DEALER_PATTERNS):
            return "Matched non-dealer business patterns."

        if self._matches_any(evidence, OEM_PATTERNS):
            return "Matched OEM manufacturer patterns."

        return "No strong pattern match; fallback classification applied."

    def _should_write_result(self, result: ValidationResult) -> bool:
        """Return True when a validation result is strong enough to store."""

        return result.classification != "unknown" and result.confidence_score >= MIN_WRITE_CONFIDENCE

    def _matches_any(self, evidence: str, patterns: list[str]) -> bool:
        """Return True when any regex pattern matches the evidence string."""

        return any(re.search(pattern, evidence, re.IGNORECASE) for pattern in patterns)

    def _normalized_host(self, value: str) -> str:
        """Normalize a URL or host string into a lowercase hostname."""

        host = (urlparse(value).netloc or value).lower().strip()
        host = host.removeprefix("www.")
        return host.split(":", 1)[0]

    def _is_vendor_host(self, host: str) -> bool:
        """Return True when the resolved host is a known automotive platform/vendor."""

        return any(host == hint or host.endswith("." + hint) for hint in VENDOR_HOST_HINTS)

    def _is_oem_host(self, host: str) -> bool:
        """Return True when the host is an official OEM site or OEM subdomain."""

        if host in OEM_ROOT_HOSTS:
            return True
        if any(host == hint or host.endswith("." + hint) for hint in OEM_HOST_HINTS):
            return True
        return any(host.endswith(suffix) for suffix in OEM_SUBDOMAIN_HINTS)

    def _extract_first_url(self, evidence: str) -> str:
        """Extract the first URL-like token from the evidence blob."""

        match = re.search(r"https?://[^\s]+", evidence)
        return match.group(0) if match else ""

    def _candidate_urls(self, account: dict[str, str]) -> list[str]:
        """Build likely homepage URLs for validation, including http fallbacks."""

        urls: list[str] = []
        if account.get("website_url"):
            urls.append(str(account["website_url"]).rstrip("/"))

        base_domain = (account.get("account_key") or "").strip().lower()
        if base_domain:
            urls.extend(
                [
                    f"https://{base_domain}",
                    f"https://www.{base_domain}",
                    f"http://{base_domain}",
                    f"http://www.{base_domain}",
                ]
            )

        unique_urls: list[str] = []
        for url in urls:
            if url and url not in unique_urls:
                unique_urls.append(url)
        return unique_urls

    def _fetch_first_candidate(self, candidate_urls: list[str]):
        """Return the first usable fetch result from the candidate URL list."""

        fallback = None
        for url in candidate_urls:
            result = self.fetcher.fetch(url)
            if not result:
                continue
            if result.ok:
                return result
            if fallback is None:
                fallback = result
        return fallback

    def _resolve_first_candidate(self, candidate_urls: list[str]) -> tuple[int | None, str]:
        """Resolve candidate URLs when full-page fetching fails."""

        for url in candidate_urls:
            status_code, final_url = self.fetcher.resolve(url)
            if status_code is not None:
                return status_code, final_url
        return None, candidate_urls[0]

    def _contains_brand_keyword(self, evidence: str) -> bool:
        """Return True when evidence includes a common automotive brand word."""

        normalized = re.sub(r"[^a-z]+", " ", evidence.lower())
        if any(re.search(rf"\b{re.escape(keyword)}\b", normalized, re.IGNORECASE) for keyword in BRAND_KEYWORDS):
            return True

        domain_tokens = re.findall(r"[a-z0-9]+", evidence.lower())
        return any(token in BRAND_KEYWORDS for token in domain_tokens)

    def _has_automotive_context(self, evidence: str) -> bool:
        """Return True when the evidence clearly refers to automotive retail or vehicles."""

        context_patterns = [
            r"\bauto\b",
            r"\bautos\b",
            r"\bautomotive\b",
            r"\bdealer\b",
            r"\bdealership\b",
            r"\bvehicle\b",
            r"\bvehicles\b",
            r"\bcar\b",
            r"\bcars\b",
            r"\btruck\b",
            r"\btrucks\b",
            r"\bsuv\b",
            r"\bsuvs\b",
            r"\bvan\b",
            r"\bvans\b",
            r"\bpickup\b",
            r"\bpre-owned\b",
            r"\bcertified pre-owned\b",
        ]
        return self._matches_any(evidence, context_patterns)

    def _looks_like_brand_alias_dealer(self, evidence: str) -> bool:
        """Return True when evidence contains a dealer-style alias domain with an OEM brand."""

        tokens = re.findall(r"[a-z0-9.-]+\.[a-z]{2,}", evidence.lower())
        return any(
            BRAND_ALIAS_PATTERN.match(token)
            or AUDIOF_AUDI_PATTERN.match(token)
            or AUDIOFTYPO_AUDI_PATTERN.match(token)
            for token in tokens
        )

    def _looks_like_brand_dealer_domain(self, evidence: str) -> bool:
        """Return True for domains that combine an OEM brand with dealer business words."""

        tokens = re.findall(r"[a-z0-9.-]+\.[a-z]{2,}", evidence.lower())
        dealer_terms = {"sales", "autoplex", "motors", "autos", "auto", "cars"}
        for token in tokens:
            normalized = re.sub(r"[^a-z]+", " ", token)
            has_brand = any(re.search(rf"\b{re.escape(keyword)}\b", normalized, re.IGNORECASE) for keyword in BRAND_KEYWORDS) or any(keyword in token for keyword in BRAND_KEYWORDS)
            has_dealer_term = any(re.search(rf"\b{re.escape(term)}\b", normalized, re.IGNORECASE) for term in dealer_terms) or any(term in token for term in dealer_terms)
            if has_brand and has_dealer_term:
                return True
        return False

    def _looks_like_rooftop_brand_domain(self, evidence: str) -> bool:
        """Return True for root domains that include a likely automotive brand string."""

        tokens = re.findall(r"[a-z0-9.-]+\.[a-z]{2,}", evidence.lower())
        for token in tokens:
            host = self._normalized_host(token)
            if not host or self._is_vendor_host(host) or self._is_oem_host(host):
                continue

            root = host.split(".", 1)[0]
            if any(alias in root and root != alias for alias in ROOFTOP_BRAND_ALIASES):
                return True

            if any(keyword in root and root != keyword for keyword in BRAND_KEYWORDS):
                return True

        return False

    def _contains_independent_dealer_keyword(self, evidence: str) -> bool:
        """Return True when evidence includes generic dealer-style business words."""

        normalized = re.sub(r"[^a-z]+", " ", evidence.lower())
        if any(re.search(rf"\b{re.escape(keyword)}\b", normalized, re.IGNORECASE) for keyword in INDEPENDENT_DEALER_KEYWORDS):
            return True

        domain_tokens = re.findall(r"[a-z0-9]+", evidence.lower())
        return any(token in INDEPENDENT_DEALER_KEYWORDS for token in domain_tokens)

    def _has_inventory_signal(self, soup: BeautifulSoup, page_url: str) -> bool:
        """Return True when the homepage exposes likely vehicle inventory paths or labels."""

        base_netloc = urlparse(page_url).netloc
        matches = 0
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            anchor_text = anchor.get_text(" ", strip=True).lower()
            try:
                full_url = urljoin(page_url, href)
            except ValueError:
                continue
            parsed = urlparse(full_url)
            if parsed.netloc and parsed.netloc != base_netloc:
                continue
            haystack = f"{href.lower()} {anchor_text}"
            if any(hint in haystack for hint in INVENTORY_URL_HINTS) and self._looks_vehicle_specific(haystack):
                matches += 1
            if matches >= 1:
                return True
        return False

    def _looks_vehicle_specific(self, haystack: str) -> bool:
        """Return True when an inventory-like link appears to refer to vehicle shopping."""

        negative_tokens = [
            "news",
            "paint",
            "hardware",
            "portfolio",
            "design",
            "marketing",
            "credit",
            "supplies",
            "blog",
            "press-release",
        ]
        if any(token in haystack for token in negative_tokens):
            return False

        positive_tokens = [
            "inventory",
            "vehicle",
            "cars-for-sale",
            "pre-owned",
            "certified",
            "new-inventory",
            "used-inventory",
        ]
        return any(token in haystack for token in positive_tokens)

    def _probe_inventory_paths(self, account_key: str, source_url: str) -> bool:
        """Probe common inventory URLs for dealer-style sites that block homepage fetches."""

        dealer_signal = (
            self._contains_brand_keyword(account_key)
            or self._contains_brand_keyword(source_url)
            or self._contains_independent_dealer_keyword(account_key)
            or self._contains_independent_dealer_keyword(source_url)
        )
        if not dealer_signal:
            return False

        base_url = source_url.rstrip("/")
        for path in INVENTORY_PATH_PROBES:
            result = self.fetcher.fetch(urljoin(base_url + "/", path.lstrip("/")))
            if not result:
                continue
            final_url = result.final_url.lower()
            if any(token in final_url for token in ["inventory", "new", "used", "vehicle"]) and (
                self._contains_brand_keyword(final_url) or self._contains_brand_keyword(account_key)
            ):
                return True
            if result.status_code == 403 and path in result.url.lower():
                return True
        return False

    def _apply_result(self, result: ValidationResult) -> None:
        """Write a validation result back into dealer_accounts."""

        query = f"""
        UPDATE `{self.settings.dealer_accounts_table_fqn}`
        SET
          dealer_classification = CASE
            WHEN dealer_classification_confidence_score IS NULL
              OR {result.confidence_score} >= dealer_classification_confidence_score
              OR (
                dealer_classification != '{result.classification}'
                AND {result.confidence_score} >= GREATEST(IFNULL(dealer_classification_confidence_score, 0.0) - 0.10, {MIN_WRITE_CONFIDENCE})
              )
              OR (
                dealer_classification = 'vendor'
                AND '{result.classification}' IN ('dealer', 'dealer_group', 'non_dealer', 'oem')
              )
            THEN '{result.classification}'
            ELSE dealer_classification
          END,
          dealer_classification_confidence_score = CASE
            WHEN dealer_classification_confidence_score IS NULL
              OR {result.confidence_score} >= dealer_classification_confidence_score
              OR (
                dealer_classification != '{result.classification}'
                AND {result.confidence_score} >= GREATEST(IFNULL(dealer_classification_confidence_score, 0.0) - 0.10, {MIN_WRITE_CONFIDENCE})
              )
              OR (
                dealer_classification = 'vendor'
                AND '{result.classification}' IN ('dealer', 'dealer_group', 'non_dealer', 'oem')
              )
            THEN {result.confidence_score}
            ELSE dealer_classification_confidence_score
          END,
          dealer_classification_source_url = CASE
            WHEN dealer_classification_confidence_score IS NULL
              OR {result.confidence_score} >= dealer_classification_confidence_score
              OR (
                dealer_classification != '{result.classification}'
                AND {result.confidence_score} >= GREATEST(IFNULL(dealer_classification_confidence_score, 0.0) - 0.10, {MIN_WRITE_CONFIDENCE})
              )
              OR (
                dealer_classification = 'vendor'
                AND '{result.classification}' IN ('dealer', 'dealer_group', 'non_dealer', 'oem')
              )
            THEN '{result.source_url.replace("'", "''")}'
            ELSE dealer_classification_source_url
          END,
          dealer_validation_checked_at = CURRENT_TIMESTAMP(),
          last_verified_at = CURRENT_TIMESTAMP(),
          updated_at = CURRENT_TIMESTAMP()
        WHERE dealer_account_id = '{result.dealer_account_id}'
        """
        self._execute_account_update_with_retry(query, result.dealer_account_id)

    def _mark_checked(self, dealer_account_id: str) -> None:
        """Record that an account was reviewed even if no trusted classification was saved."""

        query = f"""
        UPDATE `{self.settings.dealer_accounts_table_fqn}`
        SET
          dealer_validation_checked_at = CURRENT_TIMESTAMP(),
          updated_at = CURRENT_TIMESTAMP()
        WHERE dealer_account_id = '{dealer_account_id}'
        """
        self._execute_account_update_with_retry(query, dealer_account_id)

    def _execute_account_update_with_retry(self, query: str, dealer_account_id: str) -> None:
        """Retry transient BigQuery serialization conflicts for account-table writes."""

        delay_seconds = SERIALIZATION_RETRY_BASE_SECONDS
        for attempt in range(1, SERIALIZATION_RETRY_ATTEMPTS + 1):
            try:
                self.repository.execute_statement(query)
                return
            except Exception as exc:
                message = str(exc)
                is_serialization_conflict = "Could not serialize access to table" in message
                if not is_serialization_conflict or attempt >= SERIALIZATION_RETRY_ATTEMPTS:
                    raise
                logger.warning(
                    "Retrying dealer validation account write after serialization conflict | dealer_account_id=%s | attempt=%s | delay_seconds=%.1f",
                    dealer_account_id,
                    attempt,
                    delay_seconds,
                )
                time.sleep(delay_seconds)
                delay_seconds *= 2
