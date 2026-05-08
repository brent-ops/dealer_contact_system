"""Fallback GBP-style enrichment using search/provider results."""

from __future__ import annotations

from dataclasses import dataclass
import base64
from datetime import datetime
import json
import re
from typing import Any
from urllib.parse import parse_qs, quote_plus, urlparse

from bs4 import BeautifulSoup

from app.bigquery_repository import BigQueryRepository
from app.config import Settings
from app.logging_utils import get_logger
from app.web_fetcher import DEFAULT_HEADERS, NO_PROXY
import requests


logger = get_logger(__name__)

CANADA_PROVINCES = {
    "AB": "Alberta",
    "BC": "British Columbia",
    "MB": "Manitoba",
    "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador",
    "NS": "Nova Scotia",
    "NT": "Northwest Territories",
    "NU": "Nunavut",
    "ON": "Ontario",
    "PE": "Prince Edward Island",
    "QC": "Quebec",
    "SK": "Saskatchewan",
    "YT": "Yukon",
}

US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
    "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV",
    "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}
OEM_DOMAIN_TOKENS = (
    "acura",
    "audi",
    "bmw",
    "buick",
    "cadillac",
    "chevrolet",
    "chevy",
    "chrysler",
    "dodge",
    "fiat",
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
    "vw",
)

PHONE_REGEX = re.compile(r"(?:\+?1[\s.\-]?)?(?:\(?\d{3}\)?[\s.\-]?)\d{3}[\s.\-]?\d{4}")
CA_POSTAL_REGEX = re.compile(r"\b([A-Z]\d[A-Z]\s?\d[A-Z]\d)\b", re.IGNORECASE)
US_POSTAL_REGEX = re.compile(r"\b(\d{5}(?:-\d{4})?)\b")
STREET_SUFFIXES = {
    "st", "street", "ave", "avenue", "rd", "road", "blvd", "boulevard", "dr", "drive",
    "ln", "lane", "ct", "court", "cir", "circle", "pkwy", "parkway", "way", "pl", "place",
    "hwy", "highway",
}


@dataclass(frozen=True)
class GbpAccountCandidate:
    """Subset of account fields used for GBP enrichment."""

    dealer_account_id: str
    account_key: str
    account_name: str | None
    inferred_brand: str | None
    website_url: str | None
    account_city: str | None
    account_state: str | None
    account_phone: str | None
    website_phone: str | None
    best_phone: str | None
    best_phone_source: str | None
    blocked_attempt_count: int
    fetch_status: str | None


@dataclass(frozen=True)
class SearchResult:
    """One search-provider result."""

    title: str
    snippet: str
    url: str


@dataclass(frozen=True)
class GbpEnrichmentResult:
    """Summary from one GBP enrichment pass."""

    status: str
    detail: str
    enriched_accounts: int


class GbpEnrichmentService:
    """Enrich dealer accounts with fallback GBP-style phone and address signals."""

    CONNECTION_RECORD_ID = "gbp_enrichment_lane"
    BING_SEARCH_ENDPOINT = "https://www.bing.com/search"

    def __init__(self, repository: BigQueryRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(DEFAULT_HEADERS)

    def preview(self) -> dict[str, int]:
        """Return counts for accounts eligible for GBP enrichment."""

        query = f"""
        SELECT
          COUNT(*) AS candidate_accounts
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE dealer_classification IN ('dealer', 'dealer_group')
          AND (
            best_phone IS NULL
            OR TRIM(best_phone) = ''
            OR gbp_address_line IS NULL
            OR TRIM(gbp_address_line) = ''
            OR account_city IS NULL
            OR account_state IS NULL
            OR fetch_status = 'blocked'
          )
        """
        row = self.repository.fetch_one(query)
        return {"candidate_accounts": int(row.get("candidate_accounts", 0))}

    def enrich(
        self,
        dry_run: bool = False,
        limit: int | None = None,
        account_keys: list[str] | None = None,
    ) -> GbpEnrichmentResult:
        """Run one GBP enrichment batch."""

        accounts = self._load_accounts(limit=limit, account_keys=account_keys)
        updates: list[dict[str, Any]] = []
        for account in accounts:
            update = self._enrich_account(account)
            if update:
                updates.append(update)

        if dry_run:
            detail = (
                f"Would enrich {len(updates):,} of {len(accounts):,} candidate accounts "
                f"using {self.settings.gbp_provider}."
            )
            self._record_status("preview", detail)
            return GbpEnrichmentResult("preview", detail, len(updates))

        if updates:
            self._apply_updates(updates)
        detail = (
            f"GBP enrichment updated {len(updates):,} account(s) "
            f"using {self.settings.gbp_provider}."
        )
        self._record_status("healthy", detail)
        return GbpEnrichmentResult("success", detail, len(updates))

    def _load_accounts(
        self,
        limit: int | None,
        account_keys: list[str] | None,
    ) -> list[GbpAccountCandidate]:
        """Load accounts that would benefit from GBP enrichment."""

        row_limit = limit or self.settings.gbp_worker_batch_size
        account_filter = ""
        if account_keys:
            quoted = ", ".join(
                "'" + self._escape_sql(account_key.lower()) + "'"
                for account_key in account_keys
            )
            account_filter = f" AND account_key IN ({quoted})"

        query = f"""
        SELECT
          dealer_account_id,
          account_key,
          account_name,
          inferred_brand,
          website_url,
          account_city,
          account_state,
          account_phone,
          website_phone,
          best_phone,
          best_phone_source,
          IFNULL(blocked_attempt_count, 0) AS blocked_attempt_count,
          fetch_status
        FROM `{self.settings.dealer_accounts_table_fqn}`
        WHERE dealer_classification IN ('dealer', 'dealer_group')
          AND (
            best_phone IS NULL
            OR TRIM(best_phone) = ''
            OR gbp_address_line IS NULL
            OR TRIM(gbp_address_line) = ''
            OR account_city IS NULL
            OR account_state IS NULL
            OR fetch_status = 'blocked'
          )
          {account_filter}
        ORDER BY
          CASE
            WHEN fetch_status = 'blocked' THEN 0
            WHEN best_phone IS NULL OR TRIM(best_phone) = '' THEN 1
            ELSE 2
          END ASC,
          (
            CASE WHEN account_name IS NOT NULL AND TRIM(account_name) != '' THEN 1 ELSE 0 END
            + CASE WHEN inferred_brand IS NOT NULL AND TRIM(inferred_brand) != '' THEN 1 ELSE 0 END
            + CASE WHEN website_url IS NOT NULL AND TRIM(website_url) != '' THEN 1 ELSE 0 END
            + CASE WHEN account_city IS NOT NULL AND TRIM(account_city) != '' THEN 1 ELSE 0 END
            + CASE WHEN account_state IS NOT NULL AND TRIM(account_state) != '' THEN 1 ELSE 0 END
          ) DESC,
          IFNULL(last_verified_at, TIMESTAMP('1970-01-01')) ASC,
          account_key ASC
        LIMIT {row_limit}
        """
        rows = self.repository.fetch_all(query)
        return [
            GbpAccountCandidate(
                dealer_account_id=row["dealer_account_id"],
                account_key=row["account_key"],
                account_name=row.get("account_name"),
                inferred_brand=row.get("inferred_brand"),
                website_url=row.get("website_url"),
                account_city=row.get("account_city"),
                account_state=row.get("account_state"),
                account_phone=row.get("account_phone"),
                website_phone=row.get("website_phone"),
                best_phone=row.get("best_phone"),
                best_phone_source=row.get("best_phone_source"),
                blocked_attempt_count=int(row.get("blocked_attempt_count", 0) or 0),
                fetch_status=row.get("fetch_status"),
            )
            for row in rows
        ]

    def _enrich_account(self, account: GbpAccountCandidate) -> dict[str, Any] | None:
        """Resolve one account through the fallback search/provider lane."""

        results = self._search(account)
        best_candidate: dict[str, Any] | None = None
        best_score = 0.0
        for result in results[:5]:
            candidate = self._extract_candidate(account, result)
            if not candidate:
                continue
            score = float(candidate.get("confidence_score", 0.0))
            if score > best_score:
                best_score = score
                best_candidate = candidate

        if not best_candidate or best_score < 0.55:
            return None

        update: dict[str, Any] = {
            "dealer_account_id": account.dealer_account_id,
            "gbp_source": self.settings.gbp_provider,
            "gbp_phone_source_url": best_candidate.get("source_url"),
            "gbp_last_verified_at": datetime.utcnow().isoformat(),
        }

        if best_candidate.get("display_name"):
            update["gbp_display_name"] = best_candidate["display_name"]
        if best_candidate.get("phone"):
            update["gbp_phone"] = best_candidate["phone"]
            update["gbp_phone_confidence_score"] = best_candidate["confidence_score"]
        if best_candidate.get("address_line"):
            update["gbp_address_line"] = best_candidate["address_line"]
        if best_candidate.get("city"):
            update["gbp_city"] = best_candidate["city"]
        if best_candidate.get("state_or_province"):
            update["gbp_state_or_province"] = best_candidate["state_or_province"]
        if best_candidate.get("postal_code"):
            update["gbp_postal_code"] = best_candidate["postal_code"]
        if best_candidate.get("country"):
            update["gbp_country"] = best_candidate["country"]
        update["gbp_address_confidence_score"] = best_candidate["confidence_score"]

        if not account.best_phone and best_candidate.get("phone"):
            update["best_phone"] = best_candidate["phone"]
            update["best_phone_source"] = "gbp"
            update["best_phone_source_url"] = best_candidate.get("source_url")
            update["best_phone_confidence_score"] = best_candidate["confidence_score"]

        return update

    def _search(self, account: GbpAccountCandidate) -> list[SearchResult]:
        """Search for business listing signals using a public HTML search endpoint."""

        results: list[SearchResult] = []
        seen_urls: set[str] = set()
        for query in self._build_search_queries(account):
            query_results = self._search_query_variants(account, query)
            for result in query_results:
                if not result.url or result.url in seen_urls:
                    continue
                seen_urls.add(result.url)
                results.append(result)
            if len(results) >= 12:
                break
        return results

    def _search_query_variants(self, account: GbpAccountCandidate, query: str) -> list[SearchResult]:
        """Search one query against preferred and fallback public search endpoints."""

        query_results = self._search_duckduckgo(query)
        if query_results:
            return query_results
        logger.info("GBP search falling back to Bing | account=%s | query=%s", account.account_key, query)
        return self._search_bing(query)

    def _search_duckduckgo(self, query: str) -> list[SearchResult]:
        """Search the configured DuckDuckGo HTML endpoint."""

        try:
            response = self.session.get(
                f"{self.settings.gbp_search_endpoint}?q={quote_plus(query)}",
                timeout=self.settings.request_timeout_seconds,
                proxies=NO_PROXY,
            )
            response.raise_for_status()
        except requests.RequestException:
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        results: list[SearchResult] = []
        for result in soup.select(".result"):
            link = result.select_one(".result__a")
            if not link:
                continue
            snippet = result.select_one(".result__snippet")
            href = self._normalize_result_url(str(link.get("href") or ""))
            if not href:
                continue
            results.append(
                SearchResult(
                    title=link.get_text(" ", strip=True),
                    snippet=snippet.get_text(" ", strip=True) if snippet else "",
                    url=href,
                )
            )
        return results

    def _search_bing(self, query: str) -> list[SearchResult]:
        """Search Bing HTML as a public fallback when DuckDuckGo is blocked or empty."""

        try:
            response = requests.get(
                f"{self.BING_SEARCH_ENDPOINT}?q={quote_plus(query)}",
                timeout=self.settings.request_timeout_seconds,
                headers={"User-Agent": "Mozilla/5.0"},
                proxies=NO_PROXY,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("GBP Bing fallback failed | query=%s | error=%s", query, exc)
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        results: list[SearchResult] = []
        for result in soup.select("li.b_algo"):
            link = result.select_one("h2 a")
            if not link:
                continue
            snippet = result.select_one(".b_caption p")
            href = self._normalize_result_url(str(link.get("href") or ""))
            if not href:
                continue
            results.append(
                SearchResult(
                    title=link.get_text(" ", strip=True),
                    snippet=snippet.get_text(" ", strip=True) if snippet else "",
                    url=href,
                )
            )
        return results

    def _build_search_queries(self, account: GbpAccountCandidate) -> list[str]:
        """Build a few stronger search variants for thin dealer account records."""

        domain_phrase = self._domain_phrase(account.account_key)
        raw_domain_root = self._raw_domain_root(account.account_key)
        name_phrase = self._clean_search_name(account.account_name)
        brand_phrase = (account.inferred_brand or "").strip()
        city = self._clean_search_location_part(account.account_city)
        state = self._clean_search_location_part(account.account_state)

        location = " ".join(part for part in [city, state] if part).strip()
        anchor = name_phrase or " ".join(part for part in [brand_phrase, domain_phrase] if part).strip() or domain_phrase or account.account_key

        candidates = [
            " ".join(part for part in [f'\"{anchor}\"' if anchor else "", location, "dealership phone address"] if part).strip(),
            " ".join(part for part in [f'\"{anchor}\"' if anchor else "", location, "dealer phone address"] if part).strip(),
            " ".join(part for part in [brand_phrase, domain_phrase, location, "dealership contact"] if part).strip(),
            " ".join(part for part in [domain_phrase, location, "dealer address phone"] if part).strip(),
            " ".join(part for part in [raw_domain_root, location, "dealer phone address"] if part).strip(),
            " ".join(part for part in [raw_domain_root, "contact"] if part).strip(),
            " ".join(part for part in [f"{raw_domain_root}.com", "contact"] if part).strip(),
        ]

        queries: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            normalized = re.sub(r"\s+", " ", candidate).strip()
            if not normalized:
                continue
            key = normalized.lower()
            if key in seen:
                continue
            seen.add(key)
            queries.append(normalized)
        return queries[:6]

    def _clean_search_name(self, value: str | None) -> str:
        """Trim noisy marketing copy from account names before using them in search."""

        if not value:
            return ""
        cleaned = str(value).strip()
        for separator in ("|", " - ", " — "):
            if separator in cleaned:
                cleaned = cleaned.split(separator, 1)[0].strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned

    def _clean_search_location_part(self, value: str | None) -> str:
        """Drop obviously noisy pseudo-location text from search inputs."""

        if not value:
            return ""
        cleaned = str(value).strip()
        lowered = cleaned.lower()
        if any(token in lowered for token in ("dealership", "promise", "visit ", "sale in ", "serving ")):
            return ""
        if len(cleaned.split()) > 4:
            return ""
        return cleaned

    def _extract_candidate(self, account: GbpAccountCandidate, result: SearchResult) -> dict[str, Any] | None:
        """Extract phone/address fields from one search result and its page."""

        page_text = ""
        json_ld_candidate: dict[str, Any] | None = None
        try:
            response = self.session.get(
                result.url,
                timeout=self.settings.request_timeout_seconds,
                proxies=NO_PROXY,
            )
            if "text/html" in response.headers.get("content-type", ""):
                soup = BeautifulSoup(response.text, "html.parser")
                page_text = soup.get_text(" ", strip=True)[:10000]
                json_ld_candidate = self._extract_json_ld_business(soup)
        except requests.RequestException:
            page_text = ""

        combined_text = " ".join(
            part for part in [result.title, result.snippet, page_text] if part
        )
        if not self._text_matches_account(account, f"{combined_text} {result.url}"):
            return None

        phone = None
        address: dict[str, str] | None = None
        display_name = None
        if json_ld_candidate:
            display_name = json_ld_candidate.get("name")
            phone = self._normalize_phone(json_ld_candidate.get("telephone"))
            address = self._normalize_address_dict(json_ld_candidate.get("address"))

        if not phone:
            phone = self._extract_phone(combined_text)
        if not address:
            address = self._extract_address(combined_text)
        if not display_name:
            display_name = result.title

        if not phone and not address:
            return None

        confidence = 0.25
        if phone:
            confidence += 0.35
        if address:
            confidence += 0.30
        if self._text_matches_account(account, combined_text):
            confidence += 0.10

        return {
            "display_name": display_name,
            "phone": phone,
            "address_line": (address or {}).get("address_line"),
            "city": (address or {}).get("city"),
            "state_or_province": (address or {}).get("state_or_province"),
            "postal_code": (address or {}).get("postal_code"),
            "country": (address or {}).get("country"),
            "confidence_score": min(confidence, 0.95),
            "source_url": result.url,
        }

    def _extract_json_ld_business(self, soup: BeautifulSoup) -> dict[str, Any] | None:
        """Extract a LocalBusiness-like JSON-LD object when present."""

        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                payload = json.loads(script.string or "")
            except (TypeError, json.JSONDecodeError):
                continue
            for item in self._walk_json_ld(payload):
                item_type = item.get("@type")
                if isinstance(item_type, list):
                    item_type = " ".join(str(value) for value in item_type)
                if isinstance(item_type, str) and any(
                    token in item_type.lower()
                    for token in ("localbusiness", "automotiv", "store", "organization")
                ):
                    return item
        return None

    def _walk_json_ld(self, payload: Any) -> list[dict[str, Any]]:
        """Flatten nested JSON-LD objects into dictionaries."""

        items: list[dict[str, Any]] = []
        if isinstance(payload, dict):
            if "@graph" in payload and isinstance(payload["@graph"], list):
                for child in payload["@graph"]:
                    items.extend(self._walk_json_ld(child))
            else:
                items.append(payload)
        elif isinstance(payload, list):
            for child in payload:
                items.extend(self._walk_json_ld(child))
        return items

    def _extract_phone(self, text: str) -> str | None:
        """Extract one normalized North American phone number."""

        match = PHONE_REGEX.search(text)
        if not match:
            return None
        return self._normalize_phone(match.group(0))

    def _normalize_phone(self, value: Any) -> str | None:
        """Normalize a phone value into a standard display format."""

        if not isinstance(value, str):
            return None
        digits = re.sub(r"\D", "", value)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) != 10:
            return None
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"

    def _extract_address(self, text: str) -> dict[str, str] | None:
        """Extract a best-effort North American address from free text."""

        if "|" in text:
            for segment in [part.strip() for part in text.split("|") if part.strip()]:
                segmented = self._extract_segment_address(segment)
                if segmented:
                    return segmented

        canada_match = re.search(
            r"(?P<address>\d{1,6}[^,\n]{3,90}),\s*(?P<city>[A-Za-z .'\-]+),\s*"
            r"(?P<province>AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT|Alberta|British Columbia|Manitoba|New Brunswick|Newfoundland and Labrador|Nova Scotia|Northwest Territories|Nunavut|Ontario|Prince Edward Island|Quebec|Saskatchewan|Yukon)"
            r"\s*(?P<postal>[A-Z]\d[A-Z]\s?\d[A-Z]\d)",
            text,
            re.IGNORECASE,
        )
        if canada_match:
            province = canada_match.group("province").upper()
            province = province if province in CANADA_PROVINCES else canada_match.group("province").title()
            return {
                "address_line": self._clean_address_line(canada_match.group("address")),
                "city": canada_match.group("city").strip(),
                "state_or_province": province,
                "postal_code": canada_match.group("postal").upper().replace(" ", ""),
                "country": "Canada",
            }

        us_match = re.search(
            r"(?P<address>\d{1,6}[\w .'\-#]{3,90}?),\s*(?P<city>[A-Z][A-Za-z.'\-]+(?:\s+[A-Z][A-Za-z.'\-]+)*),\s*(?P<state>[A-Z]{2})\s*(?P<postal>\d{5}(?:-\d{4})?)",
            text,
        )
        if us_match and us_match.group("state").upper() in US_STATE_CODES:
            return {
                "address_line": self._clean_address_line(us_match.group("address")),
                "city": us_match.group("city").strip(),
                "state_or_province": us_match.group("state").upper(),
                "postal_code": us_match.group("postal"),
                "country": "United States",
            }

        us_inline_match = re.search(
            r"(?P<address>\d{1,6}[\w .'\-#]{3,90}?)\s+(?P<city>[A-Z][A-Za-z.'\-]+(?:\s+[A-Z][A-Za-z.'\-]+)*),\s*(?P<state>[A-Z]{2})\s*(?P<postal>\d{5}(?:-\d{4})?)",
            text,
        )
        if us_inline_match and us_inline_match.group("state").upper() in US_STATE_CODES:
            return {
                "address_line": self._clean_address_line(us_inline_match.group("address")),
                "city": us_inline_match.group("city").strip(),
                "state_or_province": us_inline_match.group("state").upper(),
                "postal_code": us_inline_match.group("postal"),
                "country": "United States",
            }

        return None

    def _extract_segment_address(self, text: str) -> dict[str, str] | None:
        """Extract an address from a compact result-title segment."""

        comma_parts = text.rsplit(",", 1)
        if len(comma_parts) != 2:
            return None
        left, right = comma_parts[0].strip(), comma_parts[1].strip()

        ca_postal = CA_POSTAL_REGEX.search(right)
        us_postal = US_POSTAL_REGEX.search(right)
        state_match = re.search(r"\b([A-Z]{2})\b", right)
        if not state_match or (not ca_postal and not us_postal):
            return None

        state = state_match.group(1).upper()
        words = left.split()
        if len(words) < 3 or not re.match(r"^\d", words[0]):
            return None

        city_word_count = 2 if len(words) >= 5 and words[-2][0:1].isupper() and words[-1][0:1].isupper() else 1
        city_words = words[-city_word_count:]
        address_words = words[:-city_word_count]
        if not address_words:
            return None
        if address_words[-1].strip(".,").lower() not in STREET_SUFFIXES and city_word_count == 2 and len(words) >= 6:
            city_words = words[-1:]
            address_words = words[:-1]

        postal_code = ca_postal.group(1).upper().replace(" ", "") if ca_postal else us_postal.group(1)
        country = "Canada" if ca_postal or state in CANADA_PROVINCES else "United States"
        return {
            "address_line": " ".join(address_words).strip(),
            "city": " ".join(city_words).strip(),
            "state_or_province": state,
            "postal_code": postal_code,
            "country": country,
        }

    def _normalize_address_dict(self, address: Any) -> dict[str, str] | None:
        """Normalize a JSON-LD address payload."""

        if not isinstance(address, dict):
            return None
        country = str(address.get("addressCountry") or "").strip()
        state = str(address.get("addressRegion") or "").strip()
        postal = str(address.get("postalCode") or "").strip()
        if not any([country, state, postal, address.get("streetAddress"), address.get("addressLocality")]):
            return None
        normalized_country = self._normalize_country(country, state, postal)
        return {
            "address_line": self._clean_address_line(str(address.get("streetAddress") or "")),
            "city": str(address.get("addressLocality") or "").strip() or None,
            "state_or_province": state or None,
            "postal_code": postal or None,
            "country": normalized_country,
        }

    def _clean_address_line(self, value: str) -> str | None:
        """Clean noisy title/snippet prefixes from an extracted address line."""

        cleaned = value.strip()
        if "|" in cleaned:
            cleaned = cleaned.split("|")[-1].strip()
        cleaned = re.sub(r"\s{2,}", " ", cleaned)
        return cleaned or None

    def _normalize_country(self, country: str, state: str, postal: str) -> str | None:
        """Normalize country from explicit value or postal/state clues."""

        lowered = country.lower()
        if lowered in {"ca", "canada"}:
            return "Canada"
        if lowered in {"us", "usa", "united states", "united states of america"}:
            return "United States"
        if CA_POSTAL_REGEX.search(postal or "") or state.upper() in CANADA_PROVINCES:
            return "Canada"
        if US_POSTAL_REGEX.search(postal or "") or state.upper() in US_STATE_CODES:
            return "United States"
        return country or None

    def _text_matches_account(self, account: GbpAccountCandidate, text: str) -> bool:
        """Return True when the result text appears relevant to the account."""

        lowered = text.lower()
        if account.account_name and account.account_name.lower() in lowered:
            return True
        if account.inferred_brand and account.inferred_brand.lower() in lowered:
            return True
        if account.account_city and account.account_city.lower() in lowered:
            return True
        domain_phrase = self._domain_phrase(account.account_key)
        if domain_phrase and domain_phrase in lowered:
            return True
        return False

    def _domain_phrase(self, value: str | None) -> str:
        """Convert a domain/account key into a search-friendly phrase."""

        if not value:
            return ""
        raw = str(value).strip().lower()
        raw = re.sub(r"^https?://", "", raw)
        raw = raw.split("/", 1)[0]
        raw = raw.removeprefix("www.")
        root = raw.split(".", 1)[0]
        for token in sorted(OEM_DOMAIN_TOKENS, key=len, reverse=True):
            if root.endswith(token) and root != token:
                root = f"{root[:-len(token)]} {token}"
                break
        phrase = re.sub(r"[-_]+", " ", root)
        phrase = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", phrase)
        phrase = re.sub(r"\s+", " ", phrase).strip()
        return phrase

    def _raw_domain_root(self, value: str | None) -> str:
        """Return the unsplit root label from an account key/domain."""

        if not value:
            return ""
        raw = str(value).strip().lower()
        raw = re.sub(r"^https?://", "", raw)
        raw = raw.split("/", 1)[0]
        raw = raw.removeprefix("www.")
        return raw.split(".", 1)[0]

    def _normalize_result_url(self, url: str) -> str:
        """Resolve DuckDuckGo redirect URLs into target URLs."""

        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        uddg = query_params.get("uddg")
        if uddg:
            return uddg[0]
        if parsed.netloc.endswith("bing.com"):
            encoded = query_params.get("u", [""])[0]
            decoded = self._decode_bing_target(encoded)
            if decoded:
                return decoded
        return url

    def _decode_bing_target(self, value: str) -> str | None:
        """Decode Bing redirect targets when present."""

        if not value:
            return None
        candidate = value
        if candidate.startswith("a1"):
            candidate = candidate[2:]
        padding = "=" * (-len(candidate) % 4)
        try:
            decoded = base64.b64decode(candidate + padding).decode("utf-8", errors="ignore")
        except Exception:
            return None
        return decoded if decoded.startswith("http") else None

    def _apply_updates(self, updates: list[dict[str, Any]]) -> None:
        """Write GBP enrichment updates to dealer accounts."""

        for update in updates:
            assignments: list[str] = []
            for key, value in update.items():
                if key == "dealer_account_id":
                    continue
                assignments.append(f"{key} = {self._sql_literal(value)}")
            assignments.append("updated_at = CURRENT_TIMESTAMP()")
            query = f"""
            UPDATE `{self.settings.dealer_accounts_table_fqn}`
            SET
              {", ".join(assignments)}
            WHERE dealer_account_id = '{self._escape_sql(str(update['dealer_account_id']))}'
            """
            self.repository.execute_statement(query)

    def _record_status(self, sync_status: str, detail: str) -> None:
        """Upsert the GBP enrichment lane status into sync_targets."""

        query = f"""
        MERGE `{self.settings.sync_targets_table_fqn}` AS target
        USING (
          SELECT
            'gbp_enrichment' AS target_system,
            'business_profile_fallback' AS target_entity_type,
            '{self._escape_sql(self.settings.gbp_provider)}' AS target_entity_id,
            'system' AS source_record_type,
            '{self.CONNECTION_RECORD_ID}' AS source_record_id,
            '{self._escape_sql(sync_status)}' AS sync_status
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
        logger.info("GBP enrichment status recorded | status=%s | detail=%s", sync_status, detail)

    def _sql_literal(self, value: Any) -> str:
        """Convert a Python value to a BigQuery SQL literal."""

        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, datetime):
            return f"TIMESTAMP('{value.isoformat()}')"
        return f"'{self._escape_sql(str(value))}'"

    def _escape_sql(self, value: str) -> str:
        """Escape one string for a BigQuery string literal."""

        return (
            value.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        )
