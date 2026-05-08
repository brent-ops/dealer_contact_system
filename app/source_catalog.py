"""Catalog of external CSV seed files and their intended segmentation metadata."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceFeed:
    """One external CSV feed that can later be imported into the seed pipeline."""

    file_name: str
    audience_type: str
    market: str
    country: str
    inferred_brand: str | None
    source_group: str
    notes: str


SOURCE_FEEDS: tuple[SourceFeed, ...] = (
    SourceFeed(
        file_name="GreenLineDigital-AudiDealers_May2024.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand="Audi",
        source_group="oem_seed",
        notes="Manufacturer-specific dealer seed list.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-AutomotivePPCCleaned.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand=None,
        source_group="automotive_services",
        notes="Automotive PPC audience; likely mixed automotive prospects.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-BMWDealers_May2024.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand="BMW",
        source_group="oem_seed",
        notes="Manufacturer-specific dealer seed list.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-CombiningSubscribers6.1.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand=None,
        source_group="combined_subscribers",
        notes="Mixed subscriber source; normalize and dedupe before promotion.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-CurrentClients.csv",
        audience_type="current_client",
        market="US",
        country="United States",
        inferred_brand=None,
        source_group="current_clients",
        notes="Current client audience; keep segmented separately from prospects.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-GMCanada.csv",
        audience_type="prospect",
        market="Canada",
        country="Canada",
        inferred_brand="General Motors",
        source_group="canada_oem_seed",
        notes="Canada-specific list; isolate for advertising-rule compliance.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-GoingBeyondNADA-Email1-openersandnonopenerswithoutaclick.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand=None,
        source_group="engagement_list",
        notes="Campaign engagement list; preserve source attribution on import.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-KenectOpenEmailList.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand=None,
        source_group="engagement_list",
        notes="Openers list from Kenect campaign activity.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-NADADealershipEmails.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand=None,
        source_group="industry_seed",
        notes="NADA dealership email list; likely mixed OEM prospects.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-NADAEmailOpeners.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand=None,
        source_group="engagement_list",
        notes="NADA engagement/openers list.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-PearlDiver10.25.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand=None,
        source_group="mixed_seed",
        notes="Mixed audience seed; expect more personal-email noise.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-PorscheDealers_May2024.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand="Porsche",
        source_group="oem_seed",
        notes="Manufacturer-specific dealer seed list.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-SpecialsPageManagementEblast1Openers.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand=None,
        source_group="engagement_list",
        notes="Openers list from specials-page management campaign.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-SubaruDealers-Oct2023.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand="Subaru",
        source_group="oem_seed",
        notes="Manufacturer-specific dealer seed list.",
    ),
    SourceFeed(
        file_name="GreenLineDigital-Volkswagen-7.27.csv",
        audience_type="prospect",
        market="US",
        country="United States",
        inferred_brand="Volkswagen",
        source_group="oem_seed",
        notes="Manufacturer-specific dealer seed list.",
    ),
)
