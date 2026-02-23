"""Autotrader dealer scraper: sitemap parser, HTTP fetcher, JSON-LD extractor."""

import gzip
import json
import logging
import random
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from crawlers.stealth import USER_AGENTS

logger = logging.getLogger(__name__)

# Stealth headers for httpx requests (subset of browser headers)
STEALTH_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

# URL pattern: /car-dealers/{city-state}/{dealer-id}/{dealer-slug}
AUTOTRADER_URL_PATTERN = re.compile(r"/car-dealers/([^/]+)/([^/]+)/([^/?]+)")


@dataclass
class AutotraderDealer:
    """Structured data extracted from an Autotrader dealer page."""

    # From URL
    autotrader_url: str = ""
    autotrader_dealer_id: str = ""
    dealer_slug: str = ""
    city_state: str = ""

    # From JSON-LD
    name: str = ""
    phone: str = ""
    street_address: str = ""
    city: str = ""
    state: str = ""
    postal_code: str = ""
    rating_value: Optional[float] = None
    review_count: Optional[int] = None
    hours: list[dict[str, str]] = field(default_factory=list)

    # From HTML
    website_url: str = ""
    inventory_count: Optional[int] = None

    @property
    def full_address(self) -> str:
        parts = [self.street_address, self.city, self.state, self.postal_code]
        return ", ".join(p for p in parts if p)

    @property
    def domain(self) -> str:
        if not self.website_url:
            return f"autotrader-{self.autotrader_dealer_id}"
        try:
            parsed = urlparse(self.website_url)
            host = parsed.hostname or ""
            return host.removeprefix("www.")
        except Exception:
            return f"autotrader-{self.autotrader_dealer_id}"


def parse_autotrader_url(url: str) -> tuple[str, str, str]:
    """Parse an Autotrader dealer URL into (dealer_id, city_state, slug).

    Args:
        url: Full Autotrader dealer URL.

    Returns:
        Tuple of (dealer_id, city_state, slug).

    Raises:
        ValueError: If URL doesn't match expected pattern.
    """
    match = AUTOTRADER_URL_PATTERN.search(url)
    if not match:
        raise ValueError(f"URL does not match Autotrader dealer pattern: {url}")
    city_state, dealer_id, slug = match.groups()
    return dealer_id, city_state, slug


async def fetch_sitemap_urls(
    client: httpx.AsyncClient,
    sitemap_url: str,
    local_path: Optional[str] = None,
) -> list[str]:
    """Download and parse the Autotrader dealer sitemap.

    Tries local_path first (decoded XML), falls back to downloading the gzipped sitemap.

    Args:
        client: httpx async client.
        sitemap_url: URL of the gzipped sitemap XML.
        local_path: Optional path to a pre-downloaded decoded XML file.

    Returns:
        List of dealer page URLs.
    """
    xml_content: Optional[bytes] = None

    # Try local file first
    if local_path:
        local = Path(local_path)
        if local.exists():
            logger.info(f"Reading local sitemap from {local_path}")
            xml_content = local.read_bytes()

    # Download if no local file
    if xml_content is None:
        logger.info(f"Downloading sitemap from {sitemap_url}")
        resp = await client.get(sitemap_url, follow_redirects=True, timeout=60.0)
        resp.raise_for_status()
        raw = resp.content
        # Decompress if gzipped
        if sitemap_url.endswith(".gz") or raw[:2] == b"\x1f\x8b":
            xml_content = gzip.decompress(raw)
        else:
            xml_content = raw

    # Parse XML
    root = ET.fromstring(xml_content)
    # Handle namespace: sitemap XML uses {http://www.sitemaps.org/schemas/sitemap/0.9}
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for loc in root.findall(".//sm:loc", ns):
        if loc.text:
            url = loc.text.strip()
            if "/car-dealers/" in url:
                urls.append(url)

    # Fallback: try without namespace
    if not urls:
        for loc in root.iter("loc"):
            if loc.text:
                url = loc.text.strip()
                if "/car-dealers/" in url:
                    urls.append(url)

    logger.info(f"Parsed {len(urls)} dealer URLs from sitemap")
    return urls


async def fetch_dealer_page(
    client: httpx.AsyncClient,
    url: str,
    timeout: float = 30.0,
) -> Optional[str]:
    """Fetch a dealer page via HTTP GET with rotated User-Agent.

    Args:
        client: httpx async client.
        url: Dealer page URL.
        timeout: Request timeout in seconds.

    Returns:
        HTML content string, or None on failure.
    """
    headers = {**STEALTH_HEADERS, "User-Agent": random.choice(USER_AGENTS)}
    try:
        resp = await client.get(url, headers=headers, follow_redirects=True, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
        logger.warning(f"HTTP {resp.status_code} for {url}")
        return None
    except httpx.TimeoutException:
        logger.warning(f"Timeout fetching {url}")
        return None
    except Exception as e:
        logger.warning(f"Error fetching {url}: {e}")
        return None


def extract_dealer_data(html: str, url: str) -> Optional[AutotraderDealer]:
    """Extract structured dealer data from an Autotrader page.

    Parses JSON-LD @type:AutoDealer for core data, scrapes HTML for website URL
    and inventory count.

    Args:
        html: Raw HTML content of the dealer page.
        url: The page URL (for metadata extraction).

    Returns:
        AutotraderDealer instance, or None if extraction fails entirely.
    """
    try:
        dealer_id, city_state, slug = parse_autotrader_url(url)
    except ValueError:
        logger.warning(f"Cannot parse dealer URL: {url}")
        return None

    soup = BeautifulSoup(html, "lxml")

    dealer = AutotraderDealer(
        autotrader_url=url,
        autotrader_dealer_id=dealer_id,
        dealer_slug=slug,
        city_state=city_state,
    )

    # Extract JSON-LD data
    jsonld = _extract_jsonld(soup)
    if jsonld:
        dealer.name = jsonld.get("name", "")
        dealer.phone = jsonld.get("telephone", "")

        address = jsonld.get("address", {})
        if isinstance(address, dict):
            dealer.street_address = address.get("streetAddress", "")
            dealer.city = address.get("addressLocality", "")
            dealer.state = address.get("addressRegion", "")
            dealer.postal_code = address.get("postalCode", "")

        rating = jsonld.get("aggregateRating", {})
        if isinstance(rating, dict):
            try:
                dealer.rating_value = float(rating.get("ratingValue", 0))
            except (ValueError, TypeError):
                pass
            try:
                dealer.review_count = int(rating.get("reviewCount", 0))
            except (ValueError, TypeError):
                pass

        # Opening hours
        hours_specs = jsonld.get("openingHoursSpecification", [])
        if isinstance(hours_specs, list):
            for spec in hours_specs:
                if isinstance(spec, dict):
                    dealer.hours.append(
                        {
                            "days": spec.get("dayOfWeek", ""),
                            "opens": spec.get("opens", ""),
                            "closes": spec.get("closes", ""),
                        }
                    )
    else:
        # Fallback: derive name from slug
        dealer.name = slug.replace("-", " ").title()

    # Extract website URL
    dealer.website_url = _extract_dealer_website(soup)

    # Extract inventory count
    dealer.inventory_count = _extract_inventory_count(soup)

    return dealer


def _extract_jsonld(soup: BeautifulSoup) -> Optional[dict]:
    """Extract AutoDealer JSON-LD from the page.

    Args:
        soup: Parsed BeautifulSoup object.

    Returns:
        JSON-LD dict for the AutoDealer, or None.
    """
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue

        # Handle single object or list
        items = data if isinstance(data, list) else [data]
        for item in items:
            if isinstance(item, dict):
                item_type = item.get("@type", "")
                if item_type in ("AutoDealer", "AutoBodyShop", "LocalBusiness"):
                    return item
                # Check @graph
                for graph_item in item.get("@graph", []):
                    if isinstance(graph_item, dict):
                        gt = graph_item.get("@type", "")
                        if gt in ("AutoDealer", "AutoBodyShop", "LocalBusiness"):
                            return graph_item
    return None


def _extract_dealer_website(soup: BeautifulSoup) -> str:
    """Extract the dealer's own website URL from the page.

    Strategy:
    1. Links with text matching "dealer website" / "visit website" patterns.
    2. Links with data-cmp attributes containing "website".
    3. External links in dealer info sections.

    Args:
        soup: Parsed BeautifulSoup object.

    Returns:
        Website URL string, or empty string if not found.
    """
    website_text_patterns = re.compile(
        r"(dealer\s*website|visit\s*(dealer\s*)?website|view\s*website|dealer\s*site)",
        re.IGNORECASE,
    )

    # Strategy 1: text-based
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        if website_text_patterns.search(text):
            href = a["href"]
            if href.startswith("http") and "autotrader.com" not in href:
                return href

    # Strategy 2: data-cmp attribute
    for a in soup.find_all("a", attrs={"data-cmp": True}):
        cmp = (a.get("data-cmp") or "").lower()
        if "website" in cmp:
            href = a.get("href", "")
            if href.startswith("http") and "autotrader.com" not in href:
                return href

    # Strategy 3: external links in dealer info sections
    info_selectors = [
        "[class*='dealer-info']",
        "[class*='dealerInfo']",
        "[class*='dealer-detail']",
        "[data-cmp*='dealer']",
    ]
    for selector in info_selectors:
        for section in soup.select(selector):
            for a in section.find_all("a", href=True):
                href = a["href"]
                if (
                    href.startswith("http")
                    and "autotrader.com" not in href
                    and "autocheck.com" not in href
                    and "facebook.com" not in href
                    and "google.com" not in href
                    and "yelp.com" not in href
                    and "kbb.com" not in href
                    and "coxautoinc.com" not in href
                    and "coxenterprises.com" not in href
                    and "apple.com" not in href
                    and "play.google.com" not in href
                ):
                    return href

    return ""


def _extract_inventory_count(soup: BeautifulSoup) -> Optional[int]:
    """Extract total inventory count from the page.

    Looks for text patterns like "123 vehicles" or "showing 1-25 of 456".

    Args:
        soup: Parsed BeautifulSoup object.

    Returns:
        Integer vehicle count, or None if not found.
    """
    text = soup.get_text(" ", strip=True)

    # Pattern: "X vehicles" or "X cars" or "X listings"
    patterns = [
        re.compile(r"(\d[\d,]*)\s+(?:vehicles?|cars?|listings?)\s+(?:found|available|for sale)", re.IGNORECASE),
        re.compile(r"showing\s+\d+[-\u2013]\d+\s+of\s+(\d[\d,]*)", re.IGNORECASE),
        re.compile(r"(\d[\d,]*)\s+(?:new|used)?\s*(?:vehicles?|cars?)\s+for\s+sale", re.IGNORECASE),
    ]

    for pattern in patterns:
        match = pattern.search(text)
        if match:
            try:
                return int(match.group(1).replace(",", ""))
            except ValueError:
                continue

    return None
