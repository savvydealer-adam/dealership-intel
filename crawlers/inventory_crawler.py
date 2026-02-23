"""Crawl dealership inventory pages to count new/used vehicles."""

import logging
import re
from typing import Any, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from config.platforms import PLATFORM_SIGNATURES
from crawlers.stealth import dismiss_cookie_consent, human_delay

logger = logging.getLogger(__name__)

# Generic inventory paths to try
DEFAULT_NEW_PATHS = [
    "/new-inventory",
    "/new-vehicles",
    "/inventory/new",
    "/new",
    "/inventory?type=new",
    "/VehicleSearchResults?search=new",
]

DEFAULT_USED_PATHS = [
    "/used-inventory",
    "/used-vehicles",
    "/inventory/used",
    "/pre-owned",
    "/pre-owned-vehicles",
    "/inventory?type=used",
    "/VehicleSearchResults?search=used",
    "/certified-pre-owned",
]


class InventoryCrawler:
    """Counts new and used inventory from dealership websites."""

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    async def crawl_inventory(self, page, base_url: str, platform: Optional[str] = None) -> dict[str, Any]:
        """Crawl inventory pages and count vehicles.

        Args:
            page: Pyppeteer page instance.
            base_url: Dealership homepage URL.
            platform: Detected platform name (for platform-specific paths).

        Returns:
            Dict with new_count, used_count, and metadata.
        """
        result: dict[str, Any] = {
            "new_count": None,
            "used_count": None,
            "new_url": None,
            "used_url": None,
        }

        # Get platform-specific paths
        new_paths, used_paths = self._get_inventory_paths(platform)

        # Count new inventory
        for path in new_paths:
            url = urljoin(base_url, path)
            count = await self._count_vehicles(page, url)
            if count is not None:
                result["new_count"] = count
                result["new_url"] = url
                break

        await human_delay(0.5, 1.0)

        # Count used inventory
        for path in used_paths:
            url = urljoin(base_url, path)
            count = await self._count_vehicles(page, url)
            if count is not None:
                result["used_count"] = count
                result["used_url"] = url
                break

        logger.info(f"Inventory for {base_url}: new={result['new_count']}, used={result['used_count']}")
        return result

    async def _count_vehicles(self, page, url: str) -> Optional[int]:
        """Navigate to an inventory page and extract the vehicle count."""
        try:
            response = await page.goto(url, {"timeout": self.timeout * 1000, "waitUntil": "domcontentloaded"})
            if not response or response.status >= 400:
                return None

            await dismiss_cookie_consent(page)
            await human_delay(0.5, 1.0)

            html = await page.content()

            # Strategy 1: Look for result count text
            count = self._extract_count_from_text(html)
            if count is not None:
                return count

            # Strategy 2: Count vehicle cards on page
            count = self._count_vehicle_cards(html)
            if count and count > 0:
                return count

            return None

        except Exception as e:
            logger.debug(f"Inventory page failed for {url}: {e}")
            return None

    def _extract_count_from_text(self, html: str) -> Optional[int]:
        """Extract vehicle count from text like 'Showing 1-25 of 142 vehicles'."""
        patterns = [
            r"(\d+)\s+(?:vehicle|car|result|match)s?\s+(?:found|available|in stock)",
            r"(?:showing|displaying)\s+\d+\s*-\s*\d+\s+of\s+(\d+)",
            r"(\d+)\s+(?:new|used|pre-owned)\s+(?:vehicle|car)s?\s+(?:for sale|available|in stock)",
            r"total[:\s]+(\d+)",
            r"(\d+)\s+results?",
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                count = int(match.group(1))
                if 1 <= count <= 10000:  # Sanity check
                    return count

        return None

    def _count_vehicle_cards(self, html: str) -> Optional[int]:
        """Count vehicle listing cards on the page."""
        soup = BeautifulSoup(html, "lxml")

        card_selectors = [
            "[class*='vehicle-card']",
            "[class*='inventory-item']",
            "[class*='vehicle-listing']",
            "[class*='srp-listing']",
            "[class*='vehicle_card']",
            "[class*='listing-item']",
            "[data-vehicle-id]",
            ".vehicle",
            ".inventory-listing",
        ]

        for selector in card_selectors:
            cards = soup.select(selector)
            if len(cards) >= 2:  # At least 2 to avoid false positives
                return len(cards)

        return None

    def _get_inventory_paths(self, platform: Optional[str]) -> tuple[list[str], list[str]]:
        """Get platform-specific inventory URL paths."""
        if platform and platform in PLATFORM_SIGNATURES:
            info = PLATFORM_SIGNATURES[platform]
            new_paths = info.new_inventory_paths + DEFAULT_NEW_PATHS
            used_paths = info.used_inventory_paths + DEFAULT_USED_PATHS
        else:
            new_paths = DEFAULT_NEW_PATHS
            used_paths = DEFAULT_USED_PATHS

        return new_paths, used_paths
