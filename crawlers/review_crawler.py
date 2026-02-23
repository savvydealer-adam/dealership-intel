"""Scrape review scores from Google, DealerRater, and Yelp."""

import logging
import re
from typing import Any, Optional

from bs4 import BeautifulSoup

from crawlers.stealth import dismiss_cookie_consent, human_delay

logger = logging.getLogger(__name__)


class ReviewCrawler:
    """Scrapes review ratings and counts from public review sites."""

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    async def crawl_reviews(self, page, dealership_name: str, location: str = "") -> list[dict[str, Any]]:
        """Collect review scores from multiple sources.

        Args:
            page: Pyppeteer page instance.
            dealership_name: Name of the dealership.
            location: Optional city/state for better search targeting.

        Returns:
            List of review dicts: {source, rating, review_count, url}
        """
        reviews: list[dict[str, Any]] = []

        # Google Reviews
        google_review = await self._scrape_google_reviews(page, dealership_name, location)
        if google_review:
            reviews.append(google_review)

        await human_delay()

        # DealerRater
        dr_review = await self._scrape_dealerrater(page, dealership_name)
        if dr_review:
            reviews.append(dr_review)

        await human_delay()

        # Yelp
        yelp_review = await self._scrape_yelp(page, dealership_name, location)
        if yelp_review:
            reviews.append(yelp_review)

        logger.info(f"Found {len(reviews)} review sources for {dealership_name}")
        return reviews

    async def _scrape_google_reviews(self, page, name: str, location: str) -> Optional[dict[str, Any]]:
        """Scrape Google Maps/Search for star rating and review count."""
        search_query = f"{name} {location} reviews".strip()
        search_url = f"https://www.google.com/search?q={search_query.replace(' ', '+')}"

        try:
            response = await page.goto(search_url, {"timeout": self.timeout * 1000, "waitUntil": "domcontentloaded"})
            if not response or response.status >= 400:
                return None

            await human_delay()

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Look for Google's star rating in the knowledge panel
            rating = None
            count = None

            # Pattern: "4.5" near "reviews" or star elements
            rating_patterns = [
                r"(\d\.\d)\s*(?:out of 5|stars?)",
                r"Rated\s*(\d\.\d)",
            ]

            text = soup.get_text()
            for pattern in rating_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    rating = float(match.group(1))
                    break

            # Extract review count
            count_patterns = [
                r"(\d[\d,]*)\s*(?:google\s+)?reviews?",
                r"Based on\s*(\d[\d,]*)\s*reviews?",
            ]
            for pattern in count_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    count = int(match.group(1).replace(",", ""))
                    break

            if rating is not None:
                return {
                    "source": "Google",
                    "rating": rating,
                    "review_count": count,
                    "url": search_url,
                }

        except Exception as e:
            logger.debug(f"Google review scrape failed for {name}: {e}")

        return None

    async def _scrape_dealerrater(self, page, name: str) -> Optional[dict[str, Any]]:
        """Scrape DealerRater for dealership rating."""
        search_url = f"https://www.dealerrater.com/dealer/search?q={name.replace(' ', '+')}"

        try:
            response = await page.goto(search_url, {"timeout": self.timeout * 1000, "waitUntil": "domcontentloaded"})
            if not response or response.status >= 400:
                return None

            await dismiss_cookie_consent(page)
            await human_delay()

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Look for rating in search results
            rating_el = soup.select_one("[class*='rating'], [class*='score'], [class*='stars']")
            if rating_el:
                rating_text = rating_el.get_text(strip=True)
                match = re.search(r"(\d\.\d)", rating_text)
                if match:
                    rating = float(match.group(1))

                    # Try to find review count
                    count = None
                    count_match = re.search(r"(\d[\d,]*)\s*reviews?", soup.get_text(), re.IGNORECASE)
                    if count_match:
                        count = int(count_match.group(1).replace(",", ""))

                    return {
                        "source": "DealerRater",
                        "rating": rating,
                        "review_count": count,
                        "url": search_url,
                    }

        except Exception as e:
            logger.debug(f"DealerRater scrape failed for {name}: {e}")

        return None

    async def _scrape_yelp(self, page, name: str, location: str) -> Optional[dict[str, Any]]:
        """Scrape Yelp for dealership rating."""
        search_query = f"{name} {location}".strip()
        desc = search_query.replace(" ", "+")
        loc = location.replace(" ", "+")
        search_url = f"https://www.yelp.com/search?find_desc={desc}&find_loc={loc}"

        try:
            response = await page.goto(search_url, {"timeout": self.timeout * 1000, "waitUntil": "domcontentloaded"})
            if not response or response.status >= 400:
                return None

            await dismiss_cookie_consent(page)
            await human_delay()

            html = await page.content()

            # Extract rating from Yelp's structured data or aria labels
            rating_match = re.search(r"(\d\.\d)\s*star", html, re.IGNORECASE)
            if not rating_match:
                rating_match = re.search(r'aria-label="(\d\.?\d?)\s*star', html, re.IGNORECASE)

            if rating_match:
                rating = float(rating_match.group(1))

                count = None
                count_match = re.search(r"(\d[\d,]*)\s*reviews?", html, re.IGNORECASE)
                if count_match:
                    count = int(count_match.group(1).replace(",", ""))

                return {
                    "source": "Yelp",
                    "rating": rating,
                    "review_count": count,
                    "url": search_url,
                }

        except Exception as e:
            logger.debug(f"Yelp scrape failed for {name}: {e}")

        return None
