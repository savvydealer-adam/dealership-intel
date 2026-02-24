"""Staff page discovery and contact extraction via headless browser."""

import asyncio
import logging
import re
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from config.platforms import (
    CONTACT_PAGE_PATHS,
    PLATFORM_SIGNATURES,
    STAFF_NAV_KEYWORDS,
    STAFF_PAGE_PATHS,
    PlatformInfo,
)
from crawlers.contact_extractor import extract_contacts_from_html
from crawlers.stealth import detect_captcha, dismiss_cookie_consent, human_delay

logger = logging.getLogger(__name__)


class StaffCrawler:
    """Discovers staff/team pages and extracts contact information."""

    def __init__(self, browser_manager=None, timeout: int = 30):
        self.browser_manager = browser_manager
        self.timeout = timeout

    async def crawl_staff_page(
        self, base_url: str, platform: str | None = None
    ) -> list[dict[str, Any]]:
        """Find and crawl staff pages for a dealership website.

        Args:
            base_url: The dealership's base URL (e.g., https://example.com).
            platform: Detected platform name for provider-specific paths/selectors.

        Returns:
            List of extracted contacts with name, title, email, phone, source.
        """
        if not self.browser_manager:
            logger.warning("No browser manager configured - skipping crawl")
            return []

        domain = urlparse(base_url).netloc.lower().replace("www.", "")
        platform_info = PLATFORM_SIGNATURES.get(platform) if platform else None
        contacts: list[dict[str, Any]] = []

        async with await self.browser_manager.get_page() as page:
            # Step 1: Try platform-specific staff page paths first
            staff_url = None
            if platform_info and platform_info.staff_page_paths:
                staff_url = await self._find_staff_page_by_path(
                    page, base_url, platform_info.staff_page_paths
                )

            # Step 2: Try generic staff page paths
            if not staff_url:
                staff_url = await self._find_staff_page_by_path(page, base_url, STAFF_PAGE_PATHS)

            # Step 3: Check navigation for staff links
            if not staff_url:
                staff_url = await self._find_staff_page_from_nav(page, base_url)

            # Step 4: Check sitemap
            if not staff_url:
                staff_url = await self._find_staff_page_from_sitemap(page, base_url)

            # Step 5: Extract contacts from the found page
            if staff_url:
                logger.info(f"Found staff page: {staff_url}")
                contacts = await self._extract_contacts_from_page(
                    page, staff_url, domain, platform_info
                )

            # Step 6: If no/few contacts from staff page, try contact page as fallback
            if len(contacts) < 2:
                contact_paths = (
                    platform_info.contact_page_paths
                    if platform_info and platform_info.contact_page_paths
                    else CONTACT_PAGE_PATHS
                )
                contact_url = await self._find_staff_page_by_path(
                    page, base_url, contact_paths, require_staff_indicators=False
                )
                if contact_url and contact_url != staff_url:
                    logger.info(f"Trying contact page fallback: {contact_url}")
                    extra = await self._extract_contacts_from_page(
                        page, contact_url, domain, platform_info
                    )
                    # Merge without duplicating
                    existing_emails = {(c.get("email") or "").lower() for c in contacts}
                    for c in extra:
                        email = (c.get("email") or "").lower()
                        if email and email not in existing_emails:
                            contacts.append(c)
                            existing_emails.add(email)
                        elif not email and c.get("name"):
                            contacts.append(c)

            if not contacts:
                logger.info(f"No staff/contacts found for {base_url}")

        return contacts

    async def _find_staff_page_by_path(
        self,
        page,
        base_url: str,
        paths: list[str],
        require_staff_indicators: bool = True,
    ) -> Optional[str]:
        """Try a list of URL paths to find a staff/contact page."""
        for path in paths:
            url = urljoin(base_url, path)
            try:
                response = await page.goto(
                    url, {"timeout": self.timeout * 1000, "waitUntil": "domcontentloaded"}
                )
                if response and response.status == 200:
                    content = await page.content()
                    if not require_staff_indicators or self._looks_like_staff_page(content):
                        return url
                await human_delay(0.5, 1.0)
            except Exception as e:
                logger.debug(f"Path {path} failed: {e}")
                continue

        return None

    async def _find_staff_page_from_nav(self, page, base_url: str) -> Optional[str]:
        """Check navigation links for staff/team pages."""
        try:
            response = await page.goto(
                base_url, {"timeout": self.timeout * 1000, "waitUntil": "domcontentloaded"}
            )
            if not response or response.status >= 400:
                return None

            await dismiss_cookie_consent(page)
            await human_delay()

            if await detect_captcha(page):
                return None

            # Find nav links containing staff keywords
            links = await page.evaluate(
                """() => {
                const links = [];
                document.querySelectorAll('nav a, header a, .menu a, [class*="nav"] a').forEach(a => {
                    const text = a.textContent.trim().toLowerCase();
                    const href = a.href;
                    if (href && text) {
                        links.push({text, href});
                    }
                });
                return links;
            }"""
            )

            for link in links:
                text = link.get("text", "").lower()
                href = link.get("href", "")

                if any(keyword in text for keyword in STAFF_NAV_KEYWORDS):
                    if href and not href.startswith("javascript:"):
                        return href

        except Exception as e:
            logger.debug(f"Nav scan failed for {base_url}: {e}")

        return None

    async def _find_staff_page_from_sitemap(self, page, base_url: str) -> Optional[str]:
        """Check sitemap.xml for staff-related URLs."""
        sitemap_url = urljoin(base_url, "/sitemap.xml")
        try:
            response = await page.goto(
                sitemap_url, {"timeout": self.timeout * 1000, "waitUntil": "domcontentloaded"}
            )
            if not response or response.status != 200:
                return None

            content = await page.content()
            soup = BeautifulSoup(content, "lxml")

            staff_keywords = [
                "staff",
                "team",
                "about-us",
                "our-team",
                "meet-the-team",
                "employees",
                "management",
            ]

            for loc in soup.find_all("loc"):
                url = loc.get_text(strip=True)
                url_lower = url.lower()
                if any(kw in url_lower for kw in staff_keywords):
                    return url

        except Exception as e:
            logger.debug(f"Sitemap scan failed for {base_url}: {e}")

        return None

    async def _extract_contacts_from_page(
        self,
        page,
        url: str,
        domain: str,
        platform_info: Optional[PlatformInfo] = None,
    ) -> list[dict[str, Any]]:
        """Navigate to a staff page and extract contacts."""
        try:
            response = await page.goto(
                url, {"timeout": self.timeout * 1000, "waitUntil": "networkidle0"}
            )
            if not response or response.status >= 400:
                return []

            await dismiss_cookie_consent(page)
            await human_delay()

            if await detect_captcha(page):
                return []

            # Wait for dynamic content
            await page.waitForSelector("body", {"timeout": 5000})

            html = await page.content()
            contacts = extract_contacts_from_html(html, domain, platform_info=platform_info)

            logger.info(f"Extracted {len(contacts)} contacts from {url}")
            return contacts

        except Exception as e:
            logger.warning(f"Contact extraction failed for {url}: {e}")
            return []

    def _looks_like_staff_page(self, html: str) -> bool:
        """Heuristic check: does this HTML look like a staff/team page?"""
        html_lower = html.lower()

        # Check for multiple person-like patterns
        staff_indicators = [
            "staff",
            "team member",
            "our team",
            "meet the team",
            "management",
            "leadership",
            "employees",
            "our people",
        ]
        indicator_count = sum(1 for ind in staff_indicators if ind in html_lower)

        # Check for email/phone density
        email_count = len(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html))

        # Check for structured card-like patterns
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select("[class*='staff'], [class*='team'], [class*='employee'], [class*='person']")

        return indicator_count >= 2 or email_count >= 3 or len(cards) >= 2


def crawl_staff_sync(
    base_url: str, browser_manager=None, platform: str | None = None
) -> list[dict[str, Any]]:
    """Synchronous wrapper for staff crawling."""
    crawler = StaffCrawler(browser_manager=browser_manager)
    loop = asyncio.get_event_loop()
    if loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(asyncio.run, crawler.crawl_staff_page(base_url, platform=platform))
            return future.result()
    else:
        return asyncio.run(crawler.crawl_staff_page(base_url, platform=platform))
