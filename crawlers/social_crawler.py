"""Find social media profile links from dealership websites."""

import logging
from urllib.parse import urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Social media platforms and their URL patterns
SOCIAL_PLATFORMS = {
    "facebook": {"domains": ["facebook.com", "fb.com"], "profile_pattern": r"/[\w.]+/?$"},
    "instagram": {"domains": ["instagram.com"], "profile_pattern": r"/[\w.]+/?$"},
    "twitter": {"domains": ["twitter.com", "x.com"], "profile_pattern": r"/[\w]+/?$"},
    "youtube": {"domains": ["youtube.com", "youtu.be"], "profile_pattern": r"/(channel|c|user|@)[\w]+"},
    "linkedin": {"domains": ["linkedin.com"], "profile_pattern": r"/company/[\w-]+"},
    "tiktok": {"domains": ["tiktok.com"], "profile_pattern": r"/@[\w.]+"},
}

# Exclude these URL patterns (share buttons, not profiles)
SHARE_BUTTON_PATTERNS = [
    "/sharer",
    "/share",
    "/dialog",
    "/intent/tweet",
    "/pin/create",
    "/shareArticle",
]


class SocialCrawler:
    """Finds social media profile links from dealership pages."""

    async def find_social_links(self, page) -> dict[str, str]:
        """Extract social media links from a loaded page.

        Args:
            page: Pyppeteer page (already navigated to homepage).

        Returns:
            Dict mapping platform name -> profile URL.
        """
        html = await page.content()
        return self.find_social_links_from_html(html)

    def find_social_links_from_html(self, html: str) -> dict[str, str]:
        """Extract social media links from HTML content."""
        soup = BeautifulSoup(html, "lxml")
        social_links: dict[str, str] = {}

        # Priority areas: footer, header, sidebar
        priority_areas = soup.select("footer, header, [class*='social'], [id*='social'], aside")

        # Collect all links, prioritizing footer/header areas
        all_links: list[dict[str, str]] = []

        # First check priority areas
        for area in priority_areas:
            for a_tag in area.find_all("a", href=True):
                all_links.append({"href": a_tag["href"], "priority": "high"})

        # Then check entire page
        for a_tag in soup.find_all("a", href=True):
            all_links.append({"href": a_tag["href"], "priority": "low"})

        for link_info in all_links:
            href = link_info["href"].strip()
            if not href or href.startswith("javascript:") or href == "#":
                continue

            platform, url = self._classify_social_link(href)
            if platform and platform not in social_links:
                social_links[platform] = url

        logger.info(f"Found {len(social_links)} social media links")
        return social_links

    def _classify_social_link(self, url: str) -> tuple[str, str]:
        """Classify a URL as a social media profile link.

        Returns:
            Tuple of (platform_name, clean_url) or ("", "") if not a social link.
        """
        # Skip share buttons
        url_lower = url.lower()
        if any(pattern in url_lower for pattern in SHARE_BUTTON_PATTERNS):
            return "", ""

        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower().replace("www.", "")
        except Exception:
            return "", ""

        for platform, config in SOCIAL_PLATFORMS.items():
            if any(d in domain for d in config["domains"]):
                # Verify it looks like a profile URL (not a share/dialog link)
                path = parsed.path.rstrip("/")
                if path and len(path) > 1:
                    return platform, url

        return "", ""
