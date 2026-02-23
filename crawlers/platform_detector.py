"""Detect dealership website platform (DealerOn, Dealer.com, DealerInspire, etc.)."""

import logging
import re
from typing import Any, Optional

from bs4 import BeautifulSoup

from config.platforms import PLATFORM_SIGNATURES

logger = logging.getLogger(__name__)


class PlatformDetector:
    """Detects the website platform a dealership uses."""

    async def detect(self, page) -> dict[str, Any]:
        """Detect the platform from a loaded page.

        Args:
            page: Pyppeteer page (already navigated to the dealership homepage).

        Returns:
            Dict with platform name, confidence, and detection method.
        """
        html = await page.content()
        return self.detect_from_html(html)

    def detect_from_html(self, html: str) -> dict[str, Any]:
        """Detect platform from raw HTML content."""
        html_lower = html.lower()
        soup = BeautifulSoup(html, "lxml")

        # Check meta generator tag first
        generator = self._check_meta_generator(soup)
        if generator:
            return {"platform": generator, "confidence": 0.95, "method": "meta_generator"}

        # Check HTML source for platform signatures
        for platform_name, info in PLATFORM_SIGNATURES.items():
            for signature in info.signatures:
                if signature.lower() in html_lower:
                    return {
                        "platform": platform_name,
                        "confidence": 0.85,
                        "method": f"signature:{signature}",
                    }

        # Check script/link URLs for known CDN patterns
        platform = self._check_asset_urls(soup)
        if platform:
            return {"platform": platform, "confidence": 0.75, "method": "asset_url"}

        # Check for common CMS patterns
        cms = self._check_cms_patterns(html_lower, soup)
        if cms:
            return {"platform": cms, "confidence": 0.7, "method": "cms_pattern"}

        return {"platform": "Custom/Unknown", "confidence": 0.0, "method": "none"}

    def _check_meta_generator(self, soup: BeautifulSoup) -> Optional[str]:
        """Check <meta name='generator'> tag."""
        meta = soup.find("meta", attrs={"name": "generator"})
        if meta and meta.get("content"):
            content = meta["content"].lower()
            for platform_name, info in PLATFORM_SIGNATURES.items():
                if platform_name.lower() in content:
                    return platform_name
            # Common CMS detection
            if "wordpress" in content:
                return "WordPress"
            if "drupal" in content:
                return "Drupal"
        return None

    def _check_asset_urls(self, soup: BeautifulSoup) -> Optional[str]:
        """Check script and link URLs for platform CDN patterns."""
        urls: list[str] = []

        for script in soup.find_all("script", src=True):
            urls.append(script["src"].lower())
        for link in soup.find_all("link", href=True):
            urls.append(link["href"].lower())

        all_urls = " ".join(urls)

        for platform_name, info in PLATFORM_SIGNATURES.items():
            for signature in info.signatures:
                if signature.lower() in all_urls:
                    return platform_name

        return None

    def _check_cms_patterns(self, html_lower: str, soup: BeautifulSoup) -> Optional[str]:
        """Check for broader CMS patterns."""
        patterns = {
            "WordPress": [r"wp-content", r"wp-includes", r"wordpress"],
            "Drupal": [r"drupal\.js", r"/sites/default/files"],
            "Squarespace": [r"squarespace\.com", r"sqsp\.com"],
            "Wix": [r"wix\.com", r"parastorage\.com"],
        }

        for cms_name, regexes in patterns.items():
            for regex in regexes:
                if re.search(regex, html_lower):
                    return cms_name

        return None
