"""Web scraping utilities for extracting website content and metadata."""

import logging
from typing import Optional

import requests
import trafilatura

logger = logging.getLogger(__name__)


def get_website_text_content(url: str, timeout: int = 15) -> Optional[str]:
    """Extract main text content from a website URL using trafilatura.

    Args:
        url: Website URL to scrape.
        timeout: Request timeout in seconds.

    Returns:
        Extracted text content or None if failed.
    """
    try:
        # Ensure URL has protocol
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        # Download the webpage content
        downloaded = trafilatura.fetch_url(url)

        if not downloaded:
            return None

        # Extract text content
        text_content = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=True,
            include_links=False,
            no_fallback=False,
        )

        return text_content

    except Exception as e:
        logger.warning(f"Error extracting content from {url}: {e}")
        return None


def get_website_metadata(url: str) -> dict[str, str]:
    """Extract metadata from website including title, description, etc.

    Args:
        url: Website URL to analyze.

    Returns:
        Dictionary containing metadata.
    """
    try:
        # Ensure URL has protocol
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        # Download the webpage content
        downloaded = trafilatura.fetch_url(url)

        if not downloaded:
            return {}

        # Extract metadata
        metadata = trafilatura.extract_metadata(downloaded)

        result: dict[str, str] = {}
        if metadata:
            result["title"] = metadata.title or ""
            result["description"] = metadata.description or ""
            result["author"] = metadata.author or ""
            result["site_name"] = metadata.sitename or ""
            result["url"] = metadata.url or url
            result["language"] = metadata.language or ""
            result["date"] = metadata.date or ""

        return result

    except Exception as e:
        logger.warning(f"Error extracting metadata from {url}: {e}")
        return {}


def is_website_accessible(url: str, timeout: int = 10) -> bool:
    """Check if a website is accessible.

    Args:
        url: Website URL to check.
        timeout: Request timeout in seconds.

    Returns:
        True if website is accessible, False otherwise.
    """
    try:
        # Ensure URL has protocol
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        response = requests.head(
            url,
            timeout=timeout,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )

        return response.status_code < 400

    except Exception:
        return False
