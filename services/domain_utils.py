"""Domain extraction and company name utilities."""

import logging
import re
from typing import Optional
from urllib.parse import urlparse

from services.web_scraper import get_website_text_content

logger = logging.getLogger(__name__)


def extract_domain(url: str) -> Optional[str]:
    """Extract domain from URL.

    Args:
        url: Website URL.

    Returns:
        Domain name without www prefix or None if invalid.
    """
    try:
        if not url:
            return None

        # Add protocol if missing
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        # Remove www prefix
        if domain.startswith("www."):
            domain = domain[4:]

        # Validate domain format
        if "." not in domain or len(domain) < 3:
            return None

        return domain

    except Exception:
        return None


def extract_company_name(website_url: str, domain: str) -> Optional[str]:
    """Extract company name from website content or domain.

    Args:
        website_url: Full website URL.
        domain: Extracted domain name.

    Returns:
        Company name or None if not found.
    """
    try:
        # First try to get company name from website content
        website_content = get_website_text_content(website_url)

        if website_content:
            # Look for common patterns that indicate company names
            company_name = _extract_name_from_content(website_content, domain)
            if company_name:
                return company_name

        # Fallback: Generate company name from domain
        return _generate_name_from_domain(domain)

    except Exception as e:
        logger.warning(f"Error extracting company name from {website_url}: {e}")
        return _generate_name_from_domain(domain)


def _extract_name_from_content(content: str, domain: str) -> Optional[str]:
    """Extract company name from website content using various patterns."""
    if not content:
        return None

    # Convert to lowercase for pattern matching
    content_lower = content.lower()
    lines = content.split("\n")

    # Patterns to look for company names
    patterns = [
        # Look for "About [Company]", "Welcome to [Company]"
        r"(?:about|welcome to)\s+([^,\n\.]{2,50}?)(?:\s|,|\.|\n)",
        # Look for copyright notices
        r"copyright.*?(?:©|\(c\)).*?(\d{4}).*?([^,\n\.]{2,50}?)(?:\s|,|\.|\n)",
        # Look for title tags or headers
        r"<title[^>]*>([^<]{5,100})</title>",
        # Look for dealership-specific terms
        r"([\w\s]{2,50}?)(?:\s+(?:auto|automotive|car|cars|dealership|dealer|motors|honda|toyota|ford|gm|chevrolet|nissan|bmw|audi|mercedes))",
        # Look for "at [Company]" patterns
        r"(?:at|from)\s+([^,\n\.]{2,50}?)(?:\s|,|\.|\n)",
    ]

    for pattern in patterns:
        matches = re.findall(pattern, content_lower, re.IGNORECASE | re.MULTILINE)
        for match in matches:
            if isinstance(match, tuple):
                # Handle patterns that return tuples
                for m in match:
                    name = _clean_company_name(m, domain)
                    if name and _is_valid_company_name(name, domain):
                        return name
            else:
                name = _clean_company_name(match, domain)
                if name and _is_valid_company_name(name, domain):
                    return name

    # Look for the first meaningful line that might be a company name
    for line in lines[:10]:  # Check first 10 lines
        line = line.strip()
        if len(line) > 5 and len(line) < 100:
            # Check if line contains dealership keywords
            if any(
                keyword in line.lower() for keyword in ["auto", "car", "dealership", "dealer", "motors", "automotive"]
            ):
                name = _clean_company_name(line, domain)
                if name and _is_valid_company_name(name, domain):
                    return name

    return None


def _clean_company_name(name: str, domain: str) -> str:
    """Clean and format company name."""
    if not name:
        return ""

    # Remove HTML tags
    name = re.sub(r"<[^>]+>", "", name)

    # Remove extra whitespace
    name = " ".join(name.split())

    # Remove common prefixes/suffixes
    name = re.sub(r"^(welcome to|about|at|from)\s+", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s+(home|page|website|site)$", "", name, flags=re.IGNORECASE)

    # Title case
    name = name.title()

    return name.strip()


def _is_valid_company_name(name: str, domain: str) -> bool:
    """Check if extracted name is likely a valid company name."""
    if not name or len(name) < 3 or len(name) > 80:
        return False

    # Skip generic terms
    generic_terms = {
        "home",
        "page",
        "website",
        "site",
        "welcome",
        "about",
        "contact",
        "services",
        "products",
        "news",
        "blog",
        "menu",
        "login",
        "search",
        "privacy",
        "policy",
        "terms",
        "copyright",
        "all rights reserved",
    }

    if name.lower() in generic_terms:
        return False

    # Must contain at least one letter
    if not re.search(r"[a-zA-Z]", name):
        return False

    # Should not be just the domain
    domain_parts = domain.split(".")
    if name.lower() == domain_parts[0].lower():
        return False

    return True


def _generate_name_from_domain(domain: str) -> str:
    """Generate a company name from domain as fallback."""
    if not domain:
        return ""

    # Take the main part before the first dot
    main_part = domain.split(".")[0]

    # Replace hyphens and underscores with spaces
    name = main_part.replace("-", " ").replace("_", " ")

    # Title case
    name = " ".join(word.capitalize() for word in name.split())

    return name
