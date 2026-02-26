"""Generic and provider-specific contact extraction from HTML."""

import logging
import re
from typing import Any, Optional

from bs4 import BeautifulSoup, Tag

from config.platforms import PlatformInfo

logger = logging.getLogger(__name__)

# Email patterns
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Obfuscated email patterns (e.g., "name [at] domain [dot] com")
OBFUSCATED_EMAIL_REGEX = re.compile(
    r"([a-zA-Z0-9._%+\-]+)\s*[\[\(]?\s*(?:at|AT)\s*[\]\)]?\s*"
    r"([a-zA-Z0-9.\-]+)\s*[\[\(]?\s*(?:dot|DOT)\s*[\]\)]?\s*"
    r"([a-zA-Z]{2,})",
    re.IGNORECASE,
)

# US phone patterns
PHONE_REGEX = re.compile(
    r"""
    (?:                        # Optional prefix
        \+?1[\s.-]?            # Country code
    )?
    (?:                        # Area code
        \(?\d{3}\)?[\s.-]?     # (xxx) or xxx
    )
    \d{3}[\s.-]?\d{4}         # xxx-xxxx
    """,
    re.VERBOSE,
)

# Excluded email patterns
EXCLUDED_EMAILS = {
    "noreply",
    "no-reply",
    "donotreply",
    "webmaster",
    "postmaster",
    "admin",
    "info",
    "support",
    "contact",
    "sales",
    "service",
    "help",
    "feedback",
    "marketing",
    "press",
    "media",
    "hr",
}

# Excluded email domains (not personal/dealership)
EXCLUDED_DOMAINS = {
    "example.com",
    "test.com",
    "sentry.io",
    "google.com",
    "facebook.com",
    "twitter.com",
    "instagram.com",
    "googleapis.com",
    "gstatic.com",
    "cloudflare.com",
}


def extract_contacts_from_html(
    html: str, base_domain: str = "", platform_info: Optional[PlatformInfo] = None
) -> list[dict[str, Any]]:
    """Extract structured contacts from an HTML page.

    When platform_info is provided, tries provider-specific selectors first
    for much more reliable extraction.
    """
    soup = BeautifulSoup(html, "lxml")

    # Try provider-specific extraction first
    if platform_info and platform_info.card_selectors:
        contacts = _extract_provider_contacts(soup, base_domain, platform_info)
        if contacts:
            return _deduplicate_contacts(contacts)

    # Fall back to generic structured extraction
    contacts = _extract_structured_contacts(soup, base_domain)

    if not contacts:
        contacts = _extract_flat_contacts(soup, html, base_domain)

    return _deduplicate_contacts(contacts)


def _extract_provider_contacts(
    soup: BeautifulSoup, base_domain: str, platform_info: PlatformInfo
) -> list[dict[str, Any]]:
    """Extract contacts using provider-specific CSS selectors."""
    contacts: list[dict[str, Any]] = []

    # Find cards using provider-specific selectors
    cards: list[Tag] = []
    for selector in platform_info.card_selectors:
        try:
            found = soup.select(selector)
            if found:
                cards = found
                logger.debug(f"Provider selector '{selector}' matched {len(found)} cards")
                break
        except Exception:
            continue

    if not cards:
        return []

    for card in cards:
        contact = _parse_provider_card(card, base_domain, platform_info)
        if contact and (contact.get("name") or contact.get("email")):
            contacts.append(contact)

    logger.info(f"Provider-specific extraction found {len(contacts)} contacts")
    return contacts


def _parse_provider_card(
    element: Tag, base_domain: str, platform_info: PlatformInfo
) -> dict[str, Any]:
    """Parse a contact card using provider-specific selectors."""
    contact: dict[str, Any] = {"source": "crawl"}

    # Extract name using provider-specific selectors
    for selector in platform_info.name_selectors:
        try:
            name_el = element.select_one(selector)
            if name_el:
                name = name_el.get_text(strip=True)
                if name and 2 < len(name) < 80 and not _is_generic_text(name) and _looks_like_person_name(name):
                    contact["name"] = name
                    break
        except Exception:
            continue

    # Extract title using provider-specific selectors
    for selector in platform_info.title_selectors:
        try:
            title_el = element.select_one(selector)
            if title_el:
                title = title_el.get_text(strip=True)
                if title and 3 < len(title) < 100 and _looks_like_title(title):
                    contact["title"] = title
                    break
        except Exception:
            continue

    # Fallback: extract title from bare text nodes in the card
    # (e.g. Overfuel puts job title as a text node, not in a tag)
    if "title" not in contact:
        from bs4 import NavigableString

        for child in element.children:
            if isinstance(child, NavigableString):
                text = child.strip()
                if text and 3 < len(text) < 100 and _looks_like_title(text):
                    contact["title"] = text
                    break

    # Extract email using provider-specific selectors, then fallback
    for selector in platform_info.email_selectors:
        try:
            email_el = element.select_one(selector)
            if email_el:
                if selector.startswith("a[href"):
                    href = email_el.get("href", "")
                    if href.startswith("mailto:"):
                        email = href.replace("mailto:", "").split("?")[0].strip()
                        if _is_valid_contact_email(email, base_domain):
                            contact["email"] = email
                            break
                else:
                    text = email_el.get_text(strip=True)
                    emails = extract_emails(text, base_domain)
                    if emails:
                        contact["email"] = emails[0]
                        break
        except Exception:
            continue

    # Fallback: scan card text for emails
    if "email" not in contact:
        for a_tag in element.find_all("a", href=True):
            href = a_tag["href"]
            if href.startswith("mailto:"):
                email = href.replace("mailto:", "").split("?")[0].strip()
                if _is_valid_contact_email(email, base_domain):
                    contact["email"] = email
                    break

    # Extract phone using provider-specific selectors, then fallback
    for selector in platform_info.phone_selectors:
        try:
            phone_el = element.select_one(selector)
            if phone_el:
                if selector.startswith("a[href"):
                    href = phone_el.get("href", "")
                    if href.startswith("tel:"):
                        contact["phone"] = href.replace("tel:", "").strip()
                        break
                else:
                    phones = extract_phones(phone_el.get_text())
                    if phones:
                        contact["phone"] = phones[0]
                        break
        except Exception:
            continue

    # Fallback: scan card for tel: links
    if "phone" not in contact:
        for a_tag in element.find_all("a", href=True):
            href = a_tag["href"]
            if href.startswith("tel:"):
                contact["phone"] = href.replace("tel:", "").strip()
                break

    # Extract photo URL
    img = element.select_one("img")
    if img and img.get("src"):
        contact["photo_url"] = img["src"]

    return contact


def _extract_structured_contacts(soup: BeautifulSoup, base_domain: str) -> list[dict[str, Any]]:
    """Extract contacts from structured card-like HTML elements."""
    contacts: list[dict[str, Any]] = []

    # Common staff card CSS class/id patterns
    card_selectors = [
        "[class*='staff']",
        "[class*='team-member']",
        "[class*='employee']",
        "[class*='person']",
        "[class*='bio']",
        "[class*='profile']",
        "[class*='card'][class*='contact']",
        "[itemtype='http://schema.org/Person']",
        "[itemtype='https://schema.org/Person']",
        ".vcard",
    ]

    cards = []
    for selector in card_selectors:
        found = soup.select(selector)
        if found:
            cards = found
            break

    if not cards:
        return []

    for card in cards:
        contact = _parse_contact_card(card, base_domain)
        if contact and (contact.get("name") or contact.get("email")):
            contacts.append(contact)

    return contacts


def _parse_contact_card(element, base_domain: str) -> dict[str, Any]:
    """Parse a single contact card element."""
    contact: dict[str, Any] = {"source": "crawl"}

    # Extract name from headings
    for tag in ["h2", "h3", "h4", "h5", "strong", ".name", "[class*='name']", "[itemprop='name']"]:
        name_el = element.select_one(tag)
        if name_el:
            name = name_el.get_text(strip=True)
            if name and len(name) > 2 and len(name) < 80 and not _is_generic_text(name) and _looks_like_person_name(name):
                contact["name"] = name
                break

    # Extract title/role
    title_selectors = [
        "[class*='title']",
        "[class*='position']",
        "[class*='role']",
        "[class*='job']",
        "[itemprop='jobTitle']",
        ".title",
        "p",
        "span",
    ]
    for sel in title_selectors:
        title_el = element.select_one(sel)
        if title_el and title_el != element.select_one("h2, h3, h4, h5"):
            title = title_el.get_text(strip=True)
            if title and len(title) > 3 and len(title) < 100 and _looks_like_title(title):
                contact["title"] = title
                break

    # Extract email
    card_text = element.get_text()
    emails = extract_emails(card_text, base_domain)
    # Also check mailto links
    for a_tag in element.find_all("a", href=True):
        href = a_tag["href"]
        if href.startswith("mailto:"):
            email = href.replace("mailto:", "").split("?")[0].strip()
            if _is_valid_contact_email(email, base_domain):
                emails.insert(0, email)

    if emails:
        contact["email"] = emails[0]

    # Extract phone
    phones = extract_phones(card_text)
    for a_tag in element.find_all("a", href=True):
        href = a_tag["href"]
        if href.startswith("tel:"):
            phone = href.replace("tel:", "").strip()
            phones.insert(0, phone)

    if phones:
        contact["phone"] = phones[0]

    # Extract photo URL
    img = element.select_one("img")
    if img and img.get("src"):
        contact["photo_url"] = img["src"]

    return contact


def _extract_flat_contacts(soup: BeautifulSoup, html: str, base_domain: str) -> list[dict[str, Any]]:
    """Fallback: extract contacts as flat lists of emails/phones/names."""
    contacts: list[dict[str, Any]] = []

    emails = extract_emails(html, base_domain)

    # Try to find names near emails
    for email in emails[:10]:
        contact: dict[str, Any] = {"email": email, "source": "crawl"}

        # Look for name near the email in the HTML
        name = _find_name_near_email(soup, email)
        if name:
            contact["name"] = name

        # Look for title near the email
        title = _find_title_near_email(soup, email)
        if title:
            contact["title"] = title

        contacts.append(contact)

    return contacts


def extract_emails(text: str, base_domain: str = "") -> list[str]:
    """Extract valid email addresses from text."""
    emails: list[str] = []
    seen: set[str] = set()

    # Standard emails
    for match in EMAIL_REGEX.finditer(text):
        email = match.group().lower().strip()
        if email not in seen and _is_valid_contact_email(email, base_domain):
            seen.add(email)
            emails.append(email)

    # Obfuscated emails
    for match in OBFUSCATED_EMAIL_REGEX.finditer(text):
        email = f"{match.group(1)}@{match.group(2)}.{match.group(3)}".lower()
        if email not in seen and _is_valid_contact_email(email, base_domain):
            seen.add(email)
            emails.append(email)

    return emails


def extract_phones(text: str) -> list[str]:
    """Extract US phone numbers from text."""
    phones: list[str] = []
    seen: set[str] = set()

    for match in PHONE_REGEX.finditer(text):
        phone = match.group().strip()
        digits = re.sub(r"\D", "", phone)

        if len(digits) < 10 or len(digits) > 11:
            continue
        if digits in seen:
            continue

        seen.add(digits)
        phones.append(phone)

    return phones


def _normalize_domain(url_or_domain: str) -> str:
    """Extract clean domain from a URL or domain string."""
    d = url_or_domain.lower().strip()
    # Strip protocol
    if "://" in d:
        d = d.split("://", 1)[1]
    # Strip path
    d = d.split("/")[0]
    # Strip www prefix
    if d.startswith("www."):
        d = d[4:]
    return d


def _is_valid_contact_email(email: str, base_domain: str = "") -> bool:
    """Check if an email looks like a real person's contact."""
    local_part = email.split("@")[0].lower()
    domain = email.split("@")[1].lower() if "@" in email else ""

    # Exclude generic addresses
    if local_part in EXCLUDED_EMAILS:
        return False

    # Exclude non-business domains
    if domain in EXCLUDED_DOMAINS:
        return False

    # Prefer emails matching the dealership domain
    if base_domain:
        normalized = _normalize_domain(base_domain)
        if domain != normalized and not domain.endswith(f".{normalized}"):
            return False

    return True


def _is_generic_text(text: str) -> bool:
    """Check if text is too generic to be a name."""
    generic = {
        "learn more",
        "read more",
        "view profile",
        "contact us",
        "our team",
        "meet our team",
        "click here",
        "more info",
        # Dealership department/section headings
        "managers",
        "management",
        "sales",
        "sales and finance",
        "sales & finance",
        "finance",
        "service",
        "parts",
        "office",
        "administration",
        "body shop",
        "internet",
        "internet sales",
        "staff",
        "our staff",
        "meet our staff",
        "leadership",
        "team",
    }
    return text.lower().strip() in generic


def _looks_like_person_name(text: str) -> bool:
    """Heuristic: does this text look like a person's name?

    Real names have at least 2 words (first + last). Single words like
    'Managers' or 'Finance' are department headings, not people.
    """
    words = text.strip().split()
    if len(words) < 2:
        return False
    # Reject if it looks like a title/role instead of a name
    title_only_words = {"general", "manager", "sales", "service", "finance", "director", "president"}
    if all(w.lower() in title_only_words for w in words):
        return False
    return True


def _looks_like_title(text: str) -> bool:
    """Heuristic: does this text look like a job title?"""
    title_keywords = {
        "manager",
        "director",
        "president",
        "owner",
        "partner",
        "specialist",
        "advisor",
        "consultant",
        "assistant",
        "sales",
        "service",
        "finance",
        "parts",
        "general",
        "vp",
        "vice",
        "chief",
        "officer",
        "head",
    }
    text_lower = text.lower()
    return any(kw in text_lower for kw in title_keywords)


def _find_name_near_email(soup: BeautifulSoup, email: str) -> str:
    """Try to find a person's name near an email in the document."""
    # Find elements containing the email
    for el in soup.find_all(string=re.compile(re.escape(email))):
        parent = el.parent
        if parent:
            # Look at siblings and parent for name-like headings
            for sibling in parent.parent.children if parent.parent else []:
                if hasattr(sibling, "name") and sibling.name in ("h2", "h3", "h4", "h5", "strong"):
                    name = sibling.get_text(strip=True)
                    if name and len(name) > 2 and len(name) < 80:
                        return name
    return ""


def _find_title_near_email(soup: BeautifulSoup, email: str) -> str:
    """Try to find a job title near an email in the document."""
    for el in soup.find_all(string=re.compile(re.escape(email))):
        parent = el.parent
        if parent and parent.parent:
            for sibling in parent.parent.children:
                if hasattr(sibling, "get_text"):
                    text = sibling.get_text(strip=True)
                    if text and _looks_like_title(text) and text.lower() != email:
                        return text
    return ""


def _deduplicate_contacts(contacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate contacts by email, then by name."""
    seen_emails: set[str] = set()
    seen_names: set[str] = set()
    unique: list[dict[str, Any]] = []

    for contact in contacts:
        email = (contact.get("email") or "").lower().strip()
        name = (contact.get("name") or "").lower().strip()

        if email:
            if email in seen_emails:
                continue
            seen_emails.add(email)
        elif name:
            if name in seen_names:
                continue
            seen_names.add(name)

        unique.append(contact)

    return unique
