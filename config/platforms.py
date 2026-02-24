"""Known dealership website platform signatures and inventory URL patterns."""

from dataclasses import dataclass, field


@dataclass
class PlatformInfo:
    name: str
    signatures: list[str] = field(default_factory=list)
    # Inventory paths
    new_inventory_paths: list[str] = field(default_factory=list)
    used_inventory_paths: list[str] = field(default_factory=list)
    # Staff/contact page discovery
    staff_page_paths: list[str] = field(default_factory=list)
    contact_page_paths: list[str] = field(default_factory=list)
    # Contact card extraction selectors
    card_selectors: list[str] = field(default_factory=list)
    name_selectors: list[str] = field(default_factory=list)
    title_selectors: list[str] = field(default_factory=list)
    email_selectors: list[str] = field(default_factory=list)
    phone_selectors: list[str] = field(default_factory=list)
    # Inventory count extraction
    inventory_count_selectors: list[str] = field(default_factory=list)
    inventory_count_patterns: list[str] = field(default_factory=list)


PLATFORM_SIGNATURES: dict[str, PlatformInfo] = {
    "Dealer.com": PlatformInfo(
        name="Dealer.com",
        signatures=["dealer.com/content", "ddc-site", "dealercom", "static.dealer.com"],
        new_inventory_paths=["/new-inventory", "/VehicleSearchResults?search=new"],
        used_inventory_paths=["/used-inventory", "/VehicleSearchResults?search=used"],
        staff_page_paths=[
            "/staff",
            "/dealership/staff.htm",
            "/dealership/meet-our-staff.htm",
            "/about-us",
            "/dealership/about.htm",
        ],
        contact_page_paths=["/contact-us", "/dealership/contact.htm"],
        card_selectors=[
            ".staffMembers .staffMember",
            "[class*='staffMember']",
            "[class*='staffDisplay'] > div",
            "[class*='ddc-content'] .staff-card",
            "[class*='staff-member']",
        ],
        name_selectors=["[class*='staffName']", ".staffTitle h3", "[itemprop='name']", "h3"],
        title_selectors=["[class*='staffJobTitle']", ".staffTitle p", "[itemprop='jobTitle']"],
        email_selectors=["a[href^='mailto:']", "[class*='staffEmail']"],
        phone_selectors=["a[href^='tel:']", "[class*='staffPhone']"],
        inventory_count_selectors=["[class*='totalCount']", ".vehicle-count", "[data-total]"],
        inventory_count_patterns=[
            r"(\d+)\s+(?:vehicle|car|result)s?\s+(?:found|available|in stock)",
            r"(?:showing|displaying)\s+\d+\s*-\s*\d+\s+of\s+(\d+)",
        ],
    ),
    "DealerOn": PlatformInfo(
        name="DealerOn",
        signatures=["dealeron.com", "dealeron-", "cdn.dealeron"],
        new_inventory_paths=["/new-vehicles", "/new-inventory"],
        used_inventory_paths=["/used-vehicles", "/used-inventory", "/pre-owned"],
        staff_page_paths=[
            "/staff",
            "/our-team",
            "/meet-our-staff",
            "/about-us",
            "/about",
        ],
        contact_page_paths=["/contact-us", "/contact"],
        card_selectors=[
            "[class*='staffMember']",
            "[class*='team-member']",
            ".staff-list .staff-item",
            "[class*='employee-card']",
        ],
        name_selectors=["[class*='name']", "h3", "h4", "[itemprop='name']"],
        title_selectors=["[class*='title']", "[class*='position']", "[itemprop='jobTitle']"],
        email_selectors=["a[href^='mailto:']", "[class*='email']"],
        phone_selectors=["a[href^='tel:']", "[class*='phone']"],
        inventory_count_selectors=[".vehicle-count", "[class*='resultCount']", "[data-count]"],
        inventory_count_patterns=[
            r"(\d+)\s+(?:vehicle|car|result)s?",
            r"(?:showing|viewing)\s+\d+\s*-\s*\d+\s+of\s+(\d+)",
        ],
    ),
    "DealerInspire": PlatformInfo(
        name="DealerInspire",
        signatures=["dealerinspire.com", "di-", "foxdealer"],
        new_inventory_paths=["/new-vehicles", "/inventory/new"],
        used_inventory_paths=["/used-vehicles", "/inventory/used", "/pre-owned-vehicles"],
        staff_page_paths=[
            "/our-team",
            "/staff",
            "/meet-our-team",
            "/about-us",
            "/about",
        ],
        contact_page_paths=["/contact-us", "/contact"],
        card_selectors=[
            "[class*='team-member']",
            "[class*='staff-member']",
            ".team-grid .team-card",
            "[class*='employee']",
        ],
        name_selectors=["[class*='member-name']", "h3", "h4", "[itemprop='name']"],
        title_selectors=["[class*='member-title']", "[class*='position']", "[itemprop='jobTitle']"],
        email_selectors=["a[href^='mailto:']", "[class*='email']"],
        phone_selectors=["a[href^='tel:']", "[class*='phone']"],
        inventory_count_selectors=[".inventory-count", "[class*='totalResults']", "[data-total]"],
        inventory_count_patterns=[
            r"(\d+)\s+(?:vehicle|car|result)s?\s+(?:found|available)",
            r"(?:showing)\s+\d+\s*-\s*\d+\s+of\s+(\d+)",
        ],
    ),
    "DealerFire": PlatformInfo(
        name="DealerFire",
        signatures=["dealerfire.com"],
        new_inventory_paths=["/new-inventory", "/inventory/new"],
        used_inventory_paths=["/used-inventory", "/inventory/used"],
        staff_page_paths=[
            "/staff",
            "/our-team",
            "/about-us",
            "/meet-the-team",
        ],
        contact_page_paths=["/contact-us", "/contact"],
        card_selectors=[
            "[class*='staff']",
            "[class*='team-member']",
            "[class*='employee']",
        ],
        name_selectors=["[class*='name']", "h3", "h4", "[itemprop='name']"],
        title_selectors=["[class*='title']", "[class*='position']", "[itemprop='jobTitle']"],
        email_selectors=["a[href^='mailto:']"],
        phone_selectors=["a[href^='tel:']"],
        inventory_count_selectors=[".vehicle-count", "[class*='count']"],
        inventory_count_patterns=[r"(\d+)\s+(?:vehicle|car|result)s?"],
    ),
    "Sincro": PlatformInfo(
        name="Sincro",
        signatures=["sincrodigital.com", "sincro."],
        new_inventory_paths=["/new-vehicles", "/inventory?type=new"],
        used_inventory_paths=["/used-vehicles", "/inventory?type=used"],
        staff_page_paths=[
            "/staff",
            "/our-team",
            "/about-us",
            "/about",
        ],
        contact_page_paths=["/contact-us", "/contact"],
        card_selectors=[
            "[class*='staff']",
            "[class*='team-member']",
            "[class*='employee']",
        ],
        name_selectors=["[class*='name']", "h3", "h4"],
        title_selectors=["[class*='title']", "[class*='position']"],
        email_selectors=["a[href^='mailto:']"],
        phone_selectors=["a[href^='tel:']"],
        inventory_count_selectors=[".result-count", "[class*='count']"],
        inventory_count_patterns=[r"(\d+)\s+(?:vehicle|car|result)s?"],
    ),
    "Dealer Car Search": PlatformInfo(
        name="Dealer Car Search",
        signatures=["dealercarsearch.com", "dcsimg", "dealer-car-search"],
        new_inventory_paths=["/new-inventory", "/new-vehicles", "/inventory?condition=new"],
        used_inventory_paths=["/used-inventory", "/used-vehicles", "/inventory?condition=used"],
        staff_page_paths=[
            "/staff",
            "/our-team",
            "/about-us",
            "/about",
        ],
        contact_page_paths=["/contact-us", "/contact"],
        card_selectors=[
            "[class*='staff']",
            "[class*='team']",
            "[class*='employee']",
        ],
        name_selectors=["[class*='name']", "h3", "h4"],
        title_selectors=["[class*='title']", "[class*='position']"],
        email_selectors=["a[href^='mailto:']"],
        phone_selectors=["a[href^='tel:']"],
        inventory_count_selectors=[".vehicle-count", "[class*='count']", "[class*='total']"],
        inventory_count_patterns=[r"(\d+)\s+(?:vehicle|car|result)s?"],
    ),
    "Cars.com": PlatformInfo(
        name="Cars.com",
        signatures=["dealer-inspire", "cars.com/dealers", "cars.com"],
        new_inventory_paths=["/new-vehicles", "/inventory/new"],
        used_inventory_paths=["/used-vehicles", "/inventory/used", "/pre-owned"],
        staff_page_paths=[
            "/our-team",
            "/staff",
            "/meet-the-team",
            "/about-us",
        ],
        contact_page_paths=["/contact-us", "/contact"],
        card_selectors=[
            "[class*='team-member']",
            "[class*='staff-member']",
            "[class*='employee']",
        ],
        name_selectors=["[class*='name']", "h3", "h4", "[itemprop='name']"],
        title_selectors=["[class*='title']", "[class*='position']", "[itemprop='jobTitle']"],
        email_selectors=["a[href^='mailto:']"],
        phone_selectors=["a[href^='tel:']"],
        inventory_count_selectors=[".inventory-count", "[class*='total']"],
        inventory_count_patterns=[r"(\d+)\s+(?:vehicle|car|result)s?"],
    ),
    "Reynolds & Reynolds": PlatformInfo(
        name="Reynolds & Reynolds",
        signatures=["rfrk.com", "reyweb", "reynolds"],
        new_inventory_paths=["/new-inventory", "/new-vehicles", "/inventory/new"],
        used_inventory_paths=["/used-inventory", "/used-vehicles", "/inventory/used"],
        staff_page_paths=[
            "/staff",
            "/about-us",
            "/our-team",
            "/about",
        ],
        contact_page_paths=["/contact-us", "/contact"],
        card_selectors=[
            "[class*='staff']",
            "[class*='team']",
            "[class*='employee']",
        ],
        name_selectors=["[class*='name']", "h3", "h4"],
        title_selectors=["[class*='title']", "[class*='position']"],
        email_selectors=["a[href^='mailto:']"],
        phone_selectors=["a[href^='tel:']"],
        inventory_count_selectors=[".vehicle-count", "[class*='count']"],
        inventory_count_patterns=[r"(\d+)\s+(?:vehicle|car|result)s?"],
    ),
}

# Common staff page paths to try across all platforms
STAFF_PAGE_PATHS = [
    "/staff",
    "/team",
    "/our-team",
    "/meet-the-team",
    "/meet-our-team",
    "/about-us",
    "/about",
    "/about/staff",
    "/about/team",
    "/employees",
    "/management",
    "/leadership",
    "/our-staff",
    "/people",
]

# Common contact page paths (fallback when no staff page found)
CONTACT_PAGE_PATHS = [
    "/contact-us",
    "/contact",
    "/get-in-touch",
]

# Navigation link keywords that suggest staff pages
STAFF_NAV_KEYWORDS = [
    "staff",
    "team",
    "about us",
    "about",
    "meet the team",
    "our people",
    "management",
    "leadership",
]
