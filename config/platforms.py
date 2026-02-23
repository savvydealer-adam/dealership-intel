"""Known dealership website platform signatures and inventory URL patterns."""

from dataclasses import dataclass, field


@dataclass
class PlatformInfo:
    name: str
    signatures: list[str] = field(default_factory=list)
    new_inventory_paths: list[str] = field(default_factory=list)
    used_inventory_paths: list[str] = field(default_factory=list)


PLATFORM_SIGNATURES: dict[str, PlatformInfo] = {
    "DealerOn": PlatformInfo(
        name="DealerOn",
        signatures=["dealeron.com", "dealeron-", "cdn.dealeron"],
        new_inventory_paths=["/new-vehicles", "/new-inventory"],
        used_inventory_paths=["/used-vehicles", "/used-inventory", "/pre-owned"],
    ),
    "Dealer.com": PlatformInfo(
        name="Dealer.com",
        signatures=["dealer.com/content", "ddc-site", "dealercom", "static.dealer.com"],
        new_inventory_paths=["/new-inventory", "/VehicleSearchResults?search=new"],
        used_inventory_paths=["/used-inventory", "/VehicleSearchResults?search=used"],
    ),
    "DealerInspire": PlatformInfo(
        name="DealerInspire",
        signatures=["dealerinspire.com", "di-", "foxdealer"],
        new_inventory_paths=["/new-vehicles", "/inventory/new"],
        used_inventory_paths=["/used-vehicles", "/inventory/used", "/pre-owned-vehicles"],
    ),
    "DealerFire": PlatformInfo(
        name="DealerFire",
        signatures=["dealerfire.com"],
        new_inventory_paths=["/new-inventory", "/inventory/new"],
        used_inventory_paths=["/used-inventory", "/inventory/used"],
    ),
    "Sincro": PlatformInfo(
        name="Sincro",
        signatures=["sincrodigital.com", "sincro."],
        new_inventory_paths=["/new-vehicles", "/inventory?type=new"],
        used_inventory_paths=["/used-vehicles", "/inventory?type=used"],
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
