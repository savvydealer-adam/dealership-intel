"""Tests for provider-specific contact extraction templates."""

import pytest

from config.platforms import PLATFORM_SIGNATURES
from crawlers.contact_extractor import extract_contacts_from_html
from crawlers.platform_detector import PlatformDetector

# --- Sample HTML fixtures ---


@pytest.fixture
def dealercom_staff_html():
    """Sample Dealer.com staff page HTML."""
    return """
    <html>
    <head>
        <link rel="stylesheet" href="https://static.dealer.com/v8/global/css/bundle.css">
        <script src="https://static.dealer.com/v8/global/js/ddc-site.js"></script>
    </head>
    <body>
        <h1>Meet Our Staff</h1>
        <div class="staffDisplay">
            <div class="staffMember">
                <img src="/staff/photos/mike.jpg" />
                <h3 class="staffName">Mike Johnson</h3>
                <p class="staffJobTitle">General Manager</p>
                <a href="mailto:mjohnson@testdealer.com" class="staffEmail">mjohnson@testdealer.com</a>
                <a href="tel:5551234567" class="staffPhone">(555) 123-4567</a>
            </div>
            <div class="staffMember">
                <img src="/staff/photos/sarah.jpg" />
                <h3 class="staffName">Sarah Williams</h3>
                <p class="staffJobTitle">Sales Manager</p>
                <a href="mailto:swilliams@testdealer.com" class="staffEmail">swilliams@testdealer.com</a>
                <a href="tel:5552345678" class="staffPhone">(555) 234-5678</a>
            </div>
            <div class="staffMember">
                <img src="/staff/photos/tom.jpg" />
                <h3 class="staffName">Tom Davis</h3>
                <p class="staffJobTitle">Finance Director</p>
                <a href="mailto:tdavis@testdealer.com" class="staffEmail">tdavis@testdealer.com</a>
            </div>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def dealeron_staff_html():
    """Sample DealerOn staff page HTML."""
    return """
    <html>
    <head>
        <link rel="stylesheet" href="https://cdn.dealeron.com/assets/main.css">
        <script src="https://cdn.dealeron.com/scripts/dealeron-core.js"></script>
    </head>
    <body>
        <h1>Our Team</h1>
        <div class="staff-list">
            <div class="staffMember">
                <h3 class="name">Alice Brown</h3>
                <span class="title">Owner / President</span>
                <a href="mailto:abrown@testdealeron.com">abrown@testdealeron.com</a>
                <a href="tel:5559876543">(555) 987-6543</a>
            </div>
            <div class="staffMember">
                <h4 class="name">Bob Martinez</h4>
                <span class="position">Service Director</span>
                <a href="mailto:bmartinez@testdealeron.com">bmartinez@testdealeron.com</a>
            </div>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def dealerinspire_staff_html():
    """Sample DealerInspire staff page HTML."""
    return """
    <html>
    <head>
        <link rel="stylesheet" href="https://cdn.dealerinspire.com/theme/style.css">
    </head>
    <body>
        <h1>Meet the Team</h1>
        <div class="team-grid">
            <div class="team-member">
                <img src="/team/chris.jpg" />
                <h3 class="member-name">Chris Taylor</h3>
                <p class="member-title">General Sales Manager</p>
                <a href="mailto:ctaylor@testinspire.com">ctaylor@testinspire.com</a>
                <a href="tel:5554567890">(555) 456-7890</a>
            </div>
            <div class="team-member">
                <img src="/team/diana.jpg" />
                <h3 class="member-name">Diana Ross</h3>
                <p class="member-title">Finance Manager</p>
                <a href="mailto:dross@testinspire.com">dross@testinspire.com</a>
            </div>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def generic_staff_html():
    """Staff page with no provider-specific patterns."""
    return """
    <html>
    <body>
        <h1>Our Staff</h1>
        <div class="staff-member">
            <h3>John Generic</h3>
            <p class="title">Sales Manager</p>
            <a href="mailto:jgeneric@genericdealer.com">jgeneric@genericdealer.com</a>
            <p>(555) 111-2222</p>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def dealercom_inventory_html():
    """Sample Dealer.com inventory page HTML."""
    return """
    <html>
    <body>
        <div class="totalCount">142 vehicles found</div>
        <div class="vehicle-card">Vehicle 1</div>
        <div class="vehicle-card">Vehicle 2</div>
    </body>
    </html>
    """


# --- Provider-specific extraction tests ---


class TestDealerComExtraction:
    def test_extracts_all_contacts(self, dealercom_staff_html):
        platform_info = PLATFORM_SIGNATURES["Dealer.com"]
        contacts = extract_contacts_from_html(dealercom_staff_html, "testdealer.com", platform_info)
        assert len(contacts) == 3

    def test_extracts_names(self, dealercom_staff_html):
        platform_info = PLATFORM_SIGNATURES["Dealer.com"]
        contacts = extract_contacts_from_html(dealercom_staff_html, "testdealer.com", platform_info)
        names = {c["name"] for c in contacts}
        assert "Mike Johnson" in names
        assert "Sarah Williams" in names
        assert "Tom Davis" in names

    def test_extracts_titles(self, dealercom_staff_html):
        platform_info = PLATFORM_SIGNATURES["Dealer.com"]
        contacts = extract_contacts_from_html(dealercom_staff_html, "testdealer.com", platform_info)
        titles = {c.get("title") for c in contacts}
        assert "General Manager" in titles
        assert "Sales Manager" in titles
        assert "Finance Director" in titles

    def test_extracts_emails(self, dealercom_staff_html):
        platform_info = PLATFORM_SIGNATURES["Dealer.com"]
        contacts = extract_contacts_from_html(dealercom_staff_html, "testdealer.com", platform_info)
        emails = {c.get("email") for c in contacts}
        assert "mjohnson@testdealer.com" in emails
        assert "swilliams@testdealer.com" in emails

    def test_extracts_phones(self, dealercom_staff_html):
        platform_info = PLATFORM_SIGNATURES["Dealer.com"]
        contacts = extract_contacts_from_html(dealercom_staff_html, "testdealer.com", platform_info)
        contacts_with_phone = [c for c in contacts if c.get("phone")]
        assert len(contacts_with_phone) >= 2

    def test_source_is_crawl(self, dealercom_staff_html):
        platform_info = PLATFORM_SIGNATURES["Dealer.com"]
        contacts = extract_contacts_from_html(dealercom_staff_html, "testdealer.com", platform_info)
        for contact in contacts:
            assert contact["source"] == "crawl"


class TestDealerOnExtraction:
    def test_extracts_contacts(self, dealeron_staff_html):
        platform_info = PLATFORM_SIGNATURES["DealerOn"]
        contacts = extract_contacts_from_html(dealeron_staff_html, "testdealeron.com", platform_info)
        assert len(contacts) == 2

    def test_extracts_names(self, dealeron_staff_html):
        platform_info = PLATFORM_SIGNATURES["DealerOn"]
        contacts = extract_contacts_from_html(dealeron_staff_html, "testdealeron.com", platform_info)
        names = {c["name"] for c in contacts}
        assert "Alice Brown" in names
        assert "Bob Martinez" in names

    def test_extracts_emails(self, dealeron_staff_html):
        platform_info = PLATFORM_SIGNATURES["DealerOn"]
        contacts = extract_contacts_from_html(dealeron_staff_html, "testdealeron.com", platform_info)
        emails = {c.get("email") for c in contacts}
        assert "abrown@testdealeron.com" in emails


class TestDealerInspireExtraction:
    def test_extracts_contacts(self, dealerinspire_staff_html):
        platform_info = PLATFORM_SIGNATURES["DealerInspire"]
        contacts = extract_contacts_from_html(dealerinspire_staff_html, "testinspire.com", platform_info)
        assert len(contacts) == 2

    def test_extracts_names(self, dealerinspire_staff_html):
        platform_info = PLATFORM_SIGNATURES["DealerInspire"]
        contacts = extract_contacts_from_html(dealerinspire_staff_html, "testinspire.com", platform_info)
        names = {c["name"] for c in contacts}
        assert "Chris Taylor" in names
        assert "Diana Ross" in names

    def test_extracts_titles(self, dealerinspire_staff_html):
        platform_info = PLATFORM_SIGNATURES["DealerInspire"]
        contacts = extract_contacts_from_html(dealerinspire_staff_html, "testinspire.com", platform_info)
        titles = {c.get("title") for c in contacts}
        assert "General Sales Manager" in titles
        assert "Finance Manager" in titles


# --- Generic fallback tests ---


class TestGenericFallback:
    def test_extracts_without_platform_info(self, generic_staff_html):
        contacts = extract_contacts_from_html(generic_staff_html, "genericdealer.com")
        assert len(contacts) >= 1

    def test_still_finds_email(self, generic_staff_html):
        contacts = extract_contacts_from_html(generic_staff_html, "genericdealer.com")
        emails = {c.get("email") for c in contacts}
        assert "jgeneric@genericdealer.com" in emails

    def test_provider_extraction_takes_priority(self, dealercom_staff_html):
        """With platform_info, provider-specific extraction should be used."""
        platform_info = PLATFORM_SIGNATURES["Dealer.com"]
        contacts_with = extract_contacts_from_html(dealercom_staff_html, "testdealer.com", platform_info)
        contacts_without = extract_contacts_from_html(dealercom_staff_html, "testdealer.com")

        # Provider-specific should find at least as many
        assert len(contacts_with) >= len(contacts_without)


# --- Platform detection tests ---


class TestPlatformDetection:
    def setup_method(self):
        self.detector = PlatformDetector()

    def test_detect_dealercom(self, dealercom_staff_html):
        result = self.detector.detect_from_html(dealercom_staff_html)
        assert result["platform"] == "Dealer.com"
        assert result["confidence"] > 0.7

    def test_detect_dealeron(self, dealeron_staff_html):
        result = self.detector.detect_from_html(dealeron_staff_html)
        assert result["platform"] == "DealerOn"
        assert result["confidence"] > 0.7

    def test_detect_dealerinspire(self, dealerinspire_staff_html):
        result = self.detector.detect_from_html(dealerinspire_staff_html)
        assert result["platform"] == "DealerInspire"
        assert result["confidence"] > 0.7

    def test_detect_unknown(self, generic_staff_html):
        result = self.detector.detect_from_html(generic_staff_html)
        assert result["platform"] == "Custom/Unknown"
        assert result["confidence"] == 0.0

    def test_detect_wordpress(self):
        html = '<html><head><link href="/wp-content/themes/flavor/style.css"></head><body></body></html>'
        result = self.detector.detect_from_html(html)
        assert result["platform"] == "WordPress"

    def test_detect_new_provider_dealer_car_search(self):
        html = '<html><head><script src="https://dealercarsearch.com/assets/js/main.js"></script></head><body></body></html>'
        result = self.detector.detect_from_html(html)
        assert result["platform"] == "Dealer Car Search"


# --- PlatformInfo completeness tests ---


class TestPlatformInfoCompleteness:
    """Verify all platform entries have the required template fields populated."""

    @pytest.mark.parametrize("platform_name", list(PLATFORM_SIGNATURES.keys()))
    def test_has_signatures(self, platform_name):
        info = PLATFORM_SIGNATURES[platform_name]
        assert len(info.signatures) > 0, f"{platform_name} has no signatures"

    @pytest.mark.parametrize("platform_name", list(PLATFORM_SIGNATURES.keys()))
    def test_has_inventory_paths(self, platform_name):
        info = PLATFORM_SIGNATURES[platform_name]
        assert len(info.new_inventory_paths) > 0, f"{platform_name} has no new inventory paths"
        assert len(info.used_inventory_paths) > 0, f"{platform_name} has no used inventory paths"

    @pytest.mark.parametrize("platform_name", list(PLATFORM_SIGNATURES.keys()))
    def test_has_staff_paths(self, platform_name):
        info = PLATFORM_SIGNATURES[platform_name]
        assert len(info.staff_page_paths) > 0, f"{platform_name} has no staff page paths"

    @pytest.mark.parametrize("platform_name", list(PLATFORM_SIGNATURES.keys()))
    def test_has_card_selectors(self, platform_name):
        info = PLATFORM_SIGNATURES[platform_name]
        assert len(info.card_selectors) > 0, f"{platform_name} has no card selectors"

    @pytest.mark.parametrize("platform_name", list(PLATFORM_SIGNATURES.keys()))
    def test_has_name_selectors(self, platform_name):
        info = PLATFORM_SIGNATURES[platform_name]
        assert len(info.name_selectors) > 0, f"{platform_name} has no name selectors"


# --- Deduplication tests ---


class TestDeduplication:
    def test_deduplicates_by_email(self):
        html = """
        <html><body>
            <div class="staff-member">
                <h3>John Doe</h3>
                <a href="mailto:jdoe@test.com">jdoe@test.com</a>
            </div>
            <div class="staff-member">
                <h3>John Doe</h3>
                <a href="mailto:jdoe@test.com">jdoe@test.com</a>
            </div>
        </body></html>
        """
        contacts = extract_contacts_from_html(html, "test.com")
        emails = [c.get("email") for c in contacts if c.get("email")]
        assert len(emails) == len(set(emails))
