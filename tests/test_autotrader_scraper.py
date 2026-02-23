"""Tests for the Autotrader dealer scraper."""

import json

import pytest

from crawlers.autotrader_scraper import (
    AutotraderDealer,
    _extract_dealer_website,
    _extract_inventory_count,
    _extract_jsonld,
    extract_dealer_data,
    parse_autotrader_url,
)

# --- Fixtures ---


@pytest.fixture
def sample_autotrader_html():
    """Realistic Autotrader dealer page HTML with JSON-LD and website link."""
    jsonld = {
        "@context": "https://schema.org",
        "@type": "AutoDealer",
        "name": "Bob's Auto Sales",
        "telephone": "(207) 555-1234",
        "address": {
            "@type": "PostalAddress",
            "streetAddress": "123 Main St",
            "addressLocality": "Portland",
            "addressRegion": "ME",
            "postalCode": "04101",
        },
        "aggregateRating": {
            "@type": "AggregateRating",
            "ratingValue": "4.7",
            "reviewCount": "142",
        },
        "openingHoursSpecification": [
            {
                "@type": "OpeningHoursSpecification",
                "dayOfWeek": "Monday",
                "opens": "09:00",
                "closes": "18:00",
            }
        ],
    }
    return f"""
    <html>
    <head>
        <script type="application/ld+json">{json.dumps(jsonld)}</script>
    </head>
    <body>
        <div class="dealer-info">
            <h1>Bob's Auto Sales</h1>
            <a href="https://www.bobsautosales.com" data-cmp="dealerWebsite">Dealer Website</a>
            <p>Showing 1-25 of 347 vehicles</p>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_autotrader_html_no_jsonld():
    """Autotrader page without JSON-LD markup."""
    return """
    <html>
    <head><title>Some Dealer</title></head>
    <body>
        <div class="dealer-info">
            <h1>Some Dealer Name</h1>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_autotrader_html_no_website():
    """Autotrader page with JSON-LD but no external website link."""
    jsonld = {
        "@context": "https://schema.org",
        "@type": "AutoDealer",
        "name": "No Website Motors",
        "telephone": "(555) 999-0000",
        "address": {
            "@type": "PostalAddress",
            "streetAddress": "456 Oak Ave",
            "addressLocality": "Austin",
            "addressRegion": "TX",
            "postalCode": "73301",
        },
    }
    return f"""
    <html>
    <head>
        <script type="application/ld+json">{json.dumps(jsonld)}</script>
    </head>
    <body>
        <div class="dealer-info">
            <h1>No Website Motors</h1>
        </div>
    </body>
    </html>
    """


# --- TestParseAutotraderUrl ---


class TestParseAutotraderUrl:
    def test_standard_url(self):
        url = "https://www.autotrader.com/car-dealers/portland-me/12345/bobs-auto-sales"
        dealer_id, city_state, slug = parse_autotrader_url(url)
        assert dealer_id == "12345"
        assert city_state == "portland-me"
        assert slug == "bobs-auto-sales"

    def test_url_with_query_params(self):
        url = "https://www.autotrader.com/car-dealers/austin-tx/67890/texas-motors?ref=sitemap"
        dealer_id, city_state, slug = parse_autotrader_url(url)
        assert dealer_id == "67890"
        assert city_state == "austin-tx"
        assert slug == "texas-motors"

    def test_invalid_url(self):
        with pytest.raises(ValueError, match="does not match"):
            parse_autotrader_url("https://www.autotrader.com/some-other-page")

    def test_url_with_trailing_slash(self):
        url = "https://www.autotrader.com/car-dealers/miami-fl/11111/sunshine-auto/"
        dealer_id, city_state, slug = parse_autotrader_url(url)
        assert dealer_id == "11111"
        assert city_state == "miami-fl"


# --- TestExtractDealerData ---


class TestExtractDealerData:
    def test_full_extraction(self, sample_autotrader_html):
        url = "https://www.autotrader.com/car-dealers/portland-me/12345/bobs-auto-sales"
        dealer = extract_dealer_data(sample_autotrader_html, url)

        assert dealer is not None
        assert dealer.autotrader_dealer_id == "12345"
        assert dealer.name == "Bob's Auto Sales"
        assert dealer.phone == "(207) 555-1234"
        assert dealer.city == "Portland"
        assert dealer.state == "ME"
        assert dealer.postal_code == "04101"
        assert dealer.rating_value == 4.7
        assert dealer.review_count == 142
        assert len(dealer.hours) == 1
        assert dealer.hours[0]["days"] == "Monday"

    def test_website_extraction(self, sample_autotrader_html):
        url = "https://www.autotrader.com/car-dealers/portland-me/12345/bobs-auto-sales"
        dealer = extract_dealer_data(sample_autotrader_html, url)

        assert dealer is not None
        assert dealer.website_url == "https://www.bobsautosales.com"
        assert dealer.domain == "bobsautosales.com"

    def test_inventory_extraction(self, sample_autotrader_html):
        url = "https://www.autotrader.com/car-dealers/portland-me/12345/bobs-auto-sales"
        dealer = extract_dealer_data(sample_autotrader_html, url)

        assert dealer is not None
        assert dealer.inventory_count == 347

    def test_missing_jsonld_fallback(self, sample_autotrader_html_no_jsonld):
        url = "https://www.autotrader.com/car-dealers/portland-me/12345/some-dealer-name"
        dealer = extract_dealer_data(sample_autotrader_html_no_jsonld, url)

        assert dealer is not None
        assert dealer.name == "Some Dealer Name"
        assert dealer.phone == ""
        assert dealer.rating_value is None

    def test_no_website_uses_synthetic_domain(self, sample_autotrader_html_no_website):
        url = "https://www.autotrader.com/car-dealers/austin-tx/67890/no-website-motors"
        dealer = extract_dealer_data(sample_autotrader_html_no_website, url)

        assert dealer is not None
        assert dealer.website_url == ""
        assert dealer.domain == "autotrader-67890"

    def test_full_address_property(self, sample_autotrader_html):
        url = "https://www.autotrader.com/car-dealers/portland-me/12345/bobs-auto-sales"
        dealer = extract_dealer_data(sample_autotrader_html, url)

        assert dealer is not None
        assert "123 Main St" in dealer.full_address
        assert "Portland" in dealer.full_address
        assert "ME" in dealer.full_address
        assert "04101" in dealer.full_address

    def test_invalid_url_returns_none(self):
        dealer = extract_dealer_data("<html></html>", "https://example.com/bad")
        assert dealer is None


# --- TestExtractJsonld ---


class TestExtractJsonld:
    def test_extracts_auto_dealer(self, sample_autotrader_html):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(sample_autotrader_html, "lxml")
        result = _extract_jsonld(soup)
        assert result is not None
        assert result["@type"] == "AutoDealer"
        assert result["name"] == "Bob's Auto Sales"

    def test_handles_graph(self):
        from bs4 import BeautifulSoup

        jsonld = {
            "@context": "https://schema.org",
            "@graph": [
                {"@type": "WebPage", "name": "Dealer Page"},
                {"@type": "AutoDealer", "name": "Graph Dealer"},
            ],
        }
        html = f'<script type="application/ld+json">{json.dumps(jsonld)}</script>'
        soup = BeautifulSoup(html, "lxml")
        result = _extract_jsonld(soup)
        assert result is not None
        assert result["name"] == "Graph Dealer"

    def test_returns_none_for_no_jsonld(self):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup("<html><body></body></html>", "lxml")
        result = _extract_jsonld(soup)
        assert result is None


# --- TestExtractDealerWebsite ---


class TestExtractDealerWebsite:
    def test_text_based_extraction(self):
        from bs4 import BeautifulSoup

        html = '<a href="https://www.dealer.com">Dealer Website</a>'
        soup = BeautifulSoup(html, "lxml")
        assert _extract_dealer_website(soup) == "https://www.dealer.com"

    def test_data_cmp_extraction(self):
        from bs4 import BeautifulSoup

        html = '<a href="https://www.dealer.com" data-cmp="dealerWebsiteLink">Visit</a>'
        soup = BeautifulSoup(html, "lxml")
        assert _extract_dealer_website(soup) == "https://www.dealer.com"

    def test_ignores_autotrader_links(self):
        from bs4 import BeautifulSoup

        html = '<a href="https://www.autotrader.com/inventory">Dealer Website</a>'
        soup = BeautifulSoup(html, "lxml")
        assert _extract_dealer_website(soup) == ""

    def test_returns_empty_when_none_found(self):
        from bs4 import BeautifulSoup

        html = "<div>No links here</div>"
        soup = BeautifulSoup(html, "lxml")
        assert _extract_dealer_website(soup) == ""


# --- TestExtractInventoryCount ---


class TestExtractInventoryCount:
    def test_showing_x_of_y(self):
        from bs4 import BeautifulSoup

        html = "<p>Showing 1-25 of 347 vehicles</p>"
        soup = BeautifulSoup(html, "lxml")
        assert _extract_inventory_count(soup) == 347

    def test_x_vehicles_for_sale(self):
        from bs4 import BeautifulSoup

        html = "<p>123 vehicles for sale</p>"
        soup = BeautifulSoup(html, "lxml")
        assert _extract_inventory_count(soup) == 123

    def test_comma_separated_number(self):
        from bs4 import BeautifulSoup

        html = "<p>Showing 1-25 of 1,234</p>"
        soup = BeautifulSoup(html, "lxml")
        assert _extract_inventory_count(soup) == 1234

    def test_returns_none_when_not_found(self):
        from bs4 import BeautifulSoup

        html = "<p>Welcome to our dealership</p>"
        soup = BeautifulSoup(html, "lxml")
        assert _extract_inventory_count(soup) is None


# --- TestAutotraderDealer ---


class TestAutotraderDealer:
    def test_domain_from_website(self):
        dealer = AutotraderDealer(
            autotrader_dealer_id="123",
            website_url="https://www.bobsauto.com/inventory",
        )
        assert dealer.domain == "bobsauto.com"

    def test_domain_synthetic_when_no_website(self):
        dealer = AutotraderDealer(
            autotrader_dealer_id="456",
            website_url="",
        )
        assert dealer.domain == "autotrader-456"

    def test_full_address(self):
        dealer = AutotraderDealer(
            street_address="100 Main St",
            city="Portland",
            state="ME",
            postal_code="04101",
        )
        assert dealer.full_address == "100 Main St, Portland, ME, 04101"

    def test_full_address_partial(self):
        dealer = AutotraderDealer(city="Portland", state="ME")
        assert dealer.full_address == "Portland, ME"
