"""Tests for contact extraction from HTML."""

from crawlers.contact_extractor import (
    extract_contacts_from_html,
    extract_emails,
    extract_phones,
)


class TestExtractEmails:
    def test_standard_email(self):
        emails = extract_emails("Contact jsmith@testdealer.com for info", "testdealer.com")
        assert "jsmith@testdealer.com" in emails

    def test_mailto_link(self):
        emails = extract_emails('<a href="mailto:jane@testdealer.com">Email Jane</a>', "testdealer.com")
        assert "jane@testdealer.com" in emails

    def test_excludes_generic_emails(self):
        text = "Email noreply@testdealer.com or support@testdealer.com"
        emails = extract_emails(text, "testdealer.com")
        assert len(emails) == 0

    def test_excludes_non_matching_domain(self):
        emails = extract_emails("user@otherdomain.com", "testdealer.com")
        assert len(emails) == 0

    def test_deduplication(self):
        text = "jsmith@testdealer.com and also jsmith@testdealer.com"
        emails = extract_emails(text, "testdealer.com")
        assert len(emails) == 1


class TestExtractPhones:
    def test_parenthesized_format(self):
        phones = extract_phones("Call us at (555) 123-4567")
        assert len(phones) == 1

    def test_dashed_format(self):
        phones = extract_phones("Phone: 555-123-4567")
        assert len(phones) == 1

    def test_dotted_format(self):
        phones = extract_phones("555.123.4567")
        assert len(phones) == 1

    def test_with_country_code(self):
        phones = extract_phones("+1 (555) 123-4567")
        assert len(phones) == 1

    def test_too_short(self):
        phones = extract_phones("123-4567")
        assert len(phones) == 0


class TestExtractContactsFromHtml:
    def test_structured_staff_page(self, sample_html_staff_page):
        contacts = extract_contacts_from_html(sample_html_staff_page, "testdealer.com")
        assert len(contacts) >= 2

        names = [c.get("name", "") for c in contacts]
        assert any("John Smith" in n for n in names)

    def test_empty_html(self):
        contacts = extract_contacts_from_html("<html><body></body></html>", "test.com")
        assert len(contacts) == 0

    def test_source_attribution(self, sample_html_staff_page):
        contacts = extract_contacts_from_html(sample_html_staff_page, "testdealer.com")
        for contact in contacts:
            assert contact.get("source") == "crawl"
