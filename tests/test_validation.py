"""Tests for contact validation and confidence scoring."""

from services.validation import ContactValidator


class TestContactValidator:
    def setup_method(self):
        self.validator = ContactValidator(email_verification_service=None)

    def test_validate_valid_email(self):
        result = self.validator.validate_email("jsmith@testdealer.com")
        assert result.is_valid

    def test_validate_invalid_email(self):
        result = self.validator.validate_email("not-an-email")
        assert not result.is_valid

    def test_validate_empty_email(self):
        result = self.validator.validate_email("")
        assert not result.is_valid

    def test_validate_personal_email(self):
        result = self.validator.validate_email("user@gmail.com")
        assert "Personal email domain" in result.issues[0]

    def test_validate_valid_phone(self):
        result = self.validator.validate_phone("(555) 123-4567")
        assert result.is_valid
        assert result.normalized_value is not None

    def test_validate_invalid_phone(self):
        result = self.validator.validate_phone("123")
        assert not result.is_valid

    def test_validate_valid_name(self):
        result = self.validator.validate_name("John Smith")
        assert result.is_valid

    def test_validate_empty_name(self):
        result = self.validator.validate_name("")
        assert not result.is_valid

    def test_validate_suspicious_name(self):
        result = self.validator.validate_name("test")
        assert not result.is_valid

    def test_validate_linkedin_url(self):
        result = self.validator.validate_linkedin_url("https://linkedin.com/in/jsmith")
        assert result.is_valid

    def test_validate_non_linkedin_url(self):
        result = self.validator.validate_linkedin_url("https://example.com/jsmith")
        assert not result.is_valid

    def test_confidence_score_complete_contact(self, sample_contact):
        validation = self.validator.validate_contact(sample_contact)
        score, factors = self.validator.calculate_confidence_score(sample_contact, validation)
        assert score > 50  # Complete contact should score well
        assert factors.data_completeness > 0

    def test_confidence_score_minimal_contact(self):
        contact = {"name": "Test", "email": "", "title": ""}
        validation = self.validator.validate_contact(contact)
        score, factors = self.validator.calculate_confidence_score(contact, validation)
        assert score < 30  # Minimal contact should score low
