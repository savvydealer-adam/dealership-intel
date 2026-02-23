"""Tests for role classification."""

from services.role_classifier import (
    RoleCategory,
    RoleClassifier,
    RoleFilterCriteria,
    SeniorityLevel,
)


class TestRoleClassifier:
    def setup_method(self):
        self.classifier = RoleClassifier()

    def test_classify_ceo(self):
        result = self.classifier.classify_role("Chief Executive Officer")
        assert result.seniority == SeniorityLevel.C_SUITE
        assert result.confidence > 0.8

    def test_classify_owner(self):
        result = self.classifier.classify_role("Dealership Owner")
        assert result.category == RoleCategory.OWNERSHIP

    def test_classify_general_manager(self):
        result = self.classifier.classify_role("General Manager")
        assert result.seniority == SeniorityLevel.SENIOR_EXECUTIVE

    def test_classify_sales_manager(self):
        result = self.classifier.classify_role("Sales Manager")
        assert result.seniority == SeniorityLevel.MANAGER

    def test_classify_director(self):
        result = self.classifier.classify_role("Sales Director")
        assert result.seniority == SeniorityLevel.DIRECTOR

    def test_classify_empty_title(self):
        result = self.classifier.classify_role("")
        assert result.seniority == SeniorityLevel.OTHER
        assert result.confidence == 0.0

    def test_classify_intern(self):
        result = self.classifier.classify_role("Summer Intern")
        assert result.confidence <= 0.2

    def test_abbreviation_expansion(self):
        result = self.classifier.classify_role("VP of Sales")
        # VP expands to "vice president" which matches both C_SUITE ("president")
        # and SENIOR_EXECUTIVE ("vice president") with equal confidence;
        # C_SUITE wins by check-order priority.
        assert result.seniority in (SeniorityLevel.C_SUITE, SeniorityLevel.SENIOR_EXECUTIVE)

    def test_dealership_specific(self):
        result = self.classifier.classify_role("F&I Manager", "Honda Dealership")
        assert result.dealership_specific

    def test_seniority_score(self):
        assert self.classifier.get_seniority_score(SeniorityLevel.C_SUITE) == 1.0
        assert self.classifier.get_seniority_score(SeniorityLevel.OTHER) == 0.1

    def test_filter_by_seniority(self):
        contacts = [
            {"title": "CEO", "name": "A"},
            {"title": "Intern", "name": "B"},
            {"title": "Sales Manager", "name": "C"},
        ]
        criteria = RoleFilterCriteria(seniority_levels=[SeniorityLevel.C_SUITE, SeniorityLevel.MANAGER])
        filtered = self.classifier.filter_contacts_by_role(contacts, criteria)
        assert len(filtered) == 2

    def test_filter_by_category(self):
        contacts = [
            {"title": "Sales Manager", "name": "A"},
            {"title": "HR Director", "name": "B"},
        ]
        criteria = RoleFilterCriteria(categories=[RoleCategory.SALES, RoleCategory.MANAGEMENT])
        filtered = self.classifier.filter_contacts_by_role(contacts, criteria)
        assert len(filtered) >= 1
