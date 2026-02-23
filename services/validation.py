"""Contact validation service for dealership contact data quality."""

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

from .email_verification import EmailVerificationService, VerificationResult
from .role_classifier import RoleClassifier

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    is_valid: bool
    issues: list[str] = field(default_factory=list)
    normalized_value: Optional[str] = None
    verification_result: Optional[VerificationResult] = None


@dataclass
class ContactValidation:
    email: ValidationResult
    phone: ValidationResult
    name: ValidationResult
    linkedin: ValidationResult
    title: ValidationResult
    overall_issues: list[str] = field(default_factory=list)


@dataclass
class ConfidenceFactors:
    data_completeness: float = 0.0
    domain_consistency: float = 0.0
    professional_title: float = 0.0
    linkedin_presence: float = 0.0
    data_consistency: float = 0.0
    email_quality: float = 0.0


class ContactValidator:
    """Validates and scores dealership contact data quality."""

    SENIOR_TITLES = {
        "owner",
        "co-owner",
        "president",
        "ceo",
        "chief executive officer",
        "cfo",
        "chief financial officer",
        "coo",
        "chief operating officer",
        "general manager",
        "managing partner",
        "principal",
        "dealer principal",
        "executive director",
        "managing director",
        "vice president",
        "vp",
        "svp",
        "evp",
    }

    MANAGEMENT_TITLES = {
        "director",
        "manager",
        "sales manager",
        "service manager",
        "finance manager",
        "parts manager",
        "marketing manager",
        "operations manager",
        "general sales manager",
        "f&i manager",
        "business manager",
        "fixed operations manager",
        "internet sales manager",
        "fleet manager",
    }

    DISPOSABLE_EMAIL_DOMAINS = {
        "mailinator.com",
        "guerrillamail.com",
        "tempmail.com",
        "throwaway.email",
        "yopmail.com",
        "sharklasers.com",
        "guerrillamailblock.com",
        "grr.la",
        "dispostable.com",
        "trashmail.com",
        "10minutemail.com",
        "temp-mail.org",
        "fakeinbox.com",
        "mailnesia.com",
        "maildrop.cc",
    }

    PERSONAL_EMAIL_DOMAINS = {
        "gmail.com",
        "yahoo.com",
        "hotmail.com",
        "outlook.com",
        "aol.com",
        "icloud.com",
        "mail.com",
        "protonmail.com",
        "zoho.com",
        "yandex.com",
        "live.com",
        "msn.com",
        "comcast.net",
        "att.net",
        "verizon.net",
        "cox.net",
    }

    def __init__(
        self,
        email_verification_service: Optional[EmailVerificationService] = None,
        role_classifier: Optional[RoleClassifier] = None,
    ):
        self.email_service = email_verification_service
        self.role_classifier = role_classifier or RoleClassifier()

    def validate_contact(self, contact: dict) -> ContactValidation:
        """Run full validation on a contact record."""
        email_result = self.validate_email(contact.get("email", ""))
        phone_result = self.validate_phone(contact.get("phone", ""))
        name_result = self.validate_name(contact.get("name", ""))
        linkedin_result = self.validate_linkedin_url(contact.get("linkedin_url", ""))
        title_result = self.validate_title(contact.get("title", ""))

        overall_issues: list[str] = []

        if not email_result.is_valid and not phone_result.is_valid:
            overall_issues.append("No valid contact method (email or phone)")

        if not name_result.is_valid:
            overall_issues.append("Invalid or missing contact name")

        if not title_result.is_valid:
            overall_issues.append("Missing or invalid job title")

        # Cross-field consistency checks
        email = contact.get("email", "")
        name = contact.get("name", "")
        if email and name and "@" in email:
            domain = email.rsplit("@", 1)[1].lower()
            if domain not in self.PERSONAL_EMAIL_DOMAINS:
                local_part = email.rsplit("@", 1)[0].lower()
                name_parts = name.lower().split()
                name_in_email = any(part in local_part for part in name_parts if len(part) > 2)
                if not name_in_email:
                    overall_issues.append("Email local part does not appear to match contact name")

        validation = ContactValidation(
            email=email_result,
            phone=phone_result,
            name=name_result,
            linkedin=linkedin_result,
            title=title_result,
            overall_issues=overall_issues,
        )

        logger.debug(
            f"Validated contact '{contact.get('name', 'unknown')}': "
            f"email={email_result.is_valid}, phone={phone_result.is_valid}, "
            f"issues={len(overall_issues)}"
        )

        return validation

    def validate_email(self, email: str) -> ValidationResult:
        """Validate an email address."""
        if not email or not isinstance(email, str):
            return ValidationResult(is_valid=False, issues=["Missing email address"])

        email = email.strip().lower()
        issues: list[str] = []

        # Basic format check
        email_regex = re.compile(
            r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"
            r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
            r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
        )
        if not email_regex.match(email):
            return ValidationResult(
                is_valid=False,
                issues=["Invalid email format"],
                normalized_value=email,
            )

        # Domain checks
        domain = email.rsplit("@", 1)[1]

        if domain in self.DISPOSABLE_EMAIL_DOMAINS:
            issues.append("Disposable email domain")

        if domain in self.PERSONAL_EMAIL_DOMAINS:
            issues.append("Personal email domain (not company email)")

        # Verify via email verification service if available
        verification_result = None
        if self.email_service:
            try:
                verification_result = self.email_service.verify_email(email)
                if not verification_result.is_valid:
                    issues.extend(verification_result.issues)
            except Exception as e:
                logger.warning(f"Email verification failed for {email}: {e}")
                issues.append(f"Verification service error: {str(e)[:100]}")

        is_valid = "Invalid email format" not in issues and "Disposable email domain" not in issues
        if verification_result and not verification_result.is_valid:
            is_valid = False

        return ValidationResult(
            is_valid=is_valid,
            issues=issues,
            normalized_value=email,
            verification_result=verification_result,
        )

    def validate_phone(self, phone: str) -> ValidationResult:
        """Validate and normalize a phone number."""
        if not phone or not isinstance(phone, str):
            return ValidationResult(is_valid=False, issues=["Missing phone number"])

        phone = phone.strip()
        issues: list[str] = []

        # Strip common formatting characters
        normalized = self._normalize_phone_number(phone)

        if not normalized:
            return ValidationResult(
                is_valid=False,
                issues=["Phone number contains no digits"],
                normalized_value=phone,
            )

        # Check length (US numbers: 10 digits, with country code: 11)
        digit_count = len(re.sub(r"\D", "", normalized))

        if digit_count < 10:
            issues.append(f"Phone number too short ({digit_count} digits)")
        elif digit_count > 15:
            issues.append(f"Phone number too long ({digit_count} digits)")

        # Check for obviously fake numbers
        digits_only = re.sub(r"\D", "", normalized)
        if digits_only and len(set(digits_only)) == 1:
            issues.append("Phone number appears to be fake (all same digit)")

        if re.match(r"^(0{10}|1{10}|123456)", digits_only):
            issues.append("Phone number appears to be a placeholder")

        is_valid = len(issues) == 0

        return ValidationResult(
            is_valid=is_valid,
            issues=issues,
            normalized_value=normalized,
        )

    def validate_name(self, name: str) -> ValidationResult:
        """Validate a contact name."""
        if not name or not isinstance(name, str):
            return ValidationResult(is_valid=False, issues=["Missing contact name"])

        name = name.strip()
        issues: list[str] = []

        # Check minimum length
        if len(name) < 2:
            issues.append("Name is too short")

        # Check for placeholder names
        placeholder_names = {
            "test",
            "unknown",
            "n/a",
            "na",
            "none",
            "null",
            "admin",
            "user",
            "contact",
            "info",
            "support",
        }
        if name.lower() in placeholder_names:
            issues.append("Name appears to be a placeholder")

        # Check for at least two words (first + last)
        name_parts = name.split()
        if len(name_parts) < 2:
            issues.append("Name appears to be missing first or last name")

        # Check for non-letter characters (allow hyphens, apostrophes, spaces, periods)
        if re.search(r"[^a-zA-Z\s\-'.]+", name):
            issues.append("Name contains unexpected characters")

        # Check for excessive length
        if len(name) > 100:
            issues.append("Name is unusually long")

        # Normalize capitalization
        normalized = " ".join(part.capitalize() for part in name_parts)

        is_valid = len(issues) == 0

        return ValidationResult(
            is_valid=is_valid,
            issues=issues,
            normalized_value=normalized,
        )

    def validate_linkedin_url(self, url: str) -> ValidationResult:
        """Validate a LinkedIn profile URL."""
        if not url or not isinstance(url, str):
            return ValidationResult(is_valid=False, issues=["Missing LinkedIn URL"])

        url = url.strip()
        issues: list[str] = []

        # Check for LinkedIn URL pattern
        linkedin_pattern = re.compile(r"^https?://(www\.)?linkedin\.com/in/[a-zA-Z0-9\-_%]+/?$")
        company_pattern = re.compile(r"^https?://(www\.)?linkedin\.com/company/[a-zA-Z0-9\-_%]+/?$")

        if not linkedin_pattern.match(url) and not company_pattern.match(url):
            # Try to normalize common issues
            if "linkedin.com" in url.lower():
                issues.append("LinkedIn URL format is non-standard")
            else:
                issues.append("Not a valid LinkedIn URL")
                return ValidationResult(is_valid=False, issues=issues, normalized_value=url)

        # Normalize URL
        normalized = url.rstrip("/")
        if not normalized.startswith("http"):
            normalized = "https://" + normalized

        is_valid = "Not a valid LinkedIn URL" not in issues

        return ValidationResult(
            is_valid=is_valid,
            issues=issues,
            normalized_value=normalized,
        )

    def validate_title(self, title: str) -> ValidationResult:
        """Validate a job title."""
        if not title or not isinstance(title, str):
            return ValidationResult(is_valid=False, issues=["Missing job title"])

        title = title.strip()
        issues: list[str] = []

        # Check minimum length
        if len(title) < 2:
            issues.append("Job title is too short")

        # Check for placeholder titles
        placeholder_titles = {
            "test",
            "unknown",
            "n/a",
            "na",
            "none",
            "null",
            "employee",
            "staff",
        }
        if title.lower() in placeholder_titles:
            issues.append("Job title appears to be a placeholder")

        # Check for excessive length
        if len(title) > 200:
            issues.append("Job title is unusually long")

        is_valid = len(issues) == 0

        return ValidationResult(
            is_valid=is_valid,
            issues=issues,
            normalized_value=title,
        )

    def calculate_confidence_score(
        self, contact: dict, validation: Optional[ContactValidation] = None
    ) -> tuple[float, ConfidenceFactors]:
        """Calculate a confidence score for a contact based on data quality."""
        if validation is None:
            validation = self.validate_contact(contact)

        factors = ConfidenceFactors()
        total_score = 0.0

        # 1. Data completeness (0-25 points)
        completeness_fields = ["email", "phone", "name", "title", "linkedin_url"]
        filled_count = sum(1 for f in completeness_fields if contact.get(f) and str(contact[f]).strip())
        factors.data_completeness = (filled_count / len(completeness_fields)) * 25
        total_score += factors.data_completeness

        # 2. Domain consistency (0-15 points)
        email = contact.get("email", "")
        company_domain = contact.get("company_domain", "")
        if email and "@" in email:
            email_domain = email.rsplit("@", 1)[1].lower()
            if company_domain and email_domain == company_domain.lower():
                factors.domain_consistency = 15.0
            elif email_domain not in self.PERSONAL_EMAIL_DOMAINS:
                factors.domain_consistency = 10.0
            else:
                factors.domain_consistency = 3.0
        total_score += factors.domain_consistency

        # 3. Professional title (0-20 points) with role classification
        title = contact.get("title", "")
        if title:
            # Use role classifier for enhanced scoring
            role_classification = self.role_classifier.classify_role(title, contact.get("company_name", ""))

            seniority_mapping = {
                "C-Suite": 20.0,
                "Senior Executive": 18.0,
                "Director": 16.0,
                "Manager": 14.0,
                "Specialist": 10.0,
                "Coordinator": 6.0,
                "Other": 4.0,
            }

            base_title_score = seniority_mapping.get(role_classification.seniority.value, 4.0)

            # Category bonus for dealership-relevant roles
            category_bonus = {
                "Ownership": 5.0,
                "Senior Leadership": 4.0,
                "Management": 3.0,
                "Department Head": 3.0,
                "Sales": 2.0,
                "Service": 2.0,
                "Finance": 2.0,
                "Marketing": 1.0,
                "Operations": 1.0,
                "IT/Technology": 0.5,
                "HR/Admin": 0.5,
                "Specialist": 1.0,
                "Other": 0.0,
            }

            bonus = category_bonus.get(role_classification.category.value, 0.0)
            factors.professional_title = min(20.0, base_title_score + bonus)

            # Dealership-specific boost
            if role_classification.dealership_specific:
                factors.professional_title = min(20.0, factors.professional_title + 2.0)

            logger.debug(
                f"Role classification for '{title}': "
                f"seniority={role_classification.seniority.value}, "
                f"category={role_classification.category.value}, "
                f"score={factors.professional_title}"
            )
        total_score += factors.professional_title

        # 4. LinkedIn presence (0-15 points)
        linkedin = contact.get("linkedin_url", "")
        if linkedin and validation.linkedin.is_valid:
            factors.linkedin_presence = 15.0
        elif linkedin:
            factors.linkedin_presence = 5.0
        total_score += factors.linkedin_presence

        # 5. Data consistency (0-15 points)
        consistency_score = 15.0

        # Deduct for overall issues
        consistency_score -= len(validation.overall_issues) * 3.0

        # Deduct for field-level issues
        all_field_issues = (
            len(validation.email.issues)
            + len(validation.phone.issues)
            + len(validation.name.issues)
            + len(validation.title.issues)
            + len(validation.linkedin.issues)
        )
        consistency_score -= all_field_issues * 1.0

        factors.data_consistency = max(0.0, consistency_score)
        total_score += factors.data_consistency

        # 6. Email quality (0-20 points)
        if validation.email.is_valid:
            email_score = 15.0
            if validation.email.verification_result:
                vr = validation.email.verification_result
                if vr.verification_level == "mailbox":
                    email_score = 20.0
                elif vr.verification_level == "domain":
                    email_score = 17.0
                email_score *= vr.confidence
            factors.email_quality = email_score
        elif email:
            factors.email_quality = 2.0
        total_score += factors.email_quality

        # Max possible is 110, scale to 100
        final_score = min(100.0, (total_score / 110.0) * 100.0)

        return round(final_score, 1), factors

    def get_quality_flags(self, contact: dict, validation: Optional[ContactValidation] = None) -> list[str]:
        """Generate quality flag labels for a contact."""
        if validation is None:
            validation = self.validate_contact(contact)

        flags: list[str] = []

        # Positive flags
        title = contact.get("title", "").lower().strip()
        if title in self.SENIOR_TITLES:
            flags.append("senior_leader")
        elif title in self.MANAGEMENT_TITLES:
            flags.append("management")

        if validation.email.is_valid:
            email = contact.get("email", "")
            if "@" in email:
                domain = email.rsplit("@", 1)[1].lower()
                if domain not in self.PERSONAL_EMAIL_DOMAINS:
                    flags.append("company_email")
                else:
                    flags.append("personal_email")

        if validation.linkedin.is_valid:
            flags.append("has_linkedin")

        if validation.phone.is_valid:
            flags.append("has_phone")

        # Negative flags
        if not validation.email.is_valid and not validation.phone.is_valid:
            flags.append("no_contact_method")

        if not validation.name.is_valid:
            flags.append("invalid_name")

        if not validation.title.is_valid:
            flags.append("missing_title")

        if validation.overall_issues:
            flags.append("has_issues")

        return flags

    def _normalize_phone_number(self, phone: str) -> str:
        """Normalize a phone number to a consistent format."""
        # Remove common formatting
        cleaned = re.sub(r"[\s\-\(\)\.\+]", "", phone)

        # Remove non-digit characters
        digits = re.sub(r"\D", "", cleaned)

        if not digits:
            return ""

        # Handle US numbers
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]

        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"

        # Return cleaned digits for non-US numbers
        return digits


class ValidationSummary:
    """Generate summaries of contact validation results."""

    @staticmethod
    def generate_summary(
        contacts: list[dict],
        validations: list[ContactValidation],
        scores: Optional[list[tuple[float, ConfidenceFactors]]] = None,
    ) -> dict:
        """Generate a summary of validation results for a batch of contacts."""
        if not contacts or not validations:
            return {
                "total_contacts": 0,
                "valid_contacts": 0,
                "invalid_contacts": 0,
                "validation_rate": 0.0,
                "avg_confidence_score": 0.0,
                "field_validity": {},
                "common_issues": {},
                "quality_distribution": {},
            }

        total = len(contacts)

        # Count valid contacts (has at least one valid contact method)
        valid_count = sum(1 for v in validations if v.email.is_valid or v.phone.is_valid)
        invalid_count = total - valid_count

        # Field validity rates
        field_validity = {
            "email": sum(1 for v in validations if v.email.is_valid) / total * 100,
            "phone": sum(1 for v in validations if v.phone.is_valid) / total * 100,
            "name": sum(1 for v in validations if v.name.is_valid) / total * 100,
            "linkedin": sum(1 for v in validations if v.linkedin.is_valid) / total * 100,
            "title": sum(1 for v in validations if v.title.is_valid) / total * 100,
        }
        field_validity = {k: round(v, 1) for k, v in field_validity.items()}

        # Common issues
        issue_counts: dict[str, int] = {}
        for v in validations:
            all_issues = (
                v.email.issues + v.phone.issues + v.name.issues + v.linkedin.issues + v.title.issues + v.overall_issues
            )
            for issue in all_issues:
                issue_counts[issue] = issue_counts.get(issue, 0) + 1

        common_issues = dict(sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:10])

        # Confidence score distribution
        avg_confidence = 0.0
        quality_distribution = {
            "excellent": 0,  # 80-100
            "good": 0,  # 60-79
            "fair": 0,  # 40-59
            "poor": 0,  # 0-39
        }

        if scores:
            score_values = [s[0] for s in scores]
            avg_confidence = round(sum(score_values) / len(score_values), 1)

            for score_val in score_values:
                if score_val >= 80:
                    quality_distribution["excellent"] += 1
                elif score_val >= 60:
                    quality_distribution["good"] += 1
                elif score_val >= 40:
                    quality_distribution["fair"] += 1
                else:
                    quality_distribution["poor"] += 1

        return {
            "total_contacts": total,
            "valid_contacts": valid_count,
            "invalid_contacts": invalid_count,
            "validation_rate": round((valid_count / total) * 100, 1),
            "avg_confidence_score": avg_confidence,
            "field_validity": field_validity,
            "common_issues": common_issues,
            "quality_distribution": quality_distribution,
        }
