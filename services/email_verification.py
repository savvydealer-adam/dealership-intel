"""Email verification service for dealership contact validation."""

import hashlib
import logging
import re
import smtplib
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import dns.resolver

logger = logging.getLogger(__name__)


@dataclass
class VerificationResult:
    email: str
    is_valid: bool
    verification_level: str  # "format", "domain", "mailbox"
    status: str  # "valid", "invalid", "unknown", "risky"
    confidence: float
    issues: list[str] = field(default_factory=list)
    checks_performed: list[str] = field(default_factory=list)
    verification_time: float = 0.0
    error_message: Optional[str] = None


@dataclass
class VerificationConfig:
    enable_format_check: bool = True
    enable_domain_check: bool = True
    enable_mailbox_check: bool = False
    domain_timeout: float = 5.0
    mailbox_timeout: float = 10.0
    max_retries: int = 2
    cache_duration_hours: int = 24
    batch_size: int = 10
    delay_between_checks: float = 0.5


class EmailVerificationService:
    """Service for verifying email addresses through format, domain, and mailbox checks."""

    EMAIL_REGEX = re.compile(
        r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"
        r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
        r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
    )

    ROLE_ADDRESSES = {
        "info",
        "admin",
        "support",
        "sales",
        "contact",
        "help",
        "webmaster",
        "postmaster",
        "abuse",
        "noreply",
        "no-reply",
        "marketing",
        "billing",
        "accounts",
        "service",
        "office",
        "team",
        "hello",
        "general",
        "enquiries",
        "inquiries",
    }

    DISPOSABLE_DOMAINS = {
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

    def __init__(self, config: Optional[VerificationConfig] = None):
        self.config = config or VerificationConfig()
        self._cache: dict[str, tuple[VerificationResult, datetime]] = {}

    def verify_email(self, email: str) -> VerificationResult:
        """Verify a single email address through configured check levels."""
        start_time = time.time()

        if not email or not isinstance(email, str):
            return VerificationResult(
                email=email or "",
                is_valid=False,
                verification_level="format",
                status="invalid",
                confidence=1.0,
                issues=["Empty or invalid email input"],
                checks_performed=["input_validation"],
                verification_time=time.time() - start_time,
            )

        email = email.strip().lower()

        # Check cache
        cached = self._get_cached_result(email)
        if cached is not None:
            logger.debug(f"Cache hit for email: {email}")
            return cached

        issues: list[str] = []
        checks_performed: list[str] = []
        verification_level = "format"
        confidence = 0.0

        # Format check
        if self.config.enable_format_check:
            format_result = self._verify_format(email)
            checks_performed.append("format")
            issues.extend(format_result["issues"])

            if not format_result["is_valid"]:
                result = VerificationResult(
                    email=email,
                    is_valid=False,
                    verification_level="format",
                    status="invalid",
                    confidence=1.0,
                    issues=issues,
                    checks_performed=checks_performed,
                    verification_time=time.time() - start_time,
                )
                self._cache_result(email, result)
                return result

            confidence = format_result["confidence"]

        # Domain check
        if self.config.enable_domain_check:
            domain_result = self._verify_domain(email)
            checks_performed.append("domain")
            issues.extend(domain_result["issues"])
            verification_level = "domain"

            if not domain_result["is_valid"]:
                result = VerificationResult(
                    email=email,
                    is_valid=False,
                    verification_level="domain",
                    status="invalid",
                    confidence=max(confidence, domain_result["confidence"]),
                    issues=issues,
                    checks_performed=checks_performed,
                    verification_time=time.time() - start_time,
                )
                self._cache_result(email, result)
                return result

            confidence = max(confidence, domain_result["confidence"])

        # Mailbox check
        if self.config.enable_mailbox_check:
            mailbox_result = self._verify_mailbox(email)
            checks_performed.append("mailbox")
            issues.extend(mailbox_result["issues"])
            verification_level = "mailbox"

            if not mailbox_result["is_valid"]:
                status = "unknown" if mailbox_result.get("uncertain") else "invalid"
                result = VerificationResult(
                    email=email,
                    is_valid=False,
                    verification_level="mailbox",
                    status=status,
                    confidence=mailbox_result["confidence"],
                    issues=issues,
                    checks_performed=checks_performed,
                    verification_time=time.time() - start_time,
                    error_message=mailbox_result.get("error_message"),
                )
                self._cache_result(email, result)
                return result

            confidence = max(confidence, mailbox_result["confidence"])

        # Adjust confidence based on overall signals
        confidence = self._adjust_confidence(email, confidence, issues)

        status = "valid" if confidence >= 0.5 else "risky"
        is_valid = confidence >= 0.5

        result = VerificationResult(
            email=email,
            is_valid=is_valid,
            verification_level=verification_level,
            status=status,
            confidence=confidence,
            issues=issues,
            checks_performed=checks_performed,
            verification_time=time.time() - start_time,
        )
        self._cache_result(email, result)
        return result

    def verify_emails_batch(self, emails: list[str]) -> list[VerificationResult]:
        """Verify a batch of email addresses with rate limiting."""
        results: list[VerificationResult] = []

        for i, email in enumerate(emails):
            result = self.verify_email(email)
            results.append(result)

            # Rate limiting between checks
            if i < len(emails) - 1 and self.config.delay_between_checks > 0:
                time.sleep(self.config.delay_between_checks)

            # Log progress for large batches
            if (i + 1) % self.config.batch_size == 0:
                logger.info(f"Verification progress: {i + 1}/{len(emails)} emails processed")

        logger.info(
            f"Batch verification complete: {len(results)} emails processed, "
            f"{sum(1 for r in results if r.is_valid)} valid"
        )
        return results

    def _verify_format(self, email: str) -> dict:
        """Verify email format using regex and structural checks."""
        issues: list[str] = []
        is_valid = True
        confidence = 0.0

        # Basic regex check
        if not self.EMAIL_REGEX.match(email):
            issues.append("Invalid email format")
            return {"is_valid": False, "confidence": 1.0, "issues": issues}

        confidence = 0.3

        # Split into local and domain parts
        try:
            local_part, domain = email.rsplit("@", 1)
        except ValueError:
            issues.append("Missing @ symbol")
            return {"is_valid": False, "confidence": 1.0, "issues": issues}

        # Local part checks
        if len(local_part) > 64:
            issues.append("Local part exceeds 64 characters")
            is_valid = False

        if local_part.startswith(".") or local_part.endswith("."):
            issues.append("Local part starts or ends with a dot")
            is_valid = False

        if ".." in local_part:
            issues.append("Local part contains consecutive dots")
            is_valid = False

        # Domain checks
        if len(domain) > 253:
            issues.append("Domain exceeds 253 characters")
            is_valid = False

        if "." not in domain:
            issues.append("Domain has no TLD")
            is_valid = False

        # Check for role addresses
        if local_part.lower() in self.ROLE_ADDRESSES:
            issues.append("Role-based email address (not personal)")
            confidence -= 0.1

        # Check for disposable domains
        if domain.lower() in self.DISPOSABLE_DOMAINS:
            issues.append("Disposable email domain")
            is_valid = False

        if is_valid and not issues:
            confidence = 0.4

        return {"is_valid": is_valid, "confidence": confidence, "issues": issues}

    def _verify_domain(self, email: str) -> dict:
        """Verify domain has valid MX records using DNS lookup."""
        issues: list[str] = []
        domain = email.rsplit("@", 1)[1]

        try:
            # Check MX records
            mx_records = dns.resolver.resolve(domain, "MX", lifetime=self.config.domain_timeout)

            if mx_records:
                mx_hosts = [str(mx.exchange).rstrip(".") for mx in mx_records]
                logger.debug(f"MX records for {domain}: {mx_hosts}")
                return {"is_valid": True, "confidence": 0.7, "issues": issues}
            else:
                issues.append(f"No MX records found for domain: {domain}")
                return {"is_valid": False, "confidence": 0.8, "issues": issues}

        except dns.resolver.NXDOMAIN:
            issues.append(f"Domain does not exist: {domain}")
            return {"is_valid": False, "confidence": 1.0, "issues": issues}

        except dns.resolver.NoAnswer:
            # Try A record as fallback
            try:
                dns.resolver.resolve(domain, "A", lifetime=self.config.domain_timeout)
                issues.append(f"No MX records but A record exists for domain: {domain}")
                return {"is_valid": True, "confidence": 0.5, "issues": issues}
            except Exception:
                issues.append(f"No MX or A records for domain: {domain}")
                return {"is_valid": False, "confidence": 0.9, "issues": issues}

        except dns.resolver.Timeout:
            issues.append(f"DNS lookup timed out for domain: {domain}")
            return {
                "is_valid": False,
                "confidence": 0.3,
                "issues": issues,
                "uncertain": True,
            }

        except Exception as e:
            logger.warning(f"DNS verification error for {domain}: {e}")
            issues.append(f"DNS verification error: {str(e)[:100]}")
            return {
                "is_valid": False,
                "confidence": 0.3,
                "issues": issues,
                "uncertain": True,
            }

    def _verify_mailbox(self, email: str) -> dict:
        """Verify mailbox exists via SMTP conversation."""
        issues: list[str] = []
        domain = email.rsplit("@", 1)[1]

        for attempt in range(self.config.max_retries + 1):
            try:
                # Get MX host
                mx_records = dns.resolver.resolve(domain, "MX", lifetime=self.config.domain_timeout)
                mx_host = str(sorted(mx_records, key=lambda x: x.preference)[0].exchange).rstrip(".")

                # SMTP conversation
                smtp = smtplib.SMTP(timeout=self.config.mailbox_timeout)
                smtp.connect(mx_host, 25)
                smtp.helo("verify.local")
                smtp.mail("")
                code, message = smtp.rcpt(email)
                smtp.quit()

                if code == 250:
                    return {"is_valid": True, "confidence": 0.95, "issues": issues}
                elif code == 550:
                    issues.append("Mailbox does not exist")
                    return {"is_valid": False, "confidence": 0.9, "issues": issues}
                else:
                    issues.append(f"SMTP returned code {code}: {message.decode('utf-8', errors='replace')[:100]}")
                    return {
                        "is_valid": False,
                        "confidence": 0.5,
                        "issues": issues,
                        "uncertain": True,
                    }

            except smtplib.SMTPServerDisconnected:
                issues.append("SMTP server disconnected (may block verification)")
                return {
                    "is_valid": False,
                    "confidence": 0.3,
                    "issues": issues,
                    "uncertain": True,
                }

            except smtplib.SMTPConnectError as e:
                if attempt < self.config.max_retries:
                    logger.debug(
                        f"SMTP connect error for {email}, retrying "
                        f"(attempt {attempt + 1}/{self.config.max_retries + 1})"
                    )
                    time.sleep(1)
                    continue
                issues.append(f"SMTP connection failed: {str(e)[:100]}")
                return {
                    "is_valid": False,
                    "confidence": 0.3,
                    "issues": issues,
                    "uncertain": True,
                    "error_message": str(e)[:200],
                }

            except (socket.timeout, TimeoutError):
                if attempt < self.config.max_retries:
                    logger.debug(
                        f"SMTP timeout for {email}, retrying (attempt {attempt + 1}/{self.config.max_retries + 1})"
                    )
                    time.sleep(1)
                    continue
                issues.append("SMTP verification timed out")
                return {
                    "is_valid": False,
                    "confidence": 0.3,
                    "issues": issues,
                    "uncertain": True,
                    "error_message": "SMTP timeout",
                }

            except Exception as e:
                logger.warning(f"Mailbox verification error for {email}: {e}")
                issues.append(f"Mailbox verification error: {str(e)[:100]}")
                return {
                    "is_valid": False,
                    "confidence": 0.3,
                    "issues": issues,
                    "uncertain": True,
                    "error_message": str(e)[:200],
                }

        # Should not reach here, but handle gracefully
        issues.append("Max retries reached for mailbox verification")
        return {
            "is_valid": False,
            "confidence": 0.3,
            "issues": issues,
            "uncertain": True,
        }

    def _adjust_confidence(self, email: str, base_confidence: float, issues: list[str]) -> float:
        """Adjust confidence based on additional email quality signals."""
        confidence = base_confidence
        local_part = email.rsplit("@", 1)[0]
        domain = email.rsplit("@", 1)[1]

        # Professional email patterns boost confidence
        professional_patterns = [
            r"^[a-z]+\.[a-z]+$",  # firstname.lastname
            r"^[a-z]\.[a-z]+$",  # f.lastname
            r"^[a-z]+[a-z]+$",  # firstnamelastname
        ]
        for pattern in professional_patterns:
            if re.match(pattern, local_part):
                confidence += 0.05
                break

        # Company domain (not free email) boosts confidence
        free_email_domains = {
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
        }
        if domain not in free_email_domains:
            confidence += 0.05
        else:
            confidence -= 0.05

        # Numbers in local part reduce confidence slightly
        if re.search(r"\d{3,}", local_part):
            confidence -= 0.05

        # Very short local parts are suspicious
        if len(local_part) < 3:
            confidence -= 0.05

        # Penalty per issue
        confidence -= len(issues) * 0.02

        return max(0.0, min(1.0, confidence))

    def _get_cache_key(self, email: str) -> str:
        """Generate a cache key for an email address."""
        return hashlib.sha256(email.encode()).hexdigest()

    def _get_cached_result(self, email: str) -> Optional[VerificationResult]:
        """Retrieve a cached verification result if still valid."""
        cache_key = self._get_cache_key(email)
        if cache_key in self._cache:
            result, cached_at = self._cache[cache_key]
            expiry = cached_at + timedelta(hours=self.config.cache_duration_hours)
            if datetime.now(tz=timezone.utc) < expiry:
                return result
            else:
                del self._cache[cache_key]
        return None

    def _cache_result(self, email: str, result: VerificationResult) -> None:
        """Cache a verification result."""
        cache_key = self._get_cache_key(email)
        self._cache[cache_key] = (result, datetime.now(tz=timezone.utc))

    def clear_cache(self) -> int:
        """Clear the verification cache. Returns number of entries cleared."""
        count = len(self._cache)
        self._cache.clear()
        logger.info(f"Cleared {count} entries from verification cache")
        return count

    def get_cache_stats(self) -> dict:
        """Get cache statistics."""
        now = datetime.now(tz=timezone.utc)
        expired = 0
        active = 0
        for _, (_, cached_at) in self._cache.items():
            expiry = cached_at + timedelta(hours=self.config.cache_duration_hours)
            if now < expiry:
                active += 1
            else:
                expired += 1
        return {"total": len(self._cache), "active": active, "expired": expired}

    def get_verification_summary(self, results: list[VerificationResult]) -> dict:
        """Generate a summary of verification results."""
        if not results:
            return {
                "total": 0,
                "valid": 0,
                "invalid": 0,
                "unknown": 0,
                "risky": 0,
                "valid_percentage": 0.0,
                "avg_confidence": 0.0,
                "avg_verification_time": 0.0,
                "common_issues": {},
                "verification_levels": {},
            }

        total = len(results)
        valid = sum(1 for r in results if r.status == "valid")
        invalid = sum(1 for r in results if r.status == "invalid")
        unknown = sum(1 for r in results if r.status == "unknown")
        risky = sum(1 for r in results if r.status == "risky")

        avg_confidence = sum(r.confidence for r in results) / total
        avg_time = sum(r.verification_time for r in results) / total

        # Count common issues
        issue_counts: dict[str, int] = {}
        for result in results:
            for issue in result.issues:
                issue_counts[issue] = issue_counts.get(issue, 0) + 1

        # Count verification levels
        level_counts: dict[str, int] = {}
        for result in results:
            level = result.verification_level
            level_counts[level] = level_counts.get(level, 0) + 1

        return {
            "total": total,
            "valid": valid,
            "invalid": invalid,
            "unknown": unknown,
            "risky": risky,
            "valid_percentage": round((valid / total) * 100, 1),
            "avg_confidence": round(avg_confidence, 3),
            "avg_verification_time": round(avg_time, 3),
            "common_issues": dict(sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
            "verification_levels": level_counts,
        }
