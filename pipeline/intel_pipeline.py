"""Main orchestrator: processes dealerships through crawl -> validate -> store pipeline."""

import asyncio
import logging
import time
from typing import Any, Callable, Optional

from services.apollo_api import ApolloAPIService
from services.database_service import DatabaseService
from services.domain_utils import extract_company_name, extract_domain
from services.role_classifier import RoleClassifier, RoleFilterCriteria
from services.validation import ContactValidator

logger = logging.getLogger(__name__)


class IntelPipeline:
    """Orchestrates dealership intelligence gathering.

    Supports two modes:
    - Apollo-only (original): company search + people search via Apollo
    - Crawl-first (new): detect platform -> crawl staff -> crawl inventory -> Apollo fallback
    """

    def __init__(
        self,
        apollo_service: Optional[ApolloAPIService] = None,
        db_service: Optional[DatabaseService] = None,
        validator: Optional[ContactValidator] = None,
        role_classifier: Optional[RoleClassifier] = None,
        # Crawl-first dependencies (optional — pipeline works without them)
        browser_manager=None,
        staff_crawler=None,
        inventory_crawler=None,
        platform_detector=None,
        use_crawling: bool = False,
    ):
        self.apollo = apollo_service
        self.db = db_service
        self.validator = validator or ContactValidator(enable_email_verification=False)
        self.role_classifier = role_classifier or RoleClassifier()
        self.browser_manager = browser_manager
        self.staff_crawler = staff_crawler
        self.inventory_crawler = inventory_crawler
        self.platform_detector = platform_detector
        self.use_crawling = use_crawling

    def process_dealerships(
        self,
        websites: list[str],
        *,
        batch_size: int = 10,
        delay_seconds: float = 1.0,
        skip_existing: bool = True,
        role_filter_criteria: Optional[RoleFilterCriteria] = None,
        run_name: str = "DealershipIntel Run",
        sheet_url: str = "",
        website_column: str = "Website",
        on_progress: Optional[Callable[[int, int, str], None]] = None,
    ) -> list[dict[str, Any]]:
        """Process a list of dealership websites through the intelligence pipeline.

        Args:
            websites: List of website URLs to process.
            batch_size: Number to process per batch.
            delay_seconds: Delay between requests.
            skip_existing: Skip already-analyzed dealerships.
            role_filter_criteria: Optional role filtering.
            run_name: Name for this analysis run.
            sheet_url: Source Google Sheet URL.
            website_column: Column name for websites.
            on_progress: Callback(current, total, message) for progress updates.

        Returns:
            List of result dictionaries.
        """
        results: list[dict[str, Any]] = []
        total = len(websites)

        # Create analysis run in DB
        analysis_run_id = None
        if self.db:
            try:
                analysis_run_id = self.db.create_analysis_run(
                    run_name=run_name,
                    google_sheet_url=sheet_url,
                    website_column=website_column,
                    batch_size=batch_size,
                    delay_seconds=delay_seconds,
                )
            except Exception as e:
                logger.error(f"Failed to create analysis run: {e}")

        stats = {"processed": 0, "successful": 0, "failed": 0, "contacts_found": 0}

        for idx, website_url in enumerate(websites):
            stats["processed"] += 1
            if on_progress:
                on_progress(stats["processed"], total, f"Processing: {website_url}")

            try:
                result = self._process_single_dealership(
                    website_url,
                    analysis_run_id=analysis_run_id,
                    skip_existing=skip_existing,
                    role_filter_criteria=role_filter_criteria,
                )
                results.append(result)

                if result.get("status") == "Success":
                    stats["successful"] += 1
                    stats["contacts_found"] += len(result.get("contacts", []))
                else:
                    stats["failed"] += 1

            except Exception as e:
                logger.error(f"Error processing {website_url}: {e}")
                domain = extract_domain(website_url) if website_url else ""
                error_result = {
                    "original_website": website_url,
                    "domain": domain or "",
                    "company_name": "Error",
                    "status": "Error",
                    "error_message": str(e),
                }
                results.append(error_result)
                stats["failed"] += 1

                if self.db and analysis_run_id and domain:
                    try:
                        self.db.save_company(error_result, analysis_run_id)
                    except Exception:
                        pass

            # Rate limiting (skip after last item)
            if idx < total - 1:
                time.sleep(delay_seconds)

        # Finalize run
        if self.db and analysis_run_id:
            try:
                self.db.update_analysis_run_stats(
                    analysis_run_id,
                    companies_processed=stats["processed"],
                    companies_successful=stats["successful"],
                    companies_failed=stats["failed"],
                    contacts_found=stats["contacts_found"],
                    status="completed",
                )
            except Exception as e:
                logger.error(f"Failed to update analysis run stats: {e}")

        return results

    def _process_single_dealership(
        self,
        website_url: str,
        *,
        analysis_run_id: Optional[int] = None,
        skip_existing: bool = True,
        role_filter_criteria: Optional[RoleFilterCriteria] = None,
    ) -> dict[str, Any]:
        """Process a single dealership website."""
        domain = extract_domain(website_url)
        if not domain:
            return {
                "original_website": website_url,
                "domain": "",
                "company_name": "Invalid URL",
                "status": "Error",
                "error_message": "Could not extract domain from URL",
            }

        # Skip existing
        if skip_existing and self.db:
            existing = self.db.get_company_by_domain(domain)
            if existing:
                logger.info(f"Skipping already-analyzed domain: {domain}")
                result = dict(existing)
                result["original_website"] = website_url
                return result

        # Extract company name
        company_name = extract_company_name(website_url, domain)

        # If crawling is enabled, use crawl-first flow
        if self.use_crawling and self.browser_manager:
            return self._process_with_crawling(
                website_url,
                domain,
                company_name,
                analysis_run_id=analysis_run_id,
                role_filter_criteria=role_filter_criteria,
            )

        # Otherwise, use Apollo-only flow (original behavior)
        return self._process_with_apollo(
            website_url,
            domain,
            company_name,
            analysis_run_id=analysis_run_id,
            role_filter_criteria=role_filter_criteria,
        )

    def _process_with_crawling(
        self,
        website_url: str,
        domain: str,
        company_name: str,
        *,
        analysis_run_id: Optional[int] = None,
        role_filter_criteria: Optional[RoleFilterCriteria] = None,
    ) -> dict[str, Any]:
        """Crawl-first flow: detect platform -> crawl staff -> crawl inventory -> Apollo fallback."""
        base_url = f"https://{domain}"
        detected_platform = None
        crawled_contacts: list[dict[str, Any]] = []
        inventory_data: dict[str, Any] = {}

        try:
            crawl_results = self._run_crawl_async(
                base_url, domain, role_filter_criteria
            )
            detected_platform = crawl_results.get("platform")
            crawled_contacts = crawl_results.get("contacts", [])
            inventory_data = crawl_results.get("inventory", {})
        except Exception as e:
            logger.warning(f"Crawl failed for {domain}: {e}")

        # Build result from crawled + Apollo data
        result: dict[str, Any] = {
            "original_website": website_url,
            "domain": domain,
            "company_name": company_name or domain,
            "platform": detected_platform or "Unknown",
            "status": "Success" if crawled_contacts else "Partial",
        }

        # Add inventory data
        if inventory_data:
            result["new_inventory_count"] = inventory_data.get("new_count")
            result["used_inventory_count"] = inventory_data.get("used_count")
            result["new_inventory_url"] = inventory_data.get("new_url")
            result["used_inventory_url"] = inventory_data.get("used_url")

        # Apollo fallback for company data + additional contacts
        apollo_contacts = []
        company_data = None
        if self.apollo:
            company_data = self.apollo.search_company_multi_strategy(domain, company_name)
            if company_data:
                result["company_name"] = company_data.get("name", company_name)
                result["company_id"] = company_data.get("id", "")
                result["industry"] = company_data.get("industry", "")
                result["company_size"] = company_data.get("estimated_num_employees", "")
                result["company_phone"] = company_data.get("phone", "")
                result["company_address"] = company_data.get("address", "")
                result["linkedin_url"] = company_data.get("linkedin_url", "")

            # Only fetch Apollo people if crawl didn't find enough
            if len(crawled_contacts) < 2:
                apollo_contacts = self.apollo.search_people(
                    company_data.get("id", "") if company_data else "",
                    domain,
                    limit=10,
                    role_filter_criteria=role_filter_criteria,
                )
                for c in apollo_contacts:
                    c["source"] = "apollo"

        # Merge contacts: crawled first, Apollo fill-in
        all_contacts = self._merge_contacts(crawled_contacts, apollo_contacts)

        if not all_contacts and not company_data:
            result["status"] = "No Data Found"
            result["error_message"] = "No contacts found via crawl or Apollo"
            if self.db and analysis_run_id:
                try:
                    self.db.save_company(result, analysis_run_id)
                except Exception:
                    pass
            return result

        result["status"] = "Success"

        # Apply role filtering
        if all_contacts and role_filter_criteria:
            for person in all_contacts:
                person["company_name"] = result.get("company_name", "")
            all_contacts = self.role_classifier.filter_contacts_by_role(all_contacts, role_filter_criteria)

        # Validate and score
        contacts = self._validate_contacts(all_contacts[:5], domain, result.get("company_name", ""))
        contacts.sort(key=lambda c: c.get("confidence_score", 0), reverse=True)
        result["contacts"] = contacts

        # Flatten contacts into result for DataFrame compatibility
        for i, contact in enumerate(contacts):
            prefix = f"contact_{i + 1}"
            result[f"{prefix}_name"] = contact.get("name", "")
            result[f"{prefix}_title"] = contact.get("title", "")
            result[f"{prefix}_email"] = contact.get("email", "")
            result[f"{prefix}_phone"] = contact.get("phone", "")
            result[f"{prefix}_linkedin"] = contact.get("linkedin_url", "")
            result[f"{prefix}_confidence_score"] = contact.get("confidence_score", 0)
            result[f"{prefix}_quality_flags"] = contact.get("quality_flags", "")

        # Save to DB
        company_id = None
        if self.db and analysis_run_id:
            try:
                company_id = self.db.save_company(result, analysis_run_id)
            except Exception as e:
                logger.warning(f"Database save failed for {domain}: {e}")

        if self.db and company_id and contacts:
            try:
                self.db.save_contacts(company_id, contacts)
            except Exception as e:
                logger.warning(f"Failed to save contacts for {domain}: {e}")

        return result

    def _run_crawl_async(
        self,
        base_url: str,
        domain: str,
        role_filter_criteria=None,
    ) -> dict[str, Any]:
        """Run the async crawl operations, handling event loop correctly."""

        async def _crawl():
            result: dict[str, Any] = {"platform": None, "contacts": [], "inventory": {}}

            async with await self.browser_manager.get_page() as page:
                # Step 1: Navigate to homepage and detect platform
                try:
                    response = await page.goto(
                        base_url,
                        {"timeout": 30000, "waitUntil": "domcontentloaded"},
                    )
                    if response and response.status < 400 and self.platform_detector:
                        platform_result = await self.platform_detector.detect(page)
                        result["platform"] = platform_result.get("platform")
                        logger.info(
                            f"Detected platform for {domain}: {result['platform']} "
                            f"(confidence={platform_result.get('confidence', 0):.2f})"
                        )
                except Exception as e:
                    logger.warning(f"Homepage load failed for {base_url}: {e}")

                # Step 2: Crawl inventory (reuse the page)
                if self.inventory_crawler:
                    try:
                        result["inventory"] = await self.inventory_crawler.crawl_inventory(
                            page, base_url, platform=result["platform"]
                        )
                    except Exception as e:
                        logger.warning(f"Inventory crawl failed for {domain}: {e}")

            # Step 3: Crawl staff page (uses its own page from browser_manager)
            if self.staff_crawler:
                try:
                    result["contacts"] = await self.staff_crawler.crawl_staff_page(
                        base_url, platform=result["platform"]
                    )
                    for c in result["contacts"]:
                        c["source"] = "crawl"
                except Exception as e:
                    logger.warning(f"Staff crawl failed for {domain}: {e}")

            return result

        # Handle running from sync context (Streamlit)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, _crawl())
                return future.result()
        else:
            return asyncio.run(_crawl())

    def _process_with_apollo(
        self,
        website_url: str,
        domain: str,
        company_name: str,
        *,
        analysis_run_id: Optional[int] = None,
        role_filter_criteria: Optional[RoleFilterCriteria] = None,
    ) -> dict[str, Any]:
        """Original Apollo-only flow."""
        # Try Apollo for company data
        company_data = None
        if self.apollo:
            company_data = self.apollo.search_company_multi_strategy(domain, company_name)

        if not company_data:
            no_data = {
                "original_website": website_url,
                "domain": domain,
                "company_name": company_name or domain,
                "status": "No Data Found",
                "error_message": "Company not found in Apollo database",
            }
            if self.db and analysis_run_id:
                try:
                    self.db.save_company(no_data, analysis_run_id)
                except Exception as e:
                    logger.warning(f"Failed to save no-data result for {domain}: {e}")
            return no_data

        # Build result entry
        result: dict[str, Any] = {
            "original_website": website_url,
            "domain": domain,
            "company_name": company_data.get("name", company_name),
            "company_id": company_data.get("id", ""),
            "industry": company_data.get("industry", ""),
            "company_size": company_data.get("estimated_num_employees", ""),
            "company_phone": company_data.get("phone", ""),
            "company_address": company_data.get("address", ""),
            "linkedin_url": company_data.get("linkedin_url", ""),
            "status": "Success",
        }

        # Save company to DB
        company_id = None
        if self.db and analysis_run_id:
            try:
                company_id = self.db.save_company(result, analysis_run_id)
            except Exception as e:
                logger.warning(f"Database save failed for {domain}: {e}")

        # Search for people
        management_data = []
        if self.apollo:
            management_data = self.apollo.search_people(
                company_data.get("id", ""),
                domain,
                limit=10,
                role_filter_criteria=role_filter_criteria,
            )

        # Apply role filtering
        if management_data and role_filter_criteria:
            for person in management_data:
                person["company_name"] = company_data.get("name", "")
            management_data = self.role_classifier.filter_contacts_by_role(management_data, role_filter_criteria)

        # Validate and score contacts
        contacts = self._validate_contacts(management_data[:5], domain, company_data.get("name", ""))

        # Sort by confidence
        contacts.sort(key=lambda c: c.get("confidence_score", 0), reverse=True)
        result["contacts"] = contacts

        # Flatten contacts into result for DataFrame compatibility
        for i, contact in enumerate(contacts):
            prefix = f"contact_{i + 1}"
            result[f"{prefix}_name"] = contact.get("name", "")
            result[f"{prefix}_title"] = contact.get("title", "")
            result[f"{prefix}_email"] = contact.get("email", "")
            result[f"{prefix}_phone"] = contact.get("phone", "")
            result[f"{prefix}_linkedin"] = contact.get("linkedin_url", "")
            result[f"{prefix}_confidence_score"] = contact.get("confidence_score", 0)
            result[f"{prefix}_quality_flags"] = contact.get("quality_flags", "")

        # Save contacts to DB
        if self.db and company_id and contacts:
            try:
                self.db.save_contacts(company_id, contacts)
            except Exception as e:
                logger.warning(f"Failed to save contacts for {domain}: {e}")

        return result

    def _merge_contacts(
        self,
        crawled: list[dict[str, Any]],
        apollo: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Merge crawled and Apollo contacts, deduplicating by email."""
        seen_emails: set[str] = set()
        merged: list[dict[str, Any]] = []

        for contact in crawled:
            email = (contact.get("email") or "").lower().strip()
            if email and email not in seen_emails:
                seen_emails.add(email)
                merged.append(contact)
            elif not email:
                merged.append(contact)

        for contact in apollo:
            email = (contact.get("email") or "").lower().strip()
            if email and email not in seen_emails:
                seen_emails.add(email)
                merged.append(contact)
            elif not email:
                name = (contact.get("name") or "").lower().strip()
                if name and not any((c.get("name") or "").lower().strip() == name for c in merged):
                    merged.append(contact)

        return merged

    def _validate_contacts(
        self,
        people: list[dict[str, Any]],
        domain: str,
        company_name: str,
    ) -> list[dict[str, Any]]:
        """Validate and score a list of contacts."""
        validated = []

        for person in people:
            role_classification = person.get("role_classification")
            if not role_classification:
                role_classification = self.role_classifier.classify_role(person.get("title", ""), company_name)

            contact_validation = self.validator.validate_contact(person, domain)
            confidence_score, confidence_factors = self.validator.calculate_confidence_score(
                person, contact_validation, domain, role_classification
            )
            quality_flags = self.validator.get_quality_flags(contact_validation)

            validated.append(
                {
                    "name": person.get("name", ""),
                    "title": person.get("title", ""),
                    "email": person.get("email", ""),
                    "phone": person.get("phone", ""),
                    "linkedin_url": person.get("linkedin_url", ""),
                    "confidence_score": confidence_score,
                    "quality_flags": "; ".join(quality_flags) if quality_flags else "",
                    "data_completeness": confidence_factors.data_completeness,
                    "domain_consistency": confidence_factors.domain_consistency,
                    "professional_title": confidence_factors.professional_title,
                    "linkedin_presence": confidence_factors.linkedin_presence,
                    "data_consistency": confidence_factors.data_consistency,
                    "email_quality": confidence_factors.email_quality,
                    "source": person.get("source", "apollo"),
                }
            )

        return validated
