"""Data source fallback chain: crawl first -> Apollo fallback -> merge & validate."""

import asyncio
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class FallbackChain:
    """Manages the priority chain for contact discovery.

    Priority:
    1. Crawl dealership website (staff pages) - provider-aware
    2. If < 2 contacts found: query Apollo API
    3. Merge results, deduplicate by email
    4. Validate all contacts
    5. Score confidence (boosted for crawled data)
    """

    def __init__(
        self,
        apollo_service=None,
        staff_crawler=None,
        browser_manager=None,
        min_crawled_contacts: int = 2,
    ):
        self.apollo = apollo_service
        self.staff_crawler = staff_crawler
        self.browser_manager = browser_manager
        self.min_crawled_contacts = min_crawled_contacts

    async def find_contacts_async(
        self,
        domain: str,
        company_name: Optional[str] = None,
        apollo_company_id: Optional[str] = None,
        role_filter_criteria=None,
        platform: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Find contacts using the fallback chain (async version)."""
        crawled_contacts: list[dict[str, Any]] = []
        apollo_contacts: list[dict[str, Any]] = []

        # Step 1: Try crawling the website with provider-aware extraction
        if self.staff_crawler:
            try:
                crawled_contacts = await self.staff_crawler.crawl_staff_page(
                    f"https://{domain}", platform=platform
                )
                logger.info(f"Crawled {len(crawled_contacts)} contacts from {domain}")
                for contact in crawled_contacts:
                    contact["source"] = "crawl"
            except Exception as e:
                logger.warning(f"Staff crawl failed for {domain}: {e}")

        # Step 2: Apollo fallback if not enough crawled contacts
        if len(crawled_contacts) < self.min_crawled_contacts and self.apollo:
            try:
                apollo_contacts = self.apollo.search_people(
                    apollo_company_id,
                    domain,
                    limit=10,
                    role_filter_criteria=role_filter_criteria,
                )
                logger.info(f"Apollo found {len(apollo_contacts)} contacts for {domain}")
                for contact in apollo_contacts:
                    contact["source"] = "apollo"
            except Exception as e:
                logger.warning(f"Apollo search failed for {domain}: {e}")

        # Step 3: Merge and deduplicate
        merged = self._merge_contacts(crawled_contacts, apollo_contacts)
        logger.info(f"Merged to {len(merged)} unique contacts for {domain}")

        return merged

    def find_contacts(
        self,
        domain: str,
        company_name: Optional[str] = None,
        apollo_company_id: Optional[str] = None,
        role_filter_criteria=None,
        platform: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Find contacts using the fallback chain (sync wrapper).

        Handles the case where we're already inside a running event loop
        (e.g. from Streamlit).
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        coro = self.find_contacts_async(
            domain,
            company_name=company_name,
            apollo_company_id=apollo_company_id,
            role_filter_criteria=role_filter_criteria,
            platform=platform,
        )

        if loop and loop.is_running():
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result()
        else:
            return asyncio.run(coro)

    def _merge_contacts(
        self,
        crawled: list[dict[str, Any]],
        apollo: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Merge crawled and Apollo contacts, deduplicating by email.

        Crawled contacts take priority when there's overlap.
        """
        seen_emails: set[str] = set()
        merged: list[dict[str, Any]] = []

        # Add crawled contacts first (higher priority)
        for contact in crawled:
            email = (contact.get("email") or "").lower().strip()
            if email and email not in seen_emails:
                seen_emails.add(email)
                merged.append(contact)
            elif not email:
                merged.append(contact)

        # Add Apollo contacts that don't duplicate
        for contact in apollo:
            email = (contact.get("email") or "").lower().strip()
            if email and email not in seen_emails:
                seen_emails.add(email)
                merged.append(contact)
            elif not email:
                # Check name-based dedup
                name = (contact.get("name") or "").lower().strip()
                if name and not any((c.get("name") or "").lower().strip() == name for c in merged):
                    merged.append(contact)

        return merged
