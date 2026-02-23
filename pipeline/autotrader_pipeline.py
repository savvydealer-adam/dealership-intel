"""Async batch pipeline for importing dealers from Autotrader sitemap."""

import asyncio
import logging
import time
from typing import Any, Callable, Optional

import httpx

from crawlers.autotrader_scraper import (
    AutotraderDealer,
    extract_dealer_data,
    fetch_dealer_page,
    fetch_sitemap_urls,
    parse_autotrader_url,
)
from crawlers.stealth import human_delay
from services.database_service import DatabaseService

logger = logging.getLogger(__name__)


class AutotraderPipeline:
    """Orchestrates bulk import of dealers from Autotrader's sitemap."""

    def __init__(
        self,
        db_service: Optional[DatabaseService] = None,
        concurrency: int = 10,
        delay_min: float = 0.5,
        delay_max: float = 1.5,
        timeout: float = 30.0,
    ) -> None:
        self.db_service = db_service
        self.concurrency = concurrency
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.timeout = timeout

    async def run(
        self,
        sitemap_url: str,
        local_sitemap_path: Optional[str] = None,
        skip_existing: bool = True,
        on_progress: Optional[Callable[[int, int, str], None]] = None,
        max_dealers: Optional[int] = None,
        state_filter: Optional[str] = None,
    ) -> dict[str, Any]:
        """Run the import pipeline.

        Args:
            sitemap_url: URL of the Autotrader dealer sitemap.
            local_sitemap_path: Optional local path to decoded sitemap XML.
            skip_existing: Whether to skip already-imported dealers.
            on_progress: Callback(current, total, message) for progress updates.
            max_dealers: Maximum number of dealers to process (None = all).
            state_filter: 2-letter state code to filter URLs (e.g. "ME", "TX").

        Returns:
            Stats dict with processed, saved, skipped, failed, elapsed keys.
        """
        stats: dict[str, Any] = {
            "processed": 0,
            "saved": 0,
            "skipped": 0,
            "failed": 0,
            "elapsed": 0.0,
            "total_sitemap": 0,
            "total_after_filter": 0,
        }
        start_time = time.monotonic()

        async with httpx.AsyncClient() as client:
            # 1. Fetch sitemap URLs
            if on_progress:
                on_progress(0, 1, "Fetching sitemap...")
            all_urls = await fetch_sitemap_urls(client, sitemap_url, local_sitemap_path)
            stats["total_sitemap"] = len(all_urls)

            if not all_urls:
                logger.warning("No dealer URLs found in sitemap")
                stats["elapsed"] = time.monotonic() - start_time
                return stats

            # 2. Filter by state
            if state_filter:
                all_urls = self._filter_urls_by_state(all_urls, state_filter)
                logger.info(f"Filtered to {len(all_urls)} URLs for state {state_filter}")

            # 3. Skip existing
            if skip_existing and self.db_service:
                existing_ids = self.db_service.get_scraped_autotrader_ids()
                before = len(all_urls)
                all_urls = [u for u in all_urls if self._extract_dealer_id(u) not in existing_ids]
                stats["skipped"] = before - len(all_urls)
                logger.info(f"Skipped {stats['skipped']} already-imported dealers")

            # 4. Limit
            if max_dealers and len(all_urls) > max_dealers:
                all_urls = all_urls[:max_dealers]

            stats["total_after_filter"] = len(all_urls)

            if not all_urls:
                logger.info("No new dealers to process")
                stats["elapsed"] = time.monotonic() - start_time
                return stats

            # 5. Process in batches with semaphore
            semaphore = asyncio.Semaphore(self.concurrency)
            total = len(all_urls)

            async def process_with_progress(idx: int, url: str) -> Optional[AutotraderDealer]:
                result = await self._process_dealer_url(client, url, semaphore)
                if on_progress:
                    on_progress(idx + 1, total, f"Processing {url.split('/')[-1]}")
                return result

            tasks = [process_with_progress(i, url) for i, url in enumerate(all_urls)]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 6. Save results
            for result in results:
                stats["processed"] += 1
                if isinstance(result, Exception):
                    stats["failed"] += 1
                    logger.error(f"Task exception: {result}")
                    continue
                if result is None:
                    stats["failed"] += 1
                    continue

                dealer: AutotraderDealer = result
                if self.db_service:
                    company_data = self._dealer_to_company_dict(dealer)
                    intel_data = self._dealer_to_intel_dict(dealer)
                    company_id = self.db_service.save_autotrader_dealer(
                        company_data, intel_data, dealer.autotrader_dealer_id
                    )
                    if company_id:
                        stats["saved"] += 1
                    else:
                        stats["failed"] += 1
                else:
                    stats["saved"] += 1  # Count as saved even without DB (dry run)

        stats["elapsed"] = time.monotonic() - start_time
        logger.info(
            f"Pipeline complete: {stats['saved']} saved, {stats['failed']} failed, "
            f"{stats['skipped']} skipped in {stats['elapsed']:.1f}s"
        )
        return stats

    def run_sync(self, **kwargs) -> dict[str, Any]:
        """Synchronous wrapper for Streamlit compatibility."""
        return asyncio.run(self.run(**kwargs))

    async def _process_dealer_url(
        self,
        client: httpx.AsyncClient,
        url: str,
        semaphore: asyncio.Semaphore,
    ) -> Optional[AutotraderDealer]:
        """Fetch and extract data for a single dealer URL."""
        async with semaphore:
            await human_delay(self.delay_min, self.delay_max)
            html = await fetch_dealer_page(client, url, timeout=self.timeout)
            if not html:
                return None
            return extract_dealer_data(html, url)

    @staticmethod
    def _filter_urls_by_state(urls: list[str], state_code: str) -> list[str]:
        """Filter URLs by 2-letter state code in the city-state slug.

        The city-state portion of the URL ends with the state abbreviation,
        e.g. "portland-me" for Maine.
        """
        suffix = f"-{state_code.lower()}"
        filtered = []
        for url in urls:
            try:
                _, city_state, _ = parse_autotrader_url(url)
                if city_state.endswith(suffix):
                    filtered.append(url)
            except ValueError:
                continue
        return filtered

    @staticmethod
    def _extract_dealer_id(url: str) -> Optional[str]:
        """Extract dealer ID from URL for skip-existing lookups."""
        try:
            dealer_id, _, _ = parse_autotrader_url(url)
            return dealer_id
        except ValueError:
            return None

    @staticmethod
    def _dealer_to_company_dict(dealer: AutotraderDealer) -> dict[str, Any]:
        """Convert AutotraderDealer to company table format."""
        return {
            "domain": dealer.domain,
            "original_website": dealer.website_url or dealer.autotrader_url,
            "company_name": dealer.name,
            "company_phone": dealer.phone,
            "company_address": dealer.full_address,
            "industry": "Automotive",
            "status": "success",
        }

    @staticmethod
    def _dealer_to_intel_dict(dealer: AutotraderDealer) -> dict[str, Any]:
        """Convert AutotraderDealer to dealership_intel table format."""
        review_scores = None
        if dealer.rating_value is not None:
            review_scores = [
                {
                    "source": "autotrader",
                    "rating": dealer.rating_value,
                    "review_count": dealer.review_count or 0,
                    "url": dealer.autotrader_url,
                }
            ]

        return {
            "new_inventory_count": None,
            "used_inventory_count": None,
            "review_scores": review_scores,
        }
