"""Enrichment pipeline: process dealers from xlsx through crawl + enrich."""

import logging
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd

from config.platforms import PLATFORM_SIGNATURES
from crawlers.browser import BrowserManager
from crawlers.inventory_crawler import InventoryCrawler
from crawlers.platform_detector import PlatformDetector
from crawlers.staff_crawler import StaffCrawler
from pipeline.intel_pipeline import IntelPipeline
from services.apollo_api import ApolloAPIService
from services.database_service import DatabaseService

logger = logging.getLogger(__name__)

# Column name mappings — the xlsx may use various column names
COLUMN_ALIASES = {
    "website": ["website", "url", "dealer_url", "dealer website", "web", "site"],
    "name": ["name", "dealer_name", "dealership_name", "dealer name", "dealership"],
    "state": ["state", "st", "dealer_state"],
    "city": ["city", "dealer_city"],
    "provider": ["provider", "platform", "website_provider", "web_provider"],
}


class EnrichmentPipeline:
    """Process dealers from xlsx through the crawl + enrich pipeline."""

    def __init__(
        self,
        db_service: Optional[DatabaseService] = None,
        browser_manager: Optional[BrowserManager] = None,
        apollo_service: Optional[ApolloAPIService] = None,
    ):
        self.db = db_service
        self.browser_manager = browser_manager
        self.apollo = apollo_service
        self.platform_detector = PlatformDetector()
        self.inventory_crawler = InventoryCrawler()
        self.staff_crawler = StaffCrawler(browser_manager=browser_manager) if browser_manager else None

    def load_dealers(self, xlsx_path: str) -> pd.DataFrame:
        """Load and normalize dealer data from xlsx.

        Returns DataFrame with standardized column names.
        """
        path = Path(xlsx_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {xlsx_path}")

        if path.suffix.lower() in (".xlsx", ".xls"):
            df = pd.read_excel(xlsx_path, engine="openpyxl")
        elif path.suffix.lower() == ".csv":
            df = pd.read_csv(xlsx_path)
        else:
            raise ValueError(f"Unsupported file format: {path.suffix}")

        df = self._normalize_columns(df)
        logger.info(f"Loaded {len(df)} dealers from {xlsx_path}")
        return df

    def process_batch(
        self,
        dealers: pd.DataFrame,
        *,
        provider_filter: str | None = None,
        state_filter: str | None = None,
        max_dealers: int | None = None,
        skip_existing: bool = True,
        delay_seconds: float = 2.0,
        run_name: str = "Enrichment Run",
        on_progress: Optional[Callable[[int, int, str], None]] = None,
    ) -> list[dict[str, Any]]:
        """Process a batch of dealers through the enrichment pipeline.

        Args:
            dealers: DataFrame with dealer data.
            provider_filter: Only process dealers on this platform.
            state_filter: Only process dealers in this state.
            max_dealers: Limit number of dealers to process.
            skip_existing: Skip dealers already in the database.
            delay_seconds: Delay between requests.
            run_name: Name for this analysis run.
            on_progress: Callback(current, total, message).

        Returns:
            List of enriched result dicts.
        """
        # Apply filters
        filtered = dealers.copy()

        if provider_filter:
            if "provider" in filtered.columns:
                filtered = filtered[
                    filtered["provider"].str.lower().str.contains(provider_filter.lower(), na=False)
                ]
            else:
                logger.warning("No 'provider' column found, skipping provider filter")

        if state_filter:
            if "state" in filtered.columns:
                filtered = filtered[filtered["state"].str.upper() == state_filter.upper()]
            else:
                logger.warning("No 'state' column found, skipping state filter")

        # Sort: known providers first (higher success rate), then unknown
        filtered = self._sort_by_provider_priority(filtered)

        if max_dealers:
            filtered = filtered.head(max_dealers)

        # Extract website list
        if "website" not in filtered.columns:
            raise ValueError("No 'website' column found in the data")

        websites = filtered["website"].dropna().tolist()
        websites = [self._normalize_url(w) for w in websites if w and str(w).strip()]

        if not websites:
            logger.warning("No valid websites to process")
            return []

        logger.info(f"Processing {len(websites)} dealers (filtered from {len(dealers)} total)")

        # Build the pipeline
        use_crawling = self.browser_manager is not None
        pipeline = IntelPipeline(
            apollo_service=self.apollo,
            db_service=self.db,
            browser_manager=self.browser_manager,
            staff_crawler=self.staff_crawler,
            inventory_crawler=self.inventory_crawler,
            platform_detector=self.platform_detector,
            use_crawling=use_crawling,
        )

        results = pipeline.process_dealerships(
            websites=websites,
            delay_seconds=delay_seconds,
            skip_existing=skip_existing,
            run_name=run_name,
            on_progress=on_progress,
        )

        return results

    def export_results(self, results: list[dict[str, Any]], output_path: str) -> str:
        """Export enriched results to xlsx or csv.

        Returns the output file path.
        """
        if not results:
            raise ValueError("No results to export")

        df = pd.DataFrame(results)

        # Drop internal fields not useful in export
        drop_cols = [c for c in df.columns if c == "contacts"]
        df = df.drop(columns=drop_cols, errors="ignore")

        path = Path(output_path)
        if path.suffix.lower() in (".xlsx", ".xls"):
            df.to_excel(output_path, index=False, engine="openpyxl")
        else:
            df.to_csv(output_path, index=False)

        logger.info(f"Exported {len(df)} results to {output_path}")
        return output_path

    def get_provider_summary(self, dealers: pd.DataFrame) -> dict[str, int]:
        """Get count of dealers by provider."""
        if "provider" not in dealers.columns:
            return {"Unknown": len(dealers)}

        counts = dealers["provider"].fillna("Unknown").value_counts().to_dict()
        return dict(counts)

    def get_state_summary(self, dealers: pd.DataFrame) -> dict[str, int]:
        """Get count of dealers by state."""
        if "state" not in dealers.columns:
            return {"Unknown": len(dealers)}

        counts = dealers["state"].fillna("Unknown").value_counts().to_dict()
        return dict(counts)

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names to standard names."""
        col_map: dict[str, str] = {}
        lower_cols = {c.lower().strip(): c for c in df.columns}

        for standard_name, aliases in COLUMN_ALIASES.items():
            for alias in aliases:
                if alias in lower_cols:
                    col_map[lower_cols[alias]] = standard_name
                    break

        if col_map:
            df = df.rename(columns=col_map)

        return df

    def _sort_by_provider_priority(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sort dealers so known providers come first."""
        if "provider" not in df.columns:
            return df

        known_providers = set(PLATFORM_SIGNATURES.keys())

        def priority(provider):
            if pd.isna(provider):
                return 2
            if provider in known_providers:
                return 0
            return 1

        df = df.copy()
        df["_priority"] = df["provider"].apply(priority)
        df = df.sort_values("_priority").drop(columns=["_priority"])
        return df

    def _normalize_url(self, url: str) -> str:
        """Ensure URL has a scheme."""
        url = str(url).strip()
        if not url:
            return ""
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        return url
