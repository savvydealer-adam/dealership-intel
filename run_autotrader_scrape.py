"""Standalone Autotrader dealer scraper using nodriver (undetected browser).

Reads the local sitemap, scrapes all dealer pages sequentially with a real
Chrome browser to bypass Akamai bot protection. Writes results to CSV
incrementally and publishes progress to a JSON file for the Streamlit
dashboard to consume.

Usage:
    python run_autotrader_scrape.py                  # full run (all 21K dealers)
    python run_autotrader_scrape.py --max-dealers 10 # test batch
    python run_autotrader_scrape.py --state TX       # Texas only
"""

import argparse
import asyncio
import csv
import json
import logging
import random
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Ensure project root is on sys.path so crawlers/ imports work
sys.path.insert(0, str(Path(__file__).resolve().parent))

from crawlers.autotrader_scraper import (
    AutotraderDealer,
    extract_dealer_data,
    parse_autotrader_url,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SITEMAP_PATH = Path("C:/Users/adam/Downloads/sitemap_dlr_decoded.xml")
CSV_PATH = Path("C:/Users/adam/Downloads/autotrader_dealers.csv")
PROGRESS_PATH = Path("C:/Users/adam/Downloads/scrape_progress.json")
LOG_PATH = Path("C:/Users/adam/Downloads/autotrader_scrape.log")

CSV_COLUMNS = [
    "autotrader_dealer_id",
    "name",
    "phone",
    "street_address",
    "city",
    "state",
    "postal_code",
    "full_address",
    "rating",
    "review_count",
    "website_url",
    "domain",
    "autotrader_url",
    "inventory_count",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("autotrader_scrape")

# Suppress noisy nodriver/websocket logs
logging.getLogger("nodriver").setLevel(logging.WARNING)
logging.getLogger("uc").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Sitemap parser (local-only, no httpx needed)
# ---------------------------------------------------------------------------
def parse_sitemap(path: Path) -> list[str]:
    """Parse dealer URLs from the local sitemap XML."""
    xml_content = path.read_bytes()
    root = ET.fromstring(xml_content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for loc in root.findall(".//sm:loc", ns):
        if loc.text:
            url = loc.text.strip()
            if "/car-dealers/" in url:
                urls.append(url)
    if not urls:
        for loc in root.iter("loc"):
            if loc.text:
                url = loc.text.strip()
                if "/car-dealers/" in url:
                    urls.append(url)
    return urls


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------
def _ensure_csv_header() -> None:
    """Create the CSV with a header row if it doesn't exist yet."""
    if not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0:
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()


def _load_existing_ids() -> set[str]:
    """Read the CSV and return a set of already-scraped dealer IDs."""
    ids: set[str] = set()
    if not CSV_PATH.exists():
        return ids
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            did = row.get("autotrader_dealer_id", "").strip()
            if did:
                ids.add(did)
    return ids


def _dealer_to_row(dealer: AutotraderDealer) -> dict[str, str]:
    return {
        "autotrader_dealer_id": dealer.autotrader_dealer_id,
        "name": dealer.name,
        "phone": dealer.phone,
        "street_address": dealer.street_address,
        "city": dealer.city,
        "state": dealer.state,
        "postal_code": dealer.postal_code,
        "full_address": dealer.full_address,
        "rating": str(dealer.rating_value) if dealer.rating_value is not None else "",
        "review_count": str(dealer.review_count) if dealer.review_count is not None else "",
        "website_url": dealer.website_url,
        "domain": dealer.domain,
        "autotrader_url": dealer.autotrader_url,
        "inventory_count": str(dealer.inventory_count) if dealer.inventory_count is not None else "",
    }


def _append_csv_row(dealer: AutotraderDealer) -> None:
    """Append a single dealer row to the CSV."""
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow(_dealer_to_row(dealer))


# ---------------------------------------------------------------------------
# Progress JSON
# ---------------------------------------------------------------------------
class ProgressTracker:
    """Tracks scrape stats and writes periodic JSON updates."""

    def __init__(self, total: int) -> None:
        self.total = total
        self.processed = 0
        self.saved = 0
        self.failed = 0
        self.skipped = 0
        self.start_time = time.monotonic()
        self._last_write = 0
        self.status = "running"

    def tick(self, saved: bool = False, failed: bool = False) -> None:
        self.processed += 1
        if saved:
            self.saved += 1
        if failed:
            self.failed += 1
        # Write every 10 processed (more frequent for sequential scraping)
        if self.processed - self._last_write >= 10 or self.processed >= self.total:
            self._write()
            self._last_write = self.processed

    def finish(self) -> None:
        self.status = "complete"
        self._write()

    def _write(self) -> None:
        elapsed = time.monotonic() - self.start_time
        elapsed_min = elapsed / 60
        rate = self.processed / elapsed_min if elapsed_min > 0 else 0
        remaining = self.total - self.processed
        eta_min = remaining / rate if rate > 0 else 0

        data = {
            "total": self.total,
            "processed": self.processed,
            "saved": self.saved,
            "failed": self.failed,
            "skipped": self.skipped,
            "rate_per_min": round(rate, 1),
            "elapsed_min": round(elapsed_min, 1),
            "eta_min": round(eta_min, 1),
            "last_updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "status": self.status,
        }
        PROGRESS_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Browser-based page fetcher
# ---------------------------------------------------------------------------
async def fetch_page_html(page, url: str, max_retries: int = 3) -> Optional[str]:
    """Navigate to a URL and return rendered HTML, with retry logic."""
    for attempt in range(max_retries):
        try:
            await page.get(url, new_tab=False)
            # Wait for page to render (JS-heavy site)
            await asyncio.sleep(random.uniform(3.0, 5.0))

            html = await page.evaluate("document.documentElement.outerHTML")
            if not isinstance(html, str):
                logger.warning(f"Non-string HTML response for {url}")
                return None

            # Check if we got the bot challenge page
            if len(html) < 10000 and "page unavailable" in html.lower():
                if attempt < max_retries - 1:
                    wait = (2 ** attempt) * 15  # 15s, 30s, 60s
                    logger.warning(
                        f"Bot challenge detected for {url}, "
                        f"backing off {wait}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait)
                    continue
                else:
                    logger.error(f"Bot challenge persisted after {max_retries} attempts: {url}")
                    return None

            return html

        except Exception as e:
            if attempt < max_retries - 1:
                wait = (2 ** attempt) * 10
                logger.warning(f"Error fetching {url}: {e}, retrying in {wait}s")
                await asyncio.sleep(wait)
            else:
                logger.error(f"Failed after {max_retries} attempts: {url} -- {e}")
                return None

    return None


# ---------------------------------------------------------------------------
# Core scrape loop
# ---------------------------------------------------------------------------
async def run_scrape(
    max_dealers: Optional[int] = None,
    state_filter: Optional[str] = None,
) -> None:
    """Main scrape orchestrator using nodriver."""
    import nodriver as uc

    # 1. Parse sitemap (local file, no HTTP needed)
    logger.info(f"Reading sitemap from {SITEMAP_PATH}")
    all_urls = parse_sitemap(SITEMAP_PATH)
    logger.info(f"Sitemap contains {len(all_urls)} dealer URLs")

    # 2. Filter by state
    if state_filter:
        suffix = f"-{state_filter.lower()}"
        all_urls = [
            u for u in all_urls
            if _city_state_from_url(u).endswith(suffix)
        ]
        logger.info(f"Filtered to {len(all_urls)} URLs for state {state_filter}")

    # 3. Resume: skip already-scraped
    _ensure_csv_header()
    existing = _load_existing_ids()
    if existing:
        before = len(all_urls)
        all_urls = [
            u for u in all_urls
            if _dealer_id_from_url(u) not in existing
        ]
        skipped = before - len(all_urls)
        logger.info(f"Resuming: skipping {skipped} already-scraped, {len(all_urls)} remaining")
    else:
        skipped = 0

    # 4. Limit
    if max_dealers and len(all_urls) > max_dealers:
        all_urls = all_urls[:max_dealers]
        logger.info(f"Limited to {max_dealers} dealers")

    if not all_urls:
        logger.info("Nothing to scrape. All done!")
        return

    # 5. Initialize progress
    tracker = ProgressTracker(len(all_urls))
    tracker.skipped = skipped
    tracker._write()

    logger.info(
        f"Starting scrape: {len(all_urls)} dealers, "
        f"sequential with 4-7s delay (nodriver)"
    )

    # 6. Launch browser
    browser = await uc.start(headless=False)

    # Warm up: visit autotrader homepage first to establish session
    logger.info("Warming up browser session on autotrader.com...")
    page = await browser.get("https://www.autotrader.com/")
    await asyncio.sleep(5)

    # 7. Sequential scrape
    consecutive_failures = 0
    for i, url in enumerate(all_urls):
        # Human-like delay between pages
        if i > 0:
            delay = random.uniform(4.0, 7.0)
            await asyncio.sleep(delay)

        html = await fetch_page_html(page, url)

        if html is None:
            tracker.tick(failed=True)
            consecutive_failures += 1
            logger.warning(
                f"[{tracker.processed}/{tracker.total}] FAILED {url} "
                f"(consecutive: {consecutive_failures})"
            )
            # If many consecutive failures, take a long break
            if consecutive_failures >= 5:
                logger.warning("5 consecutive failures, cooling down for 2 minutes...")
                await asyncio.sleep(120)
                consecutive_failures = 0
            continue

        consecutive_failures = 0

        dealer = extract_dealer_data(html, url)
        if dealer is None:
            tracker.tick(failed=True)
            logger.warning(f"[{tracker.processed}/{tracker.total}] Extraction failed: {url}")
            continue

        _append_csv_row(dealer)
        tracker.tick(saved=True)
        logger.info(
            f"[{tracker.processed}/{tracker.total}] "
            f"{dealer.name} | {dealer.city}, {dealer.state} | "
            f"rating={dealer.rating_value} reviews={dealer.review_count}"
        )

    # 8. Finish
    tracker.finish()
    elapsed_min = (time.monotonic() - tracker.start_time) / 60
    logger.info(
        f"Scrape complete: {tracker.saved} saved, {tracker.failed} failed, "
        f"{tracker.skipped} skipped in {elapsed_min:.1f} min"
    )
    logger.info(f"CSV: {CSV_PATH}")
    logger.info(f"Progress: {PROGRESS_PATH}")

    browser.stop()


def _dealer_id_from_url(url: str) -> Optional[str]:
    try:
        dealer_id, _, _ = parse_autotrader_url(url)
        return dealer_id
    except ValueError:
        return None


def _city_state_from_url(url: str) -> str:
    try:
        _, city_state, _ = parse_autotrader_url(url)
        return city_state
    except ValueError:
        return ""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Autotrader dealer scraper (nodriver)")
    parser.add_argument("--max-dealers", type=int, default=None, help="Limit number of dealers")
    parser.add_argument("--state", type=str, default=None, help="Filter by 2-letter state code")
    args = parser.parse_args()

    asyncio.run(run_scrape(max_dealers=args.max_dealers, state_filter=args.state))


if __name__ == "__main__":
    main()
