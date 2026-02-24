"""Pyppeteer browser manager with optional nodriver fallback for Cloudflare sites."""

import asyncio
import logging
import os
from typing import Optional

import pyppeteer
from pyppeteer.browser import Browser
from pyppeteer.page import Page

from config.settings import get_settings
from crawlers.stealth import apply_stealth

logger = logging.getLogger(__name__)

# Default Chromium launch args (adapted from dealership-audit)
LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-infobars",
    "--disable-extensions",
    "--disable-plugins-discovery",
    "--disable-gpu",
    "--window-size=1280,900",
]


class BrowserManager:
    """Manages a Pyppeteer browser instance with page pooling."""

    def __init__(
        self,
        headless: bool = True,
        max_pages: int = 3,
        chromium_path: Optional[str] = None,
    ):
        self.headless = headless
        self.max_pages = max_pages
        self.chromium_path = chromium_path or os.environ.get("CHROMIUM_PATH")
        self._browser: Optional[Browser] = None
        self._semaphore = asyncio.Semaphore(max_pages)

    async def launch(self) -> Browser:
        """Launch the browser if not already running."""
        if self._browser and self._browser.process and self._browser.process.returncode is None:
            return self._browser

        launch_kwargs = {
            "headless": self.headless,
            "args": LAUNCH_ARGS,
            "handleSIGINT": False,
            "handleSIGTERM": False,
            "handleSIGHUP": False,
        }

        if self.chromium_path:
            launch_kwargs["executablePath"] = self.chromium_path

        logger.info(f"Launching browser (headless={self.headless})")
        self._browser = await pyppeteer.launch(**launch_kwargs)
        return self._browser

    async def new_page(self) -> Page:
        """Create a new page with stealth applied."""
        browser = await self.launch()
        page = await browser.newPage()

        await apply_stealth(page)
        await page.setViewport({"width": 1280, "height": 900})

        return page

    async def get_page(self) -> "PageContext":
        """Get a page from the pool (context manager)."""
        return PageContext(self)

    async def close(self):
        """Gracefully shut down the browser."""
        if self._browser:
            try:
                await self._browser.close()
                logger.info("Browser closed")
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")
            finally:
                self._browser = None


class PageContext:
    """Async context manager for borrowing a page from the pool."""

    def __init__(self, manager: BrowserManager):
        self.manager = manager
        self.page: Optional[Page] = None

    async def __aenter__(self) -> Page:
        await self.manager._semaphore.acquire()
        self.page = await self.manager.new_page()
        return self.page

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.page:
            try:
                await self.page.close()
            except Exception:
                pass
        self.manager._semaphore.release()


class NodriverManager:
    """Alternative browser using nodriver for Cloudflare-protected sites.

    nodriver (undetected-chromedriver successor) is better at bypassing
    Cloudflare challenges than Pyppeteer. Used as a fallback when
    Pyppeteer gets blocked.
    """

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._browser = None

    async def get_page_content(self, url: str, timeout: int = 30) -> Optional[str]:
        """Navigate to URL using nodriver and return page HTML.

        Returns None if nodriver is not installed or navigation fails.
        """
        try:
            import nodriver as uc
        except ImportError:
            logger.warning("nodriver not installed - pip install nodriver")
            return None

        browser = None
        try:
            browser = await uc.start(headless=self.headless)
            page = await browser.get(url, new_tab=True)

            # Wait for page to stabilize (Cloudflare challenge resolution)
            await asyncio.sleep(5)

            # Check if we passed the challenge
            html = await page.get_content()
            if html and "Just a moment" not in html:
                return html

            # Wait longer for challenge to resolve
            await asyncio.sleep(10)
            html = await page.get_content()
            return html

        except Exception as e:
            logger.warning(f"nodriver fetch failed for {url}: {e}")
            return None
        finally:
            if browser:
                try:
                    browser.stop()
                except Exception:
                    pass

    async def close(self):
        """Clean up."""
        if self._browser:
            try:
                self._browser.stop()
            except Exception:
                pass
            self._browser = None


# Module-level convenience
_manager: Optional[BrowserManager] = None
_nodriver_manager: Optional[NodriverManager] = None


def get_browser_manager() -> BrowserManager:
    """Get or create the singleton browser manager."""
    global _manager
    if _manager is None:
        settings = get_settings()
        _manager = BrowserManager(
            headless=settings.browser_headless,
            max_pages=settings.browser_max_pages,
            chromium_path=settings.chromium_path,
        )
    return _manager


def get_nodriver_manager() -> NodriverManager:
    """Get or create the singleton nodriver manager."""
    global _nodriver_manager
    if _nodriver_manager is None:
        settings = get_settings()
        _nodriver_manager = NodriverManager(headless=settings.browser_headless)
    return _nodriver_manager
