"""Anti-detection layer for Pyppeteer: JS injection, headers, fingerprinting."""

import logging
import random

from pyppeteer.page import Page

logger = logging.getLogger(__name__)

# Realistic user agents to rotate through
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.2 Safari/605.1.15",
]

# Realistic viewport sizes
VIEWPORTS = [
    {"width": 1280, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
]

# Stealth JS to inject before any page scripts run
STEALTH_JS = """() => {
    // Override navigator.webdriver
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    });

    // Override chrome runtime
    window.chrome = {
        runtime: {}
    };

    // Override permissions query
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );

    // Override plugins
    Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5]
    });

    // Override languages
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en']
    });

    // Override platform
    Object.defineProperty(navigator, 'platform', {
        get: () => 'Win32'
    });

    // Override hardware concurrency
    Object.defineProperty(navigator, 'hardwareConcurrency', {
        get: () => 8
    });

    // Override device memory
    Object.defineProperty(navigator, 'deviceMemory', {
        get: () => 8
    });

    // Override connection
    Object.defineProperty(navigator, 'connection', {
        get: () => ({
            effectiveType: '4g',
            rtt: 50,
            downlink: 10,
            saveData: false
        })
    });

    // Prevent iframe detection
    Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
        get: function() {
            return window;
        }
    });
}"""

# Cookie consent auto-dismiss patterns
COOKIE_DISMISS_JS = """() => {
    const selectors = [
        '[id*="cookie"] button[class*="accept"]',
        '[id*="cookie"] button[class*="agree"]',
        '[class*="cookie"] button[class*="accept"]',
        '[class*="cookie"] button[class*="agree"]',
        '[id*="consent"] button[class*="accept"]',
        'button[id*="accept-cookie"]',
        'button[id*="acceptCookie"]',
        'a[id*="accept-cookie"]',
        '#onetrust-accept-btn-handler',
        '.cc-accept',
        '.cc-dismiss',
    ];

    for (const selector of selectors) {
        const btn = document.querySelector(selector);
        if (btn) {
            btn.click();
            return true;
        }
    }
    return false;
}"""

# CAPTCHA detection
CAPTCHA_DETECT_JS = """() => {
    const indicators = [
        document.querySelector('[class*="captcha"]'),
        document.querySelector('[id*="captcha"]'),
        document.querySelector('[class*="recaptcha"]'),
        document.querySelector('[id*="recaptcha"]'),
        document.querySelector('iframe[src*="recaptcha"]'),
        document.querySelector('iframe[src*="hcaptcha"]'),
    ];
    return indicators.some(el => el !== null);
}"""


async def apply_stealth(page: Page) -> None:
    """Apply all stealth measures to a page."""
    # Set random user agent
    ua = random.choice(USER_AGENTS)
    await page.setUserAgent(ua)

    # Set random viewport
    viewport = random.choice(VIEWPORTS)
    await page.setViewport(viewport)

    # Set extra HTTP headers
    await page.setExtraHTTPHeaders(
        {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
    )

    # Inject stealth JS before any page scripts
    await page.evaluateOnNewDocument(STEALTH_JS)

    logger.debug(f"Stealth applied: UA={ua[:50]}..., viewport={viewport}")


async def dismiss_cookie_consent(page: Page) -> bool:
    """Try to dismiss cookie consent banners."""
    try:
        result = await page.evaluate(COOKIE_DISMISS_JS)
        if result:
            logger.debug("Cookie consent dismissed")
        return result
    except Exception:
        return False


async def detect_captcha(page: Page) -> bool:
    """Check if page has a CAPTCHA challenge."""
    try:
        has_captcha = await page.evaluate(CAPTCHA_DETECT_JS)
        if has_captcha:
            logger.warning("CAPTCHA detected - skipping page")
        return has_captcha
    except Exception:
        return False


async def human_delay(min_seconds: float = 1.5, max_seconds: float = 3.0) -> None:
    """Add a human-like random delay."""
    import asyncio

    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)
