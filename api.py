"""FastAPI microservice for dealership staff scraping.

Exposes the crawlers as REST endpoints for the CRM to call.
Run alongside Streamlit or standalone: uvicorn api:app --port 8080
"""

import asyncio
import logging
import os
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from crawlers.browser import get_browser_manager, get_nodriver_manager
from crawlers.staff_crawler import StaffCrawler
from crawlers.contact_extractor import extract_contacts_from_html

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Dealership Intel API", version="1.0.0")


class ScrapeRequest(BaseModel):
    url: str
    platform: str | None = None


class ExtractRequest(BaseModel):
    html: str
    url: str


class Contact(BaseModel):
    name: str = ""
    title: str = ""
    email: str | None = None
    phone: str | None = None
    photo_url: str | None = None
    source: str = "crawl"


class ScrapeResponse(BaseModel):
    success: bool
    contacts: list[Contact]
    error: str | None = None


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/api/extract/staff", response_model=ScrapeResponse)
async def extract_staff(req: ExtractRequest):
    """Extract contacts from pre-fetched HTML (no browser needed)."""
    if not req.html:
        raise HTTPException(status_code=400, detail="HTML content is required")

    contacts = extract_contacts_from_html(req.html, req.url)
    if not contacts:
        return ScrapeResponse(success=False, contacts=[], error="No contacts found in HTML")

    return ScrapeResponse(
        success=True,
        contacts=[
            Contact(
                name=c.get("name", ""),
                title=c.get("title", ""),
                email=c.get("email"),
                phone=c.get("phone"),
                photo_url=c.get("photo_url"),
                source=c.get("source", "extract"),
            )
            for c in contacts
        ],
    )


@app.post("/api/scrape/staff", response_model=ScrapeResponse)
async def scrape_staff(req: ScrapeRequest):
    """Scrape a dealership staff page for contacts with photos and emails."""
    if not req.url:
        raise HTTPException(status_code=400, detail="URL is required")

    contacts: list[dict[str, Any]] = []

    # Try nodriver first (better at Cloudflare bypass)
    try:
        logger.info(f"Attempting nodriver for {req.url}")
        chromium = os.environ.get("CHROMIUM_PATH", "not set")
        logger.info(f"CHROMIUM_PATH={chromium}")
        nodriver_mgr = get_nodriver_manager()
        html = await nodriver_mgr.get_page_content(req.url, timeout=60)
        if html and "Just a moment" not in html:
            logger.info(f"nodriver got {len(html)} bytes of HTML")
            contacts = extract_contacts_from_html(html, req.url)
            logger.info(f"Extracted {len(contacts)} contacts from nodriver HTML")
        elif html:
            logger.warning(f"nodriver got Cloudflare challenge page ({len(html)} bytes)")
        else:
            logger.warning("nodriver returned no HTML")
    except Exception as e:
        logger.warning(f"nodriver failed for {req.url}: {e}", exc_info=True)

    # Fallback to pyppeteer staff crawler if nodriver didn't get results
    if not contacts:
        try:
            logger.info(f"Falling back to pyppeteer for {req.url}")
            browser_mgr = get_browser_manager()
            crawler = StaffCrawler(browser_manager=browser_mgr)
            from urllib.parse import urlparse
            parsed = urlparse(req.url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            contacts = await crawler.crawl_staff_page(base_url, platform=req.platform)
            if contacts:
                logger.info(f"pyppeteer found {len(contacts)} contacts")
        except Exception as e:
            logger.warning(f"pyppeteer crawl failed for {req.url}: {e}")

    if not contacts:
        return ScrapeResponse(success=False, contacts=[], error="No contacts found")

    return ScrapeResponse(
        success=True,
        contacts=[
            Contact(
                name=c.get("name", ""),
                title=c.get("title", ""),
                email=c.get("email"),
                phone=c.get("phone"),
                photo_url=c.get("photo_url"),
                source=c.get("source", "crawl"),
            )
            for c in contacts
        ],
    )
