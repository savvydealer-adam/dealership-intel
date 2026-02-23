# DealershipIntel

Dealership intelligence gathering tool that crawls automotive dealer websites to extract staff contacts, inventory counts, platform info, social links, and review scores. Uses headless browser crawling (Pyppeteer) as the primary data source with Apollo.io API as fallback.

Built for the SavvyDealer sales workflow.

## Features

- **Staff Discovery** - Crawls /staff, /team, /about-us pages and extracts contact cards (name, title, email, phone)
- **Apollo.io Fallback** - Multi-strategy API search when crawling finds fewer than 2 contacts
- **Platform Detection** - Identifies DealerOn, Dealer.com, DealerInspire, DealerFire, and other website platforms
- **Inventory Counts** - Scrapes new and used vehicle inventory totals
- **Social Media** - Finds Facebook, Instagram, Twitter/X, YouTube, LinkedIn, TikTok profile links
- **Review Scores** - Pulls ratings from Google, DealerRater, and Yelp
- **Stealth Crawling** - Anti-detection with randomized user agents, viewports, fingerprint spoofing, and human-like delays
- **Contact Validation** - Email format/domain/mailbox verification, phone normalization, confidence scoring (0-100)
- **Role Classification** - Dealership-specific title patterns with seniority scoring
- **CRM Sync** - Push results to AI CRM via REST API
- **Google Sheets** - Load dealership lists from and export results to Google Sheets

## Project Structure

```
dealership-intel/
├── app.py                    # Streamlit entry point
├── pages/                    # Multi-page Streamlit UI
│   ├── 1_Process.py          # Load sheet, run pipeline
│   ├── 2_Results.py          # View & export results
│   ├── 3_Search.py           # Search history
│   └── 4_Settings.py         # API keys, DB, crawl config
├── config/
│   ├── settings.py           # Pydantic Settings (env vars)
│   └── platforms.py          # Known dealership platform signatures
├── crawlers/
│   ├── browser.py            # Pyppeteer browser pool manager
│   ├── stealth.py            # Anti-detection JS injection & headers
│   ├── staff_crawler.py      # Staff page discovery & extraction
│   ├── contact_extractor.py  # Email, phone, name extraction from HTML
│   ├── platform_detector.py  # Website platform identification
│   ├── inventory_crawler.py  # New/used vehicle inventory counts
│   ├── social_crawler.py     # Social media link finder
│   └── review_crawler.py     # Google/DealerRater/Yelp ratings
├── pipeline/
│   ├── intel_pipeline.py     # Main orchestrator
│   └── fallback_chain.py     # Crawl -> Apollo -> merge/dedupe
├── services/
│   ├── apollo_api.py         # Apollo.io API with retry & multi-strategy search
│   ├── database_service.py   # PostgreSQL CRUD
│   ├── database_schema.py    # Schema with migrations
│   ├── validation.py         # Contact validation & confidence scoring
│   ├── email_verification.py # DNS MX + SMTP verification
│   ├── role_classifier.py    # Dealership role patterns & seniority
│   ├── google_sheets.py      # Google Sheets integration
│   ├── crm_sync.py           # AI CRM REST API sync
│   ├── web_scraper.py        # Trafilatura-based content extraction
│   └── domain_utils.py       # URL parsing, company name extraction
└── tests/
```

## Setup

### Prerequisites

- Python 3.11+
- PostgreSQL (or use Docker)
- Chromium (auto-downloaded by Pyppeteer on first run)

### Install

```bash
git clone https://github.com/savvydealer-adam/dealership-intel.git
cd dealership-intel
pip install -e ".[dev]"
```

### Configure

```bash
cp .env.example .env
# Edit .env with your API keys and database URL
```

Required:
- `APOLLO_API_KEY` - Apollo.io API key
- `DATABASE_URL` - PostgreSQL connection string

Optional:
- `GOOGLE_SHEETS_JSON` - Google service account credentials for Sheets integration
- `CRM_API_URL` / `CRM_API_KEY` - AI CRM integration
- `CHROMIUM_PATH` - Custom Chromium binary path
- `BROWSER_HEADLESS` - Set `false` for debugging (default: `true`)

### Database

**With Docker:**
```bash
docker compose up -d db
```

**Or point `DATABASE_URL` at an existing PostgreSQL instance.** The app auto-creates tables on first run.

### Run

```bash
streamlit run app.py
```

## Docker

```bash
# Full stack (app + PostgreSQL)
docker compose up

# Just the database
docker compose up -d db
```

The Dockerfile includes Chromium for headless crawling in containerized environments.

## Development

```bash
# Run tests
pytest

# Lint
ruff check .

# Format
ruff format .
```

## How It Works

1. **Load** a Google Sheet of dealership websites
2. **Crawl** each site: discover staff pages, extract contacts, detect platform, count inventory, find social links, pull reviews
3. **Fallback** to Apollo.io API if crawling finds fewer than 2 contacts
4. **Merge & deduplicate** results by email
5. **Validate** all contacts (email verification, phone normalization, name checks)
6. **Score** confidence (0-100) based on data completeness, domain match, title quality
7. **Store** in PostgreSQL and optionally sync to AI CRM
8. **Export** results to Google Sheets or CSV

## Deployment

Configured for Google Cloud Run via `cloudbuild.yaml`. Pushes to `master` trigger automatic builds.

```bash
gcloud builds submit --config=cloudbuild.yaml
```

Secrets (API keys, database URL) are managed via Google Secret Manager.
