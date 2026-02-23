"""Centralized configuration using Pydantic Settings."""

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file."""

    # Apollo.io
    apollo_api_key: Optional[str] = Field(default=None, alias="APOLLO_API_KEY")

    # Database
    database_url: Optional[str] = Field(default=None, alias="DATABASE_URL")

    # Google Sheets
    google_sheets_json: Optional[str] = Field(default=None, alias="GOOGLE_SHEETS_JSON")

    # AI CRM
    crm_api_url: str = Field(default="http://localhost:3000/api", alias="CRM_API_URL")
    crm_api_key: Optional[str] = Field(default=None, alias="CRM_API_KEY")

    # Browser / Crawling
    chromium_path: Optional[str] = Field(default=None, alias="CHROMIUM_PATH")
    browser_headless: bool = Field(default=True, alias="BROWSER_HEADLESS")
    browser_max_pages: int = Field(default=3, alias="BROWSER_MAX_PAGES")
    crawl_delay_min: float = Field(default=1.5, alias="CRAWL_DELAY_MIN")
    crawl_delay_max: float = Field(default=3.0, alias="CRAWL_DELAY_MAX")
    crawl_timeout: int = Field(default=30, alias="CRAWL_TIMEOUT")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
        "populate_by_name": True,
    }

    @property
    def has_apollo(self) -> bool:
        return bool(self.apollo_api_key)

    @property
    def has_database(self) -> bool:
        return bool(self.database_url)

    @property
    def has_google_sheets(self) -> bool:
        return bool(self.google_sheets_json)

    @property
    def has_crm(self) -> bool:
        return bool(self.crm_api_key and self.crm_api_url)


# Singleton instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get or create the singleton Settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
