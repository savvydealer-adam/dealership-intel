"""AI CRM API integration - push dealership intelligence to the CRM."""

import json
import logging
from datetime import datetime
from typing import Any, Optional

import requests

from config.settings import get_settings

logger = logging.getLogger(__name__)


class CRMSyncService:
    """Syncs dealership intelligence data to the AI CRM via REST API."""

    def __init__(self, api_url: Optional[str] = None, api_key: Optional[str] = None):
        settings = get_settings()
        self.api_url = (api_url or settings.crm_api_url).rstrip("/")
        self.api_key = api_key or settings.crm_api_key

        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({"X-API-Key": self.api_key})
        self.session.headers.update({"Content-Type": "application/json"})

    @property
    def is_configured(self) -> bool:
        return bool(self.api_url and self.api_key)

    def sync_dealership(self, intel_data: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Sync a single dealership and its contacts to the CRM.

        Args:
            intel_data: Full intelligence result from the pipeline.

        Returns:
            CRM response dict or None on failure.
        """
        if not self.is_configured:
            logger.warning("CRM not configured - skipping sync")
            return None

        domain = intel_data.get("domain", "")
        if not domain:
            logger.warning("No domain in intel data - skipping CRM sync")
            return None

        try:
            # Step 1: Upsert dealership record
            dealership_id = self._upsert_dealership(intel_data)
            if not dealership_id:
                return None

            # Step 2: Sync contacts as clients
            contacts = intel_data.get("contacts", [])
            synced_clients = []
            for contact in contacts:
                client_id = self._upsert_client(contact, dealership_id)
                if client_id:
                    synced_clients.append(client_id)

            # Step 3: Log intelligence as activity
            self._log_activity(dealership_id, intel_data)

            result = {
                "dealership_id": dealership_id,
                "clients_synced": len(synced_clients),
                "domain": domain,
            }
            logger.info(f"CRM sync complete for {domain}: {result}")
            return result

        except Exception as e:
            logger.error(f"CRM sync failed for {domain}: {e}")
            return None

    def _upsert_dealership(self, intel_data: dict[str, Any]) -> Optional[int]:
        """Create or update a dealership record in the CRM."""
        payload = {
            "name": intel_data.get("company_name", ""),
            "website": intel_data.get("original_website", ""),
            "phone": intel_data.get("company_phone", ""),
            "address": intel_data.get("company_address", ""),
            "industry": intel_data.get("industry", "Automotive"),
            "lead_source": "DealershipIntel",
        }

        try:
            # Try domain-based lookup/upsert first
            response = self.session.post(
                f"{self.api_url}/dealerships/by-domain",
                json={"domain": intel_data.get("domain", ""), **payload},
                timeout=15,
            )

            if response.ok:
                data = response.json()
                return data.get("id") or data.get("dealership_id")

            # Fallback to standard create
            if response.status_code == 404:
                response = self.session.post(
                    f"{self.api_url}/dealerships",
                    json=payload,
                    timeout=15,
                )
                if response.ok:
                    data = response.json()
                    return data.get("id") or data.get("dealership_id")

            logger.warning(f"Dealership upsert failed: HTTP {response.status_code}")
            return None

        except requests.RequestException as e:
            logger.error(f"Dealership upsert request failed: {e}")
            return None

    def _upsert_client(self, contact: dict[str, Any], dealership_id: int) -> Optional[int]:
        """Create or update a client (contact) record in the CRM."""
        payload = {
            "name": contact.get("name", ""),
            "email": contact.get("email", ""),
            "phone": contact.get("phone", ""),
            "title": contact.get("title", ""),
            "dealershipId": dealership_id,
            "lead_source": "DealershipIntel",
        }

        try:
            response = self.session.post(
                f"{self.api_url}/clients",
                json=payload,
                timeout=15,
            )

            if response.ok:
                data = response.json()
                return data.get("id") or data.get("client_id")

            logger.warning(f"Client upsert failed: HTTP {response.status_code}")
            return None

        except requests.RequestException as e:
            logger.error(f"Client upsert request failed: {e}")
            return None

    def _log_activity(self, dealership_id: int, intel_data: dict[str, Any]) -> None:
        """Log intelligence findings as an activity in the CRM."""
        metadata = {
            "domain": intel_data.get("domain", ""),
            "industry": intel_data.get("industry", ""),
            "company_size": intel_data.get("company_size", ""),
            "contacts_found": len(intel_data.get("contacts", [])),
            "platform": intel_data.get("platform", {}).get("platform", ""),
            "new_inventory": intel_data.get("inventory", {}).get("new_count"),
            "used_inventory": intel_data.get("inventory", {}).get("used_count"),
            "social_links": intel_data.get("social_links", {}),
            "reviews": intel_data.get("reviews", []),
            "scan_timestamp": datetime.now().isoformat(),
        }

        payload = {
            "dealershipId": dealership_id,
            "type": "note",
            "subject": "DealershipIntel Scan",
            "description": f"Intelligence scan completed for {intel_data.get('company_name', 'Unknown')}",
            "metadata": json.dumps(metadata),
        }

        try:
            response = self.session.post(
                f"{self.api_url}/activities",
                json=payload,
                timeout=15,
            )

            if not response.ok:
                logger.warning(f"Activity log failed: HTTP {response.status_code}")

        except requests.RequestException as e:
            logger.error(f"Activity log request failed: {e}")

    def test_connection(self) -> dict[str, Any]:
        """Test the CRM API connection."""
        if not self.is_configured:
            return {"connected": False, "error": "CRM not configured"}

        try:
            response = self.session.get(f"{self.api_url}/health", timeout=10)
            return {
                "connected": response.ok,
                "status_code": response.status_code,
                "url": self.api_url,
            }
        except requests.RequestException as e:
            return {"connected": False, "error": str(e)}
