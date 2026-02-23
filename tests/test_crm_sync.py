"""Tests for CRM sync service."""

from unittest.mock import MagicMock, patch

from services.crm_sync import CRMSyncService


class TestCRMSyncService:
    def test_not_configured(self):
        service = CRMSyncService(api_url="", api_key="")
        assert not service.is_configured

    def test_configured(self):
        service = CRMSyncService(api_url="http://localhost:3000/api", api_key="test-key")
        assert service.is_configured

    def test_sync_without_config_returns_none(self):
        service = CRMSyncService(api_url="", api_key="")
        result = service.sync_dealership({"domain": "test.com"})
        assert result is None

    def test_sync_without_domain_returns_none(self):
        service = CRMSyncService(api_url="http://localhost:3000/api", api_key="key")
        result = service.sync_dealership({})
        assert result is None

    @patch("services.crm_sync.requests.Session")
    def test_sync_dealership_success(self, mock_session_class):
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Mock dealership upsert response
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {"id": 1}
        mock_session.post.return_value = mock_response

        service = CRMSyncService(api_url="http://localhost:3000/api", api_key="key")
        service.session = mock_session

        intel_data = {
            "domain": "test.com",
            "company_name": "Test Dealer",
            "original_website": "https://test.com",
            "contacts": [
                {"name": "John", "email": "john@test.com", "title": "Manager"},
            ],
        }

        result = service.sync_dealership(intel_data)
        assert result is not None
        assert result["dealership_id"] == 1
        assert result["clients_synced"] == 1

    def test_test_connection_not_configured(self):
        service = CRMSyncService(api_url="", api_key="")
        result = service.test_connection()
        assert not result["connected"]
