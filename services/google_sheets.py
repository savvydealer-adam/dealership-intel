"""Google Sheets integration for reading dealership lists and writing results."""

import json
import logging
from typing import Any, Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


class GoogleSheetsService:
    def __init__(self, service_account_json: str):
        try:
            if isinstance(service_account_json, str):
                credentials_dict = json.loads(service_account_json)
            else:
                credentials_dict = service_account_json

            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]

            credentials = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
            self.gc = gspread.authorize(credentials)

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON credentials: {e}")
        except Exception as e:
            raise RuntimeError(f"Error initializing Google Sheets service: {e}")

    def read_sheet(self, sheet_url: str, website_column: str = "Website") -> pd.DataFrame:
        """Read data from Google Sheet."""
        try:
            sheet = self.gc.open_by_url(sheet_url)
            worksheet = sheet.get_worksheet(0)
            records = worksheet.get_all_records()

            if not records:
                raise ValueError("No data found in the spreadsheet")

            df = pd.DataFrame(records)

            if website_column not in df.columns:
                available_columns = ", ".join(df.columns.tolist())
                raise ValueError(f"Column '{website_column}' not found. Available columns: {available_columns}")

            return df[df[website_column].notna() & (df[website_column] != "")].copy()

        except Exception as e:
            raise RuntimeError(f"Error reading Google Sheet: {e}")

    def create_and_export_sheet(self, df: pd.DataFrame, sheet_name: str) -> str:
        """Create a new Google Sheet and export DataFrame to it."""
        try:
            spreadsheet = self.gc.create(sheet_name)
            worksheet = spreadsheet.get_worksheet(0)
            worksheet.clear()

            data = [df.columns.tolist()] + df.fillna("").values.tolist()
            worksheet.update("A1", data)
            spreadsheet.share("", perm_type="anyone", role="reader")

            return spreadsheet.url or ""

        except Exception as e:
            raise RuntimeError(f"Error creating and exporting to Google Sheet: {e}")

    def append_to_sheet(self, sheet_url: str, df: pd.DataFrame, worksheet_name: Optional[str] = None):
        """Append DataFrame data to existing Google Sheet."""
        try:
            sheet = self.gc.open_by_url(sheet_url)
            worksheet = sheet.worksheet(worksheet_name) if worksheet_name else sheet.get_worksheet(0)
            data = df.fillna("").values.tolist()
            worksheet.append_rows(data)

        except Exception as e:
            raise RuntimeError(f"Error appending to Google Sheet: {e}")

    def write_contacts_to_sheet(
        self,
        sheet_url: str,
        contacts_data: list[dict[str, Any]],
        website_column: str = "Website",
    ) -> bool:
        """Write contact information back to the original Google Sheet."""
        try:
            sheet = self.gc.open_by_url(sheet_url)
            worksheet = sheet.get_worksheet(0)

            current_data = worksheet.get_all_records()
            if not current_data:
                raise ValueError("No data found in the spreadsheet")

            headers = worksheet.row_values(1)

            new_columns = [
                "Manager_Name",
                "Job_Title",
                "Email_Address",
                "Phone_Number",
                "LinkedIn_URL",
                "Confidence_Score",
                "Contact_Count",
                "Last_Updated",
            ]

            columns_to_add = [col for col in new_columns if col not in headers]
            if columns_to_add:
                current_headers = worksheet.row_values(1)
                extended_headers = current_headers + columns_to_add
                worksheet.update(range_name="1:1", values=[extended_headers])
                headers = extended_headers

            # Build lookup by normalized domain
            from services.domain_utils import extract_domain

            contacts_lookup: dict[str, dict[str, str]] = {}
            for contact_data in contacts_data:
                domain = contact_data.get("domain", "")
                contacts = contact_data.get("contacts", [])

                if domain and contacts:
                    best_contact = max(contacts, key=lambda c: c.get("confidence_score", 0))
                    normalized_domain = domain.lower().strip().replace("www.", "")
                    contacts_lookup[normalized_domain] = {
                        "Manager_Name": best_contact.get("name", ""),
                        "Job_Title": best_contact.get("title", ""),
                        "Email_Address": best_contact.get("email", ""),
                        "Phone_Number": best_contact.get("phone", ""),
                        "LinkedIn_URL": best_contact.get("linkedin_url", ""),
                        "Confidence_Score": f"{best_contact.get('confidence_score', 0):.0f}%",
                        "Contact_Count": str(len(contacts)),
                        "Last_Updated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
                    }

            headers = worksheet.row_values(1)
            website_col_idx = headers.index(website_column) if website_column in headers else None
            if website_col_idx is None:
                raise ValueError(f"Website column '{website_column}' not found")

            col_indices = {col: headers.index(col) for col in new_columns if col in headers}

            all_data = worksheet.get_all_values()
            data_rows = all_data[1:] if len(all_data) > 1 else []

            batch_updates = []
            for i, row in enumerate(data_rows, 2):
                while len(row) < len(headers):
                    row.append("")

                website_url = row[website_col_idx] if website_col_idx < len(row) else ""
                if website_url:
                    row_domain = extract_domain(website_url)
                    if row_domain:
                        normalized_row_domain = row_domain.lower().strip().replace("www.", "")
                        contact_info = contacts_lookup.get(normalized_row_domain)

                        if contact_info:
                            for col_name, col_idx in col_indices.items():
                                col_letter = self._column_index_to_letter(col_idx)
                                cell_ref = f"{col_letter}{i}"
                                cell_value = contact_info.get(col_name, "")
                                batch_updates.append({"range": cell_ref, "values": [[cell_value]]})

            if batch_updates:
                chunk_size = 500
                for i in range(0, len(batch_updates), chunk_size):
                    chunk = batch_updates[i : i + chunk_size]
                    worksheet.batch_update(chunk)

            return True

        except Exception as e:
            raise RuntimeError(f"Error writing contacts to Google Sheet: {e}")

    def _column_index_to_letter(self, col_index: int) -> str:
        """Convert 0-based column index to Excel-style column letter(s)."""
        result = ""
        col_index += 1
        while col_index > 0:
            col_index -= 1
            result = chr(65 + (col_index % 26)) + result
            col_index //= 26
        return result
