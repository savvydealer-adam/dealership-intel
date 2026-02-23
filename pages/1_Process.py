"""Process tab - Load Google Sheet and run the intelligence pipeline."""

import logging
import time

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Process - DealershipIntel", page_icon="🔍", layout="wide")
st.title("Process Dealerships")


def _get_apollo_service():
    from services.apollo_api import ApolloAPIService

    api_key = st.session_state.get("apollo_api_key")
    if not api_key:
        from config.settings import get_settings

        api_key = get_settings().apollo_api_key
    if api_key:
        return ApolloAPIService(api_key)
    return None


def _get_db_service():
    from services.database_service import DatabaseService

    db_url = st.session_state.get("database_url")
    if not db_url:
        from config.settings import get_settings

        db_url = get_settings().database_url
    if db_url:
        try:
            return DatabaseService(db_url)
        except Exception as e:
            st.error(f"Database connection failed: {e}")
    return None


# --- Input Section ---
st.header("1. Load Dealership List")

input_method = st.radio("Input method:", ["Google Sheet", "Manual URLs"], horizontal=True)

websites: list[str] = []
website_column = "Website"

if input_method == "Google Sheet":
    sheet_url = st.text_input("Google Sheet URL:", value=st.session_state.get("current_sheet_url", ""))
    website_column = st.text_input("Website column name:", value="Website")

    if sheet_url and st.button("Load Sheet"):
        google_json = st.session_state.get("google_sheets_json_raw")
        if not google_json:
            from config.settings import get_settings

            google_json = get_settings().google_sheets_json

        if not google_json:
            st.error("Google Sheets credentials not configured. Go to Settings.")
        else:
            try:
                from services.google_sheets import GoogleSheetsService

                with st.spinner("Loading Google Sheet..."):
                    sheets_service = GoogleSheetsService(google_json)
                    df = sheets_service.read_sheet(sheet_url, website_column)
                    st.session_state.current_sheet_url = sheet_url
                    st.session_state.loaded_df = df
                    st.success(f"Loaded {len(df)} dealerships from Google Sheet.")
            except Exception as e:
                st.error(f"Failed to load sheet: {e}")

    if "loaded_df" in st.session_state:
        df = st.session_state.loaded_df
        st.dataframe(df.head(10), use_container_width=True)
        websites = df[website_column].dropna().tolist()
        st.info(f"{len(websites)} dealership websites ready to process.")

else:
    urls_text = st.text_area("Enter dealership URLs (one per line):", height=200)
    if urls_text:
        websites = [url.strip() for url in urls_text.strip().split("\n") if url.strip()]
        st.info(f"{len(websites)} URLs entered.")


# --- Processing Options ---
st.header("2. Processing Options")

col1, col2, col3 = st.columns(3)
with col1:
    batch_size = st.number_input("Batch size:", min_value=1, max_value=50, value=10)
with col2:
    delay_seconds = st.number_input("Delay between requests (s):", min_value=0.5, max_value=10.0, value=1.0, step=0.5)
with col3:
    skip_existing = st.checkbox("Skip already-analyzed dealerships", value=True)

run_name = st.text_input("Run name:", value=f"DealershipIntel Run - {time.strftime('%Y-%m-%d %H:%M')}")


# --- Execute ---
st.header("3. Run Pipeline")

if websites and st.button("Start Processing", type="primary", use_container_width=True):
    apollo = _get_apollo_service()
    db = _get_db_service()

    if not apollo:
        st.error("Apollo API key not configured. Go to Settings.")
    else:
        from pipeline.intel_pipeline import IntelPipeline

        pipeline = IntelPipeline(
            apollo_service=apollo,
            db_service=db,
        )

        progress_bar = st.progress(0)
        status_text = st.empty()
        results_container = st.empty()

        def on_progress(current: int, total: int, message: str):
            progress_bar.progress(current / total)
            status_text.text(f"[{current}/{total}] {message}")

        with st.spinner("Processing dealerships..."):
            results = pipeline.process_dealerships(
                websites=websites,
                batch_size=batch_size,
                delay_seconds=delay_seconds,
                skip_existing=skip_existing,
                role_filter_criteria=st.session_state.get("role_filter_criteria"),
                run_name=run_name,
                sheet_url=st.session_state.get("current_sheet_url", ""),
                website_column=website_column,
                on_progress=on_progress,
            )

        if results:
            results_df = pd.DataFrame(results)
            st.session_state.results_df = results_df
            st.session_state.last_results = results

            status_text.text("Processing completed!")
            st.success(f"Processed {len(results)} dealerships.")

            # Summary metrics
            successful = sum(1 for r in results if r.get("status") == "Success")
            failed = sum(1 for r in results if r.get("status") != "Success")
            contacts = sum(len(r.get("contacts", [])) for r in results)

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total", len(results))
            m2.metric("Successful", successful)
            m3.metric("Failed", failed)
            m4.metric("Contacts Found", contacts)

            results_container.dataframe(results_df, use_container_width=True)

            # Write back to Google Sheet
            if contacts > 0 and st.session_state.get("current_sheet_url"):
                google_json = st.session_state.get("google_sheets_json_raw")
                if not google_json:
                    from config.settings import get_settings

                    google_json = get_settings().google_sheets_json

                if google_json:
                    try:
                        from services.google_sheets import GoogleSheetsService

                        with st.spinner("Writing contacts back to Google Sheet..."):
                            sheets_service = GoogleSheetsService(google_json)
                            contacts_for_write = [
                                {
                                    "original_website": r.get("original_website", ""),
                                    "domain": r.get("domain", ""),
                                    "contacts": r.get("contacts", []),
                                }
                                for r in results
                                if r.get("contacts")
                            ]

                            if contacts_for_write:
                                success = sheets_service.write_contacts_to_sheet(
                                    sheet_url=st.session_state.current_sheet_url,
                                    contacts_data=contacts_for_write,
                                    website_column=website_column,
                                )
                                if success:
                                    st.success("Contacts written back to Google Sheet!")
                    except Exception as e:
                        st.warning(f"Could not write back to sheet: {e}")
        else:
            st.error("No results were generated.")

elif not websites:
    st.info("Load a Google Sheet or enter URLs above to get started.")
