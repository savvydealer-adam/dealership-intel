"""DealershipIntel - Dealership intelligence gathering tool.

Slim entry point for the Streamlit multi-page app.
"""

import logging

import streamlit as st

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    st.set_page_config(
        page_title="DealershipIntel",
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("DealershipIntel")
    st.markdown("Dealership intelligence gathering: contacts, inventory, platform, social, reviews.")

    # Initialize session state
    if "settings_initialized" not in st.session_state:
        _init_session_state()

    # Sidebar: API status indicators
    with st.sidebar:
        st.header("Status")
        _render_status_sidebar()

    # Landing page
    st.markdown("---")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.markdown("### Process")
        st.markdown("Upload a Google Sheet of dealerships and run the intelligence pipeline.")
        if st.button("Go to Process", use_container_width=True):
            st.switch_page("pages/1_Process.py")

    with col2:
        st.markdown("### Results")
        st.markdown("View, filter, and export results from past processing runs.")
        if st.button("Go to Results", use_container_width=True):
            st.switch_page("pages/2_Results.py")

    with col3:
        st.markdown("### Search")
        st.markdown("Search the database of previously analyzed dealerships.")
        if st.button("Go to Search", use_container_width=True):
            st.switch_page("pages/3_Search.py")

    with col4:
        st.markdown("### Settings")
        st.markdown("Configure API keys, database connection, and crawling options.")
        if st.button("Go to Settings", use_container_width=True):
            st.switch_page("pages/4_Settings.py")

    with col5:
        st.markdown("### Autotrader")
        st.markdown("Bulk import 21K+ dealers from Autotrader's sitemap.")
        if st.button("Go to Autotrader", use_container_width=True):
            st.switch_page("pages/5_Autotrader.py")


def _init_session_state():
    """Initialize session state with defaults."""
    from config.settings import get_settings

    settings = get_settings()

    st.session_state.settings_initialized = True
    st.session_state.apollo_api_key = settings.apollo_api_key or ""
    st.session_state.google_sheets_json_raw = settings.google_sheets_json or ""
    st.session_state.database_url = settings.database_url or ""
    st.session_state.current_sheet_url = ""
    st.session_state.results_df = None
    st.session_state.verification_config = None
    st.session_state.role_filter_criteria = None


def _render_status_sidebar():
    """Render API/service status indicators in sidebar."""
    from config.settings import get_settings

    settings = get_settings()

    apollo_key = st.session_state.get("apollo_api_key") or settings.apollo_api_key
    sheets_json = st.session_state.get("google_sheets_json_raw") or settings.google_sheets_json
    db_url = st.session_state.get("database_url") or settings.database_url

    st.markdown(f"- Apollo API: {'Connected' if apollo_key else 'Not configured'}")
    st.markdown(f"- Google Sheets: {'Connected' if sheets_json else 'Not configured'}")
    st.markdown(f"- Database: {'Connected' if db_url else 'Not configured'}")
    st.markdown(f"- CRM: {'Connected' if settings.has_crm else 'Not configured'}")

    if not apollo_key or not sheets_json or not db_url:
        st.warning("Configure missing services in Settings.")


if __name__ == "__main__":
    main()
