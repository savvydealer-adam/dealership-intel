"""Settings tab - Configure API keys, database, and crawling options."""

import logging

import streamlit as st

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Settings - DealershipIntel", page_icon="🔍", layout="wide")
st.title("Settings")

from config.settings import get_settings  # noqa: E402

settings = get_settings()

# --- API Keys ---
st.header("API Keys")

apollo_key = st.text_input(
    "Apollo API Key:",
    value=st.session_state.get("apollo_api_key", settings.apollo_api_key or ""),
    type="password",
)
if apollo_key != st.session_state.get("apollo_api_key"):
    st.session_state.apollo_api_key = apollo_key

google_json = st.text_area(
    "Google Sheets Service Account JSON:",
    value=st.session_state.get("google_sheets_json_raw", settings.google_sheets_json or ""),
    height=100,
)
if google_json != st.session_state.get("google_sheets_json_raw"):
    st.session_state.google_sheets_json_raw = google_json

# --- Database ---
st.header("Database")

db_url = st.text_input(
    "PostgreSQL Database URL:",
    value=st.session_state.get("database_url", settings.database_url or ""),
    type="password",
)
if db_url != st.session_state.get("database_url"):
    st.session_state.database_url = db_url

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("Test Connection"):
        if db_url:
            try:
                from services.database_service import DatabaseService

                db = DatabaseService(db_url, auto_initialize=False)
                health = db.get_database_health()
                db.close()

                if health.get("connection_healthy"):
                    st.success("Database connection successful!")
                    st.json(health.get("tables_exist", {}))
                else:
                    st.error(f"Connection failed: {health.get('error_message')}")
            except Exception as e:
                st.error(f"Connection test failed: {e}")
        else:
            st.warning("Enter a database URL first.")

with col2:
    if st.button("Initialize Schema"):
        if db_url:
            try:
                from services.database_service import DatabaseService

                db = DatabaseService(db_url, auto_initialize=True)
                st.success("Database schema initialized!")
                db.close()
            except Exception as e:
                st.error(f"Schema initialization failed: {e}")

with col3:
    if st.button("View Stats"):
        if db_url:
            try:
                from services.database_service import DatabaseService

                db = DatabaseService(db_url, auto_initialize=False)
                stats = db.get_database_stats()
                db.close()
                st.json(stats)
            except Exception as e:
                st.error(f"Could not get stats: {e}")

# --- CRM Integration ---
st.header("AI CRM Integration")

crm_url = st.text_input("CRM API URL:", value=settings.crm_api_url)
crm_key = st.text_input("CRM API Key:", value=settings.crm_api_key or "", type="password")

st.info("CRM integration will be available in Phase 4.")

# --- Crawling Settings ---
st.header("Crawling Settings")

col1, col2 = st.columns(2)
with col1:
    st.number_input("Min delay between requests (s):", value=settings.crawl_delay_min, min_value=0.5, step=0.5)
    st.number_input("Max delay between requests (s):", value=settings.crawl_delay_max, min_value=1.0, step=0.5)
with col2:
    st.number_input("Page timeout (s):", value=settings.crawl_timeout, min_value=10, step=5)
    st.number_input("Max concurrent pages:", value=settings.browser_max_pages, min_value=1, max_value=10)

st.checkbox("Headless browser mode", value=settings.browser_headless)

st.info("Crawling engine will be available in Phase 2.")

# --- Email Verification ---
st.header("Email Verification")

enable_verification = st.checkbox("Enable email verification", value=False)
if enable_verification:
    from services.email_verification import VerificationConfig

    config = VerificationConfig(
        enable_format_check=st.checkbox("Format checking", value=True),
        enable_domain_check=st.checkbox("Domain (MX) checking", value=True),
        enable_mailbox_check=st.checkbox("Mailbox (SMTP) checking", value=False),
        domain_timeout=st.number_input("Domain check timeout (s):", value=5.0, min_value=1.0),
    )
    st.session_state.verification_config = config
else:
    st.session_state.verification_config = None

# --- Role Filtering ---
st.header("Role Filtering")

enable_role_filter = st.checkbox("Enable role filtering", value=False)
if enable_role_filter:
    from services.role_classifier import RoleCategory, RoleFilterCriteria, SeniorityLevel

    selected_seniorities = st.multiselect(
        "Seniority levels:",
        options=[s.value for s in SeniorityLevel if s != SeniorityLevel.OTHER],
        default=["C-Suite", "Senior Executive", "Director", "Manager"],
    )

    selected_categories = st.multiselect(
        "Role categories:",
        options=[c.value for c in RoleCategory if c != RoleCategory.OTHER],
        default=["Ownership", "Senior Leadership", "Management", "Sales"],
    )

    dealership_only = st.checkbox("Dealership-specific roles only", value=False)

    criteria = RoleFilterCriteria(
        seniority_levels=[s for s in SeniorityLevel if s.value in selected_seniorities],
        categories=[c for c in RoleCategory if c.value in selected_categories],
        dealership_specific_only=dealership_only,
    )
    st.session_state.role_filter_criteria = criteria
else:
    st.session_state.role_filter_criteria = None

# --- Apollo Status ---
st.header("API Status")

if st.button("Check Apollo API"):
    key = st.session_state.get("apollo_api_key")
    if key:
        try:
            from services.apollo_api import ApolloAPIService

            apollo = ApolloAPIService(key)
            usage = apollo.check_api_usage()
            if usage:
                st.success("Apollo API connected!")
                st.json(usage)
            else:
                st.warning("Could not retrieve API usage info.")
        except Exception as e:
            st.error(f"Apollo check failed: {e}")
    else:
        st.warning("Apollo API key not set.")
