"""Search tab - Search the database of previously analyzed dealerships."""

import logging

import streamlit as st

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Search - DealershipIntel", page_icon="🔍", layout="wide")
st.title("Search History")


def _get_db_service():
    from services.database_service import DatabaseService

    db_url = st.session_state.get("database_url")
    if not db_url:
        from config.settings import get_settings

        db_url = get_settings().database_url
    if db_url:
        try:
            return DatabaseService(db_url, auto_initialize=False)
        except Exception as e:
            st.error(f"Database connection failed: {e}")
    return None


db = _get_db_service()

if not db:
    st.warning("Database not configured. Go to Settings to configure DATABASE_URL.")
    st.stop()

# --- Search ---
st.header("Search Companies")

col1, col2, col3 = st.columns(3)
with col1:
    search_term = st.text_input("Search (name or domain):")
with col2:
    industry_filter = st.text_input("Industry:")
with col3:
    min_confidence = st.slider("Min confidence:", 0, 100, 0)

if st.button("Search", type="primary"):
    try:
        results, total = db.search_companies(
            search_term=search_term,
            industry=industry_filter,
            min_confidence=min_confidence,
            limit=50,
        )

        st.markdown(f"**{total}** results found (showing up to 50).")

        if results:
            for company in results:
                name = company.get("company_name", company.get("domain", "Unknown"))
                with st.expander(f"{name} - {company.get('domain', '')}"):
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown(f"**Industry:** {company.get('industry', 'N/A')}")
                        st.markdown(f"**Size:** {company.get('company_size', 'N/A')}")
                        st.markdown(f"**Phone:** {company.get('company_phone', 'N/A')}")
                    with c2:
                        st.markdown(f"**Address:** {company.get('company_address', 'N/A')}")
                        st.markdown(f"**Status:** {company.get('status', 'N/A')}")
                        st.markdown(f"**Website:** {company.get('original_website', 'N/A')}")

                    contacts = company.get("contacts", [])
                    if isinstance(contacts, list) and contacts:
                        st.markdown("**Contacts:**")
                        for contact in contacts:
                            if isinstance(contact, dict):
                                st.markdown(
                                    f"- {contact.get('name', 'N/A')} | "
                                    f"{contact.get('title', 'N/A')} | "
                                    f"{contact.get('email', 'N/A')} | "
                                    f"Confidence: {contact.get('confidence_score', 0)}"
                                )
        else:
            st.info("No results found.")
    except Exception as e:
        st.error(f"Search failed: {e}")

# --- Analysis Runs ---
st.header("Recent Analysis Runs")

try:
    runs = db.get_analysis_runs(limit=10)
    if runs:
        for run in runs:
            with st.expander(f"{run.get('run_name', 'Unnamed')} - {run.get('status', 'unknown')}"):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Processed", run.get("companies_processed", 0))
                c2.metric("Successful", run.get("companies_successful", 0))
                c3.metric("Failed", run.get("companies_failed", 0))
                c4.metric("Contacts", run.get("contacts_found", 0))
                st.markdown(f"Started: {run.get('started_at', 'N/A')}")
                st.markdown(f"Sheet: {run.get('google_sheet_url', 'N/A')}")
    else:
        st.info("No analysis runs found.")
except Exception as e:
    st.error(f"Could not load analysis runs: {e}")
