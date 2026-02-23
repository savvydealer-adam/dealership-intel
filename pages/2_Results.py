"""Results tab - View, filter, and export processing results."""

import logging

import streamlit as st

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Results - DealershipIntel", page_icon="🔍", layout="wide")
st.title("Results")

results_df = st.session_state.get("results_df")

if results_df is None or results_df.empty:
    st.info("No results yet. Go to the Process page to run the pipeline.")
    st.stop()

st.markdown(f"**{len(results_df)} dealerships** in current results.")

# --- Filters ---
with st.expander("Filters", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        status_filter = st.multiselect(
            "Status:",
            options=results_df["status"].unique().tolist() if "status" in results_df.columns else [],
        )
    with col2:
        min_confidence = st.slider("Min confidence score:", 0, 100, 0)

    filtered_df = results_df.copy()
    if status_filter:
        filtered_df = filtered_df[filtered_df["status"].isin(status_filter)]

    if min_confidence > 0 and "contact_1_confidence_score" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["contact_1_confidence_score"].fillna(0) >= min_confidence]

    st.markdown(f"**{len(filtered_df)}** dealerships after filtering.")

# --- Results Table ---
st.header("Dealership Results")

# Select display columns
display_cols = [
    col
    for col in [
        "company_name",
        "domain",
        "status",
        "industry",
        "company_size",
        "company_phone",
        "contact_1_name",
        "contact_1_title",
        "contact_1_email",
        "contact_1_confidence_score",
    ]
    if col in filtered_df.columns
]

if display_cols:
    st.dataframe(filtered_df[display_cols], use_container_width=True, height=500)
else:
    st.dataframe(filtered_df, use_container_width=True, height=500)

# --- Export ---
st.header("Export")

col1, col2 = st.columns(2)

with col1:
    csv = filtered_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="dealership_results.csv",
        mime="text/csv",
        use_container_width=True,
    )

with col2:
    if st.button("Export to Google Sheet", use_container_width=True):
        google_json = st.session_state.get("google_sheets_json_raw")
        if not google_json:
            from config.settings import get_settings

            google_json = get_settings().google_sheets_json

        if google_json:
            try:
                from services.google_sheets import GoogleSheetsService

                with st.spinner("Creating Google Sheet..."):
                    sheets_service = GoogleSheetsService(google_json)
                    import time

                    sheet_name = f"DealershipIntel Export - {time.strftime('%Y-%m-%d %H:%M')}"
                    url = sheets_service.create_and_export_sheet(filtered_df, sheet_name)
                    st.success(f"Exported to: {url}")
            except Exception as e:
                st.error(f"Export failed: {e}")
        else:
            st.error("Google Sheets credentials not configured.")

# --- Contact Details ---
st.header("Contact Details")

if "last_results" in st.session_state:
    results_with_contacts = [r for r in st.session_state.last_results if r.get("contacts")]

    if results_with_contacts:
        selected_company = st.selectbox(
            "Select dealership:",
            options=[r.get("company_name", r.get("domain", "Unknown")) for r in results_with_contacts],
        )

        selected_idx = next(
            (
                i
                for i, r in enumerate(results_with_contacts)
                if r.get("company_name", r.get("domain")) == selected_company
            ),
            0,
        )

        result = results_with_contacts[selected_idx]
        contacts = result.get("contacts", [])

        if contacts:
            st.markdown(f"**{len(contacts)} contacts** found for {selected_company}")

            for i, contact in enumerate(contacts):
                with st.container():
                    c1, c2, c3, c4 = st.columns([2, 2, 3, 1])
                    c1.markdown(f"**{contact.get('name', 'N/A')}**")
                    c2.markdown(contact.get("title", "N/A"))
                    c3.markdown(contact.get("email", "N/A"))
                    c4.markdown(f"{contact.get('confidence_score', 0)}%")
    else:
        st.info("No contacts found in results.")
