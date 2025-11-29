import streamlit as st

# Global Page Config
st.set_page_config(
    page_title="Big 5 Leagues Club Overview",
    page_icon="", 
    layout="wide"
)

# Define Pages
seasonal_page = st.Page("pages/seasonal_overview.py", title="Seasonal Overview", icon="⚽")
historic_page = st.Page("pages/Historic_Overview.py", title="Historic Overview", icon="⏳")

# Navigation
pg = st.navigation([seasonal_page, historic_page])
pg.run()
