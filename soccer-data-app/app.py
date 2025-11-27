import streamlit as st
from databricks import sql
import pandas as pd
import os

# ============================================================
# 1. App Configuration
# ============================================================
st.set_page_config(
    page_title="Big 5 Leagues Club Overview",
    page_icon="⚽", 
    layout="wide"
)
st.title("⚽ Big 5 Leagues Club Overview")

# ============================================================
# 2. Database Connection
# ============================================================
SQL_HTTP_PATH = "/sql/1.0/warehouses/ced38d4ec14f78c6"
CATALOG = "soccer_data"
SCHEMA = "analyze"
TABLE_HEADLINE = "headline_stats"
TABLE_FORM = "form_stats"

@st.cache_data(ttl=3600)
# Updated connection function for Streamlit Cloud
def get_data(table_name):
    try:
        # Streamlit Cloud stores secrets in st.secrets dict
        host = st.secrets["DATABRICKS_HOST"]
        token = st.secrets["DATABRICKS_TOKEN"]
        http_path = st.secrets["SQL_HTTP_PATH"]

        if not host or not token:
            st.error("Missing secrets.")
            return pd.DataFrame()

        # Clean Hostname
        clean_host = host.replace("https://", "").replace("http://", "").strip("/")
        
        with sql.connect(
            server_hostname=clean_host,
            http_path=http_path,
            access_token=token
        ) as connection:
            
            query = f"SELECT * FROM soccer_data.analyze.{table_name}"
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(result, columns=columns)

    except Exception as e:
        st.error(f"❌ Connection Error: {e}")
        return pd.DataFrame()

# ============================================================
# 3. Load & Process Data
# ============================================================
with st.spinner("Loading stats..."):
    df_headline = get_data(TABLE_HEADLINE)
    df_form = get_data(TABLE_FORM)

# --- Transformation: Replace Underscores with Spaces ---
if not df_headline.empty:
    df_headline.columns = [c.replace("_", " ") for c in df_headline.columns]

if not df_form.empty:
    df_form.columns = [c.replace("_", " ") for c in df_form.columns]

# ============================================================
# 4. Filters
# ============================================================
st.sidebar.title("Filters")

# Initializing filtered DataFrames
df_headline_filtered = df_headline.copy() if not df_headline.empty else pd.DataFrame()
df_form_filtered = df_form.copy() if not df_form.empty else pd.DataFrame()

# --- Filter 1: League (Applies to BOTH tables) ---
selected_league = None
if not df_headline.empty and "League" in df_headline.columns:
    all_leagues = sorted(df_headline["League"].unique().tolist())
    selected_league = st.sidebar.selectbox("Select League", all_leagues)
    
    # Apply to both
    df_headline_filtered = df_headline_filtered[df_headline_filtered["League"] == selected_league]
    if not df_form_filtered.empty and "League" in df_form_filtered.columns:
        df_form_filtered = df_form_filtered[df_form_filtered["League"] == selected_league]

# --- Filter 2: Season (Applies to LEAGUE TABLE only) ---
selected_season = None
if not df_headline.empty and "Season" in df_headline.columns:
    # Sort seasons descending so the newest is first
    all_seasons = sorted(df_headline["Season"].unique().tolist(), reverse=True)
    selected_season = st.sidebar.selectbox("Select Season", all_seasons)
    
    # Apply ONLY to headline stats
    df_headline_filtered = df_headline_filtered[df_headline_filtered["Season"] == selected_season]

# ============================================================
# 5. Main UI Tabs
# ============================================================
tab1, tab2 = st.tabs(["League Table", "Form Table (Last 8 Games)"])

# Define shared column configuration for tooltips and formatting
shared_column_config = {
    "Team-Season": None, # Hide ID
    "Possession": st.column_config.ProgressColumn(
        "Possession %", 
        format="%.1f", 
        min_value=0, 
        max_value=100
    ),
    # --- Tooltips added here ---
    "xGD W": st.column_config.NumberColumn(
        "xGD W", help="Games with greater than +0.5 xGD"
    ),
    "xGD D": st.column_config.NumberColumn(
        "xGD D", help="Games with xGD between -0.5 and +0.5"
    ),
    "xGD L": st.column_config.NumberColumn(
        "xGD L", help="Games with less than -0.5 xGD"
    )
}

# Desired column order
column_order = [
    "Position", "Team", "MP", "W", "D", "L", "Points", "PPG",
    "xGD W", "xGD D", "xGD L", "xGD Points", "xGD PPG",
    "G", "GA", "GD", "xG", "xGA", "xGD",
    "G PG", "GA PG", "GD PG", "xG PG", "xGA PG", "xGD PG",
    "Possession"
]

with tab1:
    if not df_headline_filtered.empty:
        # Sort by Points
        if "Points" in df_headline_filtered.columns:
             df_headline_filtered = df_headline_filtered.sort_values(by=["Points"], ascending=False)
        
        # Add Position
        df_headline_filtered["Position"] = range(1, len(df_headline_filtered) + 1)
        
        # Filter columns (intersection with available columns to avoid errors)
        cols = [c for c in column_order if c in df_headline_filtered.columns]
             
        st.dataframe(
            df_headline_filtered[cols], 
            use_container_width=True, 
            hide_index=True,
            column_config=shared_column_config
        )
        # Footnote
        st.caption("Source: Opta")
    else:
        st.info("No data found for the selected filters.")

with tab2:
    if not df_form_filtered.empty:
        # Sort by Points
        if "Points" in df_form_filtered.columns:
            df_form_filtered = df_form_filtered.sort_values(by="Points", ascending=False)
            
        # Add Position
        df_form_filtered["Position"] = range(1, len(df_form_filtered) + 1)

        # Filter columns
        cols = [c for c in column_order if c in df_form_filtered.columns]
            
        st.dataframe(
            df_form_filtered[cols], 
            use_container_width=True, 
            hide_index=True,
            column_config=shared_column_config
        )
        # Footnote
        st.caption("Source: Opta")
    else:
        st.info("No form data found.")